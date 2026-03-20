using CSharpDB.Client.Models;
using CSharpDB.Pipelines.Models;
using CSharpDB.Pipelines.Runtime;
using CSharpDB.Pipelines.Runtime.BuiltIns;

namespace CSharpDB.Client.Pipelines;

public sealed class CSharpDbPipelineComponentFactory : IPipelineComponentFactory
{
    private readonly ICSharpDbClient _client;
    private readonly DefaultPipelineComponentFactory _fallback = new();

    public CSharpDbPipelineComponentFactory(ICSharpDbClient client)
    {
        _client = client;
    }

    public IPipelineSource CreateSource(PipelineSourceDefinition definition) => definition.Kind switch
    {
        PipelineSourceKind.CSharpDbTable => new CSharpDbTablePipelineSource(_client, definition),
        PipelineSourceKind.SqlQuery => new SqlQueryPipelineSource(_client, definition),
        _ => _fallback.CreateSource(definition),
    };

    public IReadOnlyList<IPipelineTransform> CreateTransforms(IReadOnlyList<PipelineTransformDefinition> definitions)
        => _fallback.CreateTransforms(definitions);

    public IPipelineDestination CreateDestination(PipelineDestinationDefinition definition) => definition.Kind switch
    {
        PipelineDestinationKind.CSharpDbTable => new CSharpDbTablePipelineDestination(_client, definition),
        _ => _fallback.CreateDestination(definition),
    };
}

internal sealed class CSharpDbTablePipelineSource : IPipelineSource
{
    private readonly ICSharpDbClient _client;
    private readonly PipelineSourceDefinition _definition;

    public CSharpDbTablePipelineSource(ICSharpDbClient client, PipelineSourceDefinition definition)
    {
        _client = client;
        _definition = definition;
    }

    public async Task OpenAsync(PipelineExecutionContext context, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(_definition.TableName))
            throw new InvalidOperationException("CSharpDB table source requires a table name.");

        _ = await _client.GetTableSchemaAsync(_definition.TableName, ct)
            ?? throw new InvalidOperationException($"Source table '{_definition.TableName}' was not found.");
    }

    public async IAsyncEnumerable<PipelineRowBatch> ReadBatchesAsync(
        PipelineExecutionContext context,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        int page = 1;
        int pageSize = context.Package.Options.BatchSize;
        long batchNumber = 0;
        long rowNumber = 0;

        while (true)
        {
            TableBrowseResult result = await _client.BrowseTableAsync(_definition.TableName!, page, pageSize, ct);
            if (result.Rows.Count == 0)
            {
                yield break;
            }

            string[] columnNames = result.Schema.Columns.Select(c => c.Name).ToArray();
            var rows = result.Rows.Select(values => ToDictionary(columnNames, values)).ToArray();
            batchNumber++;
            if (context.Checkpoint is not null && batchNumber <= context.Checkpoint.BatchNumber)
            {
                rowNumber += rows.Length;
                if (page >= result.TotalPages)
                    yield break;
                page++;
                continue;
            }

            yield return new PipelineRowBatch
            {
                BatchNumber = batchNumber,
                StartingRowNumber = rowNumber + 1,
                Rows = rows,
            };

            rowNumber += rows.Length;
            if (page >= result.TotalPages)
            {
                yield break;
            }

            page++;
        }
    }

    private static Dictionary<string, object?> ToDictionary(string[] columnNames, object?[] values)
    {
        var row = new Dictionary<string, object?>(columnNames.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < columnNames.Length; i++)
        {
            row[columnNames[i]] = i < values.Length ? values[i] : null;
        }

        return row;
    }
}

internal sealed class SqlQueryPipelineSource : IPipelineSource
{
    private readonly ICSharpDbClient _client;
    private readonly PipelineSourceDefinition _definition;

    public SqlQueryPipelineSource(ICSharpDbClient client, PipelineSourceDefinition definition)
    {
        _client = client;
        _definition = definition;
    }

    public Task OpenAsync(PipelineExecutionContext context, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(_definition.QueryText))
            throw new InvalidOperationException("SQL query source requires query text.");

        return Task.CompletedTask;
    }

    public async IAsyncEnumerable<PipelineRowBatch> ReadBatchesAsync(
        PipelineExecutionContext context,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        SqlExecutionResult result = await _client.ExecuteSqlAsync(_definition.QueryText!, ct);
        if (!result.IsQuery || result.Rows is null || result.ColumnNames is null)
            throw new InvalidOperationException("SQL query source must produce a query result set.");

        int batchSize = context.Package.Options.BatchSize;
        long batchNumber = 0;
        long rowNumber = 0;

        for (int i = 0; i < result.Rows.Count; i += batchSize)
        {
            ct.ThrowIfCancellationRequested();
            var slice = result.Rows.Skip(i).Take(batchSize).Select(values => ToDictionary(result.ColumnNames, values)).ToArray();
            batchNumber++;
            if (context.Checkpoint is not null && batchNumber <= context.Checkpoint.BatchNumber)
            {
                rowNumber += slice.Length;
                continue;
            }

            yield return new PipelineRowBatch
            {
                BatchNumber = batchNumber,
                StartingRowNumber = rowNumber + 1,
                Rows = slice,
            };
            rowNumber += slice.Length;
        }
    }

    private static Dictionary<string, object?> ToDictionary(string[] columnNames, object?[] values)
    {
        var row = new Dictionary<string, object?>(columnNames.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < columnNames.Length; i++)
        {
            row[columnNames[i]] = i < values.Length ? values[i] : null;
        }

        return row;
    }
}

internal sealed class CSharpDbTablePipelineDestination : IPipelineDestination
{
    private readonly ICSharpDbClient _client;
    private readonly PipelineDestinationDefinition _definition;

    public CSharpDbTablePipelineDestination(ICSharpDbClient client, PipelineDestinationDefinition definition)
    {
        _client = client;
        _definition = definition;
    }

    public async Task InitializeAsync(PipelineExecutionContext context, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(_definition.TableName))
            throw new InvalidOperationException("CSharpDB table destination requires a table name.");

        _ = await _client.GetTableSchemaAsync(_definition.TableName, ct)
            ?? throw new InvalidOperationException($"Destination table '{_definition.TableName}' was not found.");
    }

    public async Task WriteBatchAsync(PipelineRowBatch batch, PipelineExecutionContext context, CancellationToken ct = default)
    {
        foreach (var row in batch.Rows)
        {
            ct.ThrowIfCancellationRequested();
            await _client.InsertRowAsync(_definition.TableName!, new Dictionary<string, object?>(row, StringComparer.OrdinalIgnoreCase), ct);
        }
    }

    public Task CompleteAsync(PipelineExecutionContext context, CancellationToken ct = default)
        => Task.CompletedTask;
}
