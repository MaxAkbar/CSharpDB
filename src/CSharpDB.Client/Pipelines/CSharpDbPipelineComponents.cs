using CSharpDB.Client.Models;
using CSharpDB.Pipelines.Models;
using CSharpDB.Pipelines.Runtime;
using CSharpDB.Pipelines.Runtime.BuiltIns;
using System.Globalization;

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
    private TableSchema? _tableSchema;

    public CSharpDbTablePipelineDestination(ICSharpDbClient client, PipelineDestinationDefinition definition)
    {
        _client = client;
        _definition = definition;
    }

    public async Task InitializeAsync(PipelineExecutionContext context, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(_definition.TableName))
            throw new InvalidOperationException("CSharpDB table destination requires a table name.");

        _tableSchema = await _client.GetTableSchemaAsync(_definition.TableName, ct)
            ?? throw new InvalidOperationException($"Destination table '{_definition.TableName}' was not found.");
    }

    public async Task WriteBatchAsync(PipelineRowBatch batch, PipelineExecutionContext context, CancellationToken ct = default)
    {
        for (int i = 0; i < batch.Rows.Count; i++)
        {
            ct.ThrowIfCancellationRequested();
            var row = batch.Rows[i];
            var writeRow = CoerceRowValues(row);
            try
            {
                await _client.InsertRowAsync(_definition.TableName!, writeRow, ct);
            }
            catch (Exception ex)
            {
                long rowNumber = batch.StartingRowNumber + i;
                string mismatchDetails = DescribeTypeMismatch(writeRow);
                throw new InvalidOperationException(
                    $"Destination write failed for table '{_definition.TableName}' at row {rowNumber} (batch {batch.BatchNumber}). {ex.Message}{mismatchDetails}",
                    ex);
            }
        }
    }

    public Task CompleteAsync(PipelineExecutionContext context, CancellationToken ct = default)
        => Task.CompletedTask;

    private Dictionary<string, object?> CoerceRowValues(IReadOnlyDictionary<string, object?> row)
    {
        if (_tableSchema is null)
        {
            return new Dictionary<string, object?>(row, StringComparer.OrdinalIgnoreCase);
        }

        var writeRow = new Dictionary<string, object?>(_tableSchema.Columns.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var column in _tableSchema.Columns)
        {
            if (!row.TryGetValue(column.Name, out var value))
            {
                continue;
            }

            writeRow[column.Name] = value is null
                ? null
                : CoerceValue(column.Type, value);
        }

        return writeRow;
    }

    private string DescribeTypeMismatch(IReadOnlyDictionary<string, object?> row)
    {
        if (_tableSchema is null)
        {
            return string.Empty;
        }

        foreach (var column in _tableSchema.Columns)
        {
            if (!row.TryGetValue(column.Name, out var value) || value is null)
            {
                continue;
            }

            if (IsCompatibleValue(column.Type, value))
            {
                continue;
            }

            return $" Column '{column.Name}' expects {column.Type} but received {DescribeValueType(value)} value '{value}'.";
        }

        return string.Empty;
    }

    private static bool IsCompatibleValue(DbType columnType, object value) => columnType switch
    {
        DbType.Integer => value is sbyte or byte or short or ushort or int or uint or long or ulong,
        DbType.Real => value is sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal,
        DbType.Text => value is string or char,
        DbType.Blob => value is byte[],
        _ => true,
    };

    private static string DescribeValueType(object value) =>
        value switch
        {
            byte[] => "Blob",
            string => "Text",
            char => "Text",
            sbyte or byte or short or ushort or int or uint or long or ulong => "Integer",
            float or double or decimal => "Real",
            _ => value.GetType().Name,
        };

    private static object? CoerceValue(DbType columnType, object? value)
    {
        if (value is null)
        {
            return null;
        }

        return columnType switch
        {
            DbType.Integer => value switch
            {
                long longValue => longValue,
                int intValue => (long)intValue,
                short shortValue => (long)shortValue,
                sbyte sbyteValue => (long)sbyteValue,
                byte byteValue => (long)byteValue,
                ushort ushortValue => (long)ushortValue,
                uint uintValue => checked((long)uintValue),
                ulong ulongValue => checked((long)ulongValue),
                double doubleValue => checked((long)doubleValue),
                float floatValue => checked((long)floatValue),
                decimal decimalValue => checked((long)decimalValue),
                bool boolValue => boolValue ? 1L : 0L,
                string textValue => long.Parse(textValue, CultureInfo.InvariantCulture),
                _ => Convert.ToInt64(value, CultureInfo.InvariantCulture),
            },
            DbType.Real => value switch
            {
                double doubleValue => doubleValue,
                float floatValue => (double)floatValue,
                decimal decimalValue => (double)decimalValue,
                long longValue => (double)longValue,
                int intValue => intValue,
                short shortValue => shortValue,
                sbyte sbyteValue => sbyteValue,
                byte byteValue => byteValue,
                ushort ushortValue => ushortValue,
                uint uintValue => uintValue,
                ulong ulongValue => ulongValue,
                bool boolValue => boolValue ? 1d : 0d,
                string textValue => double.Parse(textValue, CultureInfo.InvariantCulture),
                _ => Convert.ToDouble(value, CultureInfo.InvariantCulture),
            },
            DbType.Text => value switch
            {
                string textValue => textValue,
                IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
                _ => value.ToString(),
            },
            DbType.Blob => value switch
            {
                byte[] bytes => bytes,
                string textValue => Convert.FromBase64String(textValue),
                _ => value,
            },
            _ => value,
        };
    }
}
