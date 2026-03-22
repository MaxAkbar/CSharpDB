using CSharpDB.Client.Models;
using CSharpDB.Pipelines.Models;
using CSharpDB.Pipelines.Runtime;
using CSharpDB.Pipelines.Runtime.BuiltIns;
using System.Globalization;
using System.Text.RegularExpressions;

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
    private static readonly Regex s_identifierPattern = new("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled);
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
        TransactionSessionInfo transaction = await _client.BeginTransactionAsync(ct);
        bool committed = false;

        try
        {
            for (int i = 0; i < batch.Rows.Count; i++)
            {
                ct.ThrowIfCancellationRequested();
                var row = batch.Rows[i];
                var writeRow = CoerceRowValues(row);
                try
                {
                    await _client.ExecuteInTransactionAsync(
                        transaction.TransactionId,
                        BuildInsertSql(_definition.TableName!, writeRow),
                        ct);
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

            await _client.CommitTransactionAsync(transaction.TransactionId, ct);
            committed = true;
        }
        finally
        {
            if (!committed)
            {
                try
                {
                    await _client.RollbackTransactionAsync(transaction.TransactionId, ct);
                }
                catch
                {
                    // Preserve the original write error.
                }
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

    private static string BuildInsertSql(string tableName, IReadOnlyDictionary<string, object?> values)
    {
        if (values.Count == 0)
            throw new InvalidOperationException($"Destination table '{tableName}' has no writable columns for the current pipeline row.");

        string normalizedTableName = RequireIdentifier(tableName, nameof(tableName));
        string[] columns = values.Keys.Select(static key => RequireIdentifier(key, nameof(values))).ToArray();
        string[] literals = values.Values.Select(FormatSqlLiteral).ToArray();
        return $"INSERT INTO {normalizedTableName} ({string.Join(", ", columns)}) VALUES ({string.Join(", ", literals)})";
    }

    private static string FormatSqlLiteral(object? value)
    {
        object? normalized = NormalizeValue(value);
        return normalized switch
        {
            null => "NULL",
            long integer => integer.ToString(CultureInfo.InvariantCulture),
            double real => real.ToString(CultureInfo.InvariantCulture),
            string text => $"'{text.Replace("'", "''", StringComparison.Ordinal)}'",
            byte[] => throw new InvalidOperationException("Blob values are not supported by the pipeline table destination."),
            _ => $"'{Convert.ToString(normalized, CultureInfo.InvariantCulture)?.Replace("'", "''", StringComparison.Ordinal) ?? string.Empty}'",
        };
    }

    private static object? NormalizeValue(object? value) => value switch
    {
        null => null,
        bool boolean => boolean ? 1L : 0L,
        byte or sbyte or short or ushort or int or uint or long => Convert.ToInt64(value, CultureInfo.InvariantCulture),
        float or double or decimal => Convert.ToDouble(value, CultureInfo.InvariantCulture),
        string text => text,
        byte[] blob => blob,
        _ => Convert.ToString(value, CultureInfo.InvariantCulture),
    };

    private static string RequireIdentifier(string value, string paramName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, paramName);
        if (!s_identifierPattern.IsMatch(value))
            throw new InvalidOperationException($"Identifier '{value}' is not supported by the pipeline table destination.");
        return value;
    }
}
