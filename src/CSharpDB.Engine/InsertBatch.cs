using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Engine;

/// <summary>
/// Reusable engine-level batch insert helper for full-row inserts into a single table.
/// The batch retains row buffers across executions to avoid per-row SQL parsing and object churn.
/// </summary>
public sealed class InsertBatch
{
    private readonly Database _database;
    private readonly string _tableName;
    private readonly int _columnCount;
    private readonly long _preparedSchemaVersion;
    private DbValue[][] _rows;
    private int _count;

    internal InsertBatch(Database database, string tableName, int columnCount, long schemaVersion, int initialCapacity)
    {
        _database = database;
        _tableName = tableName;
        _columnCount = columnCount;
        _preparedSchemaVersion = schemaVersion;
        _rows = initialCapacity > 0 ? new DbValue[initialCapacity][] : Array.Empty<DbValue[]>();
    }

    /// <summary>
    /// Table name this batch inserts into.
    /// </summary>
    public string TableName => _tableName;

    /// <summary>
    /// Number of values required per row.
    /// </summary>
    public int ColumnCount => _columnCount;

    /// <summary>
    /// Number of buffered rows awaiting execution.
    /// </summary>
    public int Count => _count;

    /// <summary>
    /// Current reusable row-buffer capacity.
    /// </summary>
    public int Capacity => _rows.Length;

    /// <summary>
    /// Add a row to the batch. The values are copied into an internal reusable buffer.
    /// </summary>
    public void AddRow(ReadOnlySpan<DbValue> values)
    {
        EnsureSchemaIsCurrent();

        if (values.Length != _columnCount)
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                $"Expected {_columnCount} values, got {values.Length}.");
        }

        EnsureCapacity(_count + 1);
        var row = _rows[_count] ??= new DbValue[_columnCount];
        values.CopyTo(row);
        _count++;
    }

    /// <summary>
    /// Add a row using a params array convenience overload.
    /// </summary>
    public void AddRow(params DbValue[] values)
        => AddRow((ReadOnlySpan<DbValue>)values);

    /// <summary>
    /// Remove all buffered rows without executing them.
    /// </summary>
    public void Clear()
        => _count = 0;

    /// <summary>
    /// Execute all buffered rows and then clear the batch on success.
    /// Honors the database's current transaction mode.
    /// </summary>
    public async ValueTask<int> ExecuteAsync(CancellationToken ct = default)
    {
        EnsureSchemaIsCurrent();

        if (_count == 0)
            return 0;

        await using var result = await _database.ExecuteAsync(new SimpleInsertSql(_tableName, _rows, _count), ct);
        int rowsAffected = result.RowsAffected;
        _count = 0;
        return rowsAffected;
    }

    private void EnsureCapacity(int required)
    {
        if (required <= _rows.Length)
            return;

        int newCapacity = _rows.Length == 0 ? 4 : _rows.Length;
        while (newCapacity < required)
            newCapacity = checked(newCapacity * 2);

        Array.Resize(ref _rows, newCapacity);
    }

    private void EnsureSchemaIsCurrent()
    {
        if (_database.SchemaVersion != _preparedSchemaVersion)
        {
            throw new InvalidOperationException(
                $"Insert batch for table '{_tableName}' is invalid because the database schema changed.");
        }
    }
}
