using System.Collections;
using System.Data;
using System.Data.Common;
using System.Globalization;
using CSharpDB.Core;
using CSharpDB.Execution;
using CoreDbType = CSharpDB.Core.DbType;

namespace CSharpDB.Data;

public sealed class CSharpDbDataReader : DbDataReader
{
    private readonly QueryResult _queryResult;
    private readonly CommandBehavior _behavior;
    private readonly CSharpDbConnection? _connection;
    private readonly ColumnDefinition[] _schema;

    private DbValue[]? _currentRow;
    private int _currentRowIndex = -1;
    private bool _reachedEnd;
    private bool _sawAnyRow;
    private bool _closed;

    internal CSharpDbDataReader(
        QueryResult queryResult,
        CommandBehavior behavior,
        CSharpDbConnection? connection)
    {
        _queryResult = queryResult;
        _behavior = behavior;
        _connection = connection;
        _schema = queryResult.Schema;
    }

    private DbValue[] CurrentRow
    {
        get
        {
            if (_currentRow is null || _currentRowIndex < 0)
                throw new InvalidOperationException("No current row.");
            return _currentRow;
        }
    }

    // ─── Navigation ──────────────────────────────────────────────────

    public override bool Read()
        => ReadAsync(CancellationToken.None).GetAwaiter().GetResult();

    public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        if (_closed) return false;
        if (_reachedEnd) return false;
        if (!await _queryResult.MoveNextAsync(cancellationToken))
        {
            _currentRow = null;
            _reachedEnd = true;
            return false;
        }

        _currentRow = _queryResult.Current;
        _currentRowIndex++;
        _sawAnyRow = true;
        return true;
    }

    public override bool NextResult() => false;
    public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        => Task.FromResult(false);

    // ─── Metadata ────────────────────────────────────────────────────

    public override int FieldCount => _schema.Length;
    public override int RecordsAffected => _queryResult.RowsAffected;
    public override bool HasRows => _sawAnyRow || !_reachedEnd;
    public override bool IsClosed => _closed;
    public override int Depth => 0;

    public override string GetName(int ordinal) => _schema[ordinal].Name;

    public override int GetOrdinal(string name)
    {
        for (int i = 0; i < _schema.Length; i++)
        {
            if (string.Equals(_schema[i].Name, name, StringComparison.OrdinalIgnoreCase))
                return i;
        }
        throw new IndexOutOfRangeException($"Column '{name}' not found.");
    }

    public override string GetDataTypeName(int ordinal)
        => TypeMapper.ToDataTypeName(_schema[ordinal].Type);

    public override Type GetFieldType(int ordinal)
        => TypeMapper.ToClrType(_schema[ordinal].Type);

    // ─── Value accessors ─────────────────────────────────────────────

    public override object GetValue(int ordinal) => TypeMapper.GetClrValue(CurrentRow[ordinal]);

    public override int GetValues(object[] values)
    {
        int count = Math.Min(values.Length, _schema.Length);
        for (int i = 0; i < count; i++)
            values[i] = GetValue(i);
        return count;
    }

    public override bool IsDBNull(int ordinal) => CurrentRow[ordinal].IsNull;

    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => GetValue(GetOrdinal(name));

    // ─── Typed accessors ─────────────────────────────────────────────

    public override bool GetBoolean(int ordinal) => CurrentRow[ordinal].AsInteger != 0;

    public override byte GetByte(int ordinal) => checked((byte)CurrentRow[ordinal].AsInteger);

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        byte[] blob = CurrentRow[ordinal].AsBlob;
        if (buffer is null) return blob.Length;

        int available = (int)Math.Min(length, blob.Length - dataOffset);
        Array.Copy(blob, (int)dataOffset, buffer, bufferOffset, available);
        return available;
    }

    public override char GetChar(int ordinal) => CurrentRow[ordinal].AsText[0];

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        string text = CurrentRow[ordinal].AsText;
        if (buffer is null) return text.Length;

        int available = (int)Math.Min(length, text.Length - dataOffset);
        text.CopyTo((int)dataOffset, buffer, bufferOffset, available);
        return available;
    }

    public override DateTime GetDateTime(int ordinal)
        => DateTime.Parse(CurrentRow[ordinal].AsText, CultureInfo.InvariantCulture,
            DateTimeStyles.RoundtripKind);

    public override decimal GetDecimal(int ordinal) => (decimal)CurrentRow[ordinal].AsReal;

    public override double GetDouble(int ordinal) => CurrentRow[ordinal].AsReal;

    public override float GetFloat(int ordinal) => (float)CurrentRow[ordinal].AsReal;

    public override Guid GetGuid(int ordinal) => Guid.Parse(CurrentRow[ordinal].AsText);

    public override short GetInt16(int ordinal) => checked((short)CurrentRow[ordinal].AsInteger);

    public override int GetInt32(int ordinal) => checked((int)CurrentRow[ordinal].AsInteger);

    public override long GetInt64(int ordinal) => CurrentRow[ordinal].AsInteger;

    public override string GetString(int ordinal) => CurrentRow[ordinal].AsText;

    public override IEnumerator GetEnumerator() => new DbEnumerator(this);

    // ─── Schema table ────────────────────────────────────────────────

    public override DataTable GetSchemaTable()
    {
        var table = new DataTable("SchemaTable");
        table.Columns.Add("ColumnName", typeof(string));
        table.Columns.Add("ColumnOrdinal", typeof(int));
        table.Columns.Add("DataType", typeof(Type));
        table.Columns.Add("AllowDBNull", typeof(bool));

        for (int i = 0; i < _schema.Length; i++)
        {
            table.Rows.Add(
                _schema[i].Name,
                i,
                TypeMapper.ToClrType(_schema[i].Type),
                _schema[i].Nullable);
        }

        return table;
    }

    // ─── Lifecycle ───────────────────────────────────────────────────

    public override void Close() => CloseAsync().GetAwaiter().GetResult();

    public override async Task CloseAsync()
    {
        if (_closed) return;
        _closed = true;
        await _queryResult.DisposeAsync();

        if (_behavior.HasFlag(CommandBehavior.CloseConnection))
            _connection?.Close();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_closed)
            Close();
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        if (!_closed)
            await CloseAsync();
    }
}
