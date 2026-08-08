using System.Collections;
using System.Data;
using System.Data.Common;
using System.Globalization;
using CSharpDB.Primitives;
using CSharpDB.Execution;
using SqlBitString = CSharpDB.Client.Models.SqlBitString;
using CoreDbType = CSharpDB.Primitives.DbType;

namespace CSharpDB.Data;

public sealed class CSharpDbDataReader : DbDataReader
{
    private const int OrdinalLookupThreshold = 8;
    private readonly QueryResult _queryResult;
    private readonly CommandBehavior _behavior;
    private readonly CSharpDbConnection? _connection;
    private readonly ColumnDefinition[] _schema;
    private readonly Dictionary<string, int>? _ordinalLookup;

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
        _ordinalLookup = BuildOrdinalLookupIfNeeded(_schema);
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
        if (_ordinalLookup != null && _ordinalLookup.TryGetValue(name, out int ordinal))
            return ordinal;

        for (int i = 0; i < _schema.Length; i++)
        {
            if (string.Equals(_schema[i].Name, name, StringComparison.OrdinalIgnoreCase))
                return i;
        }
        throw new IndexOutOfRangeException($"Column '{name}' not found.");
    }

    private static Dictionary<string, int>? BuildOrdinalLookupIfNeeded(ColumnDefinition[] schema)
    {
        if (schema.Length < OrdinalLookupThreshold)
            return null;

        var lookup = new Dictionary<string, int>(schema.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < schema.Length; i++)
        {
            string columnName = schema[i].Name;
            if (!lookup.ContainsKey(columnName))
                lookup[columnName] = i;
        }

        return lookup;
    }

    public override string GetDataTypeName(int ordinal)
    {
        ColumnDefinition column = _schema[ordinal];
        if (column.IsRowVersion)
            return "ROWVERSION";

        return column.Type == CoreDbType.Null && column.DeclaredType is null
            ? TypeMapper.ToDataTypeName(column.Type)
            : TypeMapper.ToDataTypeName(column.EffectiveType);
    }

    public override Type GetFieldType(int ordinal)
    {
        ColumnDefinition column = _schema[ordinal];
        return column.DeclaredType is null
            ? TypeMapper.ToClrType(column.Type)
            : TypeMapper.ToClrType(column.EffectiveType);
    }

    // ─── Value accessors ─────────────────────────────────────────────

    public override object GetValue(int ordinal)
    {
        ColumnDefinition column = _schema[ordinal];
        DbValue value = CurrentRow[ordinal];
        return column.DeclaredType is null
            ? TypeMapper.GetClrValue(value)
            : TypeMapper.GetClrValue(value, column.EffectiveType);
    }

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

    /// <summary>
    /// Returns the exact logical bit count of a BIT or VARBIT value. This is
    /// intentionally separate from <see cref="GetBytes"/>, whose count is the
    /// packed byte count and remains compatible with ordinary binary callers.
    /// </summary>
    public int GetBitLength(int ordinal)
    {
        DbValue value = CurrentRow[ordinal];
        if (!value.IsBitString)
        {
            throw new InvalidCastException(
                $"Column {ordinal} does not contain a SQL BIT or VARBIT value.");
        }

        return value.BitLength;
    }

    /// <summary>Returns a BIT or VARBIT value with its exact logical length.</summary>
    public SqlBitString GetBitString(int ordinal)
        => TypeMapper.GetBitString(CurrentRow[ordinal]);

    /// <summary>
    /// Keeps explicit byte-array reads compatible for BIT and VARBIT while
    /// <see cref="GetValue"/> exposes their provider-specific logical value.
    /// </summary>
    public override T GetFieldValue<T>(int ordinal)
    {
        DbValue value = CurrentRow[ordinal];
        if (typeof(T) == typeof(byte[]) && value.Type == CoreDbType.Blob)
            return (T)(object)value.AsBlob;

        return (T)GetValue(ordinal);
    }

    public override Task<T> GetFieldValueAsync<T>(
        int ordinal,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(GetFieldValue<T>(ordinal));
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
        => _schema[ordinal].DeclaredType?.Kind == SqlTypeKind.Timestamp
            ? CSharpDbTextCodec.ParseDateTime(CurrentRow[ordinal].AsText)
            : DateTime.Parse(CurrentRow[ordinal].AsText, CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind);

    public override decimal GetDecimal(int ordinal) => TypeMapper.GetDecimal(CurrentRow[ordinal]);

    public override double GetDouble(int ordinal)
        => CurrentRow[ordinal].Type == CoreDbType.Decimal
            ? (double)CurrentRow[ordinal].AsDecimal
            : CurrentRow[ordinal].AsReal;

    public override float GetFloat(int ordinal)
        => CurrentRow[ordinal].Type == CoreDbType.Decimal
            ? (float)CurrentRow[ordinal].AsDecimal
            : (float)CurrentRow[ordinal].AsReal;

    public override Guid GetGuid(int ordinal) => TypeMapper.GetGuid(CurrentRow[ordinal]);

    public override short GetInt16(int ordinal) => checked((short)CurrentRow[ordinal].AsInteger);

    public override int GetInt32(int ordinal) => checked((int)CurrentRow[ordinal].AsInteger);

    public override long GetInt64(int ordinal) => CurrentRow[ordinal].AsInteger;

    public override string GetString(int ordinal) => CurrentRow[ordinal].AsText;

    public DateOnly GetDateOnly(int ordinal)
        => CSharpDbTextCodec.ParseDate(CurrentRow[ordinal].AsText);

    public TimeOnly GetTimeOnly(int ordinal)
        => CSharpDbTextCodec.ParseTime(CurrentRow[ordinal].AsText);

    public DateTimeOffset GetDateTimeOffset(int ordinal)
        => CSharpDbTextCodec.ParseDateTimeOffset(CurrentRow[ordinal].AsText);

    public override IEnumerator GetEnumerator() => new DbEnumerator(this);

    // ─── Schema table ────────────────────────────────────────────────

    public override DataTable GetSchemaTable()
    {
        var table = new DataTable("SchemaTable");
        table.Columns.Add("ColumnName", typeof(string));
        table.Columns.Add("ColumnOrdinal", typeof(int));
        table.Columns.Add("ColumnSize", typeof(int));
        table.Columns.Add("NumericPrecision", typeof(short));
        table.Columns.Add("NumericScale", typeof(short));
        table.Columns.Add("DataType", typeof(Type));
        table.Columns.Add("ProviderType", typeof(int));
        table.Columns.Add("DataTypeName", typeof(string));
        table.Columns.Add("AllowDBNull", typeof(bool));
        table.Columns.Add("IsKey", typeof(bool));
        table.Columns.Add("IsIdentity", typeof(bool));
        table.Columns.Add("IsAutoIncrement", typeof(bool));
        table.Columns.Add("IsReadOnly", typeof(bool));
        table.Columns.Add("CollationName", typeof(string));
        table.Columns.Add("IsRowVersion", typeof(bool));

        for (int i = 0; i < _schema.Length; i++)
        {
            ColumnDefinition column = _schema[i];
            bool hasEffectiveType =
                column.DeclaredType is not null || column.Type != CoreDbType.Null;
            table.Rows.Add(
                column.Name,
                i,
                GetColumnSize(column),
                GetNumericPrecision(column),
                GetNumericScale(column),
                hasEffectiveType
                    ? TypeMapper.ToClrType(column.EffectiveType)
                    : TypeMapper.ToClrType(column.Type),
                hasEffectiveType
                    ? (int)TypeMapper.ToSystemDbType(column.EffectiveType)
                    : (int)TypeMapper.ToSystemDbType(column.Type),
                column.IsRowVersion
                    ? "ROWVERSION"
                    : hasEffectiveType
                        ? TypeMapper.ToDataTypeName(column.EffectiveType)
                        : TypeMapper.ToDataTypeName(column.Type),
                column.Nullable,
                column.IsPrimaryKey,
                column.IsIdentity,
                column.IsIdentity,
                column.IsRowVersion,
                column.Collation is null ? DBNull.Value : column.Collation,
                column.IsRowVersion);
        }

        return table;
    }

    private static object GetColumnSize(ColumnDefinition column)
    {
        if (column.IsRowVersion)
            return sizeof(ulong);

        if (column.DeclaredType is null)
        {
            return column.Type switch
            {
                CoreDbType.Integer or CoreDbType.Real => 8,
                CoreDbType.Decimal => 16,
                _ => DBNull.Value,
            };
        }

        SqlTypeDescriptor type = column.EffectiveType;
        return type.Kind switch
        {
            SqlTypeKind.Boolean or SqlTypeKind.TinyInt => 1,
            SqlTypeKind.SmallInt => 2,
            SqlTypeKind.Integer => 4,
            SqlTypeKind.BigInt or
            SqlTypeKind.Real or
            SqlTypeKind.Double => 8,
            SqlTypeKind.Decimal => 16,
            SqlTypeKind.Char or
            SqlTypeKind.VarChar or
            SqlTypeKind.Binary or
            SqlTypeKind.VarBinary or
            SqlTypeKind.Bit or
            SqlTypeKind.VarBit when type.Length.HasValue => type.Length.Value,
            SqlTypeKind.Uuid => 16,
            _ => DBNull.Value,
        };
    }

    private static object GetNumericPrecision(ColumnDefinition column)
    {
        if (column.DeclaredType is null)
        {
            return column.Type switch
            {
                CoreDbType.Integer => (short)19,
                CoreDbType.Real => (short)15,
                CoreDbType.Decimal => (short)CSharpDbDecimalCodec.DefaultPrecision,
                _ => DBNull.Value,
            };
        }

        SqlTypeDescriptor type = column.EffectiveType;
        return type.Kind switch
        {
            SqlTypeKind.Boolean => (short)1,
            SqlTypeKind.TinyInt => (short)3,
            SqlTypeKind.SmallInt => (short)5,
            SqlTypeKind.Integer => (short)10,
            SqlTypeKind.BigInt => (short)19,
            SqlTypeKind.Real => (short)15,
            SqlTypeKind.Double => (short)15,
            SqlTypeKind.Decimal => GetResolvedDecimalPrecision(type),
            _ => DBNull.Value,
        };
    }

    private static object GetNumericScale(ColumnDefinition column)
    {
        if (column.DeclaredType is null)
        {
            return column.Type switch
            {
                CoreDbType.Integer => (short)0,
                CoreDbType.Decimal => (short)CSharpDbDecimalCodec.DefaultScale,
                _ => DBNull.Value,
            };
        }

        SqlTypeDescriptor type = column.EffectiveType;
        return type.Kind switch
        {
            SqlTypeKind.Boolean or
            SqlTypeKind.TinyInt or
            SqlTypeKind.SmallInt or
            SqlTypeKind.Integer or
            SqlTypeKind.BigInt => (short)0,
            SqlTypeKind.Decimal => GetResolvedDecimalScale(type),
            _ => DBNull.Value,
        };
    }

    private static short GetResolvedDecimalPrecision(SqlTypeDescriptor type)
    {
        var facets = CSharpDbDecimalCodec.ResolveFacets(type.Precision, type.Scale);
        return checked((short)facets.Precision);
    }

    private static short GetResolvedDecimalScale(SqlTypeDescriptor type)
    {
        var facets = CSharpDbDecimalCodec.ResolveFacets(type.Precision, type.Scale);
        return checked((short)facets.Scale);
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
