using CSharpDB.Primitives;
using SqlBitString = CSharpDB.Client.Models.SqlBitString;
using CoreDbType = CSharpDB.Primitives.DbType;
using SysDbType = System.Data.DbType;

namespace CSharpDB.Data;

internal static class TypeMapper
{
    public static SysDbType ToSystemDbType(CoreDbType type) => type switch
    {
        CoreDbType.Null => SysDbType.Object,
        CoreDbType.Integer => SysDbType.Int64,
        CoreDbType.Real => SysDbType.Double,
        CoreDbType.Text => SysDbType.String,
        CoreDbType.Blob => SysDbType.Binary,
        CoreDbType.Decimal => SysDbType.Decimal,
        _ => SysDbType.Object,
    };

    public static SysDbType ToSystemDbType(SqlTypeDescriptor type) => type.Kind switch
    {
        SqlTypeKind.Boolean => SysDbType.Boolean,
        SqlTypeKind.TinyInt => SysDbType.Byte,
        SqlTypeKind.SmallInt => SysDbType.Int16,
        SqlTypeKind.Integer => SysDbType.Int64,
        SqlTypeKind.BigInt => SysDbType.Int64,
        SqlTypeKind.Real => SysDbType.Double,
        SqlTypeKind.Double => SysDbType.Double,
        SqlTypeKind.Decimal => SysDbType.Decimal,
        SqlTypeKind.Char => SysDbType.StringFixedLength,
        SqlTypeKind.VarChar or
        SqlTypeKind.Text or
        SqlTypeKind.Json or
        SqlTypeKind.IntervalYearToMonth => SysDbType.String,
        SqlTypeKind.IntervalDayToSecond => SysDbType.Object,
        SqlTypeKind.Xml => SysDbType.Xml,
        SqlTypeKind.Binary => SysDbType.Binary,
        SqlTypeKind.VarBinary or
        SqlTypeKind.Blob or
        SqlTypeKind.Bit or
        SqlTypeKind.VarBit => SysDbType.Binary,
        SqlTypeKind.Uuid => SysDbType.Guid,
        SqlTypeKind.Date => SysDbType.Date,
        SqlTypeKind.Time => SysDbType.Time,
        SqlTypeKind.Timestamp => SysDbType.DateTime2,
        SqlTypeKind.TimestampWithTimeZone => SysDbType.DateTimeOffset,
        _ => SysDbType.Object,
    };

    public static Type ToClrType(CoreDbType type) => type switch
    {
        CoreDbType.Integer => typeof(long),
        CoreDbType.Real => typeof(double),
        CoreDbType.Text => typeof(string),
        CoreDbType.Blob => typeof(byte[]),
        CoreDbType.Decimal => typeof(decimal),
        _ => typeof(object),
    };

    public static Type ToClrType(SqlTypeDescriptor type) => type.Kind switch
    {
        SqlTypeKind.Boolean => typeof(bool),
        SqlTypeKind.TinyInt => typeof(byte),
        SqlTypeKind.SmallInt => typeof(short),
        SqlTypeKind.Integer => typeof(long),
        SqlTypeKind.BigInt => typeof(long),
        SqlTypeKind.Real => typeof(double),
        SqlTypeKind.Double => typeof(double),
        SqlTypeKind.Decimal => typeof(decimal),
        SqlTypeKind.Char or
        SqlTypeKind.VarChar or
        SqlTypeKind.Text or
        SqlTypeKind.IntervalYearToMonth or
        SqlTypeKind.Json or
        SqlTypeKind.Xml => typeof(string),
        SqlTypeKind.IntervalDayToSecond => typeof(TimeSpan),
        SqlTypeKind.Binary or
        SqlTypeKind.VarBinary or
        SqlTypeKind.Blob => typeof(byte[]),
        SqlTypeKind.Bit or
        SqlTypeKind.VarBit => typeof(SqlBitString),
        SqlTypeKind.Uuid => typeof(Guid),
        SqlTypeKind.Date => typeof(DateOnly),
        SqlTypeKind.Time => typeof(TimeOnly),
        SqlTypeKind.Timestamp => typeof(DateTime),
        SqlTypeKind.TimestampWithTimeZone => typeof(DateTimeOffset),
        _ => typeof(object),
    };

    public static string ToDataTypeName(CoreDbType type) => type switch
    {
        CoreDbType.Null => "NULL",
        CoreDbType.Integer => "INTEGER",
        CoreDbType.Real => "REAL",
        CoreDbType.Text => "TEXT",
        CoreDbType.Blob => "BLOB",
        CoreDbType.Decimal => "DECIMAL",
        _ => "NULL",
    };

    public static string ToDataTypeName(SqlTypeDescriptor type) => type.ToSql();

    public static object GetClrValue(DbValue value) => value.Type switch
    {
        CoreDbType.Null => DBNull.Value,
        CoreDbType.Integer => value.AsInteger,
        CoreDbType.Real => value.AsReal,
        CoreDbType.Text => value.AsText,
        CoreDbType.Blob => value.AsBlob,
        CoreDbType.Decimal => value.AsDecimal,
        _ => DBNull.Value,
    };

    public static object GetClrValue(DbValue value, SqlTypeDescriptor type)
    {
        if (value.IsNull)
            return DBNull.Value;

        return type.Kind switch
        {
            SqlTypeKind.Boolean => value.AsInteger != 0,
            SqlTypeKind.TinyInt => checked((byte)value.AsInteger),
            SqlTypeKind.SmallInt => checked((short)value.AsInteger),
            SqlTypeKind.Integer => value.AsInteger,
            SqlTypeKind.BigInt => value.AsInteger,
            SqlTypeKind.Real => value.AsReal,
            SqlTypeKind.Double => value.AsReal,
            SqlTypeKind.Decimal => GetDecimal(value),
            SqlTypeKind.Char or
            SqlTypeKind.VarChar or
            SqlTypeKind.Text or
            SqlTypeKind.IntervalYearToMonth or
            SqlTypeKind.Json or
            SqlTypeKind.Xml => value.AsText,
            SqlTypeKind.IntervalDayToSecond => TimeSpan.Parse(
                value.AsText,
                System.Globalization.CultureInfo.InvariantCulture),
            SqlTypeKind.Binary or
            SqlTypeKind.VarBinary or
            SqlTypeKind.Blob => value.AsBlob,
            SqlTypeKind.Bit or
            SqlTypeKind.VarBit => GetBitString(value),
            SqlTypeKind.Uuid => GetGuid(value),
            SqlTypeKind.Date => CSharpDbTextCodec.ParseDate(value.AsText),
            SqlTypeKind.Time => CSharpDbTextCodec.ParseTime(value.AsText),
            SqlTypeKind.Timestamp => CSharpDbTextCodec.ParseDateTime(value.AsText),
            SqlTypeKind.TimestampWithTimeZone => CSharpDbTextCodec.ParseDateTimeOffset(value.AsText),
            _ => GetClrValue(value),
        };
    }

    internal static decimal GetDecimal(DbValue value) => value.Type switch
    {
        CoreDbType.Decimal => value.AsDecimal,
        CoreDbType.Integer => value.AsInteger,
        CoreDbType.Real => checked((decimal)value.AsReal),
        _ => throw new InvalidOperationException($"Cannot read {value.Type} as Decimal."),
    };

    internal static Guid GetGuid(DbValue value) => value.Type switch
    {
        CoreDbType.Blob when value.AsBlob.Length == 16 => new Guid(value.AsBlob, bigEndian: true),
        CoreDbType.Blob => throw new InvalidOperationException(
            $"Cannot read a {value.AsBlob.Length}-byte BLOB as UUID; exactly 16 bytes are required."),
        CoreDbType.Text => CSharpDbTextCodec.ParseGuid(value.AsText),
        _ => throw new InvalidOperationException($"Cannot read {value.Type} as UUID."),
    };

    internal static SqlBitString GetBitString(DbValue value)
    {
        if (!value.IsBitString)
        {
            throw new InvalidOperationException(
                "Cannot read an ordinary BLOB value as a SQL BIT string.");
        }

        return new SqlBitString(value.AsBlob, value.BitLength);
    }
}
