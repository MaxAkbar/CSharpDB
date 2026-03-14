using System.Data;
using CSharpDB.Primitives;
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
        _ => SysDbType.Object,
    };

    public static Type ToClrType(CoreDbType type) => type switch
    {
        CoreDbType.Integer => typeof(long),
        CoreDbType.Real => typeof(double),
        CoreDbType.Text => typeof(string),
        CoreDbType.Blob => typeof(byte[]),
        _ => typeof(object),
    };

    public static string ToDataTypeName(CoreDbType type) => type switch
    {
        CoreDbType.Null => "NULL",
        CoreDbType.Integer => "INTEGER",
        CoreDbType.Real => "REAL",
        CoreDbType.Text => "TEXT",
        CoreDbType.Blob => "BLOB",
        _ => "NULL",
    };

    public static object GetClrValue(DbValue value) => value.Type switch
    {
        CoreDbType.Null => DBNull.Value,
        CoreDbType.Integer => value.AsInteger,
        CoreDbType.Real => value.AsReal,
        CoreDbType.Text => value.AsText,
        CoreDbType.Blob => value.AsBlob,
        _ => DBNull.Value,
    };
}
