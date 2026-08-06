using ClientSqlTypeDescriptor = CSharpDB.Client.Models.SqlTypeDescriptor;
using ClientSqlTypeKind = CSharpDB.Client.Models.SqlTypeKind;
using PrimitiveSqlTypeDescriptor = CSharpDB.Primitives.SqlTypeDescriptor;
using PrimitiveSqlTypeKind = CSharpDB.Primitives.SqlTypeKind;

namespace CSharpDB.DevOps;

internal static class PrimitiveSchemaTypeMapper
{
    public static ClientSqlTypeDescriptor? Map(PrimitiveSqlTypeDescriptor? type) => type is null
        ? null
        : new ClientSqlTypeDescriptor
        {
            Kind = type.Kind switch
            {
                PrimitiveSqlTypeKind.Boolean => ClientSqlTypeKind.Boolean,
                PrimitiveSqlTypeKind.TinyInt => ClientSqlTypeKind.TinyInt,
                PrimitiveSqlTypeKind.SmallInt => ClientSqlTypeKind.SmallInt,
                PrimitiveSqlTypeKind.Integer => ClientSqlTypeKind.Integer,
                PrimitiveSqlTypeKind.BigInt => ClientSqlTypeKind.BigInt,
                PrimitiveSqlTypeKind.Real => ClientSqlTypeKind.Real,
                PrimitiveSqlTypeKind.Double => ClientSqlTypeKind.Double,
                PrimitiveSqlTypeKind.Decimal => ClientSqlTypeKind.Decimal,
                PrimitiveSqlTypeKind.Char => ClientSqlTypeKind.Char,
                PrimitiveSqlTypeKind.VarChar => ClientSqlTypeKind.VarChar,
                PrimitiveSqlTypeKind.Text => ClientSqlTypeKind.Text,
                PrimitiveSqlTypeKind.Binary => ClientSqlTypeKind.Binary,
                PrimitiveSqlTypeKind.VarBinary => ClientSqlTypeKind.VarBinary,
                PrimitiveSqlTypeKind.Blob => ClientSqlTypeKind.Blob,
                PrimitiveSqlTypeKind.Uuid => ClientSqlTypeKind.Uuid,
                PrimitiveSqlTypeKind.Date => ClientSqlTypeKind.Date,
                PrimitiveSqlTypeKind.Time => ClientSqlTypeKind.Time,
                PrimitiveSqlTypeKind.Timestamp => ClientSqlTypeKind.Timestamp,
                PrimitiveSqlTypeKind.TimestampWithTimeZone => ClientSqlTypeKind.TimestampWithTimeZone,
                PrimitiveSqlTypeKind.IntervalYearToMonth => ClientSqlTypeKind.IntervalYearToMonth,
                PrimitiveSqlTypeKind.IntervalDayToSecond => ClientSqlTypeKind.IntervalDayToSecond,
                PrimitiveSqlTypeKind.Json => ClientSqlTypeKind.Json,
                PrimitiveSqlTypeKind.Xml => ClientSqlTypeKind.Xml,
                PrimitiveSqlTypeKind.Bit => ClientSqlTypeKind.Bit,
                PrimitiveSqlTypeKind.VarBit => ClientSqlTypeKind.VarBit,
                _ => throw new ArgumentOutOfRangeException(nameof(type), type.Kind, null),
            },
            Length = type.Length,
            Precision = type.Precision,
            Scale = type.Scale,
            FractionalSecondsPrecision = type.FractionalSecondsPrecision,
        };
}
