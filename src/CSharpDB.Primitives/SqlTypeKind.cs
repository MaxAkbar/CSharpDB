namespace CSharpDB.Primitives;

/// <summary>
/// The logical SQL type declared for a column or expression. Logical types are
/// intentionally independent from the smaller set of physical <see cref="DbType"/>
/// representations used by the record format.
/// </summary>
public enum SqlTypeKind : byte
{
    Boolean = 0,
    TinyInt = 1,
    SmallInt = 2,
    Integer = 3,
    BigInt = 4,
    Real = 5,
    Double = 6,
    Decimal = 7,
    Char = 8,
    VarChar = 9,
    Text = 10,
    Binary = 11,
    VarBinary = 12,
    Blob = 13,
    Uuid = 14,
    Date = 15,
    Time = 16,
    Timestamp = 17,
    TimestampWithTimeZone = 18,
    IntervalYearToMonth = 19,
    IntervalDayToSecond = 20,
    Json = 21,
    Xml = 22,
    Bit = 23,
    VarBit = 24,
}
