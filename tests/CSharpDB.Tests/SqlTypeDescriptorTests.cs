using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class SqlTypeDescriptorTests
{
    [Fact]
    public void ToSql_FormatsCanonicalFacetSpellings()
    {
        Assert.Equal(
            "DECIMAL(18,4)",
            SqlTypeDescriptor.Create(SqlTypeKind.Decimal, precision: 18, scale: 4).ToSql());
        Assert.Equal(
            "VARCHAR(200)",
            SqlTypeDescriptor.Create(SqlTypeKind.VarChar, length: 200).ToSql());
        Assert.Equal(
            "DATETIMEOFFSET(6)",
            SqlTypeDescriptor.Create(
                SqlTypeKind.TimestampWithTimeZone,
                fractionalSecondsPrecision: 6).ToSql());
        Assert.Equal(
            "DATETIME2(7)",
            SqlTypeDescriptor.Create(
                SqlTypeKind.Timestamp,
                fractionalSecondsPrecision: 7).ToSql());
        Assert.Equal(
            "INTERVAL YEAR TO MONTH",
            SqlTypeDescriptor.Create(SqlTypeKind.IntervalYearToMonth).ToSql());
        Assert.Equal(
            "BIT VARYING(64)",
            SqlTypeDescriptor.Create(SqlTypeKind.VarBit, length: 64).ToSql());
    }

    [Fact]
    public void StorageType_SeparatesLogicalAndPhysicalTypes()
    {
        Assert.Equal(DbType.Integer, SqlTypeDescriptor.Create(SqlTypeKind.Boolean).StorageType);
        Assert.Equal(DbType.Integer, SqlTypeDescriptor.Create(SqlTypeKind.SmallInt).StorageType);
        Assert.Equal(DbType.Real, SqlTypeDescriptor.Create(SqlTypeKind.Double).StorageType);
        Assert.Equal(DbType.Decimal, SqlTypeDescriptor.Create(SqlTypeKind.Decimal).StorageType);
        Assert.Equal(DbType.Text, SqlTypeDescriptor.Create(SqlTypeKind.Json).StorageType);
        Assert.Equal(DbType.Blob, SqlTypeDescriptor.Create(SqlTypeKind.Uuid).StorageType);
    }

    [Fact]
    public void Constructor_RejectsInvalidFacetCombinations()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => SqlTypeDescriptor.Create(SqlTypeKind.VarChar, length: 0));
        Assert.Throws<ArgumentException>(
            () => SqlTypeDescriptor.Create(SqlTypeKind.Text, length: 20));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => SqlTypeDescriptor.Create(SqlTypeKind.Decimal, precision: 19));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => SqlTypeDescriptor.Create(SqlTypeKind.Decimal, precision: 4, scale: 5));
        Assert.Throws<ArgumentException>(
            () => SqlTypeDescriptor.Create(SqlTypeKind.Date, fractionalSecondsPrecision: 3));
    }

    [Theory]
    [InlineData(SqlTypeKind.Time)]
    [InlineData(SqlTypeKind.Timestamp)]
    [InlineData(SqlTypeKind.TimestampWithTimeZone)]
    [InlineData(SqlTypeKind.IntervalDayToSecond)]
    public void Constructor_RejectsFractionalSecondPrecisionBeyondClrTicks(SqlTypeKind kind)
    {
        Assert.Equal(7, SqlTypeDescriptor.MaximumFractionalSecondsPrecision);
        Assert.Throws<ArgumentOutOfRangeException>(
            () => SqlTypeDescriptor.Create(kind, fractionalSecondsPrecision: 8));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => SqlTypeDescriptor.Create(kind, fractionalSecondsPrecision: 9));
    }

    [Fact]
    public void ColumnEffectiveType_UsesDeclaredTypeOrLegacyFallback()
    {
        var legacy = new ColumnDefinition
        {
            Name = "legacy_value",
            Type = DbType.Integer,
        };
        SqlTypeDescriptor declared = SqlTypeDescriptor.Create(SqlTypeKind.TinyInt);
        var typed = new ColumnDefinition
        {
            Name = "typed_value",
            Type = DbType.Integer,
            DeclaredType = declared,
        };

        Assert.Equal(SqlTypeKind.BigInt, legacy.EffectiveType.Kind);
        Assert.Same(declared, typed.EffectiveType);
    }
}
