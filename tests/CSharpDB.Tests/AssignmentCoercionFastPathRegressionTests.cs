using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class AssignmentCoercionFastPathRegressionTests
{
    [Theory]
    [InlineData(SqlTypeKind.BigInt, long.MinValue)]
    [InlineData(SqlTypeKind.BigInt, long.MaxValue)]
    [InlineData(SqlTypeKind.BigInt, 0L)]
    [InlineData(SqlTypeKind.Integer, (long)int.MinValue)]
    [InlineData(SqlTypeKind.Integer, (long)int.MaxValue)]
    [InlineData(SqlTypeKind.Integer, 0L)]
    public void IntegerAssignments_PreserveBoundaryValuesAndPhysicalTags(
        SqlTypeKind kind,
        long number)
    {
        DbValue input = DbValue.FromInteger(number);
        DbValue actual = SqlTypeCoercion.CoerceForAssignment(input, Column(kind), "values");

        Assert.Equal(input, actual);
        Assert.Equal(DbType.Integer, actual.Type);
        Assert.Equal(number, actual.AsInteger);
    }

    [Fact]
    public void TextAssignments_PreserveEverySuppliedCodeUnitAndStringReference()
    {
        // TEXT assignment does not normalize Unicode, reject embedded NULs,
        // or validate UTF-16. Any such work belongs to a later boundary.
        string[] inputs = [string.Empty, "plain", "e\u0301", "a\0b", new string('\ud800', 1)];
        foreach (string text in inputs)
        {
            DbValue input = DbValue.FromText(text);
            DbValue actual = SqlTypeCoercion.CoerceForAssignment(
                input, Column(SqlTypeKind.Text), "values");

            Assert.Equal(input, actual);
            Assert.Equal(DbType.Text, actual.Type);
            Assert.Same(text, actual.AsText);
        }
    }

    [Theory]
    [InlineData(-2147483649L, null)]
    [InlineData(2147483648L, null)]
    [InlineData(-2147483649L, "values")]
    [InlineData(2147483648L, "values")]
    public void OutOfRangeIntegerAssignments_PreserveWrappedRangeFailure(
        long number,
        string? tableName)
    {
        string target = tableName is null ? "column 'value'" : "column 'values.value'";
        string rangeMessage = $"{number} is outside the INTEGER range -2147483648 through 2147483647.";
        CSharpDbException error = Assert.Throws<CSharpDbException>(() =>
            SqlTypeCoercion.CoerceForAssignment(
                DbValue.FromInteger(number), Column(SqlTypeKind.Integer), tableName));

        Assert.Equal(ErrorCode.TypeMismatch, error.Code);
        Assert.Equal($"Value of type Integer is not valid for {target} declared as INTEGER: {rangeMessage}", error.Message);
        Assert.Equal(rangeMessage, Assert.IsType<OverflowException>(error.InnerException).Message);
    }

    [Theory]
    [InlineData(SqlTypeKind.Integer)]
    [InlineData(SqlTypeKind.BigInt)]
    public void IntegralRealAndDecimalAssignments_StillCanonicalizeToInteger(SqlTypeKind kind)
    {
        ColumnDefinition column = Column(kind);
        foreach (DbValue input in new[] { DbValue.FromReal(42d), DbValue.FromDecimal(42m) })
        {
            DbValue actual = SqlTypeCoercion.CoerceForAssignment(input, column, "values");
            Assert.Equal(DbType.Integer, actual.Type);
            Assert.Equal(42L, actual.AsInteger);
        }
    }

    [Fact]
    public void TextAssignment_RejectsIntegerWithExistingWrappedMessage()
    {
        CSharpDbException error = Assert.Throws<CSharpDbException>(() =>
            SqlTypeCoercion.CoerceForAssignment(
                DbValue.FromInteger(42), Column(SqlTypeKind.Text), "values"));

        Assert.Equal(ErrorCode.TypeMismatch, error.Code);
        Assert.Equal(
            "Value of type Integer is not valid for column 'values.value' declared as TEXT: A character value is required.",
            error.Message);
        Assert.Equal(
            "A character value is required.",
            Assert.IsType<InvalidOperationException>(error.InnerException).Message);
    }

    [Fact]
    public void NullValue_PreservesNullButDoesNotBypassColumnArgumentValidation()
    {
        Assert.Equal(DbValue.Null, SqlTypeCoercion.CoerceForAssignment(DbValue.Null, Column(SqlTypeKind.Text)));
        ArgumentNullException error = Assert.Throws<ArgumentNullException>(() =>
            SqlTypeCoercion.CoerceForAssignment(DbValue.Null, null!));
        Assert.Equal("column", error.ParamName);
    }

    [Fact]
    public void MixedRow_TypeErrorPrecedesEarlierNotNullFailureAndPreservesPrefixMutation()
    {
        var schema = new TableSchema
        {
            TableName = "mixed",
            Columns =
            [
                Column(SqlTypeKind.Text, "required", nullable: false),
                Column(SqlTypeKind.Boolean, "normalized"),
                Column(SqlTypeKind.Integer, "invalid"),
                Column(SqlTypeKind.Boolean, "untouched"),
            ],
        };
        DbValue[] row = [DbValue.Null, DbValue.FromInteger(2), DbValue.FromText("bad"), DbValue.FromInteger(3)];

        CSharpDbException error = Assert.Throws<CSharpDbException>(() =>
            RowConstraintValidator.ValidateRow(schema, row));

        Assert.Equal(ErrorCode.TypeMismatch, error.Code);
        Assert.Equal(
            "Value of type Text is not valid for column 'mixed.invalid' declared as INTEGER: An exact integral value is required.",
            error.Message);
        Assert.Equal(DbValue.Null, row[0]);
        Assert.Equal(DbValue.FromInteger(1), row[1]);
        Assert.Equal(DbValue.FromText("bad"), row[2]);
        Assert.Equal(DbValue.FromInteger(3), row[3]);
    }

    [Fact]
    public void MixedRow_AllCoercionsFinishBeforeNotNullFailure()
    {
        var schema = new TableSchema
        {
            TableName = "mixed",
            Columns =
            [
                Column(SqlTypeKind.Text, "required", nullable: false),
                Column(SqlTypeKind.Boolean, "normalized"),
                Column(SqlTypeKind.Char, "padded", length: 3),
                Column(SqlTypeKind.Bit, "bits", length: 3),
            ],
        };
        DbValue[] row = [DbValue.Null, DbValue.FromInteger(2), DbValue.FromText("x"), DbValue.FromText("1")];

        CSharpDbException error = Assert.Throws<CSharpDbException>(() =>
            RowConstraintValidator.ValidateRow(schema, row));

        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
        Assert.Equal("NOT NULL constraint failed: mixed.required.", error.Message);
        Assert.Equal(DbValue.Null, row[0]);
        Assert.Equal(DbValue.FromInteger(1), row[1]);
        Assert.Equal("x  ", row[2].AsText);
        Assert.True(row[3].IsBitString);
        Assert.Equal(3, row[3].BitLength);
        Assert.Equal("100", row[3].AsBitString);
    }

    [Fact]
    public void LogicalBlobAndLegacyBlob_KeepTheirDifferentBitMetadataBehavior()
    {
        DbValue bits = DbValue.FromBitString([0x80], 1);
        var legacyColumn = new ColumnDefinition { Name = "value", Type = DbType.Blob };

        DbValue legacy = SqlTypeCoercion.CoerceForAssignment(bits, legacyColumn);
        DbValue logical = SqlTypeCoercion.CoerceForAssignment(bits, Column(SqlTypeKind.Blob));

        Assert.Equal(bits, legacy);
        Assert.True(legacy.IsBitString);
        Assert.False(logical.IsBitString);
        Assert.Same(bits.AsBlob, logical.AsBlob);
    }

    private static ColumnDefinition Column(
        SqlTypeKind kind,
        string name = "value",
        bool nullable = true,
        int? length = null)
    {
        SqlTypeDescriptor declaredType = SqlTypeDescriptor.Create(kind, length: length);
        return new ColumnDefinition
        {
            Name = name,
            Type = declaredType.StorageType,
            DeclaredType = declaredType,
            Nullable = nullable,
        };
    }
}
