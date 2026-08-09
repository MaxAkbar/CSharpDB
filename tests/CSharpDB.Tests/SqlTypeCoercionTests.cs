using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class SqlTypeCoercionTests
{
    private static readonly ColumnDefinition IntegerColumn = new()
    {
        Name = "value",
        Type = DbType.Integer,
        DeclaredType = SqlTypeDescriptor.Create(SqlTypeKind.Integer),
    };

    [Theory]
    [InlineData(
        null,
        "Value of type Text is not valid for column 'value' declared as INTEGER: An exact integral value is required.")]
    [InlineData(
        "bench",
        "Value of type Text is not valid for column 'bench.value' declared as INTEGER: An exact integral value is required.")]
    public void CoerceForAssignment_PreservesQualifiedTypeMismatchMessage(
        string? tableName,
        string expectedMessage)
    {
        CSharpDbException exception = Assert.Throws<CSharpDbException>(() =>
            SqlTypeCoercion.CoerceForAssignment(
                DbValue.FromText("bad"),
                IntegerColumn,
                tableName));

        Assert.Equal(ErrorCode.TypeMismatch, exception.Code);
        Assert.Equal(expectedMessage, exception.Message);
    }

    [Fact]
    public void CoerceForAssignment_SuccessPathDoesNotAllocateQualifiedTarget()
    {
        DbValue input = DbValue.FromInteger(42);
        DbValue result = SqlTypeCoercion.CoerceForAssignment(
            input,
            IntegerColumn,
            tableName: "bench");

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 4096; i++)
        {
            result = SqlTypeCoercion.CoerceForAssignment(
                input,
                IntegerColumn,
                tableName: "bench");
        }

        long allocatedBytes = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(
            allocatedBytes <= 1024,
            $"Successful assignment coercion allocated {allocatedBytes:N0} bytes.");
        Assert.Equal(42, result.AsInteger);
    }
}
