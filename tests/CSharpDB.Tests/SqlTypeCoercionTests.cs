using System.Runtime.CompilerServices;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

[Collection(SqlTypeCoercionCollection.Name)]
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
        _ = FormatQualifiedTarget("bench", IntegerColumn.Name);

        long allocatedBytes;
        long positiveControlBytes;

        // Background GC can inflate this counter with unused allocation-buffer space:
        // https://github.com/dotnet/runtime/issues/134724
        // The budget also accommodates the deliberately allocating control below.
        Assert.True(
            GC.TryStartNoGCRegion(1024 * 1024, disallowFullBlockingGC: false),
            "Could not enter a no-GC region for the allocation measurement.");
        try
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < 4096; i++)
            {
                result = SqlTypeCoercion.CoerceForAssignment(
                    input,
                    IntegerColumn,
                    tableName: "bench");
            }

            allocatedBytes = GC.GetAllocatedBytesForCurrentThread() - before;

            // Model the eager qualified-target formatting this test guards against.
            // The no-inline call keeps each returned string observable.
            before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < 4096; i++)
                GC.KeepAlive(FormatQualifiedTarget("bench", IntegerColumn.Name));

            positiveControlBytes = GC.GetAllocatedBytesForCurrentThread() - before;
        }
        finally
        {
            // A premature collection or exhausted budget invalidates the sample.
            GC.EndNoGCRegion();
        }

        Assert.True(
            allocatedBytes <= 1024,
            $"Successful assignment coercion allocated {allocatedBytes:N0} bytes.");
        Assert.Equal(42, result.AsInteger);
        Assert.True(
            positiveControlBytes > 1024,
            $"The qualified-target control allocated only {positiveControlBytes:N0} bytes.");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static string FormatQualifiedTarget(string tableName, string columnName)
        => $"column '{tableName}.{columnName}'";
}

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class SqlTypeCoercionCollection
{
    public const string Name = "SqlTypeCoercion";
}
