using CSharpDB.Engine;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class BuiltInFunctionCatalogTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void Registry_MapsAliasesToCanonicalDescriptorsAndReservesEveryBuiltInName()
    {
        Assert.True(DbBuiltInFunctionRegistry.TryGet("length", out var length));
        Assert.Equal("LEN", length.Name);
        Assert.Equal(DbBuiltInFunctionKind.Scalar, length.Kind);
        Assert.Equal(1, length.MinimumArity);
        Assert.Equal(1, length.MaximumArity);
        Assert.Equal(DbType.Integer, length.ReturnType);
        Assert.True(length.IsDeterministic);
        Assert.True(length.AllowedInChecks);

        Assert.True(DbBuiltInFunctionRegistry.TryGet("floor", out var floor));
        Assert.Equal("INT", floor.Name);
        Assert.Equal(DbType.Real, floor.ReturnType);

        Assert.True(DbBuiltInFunctionRegistry.TryGet("count", out var count));
        Assert.Equal(DbBuiltInFunctionKind.Aggregate, count.Kind);
        Assert.False(count.AllowedInDefaults);
        Assert.Equal(DbFunctionNullBehavior.AggregateIgnoresNulls, count.NullBehavior);

        Assert.Throws<ArgumentException>(() => DbFunctionRegistry.Create(functions =>
            functions.AddScalar("datetime", 0, static (_, _) => DbValue.Null)));
    }

    [Fact]
    public async Task FloorAlias_ExecutesThroughSql()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await using var result = await db.ExecuteAsync(
            "SELECT FLOOR(-12.55), FLOOR(NULL)",
            Ct);

        DbValue[] row = Assert.Single(await result.ToListAsync(Ct));
        Assert.Equal(-13, row[0].AsReal);
        Assert.True(row[1].IsNull);
    }

    [Fact]
    public async Task SystemFunctions_ExposesBuiltInsAliasesAndUserCallbacksWithMetadata()
    {
        var options = new DatabaseOptions().ConfigureFunctions(functions =>
            functions.AddScalar(
                "DoubleIt",
                1,
                new DbScalarFunctionOptions(
                    DbType.Integer,
                    IsDeterministic: true,
                    NullPropagating: true,
                    Description: "Doubles an integer."),
                static (_, args) => DbValue.FromInteger(args[0].AsInteger * 2)));

        await using var db = await Database.OpenInMemoryAsync(options, Ct);
        await using var result = await db.ExecuteAsync(
            """
            SELECT function_name, canonical_name, signature, function_kind, return_type,
                   null_behavior, volatility, is_deterministic, is_builtin
            FROM sys.functions
            WHERE function_name IN ('LENGTH', 'NOW', 'COUNT', 'DoubleIt')
            ORDER BY function_name
            """,
            Ct);

        var rows = await result.ToListAsync(Ct);
        Assert.Equal(4, rows.Count);

        Assert.Equal(["COUNT", "DoubleIt", "LENGTH", "NOW"], rows.Select(static row => row[0].AsText).ToArray());
        Assert.Equal("LEN", rows[2][1].AsText);
        Assert.Equal("LEN(1)", rows[2][2].AsText);
        Assert.Equal("SCALAR", rows[2][3].AsText);
        Assert.Equal("INTEGER", rows[2][4].AsText);
        Assert.Equal(1, rows[2][7].AsInteger);
        Assert.Equal(1, rows[2][8].AsInteger);

        Assert.Equal("statementstable", rows[3][6].AsText);
        Assert.Equal(0, rows[3][7].AsInteger);
        Assert.Equal("DoubleIt", rows[1][1].AsText);
        Assert.Equal("DoubleIt(1)", rows[1][2].AsText);
        Assert.Equal(0, rows[1][8].AsInteger);
    }
}
