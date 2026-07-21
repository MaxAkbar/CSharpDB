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
    public void Registry_DescribesOrdinalStringSearchFunctions()
    {
        string[] names = ["ORDINAL_STARTS_WITH", "ORDINAL_ENDS_WITH", "ORDINAL_CONTAINS"];

        foreach (string name in names)
        {
            Assert.True(DbBuiltInFunctionRegistry.TryGet(name, out var function));
            Assert.Equal(name, function.Name);
            Assert.Empty(function.Aliases);
            Assert.Equal(DbBuiltInFunctionKind.Scalar, function.Kind);
            Assert.Equal(2, function.MinimumArity);
            Assert.Equal(2, function.MaximumArity);
            Assert.Equal("text, text", function.AcceptedTypes);
            Assert.Equal(DbType.Integer, function.ReturnType);
            Assert.Equal("boolean integer", function.ReturnTypeRule);
            Assert.Equal(DbFunctionNullBehavior.Propagates, function.NullBehavior);
            Assert.Equal(DbFunctionVolatility.Immutable, function.Volatility);
            Assert.True(function.IsDeterministic);
            Assert.False(function.SupportsBatch);
            Assert.True(function.AllowedInDefaults);
            Assert.True(function.AllowedInChecks);
            Assert.Equal("function-defined", function.CollationBehavior);
        }
    }

    [Fact]
    public async Task OrdinalStringSearchFunctions_UseLiteralCaseSensitiveSemantics()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await using var result = await db.ExecuteAsync(
            """
            SELECT ORDINAL_STARTS_WITH('Alpha', 'Al'),
                   ORDINAL_STARTS_WITH('Alpha', 'al'),
                   ORDINAL_ENDS_WITH('Alpha', 'pha'),
                   ORDINAL_ENDS_WITH('Alpha', 'PHA'),
                   ORDINAL_CONTAINS('Alpha', 'ph'),
                   ORDINAL_CONTAINS('Alpha', 'PH'),
                   ORDINAL_STARTS_WITH('Alpha', ''),
                   ORDINAL_ENDS_WITH('Alpha', ''),
                   ORDINAL_CONTAINS('Alpha', ''),
                   ORDINAL_STARTS_WITH('short', 'longer'),
                   ORDINAL_ENDS_WITH('short', 'longer'),
                   ORDINAL_CONTAINS('short', 'longer'),
                   ORDINAL_CONTAINS('a%_b', '%_'),
                   ORDINAL_CONTAINS('alphabet', '%_'),
                   ORDINAL_ENDS_WITH('folder\file', '\file'),
                   ORDINAL_STARTS_WITH(NULL, 'a'),
                   ORDINAL_STARTS_WITH('a', NULL),
                   ORDINAL_ENDS_WITH(NULL, 'a'),
                   ORDINAL_ENDS_WITH('a', NULL),
                   ORDINAL_CONTAINS(NULL, 'a'),
                   ORDINAL_CONTAINS('a', NULL),
                   ORDINAL_CONTAINS('A😀B', '😀'),
                   ORDINAL_CONTAINS('café', 'café')
            """,
            Ct);

        DbValue[] row = Assert.Single(await result.ToListAsync(Ct));
        Assert.Equal(
            [1L, 0L, 1L, 0L, 1L, 0L, 1L, 1L, 1L, 0L, 0L, 0L, 1L, 0L, 1L],
            row[..15].Select(static value => value.AsInteger).ToArray());
        Assert.All(row[15..21], static value => Assert.True(value.IsNull));
        Assert.Equal(1, row[21].AsInteger);
        Assert.Equal(0, row[22].AsInteger);
    }

    [Fact]
    public void OrdinalStringSearchFunctions_CompareUtf16CodeUnits()
    {
        Assert.True(DbBuiltInScalarFunctions.TryEvaluate(
            "ORDINAL_CONTAINS",
            [DbValue.FromText("😀"), DbValue.FromText("\uD83D")],
            out DbValue result));

        Assert.Equal(1, result.AsInteger);
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
