using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Sql;
using System.Reflection;

namespace CSharpDB.Tests;

public sealed class MutationPlannerTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Delete_IndexedEqualityPredicate_CollectsTargetsFromIndex()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await SetupMutationItemsAsync(db);
        AssertCanBuildIndexedMutationSource(
            db,
            "DELETE FROM mutation_items WHERE code = 42",
            expectedOperatorType: typeof(IndexScanOperator));

        db.ResetMutationTargetCollectionDiagnostics();

        var deleteResult = await db.ExecuteAsync(
            "DELETE FROM mutation_items WHERE code = 42",
            Ct);

        Assert.Equal(1, deleteResult.RowsAffected);
        var diagnostics = db.GetMutationTargetCollectionDiagnosticsSnapshot();
        Assert.Equal(1, diagnostics.IndexedCollectionCount);
        Assert.Equal(0, diagnostics.ScannedCollectionCount);

        await using var deletedRows = await db.ExecuteAsync(
            "SELECT id FROM mutation_items WHERE code = 42",
            Ct);
        Assert.Empty(await deletedRows.ToListAsync(Ct));

        await using var neighborRows = await db.ExecuteAsync(
            "SELECT id FROM mutation_items WHERE code = 41",
            Ct);
        var rows = await neighborRows.ToListAsync(Ct);
        Assert.Equal(41L, Assert.Single(rows)[0].AsInteger);
    }

    [Fact]
    public async Task Update_IndexedRangePredicate_CollectsTargetsFromIndexAndMaintainsIndex()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await SetupMutationItemsAsync(db);
        AssertCanBuildIndexedMutationSource(
            db,
            "UPDATE mutation_items SET code = code + 1000 WHERE code BETWEEN 40 AND 42",
            expectedOperatorType: typeof(IndexOrderedScanOperator));

        db.ResetMutationTargetCollectionDiagnostics();

        var updateResult = await db.ExecuteAsync(
            "UPDATE mutation_items SET code = code + 1000 WHERE code BETWEEN 40 AND 42",
            Ct);

        Assert.Equal(3, updateResult.RowsAffected);
        var diagnostics = db.GetMutationTargetCollectionDiagnosticsSnapshot();
        Assert.Equal(1, diagnostics.IndexedCollectionCount);
        Assert.Equal(0, diagnostics.ScannedCollectionCount);

        await using var oldRangeRows = await db.ExecuteAsync(
            "SELECT id FROM mutation_items WHERE code BETWEEN 40 AND 42 ORDER BY id",
            Ct);
        Assert.Empty(await oldRangeRows.ToListAsync(Ct));

        await using var newRangeRows = await db.ExecuteAsync(
            "SELECT id, code FROM mutation_items WHERE code BETWEEN 1040 AND 1042 ORDER BY id",
            Ct);
        var rows = await newRangeRows.ToListAsync(Ct);

        Assert.Equal([40L, 41L, 42L], rows.Select(static row => row[0].AsInteger).ToArray());
        Assert.Equal([1040L, 1041L, 1042L], rows.Select(static row => row[1].AsInteger).ToArray());
    }

    private static async ValueTask SetupMutationItemsAsync(Database db)
    {
        await db.ExecuteAsync(
            "CREATE TABLE mutation_items (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, payload TEXT)",
            Ct);
        await db.ExecuteAsync(
            "CREATE INDEX idx_mutation_items_code ON mutation_items(code)",
            Ct);

        await db.BeginTransactionAsync(Ct);
        for (int i = 1; i <= 100; i++)
        {
            await db.ExecuteAsync(
                $"INSERT INTO mutation_items VALUES ({i}, {i}, 'payload-{i}')",
                Ct);
        }
        await db.CommitAsync(Ct);
    }

    private static void AssertCanBuildIndexedMutationSource(Database db, string sql, Type expectedOperatorType)
    {
        var plannerField = typeof(Database).GetField("_planner", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(plannerField);
        object? planner = plannerField.GetValue(db);
        Assert.NotNull(planner);

        Expression? where = Parser.Parse(sql) switch
        {
            DeleteStatement delete => delete.Where,
            UpdateStatement update => update.Where,
            _ => throw new InvalidOperationException("Expected a mutation statement."),
        };
        Assert.NotNull(where);

        var schema = db.GetTableSchema("mutation_items");
        Assert.NotNull(schema);

        foreach (string methodName in new[] { "TryBuildIndexScan", "TryBuildIntegerIndexRangeScan", "TryBuildOrderedTextIndexRangeScan" })
        {
            var method = planner.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(method);
            object?[] args = ["mutation_items", where, schema, null];
            object? op = method.Invoke(planner, args);
            if (op != null)
            {
                Assert.IsType(expectedOperatorType, op);
                return;
            }
        }

        Assert.Fail($"No indexed mutation source was built for '{sql}'.");
    }
}
