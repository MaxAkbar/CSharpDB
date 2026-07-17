using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class NumericRelationshipJoinPlannerTests : IAsyncLifetime
{
    private const int InsertBatchSize = 256;

    private readonly string _dbPath = Path.Combine(
        Path.GetTempPath(),
        $"csharpdb_numeric_relationship_planner_{Guid.NewGuid():N}.db");

    private Database _db = null!;

    public async ValueTask InitializeAsync()
    {
        _db = await Database.OpenAsync(_dbPath, TestContext.Current.CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await _db.DisposeAsync();
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal"))
            File.Delete(_dbPath + ".wal");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Force_BroadIntegerRelationship_UsesNumericRelationshipPlanForSqlAndForeignKeyIndexes(
        bool useForeignKeySupportIndex)
    {
        var ct = TestContext.Current.CancellationToken;
        await CreateIntegerRelationshipAsync(
            matchedParentCount: 8,
            fanout: 3,
            useForeignKeySupportIndex,
            addUnmatchedParent: false,
            ct);
        SetRelationshipJoinMode(NumericRelationshipJoinMode.Force);

        await using var result = await _db.ExecuteAsync(
            "SELECT p.id, c.parent_id, c.id " +
            "FROM relationship_parents p " +
            "INNER JOIN relationship_children c ON p.id = c.parent_id",
            ct);

        Assert.True(
            ContainsOperator<NumericRelationshipIndexJoinOperator>(GetRootOperator(result)),
            "A broad eligible numeric relationship join should use the forced numeric relationship plan.");

        DbValue[][] rows = (await result.ToListAsync(ct)).ToArray();
        Assert.Equal(24, rows.Length);
        Assert.All(rows, row => Assert.Equal(row[0].AsInteger, row[1].AsInteger));
        Assert.Equal(Enumerable.Range(1, 24).Select(static id => (long)id), rows.Select(row => row[2].AsInteger).Order());
    }

    [Fact]
    public async Task DefaultAuto_BroadCoveredProjectionAboveCostGate_UsesOptimizedPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await CreateIntegerRelationshipAsync(
            matchedParentCount: 256,
            fanout: 4,
            useForeignKeySupportIndex: true,
            addUnmatchedParent: false,
            ct);
        await using var result = await _db.ExecuteAsync(
            "SELECT p.id, c.parent_id, c.id " +
            "FROM relationship_parents p " +
            "INNER JOIN relationship_children c ON p.id = c.parent_id",
            ct);

        Assert.True(
            ContainsOperator<NumericRelationshipIndexJoinOperator>(GetRootOperator(result)),
            "The default Auto mode should select the optimized plan for an eligible broad relationship scan.");

        DbValue[][] rows = (await result.ToListAsync(ct)).ToArray();
        Assert.Equal(1_024, rows.Length);
        Assert.All(rows, row => Assert.Equal(row[0].AsInteger, row[1].AsInteger));
    }

    [Fact]
    public async Task Auto_BroadSqlIndexWithoutDeclaredForeignKey_RetainsExistingJoinPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await CreateIntegerRelationshipAsync(
            matchedParentCount: 256,
            fanout: 4,
            useForeignKeySupportIndex: false,
            addUnmatchedParent: false,
            ct);
        SetRelationshipJoinMode(NumericRelationshipJoinMode.Auto);

        await using var result = await _db.ExecuteAsync(
            "SELECT p.id, c.parent_id, c.id " +
            "FROM relationship_parents p " +
            "INNER JOIN relationship_children c ON p.id = c.parent_id",
            ct);

        Assert.False(
            ContainsOperator<NumericRelationshipIndexJoinOperator>(GetRootOperator(result)),
            "Auto mode requires a declared FK and must not infer a relationship from a user SQL index.");
        Assert.Equal(1_024, (await result.ToListAsync(ct)).Count);
    }

    [Fact]
    public async Task Auto_DeclaredRelationship_AfterUpsertEquivalentMutations_UsesMaintainedIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        await CreateIntegerRelationshipAsync(
            matchedParentCount: 256,
            fanout: 2,
            useForeignKeySupportIndex: true,
            addUnmatchedParent: false,
            ct);
        SetRelationshipJoinMode(NumericRelationshipJoinMode.Auto);

        const string joinSql =
            "SELECT p.id, c.parent_id, c.id " +
            "FROM relationship_parents p " +
            "INNER JOIN relationship_children c ON p.id = c.parent_id";

        await using (var initial = await _db.ExecuteAsync(joinSql, ct))
        {
            Assert.True(ContainsOperator<NumericRelationshipIndexJoinOperator>(GetRootOperator(initial)));
            Assert.Equal(512, (await initial.ToListAsync(ct)).Count);
        }

        // CSharpDB has no table-level UPSERT syntax. UPDATE-existing plus INSERT-new
        // exercises the two index-maintenance halves of an upsert, followed by DELETE.
        await _db.ExecuteAsync(
            "UPDATE relationship_children " +
            "SET parent_id = 256, amount = 999001 WHERE id = 1",
            ct);
        await _db.ExecuteAsync(
            "INSERT INTO relationship_children VALUES (513, 2, 999513)",
            ct);
        await _db.ExecuteAsync(
            "DELETE FROM relationship_children WHERE id = 2",
            ct);

        await using var result = await _db.ExecuteAsync(joinSql, ct);
        Assert.True(
            ContainsOperator<NumericRelationshipIndexJoinOperator>(GetRootOperator(result)),
            "Auto mode should continue using the declared FK index after maintained mutations.");

        DbValue[][] rows = (await result.ToListAsync(ct))
            .OrderBy(static row => row[2].AsInteger)
            .ToArray();
        Assert.Equal(512, rows.Length);
        Assert.DoesNotContain(rows, static row => row[2].AsInteger == 2);
        Assert.Equal(513, rows[^1][2].AsInteger);

        foreach (DbValue[] row in rows)
        {
            long childId = row[2].AsInteger;
            long expectedParentId = childId switch
            {
                1 => 256,
                513 => 2,
                _ => ((childId - 1) / 2) + 1,
            };
            Assert.Equal(expectedParentId, row[0].AsInteger);
            Assert.Equal(expectedParentId, row[1].AsInteger);
        }
    }

    [Fact]
    public async Task Force_NoncoveredPayloadProjection_UsesNumericRelationshipPlanForDiagnostics()
    {
        var ct = TestContext.Current.CancellationToken;
        await CreateIntegerRelationshipAsync(
            matchedParentCount: 8,
            fanout: 3,
            useForeignKeySupportIndex: true,
            addUnmatchedParent: false,
            ct);
        SetRelationshipJoinMode(NumericRelationshipJoinMode.Force);

        await using var result = await _db.ExecuteAsync(
            "SELECT p.label, c.amount " +
            "FROM relationship_parents p " +
            "INNER JOIN relationship_children c ON p.id = c.parent_id",
            ct);

        Assert.True(
            ContainsOperator<NumericRelationshipIndexJoinOperator>(GetRootOperator(result)),
            "Force mode should retain the noncovered numeric relationship path for internal comparison diagnostics.");

        DbValue[][] rows = (await result.ToListAsync(ct)).ToArray();
        Assert.Equal(24, rows.Length);
        Assert.Contains(rows, static row => row[0].AsText == "parent-8" && row[1].AsInteger == 8003);
    }

    [Fact]
    public async Task Auto_NoncoveredAndOpaqueProjectionShapes_RetainExistingJoinPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await CreateIntegerRelationshipAsync(
            matchedParentCount: 256,
            fanout: 4,
            useForeignKeySupportIndex: true,
            addUnmatchedParent: false,
            ct);
        SetRelationshipJoinMode(NumericRelationshipJoinMode.Auto);

        DbValue[][] payloadRows = await AssertAutoProjectionFallbackAsync(
            "SELECT p.label, c.amount " +
            "FROM relationship_parents p " +
            "INNER JOIN relationship_children c ON p.id = c.parent_id",
            expectedRowCount: 1_024,
            ct);
        Assert.Contains(
            payloadRows,
            static row => row[0].AsText == "parent-256" && row[1].AsInteger == 256004);

        DbValue[][] starRows = await AssertAutoProjectionFallbackAsync(
            "SELECT * FROM relationship_parents p " +
            "INNER JOIN relationship_children c ON p.id = c.parent_id",
            expectedRowCount: 1_024,
            ct);
        Assert.All(starRows, static row =>
        {
            Assert.Equal(5, row.Length);
            Assert.Equal(row[0].AsInteger, row[3].AsInteger);
        });

        DbValue[][] expressionRows = await AssertAutoProjectionFallbackAsync(
            "SELECT p.id + c.parent_id AS doubled_id " +
            "FROM relationship_parents p " +
            "INNER JOIN relationship_children c ON p.id = c.parent_id",
            expectedRowCount: 1_024,
            ct);
        Assert.Contains(expressionRows, static row => row[0].AsInteger == 512);

        DbValue[][] aggregateRows = await AssertAutoProjectionFallbackAsync(
            "SELECT COUNT(*) FROM relationship_parents p " +
            "INNER JOIN relationship_children c ON p.id = c.parent_id",
            expectedRowCount: 1,
            ct);
        Assert.Equal(1_024, aggregateRows[0][0].AsInteger);

        DbValue[][] orderedRows = await AssertAutoProjectionFallbackAsync(
            "SELECT p.id, c.id FROM relationship_parents p " +
            "INNER JOIN relationship_children c ON p.id = c.parent_id " +
            "ORDER BY c.id DESC",
            expectedRowCount: 1_024,
            ct);
        Assert.Equal(1_024, orderedRows[0][1].AsInteger);
        Assert.Equal(1, orderedRows[^1][1].AsInteger);
    }

    [Fact]
    public async Task Disabled_BroadEligibleRelationship_RetainsExistingJoinPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await CreateIntegerRelationshipAsync(
            matchedParentCount: 8,
            fanout: 3,
            useForeignKeySupportIndex: false,
            addUnmatchedParent: false,
            ct);
        SetRelationshipJoinMode(NumericRelationshipJoinMode.Disabled);

        await using var result = await _db.ExecuteAsync(
            "SELECT p.id, c.id " +
            "FROM relationship_parents p " +
            "INNER JOIN relationship_children c ON p.id = c.parent_id",
            ct);

        Assert.False(
            ContainsOperator<NumericRelationshipIndexJoinOperator>(GetRootOperator(result)),
            "Disabled mode must retain the existing join planning path.");
        Assert.Equal(24, (await result.ToListAsync(ct)).Count);
    }

    [Fact]
    public async Task Auto_BroadRelationshipBelowCostGate_RetainsExistingJoinPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await CreateIntegerRelationshipAsync(
            matchedParentCount: 8,
            fanout: 3,
            useForeignKeySupportIndex: true,
            addUnmatchedParent: false,
            ct);
        SetRelationshipJoinMode(NumericRelationshipJoinMode.Auto);

        await using var result = await _db.ExecuteAsync(
            "SELECT p.id, c.id " +
            "FROM relationship_parents p " +
            "INNER JOIN relationship_children c ON p.id = c.parent_id",
            ct);

        Assert.False(
            ContainsOperator<NumericRelationshipIndexJoinOperator>(GetRootOperator(result)),
            "Auto mode must retain the existing plan below the conservative broad-scan cost gate.");
        Assert.Equal(24, (await result.ToListAsync(ct)).Count);
    }

    [Fact]
    public async Task Force_DoesNotBypassPointFilterResidualOuterOrOrientationGates()
    {
        var ct = TestContext.Current.CancellationToken;
        await CreateIntegerRelationshipAsync(
            matchedParentCount: 8,
            fanout: 2,
            useForeignKeySupportIndex: false,
            addUnmatchedParent: true,
            ct);
        SetRelationshipJoinMode(NumericRelationshipJoinMode.Force);

        await AssertFallbackAsync(
            "SELECT p.id, c.id FROM relationship_parents p " +
            "INNER JOIN relationship_children c ON p.id = c.parent_id WHERE p.id = 2",
            expectedRowCount: 2,
            ct);

        await AssertFallbackAsync(
            "SELECT p.id, c.id FROM relationship_parents p " +
            "INNER JOIN relationship_children c ON p.id = c.parent_id WHERE p.id BETWEEN 2 AND 3",
            expectedRowCount: 4,
            ct);

        await AssertFallbackAsync(
            "SELECT p.id, c.id FROM relationship_parents p " +
            "INNER JOIN relationship_children c ON p.id = c.parent_id WHERE c.amount = 2001",
            expectedRowCount: 1,
            ct);

        await AssertFallbackAsync(
            "SELECT p.id, c.id FROM relationship_parents p " +
            "INNER JOIN relationship_children c ON p.id = c.parent_id AND c.amount > 0",
            expectedRowCount: 16,
            ct);

        await AssertFallbackAsync(
            "SELECT c.id, p.id FROM relationship_children c " +
            "INNER JOIN relationship_parents p ON c.parent_id = p.id",
            expectedRowCount: 16,
            ct);

        await AssertFallbackAsync(
            "SELECT p.id, c.id FROM relationship_parents p " +
            "INNER JOIN relationship_children c ON p.id = c.parent_id LIMIT 1",
            expectedRowCount: 1,
            ct);

        await AssertFallbackAsync(
            "SELECT p.id, c.id FROM relationship_parents p " +
            "INNER JOIN relationship_children c ON p.id = c.parent_id LIMIT 10000",
            expectedRowCount: 16,
            ct);

        await AssertFallbackAsync(
            "SELECT p.id, c.id FROM relationship_parents p " +
            "INNER JOIN relationship_children c ON p.id = c.parent_id OFFSET 1",
            expectedRowCount: 15,
            ct);

        DbValue[][] outerRows = await AssertFallbackAsync(
            "SELECT p.id, c.id FROM relationship_parents p " +
            "LEFT JOIN relationship_children c ON p.id = c.parent_id",
            expectedRowCount: 17,
            ct);
        Assert.Single(outerRows, static row => row[1].IsNull);
    }

    [Fact]
    public async Task Force_TextPrimaryKeyRelationship_RetainsExistingJoinPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE text_relationship_parents (code TEXT PRIMARY KEY, label TEXT NOT NULL)",
            ct);
        await _db.ExecuteAsync(
            "CREATE TABLE text_relationship_children " +
            "(id INTEGER PRIMARY KEY, parent_code TEXT NOT NULL, amount INTEGER NOT NULL)",
            ct);
        await _db.ExecuteAsync(
            "CREATE INDEX idx_text_relationship_children_parent " +
            "ON text_relationship_children(parent_code)",
            ct);
        await _db.ExecuteAsync(
            "INSERT INTO text_relationship_parents VALUES ('A', 'parent-a'), ('B', 'parent-b')",
            ct);
        await _db.ExecuteAsync(
            "INSERT INTO text_relationship_children VALUES " +
            "(1, 'A', 10), (2, 'A', 20), (3, 'B', 30)",
            ct);
        SetRelationshipJoinMode(NumericRelationshipJoinMode.Force);

        await using var result = await _db.ExecuteAsync(
            "SELECT p.code, c.parent_code FROM text_relationship_parents p " +
            "INNER JOIN text_relationship_children c ON p.code = c.parent_code",
            ct);

        Assert.False(
            ContainsOperator<NumericRelationshipIndexJoinOperator>(GetRootOperator(result)),
            "Force mode must not make a TEXT relationship eligible for the numeric operator.");

        DbValue[][] rows = (await result.ToListAsync(ct))
            .OrderBy(static row => row[0].AsText, StringComparer.Ordinal)
            .ThenBy(static row => row[1].AsText, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal(["A", "A", "B"], rows.Select(static row => row[0].AsText).ToArray());
        Assert.All(rows, row => Assert.Equal(row[0].AsText, row[1].AsText));
    }

    private async Task CreateIntegerRelationshipAsync(
        int matchedParentCount,
        int fanout,
        bool useForeignKeySupportIndex,
        bool addUnmatchedParent,
        CancellationToken ct)
    {
        await _db.ExecuteAsync(
            "CREATE TABLE relationship_parents " +
            "(id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
            ct);

        string parentColumn = useForeignKeySupportIndex
            ? "parent_id INTEGER NOT NULL REFERENCES relationship_parents(id)"
            : "parent_id INTEGER NOT NULL";
        await _db.ExecuteAsync(
            $"CREATE TABLE relationship_children " +
            $"(id INTEGER PRIMARY KEY, {parentColumn}, amount INTEGER NOT NULL)",
            ct);

        if (!useForeignKeySupportIndex)
        {
            await _db.ExecuteAsync(
                "CREATE INDEX idx_relationship_children_parent " +
                "ON relationship_children(parent_id)",
                ct);
        }

        int totalParentCount = matchedParentCount + (addUnmatchedParent ? 1 : 0);
        var parentValues = new List<string>(totalParentCount);
        for (int parentId = 1; parentId <= totalParentCount; parentId++)
            parentValues.Add($"({parentId}, 'parent-{parentId}')");

        var childValues = new List<string>(checked(matchedParentCount * fanout));
        int childId = 1;
        for (int parentId = 1; parentId <= matchedParentCount; parentId++)
        {
            for (int ordinal = 1; ordinal <= fanout; ordinal++)
            {
                childValues.Add($"({childId++}, {parentId}, {parentId * 1000L + ordinal})");
            }
        }

        await _db.BeginTransactionAsync(ct);
        try
        {
            await InsertValuesAsync("relationship_parents", parentValues, ct);
            await InsertValuesAsync("relationship_children", childValues, ct);
            await _db.CommitAsync(ct);
        }
        catch
        {
            await _db.RollbackAsync(CancellationToken.None);
            throw;
        }

        await _db.ExecuteAsync("ANALYZE relationship_parents", ct);
        await _db.ExecuteAsync("ANALYZE relationship_children", ct);
    }

    private async Task InsertValuesAsync(string tableName, IReadOnlyList<string> values, CancellationToken ct)
    {
        for (int start = 0; start < values.Count; start += InsertBatchSize)
        {
            int count = Math.Min(InsertBatchSize, values.Count - start);
            await _db.ExecuteAsync(
                $"INSERT INTO {tableName} VALUES {string.Join(',', values.Skip(start).Take(count))}",
                ct);
        }
    }

    private async Task<DbValue[][]> AssertFallbackAsync(
        string sql,
        int expectedRowCount,
        CancellationToken ct)
    {
        await using var result = await _db.ExecuteAsync(sql, ct);
        Assert.False(
            ContainsOperator<NumericRelationshipIndexJoinOperator>(GetRootOperator(result)),
            $"Force mode incorrectly selected the numeric relationship operator for: {sql}");

        DbValue[][] rows = (await result.ToListAsync(ct)).ToArray();
        Assert.Equal(expectedRowCount, rows.Length);
        return rows;
    }

    private async Task<DbValue[][]> AssertAutoProjectionFallbackAsync(
        string sql,
        int expectedRowCount,
        CancellationToken ct)
    {
        await using var result = await _db.ExecuteAsync(sql, ct);
        DbValue[][] rows = (await result.ToListAsync(ct)).ToArray();

        Assert.False(
            ContainsOperator<NumericRelationshipIndexJoinOperator>(GetRootOperator(result)),
            $"Auto mode incorrectly selected the numeric relationship operator for: {sql}");
        Assert.Equal(expectedRowCount, rows.Length);
        return rows;
    }

    private void SetRelationshipJoinMode(NumericRelationshipJoinMode mode)
    {
        var plannerField = typeof(Database).GetField("_planner", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Database planner field not found.");
        var planner = (QueryPlanner?)plannerField.GetValue(_db)
            ?? throw new InvalidOperationException("Database planner was not initialized.");
        planner.RelationshipJoinMode = mode;
    }

    private static IOperator GetRootOperator(QueryResult result)
    {
        var operatorField = typeof(QueryResult).GetField("_operator", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("QueryResult operator field not found.");
        var storedOperator = (IOperator?)operatorField.GetValue(result);

        if (storedOperator == null)
        {
            var batchOperatorField = typeof(QueryResult).GetField("_batchOperator", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("QueryResult batch operator field not found.");
            storedOperator = (IOperator?)batchOperatorField.GetValue(result)
                ?? throw new InvalidOperationException("QueryResult did not contain an operator.");
        }

        return storedOperator is BatchToRowOperatorAdapter batchAdapter
            ? batchAdapter.BatchSource as IOperator
                ?? throw new InvalidOperationException("Batch adapter did not expose an operator root.")
            : storedOperator;
    }

    private static bool ContainsOperator<TOperator>(IOperator root)
        where TOperator : class, IOperator
    {
        for (IOperator? current = root;
             current != null;
             current = current is IUnaryOperatorSource unary ? unary.Source : null)
        {
            if (current is TOperator)
                return true;
        }

        return false;
    }
}
