using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class PhysicalExplainTests
{
    private static readonly string[] PhysicalPlanColumnNames =
    [
        "node_id",
        "parent_node_id",
        "operator_type",
        "estimated_rows",
        "estimated_cost",
        "actual_rows",
        "actual_loops",
        "elapsed_microseconds",
        "access_path",
        "object_name",
        "index_name",
        "join_type",
        "predicate",
        "status",
        "diagnostic_code",
    ];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ExplainEstimate_LegacyContract_RemainsAvailable()
    {
        await using Database db = await OpenPlanDatabaseAsync();

        CapturedPlan plan = await ExecutePlanAsync(
            db,
            "EXPLAIN ESTIMATE FOR SELECT * FROM plan_items WHERE code = 20");

        Assert.Equal(
            [
                "node_id",
                "parent_node_id",
                "node_kind",
                "target",
                "decision",
                "estimated_rows",
                "estimated_cost",
                "stats_source",
                "stats_state",
                "detail",
            ],
            plan.ColumnNames);
        Assert.Contains(
            plan.Rows,
            row =>
                plan.Text(row, "node_kind") == "statement" &&
                plan.Text(row, "decision") == "diagnostic-only" &&
                plan.Text(row, "stats_state") == "not_executed");
        Assert.Contains(
            plan.Rows,
            row => plan.Text(row, "node_kind") is "filter" or "access_path");
    }

    [Fact]
    public async Task Explain_ReturnsStructuralSchemaAndSelectedAccessOperators()
    {
        await using Database db = await OpenPlanDatabaseAsync();

        CapturedPlan primaryKeyPlan = await ExecutePlanAsync(
            db,
            "EXPLAIN SELECT payload FROM plan_items WHERE id = 2");
        Assert.Equal(PhysicalPlanColumnNames, primaryKeyPlan.ColumnNames);
        AssertTreeShape(primaryKeyPlan);
        DbValue[] primaryKey = AssertOperator(primaryKeyPlan, "primary_key_lookup");
        Assert.Equal("primary_key", primaryKeyPlan.Text(primaryKey, "access_path"));
        Assert.Equal(1, primaryKeyPlan.Integer(primaryKey, "estimated_rows"));
        Assert.True(primaryKeyPlan.Real(primaryKey, "estimated_cost") > 0d);

        CapturedPlan indexPlan = await ExecutePlanAsync(
            db,
            "EXPLAIN SELECT payload FROM plan_items WHERE code = 20");
        AssertTreeShape(indexPlan);
        DbValue[] index = AssertOperator(
            indexPlan,
            "index_lookup",
            "index_projection",
            "index_ordered_scan");
        Assert.Contains(
            indexPlan.Text(index, "access_path"),
            new[] { "index", "unique_index", "ordered_index" });

        CapturedPlan sortPlan = await ExecutePlanAsync(
            db,
            "EXPLAIN SELECT id, payload FROM plan_items ORDER BY payload DESC");
        AssertTreeShape(sortPlan);
        _ = AssertOperator(sortPlan, "sort", "top_n_sort");
        Assert.Contains(
            sortPlan.Rows,
            row =>
                sortPlan.Text(row, "operator_type") is
                    "table_scan" or "compact_table_scan" &&
                sortPlan.Integer(row, "estimated_rows") == 3 &&
                sortPlan.Real(row, "estimated_cost") > 0d);

        CapturedPlan joinPlan = await ExecutePlanAsync(
            db,
            """
            EXPLAIN
            SELECT p.id, g.name
            FROM plan_items AS p
            JOIN plan_groups AS g ON p.group_id = g.id
            """);
        AssertTreeShape(joinPlan);
        DbValue[] joinOperator = AssertOperator(
            joinPlan,
            "hash_join",
            "adaptive_hash_join",
            "index_nested_loop_join",
            "adaptive_index_nested_loop_join",
            "nested_loop_join");
        string joinPredicate = Assert.IsType<string>(
            joinPlan.Text(joinOperator, "predicate"));
        Assert.All(
            new[] { "p", "group_id", "g", "id" },
            token => Assert.Contains(
                token,
                joinPredicate,
                StringComparison.OrdinalIgnoreCase));
        Assert.True(joinPlan.Real(joinOperator, "estimated_cost") > 0d);
        Assert.Contains(
            joinPlan.Rows,
            row =>
                joinPlan.Text(row, "operator_type") == "query" &&
                joinPlan.Text(row, "predicate") is { } predicate &&
                predicate.Contains("JOIN ON", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Explain_AttachesRedactedPredicateToApplyingAccessOperator()
    {
        await using Database db = await OpenPlanDatabaseAsync();

        CapturedPlan primaryKeyPlan = await ExecutePlanAsync(
            db,
            "EXPLAIN SELECT payload FROM plan_items WHERE id = 2");
        AssertPredicateTokens(
            primaryKeyPlan,
            AssertOperator(primaryKeyPlan, "primary_key_lookup"),
            "id",
            "=",
            "?");

        CapturedPlan indexPlan = await ExecutePlanAsync(
            db,
            "EXPLAIN SELECT payload FROM plan_items WHERE code = 20");
        AssertPredicateTokens(
            indexPlan,
            AssertOperator(
                indexPlan,
                "index_lookup",
                "index_projection",
                "index_ordered_scan"),
            "code",
            "=",
            "?");

        CapturedPlan scanPlan = await ExecutePlanAsync(
            db,
            "EXPLAIN SELECT * FROM plan_items WHERE payload = 'alpha'");
        AssertPredicateTokens(
            scanPlan,
            AssertOperator(scanPlan, "table_scan", "compact_table_scan"),
            "payload",
            "=",
            "?");
    }

    [Fact]
    public async Task Explain_EmptyTableScanReportsExactZeroEstimate()
    {
        await using Database db = await Database.OpenInMemoryAsync(Ct);
        await ExecuteNonQueryAsync(
            db,
            "CREATE TABLE empty_plan_items (id INTEGER PRIMARY KEY, payload TEXT)");

        CapturedPlan plan = await ExecutePlanAsync(
            db,
            "EXPLAIN SELECT id, payload FROM empty_plan_items");

        DbValue[] scan = AssertOperator(
            plan,
            "table_scan",
            "compact_table_scan");
        Assert.Equal(0, plan.Integer(scan, "estimated_rows"));
    }

    [Fact]
    public async Task Explain_DoesNotPublishCapacityHintsAsCardinalityEstimates()
    {
        await using Database db = await OpenPlanDatabaseAsync();

        CapturedPlan indexPlan = await ExecutePlanAsync(
            db,
            "EXPLAIN SELECT * FROM plan_items WHERE code = 20");
        DbValue[] index = AssertOperator(
            indexPlan,
            "index_lookup",
            "index_projection",
            "index_ordered_scan");
        Assert.Null(indexPlan.Integer(index, "estimated_rows"));

        CapturedPlan joinPlan = await ExecutePlanAsync(
            db,
            """
            EXPLAIN
            SELECT p.id, p.code, p.group_id, p.payload, g.name
            FROM plan_items AS p
            JOIN plan_groups AS g ON p.group_id = g.id
            """);
        DbValue[] join = AssertOperator(
            joinPlan,
            "hash_join",
            "adaptive_hash_join",
            "index_nested_loop_join",
            "adaptive_index_nested_loop_join",
            "nested_loop_join");
        Assert.Null(joinPlan.Integer(join, "estimated_rows"));
    }

    [Fact]
    public async Task ExplainAnalyze_ReportsEstimateAndActualsForSelectedLookup()
    {
        await using Database db = await OpenPlanDatabaseAsync();

        CapturedPlan plan = await ExecutePlanAsync(
            db,
            "EXPLAIN ANALYZE SELECT payload FROM plan_items WHERE id = 2");

        DbValue[] lookup = AssertOperator(plan, "primary_key_lookup");
        Assert.Equal(1, plan.Integer(lookup, "estimated_rows"));
        Assert.Equal(1, plan.Integer(lookup, "actual_rows"));
        Assert.Equal(1, plan.Integer(lookup, "actual_loops"));
        Assert.True(plan.Integer(lookup, "elapsed_microseconds") >= 0);
        Assert.Equal("completed", plan.Text(lookup, "status"));

        DbValue[] statement = AssertOperator(plan, "query");
        Assert.Equal(1, plan.Integer(statement, "actual_rows"));
        Assert.Equal(1, plan.Integer(statement, "actual_loops"));
        Assert.True(plan.Integer(statement, "elapsed_microseconds") >= 0);
        Assert.Equal("completed", plan.Text(statement, "status"));
    }

    [Fact]
    public async Task ExplainAnalyze_ReportsActualsForEveryPipelineNode()
    {
        await using Database db = await OpenPlanDatabaseAsync();

        CapturedPlan plan = await ExecutePlanAsync(
            db,
            """
            EXPLAIN ANALYZE
            SELECT *
            FROM plan_items
            WHERE (code + 0) > 0
            ORDER BY payload DESC
            """);

        _ = AssertOperator(plan, "table_scan", "compact_table_scan");
        DbValue[] filter = AssertOperator(plan, "filter");
        _ = AssertOperator(plan, "sort", "top_n_sort");
        string filterPredicate = Assert.IsType<string>(
            plan.Text(filter, "predicate"));
        Assert.All(
            new[] { "code", ">", "?" },
            token => Assert.Contains(
                token,
                filterPredicate,
                StringComparison.OrdinalIgnoreCase));
        Assert.True(plan.Real(filter, "estimated_cost") > 0d);

        DbValue[][] executionNodes = plan.Rows
            .Where(row => plan.Text(row, "operator_type") is not ("query" or "diagnostic"))
            .ToArray();
        Assert.True(executionNodes.Length >= 3);
        Assert.All(
            executionNodes,
            row =>
            {
                Assert.NotNull(plan.Integer(row, "actual_rows"));
                Assert.True(plan.Integer(row, "actual_loops") >= 1);
                Assert.True(plan.Integer(row, "elapsed_microseconds") >= 0);
                Assert.Equal("completed", plan.Text(row, "status"));
            });
    }

    [Fact]
    public async Task Explain_CountFastPathStillProducesAPhysicalOperator()
    {
        await using Database db = await OpenPlanDatabaseAsync();

        CapturedPlan planned = await ExecutePlanAsync(
            db,
            "EXPLAIN SELECT COUNT(*) FROM plan_items");
        DbValue[] plannedCount = AssertOperator(
            planned,
            "scalar_aggregate",
            "index_aggregate");
        Assert.Null(planned.Integer(plannedCount, "actual_rows"));

        CapturedPlan analyzed = await ExecutePlanAsync(
            db,
            "EXPLAIN ANALYZE SELECT COUNT(*) FROM plan_items");
        DbValue[] analyzedCount = AssertOperator(
            analyzed,
            "scalar_aggregate",
            "index_aggregate");
        Assert.Equal(1, analyzed.Integer(analyzedCount, "actual_rows"));
        Assert.Equal(1, analyzed.Integer(analyzedCount, "actual_loops"));
        Assert.Equal("completed", analyzed.Text(analyzedCount, "status"));
    }

    [Fact]
    public async Task PlainExplain_DoesNotInvokeRegisteredScalarFunction()
    {
        int invocationCount = 0;
        var options = new DatabaseOptions().ConfigureFunctions(functions =>
            functions.AddScalar(
                "Phase7Bump",
                1,
                new DbScalarFunctionOptions(DbType.Integer),
                (_, arguments) =>
                {
                    Interlocked.Increment(ref invocationCount);
                    return arguments[0];
                }));

        await using Database db = await Database.OpenInMemoryAsync(options, Ct);
        await ExecuteNonQueryAsync(
            db,
            "CREATE TABLE side_effect_source (id INTEGER PRIMARY KEY)");
        await ExecuteNonQueryAsync(db, "INSERT INTO side_effect_source VALUES (1)");

        CapturedPlan plan = await ExecutePlanAsync(
            db,
            "EXPLAIN SELECT Phase7Bump(id) AS bumped FROM side_effect_source");

        Assert.NotEmpty(plan.Rows);
        Assert.Equal(0, Volatile.Read(ref invocationCount));

        await using QueryResult result = await db.ExecuteAsync(
            "SELECT Phase7Bump(id) FROM side_effect_source",
            Ct);
        Assert.Single(await result.ToListAsync(Ct));
        Assert.Equal(1, Volatile.Read(ref invocationCount));
    }

    [Fact]
    public async Task PlainExplain_InsertBindsValuesWithoutInvokingRegisteredFunction()
    {
        int invocationCount = 0;
        var options = new DatabaseOptions().ConfigureFunctions(functions =>
            functions.AddScalar(
                "Phase7InsertValue",
                1,
                new DbScalarFunctionOptions(DbType.Integer),
                (_, arguments) =>
                {
                    Interlocked.Increment(ref invocationCount);
                    return arguments[0];
                }));

        await using Database db = await Database.OpenInMemoryAsync(options, Ct);
        await ExecuteNonQueryAsync(
            db,
            "CREATE TABLE phase7_insert_source (id INTEGER PRIMARY KEY)");
        await ExecuteNonQueryAsync(
            db,
            "INSERT INTO phase7_insert_source VALUES (1)");

        CapturedPlan plan = await ExecutePlanAsync(
            db,
            """
            EXPLAIN
            INSERT INTO phase7_insert_source VALUES (Phase7InsertValue(2))
            """);

        _ = AssertOperator(plan, "insert");
        Assert.Equal(0, Volatile.Read(ref invocationCount));
        Assert.Equal(
            1,
            await ExecuteScalarIntegerAsync(
                db,
                "SELECT COUNT(*) FROM phase7_insert_source"));

        await ExecuteNonQueryAsync(
            db,
            "INSERT INTO phase7_insert_source VALUES (Phase7InsertValue(2))");
        Assert.Equal(1, Volatile.Read(ref invocationCount));
        Assert.Equal(
            2,
            await ExecuteScalarIntegerAsync(
                db,
                "SELECT COUNT(*) FROM phase7_insert_source"));
    }

    [Fact]
    public async Task ConcurrentExplainAnalyze_DoesNotMutateSyncLookupPreference()
    {
        using var enteredA = new ManualResetEventSlim();
        using var enteredB = new ManualResetEventSlim();
        using var releaseA = new ManualResetEventSlim();
        using var releaseB = new ManualResetEventSlim();
        CancellationToken testCancellation = Ct;

        static DbValue Block(
            ManualResetEventSlim entered,
            ManualResetEventSlim release,
            CancellationToken cancellationToken,
            ReadOnlySpan<DbValue> arguments)
        {
            entered.Set();
            release.Wait(cancellationToken);
            return arguments[0];
        }

        var options = new DatabaseOptions().ConfigureFunctions(functions =>
        {
            functions.AddScalar(
                "Phase7BlockA",
                1,
                new DbScalarFunctionOptions(
                    DbType.Integer,
                    IsDeterministic: false),
                (_, arguments) =>
                    Block(
                        enteredA,
                        releaseA,
                        testCancellation,
                        arguments));
            functions.AddScalar(
                "Phase7BlockB",
                1,
                new DbScalarFunctionOptions(
                    DbType.Integer,
                    IsDeterministic: false),
                (_, arguments) =>
                    Block(
                        enteredB,
                        releaseB,
                        testCancellation,
                        arguments));
        });

        await using Database db = await Database.OpenInMemoryAsync(options, Ct);
        await ExecuteNonQueryAsync(
            db,
            "CREATE TABLE phase7_concurrent (id INTEGER PRIMARY KEY)");
        await ExecuteNonQueryAsync(
            db,
            "INSERT INTO phase7_concurrent VALUES (1)");
        Assert.True(db.PreferSyncPointLookups);

        Task<CapturedPlan> planTaskA = Task.Run(
            () => ExecutePlanAsync(
                db,
                """
                EXPLAIN ANALYZE
                SELECT Phase7BlockA(id)
                FROM phase7_concurrent
                WHERE id = 1
                """),
            testCancellation);
        Task<CapturedPlan>? planTaskB = null;

        try
        {
            Assert.True(enteredA.Wait(TimeSpan.FromSeconds(10), Ct));
            planTaskB = Task.Run(
                () => ExecutePlanAsync(
                    db,
                    """
                    EXPLAIN ANALYZE
                    SELECT Phase7BlockB(id)
                    FROM phase7_concurrent
                    WHERE id = 1
                    """),
                testCancellation);

            Assert.True(enteredB.Wait(TimeSpan.FromSeconds(10), Ct));
            Assert.True(db.PreferSyncPointLookups);

            releaseA.Set();
            CapturedPlan planA = await planTaskA;
            Assert.True(db.PreferSyncPointLookups);

            releaseB.Set();
            CapturedPlan planB = await planTaskB;
            Assert.True(db.PreferSyncPointLookups);

            _ = AssertOperator(planA, "primary_key_lookup");
            _ = AssertOperator(planB, "primary_key_lookup");
        }
        finally
        {
            releaseA.Set();
            releaseB.Set();
            await IgnoreFailureAsync(planTaskA);
            if (planTaskB is not null)
                await IgnoreFailureAsync(planTaskB);
        }
    }

    [Fact]
    public async Task Explain_RedactsLiteralValuesFromPredicateMetadata()
    {
        await using Database db = await OpenPlanDatabaseAsync();
        const string secret = "phase7-secret-literal-7eaf";

        CapturedPlan plan = await ExecutePlanAsync(
            db,
            $"EXPLAIN SELECT * FROM plan_items WHERE payload = '{secret}'");

        Assert.DoesNotContain(
            plan.TextValues,
            value => value.Contains(secret, StringComparison.Ordinal));
        Assert.Contains(
            plan.Rows,
            row =>
                plan.Text(row, "predicate") is { } predicate &&
                predicate.Contains('?'));
    }

    [Fact]
    public async Task ExplainAnalyze_MutationsExecuteExactlyOnceAndRollback()
    {
        await using Database db = await OpenPlanDatabaseAsync();

        await db.BeginTransactionAsync(Ct);
        try
        {
            CapturedPlan insert = await ExecutePlanAsync(
                db,
                "EXPLAIN ANALYZE INSERT INTO plan_items VALUES (4, 40, 2, 'delta')");
            AssertMutationSummary(insert, "insert");
            Assert.Equal(1, await ExecuteScalarIntegerAsync(
                db,
                "SELECT COUNT(*) FROM plan_items WHERE id = 4"));
        }
        finally
        {
            await db.RollbackAsync(CancellationToken.None);
        }

        Assert.Equal(0, await ExecuteScalarIntegerAsync(
            db,
            "SELECT COUNT(*) FROM plan_items WHERE id = 4"));

        await db.BeginTransactionAsync(Ct);
        try
        {
            CapturedPlan update = await ExecutePlanAsync(
                db,
                "EXPLAIN ANALYZE UPDATE plan_items SET code = code + 1 WHERE id = 1");
            AssertMutationSummary(update, "update");
            Assert.Equal(11, await ExecuteScalarIntegerAsync(
                db,
                "SELECT code FROM plan_items WHERE id = 1"));
        }
        finally
        {
            await db.RollbackAsync(CancellationToken.None);
        }

        Assert.Equal(10, await ExecuteScalarIntegerAsync(
            db,
            "SELECT code FROM plan_items WHERE id = 1"));

        await db.BeginTransactionAsync(Ct);
        try
        {
            CapturedPlan delete = await ExecutePlanAsync(
                db,
                "EXPLAIN ANALYZE DELETE FROM plan_items WHERE id = 2");
            AssertMutationSummary(delete, "delete");
            Assert.Equal(0, await ExecuteScalarIntegerAsync(
                db,
                "SELECT COUNT(*) FROM plan_items WHERE id = 2"));
        }
        finally
        {
            await db.RollbackAsync(CancellationToken.None);
        }

        Assert.Equal(1, await ExecuteScalarIntegerAsync(
            db,
            "SELECT COUNT(*) FROM plan_items WHERE id = 2"));

        CapturedPlan committedInsert = await ExecutePlanAsync(
            db,
            "EXPLAIN ANALYZE INSERT INTO plan_items VALUES (5, 50, 2, 'epsilon')");
        AssertMutationSummary(committedInsert, "insert");
        Assert.Equal(1, await ExecuteScalarIntegerAsync(
            db,
            "SELECT COUNT(*) FROM plan_items WHERE id = 5"));
    }

    [Fact]
    public async Task ExplainAnalyze_FailedMutationKeepsStatementRollbackSemantics()
    {
        await using Database db = await OpenPlanDatabaseAsync();

        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            async () =>
            {
                await using QueryResult result = await db.ExecuteAsync(
                    """
                    EXPLAIN ANALYZE
                    INSERT INTO plan_items VALUES
                        (6, 60, 2, 'zeta'),
                        (1, 70, 2, 'duplicate')
                    """,
                    Ct);
                _ = await result.ToListAsync(Ct);
            });

        Assert.Equal(ErrorCode.DuplicateKey, error.Code);
        Assert.Equal(0, await ExecuteScalarIntegerAsync(
            db,
            "SELECT COUNT(*) FROM plan_items WHERE id = 6"));
    }

    [Theory]
    [InlineData(
        "EXPLAIN SELECT missing_column FROM plan_items",
        ErrorCode.ColumnNotFound)]
    [InlineData(
        "EXPLAIN SELECT * FROM missing_table",
        ErrorCode.TableNotFound)]
    [InlineData(
        "EXPLAIN INSERT INTO plan_items (missing_column) VALUES (1)",
        ErrorCode.ColumnNotFound)]
    [InlineData(
        "EXPLAIN INSERT INTO plan_items VALUES (Phase7Missing(1), 1, 1, 'x')",
        ErrorCode.Unknown)]
    [InlineData(
        "EXPLAIN INSERT INTO plan_items VALUES (missing_value_column, 1, 1, 'x')",
        ErrorCode.ColumnNotFound)]
    [InlineData(
        "EXPLAIN INSERT INTO plan_items VALUES (7, 70, 2, 'ok'), (-'bad', 80, 2, 'invalid')",
        ErrorCode.TypeMismatch)]
    [InlineData(
        "EXPLAIN UPDATE plan_items SET missing_column = 1 WHERE id = 1",
        ErrorCode.ColumnNotFound)]
    [InlineData(
        "EXPLAIN DELETE FROM plan_items WHERE missing_column = 1",
        ErrorCode.ColumnNotFound)]
    public async Task Explain_InvalidTargetFailsWithStableSemanticDiagnostic(
        string sql,
        ErrorCode expectedCode)
    {
        await using Database db = await OpenPlanDatabaseAsync();

        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            async () =>
            {
                await using QueryResult result = await db.ExecuteAsync(sql, Ct);
                _ = await result.ToListAsync(Ct);
            });

        Assert.Equal(expectedCode, error.Code);
    }

    private static async Task<Database> OpenPlanDatabaseAsync()
    {
        Database db = await Database.OpenInMemoryAsync(Ct);
        try
        {
            await ExecuteNonQueryAsync(
                db,
                "CREATE TABLE plan_groups (id INTEGER PRIMARY KEY, name TEXT)");
            await ExecuteNonQueryAsync(
                db,
                """
                CREATE TABLE plan_items (
                    id INTEGER PRIMARY KEY,
                    code INTEGER,
                    group_id INTEGER,
                    payload TEXT
                )
                """);
            await ExecuteNonQueryAsync(
                db,
                "CREATE INDEX ix_plan_items_code ON plan_items(code)");
            await ExecuteNonQueryAsync(
                db,
                "INSERT INTO plan_groups VALUES (1, 'first'), (2, 'second')");
            await ExecuteNonQueryAsync(
                db,
                """
                INSERT INTO plan_items VALUES
                    (1, 10, 1, 'alpha'),
                    (2, 20, 1, 'beta'),
                    (3, 30, 2, 'gamma')
                """);
            return db;
        }
        catch
        {
            await db.DisposeAsync();
            throw;
        }
    }

    private static async Task ExecuteNonQueryAsync(Database db, string sql)
    {
        await using QueryResult result = await db.ExecuteAsync(sql, Ct);
        Assert.False(result.IsQuery);
    }

    private static async Task IgnoreFailureAsync(Task task)
    {
        try
        {
            await task;
        }
        catch
        {
            // The primary assertion reports the failure. Cleanup only ensures
            // no blocked explain task escapes the test.
        }
    }

    private static async Task<long> ExecuteScalarIntegerAsync(Database db, string sql)
    {
        await using QueryResult result = await db.ExecuteAsync(sql, Ct);
        DbValue[] row = Assert.Single(await result.ToListAsync(Ct));
        return row[0].AsInteger;
    }

    private static async Task<CapturedPlan> ExecutePlanAsync(Database db, string sql)
    {
        await using QueryResult result = await db.ExecuteAsync(sql, Ct);
        Assert.True(result.IsQuery);
        ColumnDefinition[] schema = result.Schema.ToArray();
        List<DbValue[]> rows = await result.ToListAsync(Ct);
        return new CapturedPlan(schema, rows);
    }

    private static void AssertTreeShape(CapturedPlan plan)
    {
        Assert.NotEmpty(plan.Rows);
        long[] nodeIds = plan.Rows
            .Select(row => plan.Integer(row, "node_id")!.Value)
            .ToArray();
        Assert.Equal(nodeIds.Length, nodeIds.Distinct().Count());

        DbValue[] root = Assert.Single(
            plan.Rows,
            row => plan.Integer(row, "parent_node_id") is null);
        Assert.Equal("query", plan.Text(root, "operator_type"));

        HashSet<long> knownIds = nodeIds.ToHashSet();
        Assert.All(
            plan.Rows,
            row =>
            {
                long? parentNodeId = plan.Integer(row, "parent_node_id");
                if (parentNodeId.HasValue)
                    Assert.Contains(parentNodeId.Value, knownIds);
            });
    }

    private static DbValue[] AssertOperator(
        CapturedPlan plan,
        params string[] acceptedOperatorTypes)
        => Assert.Single(
            plan.Rows,
            row => acceptedOperatorTypes.Contains(
                plan.Text(row, "operator_type"),
                StringComparer.Ordinal));

    private static void AssertPredicateTokens(
        CapturedPlan plan,
        DbValue[] row,
        params string[] expectedTokens)
    {
        string predicate = Assert.IsType<string>(plan.Text(row, "predicate"));
        Assert.All(
            expectedTokens,
            token => Assert.Contains(
                token,
                predicate,
                StringComparison.OrdinalIgnoreCase));
    }

    private static void AssertMutationSummary(CapturedPlan plan, string operatorType)
    {
        DbValue[] mutation = AssertOperator(plan, operatorType);
        Assert.Equal(1, plan.Integer(mutation, "actual_rows"));
        Assert.Equal(1, plan.Integer(mutation, "actual_loops"));
        Assert.True(plan.Integer(mutation, "elapsed_microseconds") >= 0);
        Assert.Equal("completed", plan.Text(mutation, "status"));
    }

    private sealed class CapturedPlan(
        ColumnDefinition[] schema,
        List<DbValue[]> rows)
    {
        private readonly Dictionary<string, int> _ordinals = schema
            .Select((column, ordinal) => (column.Name, ordinal))
            .ToDictionary(
                static item => item.Name,
                static item => item.ordinal,
                StringComparer.OrdinalIgnoreCase);

        internal ColumnDefinition[] Schema { get; } = schema;
        internal List<DbValue[]> Rows { get; } = rows;
        internal string[] ColumnNames =>
            Schema.Select(static column => column.Name).ToArray();

        internal IEnumerable<string> TextValues =>
            Rows.SelectMany(static row => row)
                .Where(static value => value.Type == DbType.Text)
                .Select(static value => value.AsText);

        internal string? Text(DbValue[] row, string columnName)
        {
            DbValue value = row[Ordinal(columnName)];
            return value.IsNull ? null : value.AsText;
        }

        internal long? Integer(DbValue[] row, string columnName)
        {
            DbValue value = row[Ordinal(columnName)];
            return value.IsNull ? null : value.AsInteger;
        }

        internal double? Real(DbValue[] row, string columnName)
        {
            DbValue value = row[Ordinal(columnName)];
            return value.IsNull ? null : value.AsReal;
        }

        private int Ordinal(string columnName)
            => _ordinals.TryGetValue(columnName, out int ordinal)
                ? ordinal
                : throw new Xunit.Sdk.XunitException(
                    $"Expected plan column '{columnName}' was not present.");
    }
}
