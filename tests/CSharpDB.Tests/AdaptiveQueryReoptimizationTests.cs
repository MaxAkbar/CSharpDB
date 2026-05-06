using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class AdaptiveQueryReoptimizationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void DatabaseOptions_DefaultsKeepAdaptiveReoptimizationDisabled()
    {
        var options = new DatabaseOptions();

        Assert.False(options.AdaptiveQueryReoptimization.Enabled);
        Assert.Equal(8, options.AdaptiveQueryReoptimization.DivergenceFactor);
        Assert.Equal(4096, options.AdaptiveQueryReoptimization.MinimumObservedRows);
        Assert.Equal(65536, options.AdaptiveQueryReoptimization.MaxBufferedRows);
        Assert.Equal(1, options.AdaptiveQueryReoptimization.MaxReoptimizationsPerQuery);
    }

    [Fact]
    public void EnableAdaptiveQueryReoptimization_ConfiguresOptInOptions()
    {
        var original = new DatabaseOptions();

        DatabaseOptions enabled = original.EnableAdaptiveQueryReoptimization(builder => builder
            .WithDivergenceFactor(3)
            .WithMinimumObservedRows(7)
            .WithMaxBufferedRows(11)
            .WithMaxReoptimizationsPerQuery(2));

        Assert.False(original.AdaptiveQueryReoptimization.Enabled);
        Assert.True(enabled.AdaptiveQueryReoptimization.Enabled);
        Assert.Equal(3, enabled.AdaptiveQueryReoptimization.DivergenceFactor);
        Assert.Equal(7, enabled.AdaptiveQueryReoptimization.MinimumObservedRows);
        Assert.Equal(11, enabled.AdaptiveQueryReoptimization.MaxBufferedRows);
        Assert.Equal(2, enabled.AdaptiveQueryReoptimization.MaxReoptimizationsPerQuery);
    }

    [Fact]
    public void EnableAdaptiveQueryReoptimization_RejectsInvalidThresholds()
    {
        var options = new DatabaseOptions();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            options.EnableAdaptiveQueryReoptimization(builder => builder.WithDivergenceFactor(1)));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            options.EnableAdaptiveQueryReoptimization(builder => builder.WithMinimumObservedRows(0)));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            options.EnableAdaptiveQueryReoptimization(builder => builder.WithMaxBufferedRows(0)));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            options.EnableAdaptiveQueryReoptimization(builder => builder.WithMaxReoptimizationsPerQuery(-1)));
    }

    [Fact]
    public async Task AdaptiveIndexNestedLoop_SwitchesToHashAlternativeBeforeRowsAreEmitted()
    {
        var counters = new AdaptiveRuntimeCounters();
        var rows = CreateSingleColumnRows(5);
        var outputSchema = OneIntegerColumnSchema();
        bool lookupChosen = false;
        bool hashChosen = false;

        var lease = new AdaptiveQueryExecutionLease(new AdaptiveQueryReoptimizationOptions
        {
            Enabled = true,
            DivergenceFactor = 2,
            MinimumObservedRows = 1,
            MaxBufferedRows = 16,
            MaxReoptimizationsPerQuery = 1,
        });
        var op = new AdaptiveIndexNestedLoopJoinOperator(
            new MaterializedOperator(rows, outputSchema),
            new MaterializedOperator([], outputSchema),
            outputSchema,
            source =>
            {
                lookupChosen = true;
                return source;
            },
            source =>
            {
                hashChosen = true;
                return source;
            },
            lease,
            counters.Diagnostics,
            estimatedOuterRows: 1,
            estimatedRowCount: null);

        List<DbValue[]> actualRows = await ReadAllRowsAsync(op);

        Assert.False(lookupChosen);
        Assert.True(hashChosen);
        Assert.Equal([1L, 2L, 3L, 4L, 5L], actualRows.Select(row => row[0].AsInteger).ToArray());
        Assert.Equal(1, counters.AttemptCount);
        Assert.Equal(1, counters.DivergenceCount);
        Assert.Equal(1, counters.SuccessfulSwitchCount);
        Assert.Equal(3, counters.BufferedRowCount);
    }

    [Fact]
    public async Task AdaptiveIndexNestedLoop_FallsBackWhenBufferedRowCapIsReached()
    {
        var counters = new AdaptiveRuntimeCounters();
        var rows = CreateSingleColumnRows(5);
        var outputSchema = OneIntegerColumnSchema();
        bool lookupChosen = false;
        bool hashChosen = false;

        var lease = new AdaptiveQueryExecutionLease(new AdaptiveQueryReoptimizationOptions
        {
            Enabled = true,
            DivergenceFactor = 2,
            MinimumObservedRows = 1,
            MaxBufferedRows = 2,
            MaxReoptimizationsPerQuery = 1,
        });
        var op = new AdaptiveIndexNestedLoopJoinOperator(
            new MaterializedOperator(rows, outputSchema),
            new MaterializedOperator([], outputSchema),
            outputSchema,
            source =>
            {
                lookupChosen = true;
                return source;
            },
            source =>
            {
                hashChosen = true;
                return source;
            },
            lease,
            counters.Diagnostics,
            estimatedOuterRows: 1,
            estimatedRowCount: null);

        List<DbValue[]> actualRows = await ReadAllRowsAsync(op);

        Assert.True(lookupChosen);
        Assert.False(hashChosen);
        Assert.Equal([1L, 2L, 3L, 4L, 5L], actualRows.Select(row => row[0].AsInteger).ToArray());
        Assert.Equal(1, counters.AttemptCount);
        Assert.Equal(1, counters.RejectedSwitchCount);
        Assert.Equal(1, counters.MaxBufferedFallbackCount);
        Assert.Equal(2, counters.BufferedRowCount);
    }

    [Fact]
    public async Task AdaptiveHashJoin_SwitchesBuildSideWhenObservedBuildSideDiverges()
    {
        var counters = new AdaptiveRuntimeCounters();
        ColumnDefinition[] leftSchema =
        [
            new() { Name = "id", Type = DbType.Integer },
            new() { Name = "code", Type = DbType.Integer },
        ];
        ColumnDefinition[] rightSchema =
        [
            new() { Name = "code", Type = DbType.Integer },
            new() { Name = "payload", Type = DbType.Integer },
        ];
        var compositeSchema = new TableSchema
        {
            TableName = "join",
            Columns = leftSchema.Concat(rightSchema).ToArray(),
        };

        var leftRows = new List<DbValue[]>
        {
            new[] { DbValue.FromInteger(1), DbValue.FromInteger(1) },
            new[] { DbValue.FromInteger(2), DbValue.FromInteger(2) },
        };
        var rightRows = Enumerable.Range(1, 6)
            .Select(i => new[] { DbValue.FromInteger(i), DbValue.FromInteger(i * 10) })
            .ToList();

        var lease = new AdaptiveQueryExecutionLease(new AdaptiveQueryReoptimizationOptions
        {
            Enabled = true,
            DivergenceFactor = 2,
            MinimumObservedRows = 1,
            MaxBufferedRows = 16,
            MaxReoptimizationsPerQuery = 1,
        });
        var op = new AdaptiveHashJoinOperator(
            new MaterializedOperator(leftRows, leftSchema),
            new MaterializedOperator(rightRows, rightSchema),
            JoinType.Inner,
            residualCondition: null,
            compositeSchema,
            leftColCount: 2,
            rightColCount: 2,
            leftKeyIndices: [1],
            rightKeyIndices: [0],
            plannedBuildRightSide: true,
            estimatedLeftRows: 2,
            estimatedRightRows: 1,
            estimatedRowCount: null,
            DbFunctionRegistry.Empty,
            lease,
            counters.Diagnostics);

        List<DbValue[]> actualRows = await ReadAllRowsAsync(op);

        Assert.Equal(2, actualRows.Count);
        Assert.Equal([1L, 2L], actualRows.Select(row => row[0].AsInteger).Order().ToArray());
        Assert.Equal(1, counters.AttemptCount);
        Assert.Equal(1, counters.DivergenceCount);
        Assert.Equal(1, counters.SuccessfulSwitchCount);
    }

    [Fact]
    public async Task AdaptiveReoptimization_IsSilentWhenDisabled()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await SetupHashJoinTablesAsync(db);

        await using var result = await db.ExecuteAsync(
            "SELECT l.id, r.payload FROM adaptive_left l JOIN adaptive_right r ON l.code = r.code",
            Ct);
        var rows = await result.ToListAsync(Ct);
        var diagnostics = db.GetAdaptiveQueryReoptimizationDiagnosticsSnapshot();

        Assert.Equal(3, rows.Count);
        Assert.Equal(0, diagnostics.EligibleQueryCount);
        Assert.Equal(0, diagnostics.AttemptCount);
        Assert.DoesNotContain(EnumerateOperatorTree(GetRootOperator(result)), op => op is AdaptiveHashJoinOperator);
    }

    [Fact]
    public async Task AdaptiveReoptimization_WrapsEligibleHashJoinWhenEnabled()
    {
        await using var db = await Database.OpenInMemoryAsync(
            new DatabaseOptions().EnableAdaptiveQueryReoptimization(builder => builder
                .WithDivergenceFactor(2)
                .WithMinimumObservedRows(1)
                .WithMaxBufferedRows(64)),
            Ct);
        await SetupHashJoinTablesAsync(db);
        db.ResetAdaptiveQueryReoptimizationDiagnostics();

        await using var result = await db.ExecuteAsync(
            "SELECT l.id, r.payload FROM adaptive_left l JOIN adaptive_right r ON l.code = r.code",
            Ct);
        var rows = await result.ToListAsync(Ct);
        var diagnostics = db.GetAdaptiveQueryReoptimizationDiagnosticsSnapshot();

        Assert.Equal(3, rows.Count);
        Assert.Contains(EnumerateOperatorTree(GetRootOperator(result)), op => op is AdaptiveHashJoinOperator);
        Assert.Equal(1, diagnostics.EligibleQueryCount);
        Assert.Equal(1, diagnostics.AttemptCount);
        Assert.True(diagnostics.BufferedRowCount > 0);
    }

    [Fact]
    public async Task AdaptiveReoptimization_PreservesLeftJoinNullExtension()
    {
        await using var db = await Database.OpenInMemoryAsync(
            new DatabaseOptions().EnableAdaptiveQueryReoptimization(builder => builder
                .WithDivergenceFactor(2)
                .WithMinimumObservedRows(1)
                .WithMaxBufferedRows(64)),
            Ct);

        await db.ExecuteAsync("CREATE TABLE adaptive_left (id INTEGER PRIMARY KEY, code INTEGER NOT NULL)", Ct);
        await db.ExecuteAsync("CREATE TABLE adaptive_right (code INTEGER PRIMARY KEY, payload INTEGER NOT NULL)", Ct);
        await db.ExecuteAsync("INSERT INTO adaptive_left VALUES (1, 1)", Ct);
        await db.ExecuteAsync("INSERT INTO adaptive_left VALUES (2, 2)", Ct);
        await db.ExecuteAsync("INSERT INTO adaptive_right VALUES (1, 10)", Ct);
        db.ResetAdaptiveQueryReoptimizationDiagnostics();

        await using var result = await db.ExecuteAsync(
            "SELECT l.id, r.payload FROM adaptive_left l LEFT JOIN adaptive_right r ON r.code = l.code ORDER BY l.id",
            Ct);
        var rows = await result.ToListAsync(Ct);
        var diagnostics = db.GetAdaptiveQueryReoptimizationDiagnosticsSnapshot();

        Assert.Equal(2, rows.Count);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(10L, rows[0][1].AsInteger);
        Assert.Equal(2L, rows[1][0].AsInteger);
        Assert.True(rows[1][1].IsNull);
        Assert.Equal(1, diagnostics.EligibleQueryCount);
        Assert.Equal(1, diagnostics.AttemptCount);
    }

    [Theory]
    [InlineData("SELECT * FROM adaptive_left l JOIN adaptive_right r ON l.code = r.code")]
    [InlineData("SELECT l.id FROM adaptive_left l CROSS JOIN adaptive_right r")]
    [InlineData("SELECT l.id FROM adaptive_left l JOIN adaptive_right r ON l.code = r.code UNION SELECT id FROM adaptive_left")]
    public async Task AdaptiveReoptimization_DoesNotAdaptUnsupportedShapes(string sql)
    {
        await using var db = await Database.OpenInMemoryAsync(
            new DatabaseOptions().EnableAdaptiveQueryReoptimization(),
            Ct);
        await SetupHashJoinTablesAsync(db);
        db.ResetAdaptiveQueryReoptimizationDiagnostics();

        await using var result = await db.ExecuteAsync(sql, Ct);
        _ = await result.ToListAsync(Ct);
        var diagnostics = db.GetAdaptiveQueryReoptimizationDiagnosticsSnapshot();

        Assert.Equal(0, diagnostics.EligibleQueryCount);
        Assert.Equal(0, diagnostics.AttemptCount);
    }

    private static async ValueTask SetupHashJoinTablesAsync(Database db)
    {
        await db.ExecuteAsync("CREATE TABLE adaptive_left (id INTEGER PRIMARY KEY, code INTEGER NOT NULL)", Ct);
        await db.ExecuteAsync("CREATE TABLE adaptive_right (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, payload INTEGER NOT NULL)", Ct);

        for (int i = 1; i <= 3; i++)
        {
            await db.ExecuteAsync($"INSERT INTO adaptive_left VALUES ({i}, {i})", Ct);
            await db.ExecuteAsync($"INSERT INTO adaptive_right VALUES ({i}, {i}, {i * 10})", Ct);
        }
    }

    private static async Task<List<DbValue[]>> ReadAllRowsAsync(IOperator op)
    {
        var rows = new List<DbValue[]>();
        await op.OpenAsync(Ct);
        try
        {
            while (await op.MoveNextAsync(Ct))
                rows.Add((DbValue[])op.Current.Clone());
        }
        finally
        {
            await op.DisposeAsync();
        }

        return rows;
    }

    private static List<DbValue[]> CreateSingleColumnRows(int count)
    {
        var rows = new List<DbValue[]>(count);
        for (int i = 1; i <= count; i++)
            rows.Add([DbValue.FromInteger(i)]);

        return rows;
    }

    private static ColumnDefinition[] OneIntegerColumnSchema()
        => [new ColumnDefinition { Name = "value", Type = DbType.Integer }];

    private static IOperator GetRootOperator(QueryResult result)
    {
        IOperator storedOperator = GetStoredOperator(result);
        return storedOperator is BatchToRowOperatorAdapter batchAdapter
            ? batchAdapter.BatchSource as IOperator
                ?? throw new InvalidOperationException("Batch adapter did not expose an operator root.")
            : storedOperator;
    }

    private static IOperator GetStoredOperator(QueryResult result)
    {
        var operatorField = typeof(QueryResult).GetField("_operator", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("QueryResult operator field not found.");
        var storedOperator = (IOperator?)operatorField.GetValue(result);
        if (storedOperator != null)
            return storedOperator;

        var batchOperatorField = typeof(QueryResult).GetField("_batchOperator", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("QueryResult batch operator field not found.");
        return (IOperator?)batchOperatorField.GetValue(result)
            ?? throw new InvalidOperationException("QueryResult did not contain an operator.");
    }

    private static IEnumerable<IOperator> EnumerateOperatorTree(IOperator? start)
    {
        for (IOperator? current = start; current != null;)
        {
            yield return current;

            if (current is BatchToRowOperatorAdapter batchAdapter &&
                batchAdapter.BatchSource is IOperator batchOperator)
            {
                current = batchOperator;
                continue;
            }

            current = current is IUnaryOperatorSource unary ? unary.Source : null;
        }
    }

    private sealed class AdaptiveRuntimeCounters
    {
        public long AttemptCount { get; private set; }
        public long SuccessfulSwitchCount { get; private set; }
        public long RejectedSwitchCount { get; private set; }
        public long DivergenceCount { get; private set; }
        public long BufferedRowCount { get; private set; }
        public long MaxBufferedFallbackCount { get; private set; }
        public long ReoptimizationLimitFallbackCount { get; private set; }
        public long UnsupportedFallbackCount { get; private set; }

        public AdaptiveQueryReoptimizationRuntimeDiagnostics Diagnostics { get; }

        public AdaptiveRuntimeCounters()
        {
            Diagnostics = new AdaptiveQueryReoptimizationRuntimeDiagnostics(
                () => AttemptCount++,
                () => SuccessfulSwitchCount++,
                RecordRejectedSwitch,
                () => DivergenceCount++,
                count => BufferedRowCount += count);
        }

        private void RecordRejectedSwitch(AdaptiveQueryReoptimizationFallbackReason reason)
        {
            RejectedSwitchCount++;
            switch (reason)
            {
                case AdaptiveQueryReoptimizationFallbackReason.MaxBufferedRows:
                    MaxBufferedFallbackCount++;
                    break;
                case AdaptiveQueryReoptimizationFallbackReason.ReoptimizationLimit:
                    ReoptimizationLimitFallbackCount++;
                    break;
                case AdaptiveQueryReoptimizationFallbackReason.Unsupported:
                    UnsupportedFallbackCount++;
                    break;
            }
        }
    }
}
