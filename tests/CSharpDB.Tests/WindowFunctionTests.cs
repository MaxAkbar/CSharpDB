using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class WindowFunctionParserTests
{
    [Fact]
    public void Parse_RankingWindow_WithPartitionOrderAndAlias()
    {
        var select = Assert.IsType<SelectStatement>(
            Parser.Parse(
                "SELECT ROW_NUMBER() OVER (PARTITION BY department ORDER BY score DESC, id) AS rn FROM scores"));

        WindowFunctionExpression window = Assert.IsType<WindowFunctionExpression>(
            Assert.Single(select.Columns).Expression);
        Assert.Equal("ROW_NUMBER", window.Function.FunctionName);
        Assert.Empty(window.Function.Arguments);
        Assert.Single(window.Window.PartitionBy);
        Assert.Equal(2, window.Window.OrderBy.Count);
        Assert.True(window.Window.OrderBy[0].Descending);
        Assert.False(window.Window.OrderBy[1].Descending);
        Assert.Equal("rn", select.Columns[0].Alias);
    }

    [Fact]
    public void Parse_AggregateWindow_WithStarAndEmptySpecification()
    {
        var select = Assert.IsType<SelectStatement>(
            Parser.Parse("SELECT COUNT(*) OVER () AS total FROM scores"));

        WindowFunctionExpression window = Assert.IsType<WindowFunctionExpression>(
            Assert.Single(select.Columns).Expression);
        Assert.Equal("COUNT", window.Function.FunctionName);
        Assert.True(window.Function.IsStarArg);
        Assert.Empty(window.Window.PartitionBy);
        Assert.Empty(window.Window.OrderBy);
    }

    [Fact]
    public void Parse_WindowAndProjectionStar_PreserveSelectItemOrder()
    {
        var select = Assert.IsType<SelectStatement>(
            Parser.Parse(
                "SELECT score + 1 AS next_score, *, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM scores"));

        Assert.Equal(3, select.Columns.Count);
        Assert.IsType<BinaryExpression>(select.Columns[0].Expression);
        Assert.Equal("next_score", select.Columns[0].Alias);
        Assert.True(select.Columns[1].IsStar);
        Assert.IsType<WindowFunctionExpression>(select.Columns[2].Expression);
        Assert.Equal("rn", select.Columns[2].Alias);
    }

    [Fact]
    public void Parse_ExplicitFrame_ReportsExperimentalBoundary()
    {
        CSharpDbException error = Assert.Throws<CSharpDbException>(
            () => Parser.Parse(
                "SELECT SUM(score) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM scores"));

        Assert.Contains("Explicit window frames are not supported", error.Message);
    }

    [Fact]
    public void Parse_NamedWindow_ReportsExperimentalBoundary()
    {
        CSharpDbException error = Assert.Throws<CSharpDbException>(
            () => Parser.Parse("SELECT ROW_NUMBER() OVER named_window FROM scores"));

        Assert.Contains("Named windows are not supported", error.Message);
    }
}

public sealed class WindowFunctionExecutionTests : IAsyncLifetime
{
    private readonly string _dbPath =
        Path.Combine(Path.GetTempPath(), $"csharpdb_window_test_{Guid.NewGuid():N}.db");
    private Database _database = null!;

    public async ValueTask InitializeAsync()
    {
        _database = await Database.OpenAsync(_dbPath);
        await _database.ExecuteAsync(
            "CREATE TABLE scores (id INTEGER PRIMARY KEY, department TEXT, score INTEGER, label TEXT)",
            TestContext.Current.CancellationToken);
        await _database.ExecuteAsync(
            """
            INSERT INTO scores VALUES
                (1, 'A', 10, 'alpha'),
                (2, 'A', 10, 'ALPHA'),
                (3, 'A', 20, 'beta'),
                (4, 'B', NULL, 'null-row'),
                (5, 'B', 5, 'five')
            """,
            TestContext.Current.CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await _database.DisposeAsync();
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal"))
            File.Delete(_dbPath + ".wal");
    }

    [Fact]
    public async Task RankingWindows_HandlePartitionsPeersAndNulls()
    {
        await using QueryResult result = await _database.ExecuteAsync(
            """
            SELECT id,
                   ROW_NUMBER() OVER (PARTITION BY department ORDER BY score) AS row_num,
                   RANK() OVER (PARTITION BY department ORDER BY score) AS rank_num,
                   DENSE_RANK() OVER (PARTITION BY department ORDER BY score) AS dense_rank_num
            FROM scores
            ORDER BY department, score, id
            """,
            TestContext.Current.CancellationToken);

        List<DbValue[]> rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(5, rows.Count);
        Assert.Equal([1L, 1L, 1L, 1L], ToIntegers(rows[0]));
        Assert.Equal([2L, 2L, 1L, 1L], ToIntegers(rows[1]));
        Assert.Equal([3L, 3L, 3L, 2L], ToIntegers(rows[2]));
        Assert.Equal([4L, 1L, 1L, 1L], ToIntegers(rows[3]));
        Assert.Equal([5L, 2L, 2L, 2L], ToIntegers(rows[4]));
    }

    [Fact]
    public async Task OrderedAggregateWindows_UsePeerAwareDefaultFrame()
    {
        await using QueryResult result = await _database.ExecuteAsync(
            """
            SELECT id,
                   COUNT(score) OVER (PARTITION BY department ORDER BY score) AS count_score,
                   SUM(score) OVER (PARTITION BY department ORDER BY score) AS sum_score,
                   AVG(score) OVER (PARTITION BY department ORDER BY score) AS avg_score,
                   MIN(score) OVER (PARTITION BY department ORDER BY score) AS min_score,
                   MAX(score) OVER (PARTITION BY department ORDER BY score) AS max_score
            FROM scores
            ORDER BY department, score, id
            """,
            TestContext.Current.CancellationToken);

        List<DbValue[]> rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, rows[0][1].AsInteger);
        Assert.Equal(20, rows[0][2].AsInteger);
        Assert.Equal(10, rows[0][3].AsReal);
        Assert.Equal(10, rows[1][3].AsReal);
        Assert.Equal(3, rows[2][1].AsInteger);
        Assert.Equal(40, rows[2][2].AsInteger);
        Assert.Equal(40d / 3d, rows[2][3].AsReal, precision: 10);
        Assert.Equal(10, rows[2][4].AsInteger);
        Assert.Equal(20, rows[2][5].AsInteger);

        Assert.Equal(0, rows[3][1].AsInteger);
        Assert.True(rows[3][2].IsNull);
        Assert.True(rows[3][3].IsNull);
        Assert.True(rows[3][4].IsNull);
        Assert.True(rows[3][5].IsNull);
        Assert.Equal(1, rows[4][1].AsInteger);
        Assert.Equal(5, rows[4][2].AsInteger);
    }

    [Fact]
    public async Task AggregateWindow_WithoutOrdering_UsesWholePartition()
    {
        await using QueryResult result = await _database.ExecuteAsync(
            """
            SELECT id,
                   COUNT(*) OVER (PARTITION BY department) AS row_count,
                   COUNT(score) OVER (PARTITION BY department) AS score_count,
                   SUM(score) OVER (PARTITION BY department) AS total_score
            FROM scores
            ORDER BY id
            """,
            TestContext.Current.CancellationToken);

        List<DbValue[]> rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal([1L, 3L, 3L, 40L], ToIntegers(rows[0]));
        Assert.Equal([2L, 3L, 3L, 40L], ToIntegers(rows[1]));
        Assert.Equal([3L, 3L, 3L, 40L], ToIntegers(rows[2]));
        Assert.Equal(2, rows[3][1].AsInteger);
        Assert.Equal(1, rows[3][2].AsInteger);
        Assert.Equal(5, rows[3][3].AsInteger);
    }

    [Fact]
    public async Task RankingPeers_RespectExplicitCollation()
    {
        await using QueryResult result = await _database.ExecuteAsync(
            """
            SELECT id,
                   RANK() OVER (PARTITION BY department ORDER BY label COLLATE NOCASE) AS rank_num,
                   DENSE_RANK() OVER (PARTITION BY department ORDER BY label COLLATE NOCASE) AS dense_rank_num
            FROM scores
            WHERE department = 'A'
            ORDER BY id
            """,
            TestContext.Current.CancellationToken);

        List<DbValue[]> rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal([1L, 1L, 1L], ToIntegers(rows[0]));
        Assert.Equal([2L, 1L, 1L], ToIntegers(rows[1]));
        Assert.Equal([3L, 3L, 2L], ToIntegers(rows[2]));
    }

    [Fact]
    public async Task FinalOrderAliasAndLimit_AreAppliedAfterWindowEvaluation()
    {
        await using QueryResult result = await _database.ExecuteAsync(
            """
            SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn
            FROM scores
            ORDER BY rn DESC
            LIMIT 2
            """,
            TestContext.Current.CancellationToken);

        List<DbValue[]> rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, rows.Count);
        Assert.Equal([5L, 5L], ToIntegers(rows[0]));
        Assert.Equal([4L, 4L], ToIntegers(rows[1]));
    }

    [Fact]
    public async Task SelectStar_WithWindowOnlyInOrderBy_DoesNotExposeHiddenSlot()
    {
        await using QueryResult result = await _database.ExecuteAsync(
            """
            SELECT *
            FROM scores
            ORDER BY ROW_NUMBER() OVER (ORDER BY id DESC)
            LIMIT 2
            """,
            TestContext.Current.CancellationToken);

        List<DbValue[]> rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(["id", "department", "score", "label"], result.Schema.Select(column => column.Name));
        Assert.All(rows, row => Assert.Equal(4, row.Length));
        Assert.Equal(5, rows[0][0].AsInteger);
        Assert.Equal(4, rows[1][0].AsInteger);
    }

    [Fact]
    public async Task WindowBeforeStar_PreservesSelectItemOrderAndAlias()
    {
        await using QueryResult result = await _database.ExecuteAsync(
            """
            SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn, *
            FROM scores
            ORDER BY id
            LIMIT 2
            """,
            TestContext.Current.CancellationToken);

        List<DbValue[]> rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(
            ["rn", "id", "department", "score", "label"],
            result.Schema.Select(column => column.Name));
        Assert.Equal([1L, 1L], [rows[0][0].AsInteger, rows[0][1].AsInteger]);
        Assert.Equal([2L, 2L], [rows[1][0].AsInteger, rows[1][1].AsInteger]);
    }

    [Fact]
    public async Task MixedExpressionsAndStar_PreserveProjectionOrderAndValues()
    {
        await using QueryResult result = await _database.ExecuteAsync(
            """
            SELECT score + 1 AS next_score,
                   *,
                   ROW_NUMBER() OVER (ORDER BY id) + 100 AS shifted_row
            FROM scores
            ORDER BY id
            LIMIT 1
            """,
            TestContext.Current.CancellationToken);

        DbValue[] row = Assert.Single(
            await result.ToListAsync(TestContext.Current.CancellationToken));

        Assert.Equal(
            ["next_score", "id", "department", "score", "label", "shifted_row"],
            result.Schema.Select(column => column.Name));
        Assert.Equal(11, row[0].AsInteger);
        Assert.Equal(1, row[1].AsInteger);
        Assert.Equal("A", row[2].AsText);
        Assert.Equal(10, row[3].AsInteger);
        Assert.Equal("alpha", row[4].AsText);
        Assert.Equal(101, row[5].AsInteger);
    }

    [Fact]
    public async Task MixedExpressionsAndStar_WithoutWindow_PreserveProjectionOrderAndValues()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE INDEX ix_scores_department ON scores (department)",
            ct);

        string[] queries =
        [
            """
            SELECT score + 1 AS next_score, *, id + 100 AS shifted_id
            FROM scores
            WHERE id = 1
            """,
            """
            SELECT score + 1 AS next_score, *, id + 100 AS shifted_id
            FROM scores
            LIMIT 1
            """,
            """
            SELECT score + 1 AS next_score, *, id + 100 AS shifted_id
            FROM scores
            WHERE department = 'A'
            LIMIT 1
            """,
        ];

        foreach (string query in queries)
        {
            await using QueryResult result = await _database.ExecuteAsync(query, ct);
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));

            Assert.Equal(
                ["next_score", "id", "department", "score", "label", "shifted_id"],
                result.Schema.Select(column => column.Name));
            Assert.Equal(11, row[0].AsInteger);
            Assert.Equal(1, row[1].AsInteger);
            Assert.Equal("A", row[2].AsText);
            Assert.Equal(10, row[3].AsInteger);
            Assert.Equal("alpha", row[4].AsText);
            Assert.Equal(101, row[5].AsInteger);
        }
    }

    [Fact]
    public async Task MixedStar_WithOrdinaryAggregate_IsRejectedExplicitly()
    {
        CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                "SELECT *, COUNT(*) FROM scores",
                TestContext.Current.CancellationToken));

        Assert.Equal(ErrorCode.SyntaxError, failure.Code);
        Assert.Contains("Mixing '*'", failure.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task MixedExpressionsAndStar_InStoredView_PreserveProjectionOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            """
            CREATE VIEW mixed_star_scores AS
            SELECT score + 1 AS next_score, *, id + 100 AS shifted_id
            FROM scores
            """,
            ct);

        await using QueryResult result = await _database.ExecuteAsync(
            "SELECT * FROM mixed_star_scores WHERE id = 1",
            ct);
        DbValue[] row = Assert.Single(await result.ToListAsync(ct));

        Assert.Equal(
            ["next_score", "id", "department", "score", "label", "shifted_id"],
            result.Schema.Select(column => column.Name));
        Assert.Equal([11L, 1L, 10L, 101L], [
            row[0].AsInteger,
            row[1].AsInteger,
            row[3].AsInteger,
            row[5].AsInteger,
        ]);
    }

    [Fact]
    public async Task WindowAndStar_InStoredView_UseWindowPlanningPipeline()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            """
            CREATE VIEW window_star_scores AS
            SELECT *, ROW_NUMBER() OVER (ORDER BY id) AS row_number
            FROM scores
            """,
            ct);

        await using QueryResult result = await _database.ExecuteAsync(
            "SELECT id, row_number FROM window_star_scores ORDER BY id LIMIT 2",
            ct);
        List<DbValue[]> rows = await result.ToListAsync(ct);

        Assert.Equal(["id", "row_number"], result.Schema.Select(column => column.Name));
        Assert.Equal([1L, 1L], ToIntegers(rows[0]));
        Assert.Equal([2L, 2L], ToIntegers(rows[1]));
    }

    [Fact]
    public async Task MixedExpressionsAndStar_WithCorrelatedProjection_PreserveOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using QueryResult result = await _database.ExecuteAsync(
            """
            SELECT (
                       SELECT inner_scores.score
                       FROM scores inner_scores
                       WHERE inner_scores.id = outer_scores.id
                   ) AS copied_score,
                   *,
                   id + 100 AS shifted_id
            FROM scores outer_scores
            WHERE id = 1
            """,
            ct);
        DbValue[] row = Assert.Single(await result.ToListAsync(ct));

        Assert.Equal(
            ["copied_score", "id", "department", "score", "label", "shifted_id"],
            result.Schema.Select(column => column.Name));
        Assert.Equal([10L, 1L, 10L, 101L], [
            row[0].AsInteger,
            row[1].AsInteger,
            row[3].AsInteger,
            row[5].AsInteger,
        ]);
    }

    [Theory]
    [InlineData("SELECT id FROM scores WHERE ROW_NUMBER() OVER () > 0", "only allowed")]
    [InlineData("SELECT ROW_NUMBER() OVER () FROM scores GROUP BY department", "GROUP BY")]
    [InlineData("SELECT ROW_NUMBER() OVER () FROM scores HAVING ROW_NUMBER() OVER () > 0", "only allowed")]
    [InlineData("SELECT s.id FROM scores s JOIN scores t ON ROW_NUMBER() OVER () = 1", "only allowed")]
    [InlineData("SELECT COUNT(*), ROW_NUMBER() OVER () FROM scores", "ordinary aggregate")]
    [InlineData("SELECT ROW_NUMBER() OVER (ORDER BY RANK() OVER (ORDER BY score)) FROM scores", "Nested")]
    [InlineData("SELECT ROW_NUMBER() OVER (ORDER BY id), RANK() OVER (ORDER BY score) FROM scores", "incompatible")]
    [InlineData("SELECT LAG(score) OVER (ORDER BY id) FROM scores", "not supported")]
    [InlineData("SELECT COUNT(DISTINCT score) OVER () FROM scores", "DISTINCT")]
    public async Task InvalidWindowPlacementOrUnsupportedCombination_IsRejected(
        string sql,
        string expectedMessage)
    {
        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            async () =>
            {
                await using QueryResult result = await _database.ExecuteAsync(
                    sql,
                    TestContext.Current.CancellationToken);
                await result.ToListAsync(TestContext.Current.CancellationToken);
            });

        Assert.Contains(expectedMessage, error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task WindowOperator_CancellationDisposesItsSource()
    {
        ColumnDefinition[] inputColumns =
        [
            new() { Name = "value", Type = DbType.Integer, Nullable = false },
        ];
        var inputSchema = new TableSchema
        {
            TableName = "input",
            Columns = inputColumns,
        };
        var source = new CancellationTrackingOperator(inputColumns);
        var expression = new WindowFunctionExpression
        {
            Function = new FunctionCallExpression
            {
                FunctionName = "ROW_NUMBER",
                Arguments = [],
            },
            Window = new WindowSpecification(),
        };
        ColumnDefinition[] outputColumns =
        [
            inputColumns[0],
            new() { Name = "rn", Type = DbType.Integer, Nullable = false },
        ];
        var window = new WindowOperator(
            source,
            inputSchema,
            [expression],
            outputColumns);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => window.OpenAsync(cts.Token).AsTask());

        Assert.Equal(1, source.DisposeCount);
        await window.DisposeAsync();
        Assert.Equal(1, source.DisposeCount);
    }

    [Fact]
    public async Task WindowOperator_CancellationDuringLargePeerLoop_DisposesItsSource()
    {
        ColumnDefinition[] inputColumns =
        [
            new() { Name = "value", Type = DbType.Integer, Nullable = false },
        ];
        var inputSchema = new TableSchema
        {
            TableName = "input",
            Columns = inputColumns,
        };
        var source = new MaterializedCancellationTrackingOperator(
            inputColumns,
            Enumerable.Range(1, 2048)
                .Select(value => new[] { DbValue.FromInteger(value) })
                .ToArray());
        using var cts = new CancellationTokenSource();
        int invocationCount = 0;
        DbFunctionRegistry functions = DbFunctionRegistry.Create(builder =>
            builder.AddScalar(
                "cancel_during_window",
                1,
                new DbScalarFunctionOptions(
                    ReturnType: DbType.Integer,
                    IsDeterministic: false),
                (_, arguments) =>
                {
                    if (Interlocked.Increment(ref invocationCount) == 1)
                        cts.Cancel();

                    return arguments[0];
                }));
        var expression = new WindowFunctionExpression
        {
            Function = new FunctionCallExpression
            {
                FunctionName = "SUM",
                Arguments =
                [
                    new FunctionCallExpression
                    {
                        FunctionName = "cancel_during_window",
                        Arguments =
                        [
                            new ColumnRefExpression { ColumnName = "value" },
                        ],
                    },
                ],
            },
            Window = new WindowSpecification
            {
                OrderBy =
                [
                    new OrderByClause
                    {
                        Expression = new LiteralExpression
                        {
                            LiteralType = TokenType.IntegerLiteral,
                            Value = 0L,
                        },
                    },
                ],
            },
        };
        ColumnDefinition[] outputColumns =
        [
            inputColumns[0],
            new() { Name = "running_total", Type = DbType.Integer, Nullable = true },
        ];
        var window = new WindowOperator(
            source,
            inputSchema,
            [expression],
            outputColumns,
            functions);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => window.OpenAsync(cts.Token).AsTask());

        Assert.InRange(invocationCount, 1, 1024);
        Assert.Equal(1, source.DisposeCount);
        await window.DisposeAsync();
        Assert.Equal(1, source.DisposeCount);
    }

    private static long[] ToIntegers(DbValue[] row) =>
        row.Select(value => value.AsInteger).ToArray();

    private sealed class CancellationTrackingOperator(ColumnDefinition[] outputSchema) : IOperator
    {
        public int DisposeCount { get; private set; }
        public ColumnDefinition[] OutputSchema { get; } = outputSchema;
        public DbValue[] Current => Array.Empty<DbValue>();

        public ValueTask OpenAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            return ValueTask.FromResult(false);
        }

        public ValueTask DisposeAsync()
        {
            DisposeCount++;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class MaterializedCancellationTrackingOperator(
        ColumnDefinition[] outputSchema,
        DbValue[][] rows) : IOperator
    {
        private int _index;

        public int DisposeCount { get; private set; }
        public ColumnDefinition[] OutputSchema { get; } = outputSchema;
        public bool ReusesCurrentRowBuffer => false;
        public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

        public ValueTask OpenAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            _index = -1;
            Current = Array.Empty<DbValue>();
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            _index++;
            if (_index >= rows.Length)
            {
                Current = Array.Empty<DbValue>();
                return ValueTask.FromResult(false);
            }

            Current = rows[_index];
            return ValueTask.FromResult(true);
        }

        public ValueTask DisposeAsync()
        {
            DisposeCount++;
            Current = Array.Empty<DbValue>();
            return ValueTask.CompletedTask;
        }
    }
}
