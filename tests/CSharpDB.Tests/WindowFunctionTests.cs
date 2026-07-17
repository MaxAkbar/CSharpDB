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
}
