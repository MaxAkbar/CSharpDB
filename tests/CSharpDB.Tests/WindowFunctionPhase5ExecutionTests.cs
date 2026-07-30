using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class WindowFunctionPhase5ExecutionTests : IAsyncLifetime
{
    private readonly string _dbPath =
        Path.Combine(Path.GetTempPath(), $"csharpdb_window_phase5_{Guid.NewGuid():N}.db");
    private Database _database = null!;

    public async ValueTask InitializeAsync()
    {
        _database = await Database.OpenAsync(_dbPath, TestContext.Current.CancellationToken);
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
        DeleteDatabaseFiles(_dbPath);
    }

    [Fact]
    public async Task ExplicitRowsFrames_ArePositionalAndShareCompatibleOrdering()
    {
        await using QueryResult result = await _database.ExecuteAsync(
            """
            SELECT id,
                   SUM(score) OVER (
                       PARTITION BY department ORDER BY score
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS rows_sum,
                   SUM(score) OVER (
                       PARTITION BY department ORDER BY score
                   ) AS peer_sum,
                   COUNT(*) OVER (
                       PARTITION BY department ORDER BY score
                       ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
                   ) AS centered_count,
                   SUM(score) OVER (
                       PARTITION BY department ORDER BY score
                       ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
                   ) AS next_sum
            FROM scores
            WHERE department = 'A'
            ORDER BY id
            """,
            TestContext.Current.CancellationToken);

        List<DbValue[]> rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(3, rows.Count);
        Assert.Equal([1L, 10L, 20L, 2L, 10L], ToIntegers(rows[0]));
        Assert.Equal([2L, 20L, 20L, 3L, 20L], ToIntegers(rows[1]));
        Assert.Equal([3L, 40L, 40L, 2L], ToIntegers(rows[2][..4]));
        Assert.True(rows[2][4].IsNull);
    }

    [Fact]
    public async Task NamedWindows_ReuseOrderingWithIndependentFrames()
    {
        await using QueryResult result = await _database.ExecuteAsync(
            """
            SELECT id,
                   SUM(score) OVER running AS running_sum,
                   SUM(score) OVER CURRENT_ONLY AS current_sum
            FROM scores
            WHERE department = 'A'
            WINDOW running AS (
                       PARTITION BY department ORDER BY id
                       ROWS UNBOUNDED PRECEDING
                   ),
                   current_only AS (
                       PARTITION BY department ORDER BY id
                       ROWS BETWEEN CURRENT ROW AND CURRENT ROW
                   )
            ORDER BY id
            """,
            TestContext.Current.CancellationToken);

        List<DbValue[]> rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal([1L, 10L, 10L], ToIntegers(rows[0]));
        Assert.Equal([2L, 20L, 10L], ToIntegers(rows[1]));
        Assert.Equal([3L, 40L, 20L], ToIntegers(rows[2]));
    }

    [Fact]
    public async Task NavigationAndValueFunctions_RespectPartitionsFramesAndNullTargets()
    {
        await using (QueryResult result = await _database.ExecuteAsync(
            """
            SELECT id,
                   LAG(score) OVER ordered AS previous_score,
                   LAG(label, 2, 'missing') OVER ordered AS two_labels_back,
                   LEAD(score, 1, -1) OVER ordered AS next_score,
                   LAG(score, 0, -1) OVER ordered AS current_score,
                   FIRST_VALUE(label) OVER ordered AS first_label,
                   LAST_VALUE(label) OVER ordered AS last_peer_label,
                   LAST_VALUE(label) OVER whole_partition AS last_partition_label
            FROM scores
            WHERE department = 'A'
            WINDOW ordered AS (PARTITION BY department ORDER BY score),
                   whole_partition AS (
                       PARTITION BY department ORDER BY score
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                   )
            ORDER BY id
            """,
            TestContext.Current.CancellationToken))
        {
            List<DbValue[]> rows = await result.ToListAsync(TestContext.Current.CancellationToken);

            Assert.True(rows[0][1].IsNull);
            Assert.Equal("missing", rows[0][2].AsText);
            Assert.Equal(10, rows[0][3].AsInteger);
            Assert.Equal(10, rows[0][4].AsInteger);
            Assert.Equal("alpha", rows[0][5].AsText);
            Assert.Equal("ALPHA", rows[0][6].AsText);
            Assert.Equal("beta", rows[0][7].AsText);

            Assert.Equal(10, rows[1][1].AsInteger);
            Assert.Equal("missing", rows[1][2].AsText);
            Assert.Equal(20, rows[1][3].AsInteger);
            Assert.Equal("alpha", rows[1][5].AsText);
            Assert.Equal("ALPHA", rows[1][6].AsText);

            Assert.Equal(10, rows[2][1].AsInteger);
            Assert.Equal("alpha", rows[2][2].AsText);
            Assert.Equal(-1, rows[2][3].AsInteger);
            Assert.Equal("beta", rows[2][6].AsText);
        }

        await using QueryResult nullTargetResult = await _database.ExecuteAsync(
            """
            SELECT id,
                   LAG(score, 1, 99) OVER (PARTITION BY department ORDER BY id) AS previous_score,
                   LEAD(score, 1, 99) OVER (PARTITION BY department ORDER BY id) AS next_score
            FROM scores
            WHERE department = 'B'
            ORDER BY id
            """,
            TestContext.Current.CancellationToken);

        List<DbValue[]> nullTargetRows =
            await nullTargetResult.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal([4L, 99L, 5L], ToIntegers(nullTargetRows[0]));
        Assert.Equal(5, nullTargetRows[1][0].AsInteger);
        Assert.True(nullTargetRows[1][1].IsNull);
        Assert.Equal(99, nullTargetRows[1][2].AsInteger);
    }

    [Fact]
    public async Task EmptyRowsFrames_ReturnSqlAggregateAndValueIdentities()
    {
        await using QueryResult result = await _database.ExecuteAsync(
            """
            SELECT id,
                   COUNT(score) OVER (
                       ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
                   ) AS prior_count,
                   SUM(score) OVER (
                       ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
                   ) AS prior_sum,
                   FIRST_VALUE(label) OVER (
                       ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
                   ) AS prior_first,
                   LAST_VALUE(label) OVER (
                       ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
                   ) AS prior_last
            FROM scores
            WHERE department = 'A'
            ORDER BY id
            """,
            TestContext.Current.CancellationToken);

        List<DbValue[]> rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(1, rows[0][0].AsInteger);
        Assert.Equal(0, rows[0][1].AsInteger);
        Assert.True(rows[0][2].IsNull);
        Assert.True(rows[0][3].IsNull);
        Assert.True(rows[0][4].IsNull);

        Assert.Equal([2L, 1L, 10L], ToIntegers(rows[1][..3]));
        Assert.Equal("alpha", rows[1][3].AsText);
        Assert.Equal("alpha", rows[1][4].AsText);
    }

    [Fact]
    public async Task BoundedRowsFrames_CoverAverageMinimumMaximumAndRanking()
    {
        await using QueryResult result = await _database.ExecuteAsync(
            """
            SELECT id,
                   AVG(score) OVER (
                       PARTITION BY department ORDER BY id
                       ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                   ) AS moving_average,
                   MIN(score) OVER (
                       PARTITION BY department ORDER BY id
                       ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                   ) AS moving_minimum,
                   MAX(score) OVER (
                       PARTITION BY department ORDER BY id
                       ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                   ) AS moving_maximum,
                   ROW_NUMBER() OVER (
                       PARTITION BY department ORDER BY id
                       ROWS BETWEEN CURRENT ROW AND CURRENT ROW
                   ) AS row_number
            FROM scores
            WHERE department = 'A'
            ORDER BY id
            """,
            TestContext.Current.CancellationToken);

        List<DbValue[]> rows = await result.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(10, rows[0][1].AsReal);
        Assert.Equal([10L, 10L, 1L], ToIntegers(rows[0][2..]));
        Assert.Equal(10, rows[1][1].AsReal);
        Assert.Equal([10L, 10L, 2L], ToIntegers(rows[1][2..]));
        Assert.Equal(15, rows[2][1].AsReal);
        Assert.Equal([10L, 20L, 3L], ToIntegers(rows[2][2..]));
    }

    [Theory]
    [InlineData("-1", "nonnegative")]
    [InlineData("1.5", "INTEGER")]
    [InlineData("NULL", "INTEGER")]
    public async Task LagAndLead_InvalidOffsetsFailPredictably(
        string offset,
        string expectedMessage)
    {
        CSharpDbException error = await AssertWindowFailureAsync(
            _database,
            $"SELECT LAG(score, {offset}) OVER (ORDER BY id) FROM scores",
            TestContext.Current.CancellationToken);

        Assert.Equal(ErrorCode.TypeMismatch, error.Code);
        Assert.Contains(expectedMessage, error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task NullOrderingAndPeers_AreDeterministic()
    {
        await _database.ExecuteAsync(
            "INSERT INTO scores VALUES (6, 'B', NULL, 'second-null')",
            TestContext.Current.CancellationToken);

        await using QueryResult ascendingResult = await _database.ExecuteAsync(
            """
            SELECT id,
                   RANK() OVER (PARTITION BY department ORDER BY score) AS ascending_rank,
                   COUNT(score) OVER (PARTITION BY department ORDER BY score) AS peer_count,
                   SUM(score) OVER (PARTITION BY department ORDER BY score) AS peer_sum
            FROM scores
            WHERE department = 'B'
            ORDER BY id
            """,
            TestContext.Current.CancellationToken);

        List<DbValue[]> ascendingRows =
            await ascendingResult.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal([4L, 1L, 0L], ToIntegers(ascendingRows[0][..3]));
        Assert.True(ascendingRows[0][3].IsNull);
        Assert.Equal([5L, 3L, 1L, 5L], ToIntegers(ascendingRows[1]));
        Assert.Equal([6L, 1L, 0L], ToIntegers(ascendingRows[2][..3]));
        Assert.True(ascendingRows[2][3].IsNull);

        await using QueryResult descendingResult = await _database.ExecuteAsync(
            """
            SELECT id,
                   RANK() OVER (PARTITION BY department ORDER BY score DESC) AS descending_rank
            FROM scores
            WHERE department = 'B'
            ORDER BY id
            """,
            TestContext.Current.CancellationToken);
        List<DbValue[]> descendingRows =
            await descendingResult.ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal([4L, 2L], ToIntegers(descendingRows[0]));
        Assert.Equal([5L, 1L], ToIntegers(descendingRows[1]));
        Assert.Equal([6L, 2L], ToIntegers(descendingRows[2]));
    }

    [Fact]
    public async Task IncompatibleOrderingGroups_RemainExplicitlyRejected()
    {
        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            async () =>
            {
                await using QueryResult result = await _database.ExecuteAsync(
                    """
                    SELECT ROW_NUMBER() OVER (ORDER BY id),
                           ROW_NUMBER() OVER (ORDER BY score)
                    FROM scores
                    """,
                    TestContext.Current.CancellationToken);
                await result.ToListAsync(TestContext.Current.CancellationToken);
            });

        Assert.Equal(ErrorCode.SyntaxError, error.Code);
        Assert.Contains("incompatible", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task NamedWindowView_WithFrameAndNavigation_RoundTripsAcrossReopen()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            """
            CREATE VIEW score_history AS
            SELECT id,
                   LAG(score, 1, -1) OVER ordered AS previous_score,
                   LAST_VALUE(label) OVER whole_partition AS final_label
            FROM scores
            WINDOW ordered AS (PARTITION BY department ORDER BY id),
                   whole_partition AS (
                       PARTITION BY department ORDER BY id
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                   )
            """,
            ct);

        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_dbPath, ct);

        await using QueryResult result = await _database.ExecuteAsync(
            "SELECT id, previous_score, final_label FROM score_history WHERE id <= 3 ORDER BY id",
            ct);
        List<DbValue[]> rows = await result.ToListAsync(ct);

        Assert.Equal([1L, -1L], ToIntegers(rows[0][..2]));
        Assert.Equal("beta", rows[0][2].AsText);
        Assert.Equal([2L, 10L], ToIntegers(rows[1][..2]));
        Assert.Equal("beta", rows[1][2].AsText);
        Assert.Equal([3L, 10L], ToIntegers(rows[2][..2]));
        Assert.Equal("beta", rows[2][2].AsText);
    }

    [Fact]
    public async Task PartitionAndStageLimits_FailWithStableResourceDiagnostics()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database partitionLimited = await Database.OpenInMemoryAsync(
            new DatabaseOptions
            {
                WindowExecution = new WindowExecutionOptions
                {
                    MaxPartitionRows = 3,
                    MaxBufferedRows = 10,
                },
            },
            ct);
        await partitionLimited.ExecuteAsync(
            "CREATE TABLE values_by_group (id INTEGER PRIMARY KEY, group_id INTEGER)",
            ct);
        await partitionLimited.ExecuteAsync(
            "INSERT INTO values_by_group VALUES (1, 1), (2, 1), (3, 1)",
            ct);

        await using (QueryResult atLimit = await partitionLimited.ExecuteAsync(
            "SELECT ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY id) FROM values_by_group",
            ct))
        {
            Assert.Equal(3, (await atLimit.ToListAsync(ct)).Count);
        }

        await partitionLimited.ExecuteAsync(
            "INSERT INTO values_by_group VALUES (4, 1)",
            ct);
        CSharpDbException partitionError = await AssertWindowFailureAsync(
            partitionLimited,
            "SELECT ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY id) FROM values_by_group",
            ct);
        Assert.Equal(ErrorCode.ResourceLimitExceeded, partitionError.Code);
        Assert.Contains("partition", partitionError.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("3", partitionError.Message, StringComparison.Ordinal);

        await using Database stageLimited = await Database.OpenInMemoryAsync(
            new DatabaseOptions
            {
                WindowExecution = new WindowExecutionOptions
                {
                    MaxPartitionRows = 2,
                    MaxBufferedRows = 3,
                },
            },
            ct);
        await stageLimited.ExecuteAsync(
            "CREATE TABLE separate_groups (id INTEGER PRIMARY KEY, group_id INTEGER)",
            ct);
        await stageLimited.ExecuteAsync(
            "INSERT INTO separate_groups VALUES (1, 1), (2, 2), (3, 3), (4, 4)",
            ct);

        CSharpDbException stageError = await AssertWindowFailureAsync(
            stageLimited,
            "SELECT ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY id) FROM separate_groups",
            ct);
        Assert.Equal(ErrorCode.ResourceLimitExceeded, stageError.Code);
        Assert.Contains("stage", stageError.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("3", stageError.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0, 10)]
    [InlineData(5, 0)]
    [InlineData(6, 5)]
    public async Task InvalidWindowLimits_AreRejectedBeforeOpening(
        int maxPartitionRows,
        int maxBufferedRows)
    {
        var options = new DatabaseOptions
        {
            WindowExecution = new WindowExecutionOptions
            {
                MaxPartitionRows = maxPartitionRows,
                MaxBufferedRows = maxBufferedRows,
            },
        };

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => Database.OpenInMemoryAsync(
                options,
                TestContext.Current.CancellationToken).AsTask());
    }

    private static async Task<CSharpDbException> AssertWindowFailureAsync(
        Database database,
        string sql,
        CancellationToken ct) =>
        await Assert.ThrowsAsync<CSharpDbException>(
            async () =>
            {
                await using QueryResult result = await database.ExecuteAsync(sql, ct);
                await result.ToListAsync(ct);
            });

    private static long[] ToIntegers(IEnumerable<DbValue> values) =>
        values.Select(value => value.AsInteger).ToArray();

    private static void DeleteDatabaseFiles(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
        if (File.Exists(path + ".wal"))
            File.Delete(path + ".wal");
    }
}
