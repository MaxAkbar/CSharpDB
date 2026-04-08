using CSharpDB.Engine;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class DatabaseConcurrencyTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private Database _db = null!;

    public DatabaseConcurrencyTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_concurrency_test_{Guid.NewGuid():N}.db");
    }

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

    [Fact]
    public async Task ConcurrentAutoCommitSqlWrites_OnSharedDatabase_ProduceExpectedRowCount()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, writer INTEGER)", ct);

        const int writerCount = 8;
        const int insertsPerWriter = 64;
        await RunConcurrentWritersAsync(
            writerCount,
            async writerId =>
            {
                for (int i = 0; i < insertsPerWriter; i++)
                {
                    int id = (writerId * insertsPerWriter) + i + 1;
                    await _db.ExecuteAsync($"INSERT INTO bench VALUES ({id}, {writerId})", ct);
                }
            },
            ct);

        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM bench", ct);
        var rows = await result.ToListAsync(ct);
        Assert.Equal(writerCount * insertsPerWriter, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task ConcurrentAutoCommitCollectionWrites_OnSharedDatabase_ProduceExpectedRowCount()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<UserDocument>("users", ct);

        const int writerCount = 8;
        const int documentsPerWriter = 48;
        await RunConcurrentWritersAsync(
            writerCount,
            async writerId =>
            {
                for (int i = 0; i < documentsPerWriter; i++)
                {
                    int id = (writerId * documentsPerWriter) + i;
                    await users.PutAsync(
                        $"user:{id}",
                        new UserDocument($"User{id}", 20 + (id % 50)),
                        ct);
                }
            },
            ct);

        Assert.Equal(writerCount * documentsPerWriter, await users.CountAsync(ct));
        Assert.NotNull(await users.GetAsync("user:0", ct));
        Assert.NotNull(await users.GetAsync($"user:{(writerCount * documentsPerWriter) - 1}", ct));
    }

    [Fact]
    public async Task ConcurrentExplicitWriteTransactions_WithRetry_ProduceExpectedRowCount()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE tx_bench (id INTEGER PRIMARY KEY, writer INTEGER)", ct);

        const int writerCount = 4;
        const int insertsPerWriter = 12;
        await RunConcurrentWritersAsync(
            writerCount,
            writerId => _db.RunWriteTransactionAsync(
                async (tx, innerCt) =>
                {
                    for (int i = 0; i < insertsPerWriter; i++)
                    {
                        int id = (writerId * insertsPerWriter) + i + 1;
                        await tx.ExecuteAsync($"INSERT INTO tx_bench VALUES ({id}, {writerId})", innerCt);
                    }
                },
                ct: ct).AsTask(),
            ct);

        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM tx_bench", ct);
        var rows = await result.ToListAsync(ct);
        Assert.Equal(writerCount * insertsPerWriter, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task ConcurrentExplicitWriteTransactions_WithoutRetry_SurfaceConflict()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE conflict_bench (id INTEGER PRIMARY KEY, writer INTEGER)", ct);

        await using var tx1 = await _db.BeginWriteTransactionAsync(ct);
        await using var tx2 = await _db.BeginWriteTransactionAsync(ct);

        await tx1.ExecuteAsync("INSERT INTO conflict_bench VALUES (1, 1)", ct);
        await tx2.ExecuteAsync("INSERT INTO conflict_bench VALUES (2, 2)", ct);

        await tx1.CommitAsync(ct);
        await Assert.ThrowsAsync<CSharpDbConflictException>(() => tx2.CommitAsync(ct).AsTask());

        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM conflict_bench", ct);
        var rows = await result.ToListAsync(ct);
        Assert.Equal(1, rows[0][0].AsInteger);
    }

    private static async Task RunConcurrentWritersAsync(
        int writerCount,
        Func<int, Task> writerAction,
        CancellationToken ct)
    {
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task[] writers = Enumerable.Range(0, writerCount)
            .Select(writerId => Task.Run(
                async () =>
                {
                    await start.Task.WaitAsync(ct);
                    await writerAction(writerId);
                },
                ct))
            .ToArray();

        start.SetResult();
        await Task.WhenAll(writers);
    }

    private sealed record UserDocument(string Name, int Age);
}
