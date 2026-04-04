using System.Threading;
using CSharpDB.Client.Internal;
using CSharpDB.Engine;

namespace CSharpDB.Tests;

public sealed class EngineTransportClientTests
{
    [Fact]
    public async Task ReleaseCachedDatabaseAsync_CancellationKeepsPendingOpenCached()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_{Guid.NewGuid():N}.db");
        var openEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowOpen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int openCount = 0;

        try
        {
            await using (var client = new EngineTransportClient(
                             dbPath,
                             async (path, ct) =>
                             {
                                 Interlocked.Increment(ref openCount);
                                 openEntered.TrySetResult();
                                 await allowOpen.Task;
                                 return await Database.OpenAsync(path, ct);
                             }))
            {
                Task<IReadOnlyList<string>> initialRequest = client.GetTableNamesAsync(CancellationToken.None);
                await openEntered.Task.WaitAsync(TestContext.Current.CancellationToken);

                using var cts = new CancellationTokenSource();
                Task releaseTask = client.ReleaseCachedDatabaseAsync(cts.Token).AsTask();
                cts.Cancel();

                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => releaseTask);

                allowOpen.TrySetResult();
                var tables = await initialRequest;
                Assert.Empty(tables);

                Database? cached = await client.TryGetDatabaseAsync(TestContext.Current.CancellationToken);
                Assert.NotNull(cached);
                Assert.Equal(1, Volatile.Read(ref openCount));
            }
        }
        finally
        {
            allowOpen.TrySetResult();

            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task ReleaseCachedDatabaseAsync_BlocksNewGetsUntilOldCachedDatabaseIsDisposed()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_{Guid.NewGuid():N}.db");
        var firstOpenEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowFirstOpen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int openCount = 0;

        try
        {
            await using var client = new EngineTransportClient(
                dbPath,
                async (path, ct) =>
                {
                    int sequence = Interlocked.Increment(ref openCount);
                    if (sequence == 1)
                    {
                        firstOpenEntered.TrySetResult();
                        await allowFirstOpen.Task;
                    }

                    return await Database.OpenAsync(path, ct);
                });

            _ = client.TryGetDatabaseAsync(CancellationToken.None);
            await firstOpenEntered.Task.WaitAsync(TestContext.Current.CancellationToken);

            Task releaseTask = client.ReleaseCachedDatabaseAsync(TestContext.Current.CancellationToken).AsTask();
            await Task.Delay(50, TestContext.Current.CancellationToken);

            Task<Database?> secondGetTask = client.TryGetDatabaseAsync(TestContext.Current.CancellationToken).AsTask();
            await Task.Delay(50, TestContext.Current.CancellationToken);

            Assert.Equal(1, Volatile.Read(ref openCount));
            Assert.False(secondGetTask.IsCompleted);

            allowFirstOpen.TrySetResult();
            await releaseTask;

            Database? reopened = await secondGetTask;
            Assert.NotNull(reopened);
            Assert.Equal(2, Volatile.Read(ref openCount));
        }
        finally
        {
            allowFirstOpen.TrySetResult();

            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task GetDatabaseAsync_WhenReleaseStartsAfterWaitBegins_RetriesInsteadOfReturningDisposedDatabase()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_{Guid.NewGuid():N}.db");
        var firstOpenEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowFirstOpen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int openCount = 0;

        try
        {
            await using var client = new EngineTransportClient(
                dbPath,
                async (path, ct) =>
                {
                    int sequence = Interlocked.Increment(ref openCount);
                    if (sequence == 1)
                    {
                        firstOpenEntered.TrySetResult();
                        await allowFirstOpen.Task;
                    }

                    return await Database.OpenAsync(path, ct);
                });

            Task<Database?> firstGetTask = client.TryGetDatabaseAsync(CancellationToken.None).AsTask();
            await firstOpenEntered.Task.WaitAsync(TestContext.Current.CancellationToken);

            Task<Database?> secondGetTask = client.TryGetDatabaseAsync(CancellationToken.None).AsTask();
            Task releaseTask = client.ReleaseCachedDatabaseAsync(TestContext.Current.CancellationToken).AsTask();

            allowFirstOpen.TrySetResult();
            await releaseTask;

            Database? firstResult = await firstGetTask;
            Database? secondResult = await secondGetTask;

            Assert.NotNull(firstResult);
            Assert.NotNull(secondResult);
            Assert.Equal(2, Volatile.Read(ref openCount));
        }
        finally
        {
            allowFirstOpen.TrySetResult();

            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task GetRowByPkAsync_UsesTargetedLookupForExistingAndMissingRows()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = new EngineTransportClient(dbPath);
            await client.ExecuteSqlAsync(
                """
                CREATE TABLE Users (
                    Id INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL
                );
                INSERT INTO Users VALUES (1, 'Ada');
                INSERT INTO Users VALUES (2, 'Grace');
                INSERT INTO Users VALUES (3, 'Linus');
                """,
                TestContext.Current.CancellationToken);

            Dictionary<string, object?>? first = await client.GetRowByPkAsync("Users", "Id", 1L, TestContext.Current.CancellationToken);
            Dictionary<string, object?>? middle = await client.GetRowByPkAsync("Users", "Id", 2L, TestContext.Current.CancellationToken);
            Dictionary<string, object?>? last = await client.GetRowByPkAsync("Users", "Id", 3L, TestContext.Current.CancellationToken);
            Dictionary<string, object?>? missing = await client.GetRowByPkAsync("Users", "Id", 99L, TestContext.Current.CancellationToken);

            Assert.NotNull(first);
            Assert.Equal("Ada", first!["Name"]);
            Assert.NotNull(middle);
            Assert.Equal("Grace", middle!["Name"]);
            Assert.NotNull(last);
            Assert.Equal("Linus", last!["Name"]);
            Assert.Null(missing);
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }
}
