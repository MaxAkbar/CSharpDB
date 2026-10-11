using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class MemoryMappedPointLookupTests
{
    private const string Label = "unchanged base-file row";
    private static readonly byte[] Payload = [0, 1, 127, 128, 254, 255];
    private static readonly TimeSpan OperationTimeout = TimeSpan.FromSeconds(10);

    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PointLookup_PreservesBasePageAcrossConcurrentCheckpointGrowth(bool mapped)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = CreatePath();
        var interceptor = new PausedPageReadInterceptor();
        DatabaseOptions options = CreateOptions(mapped, interceptor);
        DbValue[] retainedRow = [];
        try
        {
            await SeedAndCloseAsync(path, options, ct);
            // Reopen so the unchanged table leaf is not left in the writer's owned cache.
            await using (var db = await Database.OpenAsync(path, options, ct))
            {
                Pager pager = GetField<Pager>(db, "_pager");
                uint root = GetField<SchemaCatalog>(db, "_catalog").GetTableRootPage("a");
                long baseLength = new FileInfo(path).Length;
                await GrowUnrelatedTableAsync(db, ct);
                AssertGrowthIsStillInWal(pager, path, baseLength);

                // Capturing after growth lets checkpoint copy every pending WAL target,
                // while table a still depends on the unchanged main-file leaf.
                using (var reader = db.CreateReaderSession())
                {
                    AssertBaseFileSnapshot(GetField<WalSnapshot>(reader, "_snapshot"), root);
                    interceptor.Arm(root);
                    retainedRow = await RunAcrossCheckpointAsync(
                        db, path, baseLength, interceptor, mapped,
                        async operationCt =>
                        {
                            await using var result = await reader.ExecuteReadAsync(
                                "SELECT * FROM a WHERE id = 1", operationCt);
                            return Assert.Single(await result.ToListAsync(operationCt));
                        }, ct);
                    AssertOriginalRow(retainedRow);
                }

                await db.CheckpointAsync(ct);
                Assert.False(GetField<IWriteAheadLog>(pager, "_wal").HasPendingCheckpoint);
            }

            // These values must remain owned after both the snapshot and mapping are disposed.
            AssertOriginalRow(retainedRow);
        }
        finally { DeleteFiles(path); }
    }

    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExplicitTransactionClone_PreservesBasePageAcrossConcurrentCheckpointGrowth(bool mapped)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = CreatePath();
        var interceptor = new PausedPageReadInterceptor();
        DatabaseOptions options = CreateOptions(mapped, interceptor);
        try
        {
            uint root = await SeedAndCloseAsync(path, options, ct);
            // Read the expected bytes while the database is closed; file sharing differs
            // between Windows and Unix, and this must not warm the pager's cache.
            byte[] expectedPage = await ReadBasePageAsync(path, root, ct);
            await using var db = await Database.OpenAsync(path, options, ct);
            Pager pager = GetField<Pager>(db, "_pager");
            Assert.Equal(root, GetField<SchemaCatalog>(db, "_catalog").GetTableRootPage("a"));
            long baseLength = new FileInfo(path).Length;
            await GrowUnrelatedTableAsync(db, ct);
            AssertGrowthIsStillInWal(pager, path, baseLength);

            await using (var transaction = await pager.BeginWriteTransactionAsync(ct))
            {
                AssertBaseFileSnapshot(transaction.State.Snapshot, root);
                interceptor.Arm(root);
                byte[] clonedPage = await RunAcrossCheckpointAsync(
                    db, path, baseLength, interceptor, mapped,
                    async operationCt =>
                    {
                        // Binding only this task leaves manual checkpoint outside the transaction.
                        using var binding = transaction.Bind();
                        return await pager.GetPageAsync(root, operationCt);
                    }, ct);

                Assert.Equal(expectedPage, clonedPage);
                Assert.Same(clonedPage, transaction.State.ModifiedPages[root]);
                await transaction.RollbackAsync(ct);
                Assert.Equal(expectedPage, clonedPage);
            }

            await db.CheckpointAsync(ct);
            Assert.False(GetField<IWriteAheadLog>(pager, "_wal").HasPendingCheckpoint);
        }
        finally { DeleteFiles(path); }
    }

    private static async Task<T> RunAcrossCheckpointAsync<T>(
        Database db,
        string path,
        long baseLength,
        PausedPageReadInterceptor interceptor,
        bool mapped,
        Func<CancellationToken, Task<T>> operation,
        CancellationToken ct)
    {
        using var operationCancellation = CancellationTokenSource.CreateLinkedTokenSource(ct);
        operationCancellation.CancelAfter(TimeSpan.FromSeconds(20));
        Task<T>? pendingRead = null;
        try
        {
            // A cache-only interceptor callback may synchronously wait; give the read its
            // own task so the test can always release the gate in its finally block.
            pendingRead = Task.Run(() => operation(operationCancellation.Token), operationCancellation.Token);
            PageReadSource source = await interceptor.Reached.WaitAsync(OperationTimeout, ct);
            // The gate is based on page identity, not buffer ownership: an owned-copy fix
            // must still run through exactly this checkpoint interleaving.
            Assert.True(source == PageReadSource.StorageDevice ||
                (mapped && source == PageReadSource.MemoryMappedMainFile),
                $"Expected a cold main-file read (mapped={mapped}), but observed {source}.");

            await db.CheckpointAsync(operationCancellation.Token);
            IWriteAheadLog wal = GetField<IWriteAheadLog>(GetField<Pager>(db, "_pager"), "_wal");
            Assert.True(wal.HasPendingCheckpoint);
            Assert.True(wal.IsCheckpointCopyComplete);
            Assert.True(new FileInfo(path).Length > baseLength,
                "Checkpoint must grow the main file while the page read is paused.");

            interceptor.Release();
            return await pendingRead.WaitAsync(OperationTimeout, ct);
        }
        finally
        {
            interceptor.Release();
            if (pendingRead is not null)
            {
                if (!pendingRead.IsCompleted)
                    operationCancellation.Cancel();
                try { await pendingRead.WaitAsync(OperationTimeout); }
                catch when (pendingRead.IsCompleted)
                {
                    // Observe any failure before disposing the reader/transaction/database;
                    // preserve the original test failure rather than replacing it here.
                }
            }
        }
    }

    private static DatabaseOptions CreateOptions(bool mapped, IPageOperationInterceptor interceptor) => new()
    {
        StorageEngineOptions = new StorageEngineOptions
        {
            PagerOptions = new PagerOptions
            {
                UseMemoryMappedReads = mapped,
                CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
                Interceptors = [interceptor],
            },
        },
    };

    private static async Task<uint> SeedAndCloseAsync(string path, DatabaseOptions options, CancellationToken ct)
    {
        await using var db = await Database.OpenAsync(path, options, ct);
        await ExecuteAsync(db, "CREATE TABLE a (id INTEGER PRIMARY KEY, label TEXT, payload BLOB)", ct);
        await ExecuteAsync(db, $"INSERT INTO a VALUES (1, '{Label}', X'{Convert.ToHexString(Payload)}')", ct);
        await ExecuteAsync(db, "CREATE TABLE b (id INTEGER PRIMARY KEY, padding TEXT)", ct);
        await db.CheckpointAsync(ct);
        return GetField<SchemaCatalog>(db, "_catalog").GetTableRootPage("a");
    }

    private static async Task GrowUnrelatedTableAsync(Database db, CancellationToken ct)
    {
        await db.BeginTransactionAsync(ct);
        string padding = new('x', 512);
        for (int id = 0; id < 64; id++)
            await ExecuteAsync(db, $"INSERT INTO b VALUES ({id}, '{padding}')", ct);
        await db.CommitAsync(ct);
    }

    private static void AssertGrowthIsStillInWal(Pager pager, string path, long baseLength)
    {
        Assert.Equal(baseLength, new FileInfo(path).Length);
        Assert.True((long)pager.PageCount * PageConstants.PageSize > baseLength);
    }

    private static void AssertBaseFileSnapshot(WalSnapshot snapshot, uint root)
    {
        Assert.True(snapshot.HasWalFrames);
        Assert.False(snapshot.TryGet(root, out _));
    }

    private static async Task<byte[]> ReadBasePageAsync(string path, uint pageId, CancellationToken ct)
    {
        await using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        file.Position = (long)pageId * PageConstants.PageSize;
        byte[] page = new byte[PageConstants.PageSize];
        await file.ReadExactlyAsync(page, ct);
        return page;
    }

    private static async Task ExecuteAsync(Database db, string sql, CancellationToken ct)
    {
        await using var result = await db.ExecuteAsync(sql, ct);
    }

    private static void AssertOriginalRow(DbValue[] row)
    {
        Assert.Equal(3, row.Length);
        Assert.Equal(1L, row[0].AsInteger);
        Assert.Equal(Label, row[1].AsText);
        Assert.Equal(Payload, row[2].AsBlob);
    }

    private static T GetField<T>(object instance, string name)
        => Assert.IsAssignableFrom<T>(instance.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(instance));

    private static string CreatePath()
        => Path.Combine(Path.GetTempPath(), $"csharpdb_mapped_point_{Guid.NewGuid():N}.db");

    private static void DeleteFiles(string path)
    {
        foreach (string file in new[] { path, path + ".wal", path + ".lock" })
            if (File.Exists(file)) File.Delete(file);
    }

    private sealed class PausedPageReadInterceptor : IPageOperationInterceptor
    {
        private readonly TaskCompletionSource<PageReadSource> _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private uint _pageId;
        private int _armed;

        public Task<PageReadSource> Reached => _reached.Task;

        public void Arm(uint pageId)
        {
            _pageId = pageId;
            Volatile.Write(ref _armed, 1);
        }

        public void Release() => _release.TrySetResult();

        public async ValueTask OnAfterReadAsync(uint pageId, PageReadSource source, CancellationToken ct = default)
        {
            if (pageId != _pageId || Interlocked.CompareExchange(ref _armed, 0, 1) != 1)
                return;
            _reached.TrySetResult(source);
            await _release.Task.WaitAsync(ct);
        }

        public ValueTask OnBeforeReadAsync(uint pageId, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnBeforeWriteAsync(uint pageId, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnAfterWriteAsync(uint pageId, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCommitStartAsync(int dirtyPageCount, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCommitEndAsync(int dirtyPageCount, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCheckpointStartAsync(int committedFrameCount, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCheckpointEndAsync(int committedFrameCount, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnRecoveryStartAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnRecoveryEndAsync(bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
    }
}
