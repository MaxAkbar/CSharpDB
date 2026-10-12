using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Tests;

public sealed class ReaderCatalogWriteAdmissionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory(Timeout = 30_000)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task WarmReadersDuringNextLegacyWrite_KeepCommittedCountWithoutScanning(bool fileBacked, bool commit)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_reader_admission_{Guid.NewGuid():N}.db");
        try
        {
            await using var db = fileBacked
                ? await Database.OpenAsync(path, ct)
                : await Database.OpenInMemoryAsync(ct);
            await SeedAsync(db);
            using (var warm = db.CreateReaderSession())
                await AssertCountAsync(warm, 128);
            // Advance the committed pages without opening another reader. The cached
            // catalog must be refreshed before the next writer can mutate live metadata.
            await db.ExecuteAsync("INSERT INTO t VALUES (128, 'committed')", Ct);
            await db.BeginTransactionAsync(Ct);
            try
            {
                await db.ExecuteAsync("ALTER TABLE t ADD COLUMN pending TEXT", Ct);
                await db.ExecuteAsync("INSERT INTO t VALUES (129, 'pending', 'uncommitted')", Ct);
                using var reader = db.CreateReaderSession();
                AssertMetadataCount(reader, 129);
                await AssertCountAsync(reader, 129);
                Assert.Null(Field<Pager?>(reader, "_snapshotPager"));
                if (commit)
                    await db.CommitAsync(Ct);
                else
                    await db.RollbackAsync(Ct);
                await AssertCountAsync(reader, 129);
                await using var rows = await reader.ExecuteReadAsync("SELECT * FROM t WHERE id = 128", Ct);
                Assert.Equal(new[] { "id", "label" }, rows.Schema.Select(column => column.Name));
                Assert.Equal("committed", Assert.Single(await rows.ToListAsync(Ct))[1].AsText);
            }
            finally
            {
                if (Field<bool>(db, "_inTransaction")) await db.RollbackAsync(Ct);
            }
            using var latest = db.CreateReaderSession();
            await AssertCountAsync(latest, commit ? 130 : 129);
        }
        finally
        {
            foreach (string file in new[] { path, path + ".wal", path + ".lock" })
                if (File.Exists(file)) File.Delete(file);
        }
    }

    [Fact(Timeout = 30_000)]
    public async Task WarmReadersDuringImplicitInsert_KeepCommittedCountWithoutScanning()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var interceptor = new PausedCommitInterceptor();
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions { Interceptors = [interceptor] },
            },
        };
        await using var db = await Database.OpenInMemoryAsync(options, ct);
        await SeedAsync(db);
        using (var warm = db.CreateReaderSession()) await AssertCountAsync(warm, 128);
        await db.ExecuteAsync("INSERT INTO t VALUES (128, 'committed')", Ct);
        interceptor.Arm();
        Task writer = db.ExecuteAsync("INSERT INTO t VALUES (129, 'pending')", Ct).AsTask();
        try
        {
            await interceptor.Started.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);
            using var reader = db.CreateReaderSession();
            AssertMetadataCount(reader, 129);
            await AssertCountAsync(reader, 129);
            Assert.Null(Field<Pager?>(reader, "_snapshotPager"));
            interceptor.Release.TrySetResult();
            await writer.WaitAsync(TimeSpan.FromSeconds(10), Ct);
            await AssertCountAsync(reader, 129);
        }
        finally
        {
            interceptor.Release.TrySetResult();
            await writer.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        }
        using var latest = db.CreateReaderSession();
        await AssertCountAsync(latest, 130);
    }

    [Fact(Timeout = 30_000)]
    public async Task CancelledWriteAdmission_ReleasesNoUnownedGates_AndNextWriteProgresses()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        await SeedAsync(db);
        using (var warm = db.CreateReaderSession()) await AssertCountAsync(warm, 128);
        var writeGate = Field<SemaphoreSlim>(db, "_writeOperationGate");
        await writeGate.WaitAsync(ct);
        using var cancelled = CancellationTokenSource.CreateLinkedTokenSource(ct);
        try
        {
            Task begin = db.BeginTransactionAsync(cancelled.Token).AsTask();
            Assert.False(begin.IsCompleted);
            cancelled.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => begin);
            Assert.Equal(0, writeGate.CurrentCount);
        }
        finally { writeGate.Release(); }
        await db.ExecuteAsync("INSERT INTO t VALUES (128, 'after cancellation')", ct);
        Assert.Equal(1, writeGate.CurrentCount);
        Assert.Equal(1, Field<SemaphoreSlim>(db, "_sharedStateGate").CurrentCount);
        Assert.Equal(0, db.ActiveReaderCount);
        using var latest = db.CreateReaderSession();
        await AssertCountAsync(latest, 129);
    }

    [Fact(Timeout = 30_000)]
    public async Task BusySharedCatalog_FallsBackWithoutWaiting_AndCancelledLoadCanRetry()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        await SeedAsync(db);
        using (var warm = db.CreateReaderSession()) await AssertCountAsync(warm, 128);
        await db.ExecuteAsync("INSERT INTO t VALUES (128, 'committed')", ct);
        var sharedGate = Field<SemaphoreSlim>(db, "_sharedStateGate");
        await sharedGate.WaitAsync(ct);
        try
        {
            // Capture must skip a busy shared gate, even while it owns the writer gate.
            await db.BeginTransactionAsync(ct).AsTask().WaitAsync(TimeSpan.FromSeconds(10), ct);
            await db.ExecuteAsync("INSERT INTO t VALUES (129, 'pending')", ct);
            using var reader = db.CreateReaderSession();
            var state = Field<Database.ReaderCatalogState>(reader, "_catalogState");
            Assert.Null(state.Catalog);
            await state.LoadGate.WaitAsync(ct);
            using var cancelled = CancellationTokenSource.CreateLinkedTokenSource(ct);
            try
            {
                Task query = reader.ExecuteReadAsync("SELECT COUNT(*) FROM t", cancelled.Token).AsTask();
                Assert.False(query.IsCompleted);
                cancelled.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => query);
                Assert.Equal(0, state.LoadGate.CurrentCount);
            }
            finally { state.LoadGate.Release(); }
            await AssertCountAsync(reader, 129);
            Assert.Equal(1, state.LoadGate.CurrentCount);
            await db.RollbackAsync(ct);
        }
        finally
        {
            sharedGate.Release();
            if (Field<bool>(db, "_inTransaction")) await db.RollbackAsync(ct);
        }
        await db.ExecuteAsync("INSERT INTO t VALUES (129, 'after fallback')", ct);
        Assert.Equal(0, db.ActiveReaderCount);
        using var latest = db.CreateReaderSession();
        await AssertCountAsync(latest, 130);
    }

    private static async Task SeedAsync(Database db)
    {
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)", Ct);
        await db.BeginTransactionAsync(Ct);
        for (int i = 0; i < 128; i++) await db.ExecuteAsync($"INSERT INTO t VALUES ({i}, 'original')", Ct);
        await db.CommitAsync(Ct);
    }

    [Fact(Timeout = 30_000)]
    public async Task ReadersAfterPublication_RecaptureExactCountForTheSameCommittedPages()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var interceptor = new PausedCommitInterceptor(atEnd: true);
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions { Interceptors = [interceptor] },
            },
        };
        await using var db = await Database.OpenInMemoryAsync(options, ct);
        await SeedAsync(db);
        using (var warm = db.CreateReaderSession()) await AssertCountAsync(warm, 128);
        interceptor.Arm();
        Task writer = db.ExecuteAsync("INSERT INTO t VALUES (128, 'published')", ct).AsTask();
        try
        {
            await interceptor.Started.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
            using var during = db.CreateReaderSession();
            var fallback = Field<Database.ReaderCatalogState>(during, "_catalogState");
            Assert.Null(fallback.Catalog);
            await AssertCountAsync(during, 129);
            Assert.False(fallback.Catalog!.TryGetExactTableRowCount("t", out _));
            interceptor.Release.TrySetResult();
            await writer.WaitAsync(TimeSpan.FromSeconds(10), ct);
            using var after = db.CreateReaderSession();
            var captured = Field<Database.ReaderCatalogState>(after, "_catalogState");
            Assert.True(fallback.Snapshot.SharesPageState(captured.Snapshot));
            AssertMetadataCount(after, 129);
            await AssertCountAsync(after, 129);
            Assert.Null(Field<Pager?>(after, "_snapshotPager"));
            // Recapture must not mutate a fallback still used by an older reader.
            Assert.Same(fallback, Field<Database.ReaderCatalogState>(during, "_catalogState"));
            await db.ExecuteAsync("INSERT INTO t VALUES (129, 'later')", ct);
            await AssertCountAsync(during, 129);
            await AssertCountAsync(after, 129);
        }
        finally
        {
            interceptor.Release.TrySetResult();
            await writer.WaitAsync(TimeSpan.FromSeconds(10), ct);
        }
    }

    [Fact(Timeout = 30_000)]
    public async Task FailedNativePublication_DoesNotRecaptureUnsynchronizedCounts_AndWritesProgress()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var interceptor = new PausedCommitInterceptor(atEnd: true, failAtEnd: true);
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions { Interceptors = [interceptor] },
            },
        };
        await using var db = await Database.OpenInMemoryAsync(options, ct);
        await SeedAsync(db);
        using (var warm = db.CreateReaderSession()) await AssertCountAsync(warm, 128);
        await using (var writer = await db.BeginWriteTransactionAsync(ct))
        {
            await writer.ExecuteAsync("INSERT INTO t VALUES (128, 'durably published')", ct);
            interceptor.Arm();
            await Assert.ThrowsAsync<IOException>(() => writer.CommitAsync(ct).AsTask());
        }
        // The data was published, but the failed completion bypassed shared-count sync.
        // Both recapture paths must retain the conservative, snapshot-backed fallback.
        using (var reader = db.CreateReaderSession())
        {
            Assert.Null(Field<Database.ReaderCatalogState>(reader, "_catalogState").Catalog);
            await AssertCountAsync(reader, 129);
        }
        await db.ExecuteAsync("INSERT INTO t VALUES (129, 'after failure')", ct);
        Assert.Equal(0, db.ActiveReaderCount);
        Assert.Equal(1, Field<SemaphoreSlim>(db, "_writeOperationGate").CurrentCount);
        Assert.Equal(1, Field<SemaphoreSlim>(db, "_sharedStateGate").CurrentCount);
        using var latest = db.CreateReaderSession();
        Assert.Null(Field<Database.ReaderCatalogState>(latest, "_catalogState").Catalog);
        await AssertCountAsync(latest, 130);
    }

    private static void AssertMetadataCount(Database.ReaderSession reader, long expected)
    {
        var state = Field<Database.ReaderCatalogState>(reader, "_catalogState");
        Assert.NotNull(state.Catalog);
        Assert.True(state.Catalog.TryGetExactTableRowCount("t", out long count));
        Assert.Equal(expected, count);
    }

    private static async Task AssertCountAsync(Database.ReaderSession reader, long expected)
    {
        await using var result = await reader.ExecuteReadAsync("SELECT COUNT(*) FROM t", Ct);
        Assert.Equal(expected, Assert.Single(Assert.Single(await result.ToListAsync(Ct))).AsInteger);
    }

    private static T Field<T>(object target, string name)
        => (T)target.GetType().GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(target)!;

    private sealed class PausedCommitInterceptor(bool atEnd = false, bool failAtEnd = false) : IPageOperationInterceptor
    {
        internal readonly TaskCompletionSource Started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private bool _armed;
        internal void Arm() => _armed = true;
        public ValueTask OnCommitStartAsync(int dirtyPageCount, CancellationToken ct = default)
        {
            if (atEnd || !_armed) return ValueTask.CompletedTask;
            Started.TrySetResult();
            return new(Release.Task.WaitAsync(ct));
        }
        public ValueTask OnBeforeReadAsync(uint pageId, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnAfterReadAsync(uint pageId, PageReadSource source, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnBeforeWriteAsync(uint pageId, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnAfterWriteAsync(uint pageId, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCommitEndAsync(int dirtyPageCount, bool succeeded, CancellationToken ct = default)
        {
            if (!atEnd || !_armed || !succeeded) return ValueTask.CompletedTask;
            if (failAtEnd)
            {
                _armed = false;
                throw new IOException("Injected failure after durable publication and before catalog sync.");
            }
            Started.TrySetResult();
            return new(Release.Task.WaitAsync(ct));
        }
        public ValueTask OnCheckpointStartAsync(int committedFrameCount, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCheckpointEndAsync(int committedFrameCount, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnRecoveryStartAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnRecoveryEndAsync(bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
    }
}
