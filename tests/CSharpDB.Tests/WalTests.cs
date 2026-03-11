using CSharpDB.Core;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Device;
using CSharpDB.Engine;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public class WalTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private Database _db = null!;

    public WalTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_test_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        _db = await Database.OpenAsync(_dbPath, ct);
    }

    public async ValueTask DisposeAsync()
    {
        await _db.DisposeAsync();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal")) File.Delete(_dbPath + ".wal");
    }

    [Fact]
    public async Task Commit_PersistsThroughWal()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'hello')", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'world')", ct);

        await using var result = await _db.ExecuteAsync("SELECT * FROM t ORDER BY id", ct);
        var rows = await result.ToListAsync(ct);
        Assert.Equal(2, rows.Count);
        Assert.Equal("hello", rows[0][1].AsText);
        Assert.Equal("world", rows[1][1].AsText);
    }

    [Fact]
    public async Task Rollback_DiscardsChanges()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'keep')", ct);

        await _db.BeginTransactionAsync(ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'discard')", ct);
        await _db.RollbackAsync(ct);

        await using var result = await _db.ExecuteAsync("SELECT * FROM t", ct);
        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal("keep", rows[0][1].AsText);
    }

    [Fact]
    public async Task CrashRecovery_CommittedDataSurvives()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'survived')", ct);

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        await using var result = await _db.ExecuteAsync("SELECT * FROM t", ct);
        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal("survived", rows[0][1].AsText);
    }

    [Fact]
    public async Task CrashRecovery_UncommittedDataLost()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'committed')", ct);

        // Start a transaction but don't commit — dispose will rollback
        await _db.BeginTransactionAsync(ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'uncommitted')", ct);
        await _db.DisposeAsync();

        _db = await Database.OpenAsync(_dbPath, ct);

        await using var result = await _db.ExecuteAsync("SELECT * FROM t", ct);
        var rows = await result.ToListAsync(ct);
        Assert.Single(rows);
        Assert.Equal("committed", rows[0][1].AsText);
    }

    [Fact]
    public async Task ConcurrentReader_SeesSnapshotWhileWriterModifies()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'original')", ct);

        // Take a reader snapshot
        using var reader = _db.CreateReaderSession();

        // Writer modifies data
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'new')", ct);

        // Reader should still see original data only (snapshot isolation)
        await using var readerResult = await reader.ExecuteReadAsync("SELECT * FROM t", ct);
        var readerRows = await readerResult.ToListAsync(ct);
        Assert.Single(readerRows);
        Assert.Equal("original", readerRows[0][1].AsText);

        // Main database sees both rows
        await using var mainResult = await _db.ExecuteAsync("SELECT * FROM t", ct);
        var mainRows = await mainResult.ToListAsync(ct);
        Assert.Equal(2, mainRows.Count);
    }

    [Fact]
    public async Task MultipleReaders_DontBlockEachOther()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'data')", ct);

        using var reader1 = _db.CreateReaderSession();
        using var reader2 = _db.CreateReaderSession();

        await using var r1 = await reader1.ExecuteReadAsync("SELECT * FROM t", ct);
        await using var r2 = await reader2.ExecuteReadAsync("SELECT * FROM t", ct);

        var rows1 = await r1.ToListAsync(ct);
        var rows2 = await r2.ToListAsync(ct);

        Assert.Single(rows1);
        Assert.Single(rows2);
    }

    [Fact]
    public async Task ReaderSession_CanBeReusedForMultipleSequentialReads()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'original')", ct);

        using var reader = _db.CreateReaderSession();

        await using (var first = await reader.ExecuteReadAsync("SELECT COUNT(*) FROM t", ct))
        {
            var firstRows = await first.ToListAsync(ct);
            Assert.Equal(1L, firstRows[0][0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'new')", ct);

        await using (var second = await reader.ExecuteReadAsync("SELECT COUNT(*) FROM t", ct))
        {
            var secondRows = await second.ToListAsync(ct);
            Assert.Equal(1L, secondRows[0][0].AsInteger);
        }

        await using var main = await _db.ExecuteAsync("SELECT COUNT(*) FROM t", ct);
        var mainRows = await main.ToListAsync(ct);
        Assert.Equal(2L, mainRows[0][0].AsInteger);
    }

    [Fact]
    public async Task ReaderSession_RejectsConcurrentQueriesUntilPreviousResultIsDisposed()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'data')", ct);

        using var reader = _db.CreateReaderSession();
        await using var first = await reader.ExecuteReadAsync("SELECT * FROM t", ct);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await reader.ExecuteReadAsync("SELECT COUNT(*) FROM t", ct));

        Assert.Contains("one active query", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Checkpoint_CopiesDataToDbFile()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        for (int i = 0; i < 10; i++)
            await _db.ExecuteAsync($"INSERT INTO t VALUES ({i}, 'row{i}')", ct);

        // Manual checkpoint
        await _db.CheckpointAsync(ct);

        // Data should still be accessible
        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM t", ct);
        var rows = await result.ToListAsync(ct);
        Assert.Equal(10L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task ManyInserts_AutoCheckpointDoesNotCorrupt()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        for (int i = 0; i < 100; i++)
            await _db.ExecuteAsync($"INSERT INTO t VALUES ({i}, 'row{i}')", ct);

        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM t", ct);
        var rows = await result.ToListAsync(ct);
        Assert.Equal(100L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task Persistence_CloseAndReopen()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'hello')", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'world')", ct);

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        await using var result = await _db.ExecuteAsync("SELECT * FROM t ORDER BY id", ct);
        var rows = await result.ToListAsync(ct);
        Assert.Equal(2, rows.Count);
        Assert.Equal("hello", rows[0][1].AsText);
        Assert.Equal("world", rows[1][1].AsText);
    }

    [Fact]
    public async Task DeferredCheckpoint_BlockedByReader_RunsOnNextCommitAfterReaderDrains()
    {
        var ct = TestContext.Current.CancellationToken;

        await _db.DisposeAsync();

        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    // Keep auto-checkpoint effectively disabled so this test
                    // verifies reader-drain catch-up behavior.
                    CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
                }
            }
        };

        _db = await Database.OpenAsync(_dbPath, options, ct);
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);

        string walPath = _dbPath + ".wal";

        using (_db.CreateReaderSession())
        {
            await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'held_by_reader')", ct);
            await _db.CheckpointAsync(ct);

            Assert.True(File.Exists(walPath));
            long sizeWhileReaderActive = new FileInfo(walPath).Length;
            Assert.True(sizeWhileReaderActive > PageConstants.WalHeaderSize);
        }

        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'triggers_deferred_checkpoint')", ct);

        Assert.Equal(PageConstants.WalHeaderSize, new FileInfo(walPath).Length);

        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM t", ct);
        var rows = await result.ToListAsync(ct);
        Assert.Equal(2L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task BackgroundAutoCheckpoint_DoesNotBlockCommit_But_NextWriterWaits()
    {
        var ct = TestContext.Current.CancellationToken;

        await _db.DisposeAsync();

        var interceptor = new BlockingCheckpointInterceptor();
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    CheckpointPolicy = new FrameCountCheckpointPolicy(1),
                    AutoCheckpointExecutionMode = AutoCheckpointExecutionMode.Background,
                    Interceptors = [interceptor],
                }
            }
        };

        _db = await Database.OpenAsync(_dbPath, options, ct);
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        await _db.CheckpointAsync(ct);

        interceptor.Arm();

        var firstInsertTask = _db.ExecuteAsync("INSERT INTO t VALUES (1, 'first')", ct).AsTask();
        await firstInsertTask.WaitAsync(ct);
        await interceptor.WaitForCheckpointStartAsync(ct);

        Assert.True(firstInsertTask.IsCompletedSuccessfully);

        var secondInsertTask = _db.ExecuteAsync("INSERT INTO t VALUES (2, 'second')", ct).AsTask();
        Task winner = await Task.WhenAny(secondInsertTask, Task.Delay(100, ct));
        Assert.NotSame(secondInsertTask, winner);

        interceptor.Release();
        await secondInsertTask.WaitAsync(ct);

        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM t", ct);
        var rows = await result.ToListAsync(ct);
        Assert.Equal(2L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task BackgroundAutoCheckpoint_BlockedByReader_RunsAfterReaderDrains()
    {
        var ct = TestContext.Current.CancellationToken;

        await _db.DisposeAsync();

        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    CheckpointPolicy = new FrameCountCheckpointPolicy(1),
                    AutoCheckpointExecutionMode = AutoCheckpointExecutionMode.Background,
                }
            }
        };

        _db = await Database.OpenAsync(_dbPath, options, ct);
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        await _db.CheckpointAsync(ct);

        string walPath = _dbPath + ".wal";

        using (_db.CreateReaderSession())
        {
            await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'held_by_reader')", ct);

            Assert.True(File.Exists(walPath));
            long sizeWhileReaderActive = new FileInfo(walPath).Length;
            Assert.True(sizeWhileReaderActive > PageConstants.WalHeaderSize);
        }

        await WaitForWalLengthAsync(walPath, PageConstants.WalHeaderSize, TimeSpan.FromSeconds(5), ct);

        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM t", ct);
        var rows = await result.ToListAsync(ct);
        Assert.Equal(1L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task BackgroundAutoCheckpoint_LargeCommit_CompletesRemainingSlicesWhileIdle()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_background_idle_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var options = new PagerOptions
        {
            CheckpointPolicy = new FrameCountCheckpointPolicy(1),
            AutoCheckpointExecutionMode = AutoCheckpointExecutionMode.Background,
            AutoCheckpointMaxPagesPerStep = 1,
        };

        try
        {
            await using var pager = await OpenPagerAsync(dbPath, options, createNew: true, ct);

            await pager.BeginTransactionAsync(ct);
            uint rootPageId = await BTree.CreateNewAsync(pager, ct);
            var tree = new BTree(pager, rootPageId);
            byte[] payload = new byte[160];
            for (int i = 1; i <= 1500; i++)
            {
                payload[0] = (byte)(i & 0xFF);
                await tree.InsertAsync(i, payload, ct);
            }

            await pager.CommitAsync(ct);

            await WaitForWalLengthAsync(walPath, PageConstants.WalHeaderSize, TimeSpan.FromSeconds(5), ct);
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(walPath))
                File.Delete(walPath);
        }
    }

    [Fact]
    public async Task ReaderWalBackpressureLimit_BlocksCommitUntilReadersDrain()
    {
        var ct = TestContext.Current.CancellationToken;

        await _db.DisposeAsync();

        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
                    MaxWalBytesWhenReadersActive = PageConstants.WalHeaderSize + PageConstants.WalFrameSize,
                }
            }
        };

        _db = await Database.OpenAsync(_dbPath, options, ct);
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);

        using (_db.CreateReaderSession())
        {
            var ex = await Assert.ThrowsAsync<CSharpDbException>(
                async () => await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'blocked')", ct));

            Assert.Equal(ErrorCode.Busy, ex.Code);
            Assert.Contains("WAL growth limit exceeded", ex.Message, StringComparison.Ordinal);
        }

        // Reader drained: compact WAL and retry write.
        await _db.CheckpointAsync(ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'accepted')", ct);

        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM t", ct);
        var rows = await result.ToListAsync(ct);
        Assert.Equal(1L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task ReadPageAsync_InvalidFrameOffset_ThrowsWalError()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_read_test_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";

        var wal = new WriteAheadLog(dbPath, new WalIndex());
        try
        {
            await wal.OpenAsync(currentDbPageCount: 1, ct);

            var ex = await Assert.ThrowsAsync<CSharpDbException>(
                async () => await wal.ReadPageAsync(PageConstants.WalHeaderSize + 1_000_000, ct));

            Assert.Equal(ErrorCode.WalError, ex.Code);
            Assert.Contains("Short WAL read", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            await wal.CloseAndDeleteAsync();
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Fact]
    public async Task IncrementalCheckpoint_ReopenPreservesFramesCommittedAfterCheckpointStarts()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_incremental_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var device = new MemoryStorageDevice();
        WriteAheadLog? wal = null;

        try
        {
            var walIndex = new WalIndex();
            wal = new WriteAheadLog(dbPath, walIndex);
            await wal.OpenAsync(currentDbPageCount: 2, ct);

            byte[] originalRoot = CreateFilledPage(0x11);
            byte[] originalLeaf = CreateFilledPage(0x22);
            byte[] retainedLeafV1 = CreateFilledPage(0x33);
            byte[] retainedLeafV2 = CreateFilledPage(0x44);

            wal.BeginTransaction();
            await wal.AppendFramesAsync(
                new[]
                {
                    new WalFrameWrite(0, originalRoot),
                    new WalFrameWrite(1, originalLeaf),
                },
                ct);
            await wal.CommitAsync(newDbPageCount: 2, ct);

            bool completed = await wal.CheckpointStepAsync(device, pageCount: 2, maxPages: 1, ct);
            Assert.False(completed);
            Assert.True(wal.HasPendingCheckpoint);
            await AssertPageFilledAsync(device, 0, 0x11, ct);

            wal.BeginTransaction();
            await wal.AppendFrameAsync(1, retainedLeafV1, ct);
            await wal.CommitAsync(newDbPageCount: 2, ct);

            wal.BeginTransaction();
            await wal.AppendFrameAsync(1, retainedLeafV2, ct);
            await wal.CommitAsync(newDbPageCount: 2, ct);

            completed = await wal.CheckpointStepAsync(device, pageCount: 2, maxPages: 8, ct);
            Assert.True(completed);
            Assert.False(wal.HasPendingCheckpoint);
            Assert.Equal(PageConstants.WalHeaderSize + (2L * PageConstants.WalFrameSize), new FileInfo(walPath).Length);

            await AssertPageFilledAsync(device, 0, 0x11, ct);
            await AssertPageFilledAsync(device, 1, 0x22, ct);

            await wal.DisposeAsync();
            wal = null;

            var reopenedIndex = new WalIndex();
            wal = new WriteAheadLog(dbPath, reopenedIndex);
            await wal.OpenAsync(currentDbPageCount: 2, ct);

            Assert.Equal(2, reopenedIndex.FrameCount);
            Assert.True(reopenedIndex.TryGetLatest(1, out long retainedWalOffset));

            byte[] retainedPage = await wal.ReadPageAsync(retainedWalOffset, ct);
            Assert.All(retainedPage, static b => Assert.Equal((byte)0x44, b));

            await wal.CheckpointAsync(device, pageCount: 2, ct);

            Assert.False(wal.HasPendingCheckpoint);
            Assert.Equal(0, reopenedIndex.FrameCount);
            Assert.Equal(PageConstants.WalHeaderSize, new FileInfo(walPath).Length);
            await AssertPageFilledAsync(device, 1, 0x44, ct);
        }
        finally
        {
            if (wal is not null)
                await wal.CloseAndDeleteAsync();
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Fact]
    public async Task MemoryWal_CheckpointAsync_CompletesPendingIncrementalCheckpoint()
    {
        var ct = TestContext.Current.CancellationToken;
        var device = new MemoryStorageDevice();
        var walIndex = new WalIndex();
        await using var wal = new MemoryWriteAheadLog(walIndex);

        await wal.OpenAsync(currentDbPageCount: 3, ct);

        wal.BeginTransaction();
        await wal.AppendFramesAsync(
            new[]
            {
                new WalFrameWrite(0, CreateFilledPage(0x41)),
                new WalFrameWrite(1, CreateFilledPage(0x42)),
                new WalFrameWrite(2, CreateFilledPage(0x43)),
            },
            ct);
        await wal.CommitAsync(newDbPageCount: 3, ct);

        bool completed = await wal.CheckpointStepAsync(device, pageCount: 3, maxPages: 1, ct);
        Assert.False(completed);
        Assert.True(wal.HasPendingCheckpoint);

        await wal.CheckpointAsync(device, pageCount: 3, ct);

        Assert.False(wal.HasPendingCheckpoint);
        Assert.Equal(0, walIndex.FrameCount);
        await AssertPageFilledAsync(device, 0, 0x41, ct);
        await AssertPageFilledAsync(device, 1, 0x42, ct);
        await AssertPageFilledAsync(device, 2, 0x43, ct);
    }

    [Fact]
    public async Task IncrementalCheckpoint_PartialStep_DoesNotFlushDeviceUntilCompletion()
    {
        var ct = TestContext.Current.CancellationToken;
        var innerDevice = new MemoryStorageDevice();
        await using var device = new TrackingStorageDevice(innerDevice);
        var walIndex = new WalIndex();
        await using var wal = new MemoryWriteAheadLog(walIndex);

        await wal.OpenAsync(currentDbPageCount: 2, ct);

        wal.BeginTransaction();
        await wal.AppendFramesAsync(
            new[]
            {
                new WalFrameWrite(0, CreateFilledPage(0x51)),
                new WalFrameWrite(1, CreateFilledPage(0x52)),
            },
            ct);
        await wal.CommitAsync(newDbPageCount: 2, ct);

        bool completed = await wal.CheckpointStepAsync(device, pageCount: 2, maxPages: 1, ct);

        Assert.False(completed);
        Assert.True(wal.HasPendingCheckpoint);
        Assert.Equal(0, device.FlushCount);

        completed = await wal.CheckpointStepAsync(device, pageCount: 2, maxPages: 8, ct);

        Assert.True(completed);
        Assert.False(wal.HasPendingCheckpoint);
        Assert.Equal(1, device.FlushCount);
    }

    private static async Task WaitForWalLengthAsync(
        string walPath,
        long expectedLength,
        TimeSpan timeout,
        CancellationToken ct)
    {
        DateTime deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            if (File.Exists(walPath) && new FileInfo(walPath).Length == expectedLength)
                return;

            await Task.Delay(25, ct);
        }

        long actualLength = File.Exists(walPath) ? new FileInfo(walPath).Length : -1;
        throw new TimeoutException(
            $"WAL length did not reach {expectedLength} bytes within {timeout.TotalSeconds:F1}s (actual={actualLength}).");
    }

    private static byte[] CreateFilledPage(byte value)
    {
        byte[] page = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        page.AsSpan().Fill(value);
        return page;
    }

    private static async Task AssertPageFilledAsync(
        IStorageDevice device,
        uint pageId,
        byte expectedValue,
        CancellationToken ct)
    {
        byte[] buffer = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        int bytesRead = await device.ReadAsync((long)pageId * PageConstants.PageSize, buffer, ct);
        Assert.Equal(PageConstants.PageSize, bytesRead);
        Assert.All(buffer, b => Assert.Equal(expectedValue, b));
    }

    private static async ValueTask<Pager> OpenPagerAsync(
        string dbPath,
        PagerOptions options,
        bool createNew,
        CancellationToken ct)
    {
        var device = new FileStorageDevice(dbPath, createNew);
        var walIndex = new WalIndex();
        var wal = new WriteAheadLog(dbPath, walIndex);
        var pager = await Pager.CreateAsync(device, wal, walIndex, options, ct);

        if (createNew)
            await pager.InitializeNewDatabaseAsync(ct);

        await pager.RecoverAsync(ct);
        return pager;
    }

    private sealed class BlockingCheckpointInterceptor : IPageOperationInterceptor
    {
        private readonly TaskCompletionSource<bool> _checkpointStarted =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _allowCheckpoint =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _armed;

        public void Arm()
        {
            Volatile.Write(ref _armed, 1);
        }

        public void Release()
        {
            _allowCheckpoint.TrySetResult(true);
        }

        public Task WaitForCheckpointStartAsync(CancellationToken ct = default)
        {
            return _checkpointStarted.Task.WaitAsync(ct);
        }

        public ValueTask OnBeforeReadAsync(uint pageId, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnAfterReadAsync(uint pageId, PageReadSource source, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnBeforeWriteAsync(uint pageId, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnAfterWriteAsync(uint pageId, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnCommitStartAsync(int dirtyPageCount, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnCommitEndAsync(int dirtyPageCount, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnCheckpointStartAsync(int committedFrameCount, CancellationToken ct = default)
        {
            if (Volatile.Read(ref _armed) == 0)
                return ValueTask.CompletedTask;

            _checkpointStarted.TrySetResult(true);
            return new ValueTask(_allowCheckpoint.Task.WaitAsync(ct));
        }

        public ValueTask OnCheckpointEndAsync(int committedFrameCount, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnRecoveryStartAsync(CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnRecoveryEndAsync(bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
    }

    private sealed class TrackingStorageDevice : IStorageDevice
    {
        private readonly IStorageDevice _inner;
        private int _flushCount;

        public TrackingStorageDevice(IStorageDevice inner)
        {
            _inner = inner;
        }

        public int FlushCount => Volatile.Read(ref _flushCount);
        public long Length => _inner.Length;

        public ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default) =>
            _inner.ReadAsync(offset, buffer, ct);

        public ValueTask WriteAsync(long offset, ReadOnlyMemory<byte> buffer, CancellationToken ct = default) =>
            _inner.WriteAsync(offset, buffer, ct);

        public async ValueTask FlushAsync(CancellationToken ct = default)
        {
            Interlocked.Increment(ref _flushCount);
            await _inner.FlushAsync(ct);
        }

        public ValueTask SetLengthAsync(long length, CancellationToken ct = default) =>
            _inner.SetLengthAsync(length, ct);

        public ValueTask DisposeAsync() => _inner.DisposeAsync();

        public void Dispose() => _inner.Dispose();
    }
}
