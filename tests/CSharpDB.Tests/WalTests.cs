using CSharpDB.Core;
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
}
