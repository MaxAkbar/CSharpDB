using CSharpDB.Primitives;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Device;
using CSharpDB.Engine;
using CSharpDB.Sql;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;
using CSharpDB.Storage.Wal;
using System.Reflection;

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
    public async Task ReaderSession_PreparedCountStarStatement_UsesSnapshotState()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'original')", ct);

        using var reader = _db.CreateReaderSession();
        var countStatement = new SelectStatement
        {
            IsDistinct = false,
            Columns =
            [
                new SelectColumn
                {
                    IsStar = false,
                    Expression = new FunctionCallExpression
                    {
                        FunctionName = "COUNT",
                        Arguments = [],
                        IsStarArg = true,
                    },
                    Alias = null,
                },
            ],
            From = new SimpleTableRef { TableName = "t" },
            Where = null,
            GroupBy = null,
            Having = null,
            OrderBy = null,
            Limit = null,
            Offset = null,
        };

        await using (var first = await reader.ExecuteReadAsync(countStatement, ct))
        {
            var firstRows = await first.ToListAsync(ct);
            Assert.Equal(1L, firstRows[0][0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'new')", ct);

        await using (var second = await reader.ExecuteReadAsync(countStatement, ct))
        {
            var secondRows = await second.ToListAsync(ct);
            Assert.Equal(1L, secondRows[0][0].AsInteger);
        }
    }

    [Fact]
    public async Task ReaderSession_PreparedPrimaryKeyLookupStatement_ReturnsProjectedValue()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'original')", ct);

        using var reader = _db.CreateReaderSession();
        var lookupStatement = new SelectStatement
        {
            IsDistinct = false,
            Columns =
            [
                new SelectColumn
                {
                    IsStar = false,
                    Expression = new ColumnRefExpression { ColumnName = "val" },
                    Alias = null,
                },
            ],
            From = new SimpleTableRef { TableName = "t" },
            Where = new BinaryExpression
            {
                Op = BinaryOp.Equals,
                Left = new ColumnRefExpression { ColumnName = "id" },
                Right = new LiteralExpression
                {
                    LiteralType = TokenType.IntegerLiteral,
                    Value = 1L,
                },
            },
            GroupBy = null,
            Having = null,
            OrderBy = null,
            Limit = null,
            Offset = null,
        };

        await using var result = await reader.ExecuteReadAsync(lookupStatement, ct);
        var rows = await result.ToListAsync(ct);
        var row = Assert.Single(rows);
        Assert.Equal("original", row[0].AsText);
    }

    [Fact]
    public async Task ReaderSession_PreparedPrimaryKeyLookup_UsesSnapshotStateAfterCommittedUpdate()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'original')", ct);

        using var reader = _db.CreateReaderSession();
        var lookupStatement = new SelectStatement
        {
            IsDistinct = false,
            Columns =
            [
                new SelectColumn
                {
                    IsStar = false,
                    Expression = new ColumnRefExpression { ColumnName = "val" },
                    Alias = null,
                },
            ],
            From = new SimpleTableRef { TableName = "t" },
            Where = new BinaryExpression
            {
                Op = BinaryOp.Equals,
                Left = new ColumnRefExpression { ColumnName = "id" },
                Right = new LiteralExpression
                {
                    LiteralType = TokenType.IntegerLiteral,
                    Value = 1L,
                },
            },
            GroupBy = null,
            Having = null,
            OrderBy = null,
            Limit = null,
            Offset = null,
        };

        await _db.ExecuteAsync("UPDATE t SET val = 'updated' WHERE id = 1", ct);

        await using var result = await reader.ExecuteReadAsync(lookupStatement, ct);
        var rows = await result.ToListAsync(ct);
        var row = Assert.Single(rows);
        Assert.Equal("original", row[0].AsText);
    }

    [Fact]
    public async Task ReaderSession_PreparedPrimaryKeyLookup_IgnoresUncommittedDirtyPage()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'original')", ct);

        using var reader = _db.CreateReaderSession();
        var lookupStatement = new SelectStatement
        {
            IsDistinct = false,
            Columns =
            [
                new SelectColumn
                {
                    IsStar = false,
                    Expression = new ColumnRefExpression { ColumnName = "val" },
                    Alias = null,
                },
            ],
            From = new SimpleTableRef { TableName = "t" },
            Where = new BinaryExpression
            {
                Op = BinaryOp.Equals,
                Left = new ColumnRefExpression { ColumnName = "id" },
                Right = new LiteralExpression
                {
                    LiteralType = TokenType.IntegerLiteral,
                    Value = 1L,
                },
            },
            GroupBy = null,
            Having = null,
            OrderBy = null,
            Limit = null,
            Offset = null,
        };

        await _db.BeginTransactionAsync(ct);
        try
        {
            await _db.ExecuteAsync("UPDATE t SET val = 'dirty' WHERE id = 1", ct);

            await using var result = await reader.ExecuteReadAsync(lookupStatement, ct);
            var rows = await result.ToListAsync(ct);
            var row = Assert.Single(rows);
            Assert.Equal("original", row[0].AsText);
        }
        finally
        {
            await _db.RollbackAsync(ct);
        }
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
    public async Task TableRowCountMetadata_TracksTransactionStateAndPersistsOnCommit()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'one')", ct);

        await _db.BeginTransactionAsync(ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'two')", ct);

        await using (var inTxn = await _db.ExecuteAsync("SELECT COUNT(*) FROM t", ct))
        {
            var rows = await inTxn.ToListAsync(ct);
            Assert.Equal(2L, rows[0][0].AsInteger);
        }

        await _db.RollbackAsync(ct);

        await using (var afterRollback = await _db.ExecuteAsync("SELECT COUNT(*) FROM t", ct))
        {
            var rows = await afterRollback.ToListAsync(ct);
            Assert.Equal(1L, rows[0][0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO t VALUES (3, 'three')", ct);
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        await using var afterReopen = await _db.ExecuteAsync("SELECT COUNT(*) FROM t", ct);
        var reopenedRows = await afterReopen.ToListAsync(ct);
        Assert.Equal(2L, reopenedRows[0][0].AsInteger);
    }

    [Fact]
    public async Task BeginTransaction_AfterExplicitCommit_DoesNotFlushPendingImmediateTableStats()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);

        await _db.BeginTransactionAsync(ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'one')", ct);
        await _db.CommitAsync(ct);

        _db.ResetWalFlushDiagnostics();
        _db.ResetCommitPathDiagnostics();

        await _db.BeginTransactionAsync(ct);

        WalFlushDiagnosticsSnapshot walDiagnostics = _db.GetWalFlushDiagnosticsSnapshot();
        CommitPathDiagnosticsSnapshot commitDiagnostics = _db.GetCommitPathDiagnosticsSnapshot();

        Assert.Equal(0, walDiagnostics.FlushCount);
        Assert.Equal(0, walDiagnostics.FlushedCommitCount);
        Assert.Equal(0, walDiagnostics.FlushedByteCount);
        Assert.Equal(0, commitDiagnostics.BufferedFlushCount + commitDiagnostics.DurableFlushCount);

        await _db.RollbackAsync(ct);
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
            AutoCheckpointMaxPagesPerStep = 64,
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

            long initialWalLength = new FileInfo(walPath).Length;
            Assert.True(
                initialWalLength > PageConstants.WalHeaderSize + (64L * PageConstants.WalFrameSize),
                $"Expected the large commit to require multiple background checkpoint slices (walLength={initialWalLength}).");

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
            await (await wal.CommitAsync(newDbPageCount: 2, ct)).WaitAsync(ct);

            bool completed = await wal.CheckpointStepAsync(device, pageCount: 2, maxPages: 1, ct);
            Assert.False(completed);
            Assert.True(wal.HasPendingCheckpoint);
            await AssertPageFilledAsync(device, 0, 0x11, ct);

            wal.BeginTransaction();
            await wal.AppendFrameAsync(1, retainedLeafV1, ct);
            await (await wal.CommitAsync(newDbPageCount: 2, ct)).WaitAsync(ct);

            wal.BeginTransaction();
            await wal.AppendFrameAsync(1, retainedLeafV2, ct);
            await (await wal.CommitAsync(newDbPageCount: 2, ct)).WaitAsync(ct);

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
        await (await wal.CommitAsync(newDbPageCount: 3, ct)).WaitAsync(ct);

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
        await (await wal.CommitAsync(newDbPageCount: 2, ct)).WaitAsync(ct);

        bool completed = await wal.CheckpointStepAsync(device, pageCount: 2, maxPages: 1, ct);

        Assert.False(completed);
        Assert.True(wal.HasPendingCheckpoint);
        Assert.Equal(0, device.FlushCount);

        completed = await wal.CheckpointStepAsync(device, pageCount: 2, maxPages: 8, ct);

        Assert.True(completed);
        Assert.False(wal.HasPendingCheckpoint);
        Assert.Equal(1, device.FlushCount);
    }

    [Fact]
    public async Task FileWriteAheadLog_DurableMode_SelectsDurableFlushPolicy()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_flush_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        WriteAheadLog? wal = null;

        try
        {
            wal = new WriteAheadLog(dbPath, new WalIndex(), durabilityMode: DurabilityMode.Durable);
            Assert.IsType<DurableWalFlushPolicy>(wal.FlushPolicy);
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
    public async Task FileWriteAheadLog_BufferedMode_SelectsBufferedFlushPolicy()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_flush_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        WriteAheadLog? wal = null;

        try
        {
            wal = new WriteAheadLog(dbPath, new WalIndex(), durabilityMode: DurabilityMode.Buffered);
            Assert.IsType<BufferedWalFlushPolicy>(wal.FlushPolicy);
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
    public async Task FileWriteAheadLog_CommitAsync_InvokesConfiguredFlushPolicy()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_flush_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var policy = new TrackingWalFlushPolicy();
        WriteAheadLog? wal = null;

        try
        {
            wal = new WriteAheadLog(
                dbPath,
                new WalIndex(),
                checksumProvider: null,
                flushPolicy: policy);
            await wal.OpenAsync(currentDbPageCount: 1, ct);
            wal.BeginTransaction();
            await wal.AppendFrameAsync(0, CreateFilledPage(0x62), ct);

            await (await wal.CommitAsync(newDbPageCount: 1, ct)).WaitAsync(ct);

            Assert.IsType<TrackingWalFlushPolicy>(wal.FlushPolicy);
            Assert.True(policy.FlushCount > 0);
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
    public async Task FileWriteAheadLog_DurableCommit_IsNotVisibleUntilFlushCompletes()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_visibility_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var policy = new BlockingCommitWalFlushPolicy();
        WriteAheadLog? wal = null;

        try
        {
            wal = new WriteAheadLog(dbPath, new WalIndex(), checksumProvider: null, flushPolicy: policy);
            await wal.OpenAsync(currentDbPageCount: 1, ct);

            wal.BeginTransaction();
            await wal.AppendFrameAsync(0, CreateFilledPage(0x71), ct);
            WalCommitResult commit = await wal.CommitAsync(newDbPageCount: 1, ct);

            await policy.WaitForCommitFlushStartAsync(ct);
            Assert.Equal(0, wal.Index.FrameCount);
            Assert.False(wal.Index.TryGetLatest(0, out _));

            policy.Release();
            await commit.WaitAsync(ct);

            Assert.Equal(1, wal.Index.FrameCount);
            Assert.True(wal.Index.TryGetLatest(0, out _));
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
    public async Task FileWriteAheadLog_DurableCommits_CanShareOneFlush()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_group_durable_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var policy = new BlockingCommitWalFlushPolicy();
        WriteAheadLog? wal = null;
        TimeSpan batchWindow = TimeSpan.FromMilliseconds(5);

        try
        {
            wal = new WriteAheadLog(
                dbPath,
                new WalIndex(),
                checksumProvider: null,
                flushPolicy: policy,
                durableCommitBatchWindow: batchWindow);
            await wal.OpenAsync(currentDbPageCount: 2, ct);

            wal.BeginTransaction();
            await wal.AppendFrameAsync(0, CreateFilledPage(0x81), ct);
            WalCommitResult commit1 = await wal.CommitAsync(newDbPageCount: 2, ct);

            wal.BeginTransaction();
            await wal.AppendFrameAsync(1, CreateFilledPage(0x82), ct);
            WalCommitResult commit2 = await wal.CommitAsync(newDbPageCount: 2, ct);

            await policy.WaitForCommitFlushStartAsync(ct);
            Assert.Equal(0, wal.Index.FrameCount);

            policy.Release();
            await commit1.WaitAsync(ct);
            await commit2.WaitAsync(ct);

            WalFlushDiagnosticsSnapshot diagnostics =
                ((IWalRuntimeDiagnosticsProvider)wal).GetWalFlushDiagnosticsSnapshot();

            Assert.Equal(1, policy.CommitFlushCount);
            Assert.Equal(2, wal.Index.FrameCount);
            Assert.Equal(1, diagnostics.FlushCount);
            Assert.Equal(2, diagnostics.FlushedCommitCount);
            Assert.True(wal.Index.TryGetLatest(0, out _));
            Assert.True(wal.Index.TryGetLatest(1, out _));
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
    public async Task FileWriteAheadLog_DurableCommitBatchWindow_BypassesDelayWhenQueueIsHeavy()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_group_bypass_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var policy = new BlockingCommitWalFlushPolicy();
        WriteAheadLog? wal = null;

        try
        {
            wal = new WriteAheadLog(
                dbPath,
                new WalIndex(),
                checksumProvider: null,
                flushPolicy: policy,
                durableCommitBatchWindow: TimeSpan.FromSeconds(30));
            await wal.OpenAsync(currentDbPageCount: 8, ct);

            var commits = new List<WalCommitResult>(WriteAheadLog.DurableCommitBatchBypassPendingCommitThreshold);
            for (int i = 0; i < WriteAheadLog.DurableCommitBatchBypassPendingCommitThreshold; i++)
            {
                wal.BeginTransaction();
                await wal.AppendFrameAsync((uint)i, CreateFilledPage((byte)(0xC0 + i)), ct);
                commits.Add(await wal.CommitAsync(newDbPageCount: 8, ct));
            }

            Task flushStart = policy.WaitForCommitFlushStartAsync(ct);
            Task winner = await Task.WhenAny(flushStart, Task.Delay(TimeSpan.FromSeconds(1), ct));
            Assert.Same(flushStart, winner);

            policy.Release();
            foreach (WalCommitResult commit in commits)
                await commit.WaitAsync(ct);

            WalFlushDiagnosticsSnapshot diagnostics =
                ((IWalRuntimeDiagnosticsProvider)wal).GetWalFlushDiagnosticsSnapshot();

            Assert.Equal(1, policy.CommitFlushCount);
            Assert.Equal(WriteAheadLog.DurableCommitBatchBypassPendingCommitThreshold, diagnostics.FlushedCommitCount);
            Assert.True(diagnostics.BatchWindowThresholdBypassCount > 0);
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
    public async Task Database_CommitPathDiagnostics_TrackDurableAutoCommitStages()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_commit_diag_{Guid.NewGuid():N}.db");
        var options = new DatabaseOptions()
            .ConfigureStorageEngine(builder => builder.UseLowLatencyDurableWritePreset());

        try
        {
            await using var db = await Database.OpenAsync(dbPath, options, ct);
            await db.ExecuteAsync("CREATE TABLE diag_t (id INTEGER PRIMARY KEY, value INTEGER)", ct);

            db.ResetWalFlushDiagnostics();
            db.ResetCommitPathDiagnostics();

            await db.ExecuteAsync("INSERT INTO diag_t VALUES (1, 10)", ct);

            WalFlushDiagnosticsSnapshot walDiagnostics = db.GetWalFlushDiagnosticsSnapshot();
            CommitPathDiagnosticsSnapshot commitDiagnostics = db.GetCommitPathDiagnosticsSnapshot();

            Assert.True(commitDiagnostics.WalAppendCount > 0);
            Assert.True(commitDiagnostics.BufferedFlushCount > 0 || commitDiagnostics.DurableFlushCount > 0);
            Assert.True(commitDiagnostics.PublishBatchCount > 0);
            Assert.True(commitDiagnostics.FinalizeCommitCount > 0);
            Assert.True(commitDiagnostics.CheckpointDecisionCount > 0);
            Assert.True(walDiagnostics.FlushCount > 0);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal")) File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task FileWriteAheadLog_Recover_TruncatesPreallocatedTailAndRetainsCommittedFrames()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_prealloc_recover_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        const long preallocationChunkBytes = 1L * 1024 * 1024;

        try
        {
            await using (var wal = new WriteAheadLog(
                dbPath,
                new WalIndex(),
                durableCommitBatchWindow: TimeSpan.Zero,
                walPreallocationChunkBytes: preallocationChunkBytes))
            {
                await wal.OpenAsync(currentDbPageCount: 1, ct);
                wal.BeginTransaction();
                await wal.AppendFrameAsync(0, CreateFilledPage(0xD1), ct);
                await (await wal.CommitAsync(newDbPageCount: 1, ct)).WaitAsync(ct);

                WalFlushDiagnosticsSnapshot diagnostics =
                    ((IWalRuntimeDiagnosticsProvider)wal).GetWalFlushDiagnosticsSnapshot();

                Assert.True(diagnostics.PreallocationCount > 0);
                Assert.True(new FileInfo(walPath).Length > PageConstants.WalHeaderSize + PageConstants.WalFrameSize);
            }

            await using var reopened = new WriteAheadLog(dbPath, new WalIndex());
            await reopened.OpenAsync(currentDbPageCount: 1, ct);

            Assert.Equal(1, reopened.Index.FrameCount);
            Assert.True(reopened.Index.TryGetLatest(0, out _));
            Assert.Equal(PageConstants.WalHeaderSize + PageConstants.WalFrameSize, new FileInfo(walPath).Length);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Fact]
    public async Task FileWriteAheadLog_Checkpoint_RepairsStaleIndexOffsetsFromWalFile()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_checkpoint_repair_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";

        try
        {
            await using var device = new FileStorageDevice(dbPath, createNew: true);
            var walIndex = new WalIndex();
            await using var wal = new WriteAheadLog(dbPath, walIndex);
            await wal.OpenAsync(currentDbPageCount: 1, ct);

            wal.BeginTransaction();
            await wal.AppendFrameAsync(0, CreateFilledPage(0x91), ct);
            await (await wal.CommitAsync(newDbPageCount: 1, ct)).WaitAsync(ct);

            walIndex.OverwriteCommittedState(
                new Dictionary<uint, long>
                {
                    [0] = PageConstants.WalHeaderSize + PageConstants.WalFrameSize,
                },
                frameCount: 1,
                commitCounter: 1);

            await wal.CheckpointAsync(device, pageCount: 1, ct);

            Assert.Equal(0, wal.Index.FrameCount);
            Assert.False(wal.Index.TryGetLatest(0, out _));
            Assert.Equal(PageConstants.WalHeaderSize, new FileInfo(walPath).Length);
            await AssertPageFilledAsync(device, 0, 0x91, ct);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Fact]
    public async Task Checkpoint_DefaultOptions_PreserveOwnedPagesForPointLookups()
    {
        var ct = TestContext.Current.CancellationToken;
        const int rowCount = 5_000;
        const int probeCount = 512;

        await _db.ExecuteAsync("CREATE TABLE cache_hot_t (id INTEGER PRIMARY KEY, value INTEGER, note TEXT)", ct);
        await SeedPointLookupTableAsync(_db, "cache_hot_t", rowCount, ct);

        BTree tree = GetTableTree(_db, "cache_hot_t");

        await _db.CheckpointAsync(ct);

        int cacheHits = CountLookupCacheHits(tree, rowCount, probeCount);
        Assert.Equal(probeCount, cacheHits);
    }

    [Fact]
    public async Task Checkpoint_PreserveOwnedPagesOptOut_ClearsPointLookupCache()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_checkpoint_cache_clear_{Guid.NewGuid():N}.db");
        const int rowCount = 5_000;
        const int probeCount = 512;

        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    PreserveOwnedPagesOnCheckpoint = false,
                },
            },
        };

        try
        {
            await using var db = await Database.OpenAsync(dbPath, options, ct);
            await db.ExecuteAsync("CREATE TABLE cache_cold_t (id INTEGER PRIMARY KEY, value INTEGER, note TEXT)", ct);
            await SeedPointLookupTableAsync(db, "cache_cold_t", rowCount, ct);

            BTree tree = GetTableTree(db, "cache_cold_t");

            await db.CheckpointAsync(ct);

            int cacheHits = CountLookupCacheHits(tree, rowCount, probeCount);
            Assert.Equal(0, cacheHits);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal")) File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task FileWriteAheadLog_Checkpoint_RepairsStaleInRangeIndexOffsetsFromWalFile()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_checkpoint_inrange_repair_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";

        try
        {
            await using var device = new FileStorageDevice(dbPath, createNew: true);
            var walIndex = new WalIndex();
            await using var wal = new WriteAheadLog(dbPath, walIndex);
            await wal.OpenAsync(currentDbPageCount: 2, ct);

            wal.BeginTransaction();
            await wal.AppendFrameAsync(0, CreateFilledPage(0xA1), ct);
            await wal.AppendFrameAsync(1, CreateFilledPage(0xA2), ct);
            await (await wal.CommitAsync(newDbPageCount: 2, ct)).WaitAsync(ct);

            walIndex.OverwriteCommittedState(
                new Dictionary<uint, long>
                {
                    [0] = PageConstants.WalHeaderSize + PageConstants.WalFrameSize,
                    [1] = PageConstants.WalHeaderSize + PageConstants.WalFrameSize,
                },
                frameCount: 2,
                commitCounter: 1);

            await wal.CheckpointAsync(device, pageCount: 2, ct);

            Assert.Equal(0, wal.Index.FrameCount);
            Assert.False(wal.Index.TryGetLatest(0, out _));
            Assert.False(wal.Index.TryGetLatest(1, out _));
            Assert.Equal(PageConstants.WalHeaderSize, new FileInfo(walPath).Length);
            await AssertPageFilledAsync(device, 0, 0xA1, ct);
            await AssertPageFilledAsync(device, 1, 0xA2, ct);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Fact]
    public async Task Pager_BeginCommit_DoesNotClearNextWriterTransactionState()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_pager_split_commit_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var policy = new BlockingCommitWalFlushPolicy();

        try
        {
            await using var device = new FileStorageDevice(dbPath, createNew: true);
            var walIndex = new WalIndex();
            await using var wal = new WriteAheadLog(
                dbPath,
                walIndex,
                checksumProvider: null,
                flushPolicy: policy,
                durableCommitBatchWindow: TimeSpan.FromMilliseconds(5));
            await using var pager = await Pager.CreateAsync(device, wal, walIndex, new PagerOptions(), ct);
            await pager.InitializeNewDatabaseAsync(ct);
            await pager.RecoverAsync(ct);

            await pager.BeginTransactionAsync(ct);
            uint firstPageId = await pager.AllocatePageAsync(ct);
            byte[] firstPage = await pager.GetPageAsync(firstPageId, ct);
            firstPage[0] = 0x31;
            await pager.MarkDirtyAsync(firstPageId, ct);

            PagerCommitResult firstCommit = await pager.BeginCommitAsync(ct);
            await policy.WaitForCommitFlushStartAsync(ct);

            await pager.BeginTransactionAsync(ct);
            uint secondPageId = await pager.AllocatePageAsync(ct);
            byte[] secondPage = await pager.GetPageAsync(secondPageId, ct);
            secondPage[0] = 0x42;
            await pager.MarkDirtyAsync(secondPageId, ct);

            policy.Release();
            await firstCommit.WaitAsync(ct);

            await pager.CommitAsync(ct);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Fact]
    public async Task BackgroundCheckpoint_DoesNotStartWhileNextWriterIsActive()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_pager_checkpoint_writer_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var flushPolicy = new BlockingCommitWalFlushPolicy();
        var interceptor = new BlockingCheckpointInterceptor();
        interceptor.Arm();

        try
        {
            await using var device = new FileStorageDevice(dbPath, createNew: true);
            var walIndex = new WalIndex();
            await using var wal = new WriteAheadLog(
                dbPath,
                walIndex,
                checksumProvider: null,
                flushPolicy: flushPolicy,
                durableCommitBatchWindow: TimeSpan.FromMilliseconds(5));
            await using var pager = await Pager.CreateAsync(
                device,
                wal,
                walIndex,
                new PagerOptions
                {
                    CheckpointPolicy = new FrameCountCheckpointPolicy(1),
                    AutoCheckpointExecutionMode = AutoCheckpointExecutionMode.Background,
                    Interceptors = [interceptor],
                },
                ct);
            await pager.InitializeNewDatabaseAsync(ct);
            await pager.RecoverAsync(ct);

            await pager.BeginTransactionAsync(ct);
            uint rootPageId = await BTree.CreateNewAsync(pager, ct);
            var tree = new BTree(pager, rootPageId);
            await tree.InsertAsync(1, BitConverter.GetBytes(1L), ct);

            PagerCommitResult commit1 = await pager.BeginCommitAsync(ct);
            await flushPolicy.WaitForCommitFlushStartAsync(ct);

            await pager.BeginTransactionAsync(ct);
            await tree.InsertAsync(2, BitConverter.GetBytes(2L), ct);

            flushPolicy.Release();
            await commit1.WaitAsync(ct);

            Task checkpointStart = interceptor.WaitForCheckpointStartAsync(ct);
            Task winner = await Task.WhenAny(checkpointStart, Task.Delay(100, ct));
            Assert.NotSame(checkpointStart, winner);

            PagerCommitResult commit2 = await pager.BeginCommitAsync(ct);
            await checkpointStart.WaitAsync(ct);
            interceptor.Release();
            await commit2.WaitAsync(ct);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Fact]
    public async Task FileWriteAheadLog_FlushFailure_FailsQueuedCommits_AndFaultsFutureWrites()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_flush_failure_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var policy = new FailingCommitWalFlushPolicy();
        WriteAheadLog? wal = null;
        TimeSpan batchWindow = TimeSpan.FromMilliseconds(5);

        try
        {
            wal = new WriteAheadLog(
                dbPath,
                new WalIndex(),
                checksumProvider: null,
                flushPolicy: policy,
                durableCommitBatchWindow: batchWindow);
            await wal.OpenAsync(currentDbPageCount: 2, ct);

            wal.BeginTransaction();
            await wal.AppendFrameAsync(0, CreateFilledPage(0xA1), ct);
            WalCommitResult commit1 = await wal.CommitAsync(newDbPageCount: 2, ct);

            Exception? secondCommitFailure = null;
            WalCommitResult commit2 = WalCommitResult.Completed;
            try
            {
                wal.BeginTransaction();
                await wal.AppendFrameAsync(1, CreateFilledPage(0xA2), ct);
                commit2 = await wal.CommitAsync(newDbPageCount: 2, ct);
            }
            catch (Exception ex)
            {
                secondCommitFailure = ex;
            }

            var error1 = await Assert.ThrowsAsync<CSharpDbException>(() => commit1.WaitAsync(ct).AsTask());
            CSharpDbException error2 = secondCommitFailure switch
            {
                CSharpDbException walError => walError,
                null => await Assert.ThrowsAsync<CSharpDbException>(() => commit2.WaitAsync(ct).AsTask()),
                _ => throw new Xunit.Sdk.XunitException($"Unexpected exception type for second commit: {secondCommitFailure.GetType().FullName}")
            };

            Assert.Equal(ErrorCode.WalError, error1.Code);
            Assert.Equal(ErrorCode.WalError, error2.Code);
            Assert.Equal(0, wal.Index.FrameCount);

            var writeFault = Assert.Throws<CSharpDbException>(() => wal.BeginTransaction());
            Assert.Equal(ErrorCode.WalError, writeFault.Code);
        }
        finally
        {
            if (wal is not null)
                await wal.DisposeAsync();
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Fact]
    public async Task FileWriteAheadLog_FlushFailure_DoesNotRecoverFailedCommitOnReopen()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_flush_reopen_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var policy = new FailingCommitWalFlushPolicy();

        try
        {
            await using (var wal = new WriteAheadLog(
                dbPath,
                new WalIndex(),
                checksumProvider: null,
                flushPolicy: policy))
            {
                await wal.OpenAsync(currentDbPageCount: 1, ct);
                wal.BeginTransaction();
                await wal.AppendFrameAsync(0, CreateFilledPage(0xB1), ct);

                WalCommitResult commit = await wal.CommitAsync(newDbPageCount: 1, ct);
                await Assert.ThrowsAsync<CSharpDbException>(() => commit.WaitAsync(ct).AsTask());
            }

            await using var reopened = new WriteAheadLog(dbPath, new WalIndex());
            await reopened.OpenAsync(currentDbPageCount: 1, ct);

            Assert.Equal(0, reopened.Index.FrameCount);
            Assert.False(reopened.Index.TryGetLatest(0, out _));
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Fact]
    public async Task FileWriteAheadLog_AppendFrameAsync_StagesFramesUntilCommit()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_stage_commit_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";

        try
        {
            var walIndex = new WalIndex();
            await using var wal = new WriteAheadLog(dbPath, walIndex);
            await wal.OpenAsync(currentDbPageCount: 2, ct);

            Assert.Equal(PageConstants.WalHeaderSize, new FileInfo(walPath).Length);

            wal.BeginTransaction();
            await wal.AppendFrameAsync(0, CreateFilledPage(0xC1), ct);
            await wal.AppendFrameAsync(1, CreateFilledPage(0xC2), ct);

            Assert.Equal(PageConstants.WalHeaderSize, new FileInfo(walPath).Length);

            await (await wal.CommitAsync(newDbPageCount: 2, ct)).WaitAsync(ct);
            await WaitForWalLengthAsync(
                walPath,
                PageConstants.WalHeaderSize + (2L * PageConstants.WalFrameSize),
                TimeSpan.FromSeconds(2),
                ct);

            Assert.Equal(2, wal.Index.FrameCount);
            Assert.True(wal.Index.TryGetLatest(0, out long page0Offset));
            Assert.True(wal.Index.TryGetLatest(1, out long page1Offset));

            byte[] page0 = await wal.ReadPageAsync(page0Offset, ct);
            byte[] page1 = await wal.ReadPageAsync(page1Offset, ct);
            Assert.All(page0, static b => Assert.Equal((byte)0xC1, b));
            Assert.All(page1, static b => Assert.Equal((byte)0xC2, b));
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Fact]
    public async Task FileWriteAheadLog_AppendFrameAsync_CanceledBeforeFirstBufferedFrame_DoesNotStageFrame()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_stage_cancel_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";

        try
        {
            var walIndex = new WalIndex();
            await using var wal = new WriteAheadLog(dbPath, walIndex);
            await wal.OpenAsync(currentDbPageCount: 1, ct);

            wal.BeginTransaction();
            using var canceled = new CancellationTokenSource();
            canceled.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => wal.AppendFrameAsync(0, CreateFilledPage(0xD4), canceled.Token).AsTask());

            await wal.AppendFrameAsync(0, CreateFilledPage(0xD5), ct);
            await (await wal.CommitAsync(newDbPageCount: 1, ct)).WaitAsync(ct);

            Assert.Equal(1, wal.Index.FrameCount);
            Assert.True(wal.Index.TryGetLatest(0, out long pageOffset));
            byte[] page = await wal.ReadPageAsync(pageOffset, ct);
            Assert.All(page, static b => Assert.Equal((byte)0xD5, b));
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Fact]
    public async Task MemoryWriteAheadLog_AppendFrameAsync_CanceledBeforeFirstBufferedFrame_DoesNotStageFrame()
    {
        var ct = TestContext.Current.CancellationToken;
        var walIndex = new WalIndex();
        await using var wal = new MemoryWriteAheadLog(walIndex);
        await wal.OpenAsync(currentDbPageCount: 1, ct);

        wal.BeginTransaction();
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => wal.AppendFrameAsync(0, CreateFilledPage(0xD6), canceled.Token).AsTask());

        await wal.AppendFrameAsync(0, CreateFilledPage(0xD7), ct);
        await (await wal.CommitAsync(newDbPageCount: 1, ct)).WaitAsync(ct);

        Assert.Equal(1, walIndex.FrameCount);
        Assert.True(walIndex.TryGetLatest(0, out long pageOffset));
        byte[] page = await wal.ReadPageAsync(pageOffset, ct);
        Assert.All(page, static b => Assert.Equal((byte)0xD7, b));
    }

    [Fact]
    public async Task FileWriteAheadLog_AppendFrameAsync_CanMixWithAppendFramesAsyncInSameTransaction()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_stage_mixed_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";

        try
        {
            var walIndex = new WalIndex();
            await using var wal = new WriteAheadLog(dbPath, walIndex);
            await wal.OpenAsync(currentDbPageCount: 3, ct);

            wal.BeginTransaction();
            await wal.AppendFrameAsync(0, CreateFilledPage(0xD1), ct);
            Assert.Equal(PageConstants.WalHeaderSize, new FileInfo(walPath).Length);

            await wal.AppendFramesAsync(
                new[]
                {
                    new WalFrameWrite(1, CreateFilledPage(0xD2)),
                    new WalFrameWrite(2, CreateFilledPage(0xD3)),
                },
                ct);

            await (await wal.CommitAsync(newDbPageCount: 3, ct)).WaitAsync(ct);

            Assert.Equal(3, wal.Index.FrameCount);
            Assert.True(wal.Index.TryGetLatest(0, out long page0Offset));
            Assert.True(wal.Index.TryGetLatest(1, out long page1Offset));
            Assert.True(wal.Index.TryGetLatest(2, out long page2Offset));

            byte[] page0 = await wal.ReadPageAsync(page0Offset, ct);
            byte[] page1 = await wal.ReadPageAsync(page1Offset, ct);
            byte[] page2 = await wal.ReadPageAsync(page2Offset, ct);
            Assert.All(page0, static b => Assert.Equal((byte)0xD1, b));
            Assert.All(page1, static b => Assert.Equal((byte)0xD2, b));
            Assert.All(page2, static b => Assert.Equal((byte)0xD3, b));
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
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

    private static async ValueTask SeedPointLookupTableAsync(
        Database db,
        string tableName,
        int rowCount,
        CancellationToken ct)
    {
        const int batchSize = 500;
        for (int i = 0; i < rowCount; i += batchSize)
        {
            await db.BeginTransactionAsync(ct);
            int end = Math.Min(i + batchSize, rowCount);
            for (int id = i; id < end; id++)
                await db.ExecuteAsync($"INSERT INTO {tableName} VALUES ({id}, {id * 3}, 'row_{id}')", ct);
            await db.CommitAsync(ct);
        }
    }

    private static BTree GetTableTree(Database db, string tableName)
    {
        var catalogField = typeof(Database).GetField("_catalog", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Database catalog field not found.");
        object catalog = catalogField.GetValue(db)
            ?? throw new InvalidOperationException("Database catalog was null.");
        var getTableTreeMethod = catalog.GetType().GetMethod("GetTableTree", [typeof(string)])
            ?? throw new InvalidOperationException("SchemaCatalog.GetTableTree(string) not found.");
        return (BTree)(getTableTreeMethod.Invoke(catalog, [tableName])
            ?? throw new InvalidOperationException("SchemaCatalog.GetTableTree returned null."));
    }

    private static int CountLookupCacheHits(BTree tree, int rowCount, int probeCount)
    {
        var rng = new Random(7);
        int hits = 0;
        for (int i = 0; i < probeCount; i++)
        {
            if (tree.TryFindCachedMemory(rng.Next(0, rowCount), out _))
                hits++;
        }

        return hits;
    }

    private sealed class TrackingWalFlushPolicy : IWalFlushPolicy
    {
        private int _flushCount;

        public int FlushCount => Volatile.Read(ref _flushCount);
        public bool AllowsWriteConcurrencyDuringCommitFlush => false;

        public ValueTask FlushBufferedWritesAsync(FileStream stream, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }

        public ValueTask FlushCommitAsync(FileStream stream, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Interlocked.Increment(ref _flushCount);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class BlockingCommitWalFlushPolicy : IWalFlushPolicy
    {
        private readonly TaskCompletionSource<bool> _commitFlushStarted =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _allowCommitFlush =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _flushCount;

        public int CommitFlushCount => Math.Max(0, Volatile.Read(ref _flushCount) - 1);
        public bool AllowsWriteConcurrencyDuringCommitFlush => true;

        public Task WaitForCommitFlushStartAsync(CancellationToken ct = default)
        {
            return _commitFlushStarted.Task.WaitAsync(ct);
        }

        public void Release()
        {
            _allowCommitFlush.TrySetResult(true);
        }

        public ValueTask FlushBufferedWritesAsync(FileStream stream, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }

        public ValueTask FlushCommitAsync(FileStream stream, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int flushNumber = Interlocked.Increment(ref _flushCount);
            if (flushNumber == 1)
                return ValueTask.CompletedTask;

            _commitFlushStarted.TrySetResult(true);
            return new ValueTask(_allowCommitFlush.Task.WaitAsync(cancellationToken));
        }
    }

    private sealed class FailingCommitWalFlushPolicy : IWalFlushPolicy
    {
        private int _flushCount;
        public bool AllowsWriteConcurrencyDuringCommitFlush => true;

        public ValueTask FlushBufferedWritesAsync(FileStream stream, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }

        public ValueTask FlushCommitAsync(FileStream stream, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int flushNumber = Interlocked.Increment(ref _flushCount);
            if (flushNumber == 1)
                return ValueTask.CompletedTask;

            return ValueTask.FromException(new IOException("Injected commit flush failure."));
        }
    }
}
