using CSharpDB.Primitives;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Device;
using CSharpDB.Engine;
using CSharpDB.Sql;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Diagnostics;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;
using CSharpDB.Storage.Wal;
using Microsoft.Win32.SafeHandles;
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
    public async Task ReaderSession_ExplainEstimate_UsesReadOnlySnapshotRouting()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'original')", ct);

        using var reader = _db.CreateReaderSession();
        await using var result = await reader.ExecuteReadAsync(
            "EXPLAIN ESTIMATE FOR SELECT * FROM t WHERE id = 1",
            ct);
        var rows = await result.ToListAsync(ct);

        Assert.NotEmpty(rows);
        Assert.Contains(rows, row =>
            row.Any(value =>
                value.Type == DbType.Text &&
                value.AsText.Contains("t", StringComparison.OrdinalIgnoreCase)));
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
    public async Task ReaderSession_CountStarFastPath_DelaysSnapshotPagerAndPlannerUntilNeeded()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'original')", ct);

        using var reader = _db.CreateReaderSession();

        FieldInfo snapshotPagerField = typeof(Database.ReaderSession).GetField("_snapshotPager", BindingFlags.Instance | BindingFlags.NonPublic)!;
        FieldInfo plannerField = typeof(Database.ReaderSession).GetField("_planner", BindingFlags.Instance | BindingFlags.NonPublic)!;

        Assert.Null(snapshotPagerField.GetValue(reader));
        Assert.Null(plannerField.GetValue(reader));

        await using (var countResult = await reader.ExecuteReadAsync("SELECT COUNT(*) FROM t", ct))
        {
            DbValue[] row = Assert.Single(await countResult.ToListAsync(ct));
            Assert.Equal(1L, row[0].AsInteger);
        }

        Assert.Null(snapshotPagerField.GetValue(reader));
        Assert.Null(plannerField.GetValue(reader));

        await using (var fullScanResult = await reader.ExecuteReadAsync("SELECT * FROM t", ct))
        {
            DbValue[] row = Assert.Single(await fullScanResult.ToListAsync(ct));
            Assert.Equal(1L, row[0].AsInteger);
            Assert.Equal("original", row[1].AsText);
        }

        Assert.NotNull(snapshotPagerField.GetValue(reader));
        Assert.NotNull(plannerField.GetValue(reader));
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
    public async Task DeferredCheckpoint_PartiallyCopiesWhileReaderIsActive_AndFinalizesAfterReaderDrains()
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

        await _db.CheckpointAsync(ct);

        Assert.Equal(PageConstants.WalHeaderSize, new FileInfo(walPath).Length);

        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM t", ct);
        var rows = await result.ToListAsync(ct);
        Assert.Equal(1L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task Checkpoint_WithReaderHoldingNoWalFrames_CanFinalizeImmediately()
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
                }
            }
        };

        _db = await Database.OpenAsync(_dbPath, options, ct);
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ct);
        await _db.CheckpointAsync(ct);

        string walPath = _dbPath + ".wal";

        using (_db.CreateReaderSession())
        {
            await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'reader-holds-no-wal')", ct);
            await _db.CheckpointAsync(ct);

            Assert.Equal(PageConstants.WalHeaderSize, new FileInfo(walPath).Length);
        }

        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM t", ct);
        var rows = await result.ToListAsync(ct);
        Assert.Equal(1L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task Checkpoint_WithActiveReaderHoldingWalFrames_DefersFinalize_AndPreservesSnapshotView()
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
                }
            }
        };

        _db = await Database.OpenAsync(_dbPath, options, ct);
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 10)", ct);
        await _db.CheckpointAsync(ct);

        await _db.ExecuteAsync("UPDATE t SET val = 11 WHERE id = 1", ct);
        await _db.ExecuteAsync("UPDATE t SET val = 12 WHERE id = 1", ct);

        string walPath = _dbPath + ".wal";
        using var reader = _db.CreateReaderSession();
        await using (var snapshotBefore = await reader.ExecuteReadAsync("SELECT val FROM t WHERE id = 1", ct))
        {
            DbValue[] row = Assert.Single(await snapshotBefore.ToListAsync(ct));
            Assert.Equal(12L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("UPDATE t SET val = 13 WHERE id = 1", ct);

        await _db.CheckpointAsync(ct);
        long walSizeAfterCheckpoint = new FileInfo(walPath).Length;

        Assert.True(walSizeAfterCheckpoint > PageConstants.WalHeaderSize);

        await using (var snapshotAfter = await reader.ExecuteReadAsync("SELECT val FROM t WHERE id = 1", ct))
        {
            DbValue[] row = Assert.Single(await snapshotAfter.ToListAsync(ct));
            Assert.Equal(12L, row[0].AsInteger);
        }

        await using var live = await _db.ExecuteAsync("SELECT val FROM t WHERE id = 1", ct);
        var liveRows = await live.ToListAsync(ct);
        Assert.Equal(13L, liveRows[0][0].AsInteger);

        reader.Dispose();
        await _db.CheckpointAsync(ct);

        Assert.Equal(PageConstants.WalHeaderSize, new FileInfo(walPath).Length);
    }

    [Fact]
    public async Task Checkpoint_NewReaderAfterCopyCompletion_DoesNotExtendRetentionOfCopiedWalFrames()
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
                }
            }
        };

        _db = await Database.OpenAsync(_dbPath, options, ct);
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 10)", ct);
        await _db.CheckpointAsync(ct);

        await _db.ExecuteAsync("UPDATE t SET val = 11 WHERE id = 1", ct);
        await _db.ExecuteAsync("UPDATE t SET val = 12 WHERE id = 1", ct);

        string walPath = _dbPath + ".wal";
        using var oldReader = _db.CreateReaderSession();
        await using (var oldSnapshot = await oldReader.ExecuteReadAsync("SELECT val FROM t WHERE id = 1", ct))
        {
            DbValue[] row = Assert.Single(await oldSnapshot.ToListAsync(ct));
            Assert.Equal(12L, row[0].AsInteger);
        }

        await _db.CheckpointAsync(ct);
        Assert.True(new FileInfo(walPath).Length > PageConstants.WalHeaderSize);

        using var newReader = _db.CreateReaderSession();
        await using (var newSnapshot = await newReader.ExecuteReadAsync("SELECT val FROM t WHERE id = 1", ct))
        {
            DbValue[] row = Assert.Single(await newSnapshot.ToListAsync(ct));
            Assert.Equal(12L, row[0].AsInteger);
        }

        oldReader.Dispose();
        await _db.CheckpointAsync(ct);

        Assert.Equal(PageConstants.WalHeaderSize, new FileInfo(walPath).Length);

        await using (var stillReadable = await newReader.ExecuteReadAsync("SELECT val FROM t WHERE id = 1", ct))
        {
            DbValue[] row = Assert.Single(await stillReadable.ToListAsync(ct));
            Assert.Equal(12L, row[0].AsInteger);
        }
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
    public async Task FileWriteAheadLog_OpenAsync_CreatesFreshWalWhenExistingWalDisappearsBeforeRecoveryOpen()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_missing_recover_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        WriteAheadLog? wal = null;

        try
        {
            await using (var existing = new WriteAheadLog(dbPath, new WalIndex()))
            {
                await existing.OpenAsync(currentDbPageCount: 3, ct);
            }

            Assert.True(File.Exists(walPath));
            File.Delete(walPath);

            wal = CreateObservedFileWal(
                dbPath,
                new WalIndex(),
                NoOpStorageRuntimeDiagnosticsObserver.Instance);
            await InvokeRecoverExistingWalOrCreateNewAsync(wal, currentDbPageCount: 3, ct);

            Assert.True(File.Exists(walPath));
            Assert.Equal(PageConstants.WalHeaderSize, new FileInfo(walPath).Length);
            Assert.Equal(0, wal.Index.FrameCount);

            StorageRecoveryRuntimeRawSnapshot recovery =
                GetRecoveryRuntimeSnapshot(wal);
            Assert.Equal(StorageRecoveryPhaseRaw.Scanning, recovery.Phase);
            Assert.Equal(2L, recovery.AttemptCount);
            Assert.Equal(1L, recovery.RetryCount);
            Assert.Equal(
                StorageRuntimeFailureKindRaw.NotFound,
                recovery.LastRetryFailureKind);
            Assert.Equal(StorageRuntimeOperationOutcomeRaw.Running, recovery.Outcome);
            Assert.Equal(StorageRuntimeFailureKindRaw.None, recovery.FailureKind);
            Assert.Equal(StorageRecoveryTruncationReasonRaw.None, recovery.TruncationReason);
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

            long durableFlushesBeforeCompaction = Assert.IsType<long>(
                GetLiveRuntimeSnapshot(wal).DurableFlushCount);
            completed = await wal.CheckpointStepAsync(device, pageCount: 2, maxPages: 8, ct);
            Assert.True(completed);
            Assert.False(wal.HasPendingCheckpoint);
            Assert.Equal(
                durableFlushesBeforeCompaction + 1,
                GetLiveRuntimeSnapshot(wal).DurableFlushCount);
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
    public async Task IncrementalCheckpoint_WithPreallocatedTail_FinalizesUsingLogicalWalLength()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_incremental_prealloc_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        const long preallocationChunkBytes = 64 * 1024;
        await using var device = new MemoryStorageDevice();
        WriteAheadLog? wal = null;

        try
        {
            var walIndex = new WalIndex();
            wal = new WriteAheadLog(
                dbPath,
                walIndex,
                durableCommitBatchWindow: TimeSpan.Zero,
                walPreallocationChunkBytes: preallocationChunkBytes);
            await wal.OpenAsync(currentDbPageCount: 2, ct);

            wal.BeginTransaction();
            await wal.AppendFramesAsync(
                new[]
                {
                    new WalFrameWrite(0, CreateFilledPage(0x21)),
                    new WalFrameWrite(1, CreateFilledPage(0x22)),
                },
                ct);
            await (await wal.CommitAsync(newDbPageCount: 2, ct)).WaitAsync(ct);

            bool completed = await wal.CheckpointStepAsync(device, pageCount: 2, maxPages: 1, ct);
            Assert.False(completed);
            Assert.True(wal.HasPendingCheckpoint);

            for (int i = 0; i < 14; i++)
            {
                wal.BeginTransaction();
                await wal.AppendFrameAsync(1, CreateFilledPage((byte)(0x30 + i)), ct);
                await (await wal.CommitAsync(newDbPageCount: 2, ct)).WaitAsync(ct);
            }

            completed = await wal.CheckpointStepAsync(device, pageCount: 2, maxPages: 8, ct);
            Assert.True(completed);
            Assert.False(wal.HasPendingCheckpoint);
            Assert.Equal(PageConstants.WalHeaderSize + (14L * PageConstants.WalFrameSize), new FileInfo(walPath).Length);

            await wal.DisposeAsync();
            wal = null;

            var reopenedIndex = new WalIndex();
            wal = new WriteAheadLog(dbPath, reopenedIndex);
            await wal.OpenAsync(currentDbPageCount: 2, ct);

            Assert.Equal(14, reopenedIndex.FrameCount);
            Assert.True(reopenedIndex.TryGetLatest(1, out long retainedWalOffset));
            byte[] retainedPage = await wal.ReadPageAsync(retainedWalOffset, ct);
            Assert.All(retainedPage, static b => Assert.Equal((byte)0x3D, b));
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
        Assert.Equal(3L, GetLiveRuntimeSnapshot(wal).LogicalPageWriteCount);

        bool completed = await wal.CheckpointStepAsync(device, pageCount: 3, maxPages: 1, ct);
        Assert.False(completed);
        Assert.True(wal.HasPendingCheckpoint);

        await wal.CheckpointAsync(device, pageCount: 3, ct);

        Assert.False(wal.HasPendingCheckpoint);
        Assert.Equal(0, walIndex.FrameCount);
        Assert.Equal(3L, GetLiveRuntimeSnapshot(wal).LogicalPageWriteCount);
        await AssertPageFilledAsync(device, 0, 0x41, ct);
        await AssertPageFilledAsync(device, 1, 0x42, ct);
        await AssertPageFilledAsync(device, 2, 0x43, ct);
    }

    [Fact]
    public async Task MemoryWal_LiveRuntimeSnapshot_UsesLogicalExtentAndNoAllocatedExtent()
    {
        var ct = TestContext.Current.CancellationToken;
        var walIndex = new WalIndex();
        await using var wal = new MemoryWriteAheadLog(walIndex);

        await wal.OpenAsync(currentDbPageCount: 1, ct);

        WalRuntimeRawSnapshot snapshot = GetLiveRuntimeSnapshot(wal);
        Assert.Equal(PageConstants.WalHeaderSize, snapshot.LogicalBytes);
        Assert.Null(snapshot.AllocatedBytes);
        Assert.Equal(0, snapshot.FrameCount);
        Assert.Equal(0, snapshot.CommittedFrameBytes);
        Assert.Equal(0, snapshot.PendingCommitCount);
        Assert.Equal(0L, snapshot.LogicalCommitCount);
        Assert.Equal(0L, snapshot.LogicalPageWriteCount);
        Assert.Null(snapshot.CommitFlushBatchCount);
        Assert.Null(snapshot.CommittedFrameBytesWritten);

        WalCommitResult commit = await wal.AppendFramesAndCommitAsync(
            new[] { new WalFrameWrite(0, CreateFilledPage(0x45)) },
            newDbPageCount: 1,
            ct);
        await commit.WaitAsync(ct);

        snapshot = GetLiveRuntimeSnapshot(wal);
        Assert.Equal(PageConstants.WalHeaderSize + PageConstants.WalFrameSize, snapshot.LogicalBytes);
        Assert.Null(snapshot.AllocatedBytes);
        Assert.Equal(1, snapshot.FrameCount);
        Assert.Equal(PageConstants.WalFrameSize, snapshot.CommittedFrameBytes);
        Assert.Equal(0, snapshot.PendingCommitCount);
        Assert.Equal(1L, snapshot.LogicalCommitCount);
        Assert.Equal(1L, snapshot.LogicalPageWriteCount);
        Assert.Null(snapshot.CommitFlushBatchCount);
        Assert.Null(snapshot.CommittedFrameBytesWritten);
    }

    [Fact]
    public async Task MemoryWal_LiveRuntimeSnapshot_RetainedBytesAreOnlyPostCheckpointSuffix()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var device = new MemoryStorageDevice();
        var walIndex = new WalIndex();
        await using var wal = new MemoryWriteAheadLog(walIndex);

        await wal.OpenAsync(currentDbPageCount: 1, ct);
        WalCommitResult firstCommit = await wal.AppendFramesAndCommitAsync(
            new[] { new WalFrameWrite(0, CreateFilledPage(0x46)) },
            newDbPageCount: 1,
            ct);
        await firstCommit.WaitAsync(ct);

        await wal.CheckpointAsync(device, pageCount: 1, ct, allowFinalize: false);
        Assert.True(wal.HasPendingCheckpoint);
        Assert.Equal(0, GetLiveRuntimeSnapshot(wal).RetainedBytes);

        WalCommitResult retainedCommit = await wal.AppendFramesAndCommitAsync(
            new[] { new WalFrameWrite(0, CreateFilledPage(0x47)) },
            newDbPageCount: 1,
            ct);
        await retainedCommit.WaitAsync(ct);

        WalRuntimeRawSnapshot snapshot = GetLiveRuntimeSnapshot(wal);
        Assert.Equal(PageConstants.WalFrameSize, snapshot.RetainedBytes);
        Assert.Equal(2, snapshot.FrameCount);
        Assert.Equal(2L * PageConstants.WalFrameSize, snapshot.CommittedFrameBytes);
        Assert.Equal(2L, snapshot.LogicalCommitCount);
        Assert.Equal(2L, snapshot.LogicalPageWriteCount);

        await wal.CheckpointAsync(device, pageCount: 1, ct);
        snapshot = GetLiveRuntimeSnapshot(wal);
        Assert.Equal(1, snapshot.FrameCount);
        Assert.Equal(2L, snapshot.LogicalCommitCount);
        Assert.Equal(2L, snapshot.LogicalPageWriteCount);

        await wal.CheckpointAsync(device, pageCount: 1, ct);
        snapshot = GetLiveRuntimeSnapshot(wal);
        Assert.Equal(0, snapshot.FrameCount);
        Assert.Equal(2L, snapshot.LogicalCommitCount);
        Assert.Equal(2L, snapshot.LogicalPageWriteCount);
    }

    [Fact]
    public async Task MemoryWal_LiveRuntimeSnapshot_RetriesCommitPublishedBetweenSamples()
    {
        var ct = TestContext.Current.CancellationToken;
        var walIndex = new WalIndex();
        await using var wal = new MemoryWriteAheadLog(walIndex);
        await wal.OpenAsync(currentDbPageCount: 1, ct);

        int hookInvoked = 0;
        wal.RuntimeDiagnosticsBetweenSnapshotSamplesForTests = () =>
        {
            if (Interlocked.Exchange(ref hookInvoked, 1) != 0)
                return;

            WalCommitResult commit = wal.AppendFramesAndCommitAsync(
                    new[] { new WalFrameWrite(0, CreateFilledPage(0x48)) },
                    newDbPageCount: 1,
                    ct)
                .AsTask()
                .GetAwaiter()
                .GetResult();
            commit.WaitAsync(ct).AsTask().GetAwaiter().GetResult();
        };

        WalRuntimeRawSnapshot snapshot = GetLiveRuntimeSnapshot(wal);
        wal.RuntimeDiagnosticsBetweenSnapshotSamplesForTests = null;

        Assert.Equal(1, hookInvoked);
        Assert.Equal(PageConstants.WalHeaderSize + PageConstants.WalFrameSize, snapshot.LogicalBytes);
        Assert.Equal(1, snapshot.FrameCount);
        Assert.Equal(PageConstants.WalFrameSize, snapshot.CommittedFrameBytes);
        Assert.Equal(1L, snapshot.LogicalCommitCount);
        Assert.Equal(1L, snapshot.LogicalPageWriteCount);
        Assert.Null(snapshot.CommitFlushBatchCount);
        Assert.Null(snapshot.CommittedFrameBytesWritten);
        Assert.True(
            snapshot.CommittedFrameBytes <=
            snapshot.LogicalBytes - PageConstants.WalHeaderSize);
    }

    [Fact]
    public async Task FileWriteAheadLog_CheckpointAsync_CopiesPagesButDefersFinalizeWhenRequested()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_partial_checkpoint_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        FileStorageDevice? device = null;
        var walIndex = new WalIndex();
        WriteAheadLog? wal = null;

        try
        {
            device = new FileStorageDevice(dbPath, createNew: true);
            wal = new WriteAheadLog(dbPath, walIndex);
            await wal.OpenAsync(currentDbPageCount: 2, ct);

            wal.BeginTransaction();
            await wal.AppendFramesAsync(
                new[]
                {
                    new WalFrameWrite(0, CreateFilledPage(0x61)),
                    new WalFrameWrite(1, CreateFilledPage(0x62)),
                },
                ct);
            await (await wal.CommitAsync(newDbPageCount: 2, ct)).WaitAsync(ct);

            await wal.CheckpointAsync(device, pageCount: 2, ct, allowFinalize: false);

            Assert.True(wal.HasPendingCheckpoint);
            Assert.True(wal.IsCheckpointCopyComplete);
            Assert.Equal(2, walIndex.FrameCount);
            Assert.Equal(PageConstants.WalHeaderSize + (2L * PageConstants.WalFrameSize), new FileInfo(walPath).Length);
            await AssertPageFilledAsync(device, 0, 0x61, ct);
            await AssertPageFilledAsync(device, 1, 0x62, ct);

            wal.BeginTransaction();
            await wal.AppendFrameAsync(1, CreateFilledPage(0x63), ct);
            await (await wal.CommitAsync(newDbPageCount: 2, ct)).WaitAsync(ct);

            await wal.CheckpointAsync(device, pageCount: 2, ct);

            Assert.False(wal.HasPendingCheckpoint);
            Assert.Equal(1, walIndex.FrameCount);
            Assert.Equal(
                PageConstants.WalHeaderSize + PageConstants.WalFrameSize,
                new FileInfo(walPath).Length);

            WalRuntimeRawSnapshot afterReplace = GetLiveRuntimeSnapshot(wal);
            Assert.Equal(2L, afterReplace.LogicalCommitCount);
            Assert.Equal(3L, afterReplace.LogicalPageWriteCount);
            Assert.Equal(2L, afterReplace.CommitFlushBatchCount);
            Assert.Equal(
                3L * PageConstants.WalFrameSize,
                afterReplace.CommittedFrameBytesWritten);

            await wal.CheckpointAsync(device, pageCount: 2, ct);

            Assert.Equal(0, walIndex.FrameCount);
            Assert.Equal(PageConstants.WalHeaderSize, new FileInfo(walPath).Length);
            await AssertPageFilledAsync(device, 1, 0x63, ct);

            WalRuntimeRawSnapshot afterReset = GetLiveRuntimeSnapshot(wal);
            Assert.Equal(2L, afterReset.LogicalCommitCount);
            Assert.Equal(3L, afterReset.LogicalPageWriteCount);
            Assert.Equal(2L, afterReset.CommitFlushBatchCount);
            Assert.Equal(
                3L * PageConstants.WalFrameSize,
                afterReset.CommittedFrameBytesWritten);
        }
        finally
        {
            if (wal is not null)
                await wal.CloseAndDeleteAsync();
            if (device is not null)
                await device.DisposeAsync();
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
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
    public async Task FileWriteAheadLog_LifetimeDurabilityCounters_SeparateDurableFlushesFromPublication()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_wal_durable_lifetime_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var observer = new WalLifetimeObserver();
        WriteAheadLog? wal = null;

        try
        {
            wal = new WriteAheadLog(
                dbPath,
                new WalIndex(),
                checksumProvider: null,
                durabilityMode: DurabilityMode.Durable,
                durableCommitBatchWindow: TimeSpan.Zero,
                walPreallocationChunkBytes: 0,
                runtimeDiagnosticsObserver: observer);
            await wal.OpenAsync(currentDbPageCount: 1, ct);

            WalRuntimeRawSnapshot opened = GetLiveRuntimeSnapshot(wal);
            Assert.Equal(0L, opened.CommitFlushBatchCount);
            Assert.Equal(0L, opened.FlushedCommitCount);
            Assert.Equal(1L, opened.DurableFlushCount);
            Assert.Equal(0L, opened.GroupCommitBatchCount);
            Assert.Equal(0L, opened.GroupCommitCount);

            await CommitFileWalFrameAsync(
                wal,
                pageId: 0,
                value: 0x64,
                ct);

            WalRuntimeRawSnapshot committed = GetLiveRuntimeSnapshot(wal);
            Assert.Equal(1L, committed.CommitFlushBatchCount);
            Assert.Equal(1L, committed.FlushedCommitCount);
            Assert.Equal(2L, committed.DurableFlushCount);
            Assert.Equal(0L, committed.GroupCommitBatchCount);
            Assert.Equal(0L, committed.GroupCommitCount);
            Assert.Equal(1, observer.PublicationCallbackCount);
            Assert.Equal(1, observer.LastPublicationLogicalCommitCount);
            Assert.Equal(2, observer.DurableCallbackCount);
            Assert.Equal(2L, observer.LastDurableFlushCount);

            ((IWalRuntimeDiagnosticsProvider)wal)
                .ResetWalFlushDiagnostics();
            ((ICommitPathDiagnosticsProvider)wal)
                .ResetCommitPathDiagnostics();
            Assert.Equal(
                committed,
                GetLiveRuntimeSnapshot(wal));

            await using var device = new MemoryStorageDevice();
            await wal.CheckpointAsync(device, pageCount: 1, ct);

            WalRuntimeRawSnapshot checkpointed = GetLiveRuntimeSnapshot(wal);
            Assert.Equal(1L, checkpointed.CommitFlushBatchCount);
            Assert.Equal(1L, checkpointed.FlushedCommitCount);
            Assert.Equal(3L, checkpointed.DurableFlushCount);
            Assert.Equal(0L, checkpointed.GroupCommitBatchCount);
            Assert.Equal(0L, checkpointed.GroupCommitCount);
            Assert.Equal(1, observer.PublicationCallbackCount);
            Assert.Equal(3, observer.DurableCallbackCount);
            Assert.Equal(3L, observer.LastDurableFlushCount);
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
    public async Task FileWriteAheadLog_BufferedPublication_HasKnownZeroDurableAndGroupCounters()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_wal_buffered_lifetime_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var observer = new WalLifetimeObserver();
        WriteAheadLog? wal = null;

        try
        {
            wal = new WriteAheadLog(
                dbPath,
                new WalIndex(),
                checksumProvider: null,
                durabilityMode: DurabilityMode.Buffered,
                durableCommitBatchWindow: TimeSpan.Zero,
                walPreallocationChunkBytes: 0,
                runtimeDiagnosticsObserver: observer);
            await wal.OpenAsync(currentDbPageCount: 1, ct);
            await CommitFileWalFrameAsync(
                wal,
                pageId: 0,
                value: 0x65,
                ct);

            WalRuntimeRawSnapshot snapshot = GetLiveRuntimeSnapshot(wal);
            Assert.Equal(1L, snapshot.CommitFlushBatchCount);
            Assert.Equal(1L, snapshot.FlushedCommitCount);
            Assert.Equal(0L, snapshot.DurableFlushCount);
            Assert.Equal(0L, snapshot.GroupCommitBatchCount);
            Assert.Equal(0L, snapshot.GroupCommitCount);
            Assert.Equal(1, observer.PublicationCallbackCount);
            Assert.Equal(0, observer.DurableCallbackCount);
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
    public async Task FileWriteAheadLog_DurableCommit_UsesCommitFlushWithoutBufferedStreamFlush()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_direct_flush_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var policy = new CountingWalFlushPolicy(allowsWriteConcurrencyDuringCommitFlush: true);
        WriteAheadLog? wal = null;

        try
        {
            wal = new WriteAheadLog(dbPath, new WalIndex(), checksumProvider: null, flushPolicy: policy);
            await wal.OpenAsync(currentDbPageCount: 1, ct);
            policy.Reset();

            wal.BeginTransaction();
            await wal.AppendFrameAsync(0, CreateFilledPage(0x63), ct);

            await (await wal.CommitAsync(newDbPageCount: 1, ct)).WaitAsync(ct);

            Assert.Equal(1, policy.CommitFlushCount);
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
            wal = new WriteAheadLog(
                dbPath,
                new WalIndex(),
                checksumProvider: null,
                flushPolicy: policy,
                durableCommitBatchWindow: TimeSpan.FromMilliseconds(5));
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
        TimeSpan batchWindow = TimeSpan.FromMilliseconds(250);

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
            Assert.True(diagnostics.BatchWindowWaitCount > 0);
            Assert.True(wal.Index.TryGetLatest(0, out _));
            Assert.True(wal.Index.TryGetLatest(1, out _));

            WalRuntimeRawSnapshot runtime = GetLiveRuntimeSnapshot(wal);
            Assert.Equal(2L, runtime.LogicalCommitCount);
            Assert.Equal(2L, runtime.LogicalPageWriteCount);
            Assert.Equal(1L, runtime.CommitFlushBatchCount);
            Assert.Equal(2L, runtime.FlushedCommitCount);
            Assert.Equal(1L, runtime.GroupCommitBatchCount);
            Assert.Equal(2L, runtime.GroupCommitCount);
            Assert.Equal(
                2L * PageConstants.WalFrameSize,
                runtime.CommittedFrameBytesWritten);
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
    public async Task FileWriteAheadLog_DurableAppendFramesAndCommits_CanShareOneFlush()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_group_append_commit_{Guid.NewGuid():N}.db");
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
                durableCommitBatchWindow: TimeSpan.FromMilliseconds(250));
            await wal.OpenAsync(currentDbPageCount: 2, ct);

            WalCommitResult commit1 = await wal.AppendFramesAndCommitAsync(
                new[] { new WalFrameWrite(0, CreateFilledPage(0x91)) },
                newDbPageCount: 2,
                ct);
            WalCommitResult commit2 = await wal.AppendFramesAndCommitAsync(
                new[] { new WalFrameWrite(1, CreateFilledPage(0x92)) },
                newDbPageCount: 2,
                ct);

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
            Assert.True(diagnostics.BatchWindowWaitCount > 0);
            Assert.True(wal.Index.TryGetLatest(0, out _));
            Assert.True(wal.Index.TryGetLatest(1, out _));

            WalRuntimeRawSnapshot runtime = GetLiveRuntimeSnapshot(wal);
            Assert.Equal(2L, runtime.LogicalCommitCount);
            Assert.Equal(2L, runtime.LogicalPageWriteCount);
            Assert.Equal(1L, runtime.CommitFlushBatchCount);
            Assert.Equal(
                2L * PageConstants.WalFrameSize,
                runtime.CommittedFrameBytesWritten);
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
    public async Task FileWriteAheadLog_ZeroBatchWindow_AppendsAndCommitsInline()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_inline_append_commit_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var policy = new CountingWalFlushPolicy(allowsWriteConcurrencyDuringCommitFlush: true);
        WriteAheadLog? wal = null;

        try
        {
            wal = new WriteAheadLog(
                dbPath,
                new WalIndex(),
                checksumProvider: null,
                flushPolicy: policy,
                durableCommitBatchWindow: TimeSpan.Zero);
            await wal.OpenAsync(currentDbPageCount: 2, ct);
            policy.Reset();

            WalCommitResult commit = await wal.AppendFramesAndCommitAsync(
                new[] { new WalFrameWrite(0, CreateFilledPage(0x95)) },
                newDbPageCount: 2,
                ct);

            ValueTask wait = commit.WaitAsync(ct);
            Assert.True(wait.IsCompletedSuccessfully);
            await wait;

            WalFlushDiagnosticsSnapshot diagnostics =
                ((IWalRuntimeDiagnosticsProvider)wal).GetWalFlushDiagnosticsSnapshot();

            Assert.False(wal.HasPendingCommitWork);
            Assert.Equal(1, wal.Index.FrameCount);
            Assert.Equal(1, policy.CommitFlushCount);
            Assert.Equal(1, diagnostics.FlushCount);
            Assert.Equal(1, diagnostics.FlushedCommitCount);
            Assert.Equal(0, diagnostics.BatchWindowWaitCount);
            Assert.True(wal.Index.TryGetLatest(0, out _));

            WalRuntimeRawSnapshot runtime = GetLiveRuntimeSnapshot(wal);
            Assert.Equal(1L, runtime.LogicalCommitCount);
            Assert.Equal(1L, runtime.LogicalPageWriteCount);
            Assert.Equal(1L, runtime.CommitFlushBatchCount);
            Assert.Equal(
                PageConstants.WalFrameSize,
                runtime.CommittedFrameBytesWritten);
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
    public async Task FileWriteAheadLog_LiveRuntimeSnapshot_IncludesBlockedInlineCommitAndPreallocatedExtent()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_live_inline_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var policy = new BlockingCommitWalFlushPolicy();
        WriteAheadLog? wal = null;

        try
        {
            const long preallocationChunkBytes = 64 * 1024;
            wal = new WriteAheadLog(
                dbPath,
                new WalIndex(),
                checksumProvider: null,
                flushPolicy: policy,
                durableCommitBatchWindow: TimeSpan.Zero,
                walPreallocationChunkBytes: preallocationChunkBytes);
            await wal.OpenAsync(currentDbPageCount: 1, ct);

            Task<WalCommitResult> appendTask = wal.AppendFramesAndCommitAsync(
                new[] { new WalFrameWrite(0, CreateFilledPage(0x96)) },
                newDbPageCount: 1,
                ct).AsTask();

            await policy.WaitForCommitFlushStartAsync(ct);
            Assert.False(appendTask.IsCompleted);

            WalRuntimeRawSnapshot snapshot = GetLiveRuntimeSnapshot(wal);
            Assert.Equal(1, snapshot.PendingCommitCount);
            Assert.Equal(0, snapshot.FrameCount);
            Assert.Equal(0, snapshot.CommittedFrameBytes);
            Assert.Equal(PageConstants.WalHeaderSize + PageConstants.WalFrameSize, snapshot.LogicalBytes);
            Assert.Equal(preallocationChunkBytes, snapshot.AllocatedBytes);
            Assert.Equal(0L, snapshot.LogicalCommitCount);
            Assert.Equal(0L, snapshot.LogicalPageWriteCount);
            Assert.Equal(0L, snapshot.CommitFlushBatchCount);
            Assert.Equal(0L, snapshot.CommittedFrameBytesWritten);

            policy.Release();
            WalCommitResult commit = await appendTask;
            await commit.WaitAsync(ct);

            snapshot = GetLiveRuntimeSnapshot(wal);
            Assert.Equal(0, snapshot.PendingCommitCount);
            Assert.Equal(1, snapshot.FrameCount);
            Assert.Equal(PageConstants.WalFrameSize, snapshot.CommittedFrameBytes);
            Assert.Equal(PageConstants.WalHeaderSize + PageConstants.WalFrameSize, snapshot.LogicalBytes);
            Assert.Equal(preallocationChunkBytes, snapshot.AllocatedBytes);
            Assert.Equal(1L, snapshot.LogicalCommitCount);
            Assert.Equal(1L, snapshot.LogicalPageWriteCount);
            Assert.Equal(1L, snapshot.CommitFlushBatchCount);
            Assert.Equal(PageConstants.WalFrameSize, snapshot.CommittedFrameBytesWritten);

            var diagnostics = (IWalRuntimeDiagnosticsProvider)wal;
            diagnostics.ResetWalFlushDiagnostics();
            Assert.Equal(
                WalFlushDiagnosticsSnapshot.Empty,
                diagnostics.GetWalFlushDiagnosticsSnapshot());

            WalRuntimeRawSnapshot afterReset = GetLiveRuntimeSnapshot(wal);
            Assert.Equal(1L, afterReset.LogicalCommitCount);
            Assert.Equal(1L, afterReset.LogicalPageWriteCount);
            Assert.Equal(1L, afterReset.CommitFlushBatchCount);
            Assert.Equal(PageConstants.WalFrameSize, afterReset.CommittedFrameBytesWritten);
        }
        finally
        {
            policy.Release();
            if (wal is not null)
                await wal.CloseAndDeleteAsync();
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Fact]
    public async Task FileWriteAheadLog_LifetimeCounters_StickAtSaturationAcrossBenchmarkReset()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_lifetime_saturation_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var walIndex = new WalIndex();
        WriteAheadLog? wal = null;

        try
        {
            wal = new WriteAheadLog(
                dbPath,
                walIndex,
                checksumProvider: null,
                flushPolicy: new CountingWalFlushPolicy(
                    allowsWriteConcurrencyDuringCommitFlush: true),
                durableCommitBatchWindow: TimeSpan.Zero);
            await wal.OpenAsync(currentDbPageCount: 1, ct);

            SetPrivateInt64Field(walIndex, "_logicalCommitCount", long.MaxValue - 1);
            SetPrivateInt64Field(walIndex, "_logicalPageWriteCount", long.MaxValue - 1);
            SetPrivateInt64Field(wal, "_flushCount", long.MaxValue);
            SetPrivateInt64Field(wal, "_flushedCommitCount", long.MaxValue);
            SetPrivateInt64Field(wal, "_flushedByteCount", long.MaxValue);
            SetPrivateInt64Field(wal, "_groupCommitBatchCount", long.MaxValue);
            SetPrivateInt64Field(wal, "_groupCommitCount", long.MaxValue);

            WalCommitResult commit = await wal.AppendFramesAndCommitAsync(
                new[] { new WalFrameWrite(0, CreateFilledPage(0x97)) },
                newDbPageCount: 1,
                ct);
            await commit.WaitAsync(ct);

            WalRuntimeRawSnapshot saturated = GetLiveRuntimeSnapshot(wal);
            Assert.Equal(long.MaxValue, saturated.LogicalCommitCount);
            Assert.Equal(long.MaxValue, saturated.LogicalPageWriteCount);
            Assert.Equal(long.MaxValue, saturated.CommitFlushBatchCount);
            Assert.Equal(long.MaxValue, saturated.CommittedFrameBytesWritten);
            Assert.Equal(long.MaxValue, saturated.FlushedCommitCount);
            Assert.Equal(0L, saturated.DurableFlushCount);
            Assert.Equal(long.MaxValue, saturated.GroupCommitBatchCount);
            Assert.Equal(long.MaxValue, saturated.GroupCommitCount);

            var diagnostics = (IWalRuntimeDiagnosticsProvider)wal;
            diagnostics.ResetWalFlushDiagnostics();
            Assert.Equal(0, diagnostics.GetWalFlushDiagnosticsSnapshot().FlushCount);
            Assert.Equal(0, diagnostics.GetWalFlushDiagnosticsSnapshot().FlushedCommitCount);
            Assert.Equal(0, diagnostics.GetWalFlushDiagnosticsSnapshot().FlushedByteCount);

            WalRuntimeRawSnapshot afterReset = GetLiveRuntimeSnapshot(wal);
            Assert.Equal(long.MaxValue, afterReset.LogicalCommitCount);
            Assert.Equal(long.MaxValue, afterReset.LogicalPageWriteCount);
            Assert.Equal(long.MaxValue, afterReset.CommitFlushBatchCount);
            Assert.Equal(long.MaxValue, afterReset.CommittedFrameBytesWritten);
            Assert.Equal(long.MaxValue, afterReset.FlushedCommitCount);
            Assert.Equal(0L, afterReset.DurableFlushCount);
            Assert.Equal(long.MaxValue, afterReset.GroupCommitBatchCount);
            Assert.Equal(long.MaxValue, afterReset.GroupCommitCount);
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
    public async Task FileWriteAheadLog_ResetBaselines_RejectStaleResetAndExposeLaterCommit()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_reset_baseline_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var resetTotalsRead = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var allowStaleReset = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        WriteAheadLog? wal = null;
        Task? staleResetTask = null;

        try
        {
            wal = new WriteAheadLog(
                dbPath,
                new WalIndex(),
                checksumProvider: null,
                flushPolicy: new CountingWalFlushPolicy(
                    allowsWriteConcurrencyDuringCommitFlush: true),
                durableCommitBatchWindow: TimeSpan.Zero);
            await wal.OpenAsync(currentDbPageCount: 1, ct);
            var diagnostics = (IWalRuntimeDiagnosticsProvider)wal;

            await CommitFileWalFrameAsync(wal, pageId: 0, value: 0x98, ct);

            wal.RuntimeDiagnosticsAfterFlushResetTotalsReadForTests = () =>
            {
                resetTotalsRead.TrySetResult();
                allowStaleReset.Task.GetAwaiter().GetResult();
            };
            staleResetTask = Task.Run(diagnostics.ResetWalFlushDiagnostics, ct);
            await resetTotalsRead.Task.WaitAsync(ct);

            wal.RuntimeDiagnosticsAfterFlushResetTotalsReadForTests = null;
            await CommitFileWalFrameAsync(wal, pageId: 0, value: 0x99, ct);
            diagnostics.ResetWalFlushDiagnostics();

            allowStaleReset.TrySetResult();
            await staleResetTask;

            Assert.Equal(
                WalFlushDiagnosticsSnapshot.Empty,
                diagnostics.GetWalFlushDiagnosticsSnapshot());

            await CommitFileWalFrameAsync(wal, pageId: 0, value: 0x9A, ct);

            WalFlushDiagnosticsSnapshot sinceReset =
                diagnostics.GetWalFlushDiagnosticsSnapshot();
            Assert.Equal(1, sinceReset.FlushCount);
            Assert.Equal(1, sinceReset.FlushedCommitCount);
            Assert.Equal(PageConstants.WalFrameSize, sinceReset.FlushedByteCount);

            WalRuntimeRawSnapshot lifetime = GetLiveRuntimeSnapshot(wal);
            Assert.Equal(3L, lifetime.LogicalCommitCount);
            Assert.Equal(3L, lifetime.LogicalPageWriteCount);
            Assert.Equal(3L, lifetime.CommitFlushBatchCount);
            Assert.Equal(
                3L * PageConstants.WalFrameSize,
                lifetime.CommittedFrameBytesWritten);
        }
        finally
        {
            if (wal is not null)
                wal.RuntimeDiagnosticsAfterFlushResetTotalsReadForTests = null;
            allowStaleReset.TrySetResult();
            if (staleResetTask is not null)
                await staleResetTask;
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
        byte[] seedBytes;

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

                WalRuntimeRawSnapshot written = GetLiveRuntimeSnapshot(wal);
                Assert.Equal(1L, written.LogicalCommitCount);
                Assert.Equal(1L, written.LogicalPageWriteCount);
                Assert.Equal(1L, written.CommitFlushBatchCount);
                Assert.Equal(PageConstants.WalFrameSize, written.CommittedFrameBytesWritten);
            }

            seedBytes = await File.ReadAllBytesAsync(walPath, ct);

            await using var reopened = new WriteAheadLog(dbPath, new WalIndex());
            await reopened.OpenAsync(currentDbPageCount: 1, ct);

            Assert.Equal(1, reopened.Index.FrameCount);
            Assert.True(reopened.Index.TryGetLatest(0, out _));
            Assert.Equal(PageConstants.WalHeaderSize + PageConstants.WalFrameSize, new FileInfo(walPath).Length);

            WalRuntimeRawSnapshot recovered = GetLiveRuntimeSnapshot(reopened);
            Assert.Equal(0L, recovered.LogicalCommitCount);
            Assert.Equal(0L, recovered.LogicalPageWriteCount);
            Assert.Equal(0L, recovered.CommitFlushBatchCount);
            Assert.Equal(0L, recovered.CommittedFrameBytesWritten);

            var memoryIndex = new WalIndex();
            await using var seededMemoryWal = new MemoryWriteAheadLog(
                memoryIndex,
                initialBytes: seedBytes);
            await seededMemoryWal.OpenAsync(currentDbPageCount: 1, ct);

            WalRuntimeRawSnapshot memoryRecovered = GetLiveRuntimeSnapshot(seededMemoryWal);
            Assert.Equal(1, memoryRecovered.FrameCount);
            Assert.Equal(0L, memoryRecovered.LogicalCommitCount);
            Assert.Equal(0L, memoryRecovered.LogicalPageWriteCount);
            Assert.Null(memoryRecovered.CommitFlushBatchCount);
            Assert.Null(memoryRecovered.CommittedFrameBytesWritten);
            Assert.Null(memoryRecovered.FlushedCommitCount);
            Assert.Null(memoryRecovered.DurableFlushCount);
            Assert.Null(memoryRecovered.GroupCommitBatchCount);
            Assert.Null(memoryRecovered.GroupCommitCount);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Fact]
    public async Task RecoveryRuntimeSnapshot_IncompleteTail_IsCountedAndTruncatedForFileAndMemoryWal()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_wal_incomplete_tail_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        const int partialTailBytes = 17;

        try
        {
            await using (var seed = new WriteAheadLog(dbPath, new WalIndex()))
            {
                await seed.OpenAsync(currentDbPageCount: 2, ct);
                WalCommitResult commit = await seed.AppendFramesAndCommitAsync(
                    new[]
                    {
                        new WalFrameWrite(0, CreateFilledPage(0xA1)),
                        new WalFrameWrite(1, CreateFilledPage(0xA2)),
                    },
                    newDbPageCount: 2,
                    ct);
                await commit.WaitAsync(ct);
            }

            await using (var append = new FileStream(
                walPath,
                FileMode.Append,
                FileAccess.Write,
                FileShare.Read))
            {
                await append.WriteAsync(new byte[partialTailBytes], ct);
            }

            byte[] seedBytes = await File.ReadAllBytesAsync(walPath, ct);
            long expectedFinalLength =
                PageConstants.WalHeaderSize + (2L * PageConstants.WalFrameSize);
            long expectedScannedBytes =
                (2L * PageConstants.WalFrameSize) + partialTailBytes;

            await using (var fileWal = CreateObservedFileWal(
                dbPath,
                new WalIndex(),
                NoOpStorageRuntimeDiagnosticsObserver.Instance))
            {
                await fileWal.OpenAsync(currentDbPageCount: 2, ct);
                AssertRecoveryScan(
                    GetRecoveryRuntimeSnapshot(fileWal),
                    scannedFrameCount: 3,
                    scannedBytes: expectedScannedBytes,
                    recoveredFrameCount: 2,
                    discardedFrameCount: 1,
                    discardedBytes: partialTailBytes,
                    StorageRecoveryTruncationReasonRaw.IncompleteTail);
                Assert.Equal(expectedFinalLength, new FileInfo(walPath).Length);
            }

            await using var memoryWal = CreateObservedMemoryWal(
                new WalIndex(),
                seedBytes,
                NoOpStorageRuntimeDiagnosticsObserver.Instance);
            await memoryWal.OpenAsync(currentDbPageCount: 2, ct);
            AssertRecoveryScan(
                GetRecoveryRuntimeSnapshot(memoryWal),
                scannedFrameCount: 3,
                scannedBytes: expectedScannedBytes,
                recoveredFrameCount: 2,
                discardedFrameCount: 1,
                discardedBytes: partialTailBytes,
                StorageRecoveryTruncationReasonRaw.IncompleteTail);
            Assert.Equal(expectedFinalLength, GetLiveRuntimeSnapshot(memoryWal).LogicalBytes);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Fact]
    public async Task RecoveryRuntimeSnapshot_EarlySaltAndChecksumMismatch_CountOnlyExaminedCandidate()
    {
        var ct = TestContext.Current.CancellationToken;

        await AssertCorruptRecoveryCandidateAsync(
            corruptSalt: true,
            StorageRecoveryTruncationReasonRaw.SaltMismatch,
            ct);
        await AssertCorruptRecoveryCandidateAsync(
            corruptSalt: false,
            StorageRecoveryTruncationReasonRaw.ChecksumMismatch,
            ct);
    }

    [Fact]
    public async Task RecoveryRuntimeSnapshot_UncommittedBoundaryWinsBeforeLaterInvalidFrame()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_wal_uncommitted_boundary_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";

        try
        {
            await using (var seed = new WriteAheadLog(dbPath, new WalIndex()))
            {
                await seed.OpenAsync(currentDbPageCount: 3, ct);
                WalCommitResult firstCommit = await seed.AppendFramesAndCommitAsync(
                    new[] { new WalFrameWrite(0, CreateFilledPage(0xB1)) },
                    newDbPageCount: 3,
                    ct);
                await firstCommit.WaitAsync(ct);

                WalCommitResult secondCommit = await seed.AppendFramesAndCommitAsync(
                    new[]
                    {
                        new WalFrameWrite(0, CreateFilledPage(0xB2)),
                        new WalFrameWrite(1, CreateFilledPage(0xB3)),
                        new WalFrameWrite(2, CreateFilledPage(0xB4)),
                    },
                    newDbPageCount: 3,
                    ct);
                await secondCommit.WaitAsync(ct);
            }

            long invalidCommitFrameOffset =
                PageConstants.WalHeaderSize + (3L * PageConstants.WalFrameSize);
            await FlipWalByteAsync(
                walPath,
                invalidCommitFrameOffset + 8,
                ct);
            byte[] seedBytes = await File.ReadAllBytesAsync(walPath, ct);

            await using (var fileWal = CreateObservedFileWal(
                dbPath,
                new WalIndex(),
                NoOpStorageRuntimeDiagnosticsObserver.Instance))
            {
                await fileWal.OpenAsync(currentDbPageCount: 3, ct);
                AssertRecoveryScan(
                    GetRecoveryRuntimeSnapshot(fileWal),
                    scannedFrameCount: 4,
                    scannedBytes: 4L * PageConstants.WalFrameSize,
                    recoveredFrameCount: 1,
                    discardedFrameCount: 3,
                    discardedBytes: 3L * PageConstants.WalFrameSize,
                    StorageRecoveryTruncationReasonRaw.UncommittedTail);
            }

            await using var memoryWal = CreateObservedMemoryWal(
                new WalIndex(),
                seedBytes,
                NoOpStorageRuntimeDiagnosticsObserver.Instance);
            await memoryWal.OpenAsync(currentDbPageCount: 3, ct);
            AssertRecoveryScan(
                GetRecoveryRuntimeSnapshot(memoryWal),
                scannedFrameCount: 4,
                scannedBytes: 4L * PageConstants.WalFrameSize,
                recoveredFrameCount: 1,
                discardedFrameCount: 3,
                discardedBytes: 3L * PageConstants.WalFrameSize,
                StorageRecoveryTruncationReasonRaw.UncommittedTail);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Fact]
    public async Task RecoveryRuntimeSnapshot_MissingWalStartsOneEmptyAttempt()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_wal_empty_recovery_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";

        try
        {
            await using var wal = CreateObservedFileWal(
                dbPath,
                new WalIndex(),
                NoOpStorageRuntimeDiagnosticsObserver.Instance);
            await wal.OpenAsync(currentDbPageCount: 1, ct);

            AssertRecoveryScan(
                GetRecoveryRuntimeSnapshot(wal),
                scannedFrameCount: 0,
                scannedBytes: 0,
                recoveredFrameCount: 0,
                discardedFrameCount: 0,
                discardedBytes: 0,
                StorageRecoveryTruncationReasonRaw.None);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Fact]
    public async Task RecoveryRuntimeSnapshot_SaturatedAttemptKeepsRetryInvariantAndLastFailure()
    {
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_wal_recovery_saturation_{Guid.NewGuid():N}.db");
        await using var wal = CreateObservedFileWal(
            dbPath,
            new WalIndex(),
            NoOpStorageRuntimeDiagnosticsObserver.Instance);

        SetPrivateInt64Field(wal, "_recoveryAttemptCount", long.MaxValue);
        SetPrivateInt64Field(wal, "_recoveryRetryCount", long.MaxValue - 2);

        MethodInfo recordRetry = typeof(WriteAheadLog).GetMethod(
            "RecordRecoveryRuntimeRetry",
            BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("RecordRecoveryRuntimeRetry was not found.");
        MethodInfo beginAttempt = typeof(WriteAheadLog).GetMethod(
            "BeginRecoveryRuntimeAttempt",
            BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("BeginRecoveryRuntimeAttempt was not found.");

        Assert.Equal(long.MaxValue, Assert.IsType<long>(beginAttempt.Invoke(wal, null)));
        _ = recordRetry.Invoke(wal, [new FileNotFoundException()]);

        StorageRecoveryRuntimeRawSnapshot snapshot = GetRecoveryRuntimeSnapshot(wal);
        Assert.Equal(long.MaxValue, snapshot.AttemptCount);
        Assert.Equal(long.MaxValue - 1, snapshot.RetryCount);
        Assert.True(snapshot.RetryCount < snapshot.AttemptCount);
        Assert.Equal(
            StorageRuntimeFailureKindRaw.NotFound,
            snapshot.LastRetryFailureKind);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(100)]
    public async Task WalFlushObserver_RunsAfterTotalsAndOutstandingGaugeButBeforeCompletion(
        int batchWindowMilliseconds)
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_wal_flush_observer_order_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var observer = new WalFlushOrderingObserver();

        try
        {
            await using var wal = new WriteAheadLog(
                dbPath,
                new WalIndex(),
                checksumProvider: null,
                durabilityMode: DurabilityMode.Durable,
                durableCommitBatchWindow:
                    TimeSpan.FromMilliseconds(batchWindowMilliseconds),
                walPreallocationChunkBytes: 0,
                runtimeDiagnosticsObserver: observer);
            observer.AttachWal(wal);
            await wal.OpenAsync(currentDbPageCount: 1, ct);

            WalCommitResult firstCommit = await wal.AppendFramesAndCommitAsync(
                new[] { new WalFrameWrite(0, CreateFilledPage(0xCE)) },
                newDbPageCount: 1,
                ct);
            WalCommitResult secondCommit = await wal.AppendFramesAndCommitAsync(
                new[] { new WalFrameWrite(0, CreateFilledPage(0xCF)) },
                newDbPageCount: 1,
                ct);
            if (batchWindowMilliseconds == 0)
                Assert.Equal(2, observer.FlushCallbackCount);

            Task completion = Task.WhenAll(
                firstCommit.WaitAsync(ct).AsTask(),
                secondCommit.WaitAsync(ct).AsTask());
            observer.AttachCompletion(completion);
            await completion;

            int expectedFlushCallbackCount = batchWindowMilliseconds == 0 ? 2 : 1;
            Assert.Equal(expectedFlushCallbackCount, observer.FlushCallbackCount);
            Assert.False(observer.CompletionWasAlreadySignaled);
            Assert.Equal(
                (long)expectedFlushCallbackCount,
                observer.Snapshot.CommitFlushBatchCount);
            Assert.Equal(
                2L * PageConstants.WalFrameSize,
                observer.Snapshot.CommittedFrameBytesWritten);
            Assert.Equal(0, observer.Snapshot.PendingCommitCount);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Fact]
    public async Task WalFlushObserverFailure_DoesNotAlterDurableCommit()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_wal_flush_observer_failure_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";

        try
        {
            await using var wal = new WriteAheadLog(
                dbPath,
                new WalIndex(),
                checksumProvider: null,
                durabilityMode: DurabilityMode.Durable,
                durableCommitBatchWindow: TimeSpan.Zero,
                walPreallocationChunkBytes: 0,
                runtimeDiagnosticsObserver: new ThrowingWalFlushObserver());
            await wal.OpenAsync(currentDbPageCount: 1, ct);

            WalCommitResult commit = await wal.AppendFramesAndCommitAsync(
                new[] { new WalFrameWrite(0, CreateFilledPage(0xD0)) },
                newDbPageCount: 1,
                ct);
            await commit.WaitAsync(ct);

            Assert.Equal(1, wal.Index.FrameCount);
            WalRuntimeRawSnapshot snapshot = GetLiveRuntimeSnapshot(wal);
            Assert.Equal(1L, snapshot.CommitFlushBatchCount);
            Assert.Equal(
                (long)PageConstants.WalFrameSize,
                snapshot.CommittedFrameBytesWritten);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CheckpointProgressRuntimeSnapshot_TracksCopyAndNewerCommits(
        bool useMemoryWal)
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_wal_checkpoint_progress_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        IWriteAheadLog? wal = null;

        try
        {
            var index = new WalIndex();
            wal = useMemoryWal
                ? CreateObservedMemoryWal(
                    index,
                    initialBytes: default,
                    NoOpStorageRuntimeDiagnosticsObserver.Instance)
                : CreateObservedFileWal(
                    dbPath,
                    index,
                    NoOpStorageRuntimeDiagnosticsObserver.Instance);
            await wal.OpenAsync(currentDbPageCount: 2, ct);
            await (await wal.AppendFramesAndCommitAsync(
                new[]
                {
                    new WalFrameWrite(0, CreateFilledPage(0xD1)),
                    new WalFrameWrite(1, CreateFilledPage(0xD2)),
                },
                newDbPageCount: 2,
                ct)).WaitAsync(ct);

            await using var databaseDevice = new MemoryStorageDevice();
            bool completed = await wal.CheckpointStepAsync(
                databaseDevice,
                pageCount: 2,
                maxPages: 1,
                ct,
                allowFinalize: false);

            Assert.False(completed);
            WalCheckpointProgressRawSnapshot first =
                GetCheckpointProgressSnapshot(wal);
            Assert.Equal(1L, first.CompletedPageCount);
            Assert.Equal(2L, first.TotalPageCount);
            Assert.False(first.HasNewerCommits);

            await (await wal.AppendFramesAndCommitAsync(
                new[] { new WalFrameWrite(0, CreateFilledPage(0xD3)) },
                newDbPageCount: 2,
                ct)).WaitAsync(ct);

            WalCheckpointProgressRawSnapshot withNewerCommit =
                GetCheckpointProgressSnapshot(wal);
            Assert.Equal(1L, withNewerCommit.CompletedPageCount);
            Assert.Equal(2L, withNewerCommit.TotalPageCount);
            Assert.True(withNewerCommit.HasNewerCommits);
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
    public async Task ForegroundAutoCheckpointFailure_PreservesFaultedPhaseAcrossDeferredRerequest()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var device = new ArmableCheckpointFailingStorageDevice(
            new MemoryStorageDevice());
        var walIndex = new WalIndex();
        await using var wal = new MemoryWriteAheadLog(walIndex);
        await using var pager = await Pager.CreateAsync(
            device,
            wal,
            walIndex,
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(1),
                AutoCheckpointExecutionMode = AutoCheckpointExecutionMode.Foreground,
            },
            ct);
        await pager.InitializeNewDatabaseAsync(ct);
        await pager.RecoverAsync(ct);

        device.ArmFlushFailure();
        await pager.BeginTransactionAsync(ct);
        uint pageId = await pager.AllocatePageAsync(ct);
        byte[] page = await pager.GetPageAsync(pageId, ct);
        page[0] = 0x97;
        await pager.MarkDirtyAsync(pageId, ct);

        // A foreground auto-checkpoint failure is deferred rather than failing
        // an already-durable commit. Its outer re-request must retain Faulted.
        await pager.CommitAsync(ct);

        Assert.True(pager.TryGetRuntimeDiagnosticsSnapshot(out PagerRuntimeRawSnapshot failed));
        Assert.Equal(StorageCheckpointPhaseRaw.Faulted, failed.Wal.CheckpointPhase);

        device.DisarmFlushFailure();
        await pager.CheckpointAsync(ct);

        Assert.True(pager.TryGetRuntimeDiagnosticsSnapshot(out PagerRuntimeRawSnapshot recovered));
        Assert.Equal(StorageCheckpointPhaseRaw.Idle, recovered.Wal.CheckpointPhase);
    }

    [Fact]
    public async Task RuntimeSnapshot_ReportsRequestedBeforeCheckpointIsAdmitted()
    {
        var ct = TestContext.Current.CancellationToken;
        var interceptor = new BlockingCheckpointInterceptor();
        interceptor.Arm();
        var runtime = await OpenMemoryRuntimePagerAsync(
            new MemoryStorageDevice(),
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(1),
                AutoCheckpointExecutionMode = AutoCheckpointExecutionMode.Foreground,
                Interceptors = [interceptor],
            },
            ct);
        await using Pager pager = runtime.Pager;

        Task commitTask = CommitDirtyPageAsync(pager, 0x98, ct).AsTask();
        await interceptor.WaitForCheckpointStartAsync(ct);
        try
        {
            Assert.Equal(
                StorageCheckpointPhaseRaw.Requested,
                GetPagerRuntimeSnapshot(pager).Wal.CheckpointPhase);
        }
        finally
        {
            interceptor.Release();
        }

        await commitTask;
    }

    [Fact]
    public async Task RuntimeSnapshot_ReportsCopyingDuringCheckpointCopy()
    {
        var ct = TestContext.Current.CancellationToken;
        var device = new BlockingCheckpointStorageDevice(new MemoryStorageDevice());
        var runtime = await OpenMemoryRuntimePagerAsync(
            device,
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
            },
            ct);
        await using Pager pager = runtime.Pager;
        await CommitDirtyPageAsync(pager, 0x99, ct);

        device.ArmFlushBlock();
        Task checkpointTask = pager.CheckpointAsync(ct).AsTask();
        await device.WaitForBlockedFlushAsync(ct);
        try
        {
            Assert.Equal(
                StorageCheckpointPhaseRaw.Copying,
                GetPagerRuntimeSnapshot(pager).Wal.CheckpointPhase);
        }
        finally
        {
            device.ReleaseFlush();
        }

        await checkpointTask;
    }

    [Fact]
    public async Task RuntimeSnapshot_ReportsCopyCompleteAwaitingReadersOnlyForRetainedWal()
    {
        var ct = TestContext.Current.CancellationToken;
        var runtime = await OpenMemoryRuntimePagerAsync(
            new MemoryStorageDevice(),
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
            },
            ct);
        await using Pager pager = runtime.Pager;
        await CommitDirtyPageAsync(pager, 0x9A, ct);

        WalSnapshot reader = pager.AcquireReaderSnapshot();
        try
        {
            await pager.CheckpointAsync(ct);
            PagerRuntimeRawSnapshot snapshot = GetPagerRuntimeSnapshot(pager);
            Assert.Equal(
                StorageCheckpointPhaseRaw.CopyCompleteAwaitingReaders,
                snapshot.Wal.CheckpointPhase);
            Assert.True(snapshot.Wal.RetainedBytes >= 0);
            Assert.Equal(1, snapshot.Storage.ActiveReaderCount);
        }
        finally
        {
            pager.ReleaseReaderSnapshot(reader);
        }

        await pager.CheckpointAsync(ct);
    }

    [Fact]
    public async Task RuntimeSnapshot_ReportsFinalizingOnlyInsideAllowFinalizeCheckpoint()
    {
        var ct = TestContext.Current.CancellationToken;
        var device = new BlockingCheckpointStorageDevice(new MemoryStorageDevice());
        (Pager Pager, MemoryWriteAheadLog Wal) runtime =
            await OpenMemoryRuntimePagerAsync(
                device,
                new PagerOptions
                {
                    CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
                },
                ct);
        await using Pager pager = runtime.Pager;
        await CommitDirtyPageAsync(pager, 0x9B, ct);
        await runtime.Wal.CheckpointAsync(
            device,
            pager.PageCount,
            ct,
            allowFinalize: false);

        device.ArmFlushBlock();
        Task checkpointTask = pager.CheckpointAsync(ct).AsTask();
        await device.WaitForBlockedFlushAsync(ct);
        try
        {
            Assert.Equal(
                StorageCheckpointPhaseRaw.Finalizing,
                GetPagerRuntimeSnapshot(pager).Wal.CheckpointPhase);
        }
        finally
        {
            device.ReleaseFlush();
        }

        await checkpointTask;
        Assert.Equal(
            StorageCheckpointPhaseRaw.Idle,
            GetPagerRuntimeSnapshot(pager).Wal.CheckpointPhase);
    }

    [Fact]
    public async Task RuntimeSnapshot_ReaderWithNoWalFramesDoesNotReportAwaitingReaders()
    {
        var ct = TestContext.Current.CancellationToken;
        var runtime = await OpenMemoryRuntimePagerAsync(
            new MemoryStorageDevice(),
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
            },
            ct);
        await using Pager pager = runtime.Pager;

        WalSnapshot reader = pager.AcquireReaderSnapshot();
        try
        {
            Assert.False(reader.HasWalFrames);
            await CommitDirtyPageAsync(pager, 0x9C, ct);
            await pager.CheckpointAsync(ct);

            PagerRuntimeRawSnapshot snapshot = GetPagerRuntimeSnapshot(pager);
            Assert.Equal(StorageCheckpointPhaseRaw.Idle, snapshot.Wal.CheckpointPhase);
            Assert.Equal(1, snapshot.Storage.ActiveReaderCount);
            Assert.Equal(0, snapshot.Wal.FrameCount);
            Assert.Equal(0, snapshot.Wal.RetainedBytes);
        }
        finally
        {
            pager.ReleaseReaderSnapshot(reader);
        }
    }

    [Fact]
    public async Task RuntimeSnapshot_RetriesFinalizeBetweenWholePagerSamples()
    {
        var ct = TestContext.Current.CancellationToken;
        var device = new MemoryStorageDevice();
        (Pager Pager, MemoryWriteAheadLog Wal) runtime =
            await OpenMemoryRuntimePagerAsync(
                device,
                new PagerOptions
                {
                    CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
                },
                ct);
        await using Pager pager = runtime.Pager;
        await CommitDirtyPageAsync(pager, 0x9D, ct);
        await runtime.Wal.CheckpointAsync(
            device,
            pager.PageCount,
            ct,
            allowFinalize: false);

        int hookInvoked = 0;
        pager.RuntimeDiagnosticsBetweenSnapshotSamplesForTests = () =>
        {
            if (Interlocked.Exchange(ref hookInvoked, 1) == 0)
            {
                pager.CheckpointAsync(ct)
                    .AsTask()
                    .GetAwaiter()
                    .GetResult();
            }
        };

        PagerRuntimeRawSnapshot snapshot = GetPagerRuntimeSnapshot(pager);
        pager.RuntimeDiagnosticsBetweenSnapshotSamplesForTests = null;

        Assert.Equal(1, hookInvoked);
        Assert.Equal(StorageCheckpointPhaseRaw.Idle, snapshot.Wal.CheckpointPhase);
        Assert.Equal(PageConstants.WalHeaderSize, snapshot.Wal.LogicalBytes);
        Assert.Equal(0, snapshot.Wal.FrameCount);
        Assert.Equal(0, snapshot.Wal.CommittedFrameBytes);
        Assert.Equal(0, snapshot.Wal.RetainedBytes);
    }

    [Fact]
    public async Task RuntimeSnapshot_RetriesLegacyCommitAcrossDirtyWriterSample()
    {
        var ct = TestContext.Current.CancellationToken;
        var runtime = await OpenMemoryRuntimePagerAsync(
            new MemoryStorageDevice(),
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
            },
            ct);
        await using Pager pager = runtime.Pager;
        await pager.BeginTransactionAsync(ct);
        uint pageId = await pager.AllocatePageAsync(ct);
        byte[] page = await pager.GetPageAsync(pageId, ct);
        page[0] = 0x9E;
        await pager.MarkDirtyAsync(pageId, ct);

        int hookInvoked = 0;
        pager.RuntimeDiagnosticsAfterDirtyPageReadForTests = () =>
        {
            if (Interlocked.Exchange(ref hookInvoked, 1) == 0)
            {
                pager.CommitAsync(ct)
                    .AsTask()
                    .GetAwaiter()
                    .GetResult();
            }
        };

        PagerRuntimeRawSnapshot snapshot = GetPagerRuntimeSnapshot(pager);
        pager.RuntimeDiagnosticsAfterDirtyPageReadForTests = null;

        Assert.Equal(1, hookInvoked);
        Assert.Equal(0, snapshot.Storage.ActiveWriterCount);
        Assert.Equal(0, snapshot.Storage.DirtyPageCount);
    }

    [Fact]
    public async Task RuntimeSnapshot_CaptureIsAllocationFreeAfterWarmup()
    {
        var ct = TestContext.Current.CancellationToken;
        var runtime = await OpenMemoryRuntimePagerAsync(
            new MemoryStorageDevice(),
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
            },
            ct);
        await using Pager pager = runtime.Pager;

        for (int i = 0; i < 256; i++)
            _ = pager.TryGetRuntimeDiagnosticsSnapshot(out _);

        bool allAvailable = true;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++)
            allAvailable &= pager.TryGetRuntimeDiagnosticsSnapshot(out _);
        long allocatedBytes = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(allAvailable);
        Assert.Equal(0, allocatedBytes);
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

            WalRuntimeRawSnapshot failed = GetLiveRuntimeSnapshot(wal);
            Assert.Equal(0L, failed.CommitFlushBatchCount);
            Assert.Equal(0L, failed.FlushedCommitCount);
            Assert.Equal(0L, failed.DurableFlushCount);
            Assert.Equal(0L, failed.GroupCommitBatchCount);
            Assert.Equal(0L, failed.GroupCommitCount);

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

                CSharpDbException? commitFailure = null;
                try
                {
                    WalCommitResult commit = await wal.CommitAsync(newDbPageCount: 1, ct);
                    await commit.WaitAsync(ct);
                }
                catch (CSharpDbException ex)
                {
                    commitFailure = ex;
                }

                Assert.NotNull(commitFailure);
                Assert.Equal(ErrorCode.WalError, commitFailure.Code);
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

    private static WriteAheadLog CreateObservedFileWal(
        string dbPath,
        WalIndex index,
        IStorageRuntimeDiagnosticsObserver observer) =>
        new(
            dbPath,
            index,
            checksumProvider: null,
            durabilityMode: DurabilityMode.Durable,
            durableCommitBatchWindow: TimeSpan.Zero,
            walPreallocationChunkBytes: 0,
            runtimeDiagnosticsObserver: observer);

    private static MemoryWriteAheadLog CreateObservedMemoryWal(
        WalIndex index,
        ReadOnlyMemory<byte> initialBytes,
        IStorageRuntimeDiagnosticsObserver observer) =>
        new(
            index,
            checksumProvider: null,
            initialBytes: initialBytes,
            runtimeDiagnosticsObserver: observer);

    private static StorageRecoveryRuntimeRawSnapshot GetRecoveryRuntimeSnapshot(
        IWriteAheadLog wal)
    {
        var provider = Assert.IsAssignableFrom<IWalRecoveryRuntimeSnapshotProvider>(wal);
        Assert.True(provider.TryGetRecoveryRuntimeSnapshot(out StorageRecoveryRuntimeRawSnapshot snapshot));
        return snapshot;
    }

    private static WalCheckpointProgressRawSnapshot GetCheckpointProgressSnapshot(
        IWriteAheadLog wal)
    {
        var provider = Assert.IsAssignableFrom<IWalCheckpointRuntimeSnapshotProvider>(wal);
        Assert.True(provider.TryGetCheckpointProgressSnapshot(out WalCheckpointProgressRawSnapshot snapshot));
        return snapshot;
    }

    private static void AssertRecoveryScan(
        StorageRecoveryRuntimeRawSnapshot snapshot,
        long scannedFrameCount,
        long scannedBytes,
        long recoveredFrameCount,
        long discardedFrameCount,
        long discardedBytes,
        StorageRecoveryTruncationReasonRaw truncationReason)
    {
        Assert.Equal(StorageRecoveryPhaseRaw.Scanning, snapshot.Phase);
        Assert.Equal(scannedFrameCount, snapshot.ScannedFrameCount);
        Assert.Equal(scannedBytes, snapshot.ScannedBytes);
        Assert.Equal(recoveredFrameCount, snapshot.RecoveredFrameCount);
        Assert.Equal(
            recoveredFrameCount * PageConstants.WalFrameSize,
            snapshot.RecoveredBytes);
        Assert.Equal(discardedFrameCount, snapshot.DiscardedFrameCount);
        Assert.Equal(discardedBytes, snapshot.DiscardedBytes);
        Assert.Equal(truncationReason, snapshot.TruncationReason);
        Assert.Equal(1L, snapshot.AttemptCount);
        Assert.Equal(0L, snapshot.RetryCount);
        Assert.Equal(StorageRuntimeFailureKindRaw.None, snapshot.LastRetryFailureKind);
        Assert.Equal(StorageRuntimeOperationOutcomeRaw.Running, snapshot.Outcome);
        Assert.Equal(StorageRuntimeFailureKindRaw.None, snapshot.FailureKind);
    }

    private static async Task AssertCorruptRecoveryCandidateAsync(
        bool corruptSalt,
        StorageRecoveryTruncationReasonRaw expectedReason,
        CancellationToken ct)
    {
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_wal_corrupt_candidate_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";

        try
        {
            await using (var seed = new WriteAheadLog(dbPath, new WalIndex()))
            {
                await seed.OpenAsync(currentDbPageCount: 4, ct);
                await (await seed.AppendFramesAndCommitAsync(
                    new[]
                    {
                        new WalFrameWrite(0, CreateFilledPage(0xC1)),
                        new WalFrameWrite(1, CreateFilledPage(0xC2)),
                        new WalFrameWrite(2, CreateFilledPage(0xC3)),
                        new WalFrameWrite(3, CreateFilledPage(0xC4)),
                    },
                    newDbPageCount: 4,
                    ct)).WaitAsync(ct);
            }

            long corruptOffset = corruptSalt
                ? PageConstants.WalHeaderSize + 8
                : PageConstants.WalHeaderSize +
                    PageConstants.WalFrameHeaderSize + 31;
            await FlipWalByteAsync(walPath, corruptOffset, ct);
            byte[] seedBytes = await File.ReadAllBytesAsync(walPath, ct);

            await using (var fileWal = CreateObservedFileWal(
                dbPath,
                new WalIndex(),
                NoOpStorageRuntimeDiagnosticsObserver.Instance))
            {
                await fileWal.OpenAsync(currentDbPageCount: 4, ct);
                AssertRecoveryScan(
                    GetRecoveryRuntimeSnapshot(fileWal),
                    scannedFrameCount: 1,
                    scannedBytes: PageConstants.WalFrameSize,
                    recoveredFrameCount: 0,
                    discardedFrameCount: 4,
                    discardedBytes: 4L * PageConstants.WalFrameSize,
                    expectedReason);
            }

            await using var memoryWal = CreateObservedMemoryWal(
                new WalIndex(),
                seedBytes,
                NoOpStorageRuntimeDiagnosticsObserver.Instance);
            await memoryWal.OpenAsync(currentDbPageCount: 4, ct);
            AssertRecoveryScan(
                GetRecoveryRuntimeSnapshot(memoryWal),
                scannedFrameCount: 1,
                scannedBytes: PageConstants.WalFrameSize,
                recoveredFrameCount: 0,
                discardedFrameCount: 4,
                discardedBytes: 4L * PageConstants.WalFrameSize,
                expectedReason);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(walPath)) File.Delete(walPath);
        }
    }

    private static async ValueTask FlipWalByteAsync(
        string walPath,
        long offset,
        CancellationToken ct)
    {
        await using var stream = new FileStream(
            walPath,
            FileMode.Open,
            FileAccess.ReadWrite,
            FileShare.Read);
        var value = new byte[1];
        stream.Position = offset;
        Assert.Equal(1, await stream.ReadAsync(value, ct));
        value[0] ^= 0xFF;
        stream.Position = offset;
        await stream.WriteAsync(value, ct);
        await stream.FlushAsync(ct);
    }

    private static WalRuntimeRawSnapshot GetLiveRuntimeSnapshot(IWriteAheadLog wal)
    {
        var provider = Assert.IsAssignableFrom<ILiveWalRuntimeSnapshotProvider>(wal);
        Assert.True(provider.TryGetLiveRuntimeDiagnosticsSnapshot(out WalRuntimeRawSnapshot snapshot));
        return snapshot;
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

    private static PagerRuntimeRawSnapshot GetPagerRuntimeSnapshot(Pager pager)
    {
        Assert.True(pager.TryGetRuntimeDiagnosticsSnapshot(out PagerRuntimeRawSnapshot snapshot));
        return snapshot;
    }

    private static async ValueTask<(Pager Pager, MemoryWriteAheadLog Wal)>
        OpenMemoryRuntimePagerAsync(
            IStorageDevice device,
            PagerOptions options,
            CancellationToken ct)
    {
        var walIndex = new WalIndex();
        var wal = new MemoryWriteAheadLog(walIndex);
        Pager? pager = null;

        try
        {
            pager = await Pager.CreateAsync(device, wal, walIndex, options, ct);
            await pager.InitializeNewDatabaseAsync(ct);
            await pager.RecoverAsync(ct);
            return (pager, wal);
        }
        catch
        {
            if (pager is not null)
            {
                await pager.DisposeAsync();
            }
            else
            {
                await wal.DisposeAsync();
                await device.DisposeAsync();
            }

            throw;
        }
    }

    private static async ValueTask CommitDirtyPageAsync(
        Pager pager,
        byte value,
        CancellationToken ct)
    {
        await pager.BeginTransactionAsync(ct);
        uint pageId = await pager.AllocatePageAsync(ct);
        byte[] page = await pager.GetPageAsync(pageId, ct);
        page[0] = value;
        await pager.MarkDirtyAsync(pageId, ct);
        await pager.CommitAsync(ct);
    }

    private static async ValueTask CommitFileWalFrameAsync(
        WriteAheadLog wal,
        uint pageId,
        byte value,
        CancellationToken ct)
    {
        WalCommitResult commit = await wal.AppendFramesAndCommitAsync(
            new[] { new WalFrameWrite(pageId, CreateFilledPage(value)) },
            newDbPageCount: 1,
            ct);
        await commit.WaitAsync(ct);
    }

    private class NoOpStorageRuntimeDiagnosticsObserver : IStorageRuntimeDiagnosticsObserver
    {
        internal static NoOpStorageRuntimeDiagnosticsObserver Instance { get; } = new();

        public virtual object? CaptureCheckpointCorrelation(
            StorageCheckpointOriginRaw origin) => null;
        public virtual object? CaptureCheckpointCompletionCorrelation() => null;
        public virtual void OnRecoveryStarted() { }
        public virtual void OnRecoveryChanged(in StorageRecoveryRuntimeRawSnapshot snapshot) { }
        public virtual void OnRecoveryCompleted(in StorageRecoveryRuntimeRawSnapshot snapshot) { }
        public virtual void OnCheckpointStarted(
            in StorageCheckpointRuntimeRawSnapshot snapshot,
            object? correlation) { }
        public virtual void OnCheckpointChanged(in StorageCheckpointRuntimeRawSnapshot snapshot) { }
        public virtual void OnCheckpointCompleted(
            in StorageCheckpointRuntimeRawSnapshot snapshot,
            object? correlation) { }
        public virtual void OnWalFlushCompleted() { }
        public virtual void OnWalFlushCompleted(int logicalCommitCount) =>
            OnWalFlushCompleted();
        public virtual void OnWalDurableFlushCompleted(
            long durableFlushCount) { }
    }

    private sealed class WalFlushOrderingObserver : NoOpStorageRuntimeDiagnosticsObserver
    {
        private WriteAheadLog? _wal;
        private Task? _completion;
        private int _flushCallbackCount;

        internal int FlushCallbackCount => Volatile.Read(ref _flushCallbackCount);
        internal bool CompletionWasAlreadySignaled { get; private set; }
        internal WalRuntimeRawSnapshot Snapshot { get; private set; }

        internal void AttachWal(WriteAheadLog wal) => _wal = wal;

        internal void AttachCompletion(Task completion) => _completion = completion;

        public override void OnWalFlushCompleted()
        {
            Interlocked.Increment(ref _flushCallbackCount);
            CompletionWasAlreadySignaled = _completion?.IsCompleted == true;
            if (_wal is ILiveWalRuntimeSnapshotProvider provider &&
                provider.TryGetLiveRuntimeDiagnosticsSnapshot(out WalRuntimeRawSnapshot snapshot))
            {
                Snapshot = snapshot;
            }
        }
    }

    private sealed class ThrowingWalFlushObserver :
        NoOpStorageRuntimeDiagnosticsObserver
    {
        public override void OnWalFlushCompleted() =>
            throw new InvalidOperationException("Synthetic observer failure.");
    }

    private sealed class WalLifetimeObserver :
        NoOpStorageRuntimeDiagnosticsObserver
    {
        private int _publicationCallbackCount;
        private int _lastPublicationLogicalCommitCount;
        private int _durableCallbackCount;
        private long _lastDurableFlushCount;

        internal int PublicationCallbackCount =>
            Volatile.Read(ref _publicationCallbackCount);
        internal int LastPublicationLogicalCommitCount =>
            Volatile.Read(ref _lastPublicationLogicalCommitCount);
        internal int DurableCallbackCount =>
            Volatile.Read(ref _durableCallbackCount);
        internal long LastDurableFlushCount =>
            Interlocked.Read(ref _lastDurableFlushCount);

        public override void OnWalFlushCompleted(int logicalCommitCount)
        {
            Volatile.Write(
                ref _lastPublicationLogicalCommitCount,
                logicalCommitCount);
            Interlocked.Increment(ref _publicationCallbackCount);
        }

        public override void OnWalDurableFlushCompleted(
            long durableFlushCount)
        {
            Interlocked.Exchange(
                ref _lastDurableFlushCount,
                durableFlushCount);
            Interlocked.Increment(ref _durableCallbackCount);
        }
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

    private sealed class BlockingCheckpointStorageDevice(IStorageDevice inner) : IStorageDevice
    {
        private readonly TaskCompletionSource<bool> _flushBlocked =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _allowFlush =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _armed;
        private int _blocked;

        public long Length => inner.Length;

        public void ArmFlushBlock() => Volatile.Write(ref _armed, 1);

        public void ReleaseFlush() => _allowFlush.TrySetResult(true);

        public Task WaitForBlockedFlushAsync(CancellationToken ct = default) =>
            _flushBlocked.Task.WaitAsync(ct);

        public ValueTask<int> ReadAsync(
            long offset,
            Memory<byte> buffer,
            CancellationToken ct = default) => inner.ReadAsync(offset, buffer, ct);

        public ValueTask WriteAsync(
            long offset,
            ReadOnlyMemory<byte> buffer,
            CancellationToken ct = default) => inner.WriteAsync(offset, buffer, ct);

        public async ValueTask FlushAsync(CancellationToken ct = default)
        {
            if (Volatile.Read(ref _armed) != 0 &&
                Interlocked.CompareExchange(ref _blocked, 1, 0) == 0)
            {
                _flushBlocked.TrySetResult(true);
                await _allowFlush.Task.WaitAsync(ct);
            }

            await inner.FlushAsync(ct);
        }

        public ValueTask SetLengthAsync(long length, CancellationToken ct = default) =>
            inner.SetLengthAsync(length, ct);

        public ValueTask DisposeAsync() => inner.DisposeAsync();

        public void Dispose() => inner.Dispose();
    }

    private sealed class ArmableCheckpointFailingStorageDevice(IStorageDevice inner) : IStorageDevice
    {
        private int _failFlush;

        public long Length => inner.Length;

        public void ArmFlushFailure() => Interlocked.Exchange(ref _failFlush, 1);

        public void DisarmFlushFailure() => Interlocked.Exchange(ref _failFlush, 0);

        public ValueTask<int> ReadAsync(
            long offset,
            Memory<byte> buffer,
            CancellationToken ct = default) => inner.ReadAsync(offset, buffer, ct);

        public ValueTask WriteAsync(
            long offset,
            ReadOnlyMemory<byte> buffer,
            CancellationToken ct = default) => inner.WriteAsync(offset, buffer, ct);

        public ValueTask FlushAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            return Volatile.Read(ref _failFlush) != 0
                ? ValueTask.FromException(
                    new IOException("Injected checkpoint device flush failure."))
                : inner.FlushAsync(ct);
        }

        public ValueTask SetLengthAsync(long length, CancellationToken ct = default) =>
            inner.SetLengthAsync(length, ct);

        public ValueTask DisposeAsync() => inner.DisposeAsync();

        public void Dispose() => inner.Dispose();
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

    private static void SetPrivateInt64Field(object instance, string fieldName, long value)
    {
        FieldInfo field = instance.GetType().GetField(
            fieldName,
            BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException(
                $"Private Int64 field '{fieldName}' was not found on {instance.GetType().Name}.");
        field.SetValue(instance, value);
    }

    private static async ValueTask InvokeRecoverExistingWalOrCreateNewAsync(
        WriteAheadLog wal,
        uint currentDbPageCount,
        CancellationToken ct)
    {
        var method = typeof(WriteAheadLog).GetMethod(
            "RecoverExistingWalOrCreateNewAsync",
            BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("RecoverExistingWalOrCreateNewAsync was not found.");

        if (method.Invoke(wal, [currentDbPageCount, ct]) is not ValueTask task)
            throw new InvalidOperationException("RecoverExistingWalOrCreateNewAsync returned an unexpected value.");

        await task;
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

        public ValueTask FlushCommitAsync(SafeFileHandle handle, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Interlocked.Increment(ref _flushCount);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class CountingWalFlushPolicy : IWalFlushPolicy
    {
        private readonly bool _allowsWriteConcurrencyDuringCommitFlush;
        private int _commitFlushCount;

        public CountingWalFlushPolicy(bool allowsWriteConcurrencyDuringCommitFlush)
        {
            _allowsWriteConcurrencyDuringCommitFlush = allowsWriteConcurrencyDuringCommitFlush;
        }

        public int CommitFlushCount => Volatile.Read(ref _commitFlushCount);
        public bool AllowsWriteConcurrencyDuringCommitFlush => _allowsWriteConcurrencyDuringCommitFlush;

        public void Reset()
        {
            Volatile.Write(ref _commitFlushCount, 0);
        }

        public ValueTask FlushCommitAsync(SafeFileHandle handle, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Interlocked.Increment(ref _commitFlushCount);
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

        public ValueTask FlushCommitAsync(SafeFileHandle handle, CancellationToken cancellationToken)
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

        public ValueTask FlushCommitAsync(SafeFileHandle handle, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int flushNumber = Interlocked.Increment(ref _flushCount);
            if (flushNumber == 1)
                return ValueTask.CompletedTask;

            return ValueTask.FromException(new IOException("Injected commit flush failure."));
        }
    }
}
