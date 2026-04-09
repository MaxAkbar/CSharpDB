using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;

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

    [Fact]
    public async Task ConcurrentExplicitWriteTransactions_ChildInsertConflictsWithCommittedParentDelete()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))", ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);

        await using var childTx = await _db.BeginWriteTransactionAsync(ct);
        await using var parentTx = await _db.BeginWriteTransactionAsync(ct);

        await childTx.ExecuteAsync("INSERT INTO children VALUES (1, 1)", ct);
        await parentTx.ExecuteAsync("DELETE FROM parents WHERE id = 1", ct);

        await parentTx.CommitAsync(ct);
        await Assert.ThrowsAsync<CSharpDbConflictException>(() => childTx.CommitAsync(ct).AsTask());

        Assert.Equal(0L, await ScalarIntAsync("SELECT COUNT(*) FROM parents", ct));
        Assert.Equal(0L, await ScalarIntAsync("SELECT COUNT(*) FROM children", ct));
    }

    [Fact]
    public async Task ConcurrentExplicitWriteTransactions_ParentDeleteConflictsWithCommittedChildInsert()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))", ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);

        await using var parentTx = await _db.BeginWriteTransactionAsync(ct);
        await using var childTx = await _db.BeginWriteTransactionAsync(ct);

        await parentTx.ExecuteAsync("DELETE FROM parents WHERE id = 1", ct);
        await childTx.ExecuteAsync("INSERT INTO children VALUES (1, 1)", ct);

        await childTx.CommitAsync(ct);
        await Assert.ThrowsAsync<CSharpDbConflictException>(() => parentTx.CommitAsync(ct).AsTask());

        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM parents", ct));
        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM children", ct));
    }

    [Fact]
    public async Task ExplicitWriteTransaction_DdlWaitsForEarlierTransactionToComplete()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE ddl_wait_base (id INTEGER PRIMARY KEY, value INTEGER)", ct);

        await using var blockingTx = await _db.BeginWriteTransactionAsync(ct);
        await using var ddlTx = await _db.BeginWriteTransactionAsync(ct);

        await using (var result = await blockingTx.ExecuteAsync("SELECT COUNT(*) FROM ddl_wait_base", ct))
        {
            await result.ToListAsync(ct);
        }
        Task ddlExecuteTask = ddlTx.ExecuteAsync("CREATE TABLE ddl_wait_created (id INTEGER PRIMARY KEY)", ct).AsTask();

        await Task.Delay(200, ct);
        Assert.False(ddlExecuteTask.IsCompleted);

        await blockingTx.CommitAsync(ct);
        await ddlExecuteTask.WaitAsync(ct);
        await ddlTx.CommitAsync(ct);

        Assert.Contains(
            _db.GetTableNames(),
            static name => string.Equals(name, "ddl_wait_created", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task WaitingDdlTransaction_BlocksNewExplicitTransactionStarts_UntilCommitCompletes()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE ddl_begin_block_base (id INTEGER PRIMARY KEY, value INTEGER)", ct);

        await using var blockingTx = await _db.BeginWriteTransactionAsync(ct);
        await using var ddlTx = await _db.BeginWriteTransactionAsync(ct);

        await using (var result = await blockingTx.ExecuteAsync("SELECT COUNT(*) FROM ddl_begin_block_base", ct))
        {
            await result.ToListAsync(ct);
        }
        Task ddlExecuteTask = ddlTx.ExecuteAsync("CREATE TABLE ddl_begin_block_created (id INTEGER PRIMARY KEY)", ct).AsTask();

        await Task.Delay(200, ct);
        Assert.False(ddlExecuteTask.IsCompleted);

        Task<WriteTransaction> blockedBeginTask = _db.BeginWriteTransactionAsync(ct).AsTask();
        await Task.Delay(200, ct);
        Assert.False(blockedBeginTask.IsCompleted);

        await blockingTx.CommitAsync(ct);
        await ddlExecuteTask.WaitAsync(ct);

        await Task.Delay(200, ct);
        Assert.False(blockedBeginTask.IsCompleted);

        await ddlTx.CommitAsync(ct);

        await using WriteTransaction tx3 = await blockedBeginTask.WaitAsync(ct);
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_FullTableScanConflictsWithConcurrentInsert()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE range_scan_items (id INTEGER PRIMARY KEY, value INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO range_scan_items VALUES (1, 5)", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync("SELECT COUNT(*) FROM range_scan_items WHERE value >= 10", ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(0L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO range_scan_items VALUES (2, 20)", ct);

        await Assert.ThrowsAsync<CSharpDbConflictException>(() => tx.CommitAsync(ct).AsTask());
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_FilteredTableScan_AllowsConcurrentInsertOutsidePredicateRange()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE range_scan_items_ok (id INTEGER PRIMARY KEY, value INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO range_scan_items_ok VALUES (1, 5)", ct);
        await _db.ExecuteAsync("INSERT INTO range_scan_items_ok VALUES (2, 15)", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync("SELECT id FROM range_scan_items_ok WHERE value >= 10", ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(2L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO range_scan_items_ok VALUES (3, 5)", ct);

        await tx.CommitAsync(ct);
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_FullTableScanConflictsWithConcurrentUpdateIntoPredicateRange()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE range_scan_updates (id INTEGER PRIMARY KEY, value INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO range_scan_updates VALUES (1, 5)", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync("SELECT COUNT(*) FROM range_scan_updates WHERE value >= 10", ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(0L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("UPDATE range_scan_updates SET value = 20 WHERE id = 1", ct);

        await Assert.ThrowsAsync<CSharpDbConflictException>(() => tx.CommitAsync(ct).AsTask());
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_ConjunctiveTableScanConflictsWithConcurrentUpdateIntoMatchingRange()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE conjunctive_range_items (id INTEGER PRIMARY KEY, score INTEGER, tag TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO conjunctive_range_items VALUES (1, 5, 'hot')", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync("SELECT COUNT(*) FROM conjunctive_range_items WHERE score >= 10 AND tag = 'hot'", ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(0L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("UPDATE conjunctive_range_items SET score = 20 WHERE id = 1", ct);

        await Assert.ThrowsAsync<CSharpDbConflictException>(() => tx.CommitAsync(ct).AsTask());
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_ConjunctiveTableScan_AllowsConcurrentInsertOutsideTrackedRanges()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE conjunctive_range_items_ok (id INTEGER PRIMARY KEY, score INTEGER, tag TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO conjunctive_range_items_ok VALUES (1, 5, 'cold')", ct);
        await _db.ExecuteAsync("INSERT INTO conjunctive_range_items_ok VALUES (2, 15, 'hot')", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync("SELECT id FROM conjunctive_range_items_ok WHERE score >= 10 AND tag = 'hot'", ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(2L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO conjunctive_range_items_ok VALUES (3, 5, 'cold')", ct);

        await tx.CommitAsync(ct);
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_IndexedRangeScanConflictsWithConcurrentInsertInRange()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE indexed_range_items (id INTEGER PRIMARY KEY, value INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_indexed_range_items_value ON indexed_range_items(value)", ct);
        await _db.ExecuteAsync("INSERT INTO indexed_range_items VALUES (1, 5)", ct);
        await _db.ExecuteAsync("INSERT INTO indexed_range_items VALUES (2, 15)", ct);
        await _db.ExecuteAsync("INSERT INTO indexed_range_items VALUES (3, 25)", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync("SELECT COUNT(*) FROM indexed_range_items WHERE value >= 10 AND value <= 20", ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(1L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO indexed_range_items VALUES (4, 18)", ct);

        await Assert.ThrowsAsync<CSharpDbConflictException>(() => tx.CommitAsync(ct).AsTask());
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_IndexedRangeScan_AllowsConcurrentInsertOutsideRange()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE indexed_range_items_ok (id INTEGER PRIMARY KEY, value INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_indexed_range_items_ok_value ON indexed_range_items_ok(value)", ct);
        await _db.ExecuteAsync("INSERT INTO indexed_range_items_ok VALUES (1, 5)", ct);
        await _db.ExecuteAsync("INSERT INTO indexed_range_items_ok VALUES (2, 15)", ct);
        await _db.ExecuteAsync("INSERT INTO indexed_range_items_ok VALUES (3, 25)", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync("SELECT COUNT(*) FROM indexed_range_items_ok WHERE value >= 10 AND value <= 20", ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(1L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO indexed_range_items_ok VALUES (4, 50)", ct);

        await tx.CommitAsync(ct);
    }

    [Fact]
    public async Task ConcurrentExplicitWriteTransactions_WithBatchWindow_CanQueuePendingCommits()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_concurrency_batch_test_{Guid.NewGuid():N}.db");
        var options = new DatabaseOptions()
            .ConfigureStorageEngine(builder => builder.UseDurableCommitBatchWindow(TimeSpan.FromMilliseconds(1)));

        Database? db = null;
        try
        {
            db = await Database.OpenAsync(dbPath, options, ct);

            const int writerCount = 8;
            await db.ExecuteAsync("CREATE TABLE tx_batch (id INTEGER PRIMARY KEY, writer INTEGER)", ct);
            await db.BeginTransactionAsync(ct);
            for (int id = 1; id <= 2048; id++)
                await db.ExecuteAsync($"INSERT INTO tx_batch VALUES ({id}, 0)", ct);
            await db.CommitAsync(ct);

            db.ResetWalFlushDiagnostics();
            db.ResetCommitPathDiagnostics();

            var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            int[] rowIds = [64, 320, 576, 832, 1088, 1344, 1600, 1856];
            Task[] writers = Enumerable.Range(0, writerCount)
                .Select(writerId => Task.Run(
                    async () =>
                    {
                        await using var tx = await db.BeginWriteTransactionAsync(ct);
                        await tx.ExecuteAsync($"UPDATE tx_batch SET writer = writer + 1 WHERE id = {rowIds[writerId]}", ct);
                        await start.Task.WaitAsync(ct);
                        await tx.CommitAsync(ct);
                    },
                    ct))
                .ToArray();

            start.SetResult();
            await Task.WhenAll(writers);

            for (int writerId = 0; writerId < writerCount; writerId++)
            {
                await using var result = await db.ExecuteAsync($"SELECT writer FROM tx_batch WHERE id = {rowIds[writerId]}", ct);
                var rows = await result.ToListAsync(ct);
                Assert.Equal(1, rows[0][0].AsInteger);
            }

            var walDiagnostics = db.GetWalFlushDiagnosticsSnapshot();
            var commitDiagnostics = db.GetCommitPathDiagnosticsSnapshot();
            Assert.True(
                walDiagnostics.FlushedCommitCount > walDiagnostics.FlushCount,
                $"Expected multiple explicit commits per durable flush, observed flushedCommits={walDiagnostics.FlushedCommitCount}, flushes={walDiagnostics.FlushCount}.");
            Assert.True(
                commitDiagnostics.MaxPendingCommitCount > 1,
                $"Expected commit diagnostics to record pending commit batching, observed maxPendingCommits={commitDiagnostics.MaxPendingCommitCount}.");
        }
        finally
        {
            if (db is not null)
                await db.DisposeAsync();
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task ConcurrentExplicitWriteTransactions_DisjointUpdates_DoNotCorruptSharedSnapshotCache()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_concurrency_snapshot_cache_test_{Guid.NewGuid():N}.db");
        var options = new DatabaseOptions()
            .ConfigureStorageEngine(builder => builder.UseDurableCommitBatchWindow(TimeSpan.FromMilliseconds(1)));

        Database? db = null;
        try
        {
            db = await Database.OpenAsync(dbPath, options, ct);
            await db.ExecuteAsync("CREATE TABLE tx_cache (id INTEGER PRIMARY KEY, value INTEGER)", ct);
            await db.BeginTransactionAsync(ct);
            for (int id = 1; id <= 4096; id++)
                await db.ExecuteAsync($"INSERT INTO tx_cache VALUES ({id}, 0)", ct);
            await db.CommitAsync(ct);

            const int writerCount = 8;
            const int transactionsPerWriter = 96;
            const int rowsPerTransaction = 4;
            const int rowsPerWriterPartition = 512;

            await RunConcurrentWritersAsync(
                writerCount,
                writerId => RunDisjointUpdateWriterAsync(
                    db,
                    writerId,
                    transactionsPerWriter,
                    rowsPerTransaction,
                    rowsPerWriterPartition,
                    ct),
                ct);

            await using var result = await db.ExecuteAsync("SELECT SUM(value) FROM tx_cache", ct);
            var rows = await result.ToListAsync(ct);
            Assert.Equal(writerCount * transactionsPerWriter * rowsPerTransaction, rows[0][0].AsInteger);
        }
        finally
        {
            if (db is not null)
                await db.DisposeAsync();
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task ActiveExplicitWriteTransaction_BlocksManualCheckpointUntilTransactionCompletes()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_concurrency_manual_checkpoint_test_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var options = new DatabaseOptions()
            .ConfigureStorageEngine(builder => builder.UsePagerOptions(new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
            }));

        Database? db = null;
        try
        {
            db = await Database.OpenAsync(dbPath, options, ct);
            await db.ExecuteAsync("CREATE TABLE tx_checkpoint (id INTEGER PRIMARY KEY, value INTEGER)", ct);

            await using (var tx = await db.BeginWriteTransactionAsync(ct))
            {
                await tx.ExecuteAsync("SELECT COUNT(*) FROM tx_checkpoint", ct);
                await db.ExecuteAsync("INSERT INTO tx_checkpoint VALUES (1, 10)", ct);
                await db.CheckpointAsync(ct);

                Assert.True(File.Exists(walPath));
                long walLengthWhileTransactionActive = new FileInfo(walPath).Length;
                Assert.True(
                    walLengthWhileTransactionActive > PageConstants.WalHeaderSize,
                    $"Expected checkpoint to defer while an explicit write transaction holds a snapshot, observed walLength={walLengthWhileTransactionActive}.");
            }

            await db.CheckpointAsync(ct);
            Assert.Equal(PageConstants.WalHeaderSize, new FileInfo(walPath).Length);

            await using var result = await db.ExecuteAsync("SELECT COUNT(*) FROM tx_checkpoint", ct);
            var rows = await result.ToListAsync(ct);
            Assert.Equal(1L, rows[0][0].AsInteger);
        }
        finally
        {
            if (db is not null)
                await db.DisposeAsync();
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(walPath))
                File.Delete(walPath);
        }
    }

    [Fact]
    public async Task ActiveExplicitWriteTransaction_BlocksBackgroundCheckpointUntilTransactionCompletes()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_concurrency_background_checkpoint_test_{Guid.NewGuid():N}.db");
        string walPath = dbPath + ".wal";
        var options = new DatabaseOptions()
            .ConfigureStorageEngine(builder => builder.UsePagerOptions(new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(1),
                AutoCheckpointExecutionMode = AutoCheckpointExecutionMode.Background,
            }));

        Database? db = null;
        try
        {
            db = await Database.OpenAsync(dbPath, options, ct);
            await db.ExecuteAsync("CREATE TABLE tx_background_checkpoint (id INTEGER PRIMARY KEY, value INTEGER)", ct);
            await db.CheckpointAsync(ct);

            await using (var tx = await db.BeginWriteTransactionAsync(ct))
            {
                await tx.ExecuteAsync("SELECT COUNT(*) FROM tx_background_checkpoint", ct);
                await db.ExecuteAsync("INSERT INTO tx_background_checkpoint VALUES (1, 10)", ct);

                Assert.True(File.Exists(walPath));
                long walLengthWhileTransactionActive = new FileInfo(walPath).Length;
                Assert.True(
                    walLengthWhileTransactionActive > PageConstants.WalHeaderSize,
                    $"Expected WAL frames to remain while an explicit write transaction blocks background checkpoint, observed walLength={walLengthWhileTransactionActive}.");

                await Task.Delay(200, ct);

                walLengthWhileTransactionActive = new FileInfo(walPath).Length;
                Assert.True(
                    walLengthWhileTransactionActive > PageConstants.WalHeaderSize,
                    $"Expected background checkpoint to remain deferred while the explicit write transaction is active, observed walLength={walLengthWhileTransactionActive}.");
            }

            await WaitForWalLengthAsync(walPath, PageConstants.WalHeaderSize, TimeSpan.FromSeconds(5), ct);

            await using var result = await db.ExecuteAsync("SELECT COUNT(*) FROM tx_background_checkpoint", ct);
            var rows = await result.ToListAsync(ct);
            Assert.Equal(1L, rows[0][0].AsInteger);
        }
        finally
        {
            if (db is not null)
                await db.DisposeAsync();
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(walPath))
                File.Delete(walPath);
        }
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

    private static async Task RunDisjointUpdateWriterAsync(
        Database db,
        int writerId,
        int transactionsPerWriter,
        int rowsPerTransaction,
        int rowsPerWriterPartition,
        CancellationToken ct)
    {
        int partitionStart = (writerId * rowsPerWriterPartition) + 1;

        for (int txIndex = 0; txIndex < transactionsPerWriter; txIndex++)
        {
            await db.RunWriteTransactionAsync(
                async (tx, innerCt) =>
                {
                    int offsetBase = txIndex * rowsPerTransaction;
                    for (int rowOffset = 0; rowOffset < rowsPerTransaction; rowOffset++)
                    {
                        int rowId = partitionStart + ((offsetBase + rowOffset) % rowsPerWriterPartition);
                        await tx.ExecuteAsync($"UPDATE tx_cache SET value = value + 1 WHERE id = {rowId}", innerCt);
                    }
                },
                ct: ct);
        }
    }

    private static async Task WaitForWalLengthAsync(
        string walPath,
        long expectedLength,
        TimeSpan timeout,
        CancellationToken ct)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            long currentLength = File.Exists(walPath)
                ? new FileInfo(walPath).Length
                : 0;
            if (currentLength == expectedLength)
                return;

            await Task.Delay(25, ct);
        }

        long finalLength = File.Exists(walPath)
            ? new FileInfo(walPath).Length
            : 0;
        throw new Xunit.Sdk.XunitException(
            $"Timed out waiting for WAL length {expectedLength}, observed {finalLength}.");
    }

    private async Task<long> ScalarIntAsync(string sql, CancellationToken ct)
    {
        await using var result = await _db.ExecuteAsync(sql, ct);
        DbValue[] row = Assert.Single(await result.ToListAsync(ct));
        return row[0].AsInteger;
    }

    private sealed record UserDocument(string Name, int Age);
}
