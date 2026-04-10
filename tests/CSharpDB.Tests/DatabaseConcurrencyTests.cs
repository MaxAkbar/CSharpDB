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
    public async Task ExplicitWriteTransaction_IdentityMetadataRefreshesSharedCatalogState()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE split_bench (id INTEGER PRIMARY KEY IDENTITY, payload TEXT)", ct);

        await using (var tx = await _db.BeginWriteTransactionAsync(ct))
        {
            await tx.ExecuteAsync("INSERT INTO split_bench (payload) VALUES ('first')", ct);
            await tx.ExecuteAsync("INSERT INTO split_bench (payload) VALUES ('second')", ct);

            await tx.CommitAsync(ct);
        }

        await _db.ExecuteAsync("INSERT INTO split_bench (payload) VALUES ('third')", ct);
        Assert.Equal(3L, await ScalarIntAsync("SELECT MAX(id) FROM split_bench", ct));
        Assert.Equal(3L, await ScalarIntAsync("SELECT COUNT(*) FROM split_bench", ct));
    }

    [Fact]
    public async Task ConcurrentExplicitWriteTransactions_DisjointInsertOnlyLeafWritesCanCommitWithoutRetry()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE conflict_bench (id INTEGER PRIMARY KEY, writer INTEGER)", ct);

        await using (var warmupTx = await _db.BeginWriteTransactionAsync(ct))
        {
            await warmupTx.ExecuteAsync("INSERT INTO conflict_bench VALUES (100, 0)", ct);
            await warmupTx.CommitAsync(ct);
        }

        await using var tx1 = await _db.BeginWriteTransactionAsync(ct);
        await using var tx2 = await _db.BeginWriteTransactionAsync(ct);

        await tx1.ExecuteAsync("INSERT INTO conflict_bench VALUES (1, 1)", ct);
        await tx2.ExecuteAsync("INSERT INTO conflict_bench VALUES (2, 2)", ct);

        await tx1.CommitAsync(ct);
        await tx2.CommitAsync(ct);

        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM conflict_bench", ct);
        var rows = await result.ToListAsync(ct);
        Assert.Equal(3, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task ConcurrentExplicitWriteTransactions_DisjointInsertOnlyLeafWritesCanCommitWithoutRetryOnFreshTable()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE fresh_conflict_bench (id INTEGER PRIMARY KEY, writer INTEGER)", ct);

        await using var tx1 = await _db.BeginWriteTransactionAsync(ct);
        await using var tx2 = await _db.BeginWriteTransactionAsync(ct);

        await tx1.ExecuteAsync("INSERT INTO fresh_conflict_bench VALUES (1, 1)", ct);
        await tx2.ExecuteAsync("INSERT INTO fresh_conflict_bench VALUES (2, 2)", ct);

        await tx1.CommitAsync(ct);
        await tx2.CommitAsync(ct);

        await using var result = await _db.ExecuteAsync("SELECT id, writer FROM fresh_conflict_bench ORDER BY id, writer", ct);
        var rows = await result.ToListAsync(ct);

        Assert.Equal(2, rows.Count);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal(1L, rows[0][1].AsInteger);
        Assert.Equal(2L, rows[1][0].AsInteger);
        Assert.Equal(2L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task ConcurrentExplicitWriteTransactions_NonMergeableSameLeafUpdatesStillConflictWithoutRetry()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE update_conflict_bench (id INTEGER PRIMARY KEY, writer INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO update_conflict_bench VALUES (1, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO update_conflict_bench VALUES (2, 20)", ct);

        await using var tx1 = await _db.BeginWriteTransactionAsync(ct);
        await using var tx2 = await _db.BeginWriteTransactionAsync(ct);

        await tx1.ExecuteAsync("UPDATE update_conflict_bench SET writer = 11 WHERE id = 1", ct);
        await tx2.ExecuteAsync("UPDATE update_conflict_bench SET writer = 22 WHERE id = 2", ct);

        await tx1.CommitAsync(ct);
        await Assert.ThrowsAsync<CSharpDbConflictException>(() => tx2.CommitAsync(ct).AsTask());

        Assert.Equal(11L, await ScalarIntAsync("SELECT writer FROM update_conflict_bench WHERE id = 1", ct));
        Assert.Equal(20L, await ScalarIntAsync("SELECT writer FROM update_conflict_bench WHERE id = 2", ct));
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
    public async Task ConcurrentExplicitWriteTransactions_DuplicateUniqueIndexInsertConflictsWithCommittedConcurrentInsert()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE unique_users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_unique_users_email ON unique_users(email)", ct);

        await using var tx1 = await _db.BeginWriteTransactionAsync(ct);
        await using var tx2 = await _db.BeginWriteTransactionAsync(ct);

        await tx1.ExecuteAsync("INSERT INTO unique_users VALUES (1, 'dup@example.com')", ct);
        await tx2.ExecuteAsync("INSERT INTO unique_users VALUES (2, 'dup@example.com')", ct);

        await tx1.CommitAsync(ct);
        await Assert.ThrowsAsync<CSharpDbConflictException>(() => tx2.CommitAsync(ct).AsTask());

        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM unique_users", ct));
        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM unique_users WHERE email = 'dup@example.com'", ct));
    }

    [Fact]
    public async Task ConcurrentExplicitWriteTransactions_DuplicateUniqueIndexUpdateConflictsWithCommittedConcurrentUpdate()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE unique_users_update (id INTEGER PRIMARY KEY, email TEXT NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_unique_users_update_email ON unique_users_update(email)", ct);
        await _db.ExecuteAsync("INSERT INTO unique_users_update VALUES (1, 'alpha@example.com')", ct);
        await _db.ExecuteAsync("INSERT INTO unique_users_update VALUES (2, 'beta@example.com')", ct);

        await using var tx1 = await _db.BeginWriteTransactionAsync(ct);
        await using var tx2 = await _db.BeginWriteTransactionAsync(ct);

        await tx1.ExecuteAsync("UPDATE unique_users_update SET email = 'shared@example.com' WHERE id = 1", ct);
        await tx2.ExecuteAsync("UPDATE unique_users_update SET email = 'shared@example.com' WHERE id = 2", ct);

        await tx1.CommitAsync(ct);
        await Assert.ThrowsAsync<CSharpDbConflictException>(() => tx2.CommitAsync(ct).AsTask());

        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM unique_users_update WHERE email = 'shared@example.com'", ct));
    }

    [Fact]
    public async Task ConcurrentExplicitWriteTransactions_ChildInsertReferencingUniqueParentKeyConflictsWithCommittedParentDelete()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE fk_unique_parents (id INTEGER PRIMARY KEY, code TEXT NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_fk_unique_parents_code ON fk_unique_parents(code)", ct);
        await _db.ExecuteAsync("CREATE TABLE fk_unique_children (id INTEGER PRIMARY KEY, parent_code TEXT REFERENCES fk_unique_parents(code))", ct);
        await _db.ExecuteAsync("INSERT INTO fk_unique_parents VALUES (1, 'parent-1')", ct);

        await using var childTx = await _db.BeginWriteTransactionAsync(ct);
        await using var parentTx = await _db.BeginWriteTransactionAsync(ct);

        await childTx.ExecuteAsync("INSERT INTO fk_unique_children VALUES (1, 'parent-1')", ct);
        await parentTx.ExecuteAsync("DELETE FROM fk_unique_parents WHERE code = 'parent-1'", ct);

        await parentTx.CommitAsync(ct);
        await Assert.ThrowsAsync<CSharpDbConflictException>(() => childTx.CommitAsync(ct).AsTask());

        Assert.Equal(0L, await ScalarIntAsync("SELECT COUNT(*) FROM fk_unique_parents", ct));
        Assert.Equal(0L, await ScalarIntAsync("SELECT COUNT(*) FROM fk_unique_children", ct));
    }

    [Fact]
    public async Task ConcurrentExplicitWriteTransactions_ParentDeleteReferencingUniqueParentKeyConflictsWithCommittedChildInsert()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE fk_unique_parents (id INTEGER PRIMARY KEY, code TEXT NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_fk_unique_parents_code ON fk_unique_parents(code)", ct);
        await _db.ExecuteAsync("CREATE TABLE fk_unique_children (id INTEGER PRIMARY KEY, parent_code TEXT REFERENCES fk_unique_parents(code))", ct);
        await _db.ExecuteAsync("INSERT INTO fk_unique_parents VALUES (1, 'parent-1')", ct);

        await using var parentTx = await _db.BeginWriteTransactionAsync(ct);
        await using var childTx = await _db.BeginWriteTransactionAsync(ct);

        await parentTx.ExecuteAsync("DELETE FROM fk_unique_parents WHERE code = 'parent-1'", ct);
        await childTx.ExecuteAsync("INSERT INTO fk_unique_children VALUES (1, 'parent-1')", ct);

        await childTx.CommitAsync(ct);
        await Assert.ThrowsAsync<CSharpDbConflictException>(() => parentTx.CommitAsync(ct).AsTask());

        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM fk_unique_parents", ct));
        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM fk_unique_children", ct));
    }

    [Fact]
    public async Task ConcurrentExplicitWriteTransactions_ParentUniqueKeyUpdateConflictsWithCommittedChildInsert()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE fk_unique_update_parents (id INTEGER PRIMARY KEY, code TEXT NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_fk_unique_update_parents_code ON fk_unique_update_parents(code)", ct);
        await _db.ExecuteAsync("CREATE TABLE fk_unique_update_children (id INTEGER PRIMARY KEY, parent_code TEXT REFERENCES fk_unique_update_parents(code))", ct);
        await _db.ExecuteAsync("INSERT INTO fk_unique_update_parents VALUES (1, 'parent-1')", ct);

        await using var parentTx = await _db.BeginWriteTransactionAsync(ct);
        await using var childTx = await _db.BeginWriteTransactionAsync(ct);

        await parentTx.ExecuteAsync("UPDATE fk_unique_update_parents SET code = 'parent-2' WHERE code = 'parent-1'", ct);
        await childTx.ExecuteAsync("INSERT INTO fk_unique_update_children VALUES (1, 'parent-1')", ct);

        await childTx.CommitAsync(ct);
        await Assert.ThrowsAsync<CSharpDbConflictException>(() => parentTx.CommitAsync(ct).AsTask());

        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM fk_unique_update_parents WHERE code = 'parent-1'", ct));
        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM fk_unique_update_children WHERE parent_code = 'parent-1'", ct));
    }

    [Fact]
    public async Task ConcurrentExplicitWriteTransactions_CascadingParentDeleteConflictsWithCommittedConcurrentChildInsert()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE cascade_parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE cascade_children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES cascade_parents(id) ON DELETE CASCADE)", ct);
        await _db.ExecuteAsync("INSERT INTO cascade_parents VALUES (1)", ct);

        await using var parentTx = await _db.BeginWriteTransactionAsync(ct);
        await using var childTx = await _db.BeginWriteTransactionAsync(ct);

        await parentTx.ExecuteAsync("DELETE FROM cascade_parents WHERE id = 1", ct);
        await childTx.ExecuteAsync("INSERT INTO cascade_children VALUES (1, 1)", ct);

        await childTx.CommitAsync(ct);
        await Assert.ThrowsAsync<CSharpDbConflictException>(() => parentTx.CommitAsync(ct).AsTask());

        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM cascade_parents WHERE id = 1", ct));
        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM cascade_children WHERE parent_id = 1", ct));
    }

    [Fact]
    public async Task ConcurrentExplicitWriteTransactions_CascadingParentDeleteAllowsConcurrentChildInsertForDifferentParent()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE cascade_parents_ok (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE cascade_children_ok (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES cascade_parents_ok(id) ON DELETE CASCADE)", ct);
        await _db.ExecuteAsync("INSERT INTO cascade_parents_ok VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO cascade_parents_ok VALUES (2)", ct);

        await using var parentTx = await _db.BeginWriteTransactionAsync(ct);
        await using var childTx = await _db.BeginWriteTransactionAsync(ct);

        await parentTx.ExecuteAsync("DELETE FROM cascade_parents_ok WHERE id = 1", ct);
        await childTx.ExecuteAsync("INSERT INTO cascade_children_ok VALUES (1, 2)", ct);

        await parentTx.CommitAsync(ct);
        await childTx.CommitAsync(ct);

        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM cascade_parents_ok WHERE id = 2", ct));
        Assert.Equal(0L, await ScalarIntAsync("SELECT COUNT(*) FROM cascade_parents_ok WHERE id = 1", ct));
        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM cascade_children_ok WHERE parent_id = 2", ct));
    }

    [Fact]
    public async Task ConcurrentExplicitWriteTransactions_SelfReferencingCascadeDeleteConflictsWithCommittedConcurrentChildInsert()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE cascade_nodes (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES cascade_nodes(id) ON DELETE CASCADE)",
            ct);
        await _db.ExecuteAsync("INSERT INTO cascade_nodes VALUES (1, 1)", ct);
        await _db.ExecuteAsync("INSERT INTO cascade_nodes VALUES (2, 1)", ct);

        await using var deleteTx = await _db.BeginWriteTransactionAsync(ct);
        await using var childTx = await _db.BeginWriteTransactionAsync(ct);

        await deleteTx.ExecuteAsync("DELETE FROM cascade_nodes WHERE id = 1", ct);
        await childTx.ExecuteAsync("INSERT INTO cascade_nodes VALUES (3, 2)", ct);

        await childTx.CommitAsync(ct);
        await Assert.ThrowsAsync<CSharpDbConflictException>(() => deleteTx.CommitAsync(ct).AsTask());

        Assert.Equal(3L, await ScalarIntAsync("SELECT COUNT(*) FROM cascade_nodes", ct));
        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM cascade_nodes WHERE id = 3 AND parent_id = 2", ct));
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
    public async Task ReadOnlyExplicitWriteTransaction_DisjunctiveTableScan_ConflictsWithConcurrentInsertInsideTrackedRange()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE disjunctive_range_items (id INTEGER PRIMARY KEY, value INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO disjunctive_range_items VALUES (1, 5)", ct);
        await _db.ExecuteAsync("INSERT INTO disjunctive_range_items VALUES (2, 20)", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync("SELECT COUNT(*) FROM disjunctive_range_items WHERE value = 5 OR value = 20", ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(2L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO disjunctive_range_items VALUES (3, 20)", ct);

        await Assert.ThrowsAsync<CSharpDbConflictException>(() => tx.CommitAsync(ct).AsTask());
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_DisjunctiveTableScan_AllowsConcurrentInsertOutsideTrackedRanges()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE disjunctive_range_items_ok (id INTEGER PRIMARY KEY, value INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO disjunctive_range_items_ok VALUES (1, 5)", ct);
        await _db.ExecuteAsync("INSERT INTO disjunctive_range_items_ok VALUES (2, 20)", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync("SELECT COUNT(*) FROM disjunctive_range_items_ok WHERE value = 5 OR value = 20", ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(2L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO disjunctive_range_items_ok VALUES (3, 10)", ct);

        await tx.CommitAsync(ct);
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_DisjunctiveConjunctiveTableScan_ConflictsWithConcurrentInsertInTrackedDisjunct()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE disjunctive_conjunctive_items (id INTEGER PRIMARY KEY, score INTEGER, tag TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO disjunctive_conjunctive_items VALUES (1, 5, 'hot')", ct);
        await _db.ExecuteAsync("INSERT INTO disjunctive_conjunctive_items VALUES (2, 20, 'cold')", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync(
            "SELECT COUNT(*) FROM disjunctive_conjunctive_items WHERE (score = 5 AND tag = 'hot') OR (score = 20 AND tag = 'cold')",
            ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(2L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO disjunctive_conjunctive_items VALUES (3, 20, 'cold')", ct);

        await Assert.ThrowsAsync<CSharpDbConflictException>(() => tx.CommitAsync(ct).AsTask());
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_DisjunctiveConjunctiveTableScan_AllowsConcurrentInsertOutsideTrackedDisjuncts()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE disjunctive_conjunctive_items_ok (id INTEGER PRIMARY KEY, score INTEGER, tag TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO disjunctive_conjunctive_items_ok VALUES (1, 5, 'hot')", ct);
        await _db.ExecuteAsync("INSERT INTO disjunctive_conjunctive_items_ok VALUES (2, 20, 'cold')", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync(
            "SELECT COUNT(*) FROM disjunctive_conjunctive_items_ok WHERE (score = 5 AND tag = 'hot') OR (score = 20 AND tag = 'cold')",
            ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(2L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO disjunctive_conjunctive_items_ok VALUES (3, 99, 'warm')", ct);

        await tx.CommitAsync(ct);
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_CorrelatedExistsConflictsWithConcurrentInsertIntoMatchingSubquery()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE exists_customers (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE exists_orders (id INTEGER PRIMARY KEY, customer_id INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_exists_orders_customer_id ON exists_orders(customer_id)", ct);
        await _db.ExecuteAsync("INSERT INTO exists_customers VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO exists_customers VALUES (2)", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync(
            "SELECT COUNT(*) FROM exists_customers c WHERE c.id = 1 AND EXISTS (SELECT 1 FROM exists_orders o WHERE o.customer_id = c.id)",
            ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(0L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO exists_orders VALUES (1, 1)", ct);

        await Assert.ThrowsAsync<CSharpDbConflictException>(() => tx.CommitAsync(ct).AsTask());
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_CorrelatedExistsAllowsConcurrentInsertOutsideSubqueryMatch()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE exists_customers_ok (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE exists_orders_ok (id INTEGER PRIMARY KEY, customer_id INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_exists_orders_ok_customer_id ON exists_orders_ok(customer_id)", ct);
        await _db.ExecuteAsync("INSERT INTO exists_customers_ok VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO exists_customers_ok VALUES (2)", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync(
            "SELECT COUNT(*) FROM exists_customers_ok c WHERE c.id = 1 AND EXISTS (SELECT 1 FROM exists_orders_ok o WHERE o.customer_id = c.id)",
            ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(0L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO exists_orders_ok VALUES (1, 2)", ct);

        await tx.CommitAsync(ct);
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_InSubqueryConflictsWithConcurrentInsertIntoMatchingSet()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE in_products (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE in_featured (id INTEGER PRIMARY KEY, product_id INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_in_featured_product_id ON in_featured(product_id)", ct);
        await _db.ExecuteAsync("INSERT INTO in_products VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO in_products VALUES (2)", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync(
            "SELECT COUNT(*) FROM in_products p WHERE p.id = 1 AND p.id IN (SELECT f.product_id FROM in_featured f)",
            ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(0L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO in_featured VALUES (1, 1)", ct);

        await Assert.ThrowsAsync<CSharpDbConflictException>(() => tx.CommitAsync(ct).AsTask());
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_InSubqueryAllowsConcurrentInsertOutsideMatchingSet()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE in_products_ok (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE in_featured_ok (id INTEGER PRIMARY KEY, product_id INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_in_featured_ok_product_id ON in_featured_ok(product_id)", ct);
        await _db.ExecuteAsync("INSERT INTO in_products_ok VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO in_products_ok VALUES (2)", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync(
            "SELECT COUNT(*) FROM in_products_ok p WHERE p.id = 1 AND p.id IN (SELECT f.product_id FROM in_featured_ok f)",
            ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(0L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO in_featured_ok VALUES (1, 2)", ct);

        await tx.CommitAsync(ct);
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_NotExistsConflictsWithConcurrentInsertIntoMatchingSubquery()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE not_exists_customers (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE not_exists_orders (id INTEGER PRIMARY KEY, customer_id INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_not_exists_orders_customer_id ON not_exists_orders(customer_id)", ct);
        await _db.ExecuteAsync("INSERT INTO not_exists_customers VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO not_exists_customers VALUES (2)", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync(
            "SELECT COUNT(*) FROM not_exists_customers c WHERE c.id = 1 AND NOT EXISTS (SELECT 1 FROM not_exists_orders o WHERE o.customer_id = c.id)",
            ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(1L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO not_exists_orders VALUES (1, 1)", ct);

        await Assert.ThrowsAsync<CSharpDbConflictException>(() => tx.CommitAsync(ct).AsTask());
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_NotExistsAllowsConcurrentInsertOutsideMatchingSubquery()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE not_exists_customers_ok (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE not_exists_orders_ok (id INTEGER PRIMARY KEY, customer_id INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_not_exists_orders_ok_customer_id ON not_exists_orders_ok(customer_id)", ct);
        await _db.ExecuteAsync("INSERT INTO not_exists_customers_ok VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO not_exists_customers_ok VALUES (2)", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync(
            "SELECT COUNT(*) FROM not_exists_customers_ok c WHERE c.id = 1 AND NOT EXISTS (SELECT 1 FROM not_exists_orders_ok o WHERE o.customer_id = c.id)",
            ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(1L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO not_exists_orders_ok VALUES (1, 2)", ct);

        await tx.CommitAsync(ct);
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_NotInSubqueryConflictsWithConcurrentInsertIntoMatchingSet()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE not_in_products (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE not_in_featured (id INTEGER PRIMARY KEY, product_id INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_not_in_featured_product_id ON not_in_featured(product_id)", ct);
        await _db.ExecuteAsync("INSERT INTO not_in_products VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO not_in_products VALUES (2)", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync(
            "SELECT COUNT(*) FROM not_in_products p WHERE p.id = 1 AND p.id NOT IN (SELECT f.product_id FROM not_in_featured f)",
            ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(1L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO not_in_featured VALUES (1, 1)", ct);

        await Assert.ThrowsAsync<CSharpDbConflictException>(() => tx.CommitAsync(ct).AsTask());
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_NotInSubqueryAllowsConcurrentInsertOutsideMatchingSet()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE not_in_products_ok (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE not_in_featured_ok (id INTEGER PRIMARY KEY, product_id INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_not_in_featured_ok_product_id ON not_in_featured_ok(product_id)", ct);
        await _db.ExecuteAsync("INSERT INTO not_in_products_ok VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO not_in_products_ok VALUES (2)", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync(
            "SELECT COUNT(*) FROM not_in_products_ok p WHERE p.id = 1 AND p.id NOT IN (SELECT f.product_id FROM not_in_featured_ok f)",
            ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(1L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO not_in_featured_ok VALUES (1, 2)", ct);

        await tx.CommitAsync(ct);
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_IndexJoinConflictsWithConcurrentInsertIntoMatchingJoinKey()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE join_customers (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE join_orders (id INTEGER PRIMARY KEY, customer_id INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_join_orders_customer_id ON join_orders(customer_id)", ct);
        await _db.ExecuteAsync("INSERT INTO join_customers VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO join_customers VALUES (2)", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync(
            "SELECT COUNT(*) FROM join_customers c JOIN join_orders o ON c.id = o.customer_id WHERE c.id = 1",
            ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(0L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO join_orders VALUES (1, 1)", ct);

        await Assert.ThrowsAsync<CSharpDbConflictException>(() => tx.CommitAsync(ct).AsTask());
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_IndexJoinAllowsConcurrentInsertOutsideJoinKey()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE join_customers_ok (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE join_orders_ok (id INTEGER PRIMARY KEY, customer_id INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_join_orders_ok_customer_id ON join_orders_ok(customer_id)", ct);
        await _db.ExecuteAsync("INSERT INTO join_customers_ok VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO join_customers_ok VALUES (2)", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync(
            "SELECT COUNT(*) FROM join_customers_ok c JOIN join_orders_ok o ON c.id = o.customer_id WHERE c.id = 1",
            ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(0L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO join_orders_ok VALUES (1, 2)", ct);

        await tx.CommitAsync(ct);
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_LeftAntiJoinConflictsWithConcurrentInsertIntoMatchingJoinKey()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE left_anti_customers (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE left_anti_orders (id INTEGER PRIMARY KEY, customer_id INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_left_anti_orders_customer_id ON left_anti_orders(customer_id)", ct);
        await _db.ExecuteAsync("INSERT INTO left_anti_customers VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO left_anti_customers VALUES (2)", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync(
            "SELECT COUNT(*) FROM left_anti_customers c LEFT JOIN left_anti_orders o ON c.id = o.customer_id WHERE c.id = 1 AND o.id IS NULL",
            ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(1L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO left_anti_orders VALUES (1, 1)", ct);

        await Assert.ThrowsAsync<CSharpDbConflictException>(() => tx.CommitAsync(ct).AsTask());
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_LeftAntiJoinAllowsConcurrentInsertOutsideJoinKey()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE left_anti_customers_ok (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE left_anti_orders_ok (id INTEGER PRIMARY KEY, customer_id INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_left_anti_orders_ok_customer_id ON left_anti_orders_ok(customer_id)", ct);
        await _db.ExecuteAsync("INSERT INTO left_anti_customers_ok VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO left_anti_customers_ok VALUES (2)", ct);

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync(
            "SELECT COUNT(*) FROM left_anti_customers_ok c LEFT JOIN left_anti_orders_ok o ON c.id = o.customer_id WHERE c.id = 1 AND o.id IS NULL",
            ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(1L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO left_anti_orders_ok VALUES (1, 2)", ct);

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
    public async Task ReadOnlyExplicitWriteTransaction_LargeIndexedAggregateConflictsWithConcurrentUpdateInRange()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE indexed_payload_range_items (id INTEGER PRIMARY KEY, value INTEGER, writer INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_indexed_payload_range_items_value ON indexed_payload_range_items(value)", ct);

        await using (var seedTx = await _db.BeginWriteTransactionAsync(ct))
        {
            for (int id = 1; id <= 2048; id++)
                await seedTx.ExecuteAsync($"INSERT INTO indexed_payload_range_items VALUES ({id}, {id}, 0)", ct);

            await seedTx.CommitAsync(ct);
        }

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync(
            "SELECT SUM(writer) FROM indexed_payload_range_items WHERE value >= 100 AND value <= 200",
            ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(0L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("UPDATE indexed_payload_range_items SET writer = writer + 1 WHERE id = 150", ct);

        await Assert.ThrowsAsync<CSharpDbConflictException>(() => tx.CommitAsync(ct).AsTask());
    }

    [Fact]
    public async Task ReadOnlyExplicitWriteTransaction_LargeIndexedAggregateAllowsConcurrentUpdateOutsideRange()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE indexed_payload_range_items_ok (id INTEGER PRIMARY KEY, value INTEGER, writer INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_indexed_payload_range_items_ok_value ON indexed_payload_range_items_ok(value)", ct);

        await using (var seedTx = await _db.BeginWriteTransactionAsync(ct))
        {
            for (int id = 1; id <= 2048; id++)
                await seedTx.ExecuteAsync($"INSERT INTO indexed_payload_range_items_ok VALUES ({id}, {id}, 0)", ct);

            await seedTx.CommitAsync(ct);
        }

        await using var tx = await _db.BeginWriteTransactionAsync(ct);
        await using (var result = await tx.ExecuteAsync(
            "SELECT SUM(writer) FROM indexed_payload_range_items_ok WHERE value >= 100 AND value <= 200",
            ct))
        {
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(0L, row[0].AsInteger);
        }

        await _db.ExecuteAsync("UPDATE indexed_payload_range_items_ok SET writer = writer + 1 WHERE id = 1800", ct);

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
    public async Task ActiveExplicitWriteTransaction_AllowsBackgroundCheckpointCopyButDefersWalFinalizeUntilTransactionCompletes()
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
                    $"Expected WAL frames to remain while an explicit write transaction keeps checkpoint finalization deferred, observed walLength={walLengthWhileTransactionActive}.");

                await WaitForConditionAsync(
                    () => db.GetCommitPathDiagnosticsSnapshot().BackgroundCheckpointStartCount > 0,
                    TimeSpan.FromSeconds(5),
                    ct);

                walLengthWhileTransactionActive = new FileInfo(walPath).Length;
                Assert.True(
                    walLengthWhileTransactionActive > PageConstants.WalHeaderSize,
                    $"Expected background checkpoint finalization to remain deferred while the explicit write transaction is active, observed walLength={walLengthWhileTransactionActive}.");

                var commitDiagnostics = db.GetCommitPathDiagnosticsSnapshot();
                Assert.True(
                    commitDiagnostics.BackgroundCheckpointStartCount > 0,
                    $"Expected background checkpoint copying to start while the explicit write transaction was active, observed backgroundCheckpointStarts={commitDiagnostics.BackgroundCheckpointStartCount}.");
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

    private static async Task WaitForConditionAsync(
        Func<bool> condition,
        TimeSpan timeout,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(condition);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            if (condition())
                return;

            await Task.Delay(25, ct);
        }

        throw new Xunit.Sdk.XunitException("Timed out waiting for the expected condition.");
    }

    private async Task<long> ScalarIntAsync(string sql, CancellationToken ct)
    {
        await using var result = await _db.ExecuteAsync(sql, ct);
        DbValue[] row = Assert.Single(await result.ToListAsync(ct));
        return row[0].AsInteger;
    }

    private sealed record UserDocument(string Name, int Age);
}
