using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Tests;

public sealed class ReaderSessionSnapshotCatalogTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static readonly string Payload = new('x', 128);

    [Fact]
    public async Task WarmPointAndCountReads_SwitchTablesWithoutLeakingCachedMetadata()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE a (id INTEGER PRIMARY KEY, label TEXT)", Ct);
        await db.ExecuteAsync("CREATE TABLE b (label TEXT, id INTEGER PRIMARY KEY)", Ct);
        await db.ExecuteAsync("INSERT INTO a VALUES (1, 'a')", Ct);
        await db.ExecuteAsync("INSERT INTO b VALUES ('b', 1), ('second', 2)", Ct);
        await db.ExecuteAsync("CREATE VIEW v AS SELECT id FROM b", Ct);
        using var reader = db.CreateReaderSession();
        await db.ExecuteAsync("INSERT INTO a VALUES (2, 'later')", Ct);
        await db.ExecuteAsync("DROP TABLE b", Ct);

        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(1, Assert.Single((await ReadAsync(reader, "SELECT COUNT(*) FROM a")).Rows)[0].AsInteger);
            Assert.Equal("a", Assert.Single((await ReadAsync(reader, "SELECT label FROM A WHERE id = 1")).Rows)[0].AsText);
            Assert.Equal("b", Assert.Single((await ReadAsync(reader, "SELECT label FROM b WHERE id = 1")).Rows)[0].AsText);
            Assert.Equal(2, Assert.Single((await ReadAsync(reader, "SELECT COUNT(*) FROM B")).Rows)[0].AsInteger);
            Assert.Equal(2, Assert.Single((await ReadAsync(reader, "SELECT COUNT(*) FROM v")).Rows)[0].AsInteger);
        }
    }

    [Theory(Timeout = 30_000)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task OrdinaryAndPreparedReads_KeepTableAndIndexRootsAfterSplits(bool fileBacked, bool warmReader)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_snapshot_catalog_{Guid.NewGuid():N}.db");
        try
        {
            await using var db = fileBacked
                ? await Database.OpenAsync(path, ct)
                : await Database.OpenInMemoryAsync(ct);
            await CreateTableAsync(db);
            await InsertAsync(db, 0, 16);
            SchemaCatalog catalog = GetField<SchemaCatalog>(db, "_catalog");
            uint tableRoot = catalog.GetTableRootPage("t");
            uint indexRoot = catalog.GetIndexStore("ix_t_k").RootPageId;
            using var reader = db.CreateReaderSession();
            var primaryRange = Prepare("SELECT id FROM t WHERE id >= @lower AND id < @upper ORDER BY id");
            var indexRange = Prepare("SELECT id FROM t WHERE k >= @lower AND k < @upper ORDER BY k");
            if (warmReader)
            {
                Assert.Equal(16, (await ReadAsync(reader, primaryRange.Bind([0, 1024]))).Rows.Count);
                Assert.Equal(16, (await ReadAsync(reader, indexRange.Bind([0, 10240]))).Rows.Count);
            }

            await InsertAsync(db, 16, 1008);
            Assert.NotEqual(tableRoot, catalog.GetTableRootPage("t"));
            Assert.NotEqual(indexRoot, catalog.GetIndexStore("ix_t_k").RootPageId);
            await db.ExecuteAsync("UPDATE t SET payload = 'changed' WHERE id = 10", ct);

            long[] originalIds = Enumerable.Range(0, 16).Select(i => (long)i).ToArray();
            // Every result is opened after growth, including the cold reader's first query.
            Assert.Equal(originalIds, (await ReadAsync(reader, primaryRange.Bind([0, 1024]))).Rows.Select(row => row[0].AsInteger));
            Assert.Equal(originalIds, (await ReadAsync(reader, "SELECT id FROM t ORDER BY id")).Rows.Select(row => row[0].AsInteger));
            Assert.Equal(originalIds, (await ReadAsync(reader, indexRange.Bind([0, 10240]))).Rows.Select(row => row[0].AsInteger));
            Assert.Equal(originalIds, (await ReadAsync(reader, "SELECT id FROM t WHERE k >= 0 AND k < 10240 ORDER BY k")).Rows.Select(row => row[0].AsInteger));
            Assert.Equal(new long[] { 10, 11, 12, 13, 14, 15 }, (await ReadAsync(reader, primaryRange.Bind([10, 1000]))).Rows.Select(row => row[0].AsInteger));
            Assert.Empty((await ReadAsync(reader, indexRange.Bind([160, 200]))).Rows);
            Assert.Equal(Payload, Assert.Single((await ReadAsync(reader, "SELECT payload FROM t WHERE id = 10")).Rows)[0].AsText);
            await AssertCountAsync(reader, 16);

            using var latest = db.CreateReaderSession();
            Assert.Equal(1024, (await ReadAsync(latest, primaryRange.Bind([0, 1024]))).Rows.Count);
            Assert.Equal(1024, (await ReadAsync(latest, indexRange.Bind([0, 10240]))).Rows.Count);
            Assert.Equal("changed", Assert.Single((await ReadAsync(latest, "SELECT payload FROM t WHERE id = 10")).Rows)[0].AsText);
            await AssertCountAsync(latest, 1024);
        }
        finally
        {
            foreach (string file in new[] { path, path + ".wal", path + ".lock" })
                if (File.Exists(file)) File.Delete(file);
        }
    }

    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FirstReadAfterCatalogRootSplitAndTableReplacement_UsesSnapshotSchema(bool preparedFirst)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        await CreateTableAsync(db);
        await InsertAsync(db, 0, 16);
        Pager pager = GetField<Pager>(db, "_pager");
        uint catalogRoot = pager.SchemaRootPage;
        using var reader = db.CreateReaderSession();
        var range = Prepare("SELECT * FROM t WHERE id >= @lower AND id < @upper ORDER BY id");

        await db.BeginTransactionAsync(ct);
        for (int i = 0; i < 128 && pager.SchemaRootPage == catalogRoot; i++)
            await db.ExecuteAsync($"CREATE TABLE catalog_growth_{i} (id INTEGER PRIMARY KEY, description TEXT, amount BIGINT)", ct);
        await db.CommitAsync(ct);
        Assert.NotEqual(catalogRoot, pager.SchemaRootPage);
        await db.ExecuteAsync("DROP TABLE t", ct);
        await db.ExecuteAsync("CREATE TABLE t (replacement TEXT, id INTEGER PRIMARY KEY)", ct);
        await db.ExecuteAsync("CREATE INDEX ix_t_k ON t(replacement)", ct);
        await db.ExecuteAsync("INSERT INTO t VALUES ('new schema', 7)", ct);

        SnapshotRows first = preparedFirst
            ? await ReadAsync(reader, range.Bind([0, 16]))
            : await ReadAsync(reader, "SELECT * FROM t ORDER BY id");
        AssertOriginalRows(first, 16);
        AssertOriginalRows(await ReadAsync(reader, range.Bind([0, 16])), 16);
        Assert.Equal(16, (await ReadAsync(reader, "SELECT id FROM t WHERE k >= 0 ORDER BY k")).Rows.Count);
        Assert.Equal(Payload, Assert.Single((await ReadAsync(reader, "SELECT payload FROM t WHERE id = 7")).Rows)[0].AsText);
        await AssertCountAsync(reader, 16);

        using var latest = db.CreateReaderSession();
        SnapshotRows current = await ReadAsync(latest, "SELECT * FROM t");
        Assert.Equal(new[] { "replacement", "id" }, current.Columns);
        Assert.Equal("new schema", Assert.Single(current.Rows)[0].AsText);
        await AssertCountAsync(latest, 1);
    }

    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AddedColumnAndNewRows_DoNotChangeSnapshotSchemaOrCount(bool warmReader)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        await CreateTableAsync(db);
        await InsertAsync(db, 0, 3);
        using var reader = db.CreateReaderSession();
        var range = Prepare("SELECT * FROM t WHERE k >= @lower AND k < @upper ORDER BY k");
        if (warmReader)
            AssertOriginalRows(await ReadAsync(reader, range.Bind([0, 100])), 3);

        await db.ExecuteAsync("ALTER TABLE t ADD COLUMN added TEXT", ct);
        await db.ExecuteAsync("UPDATE t SET payload = 'changed', added = 'new' WHERE id = 1", ct);
        await db.ExecuteAsync("INSERT INTO t VALUES (3, 30, 'new row', 'added')", ct);

        // COUNT's metadata shortcut must use the same snapshot as subsequent planned reads.
        await AssertCountAsync(reader, 3);
        AssertOriginalRows(await ReadAsync(reader, range.Bind([0, 100])), 3);
        AssertOriginalRows(await ReadAsync(reader, "SELECT * FROM t ORDER BY id"), 3);
        SnapshotRows point = await ReadAsync(reader, "SELECT * FROM t WHERE id = 1");
        Assert.Equal(new[] { "id", "k", "payload" }, point.Columns);
        Assert.Equal(Payload, Assert.Single(point.Rows)[2].AsText);

        using var latest = db.CreateReaderSession();
        SnapshotRows current = await ReadAsync(latest, range.Bind([0, 100]));
        Assert.Equal(new[] { "id", "k", "payload", "added" }, current.Columns);
        Assert.Equal(new long[] { 0, 1, 2, 3 }, current.Rows.Select(row => row[0].AsInteger));
        Assert.All(current.Rows, row => Assert.Equal(4, row.Length));
        Assert.True(current.Rows[0][3].IsNull);
        Assert.Equal("new", current.Rows[1][3].AsText);
        Assert.True(current.Rows[2][3].IsNull);
        Assert.Equal("added", current.Rows[3][3].AsText);
        await AssertCountAsync(latest, 4);
    }

    [Theory(Timeout = 30_000)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task UncommittedAndRolledBackSchemaChanges_DoNotAffectExistingReader(bool warmReader, bool replaceTable)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        await CreateTableAsync(db);
        await InsertAsync(db, 0, 3);
        using var reader = db.CreateReaderSession();
        var range = Prepare("SELECT * FROM t WHERE id >= @lower AND id < @upper ORDER BY id");
        if (warmReader)
            AssertOriginalRows(await ReadAsync(reader, range.Bind([0, 100])), 3);

        await db.BeginTransactionAsync(ct);
        try
        {
            if (replaceTable)
            {
                await db.ExecuteAsync("DROP TABLE t", ct);
                await db.ExecuteAsync("CREATE TABLE t (replacement TEXT, id INTEGER PRIMARY KEY)", ct);
                await db.ExecuteAsync("INSERT INTO t VALUES ('uncommitted', 1)", ct);
            }
            else
            {
                await db.ExecuteAsync("ALTER TABLE t ADD COLUMN uncommitted TEXT", ct);
                await db.ExecuteAsync("DELETE FROM t WHERE id = 0", ct);
                await db.ExecuteAsync("UPDATE t SET payload = 'uncommitted' WHERE id = 1", ct);
            }

            AssertOriginalRows(await ReadAsync(reader, range.Bind([0, 100])), 3);
            AssertOriginalRows(await ReadAsync(reader, "SELECT * FROM t ORDER BY id"), 3);
            Assert.Equal(3, (await ReadAsync(reader, "SELECT id FROM t WHERE k >= 0 ORDER BY k")).Rows.Count);
            await AssertCountAsync(reader, 3);
        }
        finally
        {
            await db.RollbackAsync(ct);
        }

        AssertOriginalRows(await ReadAsync(reader, range.Bind([0, 100])), 3);
        await AssertCountAsync(reader, 3);
        await InsertAsync(db, 3, 1);
        AssertOriginalRows(await ReadAsync(reader, range.Bind([0, 100])), 3);
        await AssertCountAsync(reader, 3);
        using var latest = db.CreateReaderSession();
        AssertOriginalRows(await ReadAsync(latest, range.Bind([0, 100])), 4);
        await AssertCountAsync(latest, 4);
    }

    [Theory(Timeout = 30_000)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task ReaderCreatedDuringLegacyRootGrowth_SeesOnlyCommittedCatalog(bool warmReader, bool commit)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        await CreateTableAsync(db);
        await InsertAsync(db, 0, 16);
        var range = Prepare("SELECT * FROM t WHERE k >= @lower AND k < @upper ORDER BY k");
        await db.BeginTransactionAsync(ct);
        await AddRowsAsync(db, 16, 1008);
        await db.ExecuteAsync("ALTER TABLE t ADD COLUMN uncommitted TEXT", ct);
        using var reader = db.CreateReaderSession();
        if (warmReader)
        {
            await AssertCountAsync(reader, 16);
            AssertOriginalRows(await ReadAsync(reader, range.Bind([0, 10240])), 16);
        }

        if (commit)
            await db.CommitAsync(ct);
        else
            await db.RollbackAsync(ct);

        await AssertCountAsync(reader, 16);
        AssertOriginalRows(await ReadAsync(reader, range.Bind([0, 10240])), 16);
        AssertOriginalRows(await ReadAsync(reader, "SELECT * FROM t ORDER BY id"), 16);
        SnapshotRows point = await ReadAsync(reader, "SELECT * FROM t WHERE id = 10");
        Assert.Equal(new[] { "id", "k", "payload" }, point.Columns);
        Assert.Equal(Payload, Assert.Single(point.Rows)[2].AsText);

        using var latest = db.CreateReaderSession();
        await AssertCountAsync(latest, commit ? 1024 : 16);
        SnapshotRows current = await ReadAsync(latest, range.Bind([0, 10240]));
        if (commit)
        {
            Assert.Equal(new[] { "id", "k", "payload", "uncommitted" }, current.Columns);
            Assert.Equal(1024, current.Rows.Count);
            Assert.All(current.Rows, row => Assert.True(row[3].IsNull));
        }
        else
        {
            AssertOriginalRows(current, 16);
        }
    }

    [Fact(Timeout = 30_000)]
    public async Task RejectedExplicitConflict_PreservesCatalogCaptureAfterNextSuccessfulWrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        await CreateTableAsync(db);
        await InsertAsync(db, 0, 16);
        await using var rejected = await db.BeginWriteTransactionAsync(ct);
        await using (var read = await rejected.ExecuteAsync("SELECT payload FROM t WHERE id = 0", ct))
            Assert.Equal(Payload, Assert.Single(await read.ToListAsync(ct))[0].AsText);
        await rejected.ExecuteAsync("UPDATE t SET payload = 'rejected' WHERE id = 1", ct);

        await using (var winner = await db.BeginWriteTransactionAsync(ct))
        {
            await winner.ExecuteAsync("UPDATE t SET payload = 'winner' WHERE id = 0", ct);
            await winner.CommitAsync(ct);
        }
        await Assert.ThrowsAsync<CSharpDbConflictException>(() => rejected.CommitAsync(ct).AsTask());
        await db.ExecuteAsync("INSERT INTO t VALUES (16, 160, 'after conflict')", ct);

        using var reader = db.CreateReaderSession();
        // Check before any read can load the fallback: native rejection must not
        // permanently force subsequent committed states onto the slow load path.
        Assert.NotNull(GetField<Database.ReaderCatalogState>(reader, "_catalogState").Catalog);
        await AssertCountAsync(reader, 17);
        SnapshotRows current = await ReadAsync(reader, "SELECT * FROM t ORDER BY id");
        Assert.Equal(Enumerable.Range(0, 17).Select(i => (long)i), current.Rows.Select(row => row[0].AsInteger));
        Assert.Equal("winner", current.Rows[0][2].AsText);
        Assert.Equal(Payload, current.Rows[1][2].AsText);
        Assert.Equal("after conflict", current.Rows[16][2].AsText);
    }

    [Fact(Timeout = 30_000)]
    public async Task ColdPhysicalReaderCreatedDuringLegacyWrite_UsesCommittedSnapshotAfterCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        await CreateTableAsync(db);
        await InsertAsync(db, 0, 16);
        await db.BeginTransactionAsync(ct);
        await AddRowsAsync(db, 16, 1008);
        await db.ExecuteAsync("ALTER TABLE t ADD COLUMN pending TEXT", ct);
        using var reader = db.CreateReaderSession();
        Assert.Null(GetField<Database.ReaderCatalogState>(reader, "_catalogState").Catalog);
        await db.CommitAsync(ct);

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        SnapshotRows original = await Task.Run(async () =>
        {
            await using var physical = reader.OpenTableReader("t", afterRowIdExclusive: null, maxEncodedRowBytes: 1024 * 1024);
            var rows = new List<DbValue[]>();
            while (await physical.MoveNextAsync(timeout.Token))
            {
                DbValue[] row = physical.Current.ToArray();
                Assert.Equal(row[0].AsInteger, physical.CurrentRowId);
                rows.Add(row);
            }
            return new SnapshotRows(physical.Columns.Select(column => column.Name).ToArray(), rows);
        }, timeout.Token).WaitAsync(TimeSpan.FromSeconds(10), ct);
        AssertOriginalRows(original, 16);
        await AssertCountAsync(reader, 16);
    }

    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadersBetweenOverlappingCommitPublications_KeepCoherentCatalogs(bool completeSecondFirst)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var interceptor = new BlockingCommitEndInterceptor();
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions { Interceptors = [interceptor] },
            },
        };
        await using var db = await Database.OpenInMemoryAsync(options, ct);
        await CreateTableAsync(db);
        await InsertAsync(db, 0, 16);
        await db.ExecuteAsync("CREATE TABLE u (id INTEGER PRIMARY KEY, payload TEXT)", ct);
        await db.ExecuteAsync("INSERT INTO u VALUES (1, 'original')", ct);
        using var original = db.CreateReaderSession();
        await using var firstWriter = await db.BeginWriteTransactionAsync(ct);
        await using var secondWriter = await db.BeginWriteTransactionAsync(ct);
        for (int i = 16; i < 1024; i++)
            await firstWriter.ExecuteAsync($"INSERT INTO t VALUES ({i}, {i * 10L}, '{Payload}')", ct);
        await secondWriter.ExecuteAsync("INSERT INTO u VALUES (2, 'second')", ct);
        var range = Prepare("SELECT * FROM t WHERE id >= @lower AND id < @upper ORDER BY id");

        Task? firstCommit = null;
        Task? secondCommit = null;
        try
        {
            interceptor.Arm();
            firstCommit = firstWriter.CommitAsync(ct).AsTask();
            await interceptor.WaitForCommitAsync(0, ct);
            // WAL publication has finished, but the writer's shared-catalog refresh is blocked.
            using var between = db.CreateReaderSession();
            AssertOriginalRows(await ReadAsync(between, range.Bind([0, 1024])), 1024);
            await AssertCountAsync(between, 1024);
            Assert.Equal(new[] { 1L }, (await ReadAsync(between, "SELECT id FROM u ORDER BY id")).Rows.Select(row => row[0].AsInteger));

            secondCommit = secondWriter.CommitAsync(ct).AsTask();
            await interceptor.WaitForCommitAsync(1, ct);
            using var afterBothPublications = db.CreateReaderSession();
            AssertOriginalRows(await ReadAsync(afterBothPublications, range.Bind([0, 1024])), 1024);
            Assert.Equal(2L, Assert.Single((await ReadAsync(afterBothPublications, "SELECT COUNT(*) FROM u")).Rows)[0].AsInteger);

            int firstToComplete = completeSecondFirst ? 1 : 0;
            interceptor.Release(firstToComplete);
            await (completeSecondFirst ? secondCommit : firstCommit).WaitAsync(TimeSpan.FromSeconds(10), ct);
            interceptor.Release(1 - firstToComplete);
            await Task.WhenAll(firstCommit, secondCommit).WaitAsync(TimeSpan.FromSeconds(10), ct);

            AssertOriginalRows(await ReadAsync(original, range.Bind([0, 1024])), 16);
            await AssertCountAsync(original, 16);
            AssertOriginalRows(await ReadAsync(between, range.Bind([0, 1024])), 1024);
            Assert.Equal(new[] { 1L }, (await ReadAsync(between, "SELECT id FROM u ORDER BY id")).Rows.Select(row => row[0].AsInteger));
            Assert.Equal(new long[] { 1, 2 }, (await ReadAsync(afterBothPublications, "SELECT id FROM u ORDER BY id")).Rows.Select(row => row[0].AsInteger));
            using var latest = db.CreateReaderSession();
            await AssertCountAsync(latest, 1024);
            Assert.Equal(2L, Assert.Single((await ReadAsync(latest, "SELECT COUNT(*) FROM u")).Rows)[0].AsInteger);
        }
        finally
        {
            interceptor.Release(0);
            interceptor.Release(1);
            if (firstCommit is not null)
                await firstCommit.WaitAsync(TimeSpan.FromSeconds(10), ct);
            if (secondCommit is not null)
                await secondCommit.WaitAsync(TimeSpan.FromSeconds(10), ct);
        }
    }

    [Fact(Timeout = 30_000)]
    public async Task ColdReaderCapturedAfterCheckpoint_KeepsRowsAndSchemaAfterGrowthAndAnotherCheckpoint()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_snapshot_checkpoint_{Guid.NewGuid():N}.db");
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions { CheckpointPolicy = new FrameCountCheckpointPolicy(10_000) },
            },
        };
        try
        {
            await using var db = await Database.OpenAsync(path, options, ct);
            await CreateTableAsync(db);
            await InsertAsync(db, 0, 16);
            await db.CheckpointAsync(ct);
            using var reader = db.CreateReaderSession();
            // No query opens a snapshot pager before the writer replaces the on-disk pages.
            await InsertAsync(db, 16, 1008);
            await db.ExecuteAsync("ALTER TABLE t ADD COLUMN added TEXT", ct);
            await db.ExecuteAsync("UPDATE t SET payload = 'changed' WHERE id = 10", ct);
            await db.CheckpointAsync(ct);

            var range = Prepare("SELECT * FROM t WHERE id >= @lower AND id < @upper ORDER BY id");
            AssertOriginalRows(await ReadAsync(reader, range.Bind([0, 1024])), 16);
            AssertOriginalRows(await ReadAsync(reader, "SELECT * FROM t WHERE k >= 0 ORDER BY k"), 16);
            await AssertCountAsync(reader, 16);
            using var latest = db.CreateReaderSession();
            SnapshotRows current = await ReadAsync(latest, range.Bind([0, 1024]));
            Assert.Equal(new[] { "id", "k", "payload", "added" }, current.Columns);
            Assert.Equal(1024, current.Rows.Count);
            Assert.Equal("changed", current.Rows[10][2].AsText);
        }
        finally
        {
            foreach (string file in new[] { path, path + ".wal", path + ".lock" })
                if (File.Exists(file)) File.Delete(file);
        }
    }

    [Fact(Timeout = 30_000)]
    public async Task ViewsJoinsAndSystemCatalog_KeepSnapshotDefinitionsAfterTableReplacement()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        await CreateTableAsync(db);
        await InsertAsync(db, 0, 3);
        await db.ExecuteAsync("CREATE TABLE u (id INTEGER PRIMARY KEY, label TEXT)", ct);
        await db.ExecuteAsync("INSERT INTO u VALUES (1, 'old label')", ct);
        await db.ExecuteAsync("CREATE VIEW v AS SELECT id, payload FROM t", ct);
        using var reader = db.CreateReaderSession();

        await db.ExecuteAsync("DROP VIEW v", ct);
        await db.ExecuteAsync("DROP TABLE t", ct);
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, replacement TEXT)", ct);
        await db.ExecuteAsync("CREATE INDEX ix_t_k ON t(replacement)", ct);
        await db.ExecuteAsync("INSERT INTO t VALUES (1, 'new row')", ct);
        await db.ExecuteAsync("ALTER TABLE u ADD COLUMN added TEXT", ct);
        await db.ExecuteAsync("UPDATE u SET label = 'new label' WHERE id = 1", ct);
        await db.ExecuteAsync("CREATE VIEW v AS SELECT id, replacement FROM t", ct);

        SnapshotRows view = await ReadAsync(reader, "SELECT * FROM v ORDER BY id");
        Assert.Equal(new[] { "id", "payload" }, view.Columns);
        Assert.Equal(new long[] { 0, 1, 2 }, view.Rows.Select(row => row[0].AsInteger));
        Assert.All(view.Rows, row => Assert.Equal(Payload, row[1].AsText));
        SnapshotRows join = await ReadAsync(reader,
            "SELECT t.id AS item_id, t.payload AS original_payload, u.label AS label FROM t JOIN u ON t.id = u.id ORDER BY t.id");
        Assert.Equal(new[] { "item_id", "original_payload", "label" }, join.Columns);
        DbValue[] joined = Assert.Single(join.Rows);
        Assert.Equal(1L, joined[0].AsInteger);
        Assert.Equal(Payload, joined[1].AsText);
        Assert.Equal("old label", joined[2].AsText);
        const string columnsSql = "SELECT column_name FROM sys.columns WHERE table_name = 't' ORDER BY ordinal_position";
        const string indexSql = "SELECT column_name FROM sys.indexes WHERE index_name = 'ix_t_k'";
        Assert.Equal(new[] { "id", "k", "payload" }, (await ReadAsync(reader, columnsSql)).Rows.Select(row => row[0].AsText));
        Assert.Equal("k", Assert.Single((await ReadAsync(reader, indexSql)).Rows)[0].AsText);
        Assert.Equal(3L, Assert.Single((await ReadAsync(reader, "SELECT column_count FROM sys.tables WHERE table_name = 't'")).Rows)[0].AsInteger);
        Assert.Equal(new[] { "id", "label" }, (await ReadAsync(reader,
            "SELECT column_name FROM sys.columns WHERE table_name = 'u' ORDER BY ordinal_position")).Rows.Select(row => row[0].AsText));
        Assert.Contains("payload", Assert.Single((await ReadAsync(reader,
            "SELECT sql FROM sys.views WHERE view_name = 'v'")).Rows)[0].AsText, StringComparison.OrdinalIgnoreCase);

        using var latest = db.CreateReaderSession();
        SnapshotRows latestView = await ReadAsync(latest, "SELECT * FROM v ORDER BY id");
        Assert.Equal(new[] { "id", "replacement" }, latestView.Columns);
        Assert.Equal("new row", Assert.Single(latestView.Rows)[1].AsText);
        Assert.Equal(new[] { "id", "replacement" }, (await ReadAsync(latest, columnsSql)).Rows.Select(row => row[0].AsText));
        Assert.Equal("replacement", Assert.Single((await ReadAsync(latest, indexSql)).Rows)[0].AsText);
    }

    private static async Task CreateTableAsync(Database db)
    {
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, k BIGINT, payload TEXT)", Ct);
        await db.ExecuteAsync("CREATE INDEX ix_t_k ON t(k)", Ct);
    }

    private static async Task InsertAsync(Database db, int start, int count)
    {
        await db.BeginTransactionAsync(Ct);
        await AddRowsAsync(db, start, count);
        await db.CommitAsync(Ct);
    }

    private static async Task AddRowsAsync(Database db, int start, int count)
    {
        var batch = db.PrepareInsertBatch("t", count);
        for (int i = start; i < start + count; i++)
            batch.AddRow([DbValue.FromInteger(i), DbValue.FromInteger(i * 10L), DbValue.FromText(Payload)]);
        await batch.ExecuteAsync(Ct);
    }

    private static PreparedSelectTemplate Prepare(string sql)
        => Assert.IsType<PreparedSelectTemplate>(PreparedSelectTemplate.TryCreate(Parser.Parse(sql), ["lower", "upper"]));

    private static void AssertOriginalRows(SnapshotRows result, int count)
    {
        Assert.Equal(new[] { "id", "k", "payload" }, result.Columns);
        Assert.Equal(Enumerable.Range(0, count).Select(i => (long)i), result.Rows.Select(row => row[0].AsInteger));
        Assert.All(result.Rows, row =>
        {
            Assert.Equal(3, row.Length);
            Assert.Equal(row[0].AsInteger * 10L, row[1].AsInteger);
            Assert.Equal(Payload, row[2].AsText);
        });
    }

    private static async Task AssertCountAsync(Database.ReaderSession reader, long expected)
    {
        SnapshotRows count = await ReadAsync(reader, "SELECT COUNT(*) FROM t");
        Assert.Single(count.Columns);
        Assert.Equal(expected, Assert.Single(Assert.Single(count.Rows)).AsInteger);
    }

    private static Task<SnapshotRows> ReadAsync(Database.ReaderSession reader, string sql)
        => ReadAsync(ct => reader.ExecuteReadAsync(sql, ct));

    private static Task<SnapshotRows> ReadAsync(Database.ReaderSession reader, Statement statement)
        => ReadAsync(ct => reader.ExecuteReadAsync(statement, ct));

    private static async Task<SnapshotRows> ReadAsync(Func<CancellationToken, ValueTask<QueryResult>> execute)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        // Dispatch first: a bad root can loop synchronously before ExecuteReadAsync returns a task.
        return await Task.Run(async () =>
        {
            await using var result = await execute(timeout.Token);
            return new SnapshotRows(result.Schema.Select(column => column.Name).ToArray(), await result.ToListAsync(timeout.Token));
        }, timeout.Token).WaitAsync(TimeSpan.FromSeconds(10), Ct);
    }

    private sealed record SnapshotRows(string[] Columns, List<DbValue[]> Rows);

    private sealed class BlockingCommitEndInterceptor : IPageOperationInterceptor
    {
        private readonly TaskCompletionSource[] _started =
            [new(TaskCreationOptions.RunContinuationsAsynchronously), new(TaskCreationOptions.RunContinuationsAsynchronously)];
        private readonly TaskCompletionSource[] _released =
            [new(TaskCreationOptions.RunContinuationsAsynchronously), new(TaskCreationOptions.RunContinuationsAsynchronously)];
        private int _armed;
        private int _commitCount;

        internal void Arm() => Volatile.Write(ref _armed, 1);
        internal void Release(int ordinal) => _released[ordinal].TrySetResult();
        internal Task WaitForCommitAsync(int ordinal, CancellationToken ct)
            => _started[ordinal].Task.WaitAsync(TimeSpan.FromSeconds(10), ct);

        public ValueTask OnCommitEndAsync(int dirtyPageCount, bool succeeded, CancellationToken ct = default)
        {
            if (!succeeded || Volatile.Read(ref _armed) == 0) return ValueTask.CompletedTask;
            int ordinal = Interlocked.Increment(ref _commitCount) - 1;
            if (ordinal >= _started.Length) return ValueTask.CompletedTask;
            _started[ordinal].TrySetResult();
            return new ValueTask(_released[ordinal].Task.WaitAsync(ct));
        }

        public ValueTask OnBeforeReadAsync(uint pageId, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnAfterReadAsync(uint pageId, PageReadSource source, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnBeforeWriteAsync(uint pageId, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnAfterWriteAsync(uint pageId, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCommitStartAsync(int dirtyPageCount, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCheckpointStartAsync(int committedFrameCount, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCheckpointEndAsync(int committedFrameCount, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnRecoveryStartAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnRecoveryEndAsync(bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
    }

    private static T GetField<T>(object target, string name)
        => (T)target.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(target)!;
}
