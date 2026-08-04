using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class ForeignKeyPhase3MultiRowTests : IAsyncLifetime
{
    private readonly string _dbPath =
        Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_fk_phase3_multirow_{Guid.NewGuid():N}.db");
    private Database _db = null!;

    public async ValueTask InitializeAsync()
    {
        _db = await Database.OpenAsync(
            _dbPath,
            TestContext.Current.CancellationToken);
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
    public async Task OnUpdateCascade_MultiRowSelfReference_PreservesEarlierCascade()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            """
            CREATE TABLE nodes (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES nodes(id) ON UPDATE CASCADE
            )
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO nodes VALUES (1, NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO nodes VALUES (2, 1)", ct);

        await using (QueryResult update =
                     await _db.ExecuteAsync(
                         "UPDATE nodes SET id = id + 10",
                         ct))
        {
            Assert.Equal(2, update.RowsAffected);
        }

        await using QueryResult result =
            await _db.ExecuteAsync(
                "SELECT id, parent_id FROM nodes ORDER BY id",
                ct);
        IReadOnlyList<DbValue[]> rows = await result.ToListAsync(ct);

        Assert.Equal(2, rows.Count);
        Assert.Equal(11L, rows[0][0].AsInteger);
        Assert.True(rows[0][1].IsNull);
        Assert.Equal(12L, rows[1][0].AsInteger);
        Assert.Equal(11L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task OnUpdateCascade_MultiRowPhysicalChildKey_ResolvesRemappedTarget()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            """
            CREATE TABLE nodes (
                id INTEGER PRIMARY KEY,
                parent_key INTEGER NOT NULL
            )
            """,
            ct);
        await _db.ExecuteAsync(
            "CREATE UNIQUE INDEX ux_nodes_parent_key ON nodes(parent_key)",
            ct);
        await _db.ExecuteAsync("INSERT INTO nodes VALUES (1, 2)", ct);
        await _db.ExecuteAsync("INSERT INTO nodes VALUES (2, 1)", ct);
        await _db.ExecuteAsync(
            """
            ALTER TABLE nodes
            ADD CONSTRAINT fk_nodes_id_parent_key
            FOREIGN KEY (id) REFERENCES nodes(parent_key)
            ON UPDATE CASCADE
            """,
            ct);

        await using (QueryResult update =
                     await _db.ExecuteAsync(
                         "UPDATE nodes SET parent_key = parent_key + 10",
                         ct))
        {
            Assert.Equal(2, update.RowsAffected);
        }

        await using QueryResult result =
            await _db.ExecuteAsync(
                "SELECT id, parent_key FROM nodes ORDER BY id",
                ct);
        IReadOnlyList<DbValue[]> rows = await result.ToListAsync(ct);

        Assert.Equal(2, rows.Count);
        Assert.Equal(11L, rows[0][0].AsInteger);
        Assert.Equal(12L, rows[0][1].AsInteger);
        Assert.Equal(12L, rows[1][0].AsInteger);
        Assert.Equal(11L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task OnUpdateCascade_ReusedFreedKey_PreservesStableTargetIdentity()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            """
            CREATE TABLE nodes (
                id INTEGER PRIMARY KEY,
                parent_key INTEGER NOT NULL
            )
            """,
            ct);
        await _db.ExecuteAsync(
            "CREATE UNIQUE INDEX ux_nodes_parent_key ON nodes(parent_key)",
            ct);
        await _db.ExecuteAsync("INSERT INTO nodes VALUES (10, 20)", ct);
        await _db.ExecuteAsync("INSERT INTO nodes VALUES (20, 25)", ct);
        await _db.ExecuteAsync("INSERT INTO nodes VALUES (25, 10)", ct);
        await _db.ExecuteAsync(
            """
            ALTER TABLE nodes
            ADD CONSTRAINT fk_nodes_id_parent_key
            FOREIGN KEY (id) REFERENCES nodes(parent_key)
            ON UPDATE CASCADE
            """,
            ct);

        await using (QueryResult update =
                     await _db.ExecuteAsync(
                         "UPDATE nodes SET parent_key = parent_key - 5",
                         ct))
        {
            Assert.Equal(3, update.RowsAffected);
        }

        await using QueryResult result =
            await _db.ExecuteAsync(
                "SELECT id, parent_key FROM nodes ORDER BY id",
                ct);
        IReadOnlyList<DbValue[]> rows = await result.ToListAsync(ct);

        Assert.Equal(3, rows.Count);
        Assert.Equal(5L, rows[0][0].AsInteger);
        Assert.Equal(15L, rows[0][1].AsInteger);
        Assert.Equal(15L, rows[1][0].AsInteger);
        Assert.Equal(20L, rows[1][1].AsInteger);
        Assert.Equal(20L, rows[2][0].AsInteger);
        Assert.Equal(5L, rows[2][1].AsInteger);
    }

    [Fact]
    public async Task OnUpdateCascade_SelfReferenceToAlternateKey_UsesPendingParentImage()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            """
            CREATE TABLE nodes (
                id INTEGER PRIMARY KEY,
                alternate_parent INTEGER NOT NULL,
                alternate_key INTEGER NOT NULL,
                CONSTRAINT uq_nodes_alternate_key UNIQUE (alternate_key)
            )
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO nodes VALUES (1, 10, 10)", ct);
        await _db.ExecuteAsync(
            """
            ALTER TABLE nodes
            ADD CONSTRAINT fk_nodes_alternate_parent
            FOREIGN KEY (alternate_parent) REFERENCES nodes(alternate_key)
            ON UPDATE CASCADE
            """,
            ct);

        await using (QueryResult update =
                     await _db.ExecuteAsync(
                         "UPDATE nodes SET alternate_key = 11 WHERE id = 1",
                         ct))
        {
            Assert.Equal(1, update.RowsAffected);
        }

        await using QueryResult result =
            await _db.ExecuteAsync(
                "SELECT alternate_parent, alternate_key FROM nodes WHERE id = 1",
                ct);
        DbValue[] row = Assert.Single(await result.ToListAsync(ct));

        Assert.Equal(11L, row[0].AsInteger);
        Assert.Equal(11L, row[1].AsInteger);
    }

    [Fact]
    public async Task OnUpdateCascade_SelfCascadePropagatesNewParentKey_RegardlessOfReferenceOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            """
            CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_key INTEGER NOT NULL
            )
            """,
            ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE nodes (
                id INTEGER PRIMARY KEY,
                alternate_parent INTEGER NOT NULL,
                alternate_key INTEGER NOT NULL,
                CONSTRAINT uq_nodes_alternate_parent UNIQUE (alternate_parent),
                CONSTRAINT uq_nodes_alternate_key UNIQUE (alternate_key)
            )
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO nodes VALUES (1, 10, 10)", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (1, 10)", ct);
        await _db.ExecuteAsync(
            """
            ALTER TABLE children
            ADD CONSTRAINT fk_children_parent_key
            FOREIGN KEY (parent_key) REFERENCES nodes(alternate_parent)
            ON UPDATE CASCADE
            """,
            ct);
        await _db.ExecuteAsync(
            """
            ALTER TABLE nodes
            ADD CONSTRAINT fk_nodes_alternate_parent
            FOREIGN KEY (alternate_parent) REFERENCES nodes(alternate_key)
            ON UPDATE CASCADE
            """,
            ct);

        await using (QueryResult update =
                     await _db.ExecuteAsync(
                         "UPDATE nodes SET alternate_key = 11 WHERE id = 1",
                         ct))
        {
            Assert.Equal(1, update.RowsAffected);
        }

        await using QueryResult nodeResult =
            await _db.ExecuteAsync(
                "SELECT alternate_parent, alternate_key FROM nodes WHERE id = 1",
                ct);
        DbValue[] node = Assert.Single(await nodeResult.ToListAsync(ct));
        Assert.Equal(11L, node[0].AsInteger);
        Assert.Equal(11L, node[1].AsInteger);

        await using QueryResult childResult =
            await _db.ExecuteAsync(
                "SELECT parent_key FROM children WHERE id = 1",
                ct);
        DbValue[] child = Assert.Single(await childResult.ToListAsync(ct));
        Assert.Equal(11L, child[0].AsInteger);
    }

    [Fact]
    public async Task OnUpdateCascade_SecondOrderSelfCascadeSettlesInAdverseReferenceOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            """
            CREATE TABLE nodes (
                id INTEGER PRIMARY KEY,
                first_parent INTEGER NOT NULL,
                second_parent INTEGER NOT NULL,
                root_key INTEGER NOT NULL,
                CONSTRAINT uq_nodes_second_parent UNIQUE (second_parent),
                CONSTRAINT uq_nodes_root_key UNIQUE (root_key)
            )
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO nodes VALUES (1, 10, 10, 10)", ct);
        await _db.ExecuteAsync(
            """
            ALTER TABLE nodes
            ADD CONSTRAINT fk_nodes_first_parent
            FOREIGN KEY (first_parent) REFERENCES nodes(second_parent)
            ON UPDATE CASCADE
            """,
            ct);
        await _db.ExecuteAsync(
            """
            ALTER TABLE nodes
            ADD CONSTRAINT fk_nodes_second_parent
            FOREIGN KEY (second_parent) REFERENCES nodes(root_key)
            ON UPDATE CASCADE
            """,
            ct);

        await using (QueryResult update =
                     await _db.ExecuteAsync(
                         "UPDATE nodes SET root_key = 11 WHERE id = 1",
                         ct))
        {
            Assert.Equal(1, update.RowsAffected);
        }

        await using QueryResult result =
            await _db.ExecuteAsync(
                """
                SELECT first_parent, second_parent, root_key
                FROM nodes
                WHERE id = 1
                """,
                ct);
        DbValue[] node = Assert.Single(await result.ToListAsync(ct));

        Assert.Equal(11L, node[0].AsInteger);
        Assert.Equal(11L, node[1].AsInteger);
        Assert.Equal(11L, node[2].AsInteger);
    }

    [Fact]
    public async Task OnDeleteSetDefault_DoesNotCascadeBackIntoRowBeingDeleted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            """
            CREATE TABLE nodes (
                id INTEGER PRIMARY KEY,
                linked_owner INTEGER,
                CONSTRAINT uq_nodes_linked_owner UNIQUE (linked_owner)
            )
            """,
            ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE links (
                id INTEGER PRIMARY KEY,
                owner_id INTEGER NOT NULL DEFAULT 2
                    REFERENCES nodes(id) ON DELETE SET DEFAULT,
                CONSTRAINT uq_links_owner_id UNIQUE (owner_id)
            )
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO nodes VALUES (1, 1)", ct);
        await _db.ExecuteAsync("INSERT INTO nodes VALUES (2, NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO links VALUES (10, 1)", ct);
        await _db.ExecuteAsync(
            """
            ALTER TABLE nodes
            ADD CONSTRAINT fk_nodes_linked_owner
            FOREIGN KEY (linked_owner) REFERENCES links(owner_id)
            ON UPDATE CASCADE
            """,
            ct);

        await _db.ExecuteAsync("DELETE FROM nodes WHERE id = 1", ct);

        await using (QueryResult links =
                     await _db.ExecuteAsync(
                         "SELECT owner_id FROM links WHERE id = 10",
                         ct))
        {
            DbValue[] link = Assert.Single(await links.ToListAsync(ct));
            Assert.Equal(2L, link[0].AsInteger);
        }

        await _db.ExecuteAsync("INSERT INTO nodes VALUES (3, 2)", ct);
        await using QueryResult nodes =
            await _db.ExecuteAsync(
                "SELECT id, linked_owner FROM nodes ORDER BY id",
                ct);
        IReadOnlyList<DbValue[]> rows = await nodes.ToListAsync(ct);

        Assert.Equal(2, rows.Count);
        Assert.Equal(2L, rows[0][0].AsInteger);
        Assert.True(rows[0][1].IsNull);
        Assert.Equal(3L, rows[1][0].AsInteger);
        Assert.Equal(2L, rows[1][1].AsInteger);
    }

    [Fact]
    public async Task RealLiteralDefaultMetadataRoundTripsAcrossReopen()
    {
        const string fallbackSql = "0.0000001";
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            $"""
             CREATE TABLE measurements (
                 id INTEGER PRIMARY KEY,
                 reading REAL NOT NULL DEFAULT {fallbackSql}
             )
             """,
            ct);

        Assert.Equal(
            fallbackSql,
            _db.GetTableSchema("measurements")!.Columns[1].DefaultSql);

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        Assert.Equal(
            fallbackSql,
            _db.GetTableSchema("measurements")!.Columns[1].DefaultSql);
        await _db.ExecuteAsync(
            "INSERT INTO measurements (id) VALUES (1)",
            ct);

        await using QueryResult result =
            await _db.ExecuteAsync(
                "SELECT reading FROM measurements WHERE id = 1",
                ct);
        DbValue[] row = Assert.Single(await result.ToListAsync(ct));
        Assert.Equal(
            BitConverter.DoubleToInt64Bits(0.0000001d),
            BitConverter.DoubleToInt64Bits(row[0].AsReal));
    }
}
