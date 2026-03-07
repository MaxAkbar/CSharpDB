using System.Text;
using CSharpDB.Core;
using CSharpDB.Engine;
using CSharpDB.Storage.BTree;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Serialization;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Tests;

public sealed class IdentityIntegrationTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private Database _db = null!;

    public IdentityIntegrationTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_identity_test_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        _db = await Database.OpenAsync(_dbPath, TestContext.Current.CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await _db.DisposeAsync();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal")) File.Delete(_dbPath + ".wal");
    }

    [Fact]
    public async Task Insert_OmittedIdentityColumn_AutoGeneratesValues()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY IDENTITY, name TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t (name) VALUES ('alice')", ct);
        await _db.ExecuteAsync("INSERT INTO t (name) VALUES ('bob')", ct);

        await using var result = await _db.ExecuteAsync("SELECT id, name FROM t ORDER BY id", ct);
        var rows = await result.ToListAsync(ct);

        Assert.Equal(2, rows.Count);
        Assert.Equal(1L, rows[0][0].AsInteger);
        Assert.Equal("alice", rows[0][1].AsText);
        Assert.Equal(2L, rows[1][0].AsInteger);
        Assert.Equal("bob", rows[1][1].AsText);
    }

    [Fact]
    public async Task Insert_ExplicitIdentityValue_AdvancesGeneratedHighWaterMark()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t (id, name) VALUES (10, 'seed')", ct);
        await _db.ExecuteAsync("INSERT INTO t (name) VALUES ('next')", ct);

        await using var result = await _db.ExecuteAsync("SELECT id, name FROM t ORDER BY id", ct);
        var rows = await result.ToListAsync(ct);

        Assert.Equal(2, rows.Count);
        Assert.Equal(10L, rows[0][0].AsInteger);
        Assert.Equal(11L, rows[1][0].AsInteger);
        Assert.Equal("next", rows[1][1].AsText);
    }

    [Fact]
    public async Task IdentityHighWaterMark_PersistsAcrossReopen()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY IDENTITY, name TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t (id, name) VALUES (20, 'seed')", ct);

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        await _db.ExecuteAsync("INSERT INTO t (name) VALUES ('next')", ct);
        await using var result = await _db.ExecuteAsync("SELECT id FROM t WHERE name = 'next'", ct);
        var rows = await result.ToListAsync(ct);

        var row = Assert.Single(rows);
        Assert.Equal(21L, row[0].AsInteger);
    }

    [Fact]
    public async Task IntegerPrimaryKey_WithoutIdentityKeyword_RemainsBackwardCompatible()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO t (name) VALUES ('legacy')", ct);

        await using var result = await _db.ExecuteAsync("SELECT id FROM t WHERE name = 'legacy'", ct);
        var rows = await result.ToListAsync(ct);

        var row = Assert.Single(rows);
        Assert.Equal(1L, row[0].AsInteger);
    }

    [Fact]
    public async Task LegacyUnknownNextRowId_RemainsUnknownAcrossRename()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE legacy_t (id INTEGER PRIMARY KEY, name TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO legacy_t (id, name) VALUES (1, 'one')", ct);
        await _db.ExecuteAsync("INSERT INTO legacy_t (id, name) VALUES (3, 'three')", ct);

        await _db.DisposeAsync();
        await RewriteTableSchemaAsLegacyAsync(_dbPath, "legacy_t", ct);
        _db = await Database.OpenAsync(_dbPath, ct);

        await _db.ExecuteAsync("ALTER TABLE legacy_t RENAME TO renamed_t", ct);
        await _db.ExecuteAsync("INSERT INTO renamed_t (name) VALUES ('next')", ct);

        await using var result = await _db.ExecuteAsync("SELECT id FROM renamed_t WHERE name = 'next'", ct);
        var rows = await result.ToListAsync(ct);

        var row = Assert.Single(rows);
        Assert.Equal(4L, row[0].AsInteger);
    }

    private static async ValueTask RewriteTableSchemaAsLegacyAsync(string dbPath, string tableName, CancellationToken ct)
    {
        var factory = new DefaultStorageEngineFactory();
        var context = await factory.OpenAsync(dbPath, new StorageEngineOptions(), ct);
        try
        {
            var schema = context.Catalog.GetTable(tableName);
            Assert.NotNull(schema);

            uint tableRootPage = context.Catalog.GetTableRootPage(tableName);
            byte[] legacySchemaBytes = BuildLegacyTableSchemaPayload(tableName, schema!.Columns);
            byte[] payload = new CatalogStore().WriteRootPayload(tableRootPage, legacySchemaBytes);

            var catalogTree = new BTree(context.Pager, context.Pager.SchemaRootPage);
            long key = context.SchemaSerializer.TableNameToKey(tableName);

            await context.Pager.BeginTransactionAsync(ct);
            try
            {
                Assert.True(await catalogTree.DeleteAsync(key, ct));
                await catalogTree.InsertAsync(key, payload, ct);
                await context.Pager.CommitAsync(ct);
            }
            catch
            {
                await context.Pager.RollbackAsync(ct);
                throw;
            }
        }
        finally
        {
            await context.Pager.DisposeAsync();
        }
    }

    private static byte[] BuildLegacyTableSchemaPayload(string tableName, IReadOnlyList<ColumnDefinition> columns)
    {
        using var ms = new MemoryStream();
        WriteVarint(ms, (ulong)Encoding.UTF8.GetByteCount(tableName));
        ms.Write(Encoding.UTF8.GetBytes(tableName));
        WriteVarint(ms, (ulong)columns.Count);

        foreach (var col in columns)
        {
            WriteVarint(ms, (ulong)Encoding.UTF8.GetByteCount(col.Name));
            ms.Write(Encoding.UTF8.GetBytes(col.Name));
            ms.WriteByte((byte)col.Type);

            byte flags = 0;
            if (col.Nullable)
                flags |= 0x01;
            if (col.IsPrimaryKey)
                flags |= 0x02;
            ms.WriteByte(flags);
        }

        return ms.ToArray();
    }

    private static void WriteVarint(Stream stream, ulong value)
    {
        Span<byte> buffer = stackalloc byte[10];
        int len = Varint.Write(buffer, value);
        stream.Write(buffer[..len]);
    }
}
