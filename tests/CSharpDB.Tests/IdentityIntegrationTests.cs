using CSharpDB.Engine;

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
}
