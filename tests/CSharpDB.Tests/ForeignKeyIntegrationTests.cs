using CSharpDB.Engine;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class ForeignKeyIntegrationTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private Database _db = null!;

    public ForeignKeyIntegrationTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_fk_test_{Guid.NewGuid():N}.db");
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
    public async Task ForeignKeys_InsertRejectsMissingParent_AndAllowsNullChildValue()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))", ct);

        await _db.ExecuteAsync("INSERT INTO children VALUES (1, NULL)", ct);

        var error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync("INSERT INTO children VALUES (2, 999)", ct).AsTask());
        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);

        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM children", ct));
    }

    [Fact]
    public async Task ForeignKeys_DeleteRestrictPreventsDeletingReferencedParent()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))", ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (1, 1)", ct);

        var error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync("DELETE FROM parents WHERE id = 1", ct).AsTask());
        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);

        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM parents", ct));
        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM children", ct));
    }

    [Fact]
    public async Task ForeignKeys_DeleteCascadeDeletesDependentRows()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id) ON DELETE CASCADE)", ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (1, 1)", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (2, 1)", ct);

        await _db.ExecuteAsync("DELETE FROM parents WHERE id = 1", ct);

        Assert.Equal(0L, await ScalarIntAsync("SELECT COUNT(*) FROM parents", ct));
        Assert.Equal(0L, await ScalarIntAsync("SELECT COUNT(*) FROM children", ct));
    }

    [Fact]
    public async Task ForeignKeys_UpdatingReferencedParentKeyIsRejected()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id) ON DELETE CASCADE)", ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (1, 1)", ct);

        var error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync("UPDATE parents SET id = 2 WHERE id = 1", ct).AsTask());
        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);

        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM parents WHERE id = 1", ct));
        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM children WHERE parent_id = 1", ct));
    }

    [Fact]
    public async Task ForeignKeys_RenameTableAndColumn_RewritesReferencedMetadata()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id) ON DELETE CASCADE)", ct);

        await _db.ExecuteAsync("ALTER TABLE parents RENAME TO accounts", ct);
        await _db.ExecuteAsync("ALTER TABLE accounts RENAME COLUMN id TO account_id", ct);

        Assert.Contains(_db.GetTableNames(), static name => string.Equals(name, "accounts", StringComparison.OrdinalIgnoreCase));
        TableSchema schema = _db.GetTableSchema("children")!;
        ForeignKeyDefinition foreignKey = Assert.Single(schema.ForeignKeys);
        Assert.Equal("accounts", foreignKey.ReferencedTableName);
        Assert.Equal("account_id", foreignKey.ReferencedColumnName);
    }

    [Fact]
    public async Task ForeignKeys_DropParentTableOrBackingUniqueIndex_IsRejected()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY, code TEXT NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_parents_code ON parents(code)", ct);
        await _db.ExecuteAsync("CREATE TABLE children (id INTEGER PRIMARY KEY, parent_code TEXT REFERENCES parents(code))", ct);

        var dropIndexError = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync("DROP INDEX idx_parents_code", ct).AsTask());
        Assert.Equal(ErrorCode.ConstraintViolation, dropIndexError.Code);

        var dropTableError = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync("DROP TABLE parents", ct).AsTask());
        Assert.Equal(ErrorCode.ConstraintViolation, dropTableError.Code);
    }

    [Fact]
    public async Task ForeignKeys_SelfReferencingCascade_DeletesEntireChain()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE nodes (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES nodes(id) ON DELETE CASCADE)",
            ct);

        await _db.ExecuteAsync("INSERT INTO nodes VALUES (1, 1)", ct);
        await _db.ExecuteAsync("INSERT INTO nodes VALUES (2, 1)", ct);
        await _db.ExecuteAsync("INSERT INTO nodes VALUES (3, 2)", ct);

        await _db.ExecuteAsync("DELETE FROM nodes WHERE id = 1", ct);

        Assert.Equal(0L, await ScalarIntAsync("SELECT COUNT(*) FROM nodes", ct));
    }

    [Fact]
    public async Task ForeignKeys_CannotDropHiddenSupportIndexDirectly()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))", ct);

        TableSchema schema = _db.GetTableSchema("children")!;
        string supportingIndexName = Assert.Single(schema.ForeignKeys).SupportingIndexName;

        var error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync($"DROP INDEX {supportingIndexName}", ct).AsTask());
        Assert.Equal(ErrorCode.SyntaxError, error.Code);
    }

    private async Task<long> ScalarIntAsync(string sql, CancellationToken ct)
    {
        await using var result = await _db.ExecuteAsync(sql, ct);
        DbValue[] row = Assert.Single(await result.ToListAsync(ct));
        return row[0].AsInteger;
    }
}
