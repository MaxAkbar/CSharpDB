using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class AlterTableRewriteTests : IAsyncLifetime
{
    private readonly string _databasePath =
        Path.Combine(Path.GetTempPath(), $"csharpdb_alter_rewrite_{Guid.NewGuid():N}.db");
    private Database _database = null!;

    public async ValueTask InitializeAsync() =>
        _database = await Database.OpenAsync(
            _databasePath,
            TestContext.Current.CancellationToken);

    public async ValueTask DisposeAsync()
    {
        await _database.DisposeAsync();
        if (File.Exists(_databasePath))
            File.Delete(_databasePath);
        if (File.Exists(_databasePath + ".wal"))
            File.Delete(_databasePath + ".wal");
    }

    [Fact]
    public async Task DropColumn_UsesShadowRootAndPreservesRowsIndexesAndReopen()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE rewrite_items (" +
            "id INTEGER PRIMARY KEY, kept TEXT NOT NULL, removed TEXT)",
            ct);
        await _database.ExecuteAsync(
            "CREATE INDEX ix_rewrite_items_kept ON rewrite_items (kept)",
            ct);

        string values = string.Join(
            ", ",
            Enumerable.Range(1, 200).Select(index =>
                $"({index}, 'kept-{index:D3}', '{new string('x', 80)}')"));
        await _database.ExecuteAsync(
            $"INSERT INTO rewrite_items VALUES {values}",
            ct);

        uint originalRootPage = _database.GetTableRootPage("rewrite_items");
        await _database.ExecuteAsync(
            "ALTER TABLE rewrite_items DROP COLUMN removed",
            ct);

        Assert.NotEqual(originalRootPage, _database.GetTableRootPage("rewrite_items"));
        Assert.Equal(
            ["id", "kept"],
            _database.GetTableSchema("rewrite_items")!.Columns.Select(column => column.Name));
        Assert.Contains(
            _database.GetIndexes(),
            index => string.Equals(
                index.IndexName,
                "ix_rewrite_items_kept",
                StringComparison.OrdinalIgnoreCase));

        await AssertRowsPreservedAsync(ct);

        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_databasePath, ct);
        await AssertRowsPreservedAsync(ct);
    }

    [Fact]
    public async Task AddColumn_WithDefault_RewritesAndBackfillsExistingRows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE add_default_items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO add_default_items VALUES (1, 'one'), (2, 'two')",
            ct);

        uint originalRootPage = _database.GetTableRootPage("add_default_items");
        await _database.ExecuteAsync(
            "ALTER TABLE add_default_items ADD COLUMN status TEXT NOT NULL DEFAULT 'new'",
            ct);

        Assert.NotEqual(originalRootPage, _database.GetTableRootPage("add_default_items"));
        await using (QueryResult result = await _database.ExecuteAsync(
            "SELECT id, status FROM add_default_items ORDER BY id",
            ct))
        {
            List<DbValue[]> rows = await result.ToListAsync(ct);
            Assert.Equal(2, rows.Count);
            Assert.All(rows, row => Assert.Equal("new", row[1].AsText));
        }

        await _database.ExecuteAsync(
            "INSERT INTO add_default_items (id, name) VALUES (3, 'three')",
            ct);
        await using QueryResult inserted = await _database.ExecuteAsync(
            "SELECT status FROM add_default_items WHERE id = 3",
            ct);
        Assert.Equal("new", Assert.Single(await inserted.ToListAsync(ct))[0].AsText);
    }

    [Fact]
    public async Task FailedRewrite_LeavesOriginalTableAndAbortsExplicitTransaction()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE failed_rewrite (id INTEGER PRIMARY KEY, payload TEXT)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO failed_rewrite VALUES (1, 'original')",
            ct);
        uint originalRootPage = _database.GetTableRootPage("failed_rewrite");

        await _database.BeginTransactionAsync(ct);
        CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                "ALTER TABLE failed_rewrite ADD COLUMN required_value TEXT NOT NULL",
                ct));
        Assert.Equal(ErrorCode.ConstraintViolation, failure.Code);

        CSharpDbException commitFailure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.CommitAsync(ct));
        Assert.Contains("rolled back", commitFailure.Message, StringComparison.OrdinalIgnoreCase);

        Assert.Equal(originalRootPage, _database.GetTableRootPage("failed_rewrite"));
        Assert.Equal(
            ["id", "payload"],
            _database.GetTableSchema("failed_rewrite")!.Columns.Select(column => column.Name));
        await using QueryResult result = await _database.ExecuteAsync(
            "SELECT payload FROM failed_rewrite WHERE id = 1",
            ct);
        Assert.Equal("original", Assert.Single(await result.ToListAsync(ct))[0].AsText);
    }

    private async Task AssertRowsPreservedAsync(CancellationToken ct)
    {
        await using QueryResult result = await _database.ExecuteAsync(
            "SELECT id, kept FROM rewrite_items WHERE kept = 'kept-137'",
            ct);
        DbValue[] row = Assert.Single(await result.ToListAsync(ct));
        Assert.Equal(137L, row[0].AsInteger);
        Assert.Equal("kept-137", row[1].AsText);

        await using QueryResult count = await _database.ExecuteAsync(
            "SELECT COUNT(*) FROM rewrite_items",
            ct);
        Assert.Equal(200L, Assert.Single(await count.ToListAsync(ct))[0].AsInteger);
    }
}
