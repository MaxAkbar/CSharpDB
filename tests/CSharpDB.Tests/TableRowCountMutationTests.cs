using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Catalog;

namespace CSharpDB.Tests;

public sealed class TableRowCountMutationTests : IAsyncLifetime
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"csharpdb_row_count_{Guid.NewGuid():N}.db");
    private Database _database = null!;

    public async ValueTask InitializeAsync()
    {
        _database = await Database.OpenAsync(_path, TestContext.Current.CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await _database.DisposeAsync();
        if (File.Exists(_path)) File.Delete(_path);
        if (File.Exists(_path + ".wal")) File.Delete(_path + ".wal");
    }

    [Theory]
    [InlineData(false, 1)]
    [InlineData(false, 3)]
    [InlineData(true, 1)]
    public async Task Delete_RecountsPostMutationRowsWithoutApplyingDeltaTwice(bool staleExactCount, int rows)
    {
        var ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync("CREATE TABLE diagrams (id INTEGER PRIMARY KEY, name TEXT)", ct);
        for (int id = 1; id <= rows; id++)
            await _database.ExecuteAsync($"INSERT INTO diagrams VALUES ({id}, 'diagram')", ct);

        await _database.BeginTransactionAsync(ct);
        var catalog = Catalog();
        // Reproduce advisory statistics loaded as inexact, or an inconsistent zero
        // cached as exact. The latter must recover via the same physical recount.
        catalog.ApplyCommittedAdvisoryStatisticsSnapshot(
            [new TableStatistics { TableName = "diagrams", RowCount = 0, RowCountIsExact = staleExactCount }], []);
        await _database.ExecuteAsync("DELETE FROM diagrams WHERE id = 1", ct);

        Assert.Equal(rows - 1, catalog.GetTableStatistics("diagrams")!.RowCount);
        Assert.True(catalog.GetTableStatistics("diagrams")!.RowCountIsExact);
        Assert.Equal(-1, Assert.Single(catalog.GetPendingTableRowCountDeltas()).Value);
        await _database.CommitAsync(ct);
        await AssertCountAsync(rows - 1);
        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_path, ct);
        await AssertCountAsync(rows - 1);
    }

    [Fact]
    public async Task Insert_AfterInexactStatistics_DoesNotDoubleCountInsertedRow()
    {
        var ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync("CREATE TABLE diagrams (id INTEGER PRIMARY KEY)", ct);
        await _database.BeginTransactionAsync(ct);
        var catalog = Catalog();
        catalog.ApplyCommittedAdvisoryStatisticsSnapshot(
            [new TableStatistics { TableName = "diagrams", RowCount = 0, RowCountIsExact = false }], []);
        // Exercise the catalog's post-mutation contract directly, also used by
        // collection inserts and the non-batched SQL insert path.
        await catalog.GetTableTree("diagrams").InsertAsync(1, new byte[] { 1 }, ct);
        await catalog.AdjustTableRowCountAsync("diagrams", 1, ct);
        Assert.Equal(1, catalog.GetTableStatistics("diagrams")!.RowCount);
        Assert.Equal(1, Assert.Single(catalog.GetPendingTableRowCountDeltas()).Value);
        await _database.RollbackAsync(ct);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task AutoCommitDelete_DoesNotReintroduceNegativeSharedCountAfterRecount(int rows)
    {
        var ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync("CREATE TABLE diagrams (id INTEGER PRIMARY KEY)", ct);
        for (int id = 1; id <= rows; id++)
            await _database.ExecuteAsync($"INSERT INTO diagrams VALUES ({id})", ct);
        Catalog().ApplyCommittedAdvisoryStatisticsSnapshot(
            [new TableStatistics { TableName = "diagrams", RowCount = 0, RowCountIsExact = true }], []);
        await _database.ExecuteAsync("DELETE FROM diagrams WHERE id = 1", ct);
        await AssertCountAsync(rows - 1);
        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_path, ct);
        await AssertCountAsync(rows - 1);
    }

    private SchemaCatalog Catalog() => (SchemaCatalog)typeof(Database)
        .GetField("_catalog", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(_database)!;

    private async Task AssertCountAsync(long expected)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var count = await _database.ExecuteAsync("SELECT COUNT(*) FROM diagrams", ct);
        Assert.Equal(expected, Assert.Single(await count.ToListAsync(ct))[0].AsInteger);
        await using var stats = await _database.ExecuteAsync("SELECT row_count FROM sys.table_stats WHERE table_name = 'diagrams'", ct);
        Assert.Equal(expected, Assert.Single(await stats.ToListAsync(ct))[0].AsInteger);
    }
}
