using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Tests;

public sealed class CatalogReadSnapshotChangeTests
{
    [Fact]
    public async Task DataOnlyUpdates_ShareMetadataAndKeepIndependentHeaders()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var db = await CreateAsync(ct);
        await db.BeginTransactionAsync(ct);
        for (int i = 0; i < 32; i++)
            await db.ExecuteAsync($"CREATE TABLE unused_{i} (id INTEGER PRIMARY KEY)", ct);
        await db.CommitAsync(ct);
        await db.ExecuteAsync("UPDATE t SET payload = 'warm' WHERE id = 1", ct);
        SchemaCatalog catalog = Catalog(db);
        CatalogReadSnapshot original = catalog.CaptureReadSnapshot();
        uint originalCounter = original.ChangeCounter;
        CatalogReadSnapshot previous = original;

        for (int i = 0; i < 3; i++)
        {
            await db.ExecuteAsync($"UPDATE t SET payload = 'next{i}' WHERE id = 1", ct);
            CatalogReadSnapshot current = catalog.CaptureReadSnapshot(previous);
            Assert.Same(original.Metadata, current.Metadata);
            Assert.NotEqual(previous.ChangeCounter, current.ChangeCounter);
            Assert.True(current.TryGetReadTable("T", out CatalogReadTable table));
            Assert.Same(original.GetTable("t"), table.Schema);
            Assert.Equal(original.GetTableRootPage("t"), table.RootPage);
            previous = current;
        }
        Assert.Equal(originalCounter, original.ChangeCounter);
    }

    [Fact]
    public async Task GrowthAcrossSkippedCaptures_PreservesOldRootsAndCapturesCurrentCounts()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var db = await CreateAsync(ct);
        SchemaCatalog catalog = Catalog(db);
        Pager pager = Field<Pager>(db, "_pager");
        CatalogReadSnapshot original = catalog.CaptureReadSnapshot();
        uint oldIndexRoot = original.GetIndexStore("ix_t_k", pager).RootPageId;
        string payload = new('x', 128);
        for (int start = 2; start < 1026; start += 512)
        {
            await db.BeginTransactionAsync(ct);
            var batch = db.PrepareInsertBatch("t", 512);
            for (int i = start; i < start + 512; i++)
                batch.AddRow([DbValue.FromInteger(i), DbValue.FromInteger(i * 10L), DbValue.FromText(payload)]);
            await batch.ExecuteAsync(ct);
            await catalog.PersistDirtyTableStatisticsAsync(ct);
            await db.CommitAsync(ct);
        }

        CatalogReadSnapshot current = catalog.CaptureReadSnapshot(original);
        Assert.NotEqual(original.GetTableRootPage("t"), current.GetTableRootPage("t"));
        Assert.NotEqual(oldIndexRoot, current.GetIndexStore("ix_t_k", pager).RootPageId);
        Assert.Equal(oldIndexRoot, original.GetIndexStore("ix_t_k", pager).RootPageId);
        Assert.True(original.TryGetExactTableRowCount("t", out long oldCount));
        Assert.Equal(1, oldCount);
        Assert.True(current.TryGetExactTableRowCount("t", out long currentCount));
        Assert.Equal(1025, currentCount);
        Assert.Equal(catalog.GetTable("t")!.NextRowId, current.GetTable("t")!.NextRowId);
    }

    [Fact]
    public async Task CommittedStatsChanges_SurvivePersistenceQueueClearingAndKeepOldFreshness()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var db = await CreateAsync(ct);
        SchemaCatalog catalog = Catalog(db);
        catalog.ApplyCommittedAdvisoryStatisticsSnapshot(
            [new TableStatistics { TableName = "t", RowCount = 1, RowCountIsExact = true }],
            [ColumnStats(stale: false)], markDirty: true);
        CatalogReadSnapshot original = catalog.CaptureReadSnapshot();
        Assert.True(original.TryGetFreshColumnStatistics("t", "k", out _));
        Assert.Single(original.GetColumnStatistics("t"));

        catalog.ApplyCommittedAdvisoryStatisticsSnapshot(
            [new TableStatistics { TableName = "t", RowCount = 2, RowCountIsExact = false, HasStaleColumns = true }],
            [ColumnStats(stale: true)], markDirty: false);
        Assert.Empty(catalog.GetDirtyTableStatistics());
        Assert.Empty(catalog.GetDirtyColumnStatistics());
        CatalogReadSnapshot current = catalog.CaptureReadSnapshot(original);
        Assert.False(current.TryGetExactTableRowCount("t", out _));
        Assert.True(current.TryGetEstimatedTableRowCount("t", out long estimate));
        Assert.Equal(2, estimate);
        Assert.False(current.TryGetFreshColumnStatistics("t", "k", out _));
        Assert.True(Assert.Single(current.GetColumnStatistics("t")).IsStale);
        Assert.True(original.TryGetExactTableRowCount("t", out long oldCount));
        Assert.Equal(1, oldCount);
        Assert.False(Assert.Single(original.GetColumnStatistics("t")).IsStale);
    }

    [Fact]
    public async Task NextRowIdOnlyPersistence_ChangesFrozenSchemaWithoutChangingRoot()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var db = await CreateAsync(ct);
        SchemaCatalog catalog = Catalog(db);
        CatalogReadSnapshot original = catalog.CaptureReadSnapshot();
        long oldNextRowId = original.GetTable("t")!.NextRowId;
        await db.BeginTransactionAsync(ct);
        catalog.GetTable("t")!.NextRowId = 5000;
        Assert.False(await catalog.PersistAllRootPageChangesAndDetectChangesAsync(ct));
        await db.CommitAsync(ct);
        CatalogReadSnapshot current = catalog.CaptureReadSnapshot(original);

        Assert.Equal(5000, current.GetTable("t")!.NextRowId);
        Assert.Equal(oldNextRowId, original.GetTable("t")!.NextRowId);
        Assert.Equal(original.GetTableRootPage("t"), current.GetTableRootPage("t"));
        Assert.Same(original.GetForeignKeysForTable("t"), current.GetForeignKeysForTable("t"));
        Assert.Same(original.GetReferencingForeignKeys("t"), current.GetReferencingForeignKeys("t"));
    }

    [Fact]
    public async Task OverflowedChangeJournal_CapturesEveryUpdatedTable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        await db.BeginTransactionAsync(ct);
        int count = CatalogReadChanges.MaximumEntries + 2;
        for (int i = 0; i < count; i++)
            await db.ExecuteAsync($"CREATE TABLE t{i} (id INTEGER PRIMARY KEY)", ct);
        await db.CommitAsync(ct);
        SchemaCatalog catalog = Catalog(db);
        CatalogReadSnapshot original = catalog.CaptureReadSnapshot();
        var hints = Enumerable.Range(0, count).Select(i => new KeyValuePair<string, long>($"t{i}", 1000 + i)).ToArray();
        catalog.ApplyCommittedTableMetadataSnapshot(hints);
        CatalogReadSnapshot current = catalog.CaptureReadSnapshot(original);
        foreach (var hint in hints)
        {
            Assert.Equal(hint.Value, current.GetTable(hint.Key)!.NextRowId);
            Assert.NotEqual(hint.Value, original.GetTable(hint.Key)!.NextRowId);
        }
    }

    [Fact]
    public async Task ForeignDescriptorAndReload_RequireFullCaptureAfterSchemaChangeAndRollback()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var db = await CreateAsync(ct);
        await using var other = await CreateAsync(ct);
        SchemaCatalog catalog = Catalog(db);
        CatalogReadSnapshot original = catalog.CaptureReadSnapshot();
        CatalogReadSnapshot foreign = Catalog(other).CaptureReadSnapshot();
        catalog.ApplyCommittedTableMetadataSnapshot([new("t", 7000)]);
        CatalogReadSnapshot fromForeign = catalog.CaptureReadSnapshot(foreign);
        Assert.Equal(7000, fromForeign.GetTable("t")!.NextRowId);
        Assert.NotSame(foreign.Metadata, fromForeign.Metadata);

        await db.ExecuteAsync("ALTER TABLE t ADD COLUMN added TEXT", ct);
        CatalogReadSnapshot added = catalog.CaptureReadSnapshot(fromForeign);
        Assert.Equal(new[] { "id", "k", "payload", "added" }, added.GetTable("t")!.Columns.Select(column => column.Name));
        Assert.Equal(3, original.GetTable("t")!.Columns.Count);
        await db.BeginTransactionAsync(ct);
        await db.ExecuteAsync("ALTER TABLE t ADD COLUMN rolled_back TEXT", ct);
        await db.RollbackAsync(ct);
        CatalogReadSnapshot restored = catalog.CaptureReadSnapshot(added);
        Assert.Equal(new[] { "id", "k", "payload", "added" }, restored.GetTable("t")!.Columns.Select(column => column.Name));
        Assert.True(restored.TryGetExactTableRowCount("t", out long count));
        Assert.Equal(1, count);
    }

    private static ColumnStatistics ColumnStats(bool stale) => new()
    {
        TableName = "t", ColumnName = "k", DistinctCount = 1, NonNullCount = 1,
        MinValue = DbValue.FromInteger(10), MaxValue = DbValue.FromInteger(10), IsStale = stale,
    };

    private static async Task<Database> CreateAsync(CancellationToken ct)
    {
        Database db = await Database.OpenInMemoryAsync(ct);
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, k BIGINT, payload TEXT)", ct);
        await db.ExecuteAsync("CREATE INDEX ix_t_k ON t(k)", ct);
        await db.ExecuteAsync("INSERT INTO t VALUES (1, 10, 'initial')", ct);
        return db;
    }

    private static SchemaCatalog Catalog(Database db) => Field<SchemaCatalog>(db, "_catalog");
    private static T Field<T>(object target, string name)
        => (T)target.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(target)!;
}
