using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Storage.Catalog;

namespace CSharpDB.Tests;

public sealed class TableScanBatchLifecycleTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task RowOnlyLifecycle_DoesNotAllocateBatchStorage()
    {
        await using var db = await CreateAsync();
        await using var scan = CreateScan(db);
        AssertNoBatchStorage(scan);
        scan.SetReuseCurrentBatch(false);
        AssertNoBatchStorage(scan);
        for (int execution = 0; execution < 2; execution++)
        {
            await scan.OpenAsync(Ct);
            AssertNoBatchStorage(scan);
            for (int id = 0; id < 130; id++)
            {
                Assert.True(await scan.MoveNextAsync(Ct));
                AssertRow(scan.Current, id);
                AssertNoBatchStorage(scan);
            }
            Assert.False(await scan.MoveNextAsync(Ct));
            await scan.DisposeAsync();
            AssertNoBatchStorage(scan);
        }
    }

    [Fact]
    public async Task EmptyTable_RowReadsStayLazyAndBatchLifecyclePreservesEmptySchema()
    {
        await using var db = await CreateAsync(0);
        await using var scan = CreateScan(db);
        var batches = (IBatchOperator)scan;
        AssertNoBatchStorage(scan);
        await scan.OpenAsync(Ct);
        Assert.False(await scan.MoveNextAsync(Ct));
        AssertNoBatchStorage(scan);
        await scan.DisposeAsync();
        AssertNoBatchStorage(scan);

        RowBatch beforeOpen = batches.CurrentBatch;
        Assert.Equal(3, beforeOpen.ColumnCount);
        Assert.Equal(0, beforeOpen.Count);
        await scan.OpenAsync(Ct);
        AssertNoBatchStorage(scan);
        Assert.False(await batches.MoveNextBatchAsync(Ct));
        RowBatch afterEmptyRead = batches.CurrentBatch;
        Assert.NotSame(beforeOpen, afterEmptyRead);
        Assert.Equal(3, afterEmptyRead.ColumnCount);
        Assert.Equal(0, afterEmptyRead.Count);

        await scan.DisposeAsync();
        await scan.OpenAsync(Ct);
        AssertNoBatchStorage(scan);
        RowBatch reopened = batches.CurrentBatch;
        Assert.NotSame(afterEmptyRead, reopened);
        Assert.Equal(3, reopened.ColumnCount);
        Assert.Equal(0, reopened.Count);
        Assert.False(await batches.MoveNextBatchAsync(Ct));
        Assert.Equal(0, batches.CurrentBatch.Count);
    }

    [Fact]
    public async Task BatchReads_AreLazyAndReuseStorageAcrossTheSixtyFourRowBoundary()
    {
        await using var db = await CreateAsync();
        await using var scan = CreateScan(db);
        var batches = (IBatchOperator)scan;
        AssertNoBatchStorage(scan);
        RowBatch beforeOpen = batches.CurrentBatch;
        Assert.Equal(3, beforeOpen.ColumnCount);
        Assert.Equal(0, beforeOpen.Count);

        await scan.OpenAsync(Ct);
        AssertNoBatchStorage(scan);
        RowBatch? first = null;
        int nextId = 0;
        foreach (int expectedCount in new[] { 64, 64, 2 })
        {
            Assert.True(await batches.MoveNextBatchAsync(Ct));
            RowBatch batch = batches.CurrentBatch;
            Assert.NotSame(beforeOpen, batch);
            if (first is null) first = batch;
            else Assert.Same(first, batch);
            Assert.Equal(expectedCount, batch.Count);
            for (int row = 0; row < batch.Count; row++)
                AssertRow(batch.GetRowSpan(row), nextId++);
        }
        Assert.Equal(130, nextId);
        Assert.False(await batches.MoveNextBatchAsync(Ct));
        Assert.Equal(0, batches.CurrentBatch.Count);
    }

    [Fact]
    public async Task ReuseDisabled_RetainedBatchesSurviveFurtherReadsAndReopen()
    {
        await using var db = await CreateAsync();
        await using var scan = CreateScan(db);
        var batches = (IBatchOperator)scan;
        await scan.OpenAsync(Ct);
        scan.SetReuseCurrentBatch(false);
        AssertNoBatchStorage(scan);
        Assert.False(batches.ReusesCurrentBatch);

        var retained = new List<RowBatch>();
        while (await batches.MoveNextBatchAsync(Ct))
        {
            RowBatch batch = batches.CurrentBatch;
            Assert.DoesNotContain(retained, previous => ReferenceEquals(previous, batch));
            retained.Add(batch);
        }
        Assert.Equal(new[] { 64, 64, 2 }, retained.Select(batch => batch.Count));

        await scan.DisposeAsync();
        await scan.OpenAsync(Ct);
        AssertNoBatchStorage(scan);
        scan.SetReuseCurrentBatch(true);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        RowBatch reopened = batches.CurrentBatch;
        Assert.DoesNotContain(retained, previous => ReferenceEquals(previous, reopened));
        scan.SetReuseCurrentBatch(false);
        AssertNoBatchStorage(scan);
        AssertRow(reopened.GetRowSpan(0), 0);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        Assert.NotSame(reopened, batches.CurrentBatch);

        int nextId = 0;
        foreach (RowBatch batch in retained)
            for (int row = 0; row < batch.Count; row++)
                AssertRow(batch.GetRowSpan(row), nextId++);
        Assert.Equal(130, nextId);
    }

    [Theory]
    [InlineData("upper", 2)]
    [InlineData("selected", 3)]
    [InlineData("empty", 0)]
    public async Task ReopenedBatch_UsesCurrentDecodeWidthAndSelection(string mode, int expectedWidth)
    {
        await using var db = await CreateAsync();
        await using var scan = CreateScan(db);
        var batches = (IBatchOperator)scan;
        await scan.OpenAsync(Ct);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        RowBatch original = batches.CurrentBatch;
        Assert.Equal(3, original.ColumnCount);
        await scan.DisposeAsync();

        if (mode == "upper") scan.SetDecodedColumnUpperBound(1);
        else if (mode == "selected") scan.SetDecodedColumnIndices([2, 0, 2, -1, 99]);
        else scan.SetDecodedColumnIndices([]);

        await scan.OpenAsync(Ct);
        AssertNoBatchStorage(scan);
        RowBatch empty = batches.CurrentBatch;
        Assert.NotSame(original, empty);
        Assert.Equal(expectedWidth, empty.ColumnCount);
        Assert.Equal(0, empty.Count);
        int nextId = 0;
        while (await batches.MoveNextBatchAsync(Ct))
        {
            RowBatch batch = batches.CurrentBatch;
            Assert.Same(empty, batch);
            Assert.Equal(expectedWidth, batch.ColumnCount);
            for (int rowIndex = 0; rowIndex < batch.Count; rowIndex++, nextId++)
            {
                DbValue[] row = batch.GetRowSpan(rowIndex).ToArray();
                Assert.Equal(expectedWidth, row.Length);
                if (mode == "upper")
                    Assert.Equal(new long[] { nextId, nextId * 10 }, row.Select(value => value.AsInteger));
                else if (mode == "selected")
                {
                    Assert.Equal(nextId, row[0].AsInteger);
                    Assert.True(row[1].IsNull);
                    Assert.Equal(1000 + nextId, row[2].AsInteger);
                }
            }
        }
        Assert.Equal(130, nextId);

        await scan.DisposeAsync();
        scan.SetDecodedColumnUpperBound(2);
        await scan.OpenAsync(Ct);
        AssertNoBatchStorage(scan);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        Assert.Equal(3, batches.CurrentBatch.ColumnCount);
        AssertRow(batches.CurrentBatch.GetRowSpan(0), 0);
    }

    [Fact]
    public async Task AlternatingRowAndBatchReads_AdvanceOneSharedCursorWithoutSkippingRows()
    {
        await using var db = await CreateAsync();
        await using var scan = CreateScan(db);
        var batches = (IBatchOperator)scan;
        await scan.OpenAsync(Ct);
        int nextId = 0;
        for (int cycle = 0; cycle < 2; cycle++)
        {
            Assert.True(await scan.MoveNextAsync(Ct));
            AssertRow(scan.Current, nextId++);
            Assert.True(await batches.MoveNextBatchAsync(Ct));
            Assert.Equal(64, batches.CurrentBatch.Count);
            for (int row = 0; row < batches.CurrentBatch.Count; row++)
                AssertRow(batches.CurrentBatch.GetRowSpan(row), nextId++);
        }
        Assert.Equal(130, nextId);
        Assert.False(await scan.MoveNextAsync(Ct));
        Assert.False(await batches.MoveNextBatchAsync(Ct));
    }

    private static async Task<Database> CreateAsync(int rowCount = 130)
    {
        Database db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER, extra INTEGER)", Ct);
        if (rowCount == 0) return db;
        await db.BeginTransactionAsync(Ct);
        var insert = db.PrepareInsertBatch("t", rowCount);
        for (int id = 0; id < rowCount; id++)
            insert.AddRow([DbValue.FromInteger(id), DbValue.FromInteger(id * 10), DbValue.FromInteger(1000 + id)]);
        await insert.ExecuteAsync(Ct);
        await db.CommitAsync(Ct);
        return db;
    }

    private static TableScanOperator CreateScan(Database db)
    {
        var catalog = Assert.IsType<SchemaCatalog>(typeof(Database)
            .GetField("_catalog", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(db));
        return new TableScanOperator(catalog.GetTableTree("t"), catalog.GetTable("t")!);
    }

    private static void AssertNoBatchStorage(TableScanOperator scan)
        => Assert.Null(typeof(TableScanOperator)
            .GetField("_currentBatch", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(scan));

    private static void AssertRow(ReadOnlySpan<DbValue> row, int id)
    {
        Assert.Equal(3, row.Length);
        Assert.Equal(id, row[0].AsInteger);
        Assert.Equal(id * 10, row[1].AsInteger);
        Assert.Equal(1000 + id, row[2].AsInteger);
    }
}
