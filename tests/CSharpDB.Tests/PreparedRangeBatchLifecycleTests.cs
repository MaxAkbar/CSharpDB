using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class PreparedRangeBatchLifecycleTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("scan", 130)]
    [InlineData("projection", 130)]
    [InlineData("limit", 70)]
    [InlineData("equality", 130)]
    public async Task RowReadsThenReopenedBatchReads_PreserveBatchOwnership(string kind, int expectedRows)
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        db.PreferSyncPointLookups = false;
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER)", Ct);
        await db.BeginTransactionAsync(Ct);
        var insert = db.PrepareInsertBatch("t", 130);
        for (int value = 0; value < 130; value++)
            insert.AddRow([DbValue.FromInteger(value), DbValue.FromInteger(kind == "equality" ? 0 : value)]);
        await insert.ExecuteAsync(Ct);
        await db.CommitAsync(Ct);
        await db.ExecuteAsync("CREATE INDEX ix_value ON t(value)", Ct);

        string projection = kind is "scan" or "equality" ? "*" : "id, value";
        string limit = kind == "limit" ? " LIMIT 70" : "";
        string comparison = kind == "equality" ? "=" : ">=";
        var template = Assert.IsType<PreparedSelectTemplate>(PreparedSelectTemplate.TryCreate(
            Parser.Parse($"SELECT {projection} FROM t WHERE value {comparison} @lower ORDER BY value{limit}"), ["lower"]));
        await using var result = await db.ExecuteAsync(template.Bind([0]), Ct);
        IOperator op = kind switch
        {
            "scan" => Assert.IsType<IndexOrderedScanOperator>(result.PhysicalRootOperator),
            "projection" => Assert.IsType<IndexOrderedProjectionScanOperator>(result.PhysicalRootOperator),
            "equality" => Assert.IsType<IndexScanOperator>(result.PhysicalRootOperator),
            _ => Assert.IsType<LimitOperator>(result.PhysicalRootOperator),
        };
        var batches = Assert.IsAssignableFrom<IBatchOperator>(op);
        var controller = Assert.IsAssignableFrom<IBatchBufferReuseController>(op);

        // Merely inspecting the batch contract must remain valid before the operator is opened.
        RowBatch beforeOpen = batches.CurrentBatch;
        Assert.Equal(2, beforeOpen.ColumnCount);
        Assert.Equal(0, beforeOpen.Count);
        await op.OpenAsync(Ct);
        RowBatch afterOpen = batches.CurrentBatch;
        Assert.NotSame(beforeOpen, afterOpen);
        Assert.Equal(0, afterOpen.Count);
        for (int value = 0; value < 4; value++)
        {
            Assert.True(await op.MoveNextAsync(Ct));
            Assert.Equal(new long[] { value, kind == "equality" ? 0 : value }, op.Current.Select(cell => cell.AsInteger));
        }

        await op.DisposeAsync();
        await op.OpenAsync(Ct);
        RowBatch reopened = batches.CurrentBatch;
        Assert.NotSame(afterOpen, reopened);
        Assert.Equal(0, reopened.Count);
        controller.SetReuseCurrentBatch(false);
        Assert.False(batches.ReusesCurrentBatch);
        RowBatch afterDisablingReuse = batches.CurrentBatch;
        Assert.NotSame(reopened, afterDisablingReuse);
        Assert.Equal(0, afterDisablingReuse.Count);

        var retained = new List<(RowBatch Batch, long[][] Rows)>();
        var actual = new List<long>();
        while (await batches.MoveNextBatchAsync(Ct))
        {
            RowBatch batch = batches.CurrentBatch;
            Assert.DoesNotContain(retained, previous => ReferenceEquals(previous.Batch, batch));
            long[][] rows = Snapshot(batch);
            retained.Add((batch, rows));
            foreach (long[] row in rows)
            {
                Assert.Equal(kind == "equality" ? 0 : row[0], row[1]);
                actual.Add(row[0]);
            }
        }
        Assert.Equal(Enumerable.Range(0, expectedRows).Select(value => (long)value), actual);
        Assert.True(retained.Count >= 2);

        await op.DisposeAsync();
        await op.OpenAsync(Ct);
        RowBatch finalEmpty = batches.CurrentBatch;
        Assert.Equal(0, finalEmpty.Count);
        controller.SetReuseCurrentBatch(true);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        Assert.Same(finalEmpty, batches.CurrentBatch);
        Assert.Equal(0, batches.CurrentBatch.GetRowSpan(0)[0].AsInteger);
        foreach (var previous in retained)
        {
            Assert.NotSame(previous.Batch, batches.CurrentBatch);
            Assert.Equal(previous.Rows.Length, previous.Batch.Count);
            for (int row = 0; row < previous.Rows.Length; row++)
                Assert.Equal(previous.Rows[row], previous.Batch.GetRowSpan(row).ToArray().Select(cell => cell.AsInteger));
        }
    }

    [Fact]
    public async Task ZeroLimit_ExposesEmptyBatchesWithoutConsumingRows()
    {
        ColumnDefinition[] schema = [new() { Name = "id", Type = DbType.Integer }];
        var source = new MaterializedOperator([[DbValue.FromInteger(1)]], schema);
        await using var op = new LimitOperator(source, 0);
        var batches = (IBatchOperator)op;
        RowBatch beforeOpen = batches.CurrentBatch;
        op.SetReuseCurrentBatch(false);
        await op.OpenAsync(Ct);
        Assert.NotSame(beforeOpen, batches.CurrentBatch);
        Assert.False(await op.MoveNextAsync(Ct));
        Assert.False(await batches.MoveNextBatchAsync(Ct));
        Assert.Equal(1, batches.CurrentBatch.ColumnCount);
        Assert.Equal(0, batches.CurrentBatch.Count);
        Assert.True(await source.MoveNextAsync(Ct));
        Assert.Equal(1, source.Current[0].AsInteger);
    }

    private static long[][] Snapshot(RowBatch batch)
        => Enumerable.Range(0, batch.Count)
            .Select(row => batch.GetRowSpan(row).ToArray().Select(cell => cell.AsInteger).ToArray()).ToArray();
}
