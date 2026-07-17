using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class CompoundQueryOperatorTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task OuterLimit_DoesNotOpenRightUnionAllBranch()
    {
        ColumnDefinition[] schema =
        [
            new() { Name = "value", Type = DbType.Integer, Nullable = false },
        ];
        var leftSource = new TrackingOperator(
            schema,
            [
                [DbValue.FromInteger(1)],
                [DbValue.FromInteger(2)],
            ]);
        var rightSource = new TrackingOperator(
            schema,
            [
                [DbValue.FromInteger(3)],
            ]);
        var append = new ConcatenateOperator(
            new QueryResult(leftSource),
            new QueryResult(rightSource),
            schema);
        var result = new QueryResult(new LimitOperator(append, 1));

        var rows = await result.ToListAsync(Ct);

        Assert.Single(rows);
        Assert.Equal(1, rows[0][0].AsInteger);
        Assert.Equal(1, leftSource.OpenCount);
        Assert.Equal(0, rightSource.OpenCount);

        await result.DisposeAsync();
        Assert.Equal(1, leftSource.DisposeCount);
        Assert.Equal(1, rightSource.DisposeCount);
    }

    [Fact]
    public async Task Cancellation_DisposesBothUnionAllBranches()
    {
        ColumnDefinition[] schema =
        [
            new() { Name = "value", Type = DbType.Integer, Nullable = false },
        ];
        var leftSource = new TrackingOperator(schema, [[DbValue.FromInteger(1)]]);
        var rightSource = new TrackingOperator(schema, [[DbValue.FromInteger(2)]]);
        var append = new ConcatenateOperator(
            new QueryResult(leftSource),
            new QueryResult(rightSource),
            schema);
        await append.OpenAsync(Ct);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => append.MoveNextAsync(cts.Token).AsTask());

        Assert.Equal(1, leftSource.DisposeCount);
        Assert.Equal(1, rightSource.DisposeCount);
        Assert.Equal(0, rightSource.OpenCount);
    }

    private sealed class TrackingOperator(
        ColumnDefinition[] schema,
        IReadOnlyList<DbValue[]> rows) : IOperator
    {
        private int _index = -1;

        public int OpenCount { get; private set; }
        public int DisposeCount { get; private set; }
        public ColumnDefinition[] OutputSchema { get; } = schema;
        public bool ReusesCurrentRowBuffer => false;
        public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

        public ValueTask OpenAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            OpenCount++;
            _index = -1;
            Current = Array.Empty<DbValue>();
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            _index++;
            if (_index >= rows.Count)
            {
                Current = Array.Empty<DbValue>();
                return ValueTask.FromResult(false);
            }

            Current = rows[_index];
            return ValueTask.FromResult(true);
        }

        public ValueTask DisposeAsync()
        {
            DisposeCount++;
            Current = Array.Empty<DbValue>();
            return ValueTask.CompletedTask;
        }
    }
}
