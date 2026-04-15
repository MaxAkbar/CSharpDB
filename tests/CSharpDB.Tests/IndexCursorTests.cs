using CSharpDB.Storage.Indexing;

namespace CSharpDB.Tests;

public sealed class IndexCursorTests
{
    [Fact]
    public void UpperBoundIndexCursor_RejectsNullInnerCursor()
    {
        Assert.Throws<ArgumentNullException>(() => new UpperBoundIndexCursor(null!, upperBoundInclusive: 1));
    }

    [Fact]
    public async Task UpperBoundIndexCursor_YieldsRowsUpToInclusiveUpperBound()
    {
        var ct = TestContext.Current.CancellationToken;
        var inner = new FakeIndexCursor(
            new KeyValuePair<long, byte[]>(1, new byte[] { 1 }),
            new KeyValuePair<long, byte[]>(2, new byte[] { 2 }),
            new KeyValuePair<long, byte[]>(3, new byte[] { 3 }),
            new KeyValuePair<long, byte[]>(4, new byte[] { 4 }));
        var cursor = new UpperBoundIndexCursor(inner, upperBoundInclusive: 3);
        var keys = new List<long>();

        while (await cursor.MoveNextAsync(ct))
            keys.Add(cursor.CurrentKey);

        Assert.Equal(new long[] { 1, 2, 3 }, keys);
    }

    [Fact]
    public async Task UpperBoundIndexCursor_AfterEof_DoesNotAdvanceInnerCursorAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        var inner = new FakeIndexCursor(
            new KeyValuePair<long, byte[]>(1, new byte[] { 1 }),
            new KeyValuePair<long, byte[]>(2, new byte[] { 2 }));
        var cursor = new UpperBoundIndexCursor(inner, upperBoundInclusive: 1);

        Assert.True(await cursor.MoveNextAsync(ct));
        Assert.False(await cursor.MoveNextAsync(ct));
        int callsAfterEof = inner.MoveNextCallCount;

        Assert.False(await cursor.MoveNextAsync(ct));
        Assert.Equal(callsAfterEof, inner.MoveNextCallCount);
    }

    [Fact]
    public async Task EmptyIndexCursor_NeverYieldsRows()
    {
        var ct = TestContext.Current.CancellationToken;
        var cursor = EmptyIndexCursor.Instance;

        Assert.False(await cursor.MoveNextAsync(ct));
        Assert.Equal(default, cursor.CurrentKey);
        Assert.Equal(ReadOnlyMemory<byte>.Empty, cursor.CurrentValue);
    }

    private sealed class FakeIndexCursor : IIndexCursor
    {
        private readonly IReadOnlyList<KeyValuePair<long, byte[]>> _items;
        private int _index = -1;

        public FakeIndexCursor(params KeyValuePair<long, byte[]>[] items)
        {
            _items = items;
        }

        public int MoveNextCallCount { get; private set; }

        public long CurrentKey { get; private set; }

        public ReadOnlyMemory<byte> CurrentValue { get; private set; } = ReadOnlyMemory<byte>.Empty;

        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
        {
            MoveNextCallCount++;
            int next = _index + 1;
            if (next >= _items.Count)
                return ValueTask.FromResult(false);

            _index = next;
            var item = _items[_index];
            CurrentKey = item.Key;
            CurrentValue = item.Value;
            return ValueTask.FromResult(true);
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
