using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Indexing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Serialization;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

[Collection("StorageConcurrency")]
public sealed class PrimaryKeyRangeScanOperatorTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task InclusiveAndExclusiveBounds_AgreeInRowAndBatchModes(bool lowerInclusive, bool upperInclusive)
    {
        long[] keys = [long.MinValue, -100, -3, -1, 0, 2, 5, 100, long.MaxValue];
        await using var fixture = await Fixture.CreateAsync(keys);
        var range = new IndexScanRange(-3, lowerInclusive, 5, upperInclusive);
        long[] expected = keys.Where(key => (lowerInclusive ? key >= -3 : key > -3) &&
            (upperInclusive ? key <= 5 : key < 5)).ToArray();
        await using var op = fixture.Create(range, [0]);
        Assert.Equal(expected, (await ReadRowsAsync(op)).Select(row => row[0].AsInteger));
        Assert.Equal(expected, (await ReadBatchesAsync(op)).Select(row => row[0].AsInteger));
    }

    [Fact]
    public async Task UnboundedMissingAndExtremeBounds_DoNotOverflowOrRestartAfterExhaustion()
    {
        long[] keys = [long.MinValue, -5, 0, 5, long.MaxValue];
        await using var fixture = await Fixture.CreateAsync(keys);
        IndexScanRange[] ranges =
        [
            IndexScanRange.All,
            new(long.MinValue, true, long.MaxValue, true),
            new(long.MinValue, false, long.MaxValue, false),
            new(long.MaxValue, false, null, true),
            new(null, true, long.MinValue, false),
            new(null, true, 0, true),
            new(0, false, null, true),
            new(1, true, 6, false),
            new(5, false, 5, true),
            new(5, true, -5, true),
        ];
        foreach (IndexScanRange range in ranges)
        {
            long[] expected = keys.Where(key =>
                (!range.LowerBound.HasValue || (range.LowerInclusive ? key >= range.LowerBound.Value : key > range.LowerBound.Value)) &&
                (!range.UpperBound.HasValue || (range.UpperInclusive ? key <= range.UpperBound.Value : key < range.UpperBound.Value))).ToArray();
            await using var op = fixture.Create(range, [0]);
            Assert.Equal(expected, (await ReadRowsAsync(op)).Select(row => row[0].AsInteger));
            Assert.False(await op.MoveNextAsync(Ct));
            Assert.False(await op.MoveNextAsync(Ct));
            Assert.Equal(expected, (await ReadBatchesAsync(op)).Select(row => row[0].AsInteger));
            Assert.False(await op.MoveNextBatchAsync(Ct));
        }
    }

    [Fact]
    public async Task SeekAcrossLeafPages_DecodesOnlyRowsWithinBounds()
    {
        long[] keys = Enumerable.Range(-96, 193).Select(i => 3L * i).ToArray();
        await using var fixture = await Fixture.CreateAsync(keys, paddingBytes: 1024);
        var rootPage = await fixture.Pager.GetPageAsync(fixture.Tree.RootPageId, Ct);
        Assert.Equal(PageConstants.PageTypeInterior, new SlottedPage(rootPage, fixture.Tree.RootPageId).PageType);
        var serializer = new TrackingSerializer();
        await using var op = fixture.Create(new IndexScanRange(5, true, 251, false), null, serializer);
        long[] expected = keys.Where(key => key >= 5 && key < 251).ToArray();

        DbValue[][] rows = await ReadRowsAsync(op);
        Assert.Equal(expected, rows.Select(row => row[0].AsInteger));
        Assert.All(rows, row => Assert.StartsWith($"{row[0].AsInteger}:", row[2].AsText));
        Assert.Equal(expected.Length, serializer.DecodeCalls);
        DbValue[][] batches = await ReadBatchesAsync(op);
        Assert.Equal(expected, batches.Select(row => row[0].AsInteger));
        Assert.Equal(2 * expected.Length, serializer.DecodeCalls);
    }

    [Fact]
    public async Task ProjectionAndStar_PadOlderRowsAndPreserveRepeatedOrdinals()
    {
        await using var fixture = await Fixture.CreateAsync([1, 2, 3], alternateShortRows: true);
        await using (var star = fixture.Create(IndexScanRange.All, null))
        {
            DbValue[][] rows = await ReadRowsAsync(star);
            Assert.Equal("1:", rows[0][2].AsText);
            Assert.True(rows[1][2].IsNull);
            Assert.Equal("3:", rows[2][2].AsText);
        }

        await using (var projected = fixture.Create(IndexScanRange.All, [2, 0, 2]))
        {
            DbValue[][] rows = await ReadRowsAsync(projected);
            Assert.Equal(new long[] { 1, 2, 3 }, rows.Select(row => row[1].AsInteger));
            Assert.Equal("1:", rows[0][0].AsText);
            Assert.Equal(rows[0][0], rows[0][2]);
            Assert.True(rows[1][0].IsNull);
            Assert.True(rows[1][2].IsNull);
            Assert.Equal("3:", rows[2][0].AsText);
            DbValue[][] batches = await ReadBatchesAsync(projected);
            for (int i = 0; i < rows.Length; i++) Assert.Equal(rows[i], batches[i]);
        }

        var serializer = new TrackingSerializer();
        await using var prefix = fixture.Create(IndexScanRange.All, [1], serializer);
        Assert.Equal(new long[] { 10, 20, 30 }, (await ReadRowsAsync(prefix)).Select(row => row[0].AsInteger));
        Assert.InRange(serializer.MaxDestinationColumns, 1, 2);
    }

    [Fact]
    public async Task PrimaryKeyOnlyProjection_UsesCursorKeysWithoutRecordDecoding()
    {
        await using var fixture = await Fixture.CreateAsync([long.MinValue, 0, long.MaxValue]);
        var serializer = new TrackingSerializer { RejectDecoding = true };
        await using var op = fixture.Create(IndexScanRange.All, [0, 0], serializer);
        DbValue[][] rows = await ReadRowsAsync(op);
        Assert.Equal(new long[] { long.MinValue, 0, long.MaxValue }, rows.Select(row => row[0].AsInteger));
        Assert.All(rows, row => Assert.Equal(row[0], row[1]));
        Assert.Equal(3, (await ReadBatchesAsync(op)).Length);
        Assert.Equal(0, serializer.DecodeCalls);
    }

    [Fact]
    public async Task ReopeningAndChangingReuseModes_PreservePreviouslyOwnedRowsAndBatches()
    {
        long[] keys = Enumerable.Range(0, 140).Select(i => (long)i).ToArray();
        await using var fixture = await Fixture.CreateAsync(keys);
        await using var op = fixture.Create(IndexScanRange.All, [0]);
        Assert.Equal(0, op.CurrentBatch.Capacity);
        op.SetReuseCurrentRowBuffer(false);
        await op.OpenAsync(Ct);
        Assert.Equal(0, op.CurrentBatch.Capacity);
        Assert.True(await op.MoveNextAsync(Ct));
        DbValue[] firstRow = op.Current;
        Assert.True(await op.MoveNextAsync(Ct));
        Assert.NotSame(firstRow, op.Current);
        op.SetReuseCurrentRowBuffer(true);
        Assert.True(await op.MoveNextAsync(Ct));
        Assert.Equal(0L, firstRow[0].AsInteger);

        op.SetReuseCurrentBatch(false);
        await op.OpenAsync(Ct);
        Assert.True(await op.MoveNextBatchAsync(Ct));
        RowBatch firstBatch = op.CurrentBatch;
        Assert.True(await op.MoveNextBatchAsync(Ct));
        RowBatch secondBatch = op.CurrentBatch;
        Assert.NotSame(firstBatch, secondBatch);
        op.SetReuseCurrentBatch(true);
        Assert.True(await op.MoveNextBatchAsync(Ct));
        Assert.NotSame(secondBatch, op.CurrentBatch);
        Assert.Equal(12, op.CurrentBatch.Count);
        Assert.False(await op.MoveNextBatchAsync(Ct));
        Assert.Equal(64, firstBatch.Count);
        Assert.Equal(0L, firstBatch.GetRowSpan(0)[0].AsInteger);
        Assert.Equal(63L, firstBatch.GetRowSpan(63)[0].AsInteger);
        Assert.Equal(64, secondBatch.Count);
        Assert.Equal(64L, secondBatch.GetRowSpan(0)[0].AsInteger);
        Assert.Equal(127L, secondBatch.GetRowSpan(63)[0].AsInteger);

        await op.DisposeAsync();
        Assert.Equal(keys, (await ReadRowsAsync(op)).Select(row => row[0].AsInteger));
        Assert.Equal(keys, (await ReadBatchesAsync(op)).Select(row => row[0].AsInteger));
    }

    private static async Task<DbValue[][]> ReadRowsAsync(PrimaryKeyRangeScanOperator op)
    {
        var rows = new List<DbValue[]>();
        await op.OpenAsync(Ct);
        while (await op.MoveNextAsync(Ct)) rows.Add((DbValue[])op.Current.Clone());
        return rows.ToArray();
    }

    private static async Task<DbValue[][]> ReadBatchesAsync(PrimaryKeyRangeScanOperator op)
    {
        var rows = new List<DbValue[]>();
        await op.OpenAsync(Ct);
        while (await op.MoveNextBatchAsync(Ct))
            for (int i = 0; i < op.CurrentBatch.Count; i++) rows.Add(op.CurrentBatch.GetRowSpan(i).ToArray());
        return rows.ToArray();
    }

    private sealed class Fixture(Pager pager, BTree tree, TableSchema schema) : IAsyncDisposable
    {
        public Pager Pager { get; } = pager;
        public BTree Tree { get; } = tree;

        public static async Task<Fixture> CreateAsync(long[] keys, int paddingBytes = 0, bool alternateShortRows = false)
        {
            var walIndex = new WalIndex();
            var pager = await Pager.CreateAsync(new MemoryStorageDevice(), new MemoryWriteAheadLog(walIndex), walIndex,
                new PagerOptions { CheckpointPolicy = new FrameCountCheckpointPolicy(1_000_000), MaxCachedPages = 32 }, Ct);
            try
            {
                await pager.InitializeNewDatabaseAsync(Ct);
                await pager.RecoverAsync(Ct);
                await pager.BeginTransactionAsync(Ct);
                var tree = new BTree(pager, await BTree.CreateNewAsync(pager, Ct), "range_items");
                var serializer = new DefaultRecordSerializer();
                for (int i = 0; i < keys.Length; i++)
                {
                    DbValue[] row = alternateShortRows && i % 2 == 1
                        ? [DbValue.FromInteger(keys[i]), DbValue.FromInteger((i + 1) * 10)]
                        : [DbValue.FromInteger(keys[i]), DbValue.FromInteger((i + 1) * 10),
                            DbValue.FromText($"{keys[i]}:{new string('x', paddingBytes)}")];
                    await tree.InsertAsync(keys[i], serializer.Encode(row), Ct);
                }
                await pager.CommitAsync(Ct);
                return new Fixture(pager, tree, new TableSchema
                {
                    TableName = "range_items",
                    Columns =
                    [
                        new ColumnDefinition { Name = "id", Type = DbType.Integer, IsPrimaryKey = true, Nullable = false },
                        new ColumnDefinition { Name = "value", Type = DbType.Integer },
                        new ColumnDefinition { Name = "payload", Type = DbType.Text },
                    ],
                });
            }
            catch
            {
                await pager.DisposeAsync();
                throw;
            }
        }

        public PrimaryKeyRangeScanOperator Create(IndexScanRange range, int[]? projection, IRecordSerializer? serializer = null)
            => new(Tree, schema, range, serializer ?? new DefaultRecordSerializer(), projection,
                projection is null ? schema.Columns.ToArray() : projection.Select(column => schema.Columns[column]).ToArray());

        public ValueTask DisposeAsync() => Pager.DisposeAsync();
    }

    private sealed class TrackingSerializer : IRecordSerializer
    {
        private readonly DefaultRecordSerializer _inner = new();
        public bool RejectDecoding { get; init; }
        public int DecodeCalls { get; private set; }
        public int MaxDestinationColumns { get; private set; }
        public byte[] Encode(ReadOnlySpan<DbValue> values) => _inner.Encode(values);

        private void RecordDecode()
        {
            if (RejectDecoding) throw new InvalidOperationException("Key-only projection must not decode records.");
            DecodeCalls++;
        }

        public DbValue[] Decode(ReadOnlySpan<byte> buffer) { RecordDecode(); return _inner.Decode(buffer); }
        public int DecodeInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination)
        {
            RecordDecode();
            MaxDestinationColumns = Math.Max(MaxDestinationColumns, destination.Length);
            return _inner.DecodeInto(buffer, destination);
        }
        public void DecodeSelectedInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination, ReadOnlySpan<int> columns)
        { RecordDecode(); _inner.DecodeSelectedInto(buffer, destination, columns); }
        public void DecodeSelectedCompactInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination, ReadOnlySpan<int> columns)
        {
            RecordDecode();
            MaxDestinationColumns = Math.Max(MaxDestinationColumns, destination.Length);
            _inner.DecodeSelectedCompactInto(buffer, destination, columns);
        }
        public DbValue[] DecodeUpTo(ReadOnlySpan<byte> buffer, int maxColumnIndexInclusive)
        { RecordDecode(); return _inner.DecodeUpTo(buffer, maxColumnIndexInclusive); }
        public DbValue DecodeColumn(ReadOnlySpan<byte> buffer, int columnIndex)
        { RecordDecode(); return _inner.DecodeColumn(buffer, columnIndex); }
        public bool TryColumnTextEquals(ReadOnlySpan<byte> buffer, int columnIndex, ReadOnlySpan<byte> expectedUtf8, out bool equals)
        { RecordDecode(); return _inner.TryColumnTextEquals(buffer, columnIndex, expectedUtf8, out equals); }
        public bool IsColumnNull(ReadOnlySpan<byte> buffer, int columnIndex)
        { RecordDecode(); return _inner.IsColumnNull(buffer, columnIndex); }
        public bool TryDecodeNumericColumn(ReadOnlySpan<byte> buffer, int columnIndex, out long intValue, out double realValue, out bool isReal)
        { RecordDecode(); return _inner.TryDecodeNumericColumn(buffer, columnIndex, out intValue, out realValue, out isReal); }
    }
}
