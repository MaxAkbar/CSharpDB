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
public sealed class PrimaryKeySparseProjectionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SparseIntegerProjection_ResetsMissingAndNullColumnsAcrossBatchesAndReopen(bool batches)
    {
        await using var fixture = await Fixture.CreateAsync(130, alternateShortRows: true);
        await using var op = fixture.Create([0, 3]);
        for (int traversal = 0; traversal < 2; traversal++)
        {
            DbValue[][] rows = await ReadAsync(op, batches);
            Assert.Equal(130, rows.Length);
            for (int i = 0; i < rows.Length; i++)
            {
                Assert.Equal(i, rows[i][0].AsInteger);
                if (i % 3 == 0)
                    Assert.Equal(i * 10L, rows[i][1].AsInteger);
                else
                    Assert.True(rows[i][1].IsNull);
            }
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SparseTextAndBlobProjection_RetainsOwnedOutputsAfterMovesAndDisposal(bool batches)
    {
        await using var fixture = await Fixture.CreateAsync(130);
        var op = fixture.Create([0, 4, 5]);
        var retainedRows = new List<DbValue[]>();
        var retainedBatches = new List<RowBatch>();
        await using (op)
        {
            op.SetReuseCurrentRowBuffer(false);
            op.SetReuseCurrentBatch(false);
            await op.OpenAsync(Ct);
            if (batches)
            {
                while (await op.MoveNextBatchAsync(Ct))
                    retainedBatches.Add(op.CurrentBatch);
                Assert.Equal(new[] { 64, 64, 2 }, retainedBatches.Select(batch => batch.Count));
            }
            else
            {
                while (await op.MoveNextAsync(Ct))
                    retainedRows.Add(op.Current);
            }
        }

        foreach (RowBatch batch in retainedBatches)
            for (int row = 0; row < batch.Count; row++)
                retainedRows.Add(batch.GetRowSpan(row).ToArray());

        Assert.Equal(130, retainedRows.Count);
        for (int i = 0; i < retainedRows.Count; i++)
        {
            Assert.Equal(i, retainedRows[i][0].AsInteger);
            Assert.Equal(Text(i), retainedRows[i][1].AsText);
            Assert.Equal(Blob(i), retainedRows[i][2].AsBlob);
            if (i > 0)
            {
                Assert.NotSame(retainedRows[i - 1], retainedRows[i]);
                Assert.NotSame(retainedRows[i - 1][2].AsBlob, retainedRows[i][2].AsBlob);
            }
        }
        retainedRows[0][2].AsBlob[0] = 255;
        Assert.Equal(Blob(1), retainedRows[1][2].AsBlob);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SparseProjection_UsesConfiguredSerializerAndPadsShortCustomRecords(bool batches)
    {
        await using var fixture = await Fixture.CreateAsync(65, alternateShortRows: true,
            serializer: new PrefixSerializer());
        await using var op = fixture.Create([1, 3, 5]);
        DbValue[][] rows = await ReadAsync(op, batches);
        Assert.Equal(65, rows.Length);
        for (int i = 0; i < rows.Length; i++)
        {
            Assert.Equal($"unused:{i}:padding", rows[i][0].AsText);
            if (i % 3 == 1)
            {
                Assert.True(rows[i][1].IsNull);
                Assert.True(rows[i][2].IsNull);
            }
            else
            {
                if (i % 3 == 2)
                    Assert.True(rows[i][1].IsNull);
                else
                    Assert.Equal(i * 10L, rows[i][1].AsInteger);
                Assert.Equal(Blob(i), rows[i][2].AsBlob);
            }
        }
    }

    private static string Text(int i) => $"retained:{i}:the selected text survives subsequent records";
    private static byte[] Blob(int i) => [(byte)i, (byte)(i >> 8), 17, 29, 43];

    private static async Task<DbValue[][]> ReadAsync(PrimaryKeyRangeScanOperator op, bool batches)
    {
        var rows = new List<DbValue[]>();
        await op.OpenAsync(Ct);
        if (batches)
        {
            while (await op.MoveNextBatchAsync(Ct))
                for (int i = 0; i < op.CurrentBatch.Count; i++)
                    rows.Add(op.CurrentBatch.GetRowSpan(i).ToArray());
        }
        else
        {
            while (await op.MoveNextAsync(Ct))
                rows.Add((DbValue[])op.Current.Clone());
        }
        return rows.ToArray();
    }

    private sealed class Fixture(Pager pager, BTree tree, TableSchema schema, IRecordSerializer serializer)
        : IAsyncDisposable
    {
        public static async Task<Fixture> CreateAsync(int count, bool alternateShortRows = false,
            IRecordSerializer? serializer = null)
        {
            serializer ??= new DefaultRecordSerializer();
            var walIndex = new WalIndex();
            var pager = await Pager.CreateAsync(new MemoryStorageDevice(), new MemoryWriteAheadLog(walIndex), walIndex,
                new PagerOptions { CheckpointPolicy = new FrameCountCheckpointPolicy(1_000_000) }, Ct);
            try
            {
                await pager.InitializeNewDatabaseAsync(Ct);
                await pager.RecoverAsync(Ct);
                await pager.BeginTransactionAsync(Ct);
                var tree = new BTree(pager, await BTree.CreateNewAsync(pager, Ct), "sparse_items");
                for (int i = 0; i < count; i++)
                {
                    DbValue[] row =
                    [
                        DbValue.FromInteger(i), DbValue.FromText($"unused:{i}:padding"),
                        DbValue.FromBlob(new byte[128]),
                        alternateShortRows && i % 3 == 2 ? DbValue.Null : DbValue.FromInteger(i * 10L),
                        DbValue.FromText(Text(i)), DbValue.FromBlob(Blob(i)),
                    ];
                    if (alternateShortRows && i % 3 == 1)
                        row = row[..3];
                    await tree.InsertAsync(i, serializer.Encode(row), Ct);
                }
                await pager.CommitAsync(Ct);
                return new Fixture(pager, tree, new TableSchema
                {
                    TableName = "sparse_items",
                    Columns =
                    [
                        new() { Name = "id", Type = DbType.Integer, IsPrimaryKey = true, Nullable = false },
                        new() { Name = "unused_text", Type = DbType.Text },
                        new() { Name = "unused_blob", Type = DbType.Blob },
                        new() { Name = "value", Type = DbType.Integer },
                        new() { Name = "text", Type = DbType.Text },
                        new() { Name = "blob", Type = DbType.Blob },
                    ],
                }, serializer);
            }
            catch
            {
                await pager.DisposeAsync();
                throw;
            }
        }

        public PrimaryKeyRangeScanOperator Create(int[] projection)
            => new(tree, schema, IndexScanRange.All, serializer, projection,
                projection.Select(column => schema.Columns[column]).ToArray());

        public ValueTask DisposeAsync() => pager.DisposeAsync();
    }

    private sealed class PrefixSerializer : IRecordSerializer
    {
        private readonly DefaultRecordSerializer _inner = new();

        public byte[] Encode(ReadOnlySpan<DbValue> values)
        {
            byte[] record = _inner.Encode(values);
            byte[] prefixed = new byte[record.Length + 1];
            prefixed[0] = 0xF7;
            record.CopyTo(prefixed.AsSpan(1));
            return prefixed;
        }

        private static ReadOnlySpan<byte> Payload(ReadOnlySpan<byte> buffer)
        {
            if (buffer.IsEmpty || buffer[0] != 0xF7)
                throw new InvalidDataException("Missing custom serializer prefix.");
            return buffer[1..];
        }

        public DbValue[] Decode(ReadOnlySpan<byte> buffer) => _inner.Decode(Payload(buffer));
        public int DecodeInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination)
            => _inner.DecodeInto(Payload(buffer), destination);
        public void DecodeSelectedInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination, ReadOnlySpan<int> columns)
            => _inner.DecodeSelectedInto(Payload(buffer), destination, columns);
        public void DecodeSelectedCompactInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination, ReadOnlySpan<int> columns)
            => _inner.DecodeSelectedCompactInto(Payload(buffer), destination, columns);
        public DbValue[] DecodeUpTo(ReadOnlySpan<byte> buffer, int maximum) => _inner.DecodeUpTo(Payload(buffer), maximum);
        public DbValue DecodeColumn(ReadOnlySpan<byte> buffer, int column) => _inner.DecodeColumn(Payload(buffer), column);
        public bool TryColumnTextEquals(ReadOnlySpan<byte> buffer, int column, ReadOnlySpan<byte> text, out bool equals)
            => _inner.TryColumnTextEquals(Payload(buffer), column, text, out equals);
        public bool IsColumnNull(ReadOnlySpan<byte> buffer, int column) => _inner.IsColumnNull(Payload(buffer), column);
        public bool TryDecodeNumericColumn(ReadOnlySpan<byte> buffer, int column, out long integer, out double real, out bool isReal)
            => _inner.TryDecodeNumericColumn(Payload(buffer), column, out integer, out real, out isReal);
    }
}
