using System.Buffers.Binary;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Indexing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

[Collection("StorageConcurrency")]
public sealed class AppendOnlyRowIdChainStoreTests
{
    [Fact]
    public async Task WriteAppendableAsync_SortedSequentialRowIds_UsesDeltaVarintEncoding()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var pager = await CreatePagerAsync(ct);
        await pager.InitializeNewDatabaseAsync(ct);
        await pager.RecoverAsync(ct);

        byte[] rowIdPayload = CreateRowIdPayload(Enumerable.Range(1, 2000).Select(static value => (long)value));

        (uint firstPageId, AppendOnlyRowIdChainStore.AppendableChainMetadata metadata) =
            await AppendOnlyRowIdChainStore.WriteAppendableAsync(
                pager,
                rowIdPayload,
                isSortedAscending: true,
                lastRowId: 2000,
                ct);

        Assert.Equal(AppendableChainEncoding.DeltaVarint, metadata.Encoding);
        Assert.Equal(1, await CountChainPagesAsync(pager, firstPageId, ct));

        byte[] decodedPayload = await AppendOnlyRowIdChainStore.ReadAsync(pager, firstPageId, metadata, ct);
        Assert.Equal(rowIdPayload, decodedPayload);

        AppendOnlyRowIdChainStore.AppendableChainMetadata rereadMetadata =
            await AppendOnlyRowIdChainStore.ReadAppendableMetadataAsync(pager, firstPageId, ct);
        Assert.Equal(AppendableChainEncoding.DeltaVarint, rereadMetadata.Encoding);
        Assert.Equal(metadata.RowCount, rereadMetadata.RowCount);
        Assert.Equal(metadata.LastRowId, rereadMetadata.LastRowId);
    }

    [Fact]
    public async Task AppendBatchAsync_DeltaVarintChain_RoundTripsAndStaysSinglePage_ForDenseTail()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var pager = await CreatePagerAsync(ct);
        await pager.InitializeNewDatabaseAsync(ct);
        await pager.RecoverAsync(ct);

        byte[] initialPayload = CreateRowIdPayload(Enumerable.Range(1, 1500).Select(static value => (long)value));
        (uint firstPageId, AppendOnlyRowIdChainStore.AppendableChainMetadata metadata) =
            await AppendOnlyRowIdChainStore.WriteAppendableAsync(
                pager,
                initialPayload,
                isSortedAscending: true,
                lastRowId: 1500,
                ct);

        long[] appendedRowIds = Enumerable.Range(1501, 1000).Select(static value => (long)value).ToArray();
        AppendOnlyRowIdChainStore.AppendableChainMetadata updated =
            await AppendOnlyRowIdChainStore.AppendBatchAsync(
                pager,
                firstPageId,
                metadata,
                appendedRowIds,
                isSortedAscending: true,
                ct);

        Assert.Equal(AppendableChainEncoding.DeltaVarint, updated.Encoding);
        Assert.Equal(2500, updated.RowCount);
        Assert.Equal(2500, updated.LastRowId);
        Assert.Equal(1, await CountChainPagesAsync(pager, firstPageId, ct));

        byte[] decodedPayload = await AppendOnlyRowIdChainStore.ReadAsync(pager, firstPageId, updated, ct);
        byte[] expectedPayload = CreateRowIdPayload(Enumerable.Range(1, 2500).Select(static value => (long)value));
        Assert.Equal(expectedPayload, decodedPayload);
    }

    private static async ValueTask<Pager> CreatePagerAsync(CancellationToken ct)
    {
        var device = new MemoryStorageDevice();
        var walIndex = new WalIndex();
        var wal = new MemoryWriteAheadLog(walIndex);
        return await Pager.CreateAsync(
            device,
            wal,
            walIndex,
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(1_000_000),
                MaxCachedPages = 32,
            },
            ct);
    }

    private static byte[] CreateRowIdPayload(IEnumerable<long> rowIds)
    {
        long[] values = rowIds.ToArray();
        byte[] payload = GC.AllocateUninitializedArray<byte>(checked(values.Length * RowIdPayloadCodec.RowIdSize));
        for (int i = 0; i < values.Length; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(
                payload.AsSpan(i * RowIdPayloadCodec.RowIdSize, RowIdPayloadCodec.RowIdSize),
                values[i]);
        }

        return payload;
    }

    private static async ValueTask<int> CountChainPagesAsync(Pager pager, uint firstPageId, CancellationToken ct)
    {
        int count = 0;
        uint pageId = firstPageId;
        while (pageId != PageConstants.NullPageId)
        {
            count++;
            byte[] page = await pager.GetPageAsync(pageId, ct);
            pageId = BinaryPrimitives.ReadUInt32LittleEndian(
                page.AsSpan(PageConstants.OverflowNextOffset, sizeof(uint)));
        }

        return count;
    }
}
