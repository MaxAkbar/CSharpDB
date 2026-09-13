using BenchmarkDotNet.Attributes;
using CSharpDB.Primitives;
using CSharpDB.Storage.Caching;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Serialization;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
public class RecordEncodingBenchmarks
{
    [Params(4, 32)]
    public int ColumnCount { get; set; }

    [Params(16, 256)]
    public int TextLength { get; set; }

    private DbValue[] _values = null!;

    [GlobalSetup]
    public void Setup()
    {
        _values = Enumerable.Range(0, ColumnCount)
            .Select(i => i % 2 == 0 ? DbValue.FromInteger(i) : DbValue.FromText(new string('x', TextLength)))
            .ToArray();
    }

    [Benchmark]
    public byte[] Encode() => RecordEncoder.Encode(_values);
}

[MemoryDiagnoser]
public class PageCacheReplacementBenchmarks
{
    private readonly LruPageCache _cache = new(capacity: 16);
    private readonly byte[] _page = new byte[PageConstants.PageSize];

    [GlobalSetup]
    public void Setup()
    {
        for (uint pageId = 0; pageId < 16; pageId++)
            _cache.Set(pageId, _page);
    }

    [Benchmark(OperationsPerInvoke = 16)]
    public void UpdateExistingPages()
    {
        for (uint pageId = 0; pageId < 16; pageId++)
            _cache.Set(pageId, _page);
    }
}

[MemoryDiagnoser]
public class SlottedPageCompactionBenchmarks
{
    [Params(16, 128, 256)]
    public int CellCount { get; set; }

    [Params("ascending", "random", "descending")]
    public string InsertionOrder { get; set; } = "ascending";

    private SlottedPage _page;

    [GlobalSetup]
    public void Setup()
    {
        _page = new SlottedPage(new byte[PageConstants.PageSize], 1);
        _page.Initialize(PageConstants.PageTypeLeaf);
        var random = new Random(1729);
        byte[] cell = new byte[12];
        Varint.Write(cell, 11UL);
        for (int i = 0; i < CellCount; i++)
        {
            int index = InsertionOrder == "ascending" ? i : InsertionOrder == "descending" ? 0 : random.Next(i + 1);
            if (!_page.InsertCell(index, cell))
                throw new InvalidOperationException("Benchmark cells do not fit in one page.");
        }
    }

    // Compaction preserves the logical-to-physical ordering, so repeated calls
    // retain the selected sorting workload without allocating a page per call.
    [Benchmark]
    public void Defragment() => _page.Defragment();
}

[MemoryDiagnoser]
[InvocationCount(64, unrollFactor: 1)]
public class MemoryWalCommitBenchmarks
{
    [Params(1, 100)]
    public int FrameCount { get; set; }

    private MemoryWriteAheadLog _wal = null!;
    private WalFrameWrite[] _frames = null!;

    [GlobalSetup]
    public void Setup()
    {
        byte[] page = new byte[PageConstants.PageSize];
        page.AsSpan().Fill(0x5A);
        _frames = Enumerable.Range(1, FrameCount).Select(i => new WalFrameWrite((uint)i, page)).ToArray();
    }

    [IterationSetup]
    public void CreateWal()
    {
        _wal = new MemoryWriteAheadLog(new WalIndex());
        _wal.OpenAsync((uint)FrameCount + 1).GetAwaiter().GetResult();

        // Reserve enough WAL capacity for the measured invocations. A checkpoint
        // resets its logical length while retaining MemoryStorageDevice capacity,
        // keeping buffer growth and large-array GC out of the publication probe.
        for (int i = 0; i < 64; i++)
            CommitBatch().GetAwaiter().GetResult();
        using var device = new MemoryStorageDevice();
        _wal.CheckpointAsync(device, (uint)FrameCount + 1).GetAwaiter().GetResult();
    }

    [IterationCleanup]
    public void DisposeWal() => _wal.DisposeAsync().GetAwaiter().GetResult();

    [Benchmark]
    public async Task CommitBatch()
    {
        _wal.BeginTransaction();
        await (await _wal.AppendFramesAndCommitAsync(_frames, (uint)FrameCount + 1)).WaitAsync();
    }
}
