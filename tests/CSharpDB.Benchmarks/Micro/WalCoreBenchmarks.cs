using BenchmarkDotNet.Attributes;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Storage-only WAL benchmarks that bypass SQL parse/plan/operator overhead.
/// Measures append/commit/checkpoint costs directly at the WAL layer.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class WalCoreBenchmarks
{
    private const uint SinglePageRange = 1024;
    private const uint BatchPageRange = 4096;

    [Params(100, 500, 1000)]
    public int WalFramesBeforeCheckpoint { get; set; }

    private string _dbPath = null!;
    private FileStorageDevice _device = null!;
    private WriteAheadLog _wal = null!;
    private byte[] _pageBuffer = null!;
    private uint _singlePageCursor;
    private uint _batchPageCursor;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_core_bench_{Guid.NewGuid():N}.db");
        _device = new FileStorageDevice(_dbPath);
        _wal = new WriteAheadLog(_dbPath, new WalIndex());
        _pageBuffer = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        _singlePageCursor = 1;
        _batchPageCursor = 1;

        // Use deterministic page contents for repeatability.
        _pageBuffer.AsSpan().Fill(0x5A);

        await _wal.OpenAsync(currentDbPageCount: 1);
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        if (_wal != null)
            await _wal.CloseAndDeleteAsync();
        if (_device != null)
            await _device.DisposeAsync();

        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
        try { if (File.Exists(_dbPath + ".wal")) File.Delete(_dbPath + ".wal"); } catch { }
    }

    [Benchmark(Description = "WAL core: single-frame commit")]
    public async Task CommitSingleFrame()
    {
        _wal.BeginTransaction();
        await _wal.AppendFrameAsync(NextSinglePageId(), _pageBuffer);
        await _wal.CommitAsync(SinglePageRange + 1);
    }

    [Benchmark(Description = "WAL core: 100-frame batch commit")]
    public async Task CommitBatch_100Frames()
    {
        _wal.BeginTransaction();
        for (int i = 0; i < 100; i++)
        {
            await _wal.AppendFrameAsync(NextBatchPageId(), _pageBuffer);
        }

        await _wal.CommitAsync(BatchPageRange + 1);
    }

    [Benchmark(Description = "WAL core: manual checkpoint after N frames")]
    public async Task ManualCheckpoint()
    {
        _wal.BeginTransaction();
        for (uint pageId = 1; pageId <= (uint)WalFramesBeforeCheckpoint; pageId++)
        {
            await _wal.AppendFrameAsync(pageId, _pageBuffer);
        }

        uint pageCount = (uint)WalFramesBeforeCheckpoint + 1;
        await _wal.CommitAsync(pageCount);
        await _wal.CheckpointAsync(_device, pageCount);
    }

    private uint NextSinglePageId()
    {
        uint pageId = _singlePageCursor;
        _singlePageCursor = _singlePageCursor == SinglePageRange ? 1 : _singlePageCursor + 1;
        return pageId;
    }

    private uint NextBatchPageId()
    {
        uint pageId = _batchPageCursor;
        _batchPageCursor = _batchPageCursor == BatchPageRange ? 1 : _batchPageCursor + 1;
        return pageId;
    }
}
