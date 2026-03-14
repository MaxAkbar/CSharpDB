using BenchmarkDotNet.Attributes;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures raw forward cursor traversal over a B+tree so sequential leaf access
/// improvements can be evaluated without the SQL executor on top.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class BTreeCursorBenchmarks
{
    [Params(10_000, 100_000)]
    public int RowCount { get; set; }

    [Params(false, true)]
    public bool EnableSequentialLeafReadAhead { get; set; }

    private Pager _pager = null!;
    private BTree _tree = null!;
    private string _dbPath = null!;
    private uint _rootPageId;
    private long _lastObservedValue;

    [GlobalSetup]
    public void GlobalSetup()
    {
        GlobalSetupAsync().GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _pager.DisposeAsync().AsTask().GetAwaiter().GetResult();
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
        try { if (File.Exists(_dbPath + ".wal")) File.Delete(_dbPath + ".wal"); } catch { }
    }

    [Benchmark(Description = "BTree cursor full scan")]
    public async Task CursorFullScan()
    {
        var cursor = _tree.CreateCursor();
        long sum = 0;
        int count = 0;

        while (await cursor.MoveNextAsync())
        {
            sum += cursor.CurrentKey;
            sum += cursor.CurrentValue.Span[0];
            count++;
        }

        _lastObservedValue = sum + count;
    }

    [Benchmark(Description = "BTree cursor seek + 1024-row window")]
    public async Task CursorSeekWindow()
    {
        var cursor = _tree.CreateCursor();
        int windowCount = Math.Min(1024, RowCount);
        long startKey = Math.Max(0, (RowCount / 2) - (windowCount / 2));

        long sum = 0;
        int count = 0;

        if (await cursor.SeekAsync(startKey))
        {
            do
            {
                sum += cursor.CurrentKey;
                sum += cursor.CurrentValue.Span[0];
                count++;
            }
            while (count < windowCount && await cursor.MoveNextAsync());
        }

        _lastObservedValue = sum + count;
    }

    private async Task GlobalSetupAsync()
    {
        _dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_btree_cursor_bench_{Guid.NewGuid():N}_{RowCount}_{EnableSequentialLeafReadAhead}.db");

        await using (var buildPager = await OpenPagerAsync(createNew: true))
        {
            await buildPager.InitializeNewDatabaseAsync();
            await buildPager.RecoverAsync();
            await buildPager.BeginTransactionAsync();

            _rootPageId = await BTree.CreateNewAsync(buildPager);
            var buildTree = new BTree(buildPager, _rootPageId);

            byte[] payload = new byte[128];
            for (int key = 0; key < RowCount; key++)
            {
                payload[0] = (byte)(key & 0xFF);
                await buildTree.InsertAsync(key, payload);
            }

            await buildPager.CommitAsync();
            await buildPager.CheckpointAsync();
        }

        _pager = await OpenPagerAsync(createNew: false);
        await _pager.RecoverAsync();
        _tree = new BTree(_pager, _rootPageId);
    }

    private async ValueTask<Pager> OpenPagerAsync(bool createNew)
    {
        var device = new FileStorageDevice(_dbPath, createNew);
        var walIndex = new WalIndex();
        var wal = new WriteAheadLog(_dbPath, walIndex);
        return await Pager.CreateAsync(
            device,
            wal,
            walIndex,
            new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(1_000_000),
                MaxCachedPages = 16,
                UseMemoryMappedReads = false,
                EnableSequentialLeafReadAhead = EnableSequentialLeafReadAhead,
            });
    }
}
