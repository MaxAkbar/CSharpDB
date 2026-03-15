using BenchmarkDotNet.Attributes;
using CSharpDB.Engine;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class CollectionIndexBenchmarks
{
    private const int SeedCount = 10_000;
    private const int UpdateWorkingSetSize = 1_024;

    private static readonly string[] s_categories = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon"];

    private Database _lookupDb = null!;
    private Database _writeDb = null!;
    private Collection<BenchDoc> _lookupCollection = null!;
    private Collection<NestedBenchDoc> _nestedLookupCollection = null!;
    private Collection<BenchDoc> _writeCollection = null!;
    private Random _lookupRandom = null!;
    private int _nextInsertId;
    private int _nextUpdateSlot = -1;
    private int _nextDeleteSlot = -1;
    private long _sink;

    private sealed record BenchDoc(string Name, int Value, string Category, string Tag);
    private sealed record BenchAddress(string City, int ZipCode);
    private sealed record NestedBenchDoc(string Name, BenchAddress Address, int Value);

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => GlobalCleanupAsync().GetAwaiter().GetResult();

    [Benchmark(Baseline = true, Description = "Collection FindByIndex int equality (1 match)")]
    public async Task FindByIndex_Integer()
    {
        int id = _lookupRandom.Next(0, SeedCount);
        await foreach (var match in _lookupCollection.FindByIndexAsync(d => d.Value, id))
            _sink ^= match.Value.Value;
    }

    [Benchmark(Description = "Collection FindByIndex text equality (1 match)")]
    public async Task FindByIndex_Text()
    {
        int id = _lookupRandom.Next(0, SeedCount);
        string tag = $"tag:{id}";
        await foreach (var match in _lookupCollection.FindByIndexAsync(d => d.Tag, tag))
            _sink ^= match.Value.Value;
    }

    [Benchmark(Description = "Collection FindByIndex nested path equality (string path, many matches)")]
    public async Task FindByIndex_NestedPath_StringPath()
    {
        int id = _lookupRandom.Next(0, SeedCount);
        string city = s_categories[id % s_categories.Length];
        await foreach (var match in _nestedLookupCollection.FindByIndexAsync("$.address.city", city))
            _sink ^= match.Value.Value;
    }

    [Benchmark(Description = "Collection Put with secondary indexes (insert, tx rollback)")]
    public async Task PutWithIndexes_Insert()
    {
        int id = Interlocked.Increment(ref _nextInsertId);
        var document = CreateDoc(id, $"insert:{id}");
        await _writeDb.BeginTransactionAsync();
        await _writeCollection.PutAsync($"bench:new:{id}", document);
        await _writeDb.RollbackAsync();
        _sink ^= document.Value;
    }

    [Benchmark(Description = "Collection Put with secondary indexes (update, tx rollback)")]
    public async Task PutWithIndexes_Update()
    {
        int slot = (Interlocked.Increment(ref _nextUpdateSlot) & int.MaxValue) % UpdateWorkingSetSize;
        int version = Interlocked.Increment(ref _nextInsertId);
        var document = CreateDoc(version, $"update:{version}");
        await _writeDb.BeginTransactionAsync();
        await _writeCollection.PutAsync($"bench:slot:{slot}", document);
        await _writeDb.RollbackAsync();
        _sink ^= document.Value;
    }

    [Benchmark(Description = "Collection Delete with secondary indexes (tx rollback)")]
    public async Task DeleteWithIndexes_Restore()
    {
        int slot = (Interlocked.Increment(ref _nextDeleteSlot) & int.MaxValue) % UpdateWorkingSetSize;
        string key = $"bench:slot:{slot}";
        await _writeDb.BeginTransactionAsync();
        bool deleted = await _writeCollection.DeleteAsync(key);
        await _writeDb.RollbackAsync();
        if (deleted)
            _sink ^= slot;
    }

    private async Task GlobalSetupAsync()
    {
        _lookupRandom = new Random(42);
        var options = CreateInMemoryOptions();

        _lookupDb = await Database.OpenInMemoryAsync(options);
        _writeDb = await Database.OpenInMemoryAsync(options);

        _lookupCollection = await _lookupDb.GetCollectionAsync<BenchDoc>("bench_docs");
        _nestedLookupCollection = await _lookupDb.GetCollectionAsync<NestedBenchDoc>("nested_bench_docs");
        _writeCollection = await _writeDb.GetCollectionAsync<BenchDoc>("bench_docs");

        await SeedLookupCollectionAsync();
        await SeedNestedLookupCollectionAsync();
        await SeedWriteCollectionAsync();

        await _lookupCollection.EnsureIndexAsync(d => d.Value);
        await _lookupCollection.EnsureIndexAsync(d => d.Tag);
        await _nestedLookupCollection.EnsureIndexAsync("$.address.city");
        await _writeCollection.EnsureIndexAsync(d => d.Value);
        await _writeCollection.EnsureIndexAsync(d => d.Tag);

        _nextInsertId = SeedCount;
    }

    private async Task GlobalCleanupAsync()
    {
        if (_lookupDb != null)
            await _lookupDb.DisposeAsync();
        if (_writeDb != null)
            await _writeDb.DisposeAsync();
    }

    private async Task SeedLookupCollectionAsync()
    {
        await _lookupDb.BeginTransactionAsync();
        try
        {
            for (int i = 0; i < SeedCount; i++)
                await _lookupCollection.PutAsync($"doc:{i}", CreateDoc(i, $"tag:{i}"));

            await _lookupDb.CommitAsync();
        }
        catch
        {
            await _lookupDb.RollbackAsync();
            throw;
        }
    }

    private async Task SeedWriteCollectionAsync()
    {
        await _writeDb.BeginTransactionAsync();
        try
        {
            for (int i = 0; i < UpdateWorkingSetSize; i++)
                await _writeCollection.PutAsync($"bench:slot:{i}", CreateDoc(i, $"seed:{i}"));

            await _writeDb.CommitAsync();
        }
        catch
        {
            await _writeDb.RollbackAsync();
            throw;
        }
    }

    private async Task SeedNestedLookupCollectionAsync()
    {
        await _lookupDb.BeginTransactionAsync();
        try
        {
            for (int i = 0; i < SeedCount; i++)
            {
                await _nestedLookupCollection.PutAsync(
                    $"nested:{i}",
                    new NestedBenchDoc(
                        $"User_{i}",
                        new BenchAddress(s_categories[i % s_categories.Length], 98000 + (i % 100)),
                        i));
            }

            await _lookupDb.CommitAsync();
        }
        catch
        {
            await _lookupDb.RollbackAsync();
            throw;
        }
    }

    private static BenchDoc CreateDoc(int value, string tag)
        => new(
            $"User_{value}",
            value,
            s_categories[value % s_categories.Length],
            tag);

    private static DatabaseOptions CreateInMemoryOptions()
    {
        return new DatabaseOptions().ConfigureStorageEngine(builder =>
            builder.UsePagerOptions(new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(1_000_000),
            }));
    }
}
