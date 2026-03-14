using BenchmarkDotNet.Attributes;
using CSharpDB.Engine;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class CollectionLookupFallbackBenchmarks
{
    private const int SeedCount = 10_000;

    private static readonly string[] s_categories = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon"];

    private Database _db = null!;
    private Collection<BenchDoc> _collection = null!;
    private Random _rng = null!;
    private long _sink;

    private sealed record BenchDoc(string Name, int Value, string Category, string Tag);

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => GlobalCleanupAsync().GetAwaiter().GetResult();

    [Benchmark(Baseline = true, Description = "Collection FindByIndex fallback scan (int, no index)")]
    public async Task FindByIndexFallback_Integer_NoIndex()
    {
        int id = _rng.Next(0, SeedCount);
        await foreach (var match in _collection.FindByIndexAsync(d => d.Value, id))
            _sink ^= match.Value.Value;
    }

    [Benchmark(Description = "Collection FindByIndex fallback scan (text, no index)")]
    public async Task FindByIndexFallback_Text_NoIndex()
    {
        string category = s_categories[_rng.Next(0, s_categories.Length)];
        await foreach (var match in _collection.FindByIndexAsync(d => d.Category, category))
            _sink ^= match.Value.Value;
    }

    private async Task GlobalSetupAsync()
    {
        _rng = new Random(42);
        _db = await Database.OpenInMemoryAsync(CreateInMemoryOptions());
        _collection = await _db.GetCollectionAsync<BenchDoc>("bench_docs");

        await _db.BeginTransactionAsync();
        try
        {
            for (int i = 0; i < SeedCount; i++)
            {
                await _collection.PutAsync(
                    $"doc:{i}",
                    new BenchDoc(
                        $"User_{i}",
                        i,
                        s_categories[i % s_categories.Length],
                        $"tag:{i}"));
            }

            await _db.CommitAsync();
        }
        catch
        {
            await _db.RollbackAsync();
            throw;
        }
    }

    private async Task GlobalCleanupAsync()
    {
        if (_db != null)
            await _db.DisposeAsync();
    }

    private static DatabaseOptions CreateInMemoryOptions()
    {
        return new DatabaseOptions().ConfigureStorageEngine(builder =>
            builder.UsePagerOptions(new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(1_000_000),
            }));
    }
}
