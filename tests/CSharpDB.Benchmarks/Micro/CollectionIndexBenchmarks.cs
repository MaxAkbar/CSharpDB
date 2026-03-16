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
    private Collection<ArrayBenchDoc> _arrayLookupCollection = null!;
    private Collection<NestedArrayBenchDoc> _nestedArrayLookupCollection = null!;
    private Collection<TemporalBenchDoc> _temporalLookupCollection = null!;
    private Collection<BenchDoc> _writeCollection = null!;
    private Random _lookupRandom = null!;
    private int _nextInsertId;
    private int _nextUpdateSlot = -1;
    private int _nextDeleteSlot = -1;
    private long _sink;

    private sealed record BenchDoc(string Name, int Value, string Category, string Tag);
    private sealed record BenchAddress(string City, int ZipCode);
    private sealed record NestedBenchDoc(string Name, BenchAddress Address, int Value);
    private sealed record ArrayBenchDoc(string Name, string[] Tags, int Value);
    private sealed record BenchOrder(string Sku, int Quantity);
    private sealed record NestedArrayBenchDoc(string Name, BenchOrder[] Orders, int Value);
    private sealed record TemporalBenchDoc(string Name, Guid SessionId, DateOnly EventDate, TimeOnly StartTime, int Value);

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

    [Benchmark(Description = "Collection FindByPath nested path equality (string path, many matches)")]
    public async Task FindByPath_NestedPath_StringPath()
    {
        int id = _lookupRandom.Next(0, SeedCount);
        string city = s_categories[id % s_categories.Length];
        await foreach (var match in _nestedLookupCollection.FindByPathAsync("$.address.city", city))
            _sink ^= match.Value.Value;
    }

    [Benchmark(Description = "Collection FindByIndex array path equality (string path, many matches)")]
    public async Task FindByIndex_ArrayPath_StringPath()
    {
        int id = _lookupRandom.Next(0, SeedCount);
        string tag = s_categories[id % s_categories.Length];
        await foreach (var match in _arrayLookupCollection.FindByIndexAsync("$.tags[]", tag))
            _sink ^= match.Value.Value;
    }

    [Benchmark(Description = "Collection FindByPath array path equality (string path, many matches)")]
    public async Task FindByPath_ArrayPath_StringPath()
    {
        int id = _lookupRandom.Next(0, SeedCount);
        string tag = s_categories[id % s_categories.Length];
        await foreach (var match in _arrayLookupCollection.FindByPathAsync("$.tags[]", tag))
            _sink ^= match.Value.Value;
    }

    [Benchmark(Description = "Collection FindByPath nested array path equality (string path, many matches)")]
    public async Task FindByPath_NestedArrayPath_StringPath()
    {
        int id = _lookupRandom.Next(0, SeedCount);
        string sku = s_categories[id % s_categories.Length];
        await foreach (var match in _nestedArrayLookupCollection.FindByPathAsync("$.orders[].sku", sku))
            _sink ^= match.Value.Value;
    }

    [Benchmark(Description = "Collection FindByPath integer range (string path, 1024 matches)")]
    public async Task FindByPath_IntegerRange_StringPath()
    {
        int start = (_lookupRandom.Next(0, SeedCount - 1_024) / 64) * 64;
        int end = start + 1_023;
        await foreach (var match in _lookupCollection.FindByPathRangeAsync("Value", start, end))
            _sink ^= match.Value.Value;
    }

    [Benchmark(Description = "Collection FindByPath text range (string path, 1000 matches)")]
    public async Task FindByPath_TextRange_StringPath()
    {
        await foreach (var match in _lookupCollection.FindByPathRangeAsync("Tag", "tag:1000", "tag:1999"))
            _sink ^= match.Value.Value;
    }

    [Benchmark(Description = "Collection FindByPath Guid equality (string path, 1 match)")]
    public async Task FindByPath_GuidEquality_StringPath()
    {
        int id = _lookupRandom.Next(0, SeedCount);
        Guid sessionId = CreateGuid(id);
        await foreach (var match in _temporalLookupCollection.FindByPathAsync("SessionId", sessionId))
            _sink ^= match.Value.Value;
    }

    [Benchmark(Description = "Collection FindByPath DateOnly range (string path, 1000 matches)")]
    public async Task FindByPath_DateOnlyRange_StringPath()
    {
        int startOffset = _lookupRandom.Next(0, SeedCount - 1_000);
        DateOnly start = new(2026, 1, 1);
        await foreach (var match in _temporalLookupCollection.FindByPathRangeAsync(
            "EventDate",
            start.AddDays(startOffset),
            start.AddDays(startOffset + 999)))
        {
            _sink ^= match.Value.Value;
        }
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
        _arrayLookupCollection = await _lookupDb.GetCollectionAsync<ArrayBenchDoc>("array_bench_docs");
        _nestedArrayLookupCollection = await _lookupDb.GetCollectionAsync<NestedArrayBenchDoc>("nested_array_bench_docs");
        _temporalLookupCollection = await _lookupDb.GetCollectionAsync<TemporalBenchDoc>("temporal_bench_docs");
        _writeCollection = await _writeDb.GetCollectionAsync<BenchDoc>("bench_docs");

        await SeedLookupCollectionAsync();
        await SeedNestedLookupCollectionAsync();
        await SeedArrayLookupCollectionAsync();
        await SeedNestedArrayLookupCollectionAsync();
        await SeedTemporalLookupCollectionAsync();
        await SeedWriteCollectionAsync();

        await _lookupCollection.EnsureIndexAsync(d => d.Value);
        await _lookupCollection.EnsureIndexAsync(d => d.Tag);
        await _nestedLookupCollection.EnsureIndexAsync("$.address.city");
        await _arrayLookupCollection.EnsureIndexAsync("$.tags[]");
        await _nestedArrayLookupCollection.EnsureIndexAsync("$.orders[].sku");
        await _temporalLookupCollection.EnsureIndexAsync(d => d.SessionId);
        await _temporalLookupCollection.EnsureIndexAsync(d => d.EventDate);
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

    private async Task SeedArrayLookupCollectionAsync()
    {
        await _lookupDb.BeginTransactionAsync();
        try
        {
            for (int i = 0; i < SeedCount; i++)
            {
                await _arrayLookupCollection.PutAsync(
                    $"array:{i}",
                    new ArrayBenchDoc(
                        $"User_{i}",
                        [s_categories[i % s_categories.Length], $"tag:{i % 128}"],
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

    private async Task SeedNestedArrayLookupCollectionAsync()
    {
        await _lookupDb.BeginTransactionAsync();
        try
        {
            for (int i = 0; i < SeedCount; i++)
            {
                string sku = s_categories[i % s_categories.Length];
                await _nestedArrayLookupCollection.PutAsync(
                    $"orders:{i}",
                    new NestedArrayBenchDoc(
                        $"OrderDoc {i}",
                        [
                            new BenchOrder(sku, i % 7 + 1),
                            new BenchOrder($"sku:{i}", i % 5 + 1)
                        ],
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

    private async Task SeedTemporalLookupCollectionAsync()
    {
        await _lookupDb.BeginTransactionAsync();
        try
        {
            DateOnly start = new(2026, 1, 1);
            for (int i = 0; i < SeedCount; i++)
            {
                await _temporalLookupCollection.PutAsync(
                    $"temporal:{i}",
                    new TemporalBenchDoc(
                        $"Temporal_{i}",
                        CreateGuid(i),
                        start.AddDays(i),
                        new TimeOnly((i / 60) % 24, i % 60, i % 60),
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

    private static Guid CreateGuid(int value)
        => new(value, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    private static DatabaseOptions CreateInMemoryOptions()
    {
        return new DatabaseOptions().ConfigureStorageEngine(builder =>
            builder.UsePagerOptions(new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(1_000_000),
            }));
    }
}
