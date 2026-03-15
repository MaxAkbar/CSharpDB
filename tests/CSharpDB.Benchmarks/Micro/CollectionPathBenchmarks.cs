using System.Text.Json;
using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Primitives;
using CSharpDB.Engine;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[Config(typeof(CollectionInProcessBenchmarkConfig))]
public class CollectionPayloadBenchmarks
{
    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    private readonly CollectionDocumentCodec<BenchDoc> _directCodec = new(new DefaultRecordSerializer());
    private readonly IRecordSerializer _legacySerializer = new DefaultRecordSerializer();
    private readonly BenchDoc _document = new("Alice Example", 37, "Alpha", "alice@example.com");
    private readonly string _key = "doc:42";
    private byte[] _directPayload = null!;
    private byte[] _legacyPayload = null!;
    private string? _sink;

    private sealed record BenchDoc(string Name, int Age, string Category, string Email);

    [GlobalSetup]
    public void GlobalSetup()
    {
        _directPayload = _directCodec.Encode(_key, _document);
        _legacyPayload = EncodeLegacy();
    }

    [Benchmark(Baseline = true, Description = "Collection encode (direct payload)")]
    public byte[] Encode_DirectPayload()
        => _directCodec.Encode(_key, _document);

    [Benchmark(Description = "Collection encode (legacy row format)")]
    public byte[] Encode_LegacyRowFormat()
        => EncodeLegacy();

    [Benchmark(Description = "Collection decode (direct payload)")]
    public void Decode_DirectPayload()
    {
        var decoded = _directCodec.Decode(_directPayload);
        _sink = decoded.Document.Email;
    }

    [Benchmark(Description = "Collection decode (legacy row format)")]
    public void Decode_LegacyRowFormat()
    {
        var values = _legacySerializer.Decode(_legacyPayload);
        var document = JsonSerializer.Deserialize<BenchDoc>(values[1].AsText, s_jsonOptions);
        _sink = document?.Email;
    }

    private byte[] EncodeLegacy()
    {
        string json = JsonSerializer.Serialize(_document, s_jsonOptions);
        return _legacySerializer.Encode(
        [
            DbValue.FromText(_key),
            DbValue.FromText(json),
        ]);
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class CollectionSchemaBreadthBenchmarks
{
    private const int ExtraTableCount = 48;
    private const int ExtraCollectionCount = 48;
    private const int WorkingSetSize = 1024;

    private Database _narrowDb = null!;
    private Database _wideDb = null!;
    private Collection<BenchDoc> _narrowCollection = null!;
    private Collection<BenchDoc> _wideCollection = null!;
    private int _narrowCounter;
    private int _wideCounter;
    private string? _sink;

    private sealed record BenchDoc(string Name, int Value, string Category);

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => GlobalCleanupAsync().GetAwaiter().GetResult();

    [Benchmark(Baseline = true, Description = "Collection put (minimal schema, in-memory)")]
    public async Task CollectionPut_MinimalSchema_InMemory()
    {
        int slot = Interlocked.Increment(ref _narrowCounter) % WorkingSetSize;
        var document = new BenchDoc($"Narrow_{slot}", slot, "Alpha");
        await _narrowCollection.PutAsync($"doc:{slot}", document);
        _sink = document.Name;
    }

    [Benchmark(Description = "Collection put (many unrelated tables/collections, in-memory)")]
    public async Task CollectionPut_WideSchema_InMemory()
    {
        int slot = Interlocked.Increment(ref _wideCounter) % WorkingSetSize;
        var document = new BenchDoc($"Wide_{slot}", slot, "Beta");
        await _wideCollection.PutAsync($"doc:{slot}", document);
        _sink = document.Name;
    }

    private async Task GlobalSetupAsync()
    {
        _narrowDb = await Database.OpenInMemoryAsync();
        _wideDb = await Database.OpenInMemoryAsync();

        _narrowCollection = await _narrowDb.GetCollectionAsync<BenchDoc>("bench_docs");
        _wideCollection = await _wideDb.GetCollectionAsync<BenchDoc>("bench_docs");

        await SeedWorkingSetAsync(_narrowDb, _narrowCollection, "Narrow");
        await SeedWorkingSetAsync(_wideDb, _wideCollection, "Wide");
        await CreateWideSchemaAsync(_wideDb);
    }

    private async Task GlobalCleanupAsync()
    {
        if (_narrowDb != null)
            await _narrowDb.DisposeAsync();
        if (_wideDb != null)
            await _wideDb.DisposeAsync();
    }

    private static async Task SeedWorkingSetAsync(
        Database db,
        Collection<BenchDoc> collection,
        string prefix)
    {
        await db.BeginTransactionAsync();
        for (int i = 0; i < WorkingSetSize; i++)
        {
            await collection.PutAsync(
                $"doc:{i}",
                new BenchDoc($"{prefix}_{i}", i, i % 2 == 0 ? "Alpha" : "Beta"));
        }

        await db.CommitAsync();
    }

    private static async Task CreateWideSchemaAsync(Database db)
    {
        for (int i = 0; i < ExtraTableCount; i++)
        {
            await db.ExecuteAsync(
                $"CREATE TABLE extra_{i} (id INTEGER PRIMARY KEY, value INTEGER, category TEXT)");
        }

        for (int i = 0; i < ExtraCollectionCount; i++)
        {
            await db.GetCollectionAsync<BenchDoc>($"extra_docs_{i}");
        }
    }
}
