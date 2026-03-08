using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class InMemoryPersistenceBenchmarks
{
    private string _sqlSourcePath = null!;
    private string _collectionSourcePath = null!;
    private string _sqlSavePath = null!;
    private string _collectionSavePath = null!;
    private Database _sqlMemoryDb = null!;
    private Database _collectionMemoryDb = null!;
    private long _sink;

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => GlobalCleanupAsync().GetAwaiter().GetResult();

    [Benchmark(Baseline = true, Description = "Load into memory (SQL DB + WAL)")]
    public async Task LoadIntoMemory_SqlDatabase()
    {
        await using var db = await Database.LoadIntoMemoryAsync(_sqlSourcePath);
        _sink = db.GetTableNames().Count;
    }

    [Benchmark(Description = "Load into memory (collection DB + WAL)")]
    public async Task LoadIntoMemory_CollectionDatabase()
    {
        await using var db = await Database.LoadIntoMemoryAsync(_collectionSourcePath);
        _sink = db.GetCollectionNames().Count;
    }

    [Benchmark(Description = "Save in-memory SQL snapshot to disk")]
    public async Task SaveToFile_SqlSnapshot()
    {
        await _sqlMemoryDb.SaveToFileAsync(_sqlSavePath);
        _sink = new FileInfo(_sqlSavePath).Length;
    }

    [Benchmark(Description = "Save in-memory collection snapshot to disk")]
    public async Task SaveToFile_CollectionSnapshot()
    {
        await _collectionMemoryDb.SaveToFileAsync(_collectionSavePath);
        _sink = new FileInfo(_collectionSavePath).Length;
    }

    private async Task GlobalSetupAsync()
    {
        _sqlSourcePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededSqlDatabaseAsync("memory-persist-sql", 10_000);
        _collectionSourcePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededCollectionDatabaseAsync("memory-persist-col", 10_000);

        _sqlMemoryDb = await Database.LoadIntoMemoryAsync(_sqlSourcePath);
        _collectionMemoryDb = await Database.LoadIntoMemoryAsync(_collectionSourcePath);

        _sqlSavePath = Path.Combine(Path.GetTempPath(), $"memory-persist-save-sql_{Guid.NewGuid():N}.db");
        _collectionSavePath = Path.Combine(Path.GetTempPath(), $"memory-persist-save-col_{Guid.NewGuid():N}.db");
    }

    private async Task GlobalCleanupAsync()
    {
        if (_sqlMemoryDb != null)
            await _sqlMemoryDb.DisposeAsync();
        if (_collectionMemoryDb != null)
            await _collectionMemoryDb.DisposeAsync();

        InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(_sqlSourcePath);
        InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(_collectionSourcePath);
        InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(_sqlSavePath);
        InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(_collectionSavePath);
    }
}
