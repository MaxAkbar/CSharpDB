using BenchmarkDotNet.Attributes;
using CSharpDB.Primitives;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
[InvocationCount(1)]
public class InMemorySqlBatchBenchmarks
{
    private const int BatchSize = 100;
    private const int BatchesPerInvocation = 256;

    private string _filePath = null!;
    private Database _fileDb = null!;
    private Database _memoryDb = null!;
    private InsertBatch _fileBatch = null!;
    private InsertBatch _memoryBatch = null!;
    private int _nextFileId;
    private int _nextMemoryId;

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => GlobalCleanupAsync().GetAwaiter().GetResult();

    [Benchmark(Baseline = true, Description = "SQL batch insert x100 (file-backed)")]
    public async Task BatchInsert100_FileBacked()
    {
        for (int batchIndex = 0; batchIndex < BatchesPerInvocation; batchIndex++)
            _nextFileId = await ExecuteBatchAsync(_fileDb, _fileBatch, _nextFileId, "file_batch");
    }

    [Benchmark(Description = "SQL batch insert x100 (in-memory)")]
    public async Task BatchInsert100_InMemory()
    {
        for (int batchIndex = 0; batchIndex < BatchesPerInvocation; batchIndex++)
            _nextMemoryId = await ExecuteBatchAsync(_memoryDb, _memoryBatch, _nextMemoryId, "memory_batch");
    }

    private async Task GlobalSetupAsync()
    {
        _filePath = Path.Combine(Path.GetTempPath(), $"csharpdb_memory_batch_sql_{Guid.NewGuid():N}.db");

        _memoryDb = await Database.OpenInMemoryAsync();
        _fileDb = await Database.OpenAsync(_filePath);

        await _memoryDb.ExecuteAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT)");
        await _fileDb.ExecuteAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT)");

        _fileBatch = _fileDb.PrepareInsertBatch("bench", BatchSize);
        _memoryBatch = _memoryDb.PrepareInsertBatch("bench", BatchSize);
        _nextFileId = 1_000_000;
        _nextMemoryId = 2_000_000;
    }

    private async Task GlobalCleanupAsync()
    {
        if (_fileDb != null)
            await _fileDb.DisposeAsync();
        if (_memoryDb != null)
            await _memoryDb.DisposeAsync();

        try { if (File.Exists(_filePath)) File.Delete(_filePath); } catch { }
        try { if (File.Exists(_filePath + ".wal")) File.Delete(_filePath + ".wal"); } catch { }
    }

    private static async Task<int> ExecuteBatchAsync(Database db, InsertBatch batch, int nextId, string textValue)
    {
        batch.Clear();
        await db.BeginTransactionAsync();
        for (int i = 0; i < BatchSize; i++)
        {
            int id = nextId + 1;
            nextId = id;
            var row = new DbValue[4];
            row[0] = DbValue.FromInteger(id);
            row[1] = DbValue.FromInteger(id * 10L);
            row[2] = DbValue.FromText(textValue);
            row[3] = DbValue.FromText("Alpha");
            batch.AddRow(row);
        }

        await batch.ExecuteAsync();
        await db.CommitAsync();
        return nextId;
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
[InvocationCount(1)]
public class InMemoryCollectionBatchBenchmarks
{
    private const int BatchSize = 100;
    private const int BatchesPerInvocation = 256;

    private sealed record BenchDoc(string Name, int Value, string Category);

    private string _filePath = null!;
    private Database _fileDb = null!;
    private Database _memoryDb = null!;
    private Collection<BenchDoc> _fileCollection = null!;
    private Collection<BenchDoc> _memoryCollection = null!;
    private int _nextFileId;
    private int _nextMemoryId;

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => GlobalCleanupAsync().GetAwaiter().GetResult();

    [Benchmark(Baseline = true, Description = "Collection batch put x100 (file-backed)")]
    public async Task BatchPut100_FileBacked()
    {
        for (int batchIndex = 0; batchIndex < BatchesPerInvocation; batchIndex++)
            _nextFileId = await ExecuteBatchAsync(_fileDb, _fileCollection, _nextFileId, "file");
    }

    [Benchmark(Description = "Collection batch put x100 (in-memory)")]
    public async Task BatchPut100_InMemory()
    {
        for (int batchIndex = 0; batchIndex < BatchesPerInvocation; batchIndex++)
            _nextMemoryId = await ExecuteBatchAsync(_memoryDb, _memoryCollection, _nextMemoryId, "memory");
    }

    private async Task GlobalSetupAsync()
    {
        _filePath = Path.Combine(Path.GetTempPath(), $"csharpdb_memory_batch_collection_{Guid.NewGuid():N}.db");

        _memoryDb = await Database.OpenInMemoryAsync();
        _fileDb = await Database.OpenAsync(_filePath);
        _fileCollection = await _fileDb.GetCollectionAsync<BenchDoc>("bench_docs");
        _memoryCollection = await _memoryDb.GetCollectionAsync<BenchDoc>("bench_docs");
        _nextFileId = 1_000_000;
        _nextMemoryId = 2_000_000;
    }

    private async Task GlobalCleanupAsync()
    {
        if (_fileDb != null)
            await _fileDb.DisposeAsync();
        if (_memoryDb != null)
            await _memoryDb.DisposeAsync();

        try { if (File.Exists(_filePath)) File.Delete(_filePath); } catch { }
        try { if (File.Exists(_filePath + ".wal")) File.Delete(_filePath + ".wal"); } catch { }
    }

    private static async Task<int> ExecuteBatchAsync(Database db, Collection<BenchDoc> collection, int nextId, string prefix)
    {
        await db.BeginTransactionAsync();
        for (int i = 0; i < BatchSize; i++)
        {
            int id = nextId + 1;
            nextId = id;
            await collection.PutAsync($"doc:{prefix}:{id}", new BenchDoc($"{prefix}_{id}", id, "Alpha"));
        }

        await db.CommitAsync();
        return nextId;
    }
}
