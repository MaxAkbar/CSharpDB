using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Data;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class InMemorySqlBenchmarks
{
    private const int SeedRowCount = 10_000;

    private string _filePath = null!;
    private Database _fileDb = null!;
    private Database _memoryDb = null!;
    private int _nextFileInsertId;
    private int _nextMemoryInsertId;
    private long _sink;

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => GlobalCleanupAsync().GetAwaiter().GetResult();

    [Benchmark(Baseline = true, Description = "SQL point lookup (file-backed)")]
    public async Task SqlPointLookup_FileBacked()
    {
        await using var result = await _fileDb.ExecuteAsync("SELECT value FROM bench WHERE id = 5000");
        if (await result.MoveNextAsync())
            _sink = result.Current[0].AsInteger;
    }

    [Benchmark(Description = "SQL point lookup (in-memory)")]
    public async Task SqlPointLookup_InMemory()
    {
        await using var result = await _memoryDb.ExecuteAsync("SELECT value FROM bench WHERE id = 5000");
        if (await result.MoveNextAsync())
            _sink = result.Current[0].AsInteger;
    }

    [Benchmark(Description = "SQL insert (file-backed)")]
    public async Task SqlInsert_FileBacked()
    {
        int id = Interlocked.Increment(ref _nextFileInsertId);
        await using var result = await _fileDb.ExecuteAsync(
            $"INSERT INTO bench VALUES ({id}, {id * 10L}, 'file_{id}', 'Alpha')");
        _sink = result.RowsAffected;
    }

    [Benchmark(Description = "SQL insert (in-memory)")]
    public async Task SqlInsert_InMemory()
    {
        int id = Interlocked.Increment(ref _nextMemoryInsertId);
        await using var result = await _memoryDb.ExecuteAsync(
            $"INSERT INTO bench VALUES ({id}, {id * 10L}, 'memory_{id}', 'Alpha')");
        _sink = result.RowsAffected;
    }

    private async Task GlobalSetupAsync()
    {
        _filePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededSqlDatabaseAsync("memory-sql", SeedRowCount);
        _memoryDb = await Database.LoadIntoMemoryAsync(_filePath);
        _fileDb = await Database.OpenAsync(_filePath);
        _nextFileInsertId = SeedRowCount + 1_000_000;
        _nextMemoryInsertId = SeedRowCount + 2_000_000;
    }

    private async Task GlobalCleanupAsync()
    {
        if (_fileDb != null)
            await _fileDb.DisposeAsync();
        if (_memoryDb != null)
            await _memoryDb.DisposeAsync();

        InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(_filePath);
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class InMemoryCollectionBenchmarks
{
    private const int SeedRowCount = 10_000;

    private string _filePath = null!;
    private Database _fileDb = null!;
    private Database _memoryDb = null!;
    private Collection<BenchDoc> _fileCollection = null!;
    private Collection<BenchDoc> _memoryCollection = null!;
    private int _nextFileId;
    private int _nextMemoryId;
    private string? _sink;

    private record BenchDoc(string Name, int Value, string Category);

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => GlobalCleanupAsync().GetAwaiter().GetResult();

    [Benchmark(Baseline = true, Description = "Collection get (file-backed)")]
    public async Task CollectionGet_FileBacked()
    {
        var doc = await _fileCollection.GetAsync("doc:5000");
        _sink = doc?.Name;
    }

    [Benchmark(Description = "Collection get (in-memory)")]
    public async Task CollectionGet_InMemory()
    {
        var doc = await _memoryCollection.GetAsync("doc:5000");
        _sink = doc?.Name;
    }

    [Benchmark(Description = "Collection put (file-backed)")]
    public async Task CollectionPut_FileBacked()
    {
        int id = Interlocked.Increment(ref _nextFileId);
        await _fileCollection.PutAsync($"doc:new:{id}", new BenchDoc($"FileUser_{id}", id, "Alpha"));
    }

    [Benchmark(Description = "Collection put (in-memory)")]
    public async Task CollectionPut_InMemory()
    {
        int id = Interlocked.Increment(ref _nextMemoryId);
        await _memoryCollection.PutAsync($"doc:new:{id}", new BenchDoc($"MemoryUser_{id}", id, "Alpha"));
    }

    private async Task GlobalSetupAsync()
    {
        _filePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededCollectionDatabaseAsync("memory-collection", SeedRowCount);
        _memoryDb = await Database.LoadIntoMemoryAsync(_filePath);
        _fileDb = await Database.OpenAsync(_filePath);
        _fileCollection = await _fileDb.GetCollectionAsync<BenchDoc>("bench_docs");
        _memoryCollection = await _memoryDb.GetCollectionAsync<BenchDoc>("bench_docs");
        _nextFileId = SeedRowCount + 1_000_000;
        _nextMemoryId = SeedRowCount + 2_000_000;
    }

    private async Task GlobalCleanupAsync()
    {
        if (_fileDb != null)
            await _fileDb.DisposeAsync();
        if (_memoryDb != null)
            await _memoryDb.DisposeAsync();

        InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(_filePath);
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class InMemoryAdoNetBenchmarks
{
    private string _sharedConnectionString = null!;
    private CSharpDbConnection _privateConn = null!;
    private CSharpDbConnection _sharedConn = null!;
    private CSharpDbCommand _privateSelect = null!;
    private CSharpDbCommand _sharedSelect = null!;
    private CSharpDbCommand _privateInsert = null!;
    private CSharpDbCommand _sharedInsert = null!;
    private CSharpDbParameter _privateInsertId = null!;
    private CSharpDbParameter _sharedInsertId = null!;
    private int _nextPrivateId;
    private int _nextSharedId;
    private long _sink;

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => GlobalCleanupAsync().GetAwaiter().GetResult();

    [Benchmark(Baseline = true, Description = "ADO.NET ExecuteScalar (private :memory:)")]
    public async Task ExecuteScalar_PrivateMemory()
    {
        _sink = Convert.ToInt64(await _privateSelect.ExecuteScalarAsync());
    }

    [Benchmark(Description = "ADO.NET ExecuteScalar (named shared :memory:)")]
    public async Task ExecuteScalar_NamedSharedMemory()
    {
        _sink = Convert.ToInt64(await _sharedSelect.ExecuteScalarAsync());
    }

    [Benchmark(Description = "ADO.NET insert (private :memory:)")]
    public async Task ExecuteNonQuery_Insert_PrivateMemory()
    {
        int id = Interlocked.Increment(ref _nextPrivateId);
        _privateInsertId.Value = id;
        _sink = await _privateInsert.ExecuteNonQueryAsync();
    }

    [Benchmark(Description = "ADO.NET insert (named shared :memory:)")]
    public async Task ExecuteNonQuery_Insert_NamedSharedMemory()
    {
        int id = Interlocked.Increment(ref _nextSharedId);
        _sharedInsertId.Value = id;
        _sink = await _sharedInsert.ExecuteNonQueryAsync();
    }

    private async Task GlobalSetupAsync()
    {
        _sharedConnectionString = $"Data Source=:memory:bench-{Guid.NewGuid():N}";

        _privateConn = new CSharpDbConnection("Data Source=:memory:");
        _sharedConn = new CSharpDbConnection(_sharedConnectionString);
        await _privateConn.OpenAsync();
        await _sharedConn.OpenAsync();

        await InitializeConnectionAsync(_privateConn, "private");
        await InitializeConnectionAsync(_sharedConn, "shared");

        _privateSelect = (CSharpDbCommand)_privateConn.CreateCommand();
        _privateSelect.CommandText = "SELECT COUNT(*) FROM t";

        _sharedSelect = (CSharpDbCommand)_sharedConn.CreateCommand();
        _sharedSelect.CommandText = "SELECT COUNT(*) FROM t";

        _privateInsert = (CSharpDbCommand)_privateConn.CreateCommand();
        _privateInsert.CommandText = "INSERT INTO t VALUES (@id, 'private')";
        _privateInsertId = _privateInsert.Parameters.AddWithValue("@id", 0);
        _privateInsert.Prepare();

        _sharedInsert = (CSharpDbCommand)_sharedConn.CreateCommand();
        _sharedInsert.CommandText = "INSERT INTO t VALUES (@id, 'shared')";
        _sharedInsertId = _sharedInsert.Parameters.AddWithValue("@id", 0);
        _sharedInsert.Prepare();

        _nextPrivateId = 1_000_000;
        _nextSharedId = 2_000_000;
    }

    private async Task GlobalCleanupAsync()
    {
        _privateSelect?.Dispose();
        _sharedSelect?.Dispose();
        _privateInsert?.Dispose();
        _sharedInsert?.Dispose();

        if (_privateConn != null)
            await _privateConn.DisposeAsync();
        if (_sharedConn != null)
            await _sharedConn.DisposeAsync();

        CSharpDbConnection.ClearAllPools();
    }

    private static async Task InitializeConnectionAsync(CSharpDbConnection connection, string prefix)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();

        for (int i = 0; i < 1000; i++)
        {
            cmd.CommandText = $"INSERT INTO t VALUES ({i}, '{prefix}_{i}')";
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
