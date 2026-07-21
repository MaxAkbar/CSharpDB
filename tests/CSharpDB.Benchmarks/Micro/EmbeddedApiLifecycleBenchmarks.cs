using BenchmarkDotNet.Attributes;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Diagnoses file-backed lifecycle cost below ADO.NET. Every benchmark case owns
/// a separately prepared database, while all cases use the same explicit durable
/// storage policy.
/// </summary>
[BenchmarkCategory("Embedded", "Lifecycle")]
[MemoryDiagnoser]
[MedianColumn]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class EmbeddedApiLifecycleBenchmarks
{
    private string _rootPath = null!;
    private string _retainedClientPath = null!;
    private string _recycledClientPath = null!;
    private string _newClientPath = null!;
    private string _rollbackClientPath = null!;
    private string _commitClientPath = null!;
    private string _databasePath = null!;
    private string _storagePath = null!;

    private DatabaseOptions _retainedClientOptions = null!;
    private DatabaseOptions _recycledClientOptions = null!;
    private DatabaseOptions _newClientOptions = null!;
    private DatabaseOptions _rollbackClientOptions = null!;
    private DatabaseOptions _commitClientOptions = null!;
    private DatabaseOptions _databaseOptions = null!;
    private StorageEngineOptions _storageOptions = null!;

    private CSharpDbClient _retainedClient = null!;
    private CSharpDbClient _recycledClient = null!;
    private CSharpDbClient _rollbackClient = null!;
    private CSharpDbClient _commitClient = null!;
    private DefaultStorageEngineFactory _storageFactory = null!;

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _rootPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_embedded_lifecycle_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_rootPath);

        _retainedClientPath = GetCasePath("client-retained");
        _recycledClientPath = GetCasePath("client-recycled");
        _newClientPath = GetCasePath("client-new");
        _rollbackClientPath = GetCasePath("client-rollback");
        _commitClientPath = GetCasePath("client-commit");
        _databasePath = GetCasePath("database");
        _storagePath = GetCasePath("storage");

        _retainedClientOptions = CreateDatabaseOptions();
        _recycledClientOptions = CreateDatabaseOptions();
        _newClientOptions = CreateDatabaseOptions();
        _rollbackClientOptions = CreateDatabaseOptions();
        _commitClientOptions = CreateDatabaseOptions();
        _databaseOptions = CreateDatabaseOptions();
        _storageOptions = CreateStorageOptions();
        _storageFactory = new DefaultStorageEngineFactory();

        await PrepareDatabaseAsync(_retainedClientPath, _retainedClientOptions);
        await PrepareDatabaseAsync(_recycledClientPath, _recycledClientOptions);
        await PrepareDatabaseAsync(_newClientPath, _newClientOptions);
        await PrepareDatabaseAsync(_rollbackClientPath, _rollbackClientOptions);
        await PrepareDatabaseAsync(_commitClientPath, _commitClientOptions);
        await PrepareDatabaseAsync(_databasePath, _databaseOptions);
        await PrepareDatabaseAsync(
            _storagePath,
            new DatabaseOptions
            {
                StorageEngineFactory = _storageFactory,
                StorageEngineOptions = _storageOptions,
            });

        _retainedClient = CreateDirectClient(_retainedClientPath, _retainedClientOptions);
        _recycledClient = CreateDirectClient(_recycledClientPath, _recycledClientOptions);
        _rollbackClient = CreateDirectClient(_rollbackClientPath, _rollbackClientOptions);
        _commitClient = CreateDirectClient(_commitClientPath, _commitClientOptions);

        // Keep one direct client physically open so its benchmark represents the
        // already-warm, recommended long-lived client path.
        if (await _retainedClient.TryGetDatabaseAsync() is null)
            throw new InvalidOperationException("The retained direct client did not open a database.");
    }

    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        if (_retainedClient is not null)
            await _retainedClient.DisposeAsync();
        if (_recycledClient is not null)
            await _recycledClient.DisposeAsync();
        if (_rollbackClient is not null)
            await _rollbackClient.DisposeAsync();
        if (_commitClient is not null)
            await _commitClient.DisposeAsync();

        if (!string.IsNullOrWhiteSpace(_rootPath) && Directory.Exists(_rootPath))
            Directory.Delete(_rootPath, recursive: true);
    }

    [Benchmark(Description = "Reused direct client: access retained Database")]
    public async Task ReusedDirectClient_RetainedDatabaseAsync()
    {
        if (await _retainedClient.TryGetDatabaseAsync() is null)
            throw new InvalidOperationException("The retained direct client lost its database.");
    }

    [Benchmark(Description = "Reused direct client: open+release Database")]
    public async Task ReusedDirectClient_OpenReleaseAsync()
    {
        try
        {
            if (await _recycledClient.TryGetDatabaseAsync() is null)
                throw new InvalidOperationException("The reused direct client did not open a database.");
        }
        finally
        {
            await _recycledClient.ReleaseCachedDatabaseAsync();
        }
    }

    [Benchmark(Description = "New direct client: construct+open+dispose")]
    public async Task NewDirectClient_OpenDisposeAsync()
    {
        await using CSharpDbClient client = CreateDirectClient(_newClientPath, _newClientOptions);
        if (await client.TryGetDatabaseAsync() is null)
            throw new InvalidOperationException("The new direct client did not open a database.");
    }

    [Benchmark(Description = "Direct client transaction: begin+rollback")]
    public async Task DirectClientTransaction_BeginRollbackAsync()
    {
        TransactionSessionInfo transaction = await _rollbackClient.BeginTransactionAsync();
        await _rollbackClient.RollbackTransactionAsync(transaction.TransactionId);
    }

    [Benchmark(Description = "Direct client transaction: begin+commit")]
    public async Task DirectClientTransaction_BeginCommitAsync()
    {
        TransactionSessionInfo transaction = await _commitClient.BeginTransactionAsync();
        await _commitClient.CommitTransactionAsync(transaction.TransactionId);
    }

    [Benchmark(Description = "Database API: OpenAsync+DisposeAsync")]
    public async Task Database_OpenDisposeAsync()
    {
        await using Database database =
            await Database.OpenAsync(_databasePath, _databaseOptions);
    }

    [Benchmark(Description = "Storage factory: OpenAsync+Pager.DisposeAsync")]
    public async Task StorageFactory_OpenDisposeAsync()
    {
        StorageEngineContext context =
            await _storageFactory.OpenAsync(_storagePath, _storageOptions);
        await context.Pager.DisposeAsync();
    }

    private string GetCasePath(string caseName)
        => Path.Combine(_rootPath, $"{caseName}.db");

    private static CSharpDbClient CreateDirectClient(
        string path,
        DatabaseOptions databaseOptions)
        => (CSharpDbClient)CSharpDbClient.Create(new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Direct,
            DataSource = path,
            DirectDatabaseOptions = databaseOptions,
        });

    private static async Task PrepareDatabaseAsync(
        string path,
        DatabaseOptions options)
    {
        await using Database database = await Database.OpenAsync(path, options);
        await using (var result = await database.ExecuteAsync(
                         "CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT)"))
        {
        }

        await database.CheckpointAsync();
    }

    private static DatabaseOptions CreateDatabaseOptions()
        => new()
        {
            StorageEngineFactory = new DefaultStorageEngineFactory(),
            StorageEngineOptions = CreateStorageOptions(),
        };

    private static StorageEngineOptions CreateStorageOptions()
        => new()
        {
            DurabilityMode = DurabilityMode.Durable,
            DurableGroupCommit = DurableGroupCommitOptions.Disabled,
            AdvisoryStatisticsPersistenceMode = AdvisoryStatisticsPersistenceMode.Immediate,
            WalPreallocationChunkBytes = 0,
            PagerOptions = new PagerOptions(),
        };
}
