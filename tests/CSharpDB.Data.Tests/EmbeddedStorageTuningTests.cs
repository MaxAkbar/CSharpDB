using System.Collections.Concurrent;
using CSharpDB.Data;
using CSharpDB.Engine;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Data.Tests;

[Collection("ConnectionPoolState")]
public sealed class EmbeddedStorageTuningTests : IAsyncLifetime
{
    private readonly List<string> _paths = new();
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
        => await CSharpDbConnection.ClearAllPoolsAsync();

    public async ValueTask DisposeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();

        foreach (string path in _paths)
        {
            TryDelete(path);
            TryDelete(path + ".wal");
        }
    }

    [Fact]
    public void ConnectionStringBuilder_ParsesStoragePresetAndEmbeddedOpenMode_CaseInsensitively()
    {
        var builder = new CSharpDbConnectionStringBuilder(
            "Data Source=my.db;Storage Preset=directcoldfilelookup;Embedded Open Mode=hybridsnapshot");

        Assert.Equal(CSharpDbStoragePreset.DirectColdFileLookup, builder.StoragePreset);
        Assert.Equal(CSharpDbEmbeddedOpenMode.HybridSnapshot, builder.EmbeddedOpenMode);
    }

    [Fact]
    public async Task OpenAsync_FileBackedConnection_UsesDirectDatabaseOptions()
    {
        string dbPath = NewTempDbPath("direct_file");
        var interceptor = new TrackingPageOperationInterceptor();

        await SeedSqlDatabaseAsync(dbPath);

        await using var connection = new CSharpDbConnection(
            $"Data Source={dbPath}",
            CreateObservedDirectDatabaseOptions(interceptor, useMemoryMappedReads: true));

        await connection.OpenAsync(Ct);

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT value FROM bench WHERE id = 1;";

        Assert.Equal(10L, await command.ExecuteScalarAsync(Ct));
        Assert.True(
            interceptor.GetReadSourceCount(PageReadSource.MemoryMappedMainFile) > 0,
            "Expected file-backed direct tuning to enable memory-mapped main-file reads.");
    }

    [Fact]
    public async Task OpenAsync_FileBackedConnection_UsesHybridSnapshotFromConnectionString()
    {
        string dbPath = NewTempDbPath("hybrid_snapshot");
        await EnsureDatabaseFileExistsAsync(dbPath);

        await using (var connection = new CSharpDbConnection(
            $"Data Source={dbPath};Embedded Open Mode=HybridSnapshot"))
        {
            await connection.OpenAsync(Ct);

            using var command = connection.CreateCommand();
            command.CommandText = "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER);";
            await command.ExecuteNonQueryAsync(Ct);
            command.CommandText = "INSERT INTO items VALUES (1, 42);";
            await command.ExecuteNonQueryAsync(Ct);

            Assert.False(
                File.Exists(dbPath + ".wal"),
                "Expected snapshot hybrid mode to avoid a live WAL file while the connection stays open.");
        }

        await using var afterDispose = await Database.OpenAsync(dbPath, Ct);
        await using var result = await afterDispose.ExecuteAsync("SELECT value FROM items WHERE id = 1;", Ct);
        var rows = await result.ToListAsync(Ct);

        var row = Assert.Single(rows);
        Assert.Equal(42L, row[0].AsInteger);
    }

    [Fact]
    public async Task OpenAsync_PrivateMemoryConnection_UsesDirectDatabaseOptions()
    {
        var interceptor = new TrackingPageOperationInterceptor();

        await using var connection = new CSharpDbConnection(
            "Data Source=:memory:",
            CreateObservedDirectDatabaseOptions(interceptor, useMemoryMappedReads: false));

        await connection.OpenAsync(Ct);

        using var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER);";
        await command.ExecuteNonQueryAsync(Ct);
        command.CommandText = "INSERT INTO items VALUES (1, 21);";
        await command.ExecuteNonQueryAsync(Ct);
        command.CommandText = "SELECT value FROM items WHERE id = 1;";

        Assert.Equal(21L, await command.ExecuteScalarAsync(Ct));
        Assert.True(
            interceptor.TotalReads > 0 || interceptor.TotalWrites > 0,
            "Expected private in-memory direct tuning to attach the configured page interceptor.");
    }

    [Fact]
    public async Task OpenAsync_PrivateMemoryConnection_RejectsHybridOpenMode()
    {
        await using var connection = new CSharpDbConnection(
            "Data Source=:memory:;Embedded Open Mode=HybridSnapshot");

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => connection.OpenAsync(Ct));
        Assert.Contains("file-backed direct connections", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task OpenAsync_RemoteConnection_RejectsEmbeddedStorageTuning()
    {
        await using var connection = new CSharpDbConnection(
            "Transport=Grpc;Endpoint=http://localhost:5820;Storage Preset=WriteOptimized");

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => connection.OpenAsync(Ct));
        Assert.Contains("embedded storage tuning", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task OpenAsync_NamedSharedMemory_RejectsEmbeddedStorageTuning()
    {
        await using var connection = new CSharpDbConnection(
            $"Data Source=:memory:{Guid.NewGuid():N};Storage Preset=WriteOptimized");

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => connection.OpenAsync(Ct));
        Assert.Contains("named shared-memory", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ExplicitDirectDatabaseOptions_OverrideConflictingStoragePresetFromConnectionString()
    {
        string dbPath = NewTempDbPath("preset_override");
        var interceptor = new TrackingPageOperationInterceptor();

        await SeedSqlDatabaseAsync(dbPath);

        await using var connection = new CSharpDbConnection(
            $"Data Source={dbPath};Storage Preset=DirectColdFileLookup",
            CreateObservedDirectDatabaseOptions(interceptor, useMemoryMappedReads: false));

        await connection.OpenAsync(Ct);

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT value FROM bench WHERE id = 1;";

        Assert.Equal(10L, await command.ExecuteScalarAsync(Ct));
        Assert.True(interceptor.TotalReads > 0, "Expected the explicit direct options to attach the interceptor.");
        Assert.Equal(
            0,
            interceptor.GetReadSourceCount(PageReadSource.MemoryMappedMainFile));
    }

    [Fact]
    public async Task Pooling_ReusesConnectionsWhenEffectiveConfigurationMatches()
    {
        string dbPath = NewTempDbPath("pool_reuse");
        string connectionString =
            $"Data Source={dbPath};Pooling=true;Max Pool Size=1;Storage Preset=WriteOptimized;Embedded Open Mode=HybridIncrementalDurable";

        await using (var first = new CSharpDbConnection(connectionString))
        {
            await first.OpenAsync(Ct);
            await first.CloseAsync();
        }

        Assert.Equal(1, CSharpDbConnection.GetIdlePoolSizeForTest(connectionString));
        Assert.Equal(1, CSharpDbConnection.GetPoolCountForTest());

        await using (var second = new CSharpDbConnection(connectionString))
        {
            await second.OpenAsync(Ct);
            Assert.Equal(0, CSharpDbConnection.GetIdlePoolSizeForTest(connectionString));
            await second.CloseAsync();
        }

        Assert.Equal(1, CSharpDbConnection.GetIdlePoolSizeForTest(connectionString));
        Assert.Equal(1, CSharpDbConnection.GetPoolCountForTest());
    }

    [Fact]
    public async Task Pooling_DoesNotShareAcrossDistinctExplicitOptionsInstances()
    {
        string dbPath = NewTempDbPath("pool_distinct_options");
        string connectionString = $"Data Source={dbPath};Pooling=true;Max Pool Size=1";
        DatabaseOptions firstOptions = CreateEquivalentPooledDirectDatabaseOptions();
        DatabaseOptions secondOptions = CreateEquivalentPooledDirectDatabaseOptions();

        await using (var first = new CSharpDbConnection(connectionString, firstOptions))
        {
            await first.OpenAsync(Ct);
            await first.CloseAsync();
        }

        await using (var second = new CSharpDbConnection(connectionString, secondOptions))
        {
            await second.OpenAsync(Ct);
            await second.CloseAsync();
        }

        Assert.Equal(2, CSharpDbConnection.GetPoolCountForTest());
        Assert.Equal(1, CSharpDbConnection.GetIdlePoolSizeForTest(connectionString, firstOptions, hybridDatabaseOptions: null));
        Assert.Equal(1, CSharpDbConnection.GetIdlePoolSizeForTest(connectionString, secondOptions, hybridDatabaseOptions: null));
    }

    [Fact]
    public async Task ClearPoolAsync_ClearsAllPoolsForSameFileTarget()
    {
        string dbPath = NewTempDbPath("pool_clear");
        string connectionString = $"Data Source={dbPath};Pooling=true;Max Pool Size=1";
        DatabaseOptions directOptions = CreateEquivalentPooledDirectDatabaseOptions();

        await using (var plain = new CSharpDbConnection(connectionString))
        {
            await plain.OpenAsync(Ct);
            await plain.CloseAsync();
        }

        await using (var tuned = new CSharpDbConnection(connectionString, directOptions))
        {
            await tuned.OpenAsync(Ct);
            await tuned.CloseAsync();
        }

        Assert.Equal(2, CSharpDbConnection.GetPoolCountForTest());

        await CSharpDbConnection.ClearPoolAsync(connectionString);

        Assert.Equal(0, CSharpDbConnection.GetPoolCountForTest());
        Assert.Equal(0, CSharpDbConnection.GetIdlePoolSizeForTest(connectionString));
        Assert.Equal(0, CSharpDbConnection.GetIdlePoolSizeForTest(connectionString, directOptions, hybridDatabaseOptions: null));
    }

    private string NewTempDbPath(string prefix)
    {
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_data_{prefix}_{Guid.NewGuid():N}.db");
        _paths.Add(path);
        return path;
    }

    private static DatabaseOptions CreateObservedDirectDatabaseOptions(
        TrackingPageOperationInterceptor interceptor,
        bool useMemoryMappedReads)
    {
        return new DatabaseOptions
        {
            StorageEngineOptions = new CSharpDB.Storage.StorageEngine.StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    Interceptors = [interceptor],
                    UseMemoryMappedReads = useMemoryMappedReads,
                    MaxCachedPages = 16,
                },
            },
        };
    }

    private static DatabaseOptions CreateEquivalentPooledDirectDatabaseOptions()
        => new DatabaseOptions().ConfigureStorageEngine(builder => builder.UseWriteOptimizedPreset());

    private static async Task EnsureDatabaseFileExistsAsync(string dbPath)
    {
        await using var database = await Database.OpenAsync(dbPath, Ct);
        await database.CheckpointAsync(Ct);
    }

    private static async Task SeedSqlDatabaseAsync(string dbPath)
    {
        await using var database = await Database.OpenAsync(dbPath, Ct);
        await database.ExecuteAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, name TEXT);", Ct);
        await database.ExecuteAsync("INSERT INTO bench VALUES (1, 10, 'before');", Ct);
        await database.CheckpointAsync(Ct);
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch
        {
            // Best-effort temp file cleanup.
        }
    }

    private sealed class TrackingPageOperationInterceptor : IPageOperationInterceptor
    {
        private readonly ConcurrentDictionary<PageReadSource, int> _readSources = new();
        private int _totalReads;
        private int _totalWrites;

        public int TotalReads => _totalReads;

        public int TotalWrites => _totalWrites;

        public int GetReadSourceCount(PageReadSource source)
            => _readSources.TryGetValue(source, out int count) ? count : 0;

        public ValueTask OnBeforeReadAsync(uint pageId, CancellationToken ct = default)
            => ValueTask.CompletedTask;

        public ValueTask OnAfterReadAsync(uint pageId, PageReadSource source, CancellationToken ct = default)
        {
            Interlocked.Increment(ref _totalReads);
            _readSources.AddOrUpdate(source, 1, static (_, count) => count + 1);
            return ValueTask.CompletedTask;
        }

        public ValueTask OnBeforeWriteAsync(uint pageId, CancellationToken ct = default)
            => ValueTask.CompletedTask;

        public ValueTask OnAfterWriteAsync(uint pageId, bool succeeded, CancellationToken ct = default)
        {
            if (succeeded)
                Interlocked.Increment(ref _totalWrites);

            return ValueTask.CompletedTask;
        }

        public ValueTask OnCommitStartAsync(int dirtyPageCount, CancellationToken ct = default)
            => ValueTask.CompletedTask;

        public ValueTask OnCommitEndAsync(int dirtyPageCount, bool succeeded, CancellationToken ct = default)
            => ValueTask.CompletedTask;

        public ValueTask OnCheckpointStartAsync(int committedFrameCount, CancellationToken ct = default)
            => ValueTask.CompletedTask;

        public ValueTask OnCheckpointEndAsync(int committedFrameCount, bool succeeded, CancellationToken ct = default)
            => ValueTask.CompletedTask;

        public ValueTask OnRecoveryStartAsync(CancellationToken ct = default)
            => ValueTask.CompletedTask;

        public ValueTask OnRecoveryEndAsync(bool succeeded, CancellationToken ct = default)
            => ValueTask.CompletedTask;
    }
}
