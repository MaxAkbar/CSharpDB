using System.Collections.Concurrent;
using CSharpDB.Data;
using CSharpDB.Engine;
using CSharpDB.Storage.Paging;
using Microsoft.EntityFrameworkCore;

namespace CSharpDB.EntityFrameworkCore.Tests;

[Collection("ConnectionPoolState")]
public sealed class EmbeddedStorageTuningTests : IAsyncLifetime
{
    private readonly string _workspace = Path.Combine(Path.GetTempPath(), $"csharpdb_efcore_tuning_{Guid.NewGuid():N}");
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(_workspace);
        await CSharpDbConnection.ClearAllPoolsAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();

        if (Directory.Exists(_workspace))
            Directory.Delete(_workspace, recursive: true);
    }

    [Fact]
    public async Task UseCSharpDb_WithDirectDatabaseOptions_FlowsIntoCreatedConnection()
    {
        string dbPath = GetDbPath("direct_options");
        var interceptor = new TrackingPageOperationInterceptor();

        var options = new DbContextOptionsBuilder<ObservedRuntimeContext>()
            .UseCSharpDb(
                $"Data Source={dbPath}",
                csharpdb => csharpdb.UseDirectDatabaseOptions(
                    CreateObservedDirectDatabaseOptions(interceptor)))
            .Options;

        await using var db = new ObservedRuntimeContext(options);
        await db.Database.EnsureCreatedAsync(Ct);

        db.Items.Add(new ObservedItem { Value = 7 });
        await db.SaveChangesAsync(Ct);

        Assert.Equal(1, await db.Items.CountAsync(Ct));
        Assert.True(
            interceptor.TotalReads > 0 || interceptor.TotalWrites > 0,
            "Expected EF Core provider direct options to attach the configured page interceptor.");
    }

    [Fact]
    public async Task UseCSharpDb_WithStoragePresetAndEmbeddedOpenMode_FlowsIntoCreatedConnection()
    {
        string dbPath = GetDbPath("hybrid_snapshot");
        await EnsureDatabaseFileExistsAsync(dbPath);

        var options = new DbContextOptionsBuilder<ObservedRuntimeContext>()
            .UseCSharpDb(
                $"Data Source={dbPath}",
                csharpdb =>
                {
                    csharpdb.UseStoragePreset(CSharpDbStoragePreset.WriteOptimized);
                    csharpdb.UseEmbeddedOpenMode(CSharpDbEmbeddedOpenMode.HybridSnapshot);
                })
            .Options;

        await using (var db = new ObservedRuntimeContext(options))
        {
            await db.Database.OpenConnectionAsync(Ct);
            await db.Database.EnsureCreatedAsync(Ct);
            db.Items.Add(new ObservedItem { Value = 42 });
            await db.SaveChangesAsync(Ct);

            Assert.False(
                File.Exists(dbPath + ".wal"),
                "Expected snapshot hybrid mode to avoid a live WAL file while EF keeps the connection open.");
        }

        await using var afterDispose = await Database.OpenAsync(dbPath, Ct);
        await using var result = await afterDispose.ExecuteAsync("SELECT Value FROM Items WHERE Id = 1;", Ct);
        var rows = await result.ToListAsync(Ct);

        var row = Assert.Single(rows);
        Assert.Equal(42L, row[0].AsInteger);
    }

    [Fact]
    public async Task UseCSharpDb_WithExistingConnection_RejectsConflictingDirectDatabaseOptions()
    {
        string dbPath = GetDbPath("existing_connection_direct_conflict");
        var connection = new CSharpDbConnection($"Data Source={dbPath}");
        var providerOptions = new DbContextOptionsBuilder<ObservedRuntimeContext>()
            .UseCSharpDb(
                connection,
                csharpdb => csharpdb.UseDirectDatabaseOptions(new DatabaseOptions()))
            .Options;

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await using var db = new ObservedRuntimeContext(providerOptions);
            await db.Database.EnsureCreatedAsync(Ct);
        });
        Assert.Contains("DirectDatabaseOptions", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task UseCSharpDb_WithExistingConnection_RejectsConflictingEmbeddedOpenMode()
    {
        string dbPath = GetDbPath("existing_connection_open_mode_conflict");
        var connection = new CSharpDbConnection($"Data Source={dbPath}");
        var providerOptions = new DbContextOptionsBuilder<ObservedRuntimeContext>()
            .UseCSharpDb(
                connection,
                csharpdb => csharpdb.UseEmbeddedOpenMode(CSharpDbEmbeddedOpenMode.HybridSnapshot))
            .Options;

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await using var db = new ObservedRuntimeContext(providerOptions);
            await db.Database.EnsureCreatedAsync(Ct);
        });
        Assert.Contains("EmbeddedOpenMode", error.Message, StringComparison.Ordinal);
    }

    private string GetDbPath(string name)
        => Path.Combine(_workspace, $"{name}.db");

    private static DatabaseOptions CreateObservedDirectDatabaseOptions(
        TrackingPageOperationInterceptor interceptor)
    {
        return new DatabaseOptions
        {
            StorageEngineOptions = new CSharpDB.Storage.StorageEngine.StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    Interceptors = [interceptor],
                    MaxCachedPages = 16,
                },
            },
        };
    }

    private static async Task EnsureDatabaseFileExistsAsync(string dbPath)
    {
        await using var database = await Database.OpenAsync(dbPath, Ct);
        await database.CheckpointAsync(Ct);
    }

    private sealed class ObservedRuntimeContext(DbContextOptions<ObservedRuntimeContext> options) : DbContext(options)
    {
        public DbSet<ObservedItem> Items => Set<ObservedItem>();
    }

    private sealed class ObservedItem
    {
        public int Id { get; set; }

        public int Value { get; set; }
    }

    private sealed class TrackingPageOperationInterceptor : IPageOperationInterceptor
    {
        private readonly ConcurrentDictionary<PageReadSource, int> _readSources = new();
        private int _totalReads;
        private int _totalWrites;

        public int TotalReads => _totalReads;

        public int TotalWrites => _totalWrites;

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
