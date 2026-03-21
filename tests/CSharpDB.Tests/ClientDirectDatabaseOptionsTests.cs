using System.Collections.Concurrent;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Tests;

public sealed class ClientDirectDatabaseOptionsTests
{
    [Fact]
    public void DirectDatabaseOptions_RejectsGrpcTransport()
    {
        var ex = Assert.Throws<CSharpDbClientConfigurationException>(() => CSharpDbClient.Create(new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Grpc,
            Endpoint = "http://localhost:5820",
            DirectDatabaseOptions = new DatabaseOptions(),
        }));

        Assert.Contains("does not support DirectDatabaseOptions", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DirectDatabaseOptions_AreUsedOnFirstOpen()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();
        var interceptor = new TrackingPageOperationInterceptor();

        try
        {
            await SeedSqlDatabaseAsync(dbPath, ct);

            await using var client = Assert.IsType<CSharpDbClient>(CSharpDbClient.Create(new CSharpDbClientOptions
            {
                DataSource = dbPath,
                DirectDatabaseOptions = CreateDirectDatabaseOptions(interceptor),
            }));

            var result = await client.ExecuteSqlAsync("SELECT value FROM bench WHERE id = 1;", ct);

            Assert.Null(result.Error);
            Assert.NotNull(result.Rows);
            Assert.Equal(10L, result.Rows![0][0]);
            Assert.True(
                interceptor.GetReadSourceCount(PageReadSource.MemoryMappedMainFile) > 0,
                "Expected the first direct open to honor UseMemoryMappedReads from DirectDatabaseOptions.");
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task DirectDatabaseOptions_AreUsedAfterReleaseCachedDatabaseAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();
        var interceptor = new TrackingPageOperationInterceptor();

        try
        {
            await SeedSqlDatabaseAsync(dbPath, ct);

            await using var client = Assert.IsType<CSharpDbClient>(CSharpDbClient.Create(new CSharpDbClientOptions
            {
                DataSource = dbPath,
                DirectDatabaseOptions = CreateDirectDatabaseOptions(interceptor),
            }));

            var firstResult = await client.ExecuteSqlAsync("SELECT value FROM bench WHERE id = 1;", ct);
            Assert.Null(firstResult.Error);

            interceptor.Reset();
            await client.ReleaseCachedDatabaseAsync(ct);

            var reopenedResult = await client.ExecuteSqlAsync("SELECT value FROM bench WHERE id = 1;", ct);

            Assert.Null(reopenedResult.Error);
            Assert.NotNull(reopenedResult.Rows);
            Assert.Equal(10L, reopenedResult.Rows![0][0]);
            Assert.True(
                interceptor.GetReadSourceCount(PageReadSource.MemoryMappedMainFile) > 0,
                "Expected the reopened direct database handle to reuse DirectDatabaseOptions.");
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task DirectDatabaseOptions_AreUsedAfterRestoreTriggeredReopen()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();
        string backupPath = Path.Combine(Path.GetTempPath(), $"csharpdb_client_direct_restore_{Guid.NewGuid():N}.db");
        var interceptor = new TrackingPageOperationInterceptor();

        try
        {
            await SeedSqlDatabaseAsync(dbPath, ct);

            await using var client = Assert.IsType<CSharpDbClient>(CSharpDbClient.Create(new CSharpDbClientOptions
            {
                DataSource = dbPath,
                DirectDatabaseOptions = CreateDirectDatabaseOptions(interceptor),
            }));

            var backup = await client.BackupAsync(new BackupRequest
            {
                DestinationPath = backupPath,
                WithManifest = false,
            }, ct);

            Assert.Equal(Path.GetFullPath(backupPath), backup.DestinationPath);

            var mutate = await client.ExecuteSqlAsync("INSERT INTO bench VALUES (2, 20, 'after');", ct);
            Assert.Null(mutate.Error);

            interceptor.Reset();

            var restore = await client.RestoreAsync(new RestoreRequest
            {
                SourcePath = backupPath,
            }, ct);

            Assert.False(restore.ValidateOnly);

            var restoredRows = await client.ExecuteSqlAsync("SELECT id, value FROM bench ORDER BY id;", ct);

            Assert.Null(restoredRows.Error);
            Assert.NotNull(restoredRows.Rows);
            var row = Assert.Single(restoredRows.Rows!);
            Assert.Equal(1L, row[0]);
            Assert.Equal(10L, row[1]);
            Assert.True(
                interceptor.GetReadSourceCount(PageReadSource.MemoryMappedMainFile) > 0,
                "Expected the post-restore reopen to honor DirectDatabaseOptions.");
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
            DeleteIfExists(backupPath);
            DeleteIfExists(backupPath + ".wal");
            DeleteIfExists(backupPath + ".manifest.json");
        }
    }

    private static DatabaseOptions CreateDirectDatabaseOptions(IPageOperationInterceptor interceptor)
    {
        return new DatabaseOptions
        {
            StorageEngineOptions = new CSharpDB.Storage.StorageEngine.StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    Interceptors = [interceptor],
                    UseMemoryMappedReads = true,
                    MaxCachedPages = 16,
                },
            },
        };
    }

    private static async Task SeedSqlDatabaseAsync(string dbPath, CancellationToken ct)
    {
        await using var db = await Database.OpenAsync(dbPath, ct);
        await db.ExecuteAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, name TEXT);", ct);
        await db.ExecuteAsync("INSERT INTO bench VALUES (1, 10, 'before');", ct);
        await db.CheckpointAsync(ct);
    }

    private static string NewTempDbPath()
        => Path.Combine(Path.GetTempPath(), $"csharpdb_client_direct_{Guid.NewGuid():N}.db");

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private sealed class TrackingPageOperationInterceptor : IPageOperationInterceptor
    {
        private readonly ConcurrentDictionary<PageReadSource, int> _readSources = new();

        public int GetReadSourceCount(PageReadSource source)
            => _readSources.TryGetValue(source, out int count) ? count : 0;

        public void Reset()
            => _readSources.Clear();

        public ValueTask OnBeforeReadAsync(uint pageId, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnAfterReadAsync(uint pageId, PageReadSource source, CancellationToken ct = default)
        {
            _readSources.AddOrUpdate(source, 1, static (_, count) => count + 1);
            return ValueTask.CompletedTask;
        }

        public ValueTask OnBeforeWriteAsync(uint pageId, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnAfterWriteAsync(uint pageId, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCommitStartAsync(int dirtyPageCount, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCommitEndAsync(int dirtyPageCount, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCheckpointStartAsync(int committedFrameCount, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnCheckpointEndAsync(int committedFrameCount, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnRecoveryStartAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask OnRecoveryEndAsync(bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
    }
}
