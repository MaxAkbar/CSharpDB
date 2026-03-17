using CSharpDB.Engine;
using CSharpDB.Storage.Caching;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;
using System.Collections.Concurrent;

namespace CSharpDB.Tests;

public sealed class HybridDatabaseTests : IDisposable
{
    private readonly List<string> _paths = new();
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private sealed record UserDoc(string Name, int Age);

    [Fact]
    public async Task OpenHybridAsync_DefaultMode_PersistsCommittedSqlStateInCrashImage()
    {
        string filePath = NewTempDbPath();
        string crashImagePath = NewTempDbPath();

        await using (var db = await Database.OpenHybridAsync(filePath, Ct))
        {
            await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", Ct);
            await db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice')", Ct);

            CopyDatabaseFiles(filePath, crashImagePath);
        }

        await using var reopened = await Database.OpenAsync(crashImagePath, Ct);
        await using var result = await reopened.ExecuteAsync("SELECT name FROM users WHERE id = 1", Ct);
        var rows = await result.ToListAsync(Ct);

        var row = Assert.Single(rows);
        Assert.Equal("Alice", row[0].AsText);
    }

    [Fact]
    public async Task OpenHybridAsync_DefaultMode_PersistsCommittedCollectionStateInCrashImage()
    {
        string filePath = NewTempDbPath();
        string crashImagePath = NewTempDbPath();

        await using (var db = await Database.OpenHybridAsync(filePath, Ct))
        {
            var collection = await db.GetCollectionAsync<UserDoc>("users", Ct);
            await collection.PutAsync("user:1", new UserDoc("Committed", 42), Ct);

            CopyDatabaseFiles(filePath, crashImagePath);
        }

        await using var reopened = await Database.OpenAsync(crashImagePath, Ct);
        var reopenedCollection = await reopened.GetCollectionAsync<UserDoc>("users", Ct);
        var document = await reopenedCollection.GetAsync("user:1", Ct);

        Assert.NotNull(document);
        Assert.Equal("Committed", document!.Name);
    }

    [Fact]
    public async Task OpenHybridAsync_DefaultMode_PersistsCommittedStateOnDispose()
    {
        string filePath = NewTempDbPath();

        await using (var db = await Database.OpenHybridAsync(filePath, Ct))
        {
            await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", Ct);
            await db.ExecuteAsync("INSERT INTO users VALUES (1, 'DisposePersisted')", Ct);
        }

        await using var reopened = await Database.OpenAsync(filePath, Ct);
        await using var result = await reopened.ExecuteAsync("SELECT name FROM users WHERE id = 1", Ct);
        var rows = await result.ToListAsync(Ct);

        var row = Assert.Single(rows);
        Assert.Equal("DisposePersisted", row[0].AsText);
    }

    [Fact]
    public async Task OpenHybridAsync_SnapshotModeWithCommitTrigger_PersistsBeforeDispose()
    {
        string filePath = NewTempDbPath();
        var hybridOptions = new HybridDatabaseOptions
        {
            PersistenceMode = HybridPersistenceMode.Snapshot,
            PersistenceTriggers = HybridPersistenceTriggers.Commit,
        };

        await using var db = await Database.OpenHybridAsync(filePath, new DatabaseOptions(), hybridOptions, Ct);
        await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (1, 'Committed')", Ct);

        await using var reopened = await Database.OpenAsync(filePath, Ct);
        await using var result = await reopened.ExecuteAsync("SELECT name FROM users WHERE id = 1", Ct);
        var rows = await result.ToListAsync(Ct);

        var row = Assert.Single(rows);
        Assert.Equal("Committed", row[0].AsText);
    }

    [Fact]
    public async Task OpenHybridAsync_SnapshotModeWithManualPersistence_DoesNotWriteBackingFileUntilExplicitSave()
    {
        string filePath = NewTempDbPath();
        var hybridOptions = new HybridDatabaseOptions
        {
            PersistenceMode = HybridPersistenceMode.Snapshot,
            PersistenceTriggers = HybridPersistenceTriggers.None,
        };

        await using var db = await Database.OpenHybridAsync(filePath, new DatabaseOptions(), hybridOptions, Ct);
        await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (1, 'Manual')", Ct);

        Assert.False(File.Exists(filePath));

        await db.SaveToFileAsync(filePath, Ct);

        await using var reopened = await Database.OpenAsync(filePath, Ct);
        await using var result = await reopened.ExecuteAsync("SELECT name FROM users WHERE id = 1", Ct);
        var rows = await result.ToListAsync(Ct);

        var row = Assert.Single(rows);
        Assert.Equal("Manual", row[0].AsText);
    }

    [Fact]
    public async Task OpenHybridAsync_ReadsExistingBackingFileState()
    {
        string filePath = NewTempDbPath();

        await using (var disk = await Database.OpenAsync(filePath, Ct))
        {
            await disk.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", Ct);
            await disk.ExecuteAsync("INSERT INTO users VALUES (1, 'Seeded')", Ct);
        }

        await using var hybrid = await Database.OpenHybridAsync(filePath, Ct);
        await using var result = await hybrid.ExecuteAsync("SELECT name FROM users WHERE id = 1", Ct);
        var rows = await result.ToListAsync(Ct);

        var row = Assert.Single(rows);
        Assert.Equal("Seeded", row[0].AsText);
    }

    [Fact]
    public async Task OpenHybridAsync_DefaultMode_RetainsOwnedPagesAcrossCheckpoint()
    {
        string filePath = NewTempDbPath();
        var interceptor = new RecordingPageOperationInterceptor();
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    CheckpointPolicy = new FrameCountCheckpointPolicy(1),
                    Interceptors = new[] { interceptor },
                }
            }
        };

        await using var db = await Database.OpenHybridAsync(filePath, options, new HybridDatabaseOptions(), Ct);
        await db.ExecuteAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER)", Ct);

        await db.BeginTransactionAsync(Ct);
        try
        {
            for (int id = 1; id <= 2_000; id++)
                await db.ExecuteAsync($"INSERT INTO bench VALUES ({id}, {id * 10L})", Ct);

            await db.CommitAsync(Ct);
        }
        catch
        {
            await db.RollbackAsync(Ct);
            throw;
        }

        await using (var warm = await db.ExecuteAsync("SELECT value FROM bench WHERE id = 1", Ct))
        {
            var rows = await warm.ToListAsync(Ct);
            var row = Assert.Single(rows);
            Assert.Equal(10L, row[0].AsInteger);
        }

        await db.ExecuteAsync("INSERT INTO bench VALUES (2000000, 20000000)", Ct);

        interceptor.ResetCounts();

        await using var cachedRead = await db.ExecuteAsync("SELECT value FROM bench WHERE id = 1", Ct);
        var cachedRows = await cachedRead.ToListAsync(Ct);
        var cachedRow = Assert.Single(cachedRows);

        Assert.Equal(10L, cachedRow[0].AsInteger);
        Assert.Equal(0, interceptor.GetReadSourceCount(PageReadSource.StorageDevice));
        Assert.True(interceptor.GetReadSourceCount(PageReadSource.Cache) > 0);
    }

    [Fact]
    public async Task OpenHybridAsync_HotTableNames_RejectSnapshotMode()
    {
        string filePath = NewTempDbPath();
        var hybridOptions = new HybridDatabaseOptions
        {
            PersistenceMode = HybridPersistenceMode.Snapshot,
            HotTableNames = new[] { "bench" },
        };

        var ex = await Assert.ThrowsAsync<ArgumentException>(
            () => Database.OpenHybridAsync(filePath, new DatabaseOptions(), hybridOptions, Ct).AsTask());

        Assert.Contains("incremental-durable", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task OpenHybridAsync_HotTableNames_RejectBoundedCache()
    {
        string filePath = NewTempDbPath();
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    MaxCachedPages = 16,
                }
            }
        };

        var hybridOptions = new HybridDatabaseOptions
        {
            HotTableNames = new[] { "bench" },
        };

        var ex = await Assert.ThrowsAsync<ArgumentException>(
            () => Database.OpenHybridAsync(filePath, options, hybridOptions, Ct).AsTask());

        Assert.Contains("unbounded pager cache", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task OpenHybridAsync_HotTableNames_RejectCustomPageCacheFactory()
    {
        string filePath = NewTempDbPath();
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    PageCacheFactory = static () => new DictionaryPageCache(),
                }
            }
        };

        var hybridOptions = new HybridDatabaseOptions
        {
            HotTableNames = new[] { "bench" },
        };

        var ex = await Assert.ThrowsAsync<ArgumentException>(
            () => Database.OpenHybridAsync(filePath, options, hybridOptions, Ct).AsTask());

        Assert.Contains("default pager cache", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task OpenHybridAsync_HotTableNames_MissingTableFailsFast()
    {
        string filePath = NewTempDbPath();
        var hybridOptions = new HybridDatabaseOptions
        {
            HotTableNames = new[] { "missing" },
        };

        var ex = await Assert.ThrowsAsync<CSharpDbException>(
            () => Database.OpenHybridAsync(filePath, new DatabaseOptions(), hybridOptions, Ct).AsTask());

        Assert.Equal(ErrorCode.TableNotFound, ex.Code);
        Assert.Contains("missing", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OpenHybridAsync_HotCollectionNames_MissingCollectionFailsFast()
    {
        string filePath = NewTempDbPath();
        var hybridOptions = new HybridDatabaseOptions
        {
            HotCollectionNames = new[] { "missing_docs" },
        };

        var ex = await Assert.ThrowsAsync<CSharpDbException>(
            () => Database.OpenHybridAsync(filePath, new DatabaseOptions(), hybridOptions, Ct).AsTask());

        Assert.Equal(ErrorCode.TableNotFound, ex.Code);
        Assert.Contains("missing_docs", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OpenHybridAsync_HotTableNames_WarmSqlTableIntoCache()
    {
        string filePath = await CreateSeededBenchTableAsync(NewTempDbPath());
        var interceptor = new RecordingPageOperationInterceptor();
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    Interceptors = new[] { interceptor },
                }
            }
        };

        await using var db = await Database.OpenHybridAsync(
            filePath,
            options,
            new HybridDatabaseOptions
            {
                HotTableNames = new[] { "bench" },
            },
            Ct);

        db.PreferSyncPointLookups = false;
        interceptor.ResetCounts();

        await using var result = await db.ExecuteAsync("SELECT value FROM bench WHERE id = 1", Ct);
        var rows = await result.ToListAsync(Ct);
        var row = Assert.Single(rows);

        Assert.Equal(10L, row[0].AsInteger);
        Assert.Equal(0, interceptor.GetReadSourceCount(PageReadSource.StorageDevice));
        Assert.True(interceptor.GetReadSourceCount(PageReadSource.Cache) > 0);
    }

    [Fact]
    public async Task OpenHybridAsync_HotCollectionNames_WarmCollectionIntoCache()
    {
        string filePath = await CreateSeededBenchCollectionAsync(NewTempDbPath());
        var interceptor = new RecordingPageOperationInterceptor();
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    Interceptors = new[] { interceptor },
                }
            }
        };

        await using var db = await Database.OpenHybridAsync(
            filePath,
            options,
            new HybridDatabaseOptions
            {
                HotCollectionNames = new[] { "users" },
            },
            Ct);

        var collection = await db.GetCollectionAsync<UserDoc>("users", Ct);
        interceptor.ResetCounts();

        var document = await collection.GetAsync("user:1", Ct);

        Assert.NotNull(document);
        Assert.Equal("User_1", document!.Name);
        Assert.Equal(0, interceptor.GetReadSourceCount(PageReadSource.StorageDevice));
        Assert.True(interceptor.GetReadSourceCount(PageReadSource.Cache) > 0);
    }

    [Fact]
    public async Task OpenHybridAsync_HotTableNames_RetainWarmedPagesAcrossCheckpoint()
    {
        string filePath = await CreateSeededBenchTableAsync(NewTempDbPath(), rowCount: 2_000);
        var interceptor = new RecordingPageOperationInterceptor();
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    CheckpointPolicy = new FrameCountCheckpointPolicy(1),
                    Interceptors = new[] { interceptor },
                }
            }
        };

        await using var db = await Database.OpenHybridAsync(
            filePath,
            options,
            new HybridDatabaseOptions
            {
                HotTableNames = new[] { "bench" },
            },
            Ct);

        db.PreferSyncPointLookups = false;
        await db.ExecuteAsync("INSERT INTO bench VALUES (2000000, 20000000)", Ct);

        interceptor.ResetCounts();

        await using var reread = await db.ExecuteAsync("SELECT value FROM bench WHERE id = 1", Ct);
        var rereadRows = await reread.ToListAsync(Ct);
        var rereadRow = Assert.Single(rereadRows);

        Assert.Equal(10L, rereadRow[0].AsInteger);
        Assert.Equal(0, interceptor.GetReadSourceCount(PageReadSource.StorageDevice));
        Assert.True(interceptor.GetReadSourceCount(PageReadSource.Cache) > 0);
    }

    [Fact]
    public async Task OpenAsync_FileBacked_RefetchesOwnedPagesAfterCheckpoint()
    {
        string filePath = NewTempDbPath();
        var interceptor = new RecordingPageOperationInterceptor();
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    CheckpointPolicy = new FrameCountCheckpointPolicy(1),
                    Interceptors = new[] { interceptor },
                }
            }
        };

        await using var db = await Database.OpenAsync(filePath, options, Ct);
        await db.ExecuteAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER)", Ct);

        await db.BeginTransactionAsync(Ct);
        try
        {
            for (int id = 1; id <= 2_000; id++)
                await db.ExecuteAsync($"INSERT INTO bench VALUES ({id}, {id * 10L})", Ct);

            await db.CommitAsync(Ct);
        }
        catch
        {
            await db.RollbackAsync(Ct);
            throw;
        }

        await using (var warm = await db.ExecuteAsync("SELECT value FROM bench WHERE id = 1", Ct))
        {
            var rows = await warm.ToListAsync(Ct);
            var row = Assert.Single(rows);
            Assert.Equal(10L, row[0].AsInteger);
        }

        await db.ExecuteAsync("INSERT INTO bench VALUES (2000000, 20000000)", Ct);

        interceptor.ResetCounts();

        await using var reread = await db.ExecuteAsync("SELECT value FROM bench WHERE id = 1", Ct);
        var rereadRows = await reread.ToListAsync(Ct);
        var rereadRow = Assert.Single(rereadRows);

        Assert.Equal(10L, rereadRow[0].AsInteger);
        Assert.True(
            interceptor.GetReadSourceCount(PageReadSource.StorageDevice) > 0,
            "Expected plain file-backed mode to refetch from the storage device after checkpoint.");
    }

    public void Dispose()
    {
        foreach (string path in _paths)
        {
            TryDelete(path);
            TryDelete(path + ".wal");
        }
    }

    private string NewTempDbPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_hybrid_test_{Guid.NewGuid():N}.db");
        _paths.Add(path);
        return path;
    }

    private static void CopyDatabaseFiles(string sourcePath, string destinationPath)
    {
        File.WriteAllBytes(destinationPath, File.ReadAllBytes(sourcePath));

        string sourceWalPath = sourcePath + ".wal";
        if (File.Exists(sourceWalPath))
            File.WriteAllBytes(destinationPath + ".wal", File.ReadAllBytes(sourceWalPath));
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

    private static async Task<string> CreateSeededBenchTableAsync(string filePath, int rowCount = 128)
    {
        await using var db = await Database.OpenAsync(filePath, Ct);
        await db.ExecuteAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER)", Ct);

        await db.BeginTransactionAsync(Ct);
        try
        {
            for (int id = 1; id <= rowCount; id++)
                await db.ExecuteAsync($"INSERT INTO bench VALUES ({id}, {id * 10L})", Ct);

            await db.CommitAsync(Ct);
        }
        catch
        {
            await db.RollbackAsync(Ct);
            throw;
        }

        return filePath;
    }

    private static async Task<string> CreateSeededBenchCollectionAsync(string filePath, int rowCount = 128)
    {
        await using var db = await Database.OpenAsync(filePath, Ct);
        var collection = await db.GetCollectionAsync<UserDoc>("users", Ct);

        await db.BeginTransactionAsync(Ct);
        try
        {
            for (int id = 1; id <= rowCount; id++)
                await collection.PutAsync($"user:{id}", new UserDoc($"User_{id}", 20 + id), Ct);

            await db.CommitAsync(Ct);
        }
        catch
        {
            await db.RollbackAsync(Ct);
            throw;
        }

        return filePath;
    }

    private sealed class RecordingPageOperationInterceptor : IPageOperationInterceptor
    {
        private readonly ConcurrentDictionary<PageReadSource, int> _readSources = new();

        public ValueTask OnBeforeReadAsync(uint pageId, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnAfterReadAsync(uint pageId, PageReadSource source, CancellationToken ct = default)
        {
            _readSources.AddOrUpdate(source, 1, static (_, current) => current + 1);
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

        public void ResetCounts() => _readSources.Clear();

        public int GetReadSourceCount(PageReadSource source)
            => _readSources.TryGetValue(source, out int count) ? count : 0;
    }
}
