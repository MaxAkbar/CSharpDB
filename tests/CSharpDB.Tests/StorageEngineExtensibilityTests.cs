using System.Collections.Concurrent;
using CSharpDB.Engine;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Integrity;
using CSharpDB.Storage.Indexing;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Tests;

public sealed class StorageEngineExtensibilityTests
{
    [Fact]
    public async Task CustomChecksumProvider_IsUsedByWal()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();
        var checksum = new CountingChecksumProvider();

        try
        {
            var options = new DatabaseOptions
            {
                StorageEngineOptions = new StorageEngineOptions
                {
                    ChecksumProvider = checksum,
                }
            };

            await using (var db = await Database.OpenAsync(dbPath, options, ct))
            {
                await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)", ct);
                await db.ExecuteAsync("INSERT INTO t VALUES (1, 10)", ct);
            }

            Assert.True(checksum.ComputeCount > 0);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task PageOperationInterceptor_ReceivesReadWriteCommitCheckpointAndRecoveryEvents()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();
        var interceptor = new RecordingPageOperationInterceptor();

        try
        {
            var options = new DatabaseOptions
            {
                StorageEngineOptions = new StorageEngineOptions
                {
                    PagerOptions = new PagerOptions
                    {
                        // Force frequent checkpoints so checkpoint hooks are exercised.
                        CheckpointPolicy = new FrameCountCheckpointPolicy(1),
                        Interceptors = new[] { interceptor },
                    }
                }
            };

            await using (var db = await Database.OpenAsync(dbPath, options, ct))
            {
                await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)", ct);
                await db.ExecuteAsync("INSERT INTO t VALUES (1, 10)", ct);
                await using var result = await db.ExecuteAsync("SELECT * FROM t WHERE id = 1", ct);
                _ = await result.ToListAsync(ct);
            }

            // Re-open to exercise recovery hooks on existing files.
            await using (var db = await Database.OpenAsync(dbPath, options, ct))
            {
                await using var result = await db.ExecuteAsync("SELECT * FROM t WHERE id = 1", ct);
                _ = await result.ToListAsync(ct);
            }

            Assert.True(interceptor.BeforeReadCount > 0);
            Assert.True(interceptor.AfterReadCount > 0);
            Assert.True(interceptor.BeforeWriteCount > 0);
            Assert.True(interceptor.AfterWriteCount > 0);
            Assert.True(interceptor.CommitStartCount > 0);
            Assert.True(interceptor.CommitEndCount > 0);
            Assert.True(interceptor.CheckpointStartCount > 0);
            Assert.True(interceptor.CheckpointEndCount > 0);
            Assert.True(interceptor.RecoveryStartCount > 0);
            Assert.True(interceptor.RecoveryEndCount > 0);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public void WalSizeCheckpointPolicy_UsesEstimatedWalBytesThreshold()
    {
        var policy = new WalSizeCheckpointPolicy(8 * 1024);

        Assert.False(policy.ShouldCheckpoint(new PagerCheckpointContext(
            CommittedFrameCount: 2,
            ActiveReaderCount: 0,
            EstimatedWalBytes: 7 * 1024)));

        Assert.True(policy.ShouldCheckpoint(new PagerCheckpointContext(
            CommittedFrameCount: 2,
            ActiveReaderCount: 0,
            EstimatedWalBytes: 8 * 1024)));

        Assert.False(policy.ShouldCheckpoint(new PagerCheckpointContext(
            CommittedFrameCount: 2,
            ActiveReaderCount: 1,
            EstimatedWalBytes: 16 * 1024)));
    }

    [Fact]
    public async Task CachingIndexStore_CachesFindResultsAndInvalidatesOnDelete()
    {
        var ct = TestContext.Current.CancellationToken;
        var inner = new CountingIndexStore();
        await inner.InsertAsync(7, BitConverter.GetBytes(123L), ct);

        var store = new CachingIndexStore(inner, capacity: 8);

        var first = await store.FindAsync(7, ct);
        var second = await store.FindAsync(7, ct);

        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.Equal(1, inner.FindCallCount);

        await store.DeleteAsync(7, ct);
        var afterDelete = await store.FindAsync(7, ct);

        Assert.Null(afterDelete);
        Assert.Equal(2, inner.FindCallCount);
    }

    [Fact]
    public void DatabaseOptions_ConfigureStorageEngine_RegistersCachingIndexProvider()
    {
        var options = new DatabaseOptions()
            .ConfigureStorageEngine(builder => builder.UseCachingBTreeIndexes(findCacheCapacity: 32));

        Assert.IsType<CachingBTreeIndexProvider>(options.StorageEngineOptions.IndexProvider);
    }

    private static string NewTempDbPath() =>
        Path.Combine(Path.GetTempPath(), $"csharpdb_ext_test_{Guid.NewGuid():N}.db");

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private sealed class CountingChecksumProvider : IPageChecksumProvider
    {
        private int _computeCount;
        private readonly AdditiveChecksumProvider _inner = new();

        public int ComputeCount => Volatile.Read(ref _computeCount);

        public uint Compute(ReadOnlySpan<byte> data)
        {
            Interlocked.Increment(ref _computeCount);
            return _inner.Compute(data);
        }
    }

    private sealed class RecordingPageOperationInterceptor : IPageOperationInterceptor
    {
        private readonly ConcurrentDictionary<PageReadSource, int> _readSources = new();

        public int BeforeReadCount;
        public int AfterReadCount;
        public int BeforeWriteCount;
        public int AfterWriteCount;
        public int CommitStartCount;
        public int CommitEndCount;
        public int CheckpointStartCount;
        public int CheckpointEndCount;
        public int RecoveryStartCount;
        public int RecoveryEndCount;

        public ValueTask OnBeforeReadAsync(uint pageId, CancellationToken ct = default)
        {
            Interlocked.Increment(ref BeforeReadCount);
            return ValueTask.CompletedTask;
        }

        public ValueTask OnAfterReadAsync(uint pageId, PageReadSource source, CancellationToken ct = default)
        {
            Interlocked.Increment(ref AfterReadCount);
            _readSources.AddOrUpdate(source, 1, static (_, v) => v + 1);
            return ValueTask.CompletedTask;
        }

        public ValueTask OnBeforeWriteAsync(uint pageId, CancellationToken ct = default)
        {
            Interlocked.Increment(ref BeforeWriteCount);
            return ValueTask.CompletedTask;
        }

        public ValueTask OnAfterWriteAsync(uint pageId, bool succeeded, CancellationToken ct = default)
        {
            Interlocked.Increment(ref AfterWriteCount);
            return ValueTask.CompletedTask;
        }

        public ValueTask OnCommitStartAsync(int dirtyPageCount, CancellationToken ct = default)
        {
            Interlocked.Increment(ref CommitStartCount);
            return ValueTask.CompletedTask;
        }

        public ValueTask OnCommitEndAsync(int dirtyPageCount, bool succeeded, CancellationToken ct = default)
        {
            Interlocked.Increment(ref CommitEndCount);
            return ValueTask.CompletedTask;
        }

        public ValueTask OnCheckpointStartAsync(int committedFrameCount, CancellationToken ct = default)
        {
            Interlocked.Increment(ref CheckpointStartCount);
            return ValueTask.CompletedTask;
        }

        public ValueTask OnCheckpointEndAsync(int committedFrameCount, bool succeeded, CancellationToken ct = default)
        {
            Interlocked.Increment(ref CheckpointEndCount);
            return ValueTask.CompletedTask;
        }

        public ValueTask OnRecoveryStartAsync(CancellationToken ct = default)
        {
            Interlocked.Increment(ref RecoveryStartCount);
            return ValueTask.CompletedTask;
        }

        public ValueTask OnRecoveryEndAsync(bool succeeded, CancellationToken ct = default)
        {
            Interlocked.Increment(ref RecoveryEndCount);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class CountingIndexStore : IIndexStore
    {
        private readonly Dictionary<long, byte[]> _data = new();
        private int _findCallCount;

        public int FindCallCount => Volatile.Read(ref _findCallCount);

        public uint RootPageId => 1;

        public ValueTask<byte[]?> FindAsync(long key, CancellationToken ct = default)
        {
            Interlocked.Increment(ref _findCallCount);
            _data.TryGetValue(key, out var payload);
            return ValueTask.FromResult<byte[]?>(payload);
        }

        public ValueTask InsertAsync(long key, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
        {
            _data[key] = payload.ToArray();
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> DeleteAsync(long key, CancellationToken ct = default)
        {
            bool removed = _data.Remove(key);
            return ValueTask.FromResult(removed);
        }

        public IIndexCursor CreateCursor(IndexScanRange range) => throw new NotSupportedException();
    }
}
