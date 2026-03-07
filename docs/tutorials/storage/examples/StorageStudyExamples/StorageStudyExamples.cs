// ============================================================================
// CSharpDB.Storage — Study & Example Code
// ============================================================================
//
// This file contains standalone examples demonstrating every extensibility
// point in CSharpDB.Storage. Each example is a self-contained class that
// you can copy into your project, modify, and experiment with.
//
// TABLE OF CONTENTS:
//   1. Custom Page Cache (IPageCache)
//   2. Custom Checkpoint Policies (ICheckpointPolicy)
//   3. Custom Page Operation Interceptor (IPageOperationInterceptor)
//   4. Custom Checksum Provider (IPageChecksumProvider)
//   5. Custom Index Provider (IIndexProvider / IIndexStore)
//   6. Custom Storage Device (IStorageDevice)
//   7. Custom Serializer Provider (ISerializerProvider)
//   8. Configuration & Composition Examples
//   9. Diagnostic / Monitoring Scenarios
//  10. Testing Scenarios
//
// NOTE: This file is for reading and study. To run these examples, copy the
// relevant classes into your project that references CSharpDB.Storage and
// CSharpDB.Engine NuGet packages or project references.
// ============================================================================

using System.Collections.Concurrent;
using System.Diagnostics;
using CSharpDB.Engine;
using CSharpDB.Storage.Caching;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Indexing;
using CSharpDB.Storage.Integrity;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Serialization;
using CSharpDB.Storage.StorageEngine;
using CSharpDB.Storage.Time;

namespace StorageStudyExamples;

// ============================================================================
// 1. CUSTOM PAGE CACHE (IPageCache)
// ============================================================================
//
// The page cache controls how pages are stored in memory between disk reads.
// The default DictionaryPageCache is unbounded. LruPageCache adds eviction.
// You might want a custom cache for:
//   - Metrics collection (hit/miss rates)
//   - Priority-based eviction (pin frequently-accessed pages)
//   - Memory pressure-aware eviction
// ============================================================================

/// <summary>
/// A page cache that wraps another cache and tracks hit/miss statistics.
/// This demonstrates the Decorator pattern over IPageCache.
///
/// USAGE:
///   var metrics = new MetricsPageCache(maxPages: 500);
///   var options = new PagerOptions { PageCacheFactory = () => metrics };
///
///   // After workload completes:
///   Console.WriteLine($"Hit rate: {metrics.HitRate:P2}");
/// </summary>
public sealed class MetricsPageCache : IPageCache
{
    private readonly IPageCache _inner;
    private long _hits;
    private long _misses;
    private long _evictions;

    public MetricsPageCache(int maxPages = 0)
    {
        // Use LRU if bounded, dictionary if unbounded
        _inner = maxPages > 0
            ? new LruPageCache(maxPages)
            : new DictionaryPageCache();
    }

    public long Hits => Interlocked.Read(ref _hits);
    public long Misses => Interlocked.Read(ref _misses);
    public long Evictions => Interlocked.Read(ref _evictions);
    public double HitRate => Hits + Misses == 0 ? 0 : (double)Hits / (Hits + Misses);

    public bool TryGet(uint pageId, out byte[] page)
    {
        bool found = _inner.TryGet(pageId, out page);
        if (found)
            Interlocked.Increment(ref _hits);
        else
            Interlocked.Increment(ref _misses);
        return found;
    }

    public void Set(uint pageId, byte[] page) => _inner.Set(pageId, page);
    public bool Contains(uint pageId) => _inner.Contains(pageId);

    public bool Remove(uint pageId)
    {
        bool removed = _inner.Remove(pageId);
        if (removed) Interlocked.Increment(ref _evictions);
        return removed;
    }

    public void Clear() => _inner.Clear();

    public void PrintStats()
    {
        Console.WriteLine($"Page Cache Stats:");
        Console.WriteLine($"  Hits:      {Hits:N0}");
        Console.WriteLine($"  Misses:    {Misses:N0}");
        Console.WriteLine($"  Hit Rate:  {HitRate:P2}");
        Console.WriteLine($"  Evictions: {Evictions:N0}");
    }
}

// ============================================================================
// 2. CUSTOM CHECKPOINT POLICIES (ICheckpointPolicy)
// ============================================================================
//
// Checkpoint policies control WHEN the WAL is flushed back to the main
// database file. This bounds WAL growth and affects read performance.
//
// Built-in policies:
//   - FrameCountCheckpointPolicy(threshold)  — after N frames
//   - TimeIntervalCheckpointPolicy(interval)  — every N minutes
//   - WalSizeCheckpointPolicy(maxBytes)       — when WAL exceeds N bytes
//   - AnyCheckpointPolicy(policies...)        — composite OR
// ============================================================================

/// <summary>
/// A checkpoint policy that requires both a minimum frame count and
/// a minimum WAL size before checkpointing.
///
/// This demonstrates a policy shape that is not available in the built-ins:
/// an AND condition across multiple signals.
///
/// USAGE:
///   var options = new PagerOptions
///   {
///       CheckpointPolicy = new MinFramesAndWalSizeCheckpointPolicy(
///           minFrames: 500,
///           minWalBytes: 8 * 1024 * 1024)
///   };
/// </summary>
public sealed class MinFramesAndWalSizeCheckpointPolicy : ICheckpointPolicy
{
    private readonly int _minFrames;
    private readonly long _minWalBytes;

    /// <param name="minFrames">Minimum frame count before any checkpoint</param>
    /// <param name="minWalBytes">Minimum WAL size before any checkpoint</param>
    public MinFramesAndWalSizeCheckpointPolicy(int minFrames = 100, long minWalBytes = 4 * 1024 * 1024)
    {
        if (minFrames <= 0)
            throw new ArgumentOutOfRangeException(nameof(minFrames));
        if (minWalBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(minWalBytes));

        _minFrames = minFrames;
        _minWalBytes = minWalBytes;
    }

    public bool ShouldCheckpoint(PagerCheckpointContext context)
    {
        if (context.ActiveReaderCount != 0)
            return false;

        return context.CommittedFrameCount >= _minFrames &&
               context.EstimatedWalBytes >= _minWalBytes;
    }
}

/// <summary>
/// A checkpoint policy that never triggers automatic checkpoints.
/// Useful for batch import scenarios where you want to defer
/// checkpointing until the entire import is complete.
///
/// USAGE:
///   var options = new PagerOptions
///   {
///       CheckpointPolicy = NeverCheckpointPolicy.Instance
///   };
///
///   // After batch import, manually checkpoint:
///   await pager.CheckpointAsync(ct);
/// </summary>
public sealed class NeverCheckpointPolicy : ICheckpointPolicy
{
    public static readonly NeverCheckpointPolicy Instance = new();
    public bool ShouldCheckpoint(PagerCheckpointContext context) => false;
}

/// <summary>
/// Demonstrates combining multiple policies using AnyCheckpointPolicy.
/// This triggers when EITHER the frame count OR time interval is reached.
///
/// USAGE:
///   var policy = CheckpointPolicyExamples.CreateProductionPolicy();
///   var options = new PagerOptions { CheckpointPolicy = policy };
/// </summary>
public static class CheckpointPolicyExamples
{
    /// <summary>
    /// Production-grade policy: checkpoint every 500 frames OR every 5 minutes,
    /// whichever comes first.
    /// </summary>
    public static ICheckpointPolicy CreateProductionPolicy()
    {
        return new AnyCheckpointPolicy(
            new FrameCountCheckpointPolicy(500),
            new TimeIntervalCheckpointPolicy(TimeSpan.FromMinutes(5)));
    }

    /// <summary>
    /// High-throughput policy: checkpoint infrequently (large WAL),
    /// trading recovery time for write performance.
    /// </summary>
    public static ICheckpointPolicy CreateHighThroughputPolicy()
    {
        return new AnyCheckpointPolicy(
            new FrameCountCheckpointPolicy(10_000),
            new WalSizeCheckpointPolicy(256 * 1024 * 1024)); // 256 MB
    }

    /// <summary>
    /// Low-latency policy: checkpoint very frequently to keep WAL small,
    /// ensuring fast recovery and consistent read performance.
    /// </summary>
    public static ICheckpointPolicy CreateLowLatencyPolicy()
    {
        return new FrameCountCheckpointPolicy(50);
    }
}

// ============================================================================
// 3. CUSTOM PAGE OPERATION INTERCEPTOR (IPageOperationInterceptor)
// ============================================================================
//
// Interceptors observe lifecycle events without modifying behavior.
// They are the primary mechanism for diagnostics, testing, and monitoring.
//
// The 10 hooks are:
//   OnBeforeReadAsync / OnAfterReadAsync
//   OnBeforeWriteAsync / OnAfterWriteAsync
//   OnCommitStartAsync / OnCommitEndAsync
//   OnCheckpointStartAsync / OnCheckpointEndAsync
//   OnRecoveryStartAsync / OnRecoveryEndAsync
//
// PERFORMANCE: When no interceptors are configured, ALL hooks are
// completely skipped via the _hasInterceptor flag — zero overhead.
// ============================================================================

/// <summary>
/// Interceptor that measures page read latency and tracks read sources.
/// Use this to understand your workload's cache efficiency and I/O patterns.
///
/// USAGE:
///   var latency = new LatencyTrackingInterceptor();
///   var options = new PagerOptions
///   {
///       Interceptors = [latency]
///   };
///
///   // After workload:
///   latency.PrintReport();
/// </summary>
public sealed class LatencyTrackingInterceptor : IPageOperationInterceptor
{
    private readonly ConcurrentDictionary<uint, long> _readStartTicks = new();
    private readonly ConcurrentDictionary<PageReadSource, ConcurrentQueue<double>> _latencies = new();
    private long _totalReads;
    private long _totalWrites;
    private long _totalCommits;
    private long _totalCheckpoints;

    public ValueTask OnBeforeReadAsync(uint pageId, CancellationToken ct = default)
    {
        _readStartTicks[pageId] = Stopwatch.GetTimestamp();
        return ValueTask.CompletedTask;
    }

    public ValueTask OnAfterReadAsync(uint pageId, PageReadSource source, CancellationToken ct = default)
    {
        Interlocked.Increment(ref _totalReads);

        if (_readStartTicks.TryRemove(pageId, out long startTicks))
        {
            double elapsedMs = Stopwatch.GetElapsedTime(startTicks).TotalMilliseconds;
            _latencies.GetOrAdd(source, _ => new ConcurrentQueue<double>()).Enqueue(elapsedMs);
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask OnBeforeWriteAsync(uint pageId, CancellationToken ct = default) =>
        ValueTask.CompletedTask;

    public ValueTask OnAfterWriteAsync(uint pageId, bool succeeded, CancellationToken ct = default)
    {
        Interlocked.Increment(ref _totalWrites);
        return ValueTask.CompletedTask;
    }

    public ValueTask OnCommitStartAsync(int dirtyPageCount, CancellationToken ct = default) =>
        ValueTask.CompletedTask;

    public ValueTask OnCommitEndAsync(int dirtyPageCount, bool succeeded, CancellationToken ct = default)
    {
        Interlocked.Increment(ref _totalCommits);
        return ValueTask.CompletedTask;
    }

    public ValueTask OnCheckpointStartAsync(int committedFrameCount, CancellationToken ct = default) =>
        ValueTask.CompletedTask;

    public ValueTask OnCheckpointEndAsync(int committedFrameCount, bool succeeded, CancellationToken ct = default)
    {
        Interlocked.Increment(ref _totalCheckpoints);
        return ValueTask.CompletedTask;
    }

    public ValueTask OnRecoveryStartAsync(CancellationToken ct = default) =>
        ValueTask.CompletedTask;

    public ValueTask OnRecoveryEndAsync(bool succeeded, CancellationToken ct = default) =>
        ValueTask.CompletedTask;

    public void PrintReport()
    {
        Console.WriteLine("=== Page Operation Latency Report ===");
        Console.WriteLine($"Total reads: {Interlocked.Read(ref _totalReads):N0}");
        Console.WriteLine($"Total writes: {Interlocked.Read(ref _totalWrites):N0}");
        Console.WriteLine($"Total commits: {Interlocked.Read(ref _totalCommits):N0}");
        Console.WriteLine($"Total checkpoints: {Interlocked.Read(ref _totalCheckpoints):N0}");
        Console.WriteLine();

        foreach (var (source, latencies) in _latencies)
        {
            double[] ordered = latencies.OrderBy(x => x).ToArray();
            if (ordered.Length == 0) continue;

            int p50Index = ordered.Length / 2;
            int p99Index = Math.Min(ordered.Length - 1, (int)(ordered.Length * 0.99));

            Console.WriteLine($"  {source}:");
            Console.WriteLine($"    Count:  {ordered.Length:N0}");
            Console.WriteLine($"    P50:    {ordered[p50Index]:F3} ms");
            Console.WriteLine($"    P99:    {ordered[p99Index]:F3} ms");
            Console.WriteLine($"    Avg:    {ordered.Average():F3} ms");
        }
    }
}

/// <summary>
/// Interceptor for fault injection testing. Can simulate:
///   - Read failures (disk errors)
///   - Write failures (disk full)
///   - Slow I/O (latency injection)
///
/// USAGE:
///   var fault = new FaultInjectionInterceptor();
///   fault.FailWritesAfter = 1;  // Fail during the next commit after one page write
///
///   var options = new PagerOptions
///   {
///       Interceptors = [fault]
///   };
/// </summary>
public sealed class FaultInjectionInterceptor : IPageOperationInterceptor
{
    private int _writeCount;

    /// <summary>
    /// When set, all writes after this count will throw IOException.
    /// Simulates disk-full or hardware failure scenarios.
    /// </summary>
    public int? FailWritesAfter { get; set; }

    /// <summary>
    /// When set, all reads will be delayed by this amount.
    /// Simulates slow disk I/O.
    /// </summary>
    public TimeSpan? ReadDelay { get; set; }

    /// <summary>
    /// Specific page IDs that will fail on read.
    /// Simulates sector-level corruption.
    /// </summary>
    public HashSet<uint> CorruptPageIds { get; } = new();

    public void Reset()
    {
        Interlocked.Exchange(ref _writeCount, 0);
    }

    public async ValueTask OnBeforeReadAsync(uint pageId, CancellationToken ct = default)
    {
        if (ReadDelay.HasValue)
            await Task.Delay(ReadDelay.Value, ct);

        if (CorruptPageIds.Contains(pageId))
            throw new IOException($"Simulated read failure on page {pageId}");
    }

    public ValueTask OnAfterReadAsync(uint pageId, PageReadSource source, CancellationToken ct = default) =>
        ValueTask.CompletedTask;

    public ValueTask OnBeforeWriteAsync(uint pageId, CancellationToken ct = default)
    {
        if (FailWritesAfter.HasValue)
        {
            int count = Interlocked.Increment(ref _writeCount);
            if (count > FailWritesAfter.Value)
                throw new IOException($"Simulated write failure on page {pageId} (write #{count})");
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask OnAfterWriteAsync(uint pageId, bool succeeded, CancellationToken ct = default) =>
        ValueTask.CompletedTask;

    public ValueTask OnCommitStartAsync(int dirtyPageCount, CancellationToken ct = default) =>
        ValueTask.CompletedTask;

    public ValueTask OnCommitEndAsync(int dirtyPageCount, bool succeeded, CancellationToken ct = default) =>
        ValueTask.CompletedTask;

    public ValueTask OnCheckpointStartAsync(int committedFrameCount, CancellationToken ct = default) =>
        ValueTask.CompletedTask;

    public ValueTask OnCheckpointEndAsync(int committedFrameCount, bool succeeded, CancellationToken ct = default) =>
        ValueTask.CompletedTask;

    public ValueTask OnRecoveryStartAsync(CancellationToken ct = default) =>
        ValueTask.CompletedTask;

    public ValueTask OnRecoveryEndAsync(bool succeeded, CancellationToken ct = default) =>
        ValueTask.CompletedTask;
}

/// <summary>
/// Interceptor that logs all lifecycle events to the console.
/// Perfect for learning what happens during database operations.
///
/// USAGE:
///   var logger = new ConsoleLoggingInterceptor();
///   var options = new PagerOptions
///   {
///       Interceptors = [logger]
///   };
///
/// OUTPUT (during a write transaction):
///   [READ] Page 0 from Cache
///   [READ] Page 1 from StorageDevice
///   [COMMIT START] 3 dirty pages
///   [WRITE] Page 0
///   [WRITE] Page 1
///   [WRITE] Page 2
///   [COMMIT END] 3 pages, success=True
///   [CHECKPOINT START] 3 frames
///   [CHECKPOINT END] 3 frames, success=True
/// </summary>
public sealed class ConsoleLoggingInterceptor : IPageOperationInterceptor
{
    public ValueTask OnBeforeReadAsync(uint pageId, CancellationToken ct = default)
    {
        Console.WriteLine($"  [READ BEGIN] Page {pageId}");
        return ValueTask.CompletedTask;
    }

    public ValueTask OnAfterReadAsync(uint pageId, PageReadSource source, CancellationToken ct = default)
    {
        Console.WriteLine($"  [READ] Page {pageId} from {source}");
        return ValueTask.CompletedTask;
    }

    public ValueTask OnBeforeWriteAsync(uint pageId, CancellationToken ct = default)
    {
        Console.WriteLine($"  [WRITE] Page {pageId}");
        return ValueTask.CompletedTask;
    }

    public ValueTask OnAfterWriteAsync(uint pageId, bool succeeded, CancellationToken ct = default)
    {
        Console.WriteLine($"  [WRITE DONE] Page {pageId}, success={succeeded}");
        return ValueTask.CompletedTask;
    }

    public ValueTask OnCommitStartAsync(int dirtyPageCount, CancellationToken ct = default)
    {
        Console.WriteLine($"  [COMMIT START] {dirtyPageCount} dirty pages");
        return ValueTask.CompletedTask;
    }

    public ValueTask OnCommitEndAsync(int dirtyPageCount, bool succeeded, CancellationToken ct = default)
    {
        Console.WriteLine($"  [COMMIT END] {dirtyPageCount} pages, success={succeeded}");
        return ValueTask.CompletedTask;
    }

    public ValueTask OnCheckpointStartAsync(int committedFrameCount, CancellationToken ct = default)
    {
        Console.WriteLine($"  [CHECKPOINT START] {committedFrameCount} frames");
        return ValueTask.CompletedTask;
    }

    public ValueTask OnCheckpointEndAsync(int committedFrameCount, bool succeeded, CancellationToken ct = default)
    {
        Console.WriteLine($"  [CHECKPOINT END] {committedFrameCount} frames, success={succeeded}");
        return ValueTask.CompletedTask;
    }

    public ValueTask OnRecoveryStartAsync(CancellationToken ct = default)
    {
        Console.WriteLine($"  [RECOVERY START]");
        return ValueTask.CompletedTask;
    }

    public ValueTask OnRecoveryEndAsync(bool succeeded, CancellationToken ct = default)
    {
        Console.WriteLine($"  [RECOVERY END] success={succeeded}");
        return ValueTask.CompletedTask;
    }
}

// ============================================================================
// 4. CUSTOM CHECKSUM PROVIDER (IPageChecksumProvider)
// ============================================================================
//
// The checksum provider computes checksums for WAL frames to detect corruption.
// The default AdditiveChecksumProvider is fast (SIMD-optimized additive hash).
// You might want a stronger algorithm for production use.
// ============================================================================

/// <summary>
/// CRC32 checksum provider for stronger corruption detection.
/// This implementation is self-contained so readers can copy it
/// without adding extra package references.
///
/// USAGE:
///   var options = new StorageEngineOptions
///   {
///       ChecksumProvider = new Crc32ChecksumProvider()
///   };
///
///   // Or via builder:
///   builder.UseChecksumProvider(new Crc32ChecksumProvider());
/// </summary>
public sealed class Crc32ChecksumProvider : IPageChecksumProvider
{
    private static readonly uint[] Table = BuildTable();

    public uint Compute(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFF_FFFFu;
        foreach (byte value in data)
        {
            uint tableIndex = (crc ^ value) & 0xFFu;
            crc = (crc >> 8) ^ Table[tableIndex];
        }

        return ~crc;
    }

    private static uint[] BuildTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < table.Length; i++)
        {
            uint value = i;
            for (int bit = 0; bit < 8; bit++)
            {
                value = (value & 1u) != 0
                    ? 0xEDB8_8320u ^ (value >> 1)
                    : value >> 1;
            }

            table[i] = value;
        }

        return table;
    }
}

/// <summary>
/// Counting wrapper that delegates to any inner provider.
/// Demonstrates the Decorator pattern for diagnostics.
///
/// USAGE:
///   var counting = new CountingChecksumProvider(new Crc32ChecksumProvider());
///   // ... run workload ...
///   Console.WriteLine($"Checksum calls: {counting.ComputeCount}");
/// </summary>
public sealed class CountingChecksumProvider : IPageChecksumProvider
{
    private int _computeCount;
    private readonly IPageChecksumProvider _inner;

    public CountingChecksumProvider(IPageChecksumProvider? inner = null)
    {
        _inner = inner ?? new AdditiveChecksumProvider();
    }

    public int ComputeCount => Volatile.Read(ref _computeCount);

    public uint Compute(ReadOnlySpan<byte> data)
    {
        Interlocked.Increment(ref _computeCount);
        return _inner.Compute(data);
    }
}

// ============================================================================
// 5. CUSTOM INDEX PROVIDER (IIndexProvider / IIndexStore)
// ============================================================================
//
// Index providers create IIndexStore instances that back table/index storage.
// The default BTreeIndexProvider creates B+Tree-backed stores.
// CachingBTreeIndexProvider wraps them with an in-memory find cache.
//
// You could implement alternatives like hash indexes for equality-only
// lookups, or augmented indexes with bloom filters.
// ============================================================================

/// <summary>
/// An in-memory index store for testing purposes.
/// Demonstrates what IIndexStore requires without any disk I/O.
///
/// This is the simplest possible implementation — useful for understanding
/// the interface contract before building more complex stores.
/// </summary>
public sealed class InMemoryIndexStore : IIndexStore
{
    private readonly SortedDictionary<long, byte[]> _data = new();

    public uint RootPageId => 0; // Not backed by a pager

    public ValueTask<byte[]?> FindAsync(long key, CancellationToken ct = default)
    {
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
        return ValueTask.FromResult(_data.Remove(key));
    }

    public IIndexCursor CreateCursor(IndexScanRange range)
    {
        // Return a cursor that iterates over the sorted data within the range
        return new InMemoryIndexCursor(_data, range);
    }

    private sealed class InMemoryIndexCursor : IIndexCursor
    {
        private readonly IEnumerator<KeyValuePair<long, byte[]>> _enumerator;

        public InMemoryIndexCursor(SortedDictionary<long, byte[]> data, IndexScanRange range)
        {
            // Filter entries using the same inclusive/exclusive semantics as BTreeIndexStore.
            var filtered = data.Where(kv =>
            {
                if (range.LowerBound.HasValue)
                {
                    bool belowLower = range.LowerInclusive
                        ? kv.Key < range.LowerBound.Value
                        : kv.Key <= range.LowerBound.Value;
                    if (belowLower) return false;
                }

                if (range.UpperBound.HasValue)
                {
                    bool aboveUpper = range.UpperInclusive
                        ? kv.Key > range.UpperBound.Value
                        : kv.Key >= range.UpperBound.Value;
                    if (aboveUpper) return false;
                }

                return true;
            });
            _enumerator = filtered.GetEnumerator();
        }

        public long CurrentKey => _enumerator.Current.Key;
        public ReadOnlyMemory<byte> CurrentValue => _enumerator.Current.Value;

        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
        {
            return ValueTask.FromResult(_enumerator.MoveNext());
        }
    }
}

/// <summary>
/// An index provider that creates InMemoryIndexStore instances.
/// Useful for integration testing without disk I/O.
/// </summary>
public sealed class InMemoryIndexProvider : IIndexProvider
{
    private readonly ConcurrentDictionary<uint, InMemoryIndexStore> _stores = new();

    public IIndexStore CreateIndexStore(Pager pager, uint rootPageId)
    {
        return _stores.GetOrAdd(rootPageId, _ => new InMemoryIndexStore());
    }
}

// ============================================================================
// 6. CUSTOM STORAGE DEVICE (IStorageDevice)
// ============================================================================
//
// The storage device is the lowest-level I/O abstraction. The default
// FileStorageDevice uses RandomAccess APIs for async file I/O.
//
// Custom devices enable:
//   - In-memory databases for testing
//   - Encrypted storage (transparent encrypt/decrypt)
//   - Remote/cloud block storage (S3, Azure Blob, etc.)
//   - Compressed storage
//   - I/O throttling
// ============================================================================

/// <summary>
/// In-memory storage device for testing without disk I/O.
/// Data is stored in a MemoryStream that mimics file behavior.
///
/// USAGE:
///   var device = new InMemoryStorageDevice();
///   // Pass to Pager.CreateAsync() or use in tests
/// </summary>
public sealed class InMemoryStorageDevice : IStorageDevice, IAsyncDisposable, IDisposable
{
    private MemoryStream _stream = new();

    public long Length => _stream.Length;

    public ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default)
    {
        if (offset >= _stream.Length)
        {
            // Zero-fill when reading beyond end (matches FileStorageDevice behavior)
            buffer.Span.Clear();
            return ValueTask.FromResult(0);
        }

        _stream.Position = offset;
        int bytesToRead = (int)Math.Min(buffer.Length, _stream.Length - offset);
        _stream.Read(buffer.Span[..bytesToRead]);

        // Zero-fill remainder if we read less than requested
        if (bytesToRead < buffer.Length)
            buffer.Span[bytesToRead..].Clear();

        return ValueTask.FromResult(bytesToRead);
    }

    public ValueTask WriteAsync(long offset, ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
    {
        if (offset + buffer.Length > _stream.Length)
            _stream.SetLength(offset + buffer.Length);

        _stream.Position = offset;
        _stream.Write(buffer.Span);
        return ValueTask.CompletedTask;
    }

    public ValueTask FlushAsync(CancellationToken ct = default)
    {
        _stream.Flush();
        return ValueTask.CompletedTask;
    }

    public ValueTask SetLengthAsync(long length, CancellationToken ct = default)
    {
        _stream.SetLength(length);
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _stream.Dispose();
        return ValueTask.CompletedTask;
    }

    public void Dispose() => _stream.Dispose();
}

/// <summary>
/// A storage device decorator that adds I/O latency tracking.
/// Wraps any other IStorageDevice to measure read/write performance.
///
/// USAGE:
///   var file = new FileStorageDevice("mydb.cdb");
///   var tracked = new LatencyTrackingDevice(file);
///   // ... use tracked as the storage device ...
///   Console.WriteLine($"Avg read: {tracked.AvgReadMs:F2}ms");
/// </summary>
public sealed class LatencyTrackingDevice : IStorageDevice
{
    private readonly IStorageDevice _inner;
    private long _readCount;
    private long _writeCount;
    private long _readTotalTicks;
    private long _writeTotalTicks;

    public LatencyTrackingDevice(IStorageDevice inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public long Length => _inner.Length;

    public double AvgReadMs => _readCount == 0 ? 0 :
        TimeSpan.FromTicks(Interlocked.Read(ref _readTotalTicks) / Interlocked.Read(ref _readCount)).TotalMilliseconds;

    public double AvgWriteMs => _writeCount == 0 ? 0 :
        TimeSpan.FromTicks(Interlocked.Read(ref _writeTotalTicks) / Interlocked.Read(ref _writeCount)).TotalMilliseconds;

    public async ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default)
    {
        long start = Stopwatch.GetTimestamp();
        int result = await _inner.ReadAsync(offset, buffer, ct);
        long elapsed = Stopwatch.GetTimestamp() - start;
        Interlocked.Add(ref _readTotalTicks, elapsed);
        Interlocked.Increment(ref _readCount);
        return result;
    }

    public async ValueTask WriteAsync(long offset, ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
    {
        long start = Stopwatch.GetTimestamp();
        await _inner.WriteAsync(offset, buffer, ct);
        long elapsed = Stopwatch.GetTimestamp() - start;
        Interlocked.Add(ref _writeTotalTicks, elapsed);
        Interlocked.Increment(ref _writeCount);
    }

    public ValueTask FlushAsync(CancellationToken ct = default) => _inner.FlushAsync(ct);
    public ValueTask SetLengthAsync(long length, CancellationToken ct = default) => _inner.SetLengthAsync(length, ct);

    public ValueTask DisposeAsync() => _inner.DisposeAsync();
    public void Dispose() => _inner.Dispose();
}

// ============================================================================
// 7. CUSTOM SERIALIZER PROVIDER (ISerializerProvider)
// ============================================================================
//
// Serializer providers bundle IRecordSerializer + ISchemaSerializer together.
// The default uses variable-length encoding (LEB128 varints + type tags).
//
// Custom providers could use:
//   - Protocol Buffers or MessagePack for records
//   - Schema-aware fixed-width encoding for analytics
//   - Columnar encoding for compression
// ============================================================================

/// <summary>
/// Example showing how to wrap the default serializer with logging.
/// This helps you understand what's being serialized and when.
///
/// USAGE:
///   builder.UseSerializerProvider(new LoggingSerializerProvider());
/// </summary>
public sealed class LoggingSerializerProvider : ISerializerProvider
{
    private readonly ISerializerProvider _inner = new DefaultSerializerProvider();

    public IRecordSerializer RecordSerializer => _inner.RecordSerializer;
    public ISchemaSerializer SchemaSerializer => _inner.SchemaSerializer;

    // In a real implementation, you'd wrap the serializers with logging
    // decorators that log encode/decode calls. For study purposes,
    // this shows the shape of the ISerializerProvider interface.
}

// ============================================================================
// 8. CONFIGURATION & COMPOSITION EXAMPLES
// ============================================================================
//
// These examples show how to wire everything together using
// StorageEngineOptionsBuilder and DatabaseOptions.
// ============================================================================

/// <summary>
/// Complete configuration examples showing different deployment scenarios.
/// </summary>
public static class ConfigurationExamples
{
    /// <summary>
    /// Default configuration — zero customization needed.
    /// Uses: DictionaryPageCache (unbounded), FrameCountCheckpointPolicy(1000),
    ///       AdditiveChecksumProvider, BTreeIndexProvider, DefaultSerializerProvider.
    /// </summary>
    public static async Task DefaultConfigurationAsync()
    {
        string dbPath = "mydb.cdb";

        // The simplest way — all defaults
        await using var db = await Database.OpenAsync(dbPath);

        await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        await db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice')");
    }

    /// <summary>
    /// Production configuration with bounded cache, strong checksums,
    /// and caching indexes. This is what a serious deployment looks like.
    /// </summary>
    public static async Task ProductionConfigurationAsync()
    {
        string dbPath = "production.cdb";

        var options = new DatabaseOptions()
            .ConfigureStorageEngine(builder =>
            {
                builder.UsePagerOptions(new PagerOptions
                {
                    // Bound memory usage: keep at most 5000 pages (20MB) in cache
                    MaxCachedPages = 5000,

                    // Increase writer lock timeout for long transactions
                    WriterLockTimeout = TimeSpan.FromSeconds(30),

                    // Checkpoint every 500 frames OR every 5 minutes
                    CheckpointPolicy = new AnyCheckpointPolicy(
                        new FrameCountCheckpointPolicy(500),
                        new TimeIntervalCheckpointPolicy(TimeSpan.FromMinutes(5))),
                });

                // Use caching B+Tree indexes for faster repeated lookups
                builder.UseCachingBTreeIndexes(findCacheCapacity: 4096);

                // Use CRC32 for stronger data integrity
                builder.UseChecksumProvider(new Crc32ChecksumProvider());
            });

        await using var db = await Database.OpenAsync(dbPath, options);
    }

    /// <summary>
    /// Development/debugging configuration with full logging interceptor.
    /// Shows exactly what the storage engine is doing on every operation.
    /// </summary>
    public static async Task DebugConfigurationAsync()
    {
        string dbPath = "debug.cdb";

        var logger = new ConsoleLoggingInterceptor();

        var options = new DatabaseOptions()
            .ConfigureStorageEngine(builder =>
            {
                builder.UsePagerOptions(new PagerOptions
                {
                    // Frequent checkpoints so we see checkpoint events
                    CheckpointPolicy = new FrameCountCheckpointPolicy(5),
                    Interceptors = [logger],
                });
            });

        await using var db = await Database.OpenAsync(dbPath, options);

        Console.WriteLine("--- CREATE TABLE ---");
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");

        Console.WriteLine("--- INSERT ---");
        await db.ExecuteAsync("INSERT INTO t VALUES (1, 'hello')");

        Console.WriteLine("--- SELECT ---");
        await using var result = await db.ExecuteAsync("SELECT * FROM t WHERE id = 1");
        await result.ToListAsync();
    }

    /// <summary>
    /// Batch import configuration optimized for high write throughput.
    /// Defers checkpointing until the import is complete.
    /// </summary>
    public static async Task BatchImportConfigurationAsync()
    {
        string dbPath = "import.cdb";

        var options = new DatabaseOptions()
            .ConfigureStorageEngine(builder =>
            {
                builder.UsePagerOptions(new PagerOptions
                {
                    // No automatic checkpoints during import
                    CheckpointPolicy = NeverCheckpointPolicy.Instance,

                    // Large unbounded cache (no MaxCachedPages = DictionaryPageCache)
                    // This keeps all pages in memory during the import
                });
            });

        await using var db = await Database.OpenAsync(dbPath, options);

        await db.ExecuteAsync("CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)");

        // Bulk insert without checkpoint overhead
        for (int i = 0; i < 100_000; i++)
        {
            await db.ExecuteAsync($"INSERT INTO data VALUES ({i}, 'value_{i}')");
        }

        // Manually checkpoint after import is complete
        // (In a real app, you'd access the Pager through the engine internals
        //  or switch to a normal checkpoint policy after import)
    }

    /// <summary>
    /// Custom page cache with metrics collection.
    /// Demonstrates the PageCacheFactory extensibility point.
    /// </summary>
    public static async Task MetricsCacheConfigurationAsync()
    {
        string dbPath = "metrics.cdb";
        var metricsCache = new MetricsPageCache(maxPages: 1000);

        var options = new DatabaseOptions()
            .ConfigureStorageEngine(builder =>
            {
                builder.UsePagerOptions(new PagerOptions
                {
                    PageCacheFactory = () => metricsCache,
                });
            });

        await using var db = await Database.OpenAsync(dbPath, options);

        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");

        for (int i = 0; i < 1000; i++)
            await db.ExecuteAsync($"INSERT INTO t VALUES ({i}, {i * 10})");

        // Point lookups to exercise cache
        for (int i = 0; i < 1000; i++)
        {
            await using var r = await db.ExecuteAsync($"SELECT * FROM t WHERE id = {i}");
            await r.ToListAsync();
        }

        metricsCache.PrintStats();
    }

    /// <summary>
    /// Multiple interceptors combined automatically via CompositePageOperationInterceptor.
    /// </summary>
    public static async Task MultipleInterceptorsAsync()
    {
        string dbPath = "multi.cdb";

        var logger = new ConsoleLoggingInterceptor();
        var latency = new LatencyTrackingInterceptor();

        var options = new DatabaseOptions()
            .ConfigureStorageEngine(builder =>
            {
                builder.UsePagerOptions(new PagerOptions
                {
                    // Multiple interceptors are automatically composed
                    // via CompositePageOperationInterceptor
                    Interceptors = [logger, latency],
                });
            });

        await using var db = await Database.OpenAsync(dbPath, options);

        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY)");
        await db.ExecuteAsync("INSERT INTO t VALUES (1)");

        latency.PrintReport();
    }
}

// ============================================================================
// 9. DIAGNOSTIC / MONITORING SCENARIOS
// ============================================================================

/// <summary>
/// Shows how to build a dashboard-style monitor that periodically
/// reports storage engine health metrics.
/// </summary>
public sealed class StorageHealthMonitor
{
    private readonly LatencyTrackingInterceptor _latency = new();
    private MetricsPageCache? _cache;

    /// <summary>
    /// Creates DatabaseOptions configured with monitoring interceptors.
    /// </summary>
    public DatabaseOptions CreateMonitoredOptions(int maxCachedPages = 2000)
    {
        _cache = new MetricsPageCache(maxCachedPages);

        return new DatabaseOptions()
            .ConfigureStorageEngine(builder =>
            {
                builder.UsePagerOptions(new PagerOptions
                {
                    PageCacheFactory = () => _cache,
                    Interceptors = [_latency],
                    CheckpointPolicy = new AnyCheckpointPolicy(
                        new FrameCountCheckpointPolicy(500),
                        new TimeIntervalCheckpointPolicy(TimeSpan.FromMinutes(2))),
                });
            });
    }

    /// <summary>
    /// Prints a health report to the console.
    /// Call this periodically (e.g., every 30 seconds).
    /// </summary>
    public void PrintHealthReport()
    {
        Console.WriteLine("╔══════════════════════════════════════╗");
        Console.WriteLine("║   CSharpDB Storage Health Report     ║");
        Console.WriteLine("╠══════════════════════════════════════╣");

        if (_cache != null)
        {
            Console.WriteLine($"║ Cache Hit Rate:  {_cache.HitRate,8:P1}          ║");
            Console.WriteLine($"║ Cache Hits:      {_cache.Hits,8:N0}          ║");
            Console.WriteLine($"║ Cache Misses:    {_cache.Misses,8:N0}          ║");
        }

        Console.WriteLine("╚══════════════════════════════════════╝");
    }
}

// ============================================================================
// 10. TESTING SCENARIOS
// ============================================================================

/// <summary>
/// Examples of how CSharpDB.Storage extensibility helps with testing.
/// </summary>
public static class TestingExamples
{
    /// <summary>
    /// Test crash recovery by using fault injection.
    /// Simulates a write failure mid-transaction and verifies
    /// that the database recovers correctly.
    /// </summary>
    public static async Task CrashRecoveryTestAsync()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"crash_test_{Guid.NewGuid():N}.cdb");

        try
        {
            var fault = new FaultInjectionInterceptor();

            var options = new DatabaseOptions()
                .ConfigureStorageEngine(builder =>
                {
                    builder.UsePagerOptions(new PagerOptions
                    {
                        Interceptors = [fault],
                    });
                });

            // Phase 1: Create database with some data
            await using (var db = await Database.OpenAsync(dbPath, options))
            {
                await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
                await db.ExecuteAsync("INSERT INTO t VALUES (1, 100)");
                await db.ExecuteAsync("INSERT INTO t VALUES (2, 200)");
            }

            // Phase 2: Simulate failure during the next commit
            fault.Reset();
            fault.FailWritesAfter = 1;

            try
            {
                await using var db = await Database.OpenAsync(dbPath, options);

                // This should fail after the first WAL page write in the next commit
                await db.ExecuteAsync("INSERT INTO t VALUES (3, 300)");
                Console.WriteLine("Write succeeded (no crash simulated yet)");
            }
            catch (IOException ex)
            {
                Console.WriteLine($"Simulated crash: {ex.Message}");
            }

            // Phase 3: Recover and verify original data is intact
            fault.FailWritesAfter = null; // Disable fault injection

            await using (var db = await Database.OpenAsync(dbPath, options))
            {
                await using var result = await db.ExecuteAsync("SELECT * FROM t");
                var rows = await result.ToListAsync();
                Console.WriteLine($"Recovered {rows.Count} rows after crash");
                // Should have 2 rows (the committed ones before the crash)
            }
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal")) File.Delete(dbPath + ".wal");
        }
    }

    /// <summary>
    /// Test checkpoint policy behavior using a fake clock.
    /// This is how you deterministically test time-based policies
    /// without waiting for real time to pass.
    /// </summary>
    public static void CheckpointPolicyUnitTest()
    {
        // Create a time-based policy with a fake clock
        var clock = new FakeClock(DateTimeOffset.UtcNow);
        var policy = new TimeIntervalCheckpointPolicy(
            TimeSpan.FromMinutes(5),
            clock);

        var context = new PagerCheckpointContext(
            CommittedFrameCount: 10,
            ActiveReaderCount: 0,
            EstimatedWalBytes: 1024);

        // Should NOT checkpoint yet (just started)
        bool should1 = policy.ShouldCheckpoint(context);
        Console.WriteLine($"After 0 min: ShouldCheckpoint = {should1}"); // false

        // Advance clock by 6 minutes
        clock.Advance(TimeSpan.FromMinutes(6));

        bool should2 = policy.ShouldCheckpoint(context);
        Console.WriteLine($"After 6 min: ShouldCheckpoint = {should2}"); // true

        // Should NOT checkpoint if readers are active
        var contextWithReaders = new PagerCheckpointContext(
            CommittedFrameCount: 10,
            ActiveReaderCount: 2,
            EstimatedWalBytes: 1024);

        clock.Advance(TimeSpan.FromMinutes(6));
        bool should3 = policy.ShouldCheckpoint(contextWithReaders);
        Console.WriteLine($"With readers: ShouldCheckpoint = {should3}"); // false
    }

    /// <summary>
    /// Test WalSizeCheckpointPolicy behavior.
    /// </summary>
    public static void WalSizePolicyTest()
    {
        var policy = new WalSizeCheckpointPolicy(8 * 1024); // 8 KB threshold

        // Below threshold
        var ctx1 = new PagerCheckpointContext(
            CommittedFrameCount: 2,
            ActiveReaderCount: 0,
            EstimatedWalBytes: 7 * 1024);
        Console.WriteLine($"7KB WAL: ShouldCheckpoint = {policy.ShouldCheckpoint(ctx1)}"); // false

        // At threshold
        var ctx2 = new PagerCheckpointContext(
            CommittedFrameCount: 2,
            ActiveReaderCount: 0,
            EstimatedWalBytes: 8 * 1024);
        Console.WriteLine($"8KB WAL: ShouldCheckpoint = {policy.ShouldCheckpoint(ctx2)}"); // true

        // Above threshold but readers active
        var ctx3 = new PagerCheckpointContext(
            CommittedFrameCount: 2,
            ActiveReaderCount: 1,
            EstimatedWalBytes: 16 * 1024);
        Console.WriteLine($"16KB WAL with readers: ShouldCheckpoint = {policy.ShouldCheckpoint(ctx3)}"); // false
    }
}

/// <summary>
/// Fake clock for deterministic testing of time-based policies.
/// Implements IClock with manual time advancement.
///
/// USAGE:
///   var clock = new FakeClock(DateTimeOffset.UtcNow);
///   var policy = new TimeIntervalCheckpointPolicy(TimeSpan.FromMinutes(5), clock);
///   clock.Advance(TimeSpan.FromMinutes(6)); // trigger checkpoint
/// </summary>
public sealed class FakeClock : IClock
{
    private DateTimeOffset _now;

    public FakeClock(DateTimeOffset initialTime) => _now = initialTime;

    public DateTimeOffset UtcNow => _now;

    public void Advance(TimeSpan duration) => _now += duration;
    public void Set(DateTimeOffset time) => _now = time;
}

// ============================================================================
// QUICK REFERENCE: How to wire each extensibility point
// ============================================================================
//
// IPageCache:
//   new PagerOptions { PageCacheFactory = () => new MyCache() }
//   new PagerOptions { MaxCachedPages = 1000 }  // uses built-in LruPageCache
//
// ICheckpointPolicy:
//   new PagerOptions { CheckpointPolicy = new MyPolicy() }
//
// IPageOperationInterceptor:
//   new PagerOptions { Interceptors = [new MyInterceptor()] }
//
// IPageChecksumProvider:
//   builder.UseChecksumProvider(new MyChecksumProvider())
//   builder.UseChecksumProvider<MyChecksumProvider>()
//   new StorageEngineOptions { ChecksumProvider = new MyProvider() }
//
// IIndexProvider:
//   builder.UseIndexProvider(new MyIndexProvider())
//   builder.UseBTreeIndexes()
//   builder.UseCachingBTreeIndexes(findCacheCapacity: 2048)
//
// ISerializerProvider:
//   builder.UseSerializerProvider(new MyProvider())
//   builder.UseSerializerProvider<MyProvider>()
//
// ICatalogStore:
//   builder.UseCatalogStore(new MyCatalog())
//   builder.UseCatalogStore<MyCatalog>()
//
// IStorageDevice:
//   Passed directly when creating Pager via Pager.CreateAsync()
//   Or implement IStorageEngineFactory for full control
//
// IClock:
//   new TimeIntervalCheckpointPolicy(interval, new FakeClock(...))
// ============================================================================
