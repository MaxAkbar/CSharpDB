# Configuration Reference

CSharpDB is configured through a hierarchy of options objects passed to `Database.OpenAsync`.
All options use sensible defaults — you only need to override what you want to change.

---

## Opening a Database

```csharp
// Defaults — zero configuration
await using var db = await Database.OpenAsync("mydata.db");

// With options
await using var db = await Database.OpenAsync("mydata.db", new DatabaseOptions
{
    StorageEngineOptions = new StorageEngineOptions
    {
        DurabilityMode = DurabilityMode.Buffered,
        PagerOptions = new PagerOptions
        {
            MaxCachedPages = 5000,
            CheckpointPolicy = new FrameCountCheckpointPolicy(2000),
        }
    }
});

// In-memory
await using var db = await Database.OpenInMemoryAsync();

// Hybrid (lazy-resident with on-disk persistence)
await using var db = await Database.OpenHybridAsync("mydata.db",
    new DatabaseOptions(),
    new HybridDatabaseOptions
    {
        PersistenceMode = HybridPersistenceMode.IncrementalDurable,
        HotTableNames = ["users", "sessions"],
    });
```

---

## Options Hierarchy

```
DatabaseOptions
├── StorageEngineOptions
│   ├── DurabilityMode
│   ├── DurableCommitBatchWindow
│   ├── AdvisoryStatisticsPersistenceMode
│   ├── WalPreallocationChunkBytes
│   └── PagerOptions
│       ├── WriterLockTimeout
│       ├── CheckpointPolicy
│       ├── AutoCheckpointExecutionMode
│       ├── AutoCheckpointMaxPagesPerStep
│       ├── MaxCachedPages
│       ├── MaxCachedWalReadPages
│       ├── PageCacheFactory
│       ├── MaxWalBytesWhenReadersActive
│       ├── UseMemoryMappedReads
│       ├── EnableSequentialLeafReadAhead
│       └── PreserveOwnedPagesOnCheckpoint
└── StorageEngineFactory

HybridDatabaseOptions (separate, for OpenHybridAsync)
├── PersistenceMode
├── PersistenceTriggers
├── HotTableNames
└── HotCollectionNames

FullTextIndexOptions (per-index, for EnsureFullTextIndexAsync)
├── Normalization
├── LowercaseInvariant
└── StorePositions
```

---

## StorageEngineOptions

Top-level storage engine configuration.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `DurabilityMode` | `DurabilityMode` | `Durable` | WAL commit flushing strategy |
| `DurableCommitBatchWindow` | `TimeSpan` | `TimeSpan.Zero` | Delay for group commit batching before OS flush |
| `AdvisoryStatisticsPersistenceMode` | `AdvisoryStatisticsPersistenceMode` | `Immediate` | When to persist query planner statistics |
| `WalPreallocationChunkBytes` | `long` | `0` (disabled) | Pre-reserve WAL file space in chunks of this size |
| `PagerOptions` | `PagerOptions` | `new()` | Page cache, locking, and checkpoint settings |

### DurabilityMode

Controls how WAL commits are flushed to disk.

| Value | Description |
|-------|-------------|
| `Durable` | Forces an OS-level flush on every commit. Maximum safety, analogous to SQLite `synchronous=FULL`. This is the default. |
| `Buffered` | Flushes managed buffers to the OS but does not force an OS flush per commit. Higher throughput at the cost of potential data loss of recent commits on power failure. Analogous to SQLite `synchronous=NORMAL`. |

### DurableCommitBatchWindow

When using `Durable` mode, setting this to a non-zero `TimeSpan` (e.g., `TimeSpan.FromMilliseconds(5)`) causes the engine to batch concurrent commits into a single OS flush. This amortizes the cost of `fsync` across multiple writers without reducing durability guarantees for any individual commit.

### AdvisoryStatisticsPersistenceMode

| Value | Description |
|-------|-------------|
| `Immediate` | Statistics updated during ordinary commit flows. Default — keeps stats fresh with minimal lag. |
| `Deferred` | Statistics kept current in memory but only persisted on `ANALYZE`, clean close, or export. Reduces write amplification for write-heavy workloads. |

---

## PagerOptions

Controls the page cache, writer locking, and automatic checkpointing.

### Cache

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `MaxCachedPages` | `int?` | `null` (unbounded) | Maximum pages in the shared cache. When set, uses an LRU eviction policy. When `null`, all pages are retained. |
| `MaxCachedWalReadPages` | `int` | `0` | Dedicated cache for immutable WAL frame reads. `0` disables caching (always reads from WAL). |
| `PageCacheFactory` | `Func<IPageCache>?` | `null` | Custom cache implementation. Overrides `MaxCachedPages` when set. |
| `OnCachePageEvicted` | `Action<uint, byte[]>?` | `null` | Diagnostic callback invoked when a page is evicted from cache. |
| `PreserveOwnedPagesOnCheckpoint` | `bool` | `true` | Keep materialized main-file pages in cache after checkpoint. Only transient WAL and memory-mapped views are invalidated. |

### Read Path

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `UseMemoryMappedReads` | `bool` | `false` | Enable memory-mapped I/O for clean main-file page reads. |
| `EnableSequentialLeafReadAhead` | `bool` | `true` | Speculative read-ahead of next B-tree leaf during sequential scans. |

### Writer Lock

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `WriterLockTimeout` | `TimeSpan` | `5 seconds` | Maximum wait time to acquire the single-writer lock. |

### Checkpoint Policy

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `CheckpointPolicy` | `ICheckpointPolicy` | `FrameCountCheckpointPolicy(1000)` | When to trigger automatic checkpoints. |
| `AutoCheckpointExecutionMode` | `AutoCheckpointExecutionMode` | `Foreground` | Run checkpoints inline or in the background. |
| `AutoCheckpointMaxPagesPerStep` | `int` | `64` | Max pages per background checkpoint step (ignored for foreground). |
| `MaxWalBytesWhenReadersActive` | `long?` | `null` | Optional WAL growth cap while snapshot readers are active. Commits fail with `ErrorCode.Busy` if exceeded. |

#### Built-in Checkpoint Policies

**FrameCountCheckpointPolicy(int threshold)**

Triggers when committed frame count reaches the threshold and no snapshot readers are
active. Default threshold is 1000 frames.

```csharp
new FrameCountCheckpointPolicy(2000)
```

**TimeIntervalCheckpointPolicy(TimeSpan interval)**

Triggers at fixed wall-clock intervals when no readers are active.

```csharp
new TimeIntervalCheckpointPolicy(TimeSpan.FromMinutes(5))
```

**WalSizeCheckpointPolicy(long thresholdBytes)**

Triggers when estimated WAL size in bytes reaches the threshold and no readers are active.

```csharp
new WalSizeCheckpointPolicy(50 * 1024 * 1024) // 50 MB
```

**AnyCheckpointPolicy(params ICheckpointPolicy[] policies)**

Composite policy — triggers when any of the inner policies fires.

```csharp
new AnyCheckpointPolicy(
    new FrameCountCheckpointPolicy(1000),
    new WalSizeCheckpointPolicy(100 * 1024 * 1024)
)
```

#### AutoCheckpointExecutionMode

| Value | Description |
|-------|-------------|
| `Foreground` | Checkpoint runs inline with the triggering commit. The commit does not return until the checkpoint completes. Simplest model. |
| `Background` | Checkpoint is scheduled asynchronously after commit returns. Copies at most `AutoCheckpointMaxPagesPerStep` pages per step to avoid blocking subsequent writes. |

---

## HybridDatabaseOptions

Configuration for hybrid databases opened with `Database.OpenHybridAsync`. Hybrid databases
keep frequently accessed pages resident in memory while persisting committed state to a
backing file.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `PersistenceMode` | `HybridPersistenceMode` | `IncrementalDurable` | Persistence strategy. |
| `PersistenceTriggers` | `HybridPersistenceTriggers` | `Commit \| Dispose` | Lifecycle points that trigger persistence (snapshot mode only). |
| `HotTableNames` | `IReadOnlyList<string>` | `[]` | SQL tables to preload into cache at open time. |
| `HotCollectionNames` | `IReadOnlyList<string>` | `[]` | Collections to preload into cache at open time. |

### HybridPersistenceMode

| Value | Description |
|-------|-------------|
| `IncrementalDurable` | Uses on-disk WAL and page-level checkpoints. Touched pages remain resident by cache policy. Default and recommended. |
| `Snapshot` | Full-image export to backing file. Simpler but higher write amplification. |

### HybridPersistenceTriggers (Flags)

Only applies when `PersistenceMode` is `Snapshot`.

| Flag | Description |
|------|-------------|
| `None` | Manual persistence only |
| `Commit` | Persist after each commit |
| `Checkpoint` | Persist on checkpoint |
| `Dispose` | Persist on database close |

---

## FullTextIndexOptions

Per-index configuration passed to `Database.EnsureFullTextIndexAsync`.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Normalization` | `NormalizationForm` | `FormKC` | Unicode normalization form applied during indexing. |
| `LowercaseInvariant` | `bool` | `true` | Apply case-insensitive indexing. |
| `StorePositions` | `bool` | `true` | Store token positions for proximity/phrase searches. |

---

## Runtime Settings

These properties can be changed on an open `Database` instance:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `PreferSyncPointLookups` | `bool` | `true` | Simple primary-key equality lookups use a synchronous cache-only fast path instead of the async pipeline. |

---

## Key Defaults Summary

| Setting | Default |
|---------|---------|
| Page size | 4096 bytes (fixed) |
| Durability | `Durable` (fsync on every commit) |
| Page cache | Unbounded (all pages retained) |
| Checkpoint threshold | 1000 committed WAL frames |
| Checkpoint execution | Foreground (inline) |
| Writer lock timeout | 5 seconds |
| Sequential read-ahead | Enabled |
| Statistics persistence | Immediate |
