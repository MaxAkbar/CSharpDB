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
├── ImplicitInsertExecutionMode
├── StorageEngineOptions
│   ├── DurabilityMode
│   ├── DurableGroupCommit
│   ├── DurableCommitBatchWindow (compatibility alias)
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

## DatabaseOptions

Top-level database composition and execution-shape configuration.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `ImplicitInsertExecutionMode` | `ImplicitInsertExecutionMode` | `Serialized` | Controls whether shared auto-commit `INSERT` statements stay behind the legacy database write gate or run as isolated `WriteTransaction` commits. This does not disable the explicit multi-writer `WriteTransaction` APIs. |
| `StorageEngineOptions` | `StorageEngineOptions` | default instance | Storage engine durability, pager, WAL, and checkpoint settings |
| `StorageEngineFactory` | `IStorageEngineFactory` | `DefaultStorageEngineFactory` | Factory used to compose the backing storage engine |

---

## StorageEngineOptions

Top-level storage engine configuration.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `DurabilityMode` | `DurabilityMode` | `Durable` | WAL commit flushing strategy |
| `DurableGroupCommit` | `DurableGroupCommitOptions` | `Disabled` | Opt-in durable group-commit settings |
| `DurableCommitBatchWindow` | `TimeSpan` | `TimeSpan.Zero` | Compatibility alias for `DurableGroupCommit.BatchWindow` |
| `AdvisoryStatisticsPersistenceMode` | `AdvisoryStatisticsPersistenceMode` | `Immediate` | When to persist query planner statistics |
| `WalPreallocationChunkBytes` | `long` | `0` (disabled) | Pre-reserve WAL file space in chunks of this size |
| `PagerOptions` | `PagerOptions` | `new()` | Page cache, locking, and checkpoint settings |

### DurabilityMode

Controls how WAL commits are flushed to disk.

| Value | Description |
|-------|-------------|
| `Durable` | Forces an OS-level flush on every commit. Maximum safety, analogous to SQLite `synchronous=FULL`. This is the default. |
| `Buffered` | Flushes managed buffers to the OS but does not force an OS flush per commit. Higher throughput at the cost of potential data loss of recent commits on power failure. Analogous to SQLite `synchronous=NORMAL`. |

### DurableGroupCommit

When using `Durable` mode, setting `DurableGroupCommit.BatchWindow` to a non-zero `TimeSpan` (for example, `TimeSpan.FromMilliseconds(0.25)`) allows the engine to batch concurrent commits into a single OS flush. This amortizes the cost of `fsync` across multiple writers without reducing durability guarantees for any individual commit.

`DurableCommitBatchWindow` remains available as a compatibility alias for the same `BatchWindow` value.

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

Snapshot readers now block checkpoint finalization only when they still reference
retained WAL frames. Readers whose snapshot was taken after the WAL had already
been drained can coexist with auto-checkpoint finalization without forcing WAL
growth.

#### Built-in Checkpoint Policies

**FrameCountCheckpointPolicy(int threshold)**

Triggers when committed frame count reaches the threshold and no active snapshot
currently requires retained WAL frames. Default threshold is 1000 frames.

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
| `ImplicitInsertExecutionMode` | `ImplicitInsertExecutionMode` | `Serialized` | Shared auto-commit `INSERT` statements use the legacy serialized path by default; set `ConcurrentWriteTransactions` to route them through isolated write transactions for better low-conflict insert fan-in. This setting only changes the shared implicit `INSERT` path. |
| `PreferSyncPointLookups` | `bool` | `true` | Simple primary-key equality lookups use a synchronous cache-only fast path instead of the async pipeline. |

---

## Daemon Host Configuration

`CSharpDB.Daemon` has a daemon-only host database section layered on top of the
normal `ConnectionStrings:CSharpDB` input. The daemon still uses the same
database locator, but it now builds its direct host client explicitly and
applies daemon defaults for a long-lived shared process.

Relevant keys:

| Key | Default | Description |
|-----|---------|-------------|
| `ConnectionStrings:CSharpDB` | `Data Source=csharpdb.db` | Backing database connection string used by the daemon host |
| `CSharpDB:HostDatabase:OpenMode` | `HybridIncrementalDurable` | `HybridIncrementalDurable` keeps the daemon in lazy-resident hybrid mode; `Direct` opens the file directly without the hybrid resident cache |
| `CSharpDB:HostDatabase:ImplicitInsertExecutionMode` | `ConcurrentWriteTransactions` | Enables concurrent host-side implicit insert execution by default |
| `CSharpDB:HostDatabase:UseWriteOptimizedPreset` | `true` | Applies `UseWriteOptimizedPreset()` to the daemon's direct host database options |
| `CSharpDB:HostDatabase:HotTableNames` | `[]` | Optional hybrid hot-table preload hints |
| `CSharpDB:HostDatabase:HotCollectionNames` | `[]` | Optional hybrid hot-collection preload hints |

Default daemon `appsettings.json` shape:

```json
{
  "ConnectionStrings": {
    "CSharpDB": "Data Source=csharpdb.db"
  },
  "CSharpDB": {
    "HostDatabase": {
      "OpenMode": "HybridIncrementalDurable",
      "ImplicitInsertExecutionMode": "ConcurrentWriteTransactions",
      "UseWriteOptimizedPreset": true,
      "HotTableNames": [],
      "HotCollectionNames": []
    }
  }
}
```

Environment variable example:

```powershell
$env:ConnectionStrings__CSharpDB = "Data Source=C:\\data\\app.db"
$env:CSharpDB__HostDatabase__OpenMode = "Direct"
$env:CSharpDB__HostDatabase__ImplicitInsertExecutionMode = "Serialized"
$env:CSharpDB__HostDatabase__HotTableNames__0 = "users"
```

Use the daemon defaults when you want a warm long-lived gRPC host. Override
them only when your deployment has a measured reason to prefer direct open mode
or the legacy serialized implicit-insert path.

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
