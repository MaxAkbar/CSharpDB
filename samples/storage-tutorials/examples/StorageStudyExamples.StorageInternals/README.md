# StorageStudyExamples.StorageInternals

9 demo-only examples showing how to customize the CSharpDB storage engine internals via `PagerOptions` and `StorageEngineOptionsBuilder`.

These examples implement only `IExample` (not `IInteractiveExample`) — they run a scripted demo and do not support interactive commands.

## Load in the REPL

```
> load default-config
> demo
```

## Examples

| Command | What it demonstrates |
|---------|---------------------|
| `default-config` | Open a database with all default settings |
| `production-config` | Bounded LRU cache, CRC32 checksums, caching indexes |
| `debug-config` | Verbose interceptor logging on every page operation |
| `batch-import` | Disable auto-checkpoint for high-throughput bulk writes |
| `metrics-cache` | Instrument the page cache with hit/miss/eviction stats |
| `multiple-interceptors` | Combine a logger and latency tracker in one pipeline |
| `crash-recovery-test` | Fault-inject a write failure and verify WAL recovery |
| `checkpoint-policy-test` | Deterministic test of `TimeIntervalCheckpointPolicy` with a fake clock |
| `wal-size-policy-test` | Deterministic test of `WalSizeCheckpointPolicy` thresholds |

**Suggested starting points:** `default-config`, `debug-config`, `metrics-cache`

## Contents

| File | Description |
|------|-------------|
| `StorageInternalsExample.cs` | Adapter that wraps each demo as an `IExample` instance |
| `StorageStudyExamples.cs` | All 9 demos — `ConfigurationExamples` (default, production, debug, batch-import, metrics-cache, multiple-interceptors) and `TestingExamples` (crash-recovery, checkpoint-policy, WAL-size-policy) |

See the [root README](../README.md) for full documentation.
