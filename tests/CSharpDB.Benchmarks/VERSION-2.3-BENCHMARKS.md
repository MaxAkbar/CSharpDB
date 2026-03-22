# CSharpDB Version 2.3 Benchmark Results

Captured: March 21, 2026 (branch `version2.3.0`)

## Benchmark Results vs. Version 2.0 Baseline

### Metric Mapping

Per the README: point lookups use `ColdLookupBenchmarks` (cache-pressured), in-memory single writes use `InMemory*Benchmarks` micros, file-backed writes use the reproducible `macro` capture, batched writes use `macro-batch-memory` for in-memory and `macro` for file-backed.

`c2.3_test` reflects the local benchmark run captured on March 21, 2026 from:

- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/macro-20260321-183150-median-of-3.csv`
- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/macro-batch-memory-20260321-185554-median-of-3.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.ColdLookupBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.InMemorySqlBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.InMemoryCollectionBenchmarks-report.csv`

### CSharpDB SQL (file-backed)

| Metric | v2.0 | v2.2 | c2.3_test | Delta |
|--------|------|------|-----------|-------|
| Single INSERT | **17.7K** ops/sec | **28.9K** ops/sec | **441** ops/sec | **-97.5%** |
| Batched INSERT | **~417.6K** rows/sec | **~701.0K** rows/sec | **~42.6K** rows/sec | **-89.8%** |
| Point Lookup (cold) | **~38.0K** ops/sec | **~31.0K** ops/sec | **~17.8K** ops/sec | **-53.1%** |
| Concurrent Reads (8r) | 112,913 / 1,854,122 | 1,066,690 / 8,034,923 | 529,381 / 6,243,917 | **+368.8% / +236.8%** |

### CSharpDB SQL (in-memory)

| Metric | v2.0 | v2.2 | c2.3_test | Delta |
|--------|------|------|-----------|-------|
| Single INSERT | **~285.6K** ops/sec | **~358.0K** ops/sec | **~344.6K** ops/sec | **+20.7%** |
| Batched INSERT | **~951.7K** rows/sec | **~1.45M** rows/sec | **~1.53M** rows/sec | **+61.1%** |
| Point Lookup (cold) | **~604.2K** ops/sec | **~508.9K** ops/sec | **~528.5K** ops/sec | **-12.5%** |

### CSharpDB Collection (file-backed)

| Metric | v2.0 | v2.2 | c2.3_test | Delta |
|--------|------|------|-----------|-------|
| Single Put | **21.3K** ops/sec | **30.7K** ops/sec | **440** ops/sec | **-97.9%** |
| Batched PUT | **~290.7K** docs/sec | **~403.6K** docs/sec | **~40.3K** docs/sec | **-86.1%** |
| Point Get (cold) | **~37.2K** ops/sec | **~31.8K** ops/sec | **~15.9K** ops/sec | **-57.3%** |

### CSharpDB Collection (in-memory)

| Metric | v2.0 | v2.2 | c2.3_test | Delta |
|--------|------|------|-----------|-------|
| Single Put | **~413.6K** ops/sec | **~519.3K** ops/sec | **~332.7K** ops/sec | **-19.6%** |
| Batched PUT | **~809.9K** docs/sec | **~932.9K** docs/sec | **~966.1K** docs/sec | **+19.3%** |
| Point Get (cold) | **~596.3K** ops/sec | **~597.7K** ops/sec | **~459.6K** ops/sec | **-22.9%** |

## Summary

**The headline change in v2.3 is file-backed durability realism:**

- File-backed SQL single-write throughput fell from the prior pseudo-durable range to **441 ops/sec**
- File-backed collection single-write throughput fell to **440 ops/sec**
- File-backed batched write throughput also dropped sharply, to roughly **40-43K** rows/docs per second

This aligns with the v2.3 storage change: commit durability is now explicit and the default durable mode uses a real OS-buffer flush primitive instead of `FlushAsync`. The earlier `2.2` numbers were materially overstating durable write throughput because they were not paying the full durability cost.

**In-memory throughput is still healthy:**

- In-memory SQL single inserts remain above the v2.0 baseline at **~344.6K ops/sec**
- In-memory SQL batched inserts improved further to **~1.53M rows/sec**
- In-memory collection batched puts reached **~966.1K docs/sec**

**Read-side results are mixed:**

- File-backed cold lookups regressed further, landing around **17.8K SQL ops/sec** and **15.9K collection ops/sec**
- In-memory cold lookups are still below v2.0 but remain in the **~460K-529K ops/sec** range
- 8-reader concurrent throughput is still well above v2.0, though the burst-session path is below the `2.2` capture

The practical read on `2.3` is: correctness improved substantially for durable file-backed commits, and the benchmark numbers now show the true price of that guarantee.
