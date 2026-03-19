# CSharpDB Version 2.2 Benchmark Results

Captured: March 19, 2026 (branch `version2.2.0`)

## Benchmark Results vs. Version 2.0 Baseline

### Metric Mapping

Per the README: point lookups use `ColdLookupBenchmarks` (cache-pressured), in-memory single writes use `InMemory*Benchmarks` micros, file-backed writes use `SustainedWriteBenchmark`, batched writes use `InMemoryBatchBenchmark`.

### CSharpDB SQL (file-backed)

| Metric | v2.0 | v2.2 | Delta |
|--------|------|------|-------|
| Single INSERT | **17.7K** ops/sec | **28.9K** ops/sec | **+63.2%** |
| Batched INSERT | **~417.6K** rows/sec | **~701.0K** rows/sec | **+67.9%** |
| Point Lookup (cold) | **~38.0K** ops/sec | **~31.0K** ops/sec | **-18.4%** |
| Concurrent Reads (8r) | 112,913 / 1,854,122 | 1,066,690 / 8,034,923 | **+844% / +333%** |

### CSharpDB SQL (in-memory)

| Metric | v2.0 | v2.2 | Delta |
|--------|------|------|-------|
| Single INSERT | **~285.6K** ops/sec | **~358.0K** ops/sec | **+25.3%** |
| Batched INSERT | **~951.7K** rows/sec | **~1.45M** rows/sec | **+52.2%** |
| Point Lookup (cold) | **~604.2K** ops/sec | **~508.9K** ops/sec | **-15.8%** |

### CSharpDB Collection (file-backed)

| Metric | v2.0 | v2.2 | Delta |
|--------|------|------|-------|
| Single Put | **21.3K** ops/sec | **30.7K** ops/sec | **+44.1%** |
| Batched PUT | **~290.7K** docs/sec | **~403.6K** docs/sec | **+38.8%** |
| Point Get (cold) | **~37.2K** ops/sec | **~31.8K** ops/sec | **-14.5%** |

### CSharpDB Collection (in-memory)

| Metric | v2.0 | v2.2 | Delta |
|--------|------|------|-------|
| Single Put | **~413.6K** ops/sec | **~519.3K** ops/sec | **+25.6%** |
| Batched PUT | **~809.9K** docs/sec | **~932.9K** docs/sec | **+15.2%** |
| Point Get (cold) | **~596.3K** ops/sec | **~597.7K** ops/sec | **+0.2%** |

## Summary

**Write throughput improved dramatically across the board in v2.2:**
- File-backed single writes: up 44-63%
- File-backed batched writes: up 39-68%
- In-memory single writes: up 25-26%
- In-memory batched writes: up 15-52%
- Concurrent reader throughput (8r): up 333-844%

**Point lookups have regressed slightly:**
- File-backed cold lookups: down 14-18%
- In-memory SQL cold lookups: down 16%
- In-memory Collection cold lookups: flat (+0.2%)

The write path improvements in v2.2 more than recover the regressions seen in v2.0 vs. the original March 12 baseline, with single INSERT and batched INSERT now exceeding the original numbers. The cold lookup regression is modest and may warrant investigation.
