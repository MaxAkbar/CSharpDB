# CSharpDB Version 2.0 Benchmark Results

Captured: March 19, 2026

## Benchmark Results vs. Master Comparison Table (March 12, 2026)

### Metric Mapping

Per the README: point lookups use `ColdLookupBenchmarks` (cache-pressured), in-memory single writes use `InMemory*Benchmarks` micros, file-backed writes use `SustainedWriteBenchmark`, batched writes use `InMemoryBatchBenchmark`.

### CSharpDB SQL (file-backed)

| Metric | Old (Mar 12) | New (Mar 19) | Delta |
|--------|-------------|-------------|-------|
| Single INSERT | **24.4K** ops/sec | **17.7K** ops/sec | **-27.5%** |
| Batched INSERT | **~684K** rows/sec | **~417.6K** rows/sec | **-38.9%** |
| Point Lookup (cold) | **~36.2K** ops/sec | **~38.0K** ops/sec | **+5.0%** |
| Concurrent Reads (8r) | ~153 / ~4.75K | 112,913 / 1,854,122 | *see note below* |

### CSharpDB SQL (in-memory)

| Metric | Old (Mar 12) | New (Mar 19) | Delta |
|--------|-------------|-------------|-------|
| Single INSERT | **~360K** ops/sec | **~285.6K** ops/sec | **-20.7%** |
| Batched INSERT | **~1.67M** rows/sec | **~951.7K** rows/sec | **-43.0%** |
| Point Lookup (cold) | **~582K** ops/sec | **~604.2K** ops/sec | **+3.8%** |

### CSharpDB Collection (file-backed)

| Metric | Old (Mar 12) | New (Mar 19) | Delta |
|--------|-------------|-------------|-------|
| Single Put | **30.2K** ops/sec | **21.3K** ops/sec | **-29.5%** |
| Batched PUT | **~441K** docs/sec | **~290.7K** docs/sec | **-34.1%** |
| Point Get (cold) | **~35.0K** ops/sec | **~37.2K** ops/sec | **+6.3%** |

### CSharpDB Collection (in-memory)

| Metric | Old (Mar 12) | New (Mar 19) | Delta |
|--------|-------------|-------------|-------|
| Single Put | **~408K** ops/sec | **~413.6K** ops/sec | **+1.4%** |
| Batched PUT | **~1.07M** docs/sec | **~809.9K** docs/sec | **-24.3%** |
| Point Get (cold) | **~554K** ops/sec | **~596.3K** ops/sec | **+7.6%** |

## Summary

**Point lookups are stable or slightly improved** (+3.8% to +7.6% across all modes).

**Write throughput has regressed significantly across the board:**
- File-backed single writes: down 27-30%
- File-backed batched writes: down 34-39%
- In-memory single writes: down 21% (SQL) / flat (Collection)
- In-memory batched writes: down 24-43%

**Concurrent Reads note:** The old values (153 / 4.75K) represented `COUNT(*)` aggregate queries. The new reader scaling benchmark reports individual read operations instead, producing numbers ~740x/~390x higher. These are **not directly comparable** -- the benchmark semantics have changed.
