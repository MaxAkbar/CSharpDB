# Performance Guardrail Report

- Baseline: `C:\Users\maxim\source\Code\CSharpDB\tests\CSharpDB.Benchmarks\baselines\20260302-001757`
- Note: one or more checks use per-check `baselineSnapshot` overrides
- Current: `C:\Users\maxim\source\Code\CSharpDB\BenchmarkDotNet.Artifacts\\results`
- Thresholds: `C:\Users\maxim\AppData\Local\Temp\perf-thresholds-query-plan-plus-distinct.json`
- Generated (UTC): 2026-03-04 00:50:57Z

Compared 5 rows against baseline. PASS=4, FAIL=1

| CSV | Key | Mean Δ% | Alloc Δ% | Alloc Δ B | Status |
|---|---|---:|---:|---:|---|
| CSharpDB.Benchmarks.Micro.DistinctBenchmarks-report.csv | Method='SELECT DISTINCT value ORDER BY value + LIMIT 100'; RowCount=1000 | 11.70 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.DistinctBenchmarks-report.csv | Method='SELECT DISTINCT value ORDER BY value + LIMIT 100'; RowCount=10000 | -0.14 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.DistinctBenchmarks-report.csv | Method='SELECT DISTINCT value'; RowCount=1000 | 7.59 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.DistinctBenchmarks-report.csv | Method='SELECT DISTINCT value'; RowCount=10000 | -1.75 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.QueryPlanCacheBenchmarks-report.csv | <missing baseline file> |  |  |  | FAIL |

## Select Plan Cache Diagnostics

| Run | Sample | Hits | Misses | Reclassifications | Stores | Entries |
|---|---:|---:|---:|---:|---:|---:|
| Micro (*Micro.QueryPlanCacheBenchmarks*) | 1 | 16384 | 1 | 0 | 1 | 1 |
| Micro (*Micro.QueryPlanCacheBenchmarks*) | 2 | 16384 | 1 | 0 | 1 | 1 |
| Micro (*Micro.QueryPlanCacheBenchmarks*) | 3 | 0 | 8193 | 0 | 8193 | 1024 |
