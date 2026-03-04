# Performance Guardrail Report

- Baseline: `C:\Users\maxim\source\Code\CSharpDB\tests\CSharpDB.Benchmarks\baselines\20260302-001757`
- Note: one or more checks use per-check `baselineSnapshot` overrides
- Current: `C:\Users\maxim\source\Code\CSharpDB\BenchmarkDotNet.Artifacts\\results`
- Thresholds: `C:\Users\maxim\source\Code\CSharpDB\tests\CSharpDB.Benchmarks\perf-thresholds.json`
- Generated (UTC): 2026-03-04 02:32:15Z

Compared 105 rows against baseline. PASS=104, FAIL=1

| CSV | Key | Mean Δ% | Alloc Δ% | Alloc Δ B | Status |
|---|---|---:|---:|---:|---|
| CSharpDB.Benchmarks.Micro.CompositeIndexBenchmarks-report.csv | Method='WHERE a+b (composite index)'; RowCount=10000 | 12.81 | 10.43 | 123 | PASS |
| CSharpDB.Benchmarks.Micro.CompositeIndexBenchmarks-report.csv | Method='WHERE a+b (composite index)'; RowCount=100000 | 14.68 | 10.43 | 123 | PASS |
| CSharpDB.Benchmarks.Micro.CompositeIndexBenchmarks-report.csv | Method='WHERE a+b (no index)'; RowCount=10000 | 3.50 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.CompositeIndexBenchmarks-report.csv | Method='WHERE a+b (no index)'; RowCount=100000 | 3.08 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.CompositeIndexBenchmarks-report.csv | Method='WHERE a+b (single-column index)'; RowCount=10000 | -7.23 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.CompositeIndexBenchmarks-report.csv | Method='WHERE a+b (single-column index)'; RowCount=100000 | -2.67 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.DistinctBenchmarks-report.csv | Method='SELECT DISTINCT value ORDER BY value + LIMIT 100'; RowCount=1000 | -4.04 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.DistinctBenchmarks-report.csv | Method='SELECT DISTINCT value ORDER BY value + LIMIT 100'; RowCount=10000 | 14.32 | -0.00 | -10 | PASS |
| CSharpDB.Benchmarks.Micro.DistinctBenchmarks-report.csv | Method='SELECT DISTINCT value'; RowCount=1000 | -0.61 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.DistinctBenchmarks-report.csv | Method='SELECT DISTINCT value'; RowCount=10000 | 3.82 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Batch INSERT x100 in transaction'; PreSeededRows=100 | -33.04 | -12.71 | -30,863 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Batch INSERT x100 in transaction'; PreSeededRows=1000 | -28.02 | -12.70 | -30,843 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Batch INSERT x100 in transaction'; PreSeededRows=10000 | -37.44 | -12.69 | -30,833 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Batch INSERT x1000 in transaction'; PreSeededRows=100 | -27.75 | -12.49 | -301,189 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Batch INSERT x1000 in transaction'; PreSeededRows=1000 | -22.95 | -12.49 | -301,486 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Batch INSERT x1000 in transaction'; PreSeededRows=10000 | -26.41 | -12.49 | -301,414 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Single INSERT (auto-commit)'; PreSeededRows=100 | -42.80 | -21.79 | -1,096 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Single INSERT (auto-commit)'; PreSeededRows=1000 | -41.21 | -21.79 | -1,096 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Single INSERT (auto-commit)'; PreSeededRows=10000 | -42.88 | -21.79 | -1,096 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Single INSERT in explicit transaction'; PreSeededRows=100 | -42.61 | -19.62 | -952 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Single INSERT in explicit transaction'; PreSeededRows=1000 | -40.95 | -19.79 | -963 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Single INSERT in explicit transaction'; PreSeededRows=10000 | -45.96 | -19.79 | -963 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='CROSS JOIN 100x100' | -94.25 | -66.84 | -1,976,914 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN 1Kx1K (forced nested-loop)' | -73.51 | -99.91 | -288,168,991 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN 1Kx1K LIMIT 1' | -77.72 | -60.52 | -243,036 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN 1Kx1K' | -80.40 | -66.61 | -587,244 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN 1Kx20K (no swap via view)' | -65.29 | -52.33 | -5,215,232 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN 1Kx20K (planner swap build side)' | -71.33 | -77.57 | -7,327,212 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN 20Kx1K (natural build side)' | -69.28 | -77.57 | -7,327,181 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN on right PK (forced hash)' | -56.23 | -50.97 | -494,725 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN on right PK (index nested-loop)' | -50.70 | -63.79 | -344,340 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN with filter' | -77.08 | -39.85 | -330,865 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='LEFT JOIN 1Kx1K' | -81.21 | -66.60 | -587,100 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='RIGHT JOIN on left PK (forced hash)' | -41.80 | -27.79 | -270,029 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='RIGHT JOIN on left PK (rewritten index nested-loop)' | -31.09 | -22.14 | -119,757 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value (index-order scan)'; RowCount=1000 | 1.45 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value (index-order scan)'; RowCount=10000 | 0.54 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value (index-order scan)'; RowCount=100000 | -1.99 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value (no index)'; RowCount=1000 | 4.13 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value (no index)'; RowCount=10000 | 0.46 | 0.01 | 328 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value (no index)'; RowCount=100000 | -8.66 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value + LIMIT 100 (index-order scan)'; RowCount=1000 | -16.05 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value + LIMIT 100 (index-order scan)'; RowCount=10000 | 0.12 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value + LIMIT 100 (index-order scan)'; RowCount=100000 | -11.72 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value + LIMIT 100 (no index)'; RowCount=1000 | -1.45 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value + LIMIT 100 (no index)'; RowCount=10000 | 0.49 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value + LIMIT 100 (no index)'; RowCount=100000 | -0.54 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv | Method='SELECT by PK with residual conjunct'; RowCount=1000 | -26.44 | -50.78 | -1,079 | PASS |
| CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv | Method='SELECT by PK with residual conjunct'; RowCount=10000 | -35.14 | -60.59 | -1,516 | PASS |
| CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv | Method='SELECT by PK with residual conjunct'; RowCount=100000 | -49.20 | -62.17 | -1,579 | PASS |
| CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv | Method='SELECT by primary key'; RowCount=1000 | -22.82 | -34.59 | -394 | PASS |
| CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv | Method='SELECT by primary key'; RowCount=10000 | -21.40 | -40.18 | -538 | PASS |
| CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv | Method='SELECT by primary key'; RowCount=100000 | -31.64 | -41.13 | -559 | PASS |
| CSharpDB.Benchmarks.Micro.QueryPlanCacheBenchmarks-report.csv | Method='Pre-parsed statement (plan cache hit)'; RowCount=10000 | 4.87 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.QueryPlanCacheBenchmarks-report.csv | Method='Stable SQL text (statement+plan cache hits)'; RowCount=10000 | -5.45 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.QueryPlanCacheBenchmarks-report.csv | Method='Varying SQL text (limited plan reuse)'; RowCount=10000 | 1.74 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Hash SUM(value) via GROUP BY 1'; RowCount=1000 | -2.84 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Hash SUM(value) via GROUP BY 1'; RowCount=10000 | 0.49 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Hash SUM(value) via GROUP BY 1'; RowCount=100000 | 3.29 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar AVG(value)'; RowCount=1000 | 9.35 | -34.09 | -240 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar AVG(value)'; RowCount=10000 | -1.94 | -34.09 | -240 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar AVG(value)'; RowCount=100000 | 4.96 | -33.99 | -240 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar COUNT(DISTINCT value)'; RowCount=1000 | 1.39 | -0.32 | -240 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar COUNT(DISTINCT value)'; RowCount=10000 | -7.20 | -0.12 | -240 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar COUNT(DISTINCT value)'; RowCount=100000 | 17.76 | -0.12 | -240 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar COUNT(value)'; RowCount=1000 | 2.06 | -33.71 | -240 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar COUNT(value)'; RowCount=10000 | 1.00 | -33.71 | -240 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar COUNT(value)'; RowCount=100000 | 23.82 | -33.61 | -240 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar MAX(value)'; RowCount=1000 | 0.65 | -34.09 | -240 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar MAX(value)'; RowCount=10000 | -1.01 | -34.09 | -240 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar MAX(value)'; RowCount=100000 | 6.56 | -33.99 | -240 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar MIN(value)'; RowCount=1000 | 2.25 | -34.09 | -240 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar MIN(value)'; RowCount=10000 | 5.66 | -34.09 | -240 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar MIN(value)'; RowCount=100000 | 31.39 | -33.99 | -240 | FAIL |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar SUM(DISTINCT value)'; RowCount=1000 | 2.43 | -0.32 | -240 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar SUM(DISTINCT value)'; RowCount=10000 | 3.71 | -0.12 | -241 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar SUM(DISTINCT value)'; RowCount=100000 | 29.23 | -0.13 | -257 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar SUM(value)'; RowCount=1000 | 2.05 | -34.09 | -240 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar SUM(value)'; RowCount=10000 | 0.98 | -34.09 | -240 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar SUM(value)'; RowCount=100000 | 5.57 | -33.99 | -240 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='Index lookup scalar COUNT(text_col)'; RowCount=1000 | -31.83 | -35.59 | -477 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='Index lookup scalar COUNT(text_col)'; RowCount=10000 | -33.57 | -35.59 | -477 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='Index lookup scalar COUNT(text_col)'; RowCount=100000 | -40.22 | -35.59 | -477 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='Index lookup scalar SUM(id)'; RowCount=1000 | -35.63 | -36.41 | -481 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='Index lookup scalar SUM(id)'; RowCount=10000 | -36.43 | -36.41 | -481 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='Index lookup scalar SUM(id)'; RowCount=100000 | -36.92 | -36.41 | -481 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='PK lookup scalar COUNT(text_col)'; RowCount=1000 | -40.85 | -39.71 | -516 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='PK lookup scalar COUNT(text_col)'; RowCount=10000 | -37.23 | -39.71 | -516 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='PK lookup scalar COUNT(text_col)'; RowCount=100000 | -37.00 | -39.71 | -516 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='PK lookup scalar SUM(value)'; RowCount=1000 | -40.07 | -40.15 | -510 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='PK lookup scalar SUM(value)'; RowCount=10000 | -40.29 | -40.15 | -510 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='PK lookup scalar SUM(value)'; RowCount=100000 | -43.01 | -40.15 | -510 | PASS |
| CSharpDB.Benchmarks.Micro.TextIndexBenchmarks-report.csv | Method='WHERE text eq (no index)'; RowCount=10000 | -5.25 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.TextIndexBenchmarks-report.csv | Method='WHERE text eq (no index)'; RowCount=100000 | -15.62 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.TextIndexBenchmarks-report.csv | Method='WHERE text eq (text index)'; RowCount=10000 | -2.52 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.TextIndexBenchmarks-report.csv | Method='WHERE text eq (text index)'; RowCount=100000 | -9.71 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='100-row batch commit'; WalFramesBeforeCheckpoint=100 | -29.71 | -12.85 | -30,577 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='100-row batch commit'; WalFramesBeforeCheckpoint=1000 | -25.33 | -12.83 | -30,536 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='100-row batch commit'; WalFramesBeforeCheckpoint=500 | -23.32 | -12.82 | -30,515 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='Manual checkpoint after N writes'; WalFramesBeforeCheckpoint=100 | 16.18 | -12.07 | -31,345 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='Manual checkpoint after N writes'; WalFramesBeforeCheckpoint=1000 | -8.13 | -12.72 | -303,493 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='Manual checkpoint after N writes'; WalFramesBeforeCheckpoint=500 | 0.77 | -12.65 | -152,463 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='Single-row commit (WAL flush)'; WalFramesBeforeCheckpoint=100 | -59.63 | -20.84 | -809 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='Single-row commit (WAL flush)'; WalFramesBeforeCheckpoint=1000 | -60.61 | -21.32 | -829 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='Single-row commit (WAL flush)'; WalFramesBeforeCheckpoint=500 | -64.07 | -21.73 | -850 | PASS |

## Select Plan Cache Diagnostics

| Run | Sample | Hits | Misses | Reclassifications | Stores | Entries |
|---|---:|---:|---:|---:|---:|---:|
| Micro (*Micro.QueryPlanCacheBenchmarks*) | 1 | 8192 | 1 | 0 | 1 | 1 |
| Micro (*Micro.QueryPlanCacheBenchmarks*) | 2 | 8192 | 1 | 0 | 1 | 1 |
| Micro (*Micro.QueryPlanCacheBenchmarks*) | 3 | 0 | 8193 | 0 | 8193 | 1024 |
