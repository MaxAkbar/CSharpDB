# Performance Guardrail Report

- Baseline: `C:\Users\maxim\source\Code\CSharpDB\tests\CSharpDB.Benchmarks\baselines\20260302-001757`
- Note: one or more checks use per-check `baselineSnapshot` overrides
- Current: `C:\Users\maxim\source\Code\CSharpDB\BenchmarkDotNet.Artifacts\\results`
- Thresholds: `C:\Users\maxim\source\Code\CSharpDB\tests\CSharpDB.Benchmarks\perf-thresholds.json`
- Generated (UTC): 2026-03-03 19:36:55Z

Compared 88 rows against baseline. PASS=88, FAIL=0

| CSV | Key | Mean Δ% | Alloc Δ% | Alloc Δ B | Status |
|---|---|---:|---:|---:|---|
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Batch INSERT x100 in transaction'; PreSeededRows=100 | -38.55 | -12.71 | -30,863 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Batch INSERT x100 in transaction'; PreSeededRows=1000 | -36.55 | -12.70 | -30,843 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Batch INSERT x100 in transaction'; PreSeededRows=10000 | -45.66 | -12.69 | -30,833 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Batch INSERT x1000 in transaction'; PreSeededRows=100 | -32.77 | -12.49 | -301,199 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Batch INSERT x1000 in transaction'; PreSeededRows=1000 | -26.72 | -12.49 | -301,486 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Batch INSERT x1000 in transaction'; PreSeededRows=10000 | -31.48 | -12.49 | -301,414 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Single INSERT (auto-commit)'; PreSeededRows=100 | -59.33 | -22.40 | -1,126 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Single INSERT (auto-commit)'; PreSeededRows=1000 | -58.31 | -22.40 | -1,126 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Single INSERT (auto-commit)'; PreSeededRows=10000 | -61.17 | -22.40 | -1,126 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Single INSERT in explicit transaction'; PreSeededRows=100 | -58.92 | -20.25 | -983 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Single INSERT in explicit transaction'; PreSeededRows=1000 | -54.98 | -20.00 | -973 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Single INSERT in explicit transaction'; PreSeededRows=10000 | -60.58 | -20.21 | -983 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='CROSS JOIN 100x100' | -94.23 | -66.84 | -1,976,914 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN 1Kx1K (forced nested-loop)' | -72.69 | -99.91 | -288,168,991 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN 1Kx1K LIMIT 1' | -81.15 | -60.52 | -243,036 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN 1Kx1K' | -79.59 | -66.61 | -587,244 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN 1Kx20K (no swap via view)' | -63.06 | -52.33 | -5,215,222 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN 1Kx20K (planner swap build side)' | -73.57 | -77.57 | -7,327,212 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN 20Kx1K (natural build side)' | -67.69 | -77.57 | -7,327,191 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN on right PK (forced hash)' | -57.09 | -50.97 | -494,725 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN on right PK (index nested-loop)' | -55.51 | -63.79 | -344,340 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN with filter' | -77.34 | -39.85 | -330,865 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='LEFT JOIN 1Kx1K' | -80.73 | -66.60 | -587,100 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='RIGHT JOIN on left PK (forced hash)' | -42.05 | -27.79 | -270,029 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='RIGHT JOIN on left PK (rewritten index nested-loop)' | -22.95 | -22.14 | -119,757 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value (index-order scan)'; RowCount=1000 | 2.82 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value (index-order scan)'; RowCount=10000 | 2.45 | -0.00 | -82 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value (index-order scan)'; RowCount=100000 | 7.46 | -0.00 | -338 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value (no index)'; RowCount=1000 | 3.17 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value (no index)'; RowCount=10000 | 22.26 | 0.01 | 266 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value (no index)'; RowCount=100000 | -3.46 | 0.00 | 143 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value + LIMIT 100 (index-order scan)'; RowCount=1000 | -15.86 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value + LIMIT 100 (index-order scan)'; RowCount=10000 | -3.58 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value + LIMIT 100 (index-order scan)'; RowCount=100000 | -14.37 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value + LIMIT 100 (no index)'; RowCount=1000 | 0.43 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value + LIMIT 100 (no index)'; RowCount=10000 | -9.81 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value + LIMIT 100 (no index)'; RowCount=100000 | -14.89 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv | Method='SELECT by PK with residual conjunct'; RowCount=1000 | -37.62 | -50.87 | -1,081 | PASS |
| CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv | Method='SELECT by PK with residual conjunct'; RowCount=10000 | -47.85 | -60.63 | -1,517 | PASS |
| CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv | Method='SELECT by PK with residual conjunct'; RowCount=100000 | -51.72 | -62.17 | -1,579 | PASS |
| CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv | Method='SELECT by primary key'; RowCount=1000 | -29.43 | -34.77 | -396 | PASS |
| CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv | Method='SELECT by primary key'; RowCount=10000 | -27.67 | -40.25 | -539 | PASS |
| CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv | Method='SELECT by primary key'; RowCount=100000 | -39.41 | -41.13 | -559 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Hash SUM(value) via GROUP BY 1'; RowCount=1000 | 13.71 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Hash SUM(value) via GROUP BY 1'; RowCount=10000 | 11.51 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Hash SUM(value) via GROUP BY 1'; RowCount=100000 | 25.95 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar AVG(value)'; RowCount=1000 | 6.07 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar AVG(value)'; RowCount=10000 | -2.13 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar AVG(value)'; RowCount=100000 | 10.23 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar COUNT(DISTINCT value)'; RowCount=1000 | 0.61 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar COUNT(DISTINCT value)'; RowCount=10000 | 3.01 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar COUNT(DISTINCT value)'; RowCount=100000 | -0.59 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar COUNT(value)'; RowCount=1000 | -3.22 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar COUNT(value)'; RowCount=10000 | 4.78 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar COUNT(value)'; RowCount=100000 | 21.06 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar MAX(value)'; RowCount=1000 | -1.06 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar MAX(value)'; RowCount=10000 | -3.13 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar MAX(value)'; RowCount=100000 | 22.07 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar MIN(value)'; RowCount=1000 | 13.00 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar MIN(value)'; RowCount=10000 | 18.28 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar MIN(value)'; RowCount=100000 | -2.17 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar SUM(DISTINCT value)'; RowCount=1000 | -0.46 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar SUM(DISTINCT value)'; RowCount=10000 | 9.67 | -0.00 | -1 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar SUM(DISTINCT value)'; RowCount=100000 | -2.85 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar SUM(value)'; RowCount=1000 | 1.43 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar SUM(value)'; RowCount=10000 | 7.74 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar SUM(value)'; RowCount=100000 | 0.01 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='Index lookup scalar COUNT(text_col)'; RowCount=1000 | 0.19 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='Index lookup scalar COUNT(text_col)'; RowCount=10000 | 6.78 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='Index lookup scalar COUNT(text_col)'; RowCount=100000 | 6.84 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='Index lookup scalar SUM(id)'; RowCount=1000 | 2.46 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='Index lookup scalar SUM(id)'; RowCount=10000 | 5.58 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='Index lookup scalar SUM(id)'; RowCount=100000 | 7.23 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='PK lookup scalar COUNT(text_col)'; RowCount=1000 | 7.22 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='PK lookup scalar COUNT(text_col)'; RowCount=10000 | 6.14 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='PK lookup scalar COUNT(text_col)'; RowCount=100000 | 3.31 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='PK lookup scalar SUM(value)'; RowCount=1000 | 6.45 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='PK lookup scalar SUM(value)'; RowCount=10000 | 10.99 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='PK lookup scalar SUM(value)'; RowCount=100000 | 4.11 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='100-row batch commit'; WalFramesBeforeCheckpoint=100 | -38.88 | -12.85 | -30,577 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='100-row batch commit'; WalFramesBeforeCheckpoint=1000 | -37.11 | -12.85 | -30,577 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='100-row batch commit'; WalFramesBeforeCheckpoint=500 | -37.96 | -12.84 | -30,556 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='Manual checkpoint after N writes'; WalFramesBeforeCheckpoint=100 | 16.30 | -12.07 | -31,345 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='Manual checkpoint after N writes'; WalFramesBeforeCheckpoint=1000 | -6.83 | -12.72 | -303,452 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='Manual checkpoint after N writes'; WalFramesBeforeCheckpoint=500 | 4.68 | -12.66 | -152,494 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='Single-row commit (WAL flush)'; WalFramesBeforeCheckpoint=100 | -62.64 | -21.37 | -829 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='Single-row commit (WAL flush)'; WalFramesBeforeCheckpoint=1000 | -64.25 | -21.84 | -850 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='Single-row commit (WAL flush)'; WalFramesBeforeCheckpoint=500 | -54.08 | -21.47 | -840 | PASS |
