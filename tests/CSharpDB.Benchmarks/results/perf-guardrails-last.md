# Performance Guardrail Report

- Baseline: `C:\Users\maxim\source\Code\CSharpDB\tests\CSharpDB.Benchmarks\baselines\20260302-001757`
- Note: one or more checks use per-check `baselineSnapshot` overrides
- Current: `C:\Users\maxim\source\Code\CSharpDB\BenchmarkDotNet.Artifacts\\results`
- Thresholds: `C:\Users\maxim\source\Code\CSharpDB\tests\CSharpDB.Benchmarks\perf-thresholds.json`
- Generated (UTC): 2026-03-04 13:59:38Z

Compared 88 rows against baseline. PASS=88, FAIL=0

| CSV | Key | Mean Δ% | Alloc Δ% | Alloc Δ B | Status |
|---|---|---:|---:|---:|---|
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Batch INSERT x100 in transaction'; PreSeededRows=100 | -33.82 | -12.71 | -30,863 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Batch INSERT x100 in transaction'; PreSeededRows=1000 | -35.19 | -12.70 | -30,843 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Batch INSERT x100 in transaction'; PreSeededRows=10000 | -38.23 | -12.69 | -30,833 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Batch INSERT x1000 in transaction'; PreSeededRows=100 | -30.00 | -12.49 | -301,189 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Batch INSERT x1000 in transaction'; PreSeededRows=1000 | -26.70 | -12.49 | -301,486 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Batch INSERT x1000 in transaction'; PreSeededRows=10000 | -21.43 | -12.49 | -301,414 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Single INSERT (auto-commit)'; PreSeededRows=100 | -56.14 | -22.20 | -1,116 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Single INSERT (auto-commit)'; PreSeededRows=1000 | -57.14 | -22.20 | -1,116 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Single INSERT (auto-commit)'; PreSeededRows=10000 | -60.12 | -22.20 | -1,116 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Single INSERT in explicit transaction'; PreSeededRows=100 | -56.77 | -20.04 | -973 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Single INSERT in explicit transaction'; PreSeededRows=1000 | -52.20 | -20.00 | -973 | PASS |
| CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv | Method='Single INSERT in explicit transaction'; PreSeededRows=10000 | -57.44 | -20.00 | -973 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='CROSS JOIN 100x100' | -94.50 | -66.84 | -1,976,914 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN 1Kx1K (forced nested-loop)' | -74.49 | -99.91 | -288,168,991 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN 1Kx1K LIMIT 1' | -80.62 | -60.52 | -243,036 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN 1Kx1K' | -78.74 | -66.61 | -587,244 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN 1Kx20K (no swap via view)' | -63.77 | -52.33 | -5,215,273 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN 1Kx20K (planner swap build side)' | -71.65 | -77.57 | -7,327,212 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN 20Kx1K (natural build side)' | -68.82 | -77.57 | -7,327,181 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN on right PK (forced hash)' | -58.13 | -50.97 | -494,725 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN on right PK (index nested-loop)' | -55.85 | -63.79 | -344,340 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='INNER JOIN with filter' | -77.82 | -39.85 | -330,865 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='LEFT JOIN 1Kx1K' | -81.40 | -66.60 | -587,100 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='RIGHT JOIN on left PK (forced hash)' | -42.40 | -27.79 | -270,029 | PASS |
| CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv | Method='RIGHT JOIN on left PK (rewritten index nested-loop)' | -29.95 | -22.14 | -119,757 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value (index-order scan)'; RowCount=1000 | -0.97 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value (index-order scan)'; RowCount=10000 | -14.93 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value (index-order scan)'; RowCount=100000 | -3.00 | 0.00 | 41 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value (no index)'; RowCount=1000 | 5.20 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value (no index)'; RowCount=10000 | -1.09 | 0.01 | 184 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value (no index)'; RowCount=100000 | -11.73 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value + LIMIT 100 (index-order scan)'; RowCount=1000 | -18.33 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value + LIMIT 100 (index-order scan)'; RowCount=10000 | -6.56 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value + LIMIT 100 (index-order scan)'; RowCount=100000 | -12.91 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value + LIMIT 100 (no index)'; RowCount=1000 | -3.77 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value + LIMIT 100 (no index)'; RowCount=10000 | -8.00 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv | Method='ORDER BY value + LIMIT 100 (no index)'; RowCount=100000 | -14.01 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv | Method='SELECT by PK with residual conjunct'; RowCount=1000 | -40.04 | -50.87 | -1,081 | PASS |
| CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv | Method='SELECT by PK with residual conjunct'; RowCount=10000 | -48.15 | -60.63 | -1,517 | PASS |
| CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv | Method='SELECT by PK with residual conjunct'; RowCount=100000 | -52.09 | -62.17 | -1,579 | PASS |
| CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv | Method='SELECT by primary key'; RowCount=1000 | -30.73 | -34.77 | -396 | PASS |
| CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv | Method='SELECT by primary key'; RowCount=10000 | -28.72 | -40.25 | -539 | PASS |
| CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv | Method='SELECT by primary key'; RowCount=100000 | -39.69 | -41.13 | -559 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Hash SUM(value) via GROUP BY 1'; RowCount=1000 | -7.42 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Hash SUM(value) via GROUP BY 1'; RowCount=10000 | 6.35 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Hash SUM(value) via GROUP BY 1'; RowCount=100000 | -9.65 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar AVG(value)'; RowCount=1000 | -3.58 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar AVG(value)'; RowCount=10000 | -4.31 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar AVG(value)'; RowCount=100000 | -9.84 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar COUNT(DISTINCT value)'; RowCount=1000 | -8.93 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar COUNT(DISTINCT value)'; RowCount=10000 | -14.12 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar COUNT(DISTINCT value)'; RowCount=100000 | -8.29 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar COUNT(value)'; RowCount=1000 | 0.43 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar COUNT(value)'; RowCount=10000 | -6.90 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar COUNT(value)'; RowCount=100000 | -4.02 | -0.14 | -1 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar MAX(value)'; RowCount=1000 | -3.01 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar MAX(value)'; RowCount=10000 | -6.54 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar MAX(value)'; RowCount=100000 | -18.70 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar MIN(value)'; RowCount=1000 | -3.04 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar MIN(value)'; RowCount=10000 | -0.59 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar MIN(value)'; RowCount=100000 | -10.86 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar SUM(DISTINCT value)'; RowCount=1000 | -8.99 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar SUM(DISTINCT value)'; RowCount=10000 | -7.17 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar SUM(DISTINCT value)'; RowCount=100000 | -11.51 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar SUM(value)'; RowCount=1000 | -2.53 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar SUM(value)'; RowCount=10000 | -5.32 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv | Method='Scalar SUM(value)'; RowCount=100000 | -18.40 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='Index lookup scalar COUNT(text_col)'; RowCount=1000 | -6.91 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='Index lookup scalar COUNT(text_col)'; RowCount=10000 | -2.93 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='Index lookup scalar COUNT(text_col)'; RowCount=100000 | -2.43 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='Index lookup scalar SUM(id)'; RowCount=1000 | -5.97 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='Index lookup scalar SUM(id)'; RowCount=10000 | -3.15 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='Index lookup scalar SUM(id)'; RowCount=100000 | -3.35 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='PK lookup scalar COUNT(text_col)'; RowCount=1000 | -3.18 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='PK lookup scalar COUNT(text_col)'; RowCount=10000 | -5.35 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='PK lookup scalar COUNT(text_col)'; RowCount=100000 | -3.57 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='PK lookup scalar SUM(value)'; RowCount=1000 | -5.88 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='PK lookup scalar SUM(value)'; RowCount=10000 | -3.50 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.ScalarAggregateLookupBenchmarks-report.csv | Method='PK lookup scalar SUM(value)'; RowCount=100000 | -3.69 | 0.00 | 0 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='100-row batch commit'; WalFramesBeforeCheckpoint=100 | -11.65 | -12.83 | -30,536 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='100-row batch commit'; WalFramesBeforeCheckpoint=1000 | -34.12 | -12.83 | -30,536 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='100-row batch commit'; WalFramesBeforeCheckpoint=500 | -33.37 | -12.84 | -30,556 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='Manual checkpoint after N writes'; WalFramesBeforeCheckpoint=100 | 18.74 | -12.07 | -31,345 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='Manual checkpoint after N writes'; WalFramesBeforeCheckpoint=1000 | -13.81 | -12.72 | -303,483 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='Manual checkpoint after N writes'; WalFramesBeforeCheckpoint=500 | 5.00 | -12.65 | -152,463 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='Single-row commit (WAL flush)'; WalFramesBeforeCheckpoint=100 | -60.31 | -20.84 | -809 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='Single-row commit (WAL flush)'; WalFramesBeforeCheckpoint=1000 | -68.10 | -21.58 | -840 | PASS |
| CSharpDB.Benchmarks.Micro.WalBenchmarks-report.csv | Method='Single-row commit (WAL flush)'; WalFramesBeforeCheckpoint=500 | -66.91 | -21.99 | -860 | PASS |
