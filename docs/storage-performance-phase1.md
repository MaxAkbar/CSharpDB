# Storage performance: Phase 1

A subsequent [paired verification on SDK 10.0.401](storage-performance-sdk-10.0.401.md) confirmed targeted encoding/cache/compaction savings, while application throughput remained broadly unchanged within the observed variation. It records all repeated results, latency outliers, and measurement limits. The original SDK 10.0.203 measurements below are preserved as historical evidence.

Measured locally on 2026-09-12, on `version4.6.5`, against commit `1b2bc1fa` before the storage edits. Phase 1 reduces encoding, cache replacement, compaction, and WAL publication overhead. Public APIs, persisted formats, durability settings, cache defaults, and checkpoint policy are unchanged. These results support the component optimizations; complete release qualification remains pending.

The implementation uses the existing size-aware encoder overload, updates existing LRU and WAL-read cache entries in place, and retains insertion sort for at most 16 cells while sorting larger compaction lists with a comparison sort. Already ordered pages retain a linear fast path. Live WAL publication now updates the map and commit counters under one lock after the existing flush boundary. Recovery retains its separate counter semantics. Both cursor benchmarks now dispose their cursors.

Local BenchmarkDotNet observations are below. Lower time is better. These improvements are workload-specific and must not be added together.

| Operation | Before | After | Observation |
| --- | ---: | ---: | --- |
| Encode: 4 columns, 16-character text | 60.9 ns | 47.4 ns | 22% less time |
| Encode: 4 columns, 256-character text | 180.5 ns | 166.2 ns | 8% less time |
| Encode: 32 columns, 16-character text | 459.7 ns | 352.9 ns | 23% less time |
| Encode: 32 columns, 256-character text | 1.131 us | 0.986 us | 13% less time |
| Existing LRU entry update | 28.7 ns / 32 B | 20.2 ns / 0 B | 30% less time; replacement allocation removed |
| Compact 128 cells, random insertion order | 4.403 us | 1.782 us | 2.5x faster |
| Compact 256 cells, random insertion order | 17.610 us | 3.664 us | 4.8x faster |
| Compact 256 cells, descending insertion order | 31.576 us | 3.411 us | 9.3x faster |

Small compaction lists were approximately unchanged; already ordered 128/256-cell pages improved modestly. Encoding allocations were unchanged. Memory-WAL commits remained noisy even after moving buffer-capacity growth outside measurement: 100-frame means were 44.45 us before and 42.85 us after, with overlapping uncertainty. The single-frame sample also had substantial uncertainty and was slower in the refined run. No WAL throughput improvement is claimed from those samples.

Three paired repetitions of the existing durable SQL benchmark, with fresh databases, two seconds of warmup and ten seconds of measurement per scenario, produced the following medians. Execution order was baseline/candidate, candidate/baseline, baseline/candidate. Durability was explicitly set to `Durable` for both versions. Rows per second accounts for each scenario's batch size; latency measures a commit/batch.

| Durable SQL workload | Before rows/s | After rows/s | Change | Before / after P95 | Before / after P99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Single-row auto-commit, analyzed, low latency preset | 266 | 270 | +1.7% | 4.12 / 4.08 ms | 5.26 / 6.69 ms |
| 100-row batch, low latency preset | 25,847 | 26,481 | +2.5% | 4.09 / 4.20 ms | 7.57 / 5.73 ms |
| 1,000-row batch, random keys, write optimized preset | 24,799 | 25,432 | +2.6% | 173.43 / 160.56 ms | 192.81 / 193.95 ms |

The database-level changes are inconclusive: run-to-run variation exceeds the small median throughput gains, disk flushes dominate the low-latency scenarios, and single-row P99 increased about 27%. This is not a passed durable-performance gate. Controlled qualification is required before attributing the throughput or tail-latency differences to the patch.

Read controls used the same corrected cursor harness on both assemblies. An initial 10,000-row scan without read-ahead was 12% slower; a longer repeat with reversed version order did not reproduce it. In that repeat, 10,000-row scans were 4.797 / 4.706 ms without read-ahead and 4.153 / 4.402 ms with read-ahead; 100,000-row scans were 53.883 / 51.103 ms and 47.972 / 45.170 ms respectively. Results remain mixed, including a 6% slowdown in one control, rather than establishing a general read-performance gain.

Validation passed for 234 focused tests, including 43 new cases. Coverage includes eviction notification order and failure, dirty-page retention, compaction boundaries and overlapping moves, concurrent snapshots, B-tree deletes/overflow, rollback, failed commits, grouped durable commits, checkpoint/recovery diagnostics, saturating counters, and five process-crash scenarios. A separate comparison of 10,000 mixed records and 36 compacted page layouts produced identical bytes on both versions.

At measurement time, regular test and benchmark builds were blocked by the preceding NuGet update: `CSharpDB.Generators` referenced Roslyn 5.9, while SDK 10.0.203 ran compiler 5.3. CS9057 prevented generator execution, followed by missing generated `Collection` members. The focused validation projects linked the actual repository test/benchmark sources and used xUnit 4.0.1 and BenchmarkDotNet 0.15.8. The process-crash tests used a freshly compiled copy of the existing crash harness.

The repository subsequently moved to SDK 10.0.401, whose compiler is Roslyn 5.9, retaining the generator's updated 5.9.0 packages. Both regular Release builds now pass with zero warnings/errors, including when CS9057 is promoted to an error. All 2,948 tests in the regular `CSharpDB.Tests` assembly passed through its xUnit executable, with no failures or skips. Build and execution evidence for the SDK update is retained in [sdk-10.0.401-validation-20260912](../.tmp/sdk-10.0.401-validation-20260912/). The performance measurements above were made with SDK 10.0.203; the separate SDK 10.0.401 verification linked above was collected later.

The xUnit 4 runner configuration is also resolved: `global.json` selects Microsoft.Testing.Platform, and CI and release qualification use its project/solution, reporting, and diagnostic options. All 2,948 core tests passed again through `dotnet test --project tests/CSharpDB.Tests/CSharpDB.Tests.csproj -c Release --no-build`, with hang diagnostics and TRX reporting enabled. A temporary validation solution passed another 225 tests with `--max-parallel-test-modules 1`: 72 DevOps tests, 134 observability tests, and 19 packaging-asset tests linked from the actual daemon test source. This confirms serial module execution and separate TRX files. The old test argument `--maxcpucount:1` caused a zero-test run and was removed. Logs, TRX files, and the temporary validation projects are retained in [mtp-runner-fix-20260912](../.tmp/mtp-runner-fix-20260912/). Full-solution validation remains blocked by a separate API build error: `Microsoft.AspNetCore.OpenApi` 10.0.12 requires `Microsoft.OpenApi` below 3.0.0, while the package update selected 3.10.2, causing CS0200 in generated XML-comment support.

Evidence is retained locally in the ignored folder [storage-performance-phase1-20260912-134556](../.tmp/storage-performance-phase1-20260912-134556/): focused project files, build/test logs, xUnit XML, benchmark JSON/CSV, paired SQL samples, the compatibility probe, and `Summarize-Results.ps1`. Only `CSharpDB.Storage.dll` differs between the paired SQL runner directories.

- Baseline Storage SHA-256: `501039DEF73C6168D49AE247DA9200ED2BE58B8A1235294CBD329E44DCAB5EB9`
- Candidate Storage SHA-256: `491D7E6E9CCAB5A6F93097ED16D2BD78409F61B355136349F66AB8462712C179`
- Compatibility output SHA-256: `68132C7FDD2AE4B9A649FC4D7D2E54E8F1E27C2E41372C5549A32FC2EAFFBB78`

Measurements used an i9-11900K, Windows 11, .NET 10.0.12, and Release builds. Microbenchmarks ran in separate baseline/candidate processes with the in-process BenchmarkDotNet toolchain to prevent accidental rebuilding against the other version. Compaction used three warmups and seven 100 ms iterations; refined encoding/cache runs used seven 300 ms iterations in reversed version order. Final cursor controls used five warmups and ten 500 ms iterations. The memory-WAL probe uses 64 invocations per iteration and prewarms capacity. Short-iteration warnings and noisy samples limit precision. Attribute-added default cursor jobs could not generate projects in the first runner and were excluded; the final cursor-only runner supplies one explicit job and all four cases completed on both versions.

Before release acceptance, resolve the remaining full-solution package incompatibilities and run the paired PR/release guardrails with the new SDK. Larger snapshot-map, conflict-index, mapping-window, and checkpoint redesigns remain Phase 2 work.
