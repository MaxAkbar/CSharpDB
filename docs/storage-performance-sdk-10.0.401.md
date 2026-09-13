# Storage performance verification on SDK 10.0.401

The subsequent [Phase 2 profile and checkpoint change](storage-performance-phase2.md) records checkpoint allocation hotspots, retained-snapshot measurements, broader test validation, and completed paired comparisons. Targeted checkpoint copying takes 61–67% less time, while application results remain mixed: random-key batch throughput rises about 3% and the single-row control falls about 4%. The Phase 1 measurements below remain unchanged.

The Phase 1 changes produce repeatable savings in record encoding, resident-cache replacement, and large disordered-page compaction. These measurements do **not** establish a general database-throughput improvement. Durable and in-memory application workloads remain close to baseline, with mixed paired results and occasional adverse tail latency.

Measured locally on 2026-09-12 on `version4.6.5`. Baseline storage source is commit `1b2bc1facf0e2934777917903f21e0dbb70a4f00`; candidate storage is the frozen Phase 1 implementation identified by the assembly hash below. Both were built in Release with SDK 10.0.401 and ran on .NET 10.0.12, Windows 11, and an i9-11900K (8 cores / 16 logical processors). Only `CSharpDB.Storage.dll` differs between the isolated runners; benchmark code and other dependency assemblies are identical.

## Component results

The table reports the range of changes across two paired passes, with execution order reversed in the second pass. Lower operation time is better. These improvements are workload-specific and cannot be added together.

| Operation | Observed improvement |
| --- | ---: |
| Encode 4 columns, 16-character text | 26.0–27.4% less time |
| Encode 4 columns, 256-character text | 11.7–21.1% less time |
| Encode 32 columns, 256-character text | 21.4–23.5% less time |
| Update an existing LRU cache entry | 30.4–32.2% less time; allocation falls from 32 B to 0 B per update |
| Compact 128 cells, random order | 2.7–2.8× faster |
| Compact 128 cells, descending order | 5.8–5.9× faster |
| Compact 256 cells, random order | 4.4–4.8× faster |
| Compact 256 cells, descending order | 10.1–10.3× faster |
| Compact already ordered 128/256-cell pages | 6.2–13.6% less time |

Both passes' means improved for the three encoding cases, but the weaker second-pass 4-column/256-character comparison had overlapping confidence intervals. The ranges describe observed measurements, not guaranteed gains on every run.

The 16-cell controls were approximately unchanged: observed time changes ranged from −5.9% to +5.0%. The ascending case was 1.3–2.4% slower, with overlapping reported uncertainty. There is no reliable general WAL speedup claim: the one-frame memory-WAL result changed direction between passes, and the 100-frame result's 3.8–10.7% time reductions have broad overlapping uncertainty. Its fixed 64 invocations make measured intervals particularly short.

The 32-column, 16-character encoding case is retained in the raw results but excluded from the clean encoder improvement range. Its text cache uses object-identity hashes, and the processes had different collision behavior. Pass 1 allocations were 944/784 B per operation; pass 2 was 704/464 B. Removing a sizing traversal can avoid allocations when collisions occur, but differing collisions also affect this comparison. A universal per-record allocation reduction cannot be inferred. Allocations matched between versions for the three encoding cases in the table.

The compaction probe preserves the chosen logical/physical ordering while repeatedly compacting an already contiguous page. It isolates sorting and compaction bookkeeping, rather than measuring arbitrary fragmented-payload movement or the speed of an entire database operation. The cache probe measures resident replacements, not cache misses or eviction. WAL-read-cache replacement was not benchmarked separately.

## Database and read results

Each value below is the median of the paired percentage changes; brackets show the smallest and largest paired change. They are observed ranges, not confidence intervals. Positive throughput is better. Five pairs were collected for writes, and three for reads. Paired medians need not equal the percentage difference between independently computed baseline and candidate rate medians.

| Workload | Median paired throughput change [range] |
| --- | ---: |
| Durable single-row SQL, low-latency preset | +2.39% [−2.90%, +3.99%] |
| Durable 100-row batch, low-latency preset | +0.28% [−3.91%, +3.05%] |
| Durable 1,000-row batch, random keys, write-optimized preset | +1.84% [−0.50%, +2.58%] |
| In-memory SQL, rotating 100-row batches | +0.70% [−4.51%, +1.64%] |
| In-memory collection, rotating 100-row batches | −0.08% [−3.34%, +4.10%] |
| Scan 10,000 rows, read-ahead off | +0.33% [−0.34%, +0.60%] |
| Scan 10,000 rows, read-ahead on | +0.16% [−2.21%, +0.26%] |
| Scan 100,000 rows, read-ahead off | +0.15% [−2.64%, +1.23%] |
| Scan 100,000 rows, read-ahead on | +1.35% [+0.05%, +1.82%] |

These small differences do not support the earlier planning estimate of a general 5–10% gain. No consistent material throughput regression was established either. Read throughput remained within roughly −3% to +2% across individual pairs; these were repeated warm filesystem-cache scans, not cold-storage tests.

Tail latency was mixed. In single-row pair 2, P99 increased from **4.938 ms to 9.980 ms (+102.1%)**, with 2,806 baseline and 2,725 candidate samples. Pair 4 increased 20.2%. The median paired single-row P99 change was −0.37%, illustrating why the median alone is insufficient. These samples do not establish a repeatable regression, but uniformly improved tail latency cannot be claimed. Full P95/P99 values and every individual pair are retained in the linked results.

Durable single-row/100-row runs retained 2,576–2,828 latency samples; random 1,000-row runs retained 272–281. In-memory runs retained 112,380–180,968. The 100,000-row scan runs retained only 98–116 samples, so their P99 estimates are particularly fragile. In-memory results include the harness's database rotation after 100,000 rows and associated creation/disposal costs.

## Method and validation

- All 50 benchmark processes completed with exit code 0: 64 BenchmarkDotNet case executions (16 cases × 2 versions × 2 passes), and 37 paired application/read measurements covering nine workloads. Evidence validation found no missing or structurally invalid expected results.
- Component runs used BenchmarkDotNet 0.15.8, the in-process toolchain in separate baseline/candidate processes, five warmups, ten measured iterations, and a requested 300 ms iteration time. The memory-WAL cases retain their 64-invocation override. No builds ran concurrently with accepted measurements.
- Durable scenarios explicitly used `CSHARPDB_BENCH_DURABILITY=Durable`, fresh databases, two seconds of warmup, and at least ten seconds of measurement. Version order alternated between pairs. In-memory scenarios used two seconds of warmup and ten seconds of measurement; scan scenarios used two and five seconds respectively.
- The new builds passed the byte-equivalence probe over 10,000 mixed records and 36 compacted page layouts. Every scan also checked its count/key/value checksum outside measurement. The earlier regular core suite passed all 2,948 tests; it was not rerun during timing because storage source was unchanged.
- The initial run under heavy background load was interrupted and excluded. After the user stopped that workload, accepted samples passed coarse pre-sample CPU checks without an override. A rounding issue in that check was identified; independent double-precision observations were retained, and the reusable script was corrected after execution. The exact executed script is preserved separately. This is a local diagnostic comparison, not continuous environment certification or release qualification.

The result supports the targeted CPU and allocation optimizations. Further database-level gains need evidence from the workloads and remaining bottlenecks that dominate actual application time; this run does not justify a larger overall performance claim.

## Retained evidence

Local evidence is in the ignored directory [storage-performance-sdk10401-20260912-154015](../.tmp/storage-performance-sdk10401-20260912-154015/):

- [Complete tables and individual pairs](../.tmp/storage-performance-sdk10401-20260912-154015/summary.md)
- [Machine-readable results, uncertainty, and source hashes](../.tmp/storage-performance-sdk10401-20260912-154015/summary.json)
- [Assembly comparison manifest](../.tmp/storage-performance-sdk10401-20260912-154015/assembly-manifest.json)
- [Executed comparison script](../.tmp/storage-performance-sdk10401-20260912-154015/Run-Comparisons.executed.ps1)
- [Raw run events](../.tmp/storage-performance-sdk10401-20260912-154015/run-events.jsonl)

Baseline Storage SHA-256: `3A17150C689C05506FA374B05DE0E416AC2529822C37A4ED57A56BC5EC30B395`.

Candidate Storage SHA-256: `4D874D944073354C42F4C1BB4728A44BA4D1D120068972675254DA9D92A00898`.

Both byte-equivalence probes produced SHA-256 `68132C7FDD2AE4B9A649FC4D7D2E54E8F1E27C2E41372C5549A32FC2EAFFBB78`.
