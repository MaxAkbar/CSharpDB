# Storage performance: checkpoint reads and retained snapshots

This follow-up profiles the Phase 1 storage implementation on SDK 10.0.401 / .NET 10.0.12 and evaluates one additional change: combining adjacent WAL frames within a partially contiguous checkpoint batch. It also measures retained-WAL snapshot costs to establish the next optimization target.

**The checkpoint change reduces targeted copy time by 61–67% and copy-path allocation by about 70%, but the application results are mixed.** Random-key 1,000-row durable batches improved by a median 3.15% across five pairs. Single-row durable writes regressed by a median 3.96% across three pairs, and median P99 increased in both workloads. These results support a substantial component improvement, not an unconditional database-wide performance claim.

The completed comparison uses three alternating process pairs for checkpoint copying (four WAL layouts), followed by five pairs of complete random-key 1,000-row durable batches and three single-row durable control pairs. All results compare the additional checkpoint change against the frozen Phase 1 implementation, rather than the original main branch. Measurements ran on 2026-09-12 local time on Windows 11 / i9-11900K, with recorded desktop background activity. They are not idle-machine measurements; the conditions and earlier rejected attempts are described below.

## Measured checkpoint copying

Each process performs two warmups and eleven measured samples per layout. Each timed sample copies 1,024 pages (4 MiB) in 64 output batches from a real file-backed WAL into a preallocated memory device. The timer excludes fixture appends, checkpoint header validation and state preparation, buffer allocation, destination growth, finalization, truncation and fsync. Every sample checks all 1,040 prepared/output page images and WAL reset. This isolates warm file-cache read/copy overhead. [Method and commands](../.tmp/storage-phase2-profile-20260912-172454/checkpoint-probe/README.md) explain the boundaries.

The comparison unit is a separate process pair, not each of the eleven correlated samples. Positive time reductions mean faster copying; negative reductions mean slower copying. Allocation columns are medians of the three per-process medians, in bytes per 4 MiB copy.

| WAL layout | Median paired time reduction | Paired range | Baseline allocation | Candidate allocation |
| --- | ---: | ---: | ---: | ---: |
| Fully contiguous control | -4.73% | -16.57% to +1.70% | 21,496 B | 21,496 B |
| Adjacent runs of four pages | **66.06%** | +63.64% to +66.66% | 167,928 B | 49,144 B |
| All singleton reads control | -2.71% | -8.45% to -1.15% | 167,928 B | 167,776 B |
| Overwritten pages splitting interior runs | **63.98%** | +60.63% to +65.79% | 167,928 B | 50,168 B |

The targeted layouts copy roughly 2.5–3 times faster, with about 70% less measured allocation. Neither control shows an improvement. The singleton control is slower in all three pairs; the unchanged contiguous fast path also varies materially. These adverse results remain visible and limit claims about small differences. Default runtime tiering remains enabled and layouts run in a fixed order. Process-wide allocation counters can include runtime background work.

All 264 measured samples and eight validation-only samples passed content/count checks. The [component audit](../.tmp/storage-phase2-profile-20260912-172454/quiet-rerun-20260912-202254/checkpoint-results-04/component-audit.md) retains process medians, paired results, raw ranges, allocations, identities and correctness evidence. Validation-only timings are excluded from performance calculations.

## Complete durable-write workloads

Each process uses a fresh database, a two-second warmup and a ten-second measurement interval, with one durable flush per measured commit. Version order alternates within pairs. No timing samples or slower runs were excluded.

| Workload | Pairs | Median paired throughput change | Pair range | Baseline median P99 | Candidate median P99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Random-key batches of 1,000 rows | 5 | **+3.15%** | -2.26% to +6.61% | 179.679 ms | 184.325 ms |
| Single-row durable commits | 3 | **-3.96%** | -5.09% to -1.31% | 4.909 ms | 5.381 ms |

Four of five random-key pairs improve throughput; all three single-row pairs regress. P99 worsens in four of five random-key pairs and all three single-row pairs. P99 values are per-run histogram quantiles, and the table reports the median of those quantiles, not pooled request latencies. Pair ranges are observed ranges, not confidence intervals. The [validated durable comparison](../.tmp/storage-phase2-profile-20260912-172454/checkpoint-comparison/paired-summary.md) includes every pair, while its [JSON](../.tmp/storage-phase2-profile-20260912-172454/checkpoint-comparison/paired-summary.json) retains diagnostics, hashes and environment checks.

The component gain has only a modest effect on this batch workload, which also pays for SQL execution, WAL header validation and durable I/O. The control and latency regressions need investigation before treating the change as an overall performance win. These desktop measurements cannot distinguish the contribution of the code change from scheduling, storage latency and other background activity at the scale of a few percent.

Single-row diagnostics help locate the slowdown without establishing its cause. Average durable-flush times rise from baseline to candidate in every pair: 3.540 to 3.592 ms, 3.367 to 3.498 ms, and 3.385 to 3.572 ms. Measured flush time accounts for approximately 93.6–93.9% of elapsed time, and every single-row run starts exactly two background checkpoints during its ten-second measurement. Longer flush times account for most of the mean-latency difference; these counters cannot separate storage/desktop variation from indirect checkpoint effects. The next investigation should isolate that control regression before claiming a general gain.

## Why checkpoint reads

Three whole-process EventPipe traces covered random-key 1,000-row durable batches before/after Phase 1 and a Phase 1 single-row durable workload. They include startup, warmup, measurement and cleanup; they cannot attribute every event to a measured transaction or explain the earlier isolated P99 spike.

In the Phase 1 random-key trace, allocation samples for two async read paths account for **35.34% of allocation-tick weights**: per-page header reads in `RequiresCommittedStateRebuildAsync`, and per-page checkpoint copy reads beneath `FlushCheckpointBatchCoreAsync`. Including the checkpoint method's own async boxes brings the sampled weight share to 38.59%. These are sampled estimates, not exact allocated byte counts or predicted speedups.

GC-related suspension totals 23.21 ms over the candidate's 12.55-second trace, about 0.185%; the pre-Phase-1 trace has 21.61 ms over 12.78 seconds. This points toward reducing repeated I/O and async work rather than treating GC pauses as the main cause of this workload's elapsed time. The single-row trace repeatedly samples the durable flush path, consistent with earlier commit diagnostics.

The managed sampling profile includes blocked threads. Its stack percentages are **not CPU utilization**, and no contention events were captured, so these traces do not rule out lock contention. See the [official profiler documentation](https://learn.microsoft.com/en-us/dotnet/core/diagnostics/dotnet-trace) and [retained trace analysis](../.tmp/storage-phase2-profile-20260912-172454/trace-analysis/findings.md) for event counts, allocation stacks and limitations.

## Checkpoint change

The existing checkpoint code groups up to 16 adjacent database pages. Previously, if any selected WAL offset broke contiguity, all pages in that batch were read individually. The change finds adjacent runs within that batch and reads each run together. Isolated pages retain individual reads; already contiguous batches retain their existing fast path. Existing buffers, locking, checksums, checkpoint finalization and durability boundaries are preserved. This addresses checkpoint copy reads; the separate per-page header-validation reads remain a follow-up opportunity.

Six new correctness cases exercise single pages, the 16/17-page boundary, larger batches, replacements inside runs, reverse append order, sparse page IDs, and commits arriving after checkpoint copying. Each checks complete page images and retained commit behavior through finalization. The complete core module passes **2,954 tests**. The existing record/page byte-equivalence probe still produces `68132C7FDD2AE4B9A649FC4D7D2E54E8F1E27C2E41372C5549A32FC2EAFFBB78`.

## Retained-WAL snapshot measurements

The following are medians of seven batches using normal Pager/Engine APIs in a memory-backed fixture, with automatic checkpoints disabled. Session and transaction timings include disposal but execute no SQL. The fixture deliberately separates retained page-map size from schema and query complexity.

| Operation | Retained pages | Time | Allocated bytes |
| --- | ---: | ---: | ---: |
| Reader session lifecycle | 0 | 0.22 microseconds | 376 |
| Reader session lifecycle | 1,000 | 6.67 microseconds | 31,416 |
| Reader session lifecycle | 10,000 | 74.93 microseconds | 283,416 |
| Independent write transaction lifecycle | 0 | 8.60 microseconds | 11,856 |
| Independent write transaction lifecycle | 1,000 | 7.56 microseconds | 42,896 |
| Independent write transaction lifecycle | 10,000 | 104.63 microseconds | 294,896 |

These characterize current costs, not before/after improvements. The write transaction API is `BeginWriteTransactionAsync`; the separate legacy `BeginTransactionAsync` path does not acquire this snapshot. Small timing differences, including the non-monotonic transaction controls, should not be interpreted as improvements.

For each map size, three alternating-order paired comparisons used 300 writer commits at a 5 ms target interval and either zero or eight continuously renewing reader threads. Every sample also retained one old snapshot, including the writer-alone control. With 10,000 initially retained pages, writer P99 was approximately **15.6–15.8 ms with readers**, versus **0.06–0.07 ms alone**. This combines allocation, GC, scheduling and locking effects; the probe does not apportion their contributions. It is not a disk-backed application latency claim. Every old-snapshot and repeated-read isolation check passed.

The next substantial snapshot opportunity is avoiding a dictionary clone for every reader at the same committed state. That requires an ownership design: active snapshots currently remap retained WAL offsets in place during checkpoint compaction, so sharing their mutable dictionaries would be incorrect. Fusing a minimum-offset scan alone would leave the dominant allocation intact.

The [snapshot harness and method](../.tmp/storage-phase2-profile-20260912-172454/snapshot-probe/README.md), [corrected sequential samples](../.tmp/storage-phase2-profile-20260912-172454/snapshot-probe/results-sequential-corrected/results.json), and [concurrency samples](../.tmp/storage-phase2-profile-20260912-172454/snapshot-probe/results/results.json) are retained locally. The original direct microprobe allowed JIT allocation elision for discarded empty snapshots; the corrected sequential rerun forces those objects to escape. The actual Engine lifecycle and concurrency results did not depend on that discarded-object probe.

## Evidence boundaries

All artifacts are local under `.tmp/storage-phase2-profile-20260912-172454`, which is ignored by Git. Raw traces, frozen assemblies, source hashes, harnesses, logs and individual samples are retained there. Phase 1's broader results remain in [the earlier paired report](storage-performance-sdk-10.0.401.md).

The checkpoint experiment compares the frozen Phase 1 Storage assembly (`4D874D944073354C42F4C1BB4728A44BA4D1D120068972675254DA9D92A00898`) with the additional checkpoint change (`9D52FF6EB0EF489451C84B82FA255A36B5590D83F874AFAC619F99E40B7E1CB6`). The durable runner and dependencies match; only Storage DLL/PDB differ. Instrumented traces are used for diagnosis, not as new throughput comparisons.

Before timing, an earlier check found `cod` consuming about eight CPU cores and total background use around 11.3 cores, so those runs were paused. After the user closed the heavy workload, an initial one-total-core gate still rejected several attempts. Process-path inspection established that the remaining processes named `ChatGPT.exe` belonged to the **OpenAI.Codex installation**, correcting the earlier attribution to the separate ChatGPT app. The [original precheck](../.tmp/storage-phase2-profile-20260912-172454/checkpoint-probe/cpu-precheck.json), rejected attempts `checkpoint-results` through `checkpoint-results-03`, and [verified process identities](../.tmp/storage-phase2-profile-20260912-172454/quiet-rerun-20260912-202254/codex-process-identity.json) are retained. Earlier validation-only processes did not contribute performance measurements.

The revised, explicitly disclosed policy accepted a process only after a three-second sample showed at most **one non-Codex background core and two total background cores**. Codex membership used its verified executable path; unknown paths counted as other activity. Each process could retry up to twelve checks, waiting five seconds after rejection. The successful component run accepted eight checks with observed total activity of 1.31–1.92 cores and retained seven rejected checks. The durable run accepted sixteen checks at 1.21–1.98 observed total cores and retained twenty-seven rejections. There were no overlapping benchmark processes or builds/tests. The checks establish conditions immediately before launch, not continuous background-load measurements throughout each run; process churn and inaccessible CPU counters also limit complete accounting.

## Package compatibility and broader validation

The full Release solution build passed with zero warnings and errors. The subsequent full-solution test invocation exercised 7,457 tests: 7,418 passed, 35 failed and four opt-in external-fixture cases were skipped. The failed modules were diagnosed and rerun separately; the original failed evidence is preserved.

- ASP.NET OpenAPI 10.0.12 requires the OpenAPI 2.x API surface, so the direct package reference is now 2.12.0. The Prometheus exporter is aligned to OpenTelemetry 1.18.0; its old serializer referenced a removed internal helper and returned HTTP 500 for histogram scrapes. The final API module passes 190/190 tests.
- EF package references again use the shared version property, with both it and `dotnet-ef` at 10.0.12. The existing consistency guard passes all 12 references. The generated migration corpus differed only in three current `ProductVersion` literals; after updating those, exact SQL comparison and replay pass in the 153/153-test EF module. Historical C# migration snapshots were preserved.
- SQL Server packaging's exact dependency list and SNI license filenames were synchronized with the already upgraded packages. The package set remains the same 24 names, and the SNI license hash is unchanged. Exact dependency and license checks remain enforced. The CLI module passes 416 tests with its one opt-in Access case skipped.
- The original inherited PATH was 10,505 characters; child batch launchers could not resolve PowerShell. A 177-character tool PATH applied only to the rerun process restored command lookup. This required no persistent machine configuration change. The Daemon rerun then passed 246/248 tests; two release-script fixtures exceeded their existing deadlines. Their timeout diagnostics now retain bounded output and report the actual configured duration.

A serial targeted rerun passed the wrapper fixture with its original limit. An earlier exact-master release-script rerun reached its 300-second limit while making steady progress: the retained snapshot had 110/120 recorded simulated runs completed, at about 2.56 seconds per run. It executes fake processes, not storage benchmarks. After the user closed the heavy background workload, the final targeted rerun **passed the exact-master fixture**, with one selected test, one pass and zero failures; the module completed in about 141 seconds. The 300-second child-process limit was unchanged. See the [verified result](../.tmp/storage-phase2-profile-20260912-172454/quiet-rerun-20260912-202254/verified-test-summary.json) and [retained test log](../.tmp/storage-phase2-profile-20260912-172454/quiet-rerun-20260912-202254/release-harness.log).

All previously failing tests now have passing outcomes: **7,453 passed and four opt-in skips across the full run and subsequent targeted reruns**. This is not a clean single full-suite invocation. Neither production qualification rules nor timeout limits were relaxed. The [earlier timeout diagnosis and retained progress](../.tmp/storage-phase2-profile-20260912-172454/daemon-timeout-diagnostics/findings.md) remain available.

The later CPU check identified a `cod` process whose start time was 17:50:18 local, before the serial timeout reproductions. Its CPU usage during those earlier windows was not recorded. Those tests were serialized relative to other builds/tests, but were **not verified to run on an otherwise idle machine**. The passing rerun resolves the outstanding failure without establishing a precise causal contribution from background load; its initial CPU sample still showed about two cores of background activity.

Compatibility evidence is in [OpenAPI/EF validation](../.tmp/storage-phase2-profile-20260912-172454/openapi-validation/), [SQL Server packaging validation](../.tmp/storage-phase2-profile-20260912-172454/sqlserver-packaging-compatibility/findings.md), and the [full-solution test log](../.tmp/storage-phase2-profile-20260912-172454/solution-tests.log). These local checks do not constitute publishing or release qualification.
