# Storage performance: shared WAL snapshot maps

This follow-up removes repeated page-map copies when readers acquire snapshots at the same WAL index state and with the same minimum-offset filter. It follows the retained-WAL measurements in [the Phase 2 report](storage-performance-phase2.md). Timing runs remain deferred; this change does not establish a new throughput or latency result.

## Implementation and ownership

`WalIndex.TakeSnapshot` lazily copies the live page map and computes its minimum WAL offset on the first acquisition. Later acquisitions reuse that private, immutable dictionary and minimum offset. Each acquisition still creates a distinct `WalSnapshot` wrapper with the current commit counter, preserving independent reader registration and disposal in `CheckpointCoordinator`.

The cache has one slot, keyed by the exact nullable minimum WAL offset. Changing the filter replaces that slot. Every content mutation invalidates it under the existing index lock before changing the live map: individual recovered/appended frames, both live publication paths, reset, replacement and overwrite. This does not rely on the commit counter, because checkpoint and recovery maintenance can change offsets without advancing that counter. Capacity-only changes and counter-only advances can reuse the map; each wrapper captures the current counter separately.

Existing readers retain their old dictionary after invalidation. The copied dictionary is private and exposes only lookup through the snapshot; no production code mutates it. The unused internal snapshot remapping method and unused snapshot filtering helper were removed. Public APIs, database and WAL formats, durability, checkpoint policy and reader lifetime rules are unchanged. Checkpoint finalization still waits for readers that retain WAL frames.

## Expected benefit and limits

The retained baseline measured approximately 283 KB allocated and 75 microseconds for a reader-session lifecycle with 10,000 retained pages. Those are historical baseline observations, not new measurements of this implementation.

Repeated acquisitions at one state/filter now avoid the dictionary copy and minimum-offset scan. The first acquisition after a content change or a filter change still performs both operations. This primarily targets several readers per committed state; one reader after every write or frequently alternating filters can receive little benefit. It does not address the durable-flush bottleneck or establish a fix for the earlier single-row and tail-latency regressions.

The index retains at most one copied map even after its last reader is disposed, until invalidation, a filter change or index collection. Older maps survive only while referenced by existing snapshots. This trades bounded retained memory for avoiding repeated allocations; the one slot is bounded in count, not in bytes, and its size still follows the retained page map.

## Validation

On 2026-09-12, SDK 10.0.401, the full core Release test module passed **2,985/2,985 tests**, with zero failures and zero skips in one invocation. This includes 31 new cases covering shared-map reuse with distinct reader wrappers, every invalidation path, same-counter state replacement, filtered and empty snapshots, old-snapshot isolation, concurrent commit publication, and checkpoint retention through the last relevant reader's disposal. Existing concurrency, rollback and recovery coverage also passed.

The crash-test helper was rebuilt first with zero warnings/errors so its five process-crash durability tests used the updated storage code. The source output, core test output and crash helper all contained the same Storage assembly, SHA-256 `A80628B041902778CD0C7927AFFBC8D91F9816E802FFA747DA073560ACD4E36A`. The four implementation/test source hashes were unchanged during validation. Map-identity assertions check reuse without relying on timing or GC thresholds.

Command: `dotnet test --project tests/CSharpDB.Tests/CSharpDB.Tests.csproj -c Release --no-restore --output Normal --report-trx --results-directory .tmp/storage-snapshot-sharing-20260912-220119/test-results`.

The [test log](../.tmp/storage-snapshot-sharing-20260912-220119/core-tests.log), [TRX](../.tmp/storage-snapshot-sharing-20260912-220119/test-results/CSharpDB.Tests_net10.0_x64.trx), [verified summary](../.tmp/storage-snapshot-sharing-20260912-220119/verified-test-summary.json), source hashes, crash-helper build log and candidate Storage DLL/PDB are retained in the ignored local evidence directory. This is core correctness validation, not a full-solution rerun or performance/release qualification.

## Deferred measurements

The next timing comparison should separate first acquisition from repeated acquisitions, include empty/1,000/10,000-page maps and changing filters, and measure reader sessions alongside concurrent writer throughput and P95/P99. Keep the preserved Phase 1 and checkpoint candidate binaries unchanged. Qualify the earlier checkpoint control regression separately before making an overall performance claim.
