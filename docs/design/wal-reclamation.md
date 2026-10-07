# Draft WAL recovery boundary and bounded reclamation

Use an explicit durable recovery boundary to retire checkpointed WAL bytes,
then reclaim the file through a quiescent full drain and generation reset.
Two independently checksummed control records protect the boundary. This is a
proposal for an isolated prototype; the production WAL format and behavior
remain unchanged. No performance improvement has been established.

The immediate target is the recurring cost of relocating a retained suffix
after an incremental checkpoint. Current measurements show approximately
10.46 MiB moved through 167 read/write pairs per affected checkpoint. Simply
removing relocation is unsafe: an invalid indexed or superseded prefix frame
can stop recovery before a newer committed suffix. The current implementation
passes both cases. See the [benchmark history](../../tests/CSharpDB.Benchmarks/HISTORY.md)
and [recovery regressions](../../tests/CSharpDB.Tests/WalCheckpointBatchTests.cs).

## Proposed format and compatibility

Keep the database format unchanged for the initial prototype. Use a distinct
WAL magic, tentatively `CWA2`, with two 4,096-byte control slots at offsets 0 and
4,096. Frame data starts at `H = 8,192`. Retain the existing 4,120-byte frame
shape initially; its salt pair identifies the current WAL generation. These
are prototype choices, not a published format specification.

Each slot contains the magic, format version, recognized feature flags, page
and frame sizes, data start, file identity, monotonically increasing sequence,
epoch, generation salts, mode, replay floor and checkpoint page count. The
replay floor is an absolute physical offset aligned relative to `H`. A SHA-256
digest covers the complete slot except its digest field, including reserved
bytes. The isolated codec below specifies and tests these fields; this remains
an experimental layout rather than a published format contract.

Recovery validates both slots independently. A torn slot is unavailable. With
two valid slots, identities and layouts must agree; equal sequences with
different payloads are an error. Select the highest sequence. A checksummed
but unsupported version, unknown mode or impossible boundary is an error, not
permission to choose an older record. No valid record means recovery stops
without modifying the input. Sequence or epoch exhaustion also stops writes.

Two modes are required:

| Mode | Recovery behavior | Writer admission |
|---|---|---|
| ACTIVE | Replay this generation from its physical floor | Allowed only after both current ACTIVE records are durable |
| EMPTY | Trust the fully flushed database and ignore all physical frame bytes | Closed until truncation and ACTIVE publication finish |

The EMPTY mode prevents stale or invalid bytes from a previous generation from
being mistaken for a new log while reset is interrupted. Before a writer is
admitted, both slots must describe ACTIVE for that generation. Before obsolete
bytes are physically removed, both must describe EMPTY for the new generation.

A version bump with the old `CWAL` magic is insufficient. The current
[file recovery path](../../src/CSharpDB.Storage/Wal/WriteAheadLog.cs) and
[memory recovery path](../../src/CSharpDB.Storage/Wal/MemoryWriteAheadLog.cs)
do not reject an unfamiliar version before replay. A probe of the unchanged
file-backed reader accepted version 77 with `CWAL`, but rejected `CWA2` before
changing the input. This verifies that reader's magic guard, not compatibility
with every historical release or a complete new-format codec.

The first prototype creates fresh scratch databases only. No automatic upgrade,
downgrade, mixed-format live file, or modification of an already-open database
is allowed. Existing version-one databases continue using their existing path.
An eventual migration needs its own interruption and old-binary tests.

## Retiring a copied prefix

Use the existing checkpoint copy checks and writer/admission barriers. Do not
relax reader isolation in the first prototype. Let `F` be the captured published
boundary, `T` the current physical published tail, and `E` the generation.

1. Complete and flush the database copy through `F`. Later committed frames
   between `F` and `T` remain in the WAL.
2. Under finalization barriers, verify the captured generation and boundary
   still apply. Preserve the existing pending-commit and reader checks.
3. Write slot A as ACTIVE for `E` with floor `F` and the next sequence. Flush it.
4. Write slot B with the same epoch, salts, floor and mode, and a higher sequence.
   Flush it separately.
5. Retire the copied prefix from the live index and complete finalization.
   Keep retained frame offsets and the published physical tail unchanged.

Normal retirement does not move, truncate or overwrite any frame bytes. An
interruption before both records persist leaves either the previous boundary
or the new one. From valid input, either can replay the acknowledged state.
If an old boundary encounters a corrupt active prefix, recovery must fail
explicitly rather than return a silently shortened view. After successful
retirement, losing either control slot still leaves a boundary beyond the
obsolete invalid prefix covered by the new regression cases.

This changes corruption handling for the proposed format. It does not grant
permission to change version-one recovery semantics. Torn/uncommitted tails,
commit markers and partial frame writes need separate new-format parser tests;
the abstract model below treats each frame as a complete committed record.

## Reclaiming physical space

Retirement alone does not reclaim bytes. At the budget, stop new writer
admission and reach a quiescent state with no active transaction, pending commit
or reader snapshot. If readers cannot drain within the configured wait, return
Busy before appending; never invalidate their views. Do not wait for reader
release while holding the coordinator lock required by that release.

The first prototype uses a full drain instead of copying a suffix in place or
depending on file replacement and directory durability:

| Step | Required durable state before proceeding |
|---|---|
| Copy every remaining committed page | Database flush completes through `T` |
| Publish EMPTY for generation `E + 1` | Slot A is written and flushed, then slot B is written and flushed |
| Truncate to `H` | Both EMPTY records are durable; truncate and flush the WAL |
| Publish ACTIVE for `E + 1`, floor `H` | Slot A is written and flushed, then slot B is written and flushed |
| Resume writers | Both ACTIVE records are durable; initialize empty live index and physical append position |

The new generation gets new salts. The file identity stays fixed. Each slot
write advances the sequence, including changes of mode. A recovered EMPTY
state must finish truncation and mirrored ACTIVE publication before accepting
writes. A recovered ACTIVE state with a stale or missing peer record must repair
that peer before destructive maintenance or writer admission.

Recovery must establish durability even when the readable slots already match.
A process-only crash can leave unflushed control bytes in the OS cache. Flush
the WAL before trusting a matching pair for truncation or admission. When a peer
needs repair, flushing that repair also flushes the selected record. If any
write, flush or truncation fails, keep admission closed and require reopen.

The second ACTIVE flush is necessary. If a writer were admitted after only one
ACTIVE record persisted, failure of that record could make recovery select
EMPTY and ignore the newly acknowledged commit. Likewise, truncating before
EMPTY is mirrored can leave only an old boundary referring to removed bytes.

This reset adds four control-record flushes and one truncation flush after the
database flush. Ordinary retirement adds two control-record flushes. Their
cost must be measured; reducing data movement does not establish a latency win.
The full drain may create a rarer, larger pause, which still counts in P99 and
maximum-latency qualification.

## Physical size budget and backpressure

Start the isolated integration experiment with a fixed 64 MiB WAL budget,
including `H`. This number is an experimental parameter, not a new default.
Keep separate values for physical append position, allocated file extent,
retired prefix bytes, live frames and outstanding append reservations.

Reserve space before issuing any append. Check the physical append position
plus all outstanding reservations and the entire new request, using checked
arithmetic. Preallocation must stay within the budget. Retirement reduces live
work but never reduces the physical extent used by this check.

At the limit, a quiescent full drain can make room. Inside an active transaction,
do not start a nested checkpoint or silently exceed the budget: reject further
append reservation and use the existing rollback/failure path. An individual
request larger than the available post-reset capacity fails predictably in the
prototype. Whether a shipping feature should offer a larger configurable cap,
another storage strategy or that rejection behavior remains a product decision.

Begin with one writer and durable group commit disabled. A later concurrent
integration must reserve for every pending batch and release reservations once
on publish, cancellation or rollback. The abstract model's single-reservation
coverage does not qualify group commit.

The existing [pager growth check](../../src/CSharpDB.Storage/Paging/Pager.cs)
uses live frame count and applies only while readers are active. It cannot
serve as this physical bound. `WalPreallocationChunkBytes` reserves capacity;
it is not a reclamation policy.

## Integration boundaries

| Component | Required change or qualification |
|---|---|
| [WriteAheadLog](../../src/CSharpDB.Storage/Wal/WriteAheadLog.cs) | Version-aware layout, control publication, new-format recovery, append reservations and restartable reset |
| [WalIndex](../../src/CSharpDB.Storage/Wal/WalIndex.cs) | Preserve an explicit physical published tail when retiring live frames; do not derive it from compacted frame count |
| [Pager](../../src/CSharpDB.Storage/Paging/Pager.cs) and [checkpoint coordinator](../../src/CSharpDB.Storage/Checkpointing/CheckpointCoordinator.cs) | Preserve copy visibility checks and lock order; add bounded admission and quiescent reclamation without deadlocking snapshot release |
| [MemoryWriteAheadLog](../../src/CSharpDB.Storage/Wal/MemoryWriteAheadLog.cs) | Explicitly support or reject imported new-format bytes; do not assume the version-one header length |
| [WalInspector](../../src/CSharpDB.Storage.Diagnostics/WalInspector.cs) and [diagnostic replay](../../src/CSharpDB.Storage.Diagnostics/Internal/InspectorEngine.cs) | Recognize layout and boundary or report unsupported format; never silently omit active WAL contents |
| [Retained snapshots](../../src/CSharpDB.Engine/RetainedDatabaseSnapshot.cs) and [snapshot file validation](../../src/CSharpDB.Engine/RetainedDatabaseSnapshotFile.cs) | Validate the selected boundary and physical/replay limits when copying the database/WAL pair |
| [Backup and restore](../../src/CSharpDB.Engine/DatabaseBackupCoordinator.cs) | Preserve the database/control-generation pairing and qualify import, export and recovery |

Keep layout information per WAL instance. Changing the global
`PageConstants.WalHeaderSize` would break the existing format and its consumers.
The first engine prototype must have one exclusive owner and operate only on
fresh test files. Existing snapshots, old binaries and mixed-version tools are
separate compatibility gates before enabling the format for users.

## Model evidence and limits

The executable [boundary model](wal_boundary_model.py) passed **1,740** bounded
interruption/slot-failure cases: **1,445** recovered the expected state and
**295** rejected unusable or corrupt state as specified. It includes partial
database persistence before flush, absent/complete/torn control writes, reset
truncation cuts and one additional unreadable control slot. Four deliberately
incorrect operation orders produced the expected failure witnesses.

A separate eight-step admission exploration reached **641 states** and checked
**1,791 transitions**, including reservations, publication, rollback, logical
retirement, pin/release and reset. It refused **358 reservations**. Two negative
accounting controls show why ignoring retired bytes or earlier reservations can
exceed a physical cap. Counts refer to the bounded model, not production tests.

Run with a new output path:

```text
python docs/design/wal_boundary_model.py --output <new-results.json>
```

The model assumes ordered durable flushes and detected slot tears. It excludes
checksum collisions, actual byte encoding, torn frame/commit writes, OS I/O
behavior, concurrent scheduling, migration and power loss. Invalid active
frames fail closed. It does not prove that the complete CSharpDB implementation
is correct or fast. The earlier two recovery regressions still pass as part of
24 focused checkpoint batch tests against the unchanged production binary.

## Implementation gates

The control codec and isolated page-store integration pass their bounded checks,
including real process terminations. An opt-in committed-tail extension now
recovers the tested torn append cases, but its mirrored per-commit publication
failed the isolated cost screen described below. Keep this design experimental;
do not carry that protocol into the production Pager as a performance change.
The subsequent format-preserving 256-frame compaction experiment also failed
its paired performance gates, as recorded below. Park buffer-size tuning and
select the next target from end-to-end phase evidence. Any later format proposal
needs a cheaper commit-boundary protocol plus the existing engine reader,
group-commit, realistic-growth and format-consumer qualification gates.

For timing, retain the existing paired latency/throughput/allocation gates and
include enough data to cross the physical budget repeatedly. Report P99 and
maximum transaction latency, throughput with final drain, total bytes moved and
flushed, maximum allocated WAL extent, Busy responses, and reopen/replay cost.
Do not omit reclamation pauses or compare against a baseline with a different
durability guarantee. Adoption requires a repeatable measured benefit after
these correctness and compatibility gates.

## Isolated control codec and file experiment

The [prototype test project](../../tests/CSharpDB.WalPrototype.Tests/CSharpDB.WalPrototype.Tests.csproj)
has no product references or solution entry. It cannot change how a production
database opens or writes. Its [codec](../../tests/CSharpDB.WalPrototype.Tests/ControlRecord.cs)
uses the following offsets within each 4,096-byte slot. All integers are unsigned
little-endian values; the floor must also fit a signed 64-bit file offset.

| Offset | Bytes | Field |
|---|---|---|
| 0 | 4 | ASCII `CWA2` |
| 4 | 4 | Version 2 |
| 8 | 4 | Feature flags: zero for floor-only; bit 0 enables the experimental committed tail |
| 12 | 4 | Slot size, 4,096 |
| 16 | 4 | Page size, 4,096 |
| 20 | 4 | Frame size, 4,120 |
| 24 | 8 | Data start, 8,192 |
| 32 | 16 | Opaque identity as two 64-bit words; not Guid byte order |
| 48 | 8 | Sequence, starting at 1 |
| 56 | 8 | Epoch, starting at 1 |
| 64 | 8 | Two nonzero 32-bit generation salts |
| 72 | 4 | Mode: ACTIVE = 1, EMPTY = 2 |
| 76 | 4 | Reserved zero |
| 80 | 8 | Absolute replay floor |
| 88 | 4 | Checkpoint page count |
| 92 | 4 | Reserved zeros |
| 96 | 8 | Committed tail when feature bit 0 is set; otherwise zero |
| 104 | 3,960 | Reserved zeros |
| 4,064 | 32 | SHA-256 of bytes 0 through 4,063 |

Identity generation uses 128 cryptographically random bits and rejects the
all-zero value. Salt generation uses two random nonzero words and rejects the
previous pair. The checksum protects integrity; it does not authenticate a file.
Unknown fields or nonzero reserved bytes with a valid checksum are errors.
The decoder permits a terminal counter value for inspection, but the protocol
refuses admission or maintenance that exhausts the counters and never wraps.

Floors must be at least `H`, frame-aligned relative to `H`, and no greater than
the physical file length. EMPTY requires floor `H`. An unaligned physical tail
does not itself invalidate the control record; a future frame parser must
determine which trailing bytes constitute complete committed data. The codec
does not mistake allocated file extent for a proven commit boundary.

The [protocol](../../tests/CSharpDB.WalPrototype.Tests/ControlProtocol.cs) writes
and flushes each slot separately, repairs a stale or missing peer, completes
interrupted EMPTY resets, and keeps admission closed after any failure. Quiescence
and the copied/committed boundary are inputs to this experiment; engine locks,
reader tracking, physical-budget reservations and commit parsing are not yet
implemented here.

The final Release run passed **51 tests**, including these bounded checks:

- An independently packed little-endian golden vector, 32,768 single-bit
  corruptions, 8,194 prefix/suffix tear images, and all 256 combinations of eight
  512-byte sectors. A tear must decode exactly the old/new record or be rejected.
- **3,831 file-backed interruption cases** across retirement, reset and restart
  repair: **2,747** recovered the exact expected pages and **1,084** failed closed
  as specified. Cases include partial base-page persistence, torn slot writes,
  interrupted flushes/truncation, and loss of either additional control slot.
- **18 post-admission append cases** retained the new acknowledged fixture page
  after loss of either slot. Exceptions before/after each of 12 reset I/O
  operations stopped further work; four recovery-flush failures kept admission
  closed. Four incorrect-order controls exposed the expected hidden commit or
  unusable recovery boundary.

The harness exercises real file writes and `Flush(true)`, then writes each
modeled interruption image to a separate file, closes it and reopens it through
the codec. Recovery checks leave every input byte unchanged. Its page fixture
uses whole, individually committed records and known checkpoint page contents;
it does not implement the production frame checksum, commit markers or corrupt
page repair. These are deterministic fault images, **not actual power-loss or
process-kill tests**, and they do not qualify the engine's durability or speed.

Build and run this project directly, choosing a fresh evidence directory:

```powershell
dotnet build tests/CSharpDB.WalPrototype.Tests/CSharpDB.WalPrototype.Tests.csproj -c Release
$env:CSHARPDB_WAL_PROTOTYPE_EVIDENCE = '<fresh-evidence-directory>'
dotnet tests/CSharpDB.WalPrototype.Tests/bin/Release/net10.0/CSharpDB.WalPrototype.Tests.dll
```

The harness preserves each case in a unique subdirectory. With no environment
override, it uses `CSharpDB.WalPrototype` under the system temporary directory.
The recorded runs are under `.tmp/wal-control-codec-20261005`. The initial 50-test
pass and its source snapshot remain preserved; the final run adds the explicit
recovery durability barrier. Production source and all eleven qualified product
DLLs remain unchanged. No new performance claim or full production-suite result
is implied by this experiment.

## Isolated page store and real process interruptions

The [experimental page store](../../tests/CSharpDB.WalPrototype.Tests/ExperimentalWalStore.cs)
now connects the control protocol to file-backed transactional append, replay,
checkpoint retirement and bounded reset. It operates only on fresh raw page
files, with one owner and writer. This is a storage-level integration, not an
implementation in the production Pager or SQL engine.

Its [frame codec](../../tests/CSharpDB.WalPrototype.Tests/WalFrameCodec.cs) uses the
existing 24-byte header, 4,096-byte payload, default additive checksum and
nonzero final-frame page-count commit marker. The test and crash-worker projects
link the unchanged checksum source; they do not build or reference product DLLs.
The marker preserves the checksum of the encoded page already written, avoiding
a second checksum over a caller buffer that could have changed.

The store keeps physical append offsets after prefix retirement, reserves the
whole batch before writing, and counts retired bytes toward the physical cap.
It has no preallocation or group commit. A batch larger than post-reset capacity
is rejected before I/O. At the cap, a quiescent full drain/reset makes space;
reader pins or a pending checkpoint return Busy before another append. The
prototype supports page-count growth up to 65,536 pages, but not shrinking.
That bound is an experiment limit, not a product setting.

Reader snapshots retain their own page-offset map and generation. Their base
fallback pages remain stable because any active reader conservatively blocks
base copying and reset. The test store uses a single gate and returns Busy
without waiting while holding it; reader release can therefore make progress.
These checks do not qualify the more permissive production checkpoint coordinator.
Cancellation before the commit marker truncates and flushes back to the prior
published tail before permitting another batch. I/O failure closes the store
until reopen and releases its reservation.

Recovery validates the complete active region before repairing controls or
changing either file. Its current tail rules deliberately favor detection:

| Recovered suffix | Result |
|---|---|
| Complete checksummed transaction with a commit marker | Publish the whole batch |
| Complete checksummed frames with no final commit marker | Discard the entire pending batch before the next append |
| Partial header/payload, torn marker, invalid checksum, generation or page range | Fail without modifying either file |
| Invalid bytes below the durable replay floor | Skip the retired prefix and recover later committed frames |

The last-frame shape alone cannot distinguish every interrupted append from
damage to acknowledged data. Five tested mid-write process crashes therefore
leave an explicitly rejected WAL instead of an automatically reopened database.
This is an **availability limitation**, not a passing recovery result, and needs
resolution before adoption. This experiment does not change version-one recovery.

The final Release build had no warnings or errors, and **121 tests passed**:
30 codec, 21 control-protocol, 33 page-store and 37 process-crash cases. The 51
earlier control tests are included, along with their 3,831 modeled fault images.
The new storage coverage includes whole-batch replay, duplicate pages, growth,
both corrupt retired-prefix cases, partial/clean uncommitted tails, active
corruption, cancellation rollback, buffer ownership, oversize rejection, and
concurrent writer/pinned-reader views.

The [crash tests](../../tests/CSharpDB.WalPrototype.Tests/WalProcessCrashTests.cs)
performed **43 actual child-process terminations** across 37 cases: 31 first
interruptions and six two-stage interrupted-reset/recovery cases. Each worker
recorded the reached point and its PID, then killed only itself without disposing
the store. Cases cover partial frames/markers, commit return, checkpoint copying,
both control slots, truncation and writer admission. Of the 31 first interruptions,
26 reopened with the specified committed state and five rejected ambiguous
tails without changing input bytes. All six interrupted recoveries completed on
the following open; 12 additional slot-loss checks preserved a commit appended
after that recovery. These are process-crash tests with the OS still running,
not power-loss tests.

A bounded physical-growth test made **24 two-frame commits**, crossed a four-frame
budget through **11 resets**, and reopened after every commit. Maximum WAL extent
was **24,672 bytes**, exactly its configured test cap. This proves the tested
accounting/reset path, not long-running engine capacity or latency.

Evidence and pre-recovery file copies are preserved under
`.tmp/wal-storage-prototype-20261005`. The first focused 68-test pass is retained;
the final run adds cancellation/buffer-ownership guards and reruns the full
isolated suite. Production source, all eleven qualified product DLLs, prior
regression edits and unrelated files remain unchanged. No timing workload or
production suite was rerun. Compatibility/migration, actual engine reader modes,
backup/inspection consumers, group commit and OS/power-loss durability remain
separate gates before any performance claim.

## Committed-tail recovery option and rejected commit-cost screen

The October 6 extension gives fresh experimental files an explicit durable
committed tail. It is opt-in through `committedTail: true` at creation; opening a
floor-only file does not upgrade it. Both slots must agree on feature presence.
The tail at byte 96 is protected by the slot's SHA-256, aligned relative to `H`,
at least the replay floor and no greater than the physical file extent. EMPTY
requires both floor and tail to be `H`. A checksum-valid impossible tail is an
error even when the peer record could otherwise be used.

The [publication protocol](../../tests/CSharpDB.WalPrototype.Tests/ControlProtocol.cs)
orders a commit as follows:

1. Write all frames and their final commit marker, then flush the WAL data.
2. Write and flush ACTIVE slot A with the new committed tail.
3. Write and flush ACTIVE slot B with that same tail and a higher sequence.
4. Publish the in-memory page map and return success.

A recovered higher-tail record can safely include a complete batch whose caller
never received success. A surviving older-tail record can omit only an
unacknowledged batch, because acknowledged commits had both current copies
flushed. Recovery validates every frame through the selected tail and requires
it to end at a complete commit boundary. It then repairs/flushes the controls
before trimming later bytes. Those later bytes are proven unacknowledged by
this protocol, whether they contain a partial frame, torn marker or complete
but unpublished batch. Damage within the declared committed range still fails
without changing either input file.

Cancellation may truncate back only before commit publication starts. Once
publication has started, any failure requires reopen; truncating then could
remove bytes already named by a surviving higher-tail record. Reset advances
the generation and returns the tail to `H` only after flushing the complete base
copy and mirroring EMPTY. Ordinary retirement advances the floor and preserves
the latest committed tail, including later commits after the captured copy.

The final Release suite passed **179 tests**, retaining the earlier 121 cases.
The new option passed **41 process-crash cases with 47 actual terminations**,
including all five torn-write points rejected by the floor-only prototype and
six twice-interrupted reset/recovery cases. The full suite terminated 90 child
processes across the two formats. Twelve post-recovery slot-loss checks retained
a newly acknowledged commit in the new format. Additional guards cover loss of
either control followed by a torn append, damaged/truncated acknowledged data,
a reverted commit marker, a boundary inside a transaction, cancellation during
publication, page growth, reader backpressure and generation reset.

A separate **369-case** file-backed control-publication matrix recovered the
prior committed state in 148 cases and the newly published state in 157 cases.
It rejected 64 cases where a torn slot plus the additional injected slot loss
left neither control readable. Those refusals do not discard acknowledged data.
The matrix assumes the ordered successful data flush before tail publication;
actual power loss and storage-device flush guarantees remain unqualified.

The correctness fix is too expensive for the current performance objective.
It requires **three WAL flushes per commit instead of one**, plus **8,192 control
bytes written per commit**. A fixed ABBA screen ran the same fresh Release binary
with only the creation flag changed: 40 warmup commits in a separate file, then
500 one-page commits per slot, a full drain and exact durable reopen. All four
reopens passed. The desktop remained in normal use, with no idle gates or reruns.

| Pair / execution order | Floor-only P99 | Committed-tail P99 | Floor-only commits/s incl. drain | Committed-tail commits/s incl. drain |
|---|---:|---:|---:|---:|
| Baseline then candidate | 3.8555 ms | 13.2950 ms | 451.88 | 141.74 |
| Candidate then baseline | 4.0372 ms | 10.5400 ms | 446.05 | 150.68 |

P99 was **2.61–3.45 times baseline**, throughput fell **66.2–68.6%**, and measured
allocation rose by about **9.2 KB per commit** in both pairs. These are isolated
page-store commit costs, not SQL/engine numbers or an end-to-end reclamation
comparison. The timed loop did not cross its 64 MiB cap; final-drain time is
included in the throughput shown above. No performance promotion follows from
the correctness results.

The extension remains disabled by default in the test prototype, and production
code and all eleven qualified product DLLs remain unchanged. Evidence, the fixed
screen protocol, all four captures, test output and source snapshots are retained
under `.tmp/wal-commit-boundary-20261006`. The direct per-commit mirrored-tail
approach is parked as a correctness reference. The following experiment evaluated
format-preserving compaction batching. Revisit a new WAL format only with a
commit-boundary design that can meet the latency and allocation gates.

## Format-preserving compaction batching screen

The October 6 follow-up tested 256-frame sequential compaction moves instead of
16-frame moves in an isolated Storage build. A temporary pooled buffer served
larger suffixes, with an explicit frame-aligned logical size of at most 1,054,720
bytes and return in `finally`; smaller suffixes retained the existing buffer.
No database-copy, WAL-format, durable-flush or index-publication ordering changed.

The candidate passed 133 focused existing WAL/checkpoint/fault cases. A separate
current checkpoint-class build passed 29 cases on each arm, adding boundary
sizes 255, 256, 512, 513 and 4097 to the existing overlap/recovery coverage.
Both builds had zero warnings/errors. These are bounded correctness checks;
full candidate qualification was conditional on passing the performance screen.

The fixed normal-desktop ABBA screen used identical uninstrumented harness code
and dependencies except Storage, with 300 measured commits of 10,000 rows per
slot. All four measured and two smoke processes passed exact durable reopen.
Whole-operation P99 improved only **3.96% and 6.45%**, below the required 10% in
both pairs. Allocations rose **1.381% and 1.387%**, exceeding the 1% gate;
throughput including drain fell **0.018% and 2.402%**. P95 passed its 5% guard.
There were no retries or smaller-workload guards after rejection.

The candidate was not adopted, and production sources/binaries remain unchanged.
Evidence is preserved under `.tmp/wal-compaction-batching-20261006`. Alongside the
earlier rejected 128-frame and read-prefetch variants, this result parks further
buffer-size tuning. Choose a different bottleneck from end-to-end phase evidence
before another optimization; these timings do not establish the cause of the
remaining stall or the previously intermittent extreme tail.
