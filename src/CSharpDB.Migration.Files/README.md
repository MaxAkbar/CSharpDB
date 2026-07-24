# CSharpDB.Migration.Files

Streaming file-format adapters for `CSharpDB.Migration`.

The current Phase 4A slices provide a forward-only CSV logical-record reader,
an immutable raw-byte source snapshot, bounded delimiter/BOM inspection, a
deterministic content-plus-format source binding, and confidence-bearing schema
inference with ordinal overrides, migration-catalog adaptation, and a repeatable
`IMigrationDataSource` whose strict behavior remains the default. The reader
handles quoted multiline fields
and escaped quotes without loading the source file into memory, preserves exact
decoded text, and keeps null, empty, missing,
and trailing-empty fields distinct. Strict decoding and absolute field,
record, column, inspection, inference, manifest, and snapshot limits keep
malformed or hostile inputs bounded.

This project owns its file-parser dependencies. It does not expose CsvHelper
types through its public API, and the provider-neutral migration project does
not reference CsvHelper.

Schema inference retains only per-column counters and candidate flags. It
defaults ambiguous, sparse, mixed, empty, or lexically significant evidence to
`Text`; sampled profiles report an unknown total and require later full-stream
validation. `CsvMigrationSourceInspector` emits a validated shared migration
catalog without claiming that a sampled prefix proves the unseen tail. See
[`migration-csv-schema-inference.md`](../../docs/migration-csv-schema-inference.md).

`CsvMigrationDataSource` validates every projected scalar, preserves request
column order, splits before row or canonical-byte limits, and emits
snapshot/policy-bound cursors. Each pass opens a fresh reader over the same
caller-owned snapshot, which supports receipt replay and validation rereads.
Opt-in deterministic replay records only `MissingField`, `NullNotAllowed`, and
`TypeMismatch`; its frozen evidence preserves record/line coordinates, field
kind, quote state, and decoded pre-normalization text. Accepted and rejected
outcomes share one contiguous cursor interval, including all-reject batches.
Parser, encoding, integrity, target-conversion, and resource-limit failures stay
fatal.
See [`migration-csv-data-source.md`](../../docs/migration-csv-data-source.md).

`CsvSnapshotPackage` atomically retains the raw snapshot and exact reader,
inference, source, and catalog policy for digest-pinned cross-process reopen.
The CLI uses that boundary for inspect, apply, resume, and validation. A
50,000-row CI fixture and isolated 100K/1M measurements qualify fixed live
batches, exact resume/checksum behavior, bounded memory, and temporary cleanup;
see [`migration-csv-performance.md`](../../docs/migration-csv-performance.md).

The capability-qualified SDK validation path now replays accepted/rejected
outcomes and compares them with the target snapshot's authoritative receipts
and ledger before report publication. The provider-neutral SDK can materialize
the canonical reject artifact from that target-owned evidence. Retained CSV
and untyped retained JSON v1 now expose deterministic tolerant planning, apply,
resume, and validation through explicit reject policy, limit, opt-in, and
retained-artifact arguments; strict fail-fast remains the default.

The first Phase 4B slices add a bounded, forward-only JSON reader for exact
root-array and whitespace-separated multiple-value framing (including
conventional NDJSON), immutable source snapshots with exact reader bindings,
and full-stream object-row schema discovery with independently bounded type
profiling. They preserve property encounter order, decoded names, explicit
nulls, exact number lexemes, and nested ordered values, while rejecting
duplicate decoded property names at every depth. The inferred catalog keeps
missing distinct from explicit null and maps nested, mixed, or lexically
significant values to versioned canonical JSON text. Catalog-bound row
streaming now revalidates projected values, preserves arbitrary projection
order, emits deterministic reject evidence, bounds batches by rows and
canonical bytes, and supplies snapshot/policy-bound replay cursors.
Raw and typed retained packages, source-bound typed intent, typed table/apply
conversion, JSON/NDJSON table export, and durable retained-adapter/CLI resume
routing are now implemented. Retained package v1 is available through
fail-fast and deterministic-reject CLI inspect, plan, apply/resume, and
validation for root-array JSON and NDJSON. Its source-aware deterministic route
is qualified for bounded late-tail type rejects, exact artifact reuse, resume,
and validation in both framings. An explicitly supplied, independently pinned
typed-intent sidecar selects package v2 during inspect. The resulting
versioned catalog selects fail-fast plan, apply/resume, and validation routing,
and those execution commands require the independently retained package pin.
Package selection is catalog-facet driven because v1 and v2 share the
`.csdbjson` extension. Typed-v2 deterministic rejects, collection projection,
and typed export-intent generation remain later slices. See
[`migration-json-reader-foundation.md`](../../docs/migration-json-reader-foundation.md)
and
[`migration-json-table-schema.md`](../../docs/migration-json-table-schema.md),
plus
[`migration-json-data-source.md`](../../docs/migration-json-data-source.md).

`JsonStreamingExporter` writes one ordered physical CSharpDB table as a
compact root array or LF-terminated NDJSON without buffering the table. Its
canonical manifest binds retained source identity, ordered schema, framing,
resource ceilings, physical bytes, and matching lossless logical evidence.
The local-Windows restart-only publisher uses private sibling staging,
handle-bound no-replace data-before-manifest commits, exact pair/data-only
reuse, and fail-closed namespace and ACL qualification.

`JsonExportCheckpointSerializer` freezes the bounded canonical
`csharpdb-json-export-checkpoint/v1` artifact before it is activated by a
prepared-output lease. It binds the immutable source/schema/framing contract
to complete-object physical and logical prefixes, validates exact root-array
and NDJSON zero/nonzero boundary geometry, reconstructs terminal manifests,
and permits only idempotent or exact next-generation transitions.
Root-array `Writing` checkpoints omit and reserve `]\n`; NDJSON completion can
be a phase-only transition over identical bytes.

The platform-neutral resumable coordinator initializes generation zero,
replays exactly to recovered object boundaries without lookahead,
independently rehashes the qualified prepared prefix, resumes after the signed
row ID, and emits periodic and EOF-qualified terminal checkpoints.
`JsonExportPreparedOutputLease` now supplies that session's local-Windows
filesystem boundary: exact-spelling deterministic private siblings, an
exclusive current-owner-only prepared handle, active-only recovery, stale
pending reclamation, verified tail truncation, and data-first durable
pending/active replacement relative to a pinned parent. Any uncertain
replacement failure poisons the live lease so recovery must reopen and
requalify durable authority. Restart-only publication staging now uses
deterministic exclusive `.next` siblings and safely reclaims qualified crash
leftovers. Public `WriteResumableAsync` composes the coordinator and durable
lease without publishing finals. Same-lease
`WriteResumableAndPublishAsync` keeps that exclusive prepared handle through
source requalification and manifest-last publication, while
`JsonExportPublisher.PublishCompletedAsync` can reopen a terminal prepared
output using an independently retained manifest digest. Prepared data is
copied and independently rehashed into deterministic publication staging,
exact data-only and pair states are recoverable, and prepared/checkpoint
authority is preserved after success. The retained adapter and CLI use that
same source-qualified workflow with a configurable checkpoint row interval;
an exact command rerun is the resume command, including bootstrap of a
same-binding restart-only exact pair. Reader/source-version binding changes
fail closed. The `.csharpdb-json-export-*` leaf prefix is globally private and
cannot be selected for an external source, destination, or manifest.
Child-process kill/restart tests qualify all three checkpoint persistence
cutoffs after journal authority exists for root-array JSON and NDJSON and all
five publication cutoffs for both framings, including zero-byte empty NDJSON.
The one-time restart-only adoption transition and disposable-VM hard-power
qualification remain later gates. See
[`migration-json-export-contract.md`](../../docs/migration-json-export-contract.md).

The typed `csharpdb-csv-export-manifest/v1` sidecar contract now binds a
CSharpDB snapshot, ordered typed schema, fixed RFC 4180 codec, physical data
digest, source/export logical digests, and a BLOB decoded-size ceiling.
`LosslessV1` preserves source values; the separately named
`SpreadsheetSafeLossyV1` profile records
aggregate formula-mitigation changes without putting cell values in the
manifest and rejects BLOB columns whose base64 could resemble a formula.
`CsvStreamingExporter` now writes the fixed RFC 4180 codec to a caller-owned
empty stream, validates whole typed rows before emitting them, streams UTF-8
and padded base64 in bounded chunks, and returns canonical physical/logical
manifest evidence. `CsvExportCheckpointSerializer` now freezes
`csharpdb-csv-export-checkpoint/v1`: it binds the retained source identity,
schema, profile, codec and resource limits to complete-record physical and
logical prefixes, a signed last row ID, transform counts, and optional
data-complete manifest evidence. Zero-row progress must describe the exact
rendered header bytes and digest.

Checkpoint prefix digests are verification-only; they are not resumable
SHA-256 state. `CsvExportPreparedOutputLease` now provides the
Windows-qualified physical recovery boundary for local filesystems. It derives
deterministic private prepared-data, active-checkpoint, and pending-checkpoint
siblings solely from the normalized future destination, fails closed if that
final path exists, and holds the prepared file through an exclusive
current-owner-only handle. The active checkpoint read is bounded. A missing
checkpoint with nonempty prepared data requires explicit reset; the lease
never silently adopts those bytes.

Recovery validates the canonical checkpoint and immutable binding, rehashes
the exact physical prefix, verifies its CRLF boundary, and only then truncates
and durably flushes an uncheckpointed tail. Generations start at zero, advance
by exactly one, allow same-generation idempotence only for identical canonical
bytes, keep row/byte/evidence progress monotonic, and make `DataComplete`
terminal. Persistence orders a durable prepared stream carrying exact
prefix-length/hash evidence before a durable exact pending checkpoint and a
handle-based atomic active replacement. A stale pending file is never recovery
authority, and disposing the lease preserves every private file.

The prepared-output substrate is limited to local Windows filesystems; it fails
closed on UNC and mapped network volumes and is unsupported on non-Windows
platforms. Retained-source replay, the stateful prepared-output coordinator,
cross-process export/resume, the retained-snapshot CLI, and fail-closed
manifest-last publication are implemented and process-crash qualified.
Abrupt hard-power qualification of checkpoint and publication namespace
replacement remains open against the documented disposable-VM filesystem and
cache matrix. The offline retained read-only CSharpDB snapshot can be reopened
and verified across processes, and its physical table reader provides strictly
ascending signed row IDs with an exclusive resume boundary. See
[`migration-csv-export-contract.md`](../../docs/migration-csv-export-contract.md)
and
[`migration-csharpdb-retained-snapshot.md`](../../docs/migration-csharpdb-retained-snapshot.md),
plus the
[`migration-csv-export-power-loss-qualification.md`](../../docs/migration-csv-export-power-loss-qualification.md)
runbook for the remaining external gate.
