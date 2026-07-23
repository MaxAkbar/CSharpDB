# Retained CSharpDB Snapshot V1

This note records the source-snapshot boundary used by resumable migration
exports. It provides a durable database view that can be verified and reopened
by a later process without reopening or recovering the original database.

## Contract

`RetainedDatabaseSnapshot.CaptureAsync` publishes one clean CSharpDB database
file and returns a path-independent identity containing:

- the exact snapshot byte length;
- a lowercase `sha256:<64 hex>` digest of every published byte; and
- the stable `csharpdb-retained-snapshot/v1` identity derived from those two
  values.

The caller must retain the byte length and SHA-256 independently, for example
inside an export checkpoint. The hash detects accidental corruption and a
mismatched generation; it is not a signature and does not authenticate an
artifact against an attacker who can replace both the snapshot and its pin.

Capture never opens the source through `Database.OpenAsync`. That normal path
can create a missing database or WAL, run recovery into the source, install
missing internal stores, checkpoint, and delete the WAL on close. Instead,
capture opens an existing regular database read-only, holds that handle while
selecting the existing companion WAL, and streams both into a private bounded
workspace. WAL recovery and checkpointing occur only on those private copies.
The resulting single-file database is flushed and published without replacing
an existing destination.

## Verified Read-Only Reopen

`RetainedDatabaseSnapshot.OpenAsync` requires the independently retained
identity. It streams the retained file into a new private workspace while
checking its exact length and SHA-256, then opens that verified copy. The
retained artifact itself is never used as a live database and never receives a
companion WAL.

The returned `RetainedDatabaseSnapshotSession` exposes database metadata and
read-only SQL through a reader snapshot. It does not expose the underlying
writable `Database`, transaction, checkpoint, or save surfaces. Its private
database open also skips the normal full-text internal-store repair step, so
reopening does not change the captured logical catalog before export.

The identity covers the CSharpDB database bytes only. A registered external
table points at a separate table-archive file, which v1 does not copy or bind.
Queries that read an external table are therefore outside the retained-snapshot
guarantee, and migration exporters must reject them unless that archive is
retained and verified through its own contract.

### Deterministic Physical Table Reads

`RetainedDatabaseSnapshotSession.OpenTableReader` exposes local physical-table
rows directly from the verified reader snapshot. It returns rows in strictly
ascending physical row-ID order and accepts an optional `afterRowIdExclusive`
resume boundary. That boundary is the last fully completed physical row ID,
not a row ordinal or user primary-key value; the first returned row is strictly
greater than it. Deleted row-ID gaps therefore do not affect resume.

The reader rejects views, system/internal tables, external tables, and missing
local tables before scanning. In particular, it never follows an external
table's archive path because those bytes are outside the retained identity.
Only one SQL query or physical table reader may be active on a session at a
time. The current row uses one reusable buffer, so callers must consume it
before advancing and must dispose the table reader before starting another
read. Disposing the owning retained session also disposes its active physical
reader before closing the private pager and workspace.

The physical boundary alone is not a durable export checkpoint. A resumable
export must also bind it to the retained snapshot identity, table, ordered
schema, row-order contract, export profile and codec, durable CSV byte
boundary, and verified output-prefix digest.

Disposing the session closes its reader and database and removes only its owned
private workspace. It never deletes the retained snapshot.

## Resource And Failure Boundaries

Database bytes, WAL bytes, consolidated snapshot bytes, copy-buffer size, and
private pager caches have explicit limits. Physical table readers also enforce
`MaxEncodedRowBytes` (64 MiB by default); an oversized overflow reference is
rejected before its chain is read or its declared payload is allocated.
Copying and hashing are streaming; the implementation does not load the
database or WAL in proportion to file size. Cancellation before publication
removes owned temporary state and leaves the destination absent. Once the
no-replace publication succeeds, the method returns the valid receipt rather
than treating a late cancellation as a failed operation whose published file
is ambiguous.

Missing input never creates a database or WAL. An existing destination is
preserved. Reopen rejects a wrong pin, truncation, appended bytes, or changed
content before the private database is exposed.

## Offline V1 Boundary

The path-based v1 capture is deliberately offline. A conforming active
CSharpDB writer conflicts with the source handles rather than being paused
while the copy runs. On platforms where file-sharing locks are advisory, the
source and destination parents, plus any caller-supplied workspace parent, must
be trusted and stable, and operators must enforce a write freeze. By default a
random private workspace is reserved directly under the physically resolved
operating-system temporary directory; shared-writable Unix parents must have
the sticky bit, all Unix default parents must be owned by root or the effective
user, and the child is forced to owner-only mode. On Windows the child inherits
access policy from the trusted temporary or caller-supplied parent. A
non-cooperating process that changes source files outside this contract is not
an online snapshot source.

An online overload could later materialize the engine's in-process WAL reader
snapshot while writers continue. V1 does not claim that capability.

## CSV Export Integration

`CSharpDbCsvExportAdapter.WriteResumableTableAsync` now joins this verified
source boundary to the stateful CSV prepared-output coordinator. Its public
request takes the retained snapshot path and an independently pinned
`RetainedDatabaseSnapshotIdentity`, plus the physical table name, future CSV
destination, export profile, and CSV resource ceilings. It does not accept
caller-created source evidence, a schema, a row factory, or Engine reader
configuration.

The adapter passes the path and expected identity to
`RetainedDatabaseSnapshot.OpenAsync` and owns the resulting default-configured
session until the CSV operation returns or fails. It first opens the requested
physical table reader without advancing it. That preflight applies the
reader's existing rejection of views, system/internal tables, external tables,
and missing tables, and captures the exact catalog table name and persisted
column order from the physical reader rather than from caller input.

Replay and continuation are separate physical readers opened sequentially on
that same verified session. Each open rechecks the exact table name and every
column's ordinal, name, type, and nullability against the preflight schema
before yielding the reader's signed physical row IDs and values. The session's
single-active-read gate therefore aligns with the stateful writer's source
sequence: recovery first opens from the beginning to rebuild and verify the
checkpoint prefix, disposes that reader, and only then opens after the last
completed row ID for exclusive-boundary continuation. A `DataComplete` reopen
uses the same session and proves EOF on its replay reader.

CSV source evidence is derived only from the verified session identity:

- `ByteLength` becomes the manifest snapshot byte length;
- canonical `sha256:<64 lowercase hex>` is split into algorithm `sha256` and
  the bare 64-hex manifest digest value; and
- `SnapshotIdentity` is copied exactly into the checkpoint binding.

The source kind is `csharpdb`. The source version is the informational version
of the `CSharpDB.Engine` assembly that qualified and read the snapshot, with
`+` build metadata removed and the assembly version used only as a fallback.
That version is part of the immutable CSV checkpoint binding.

The adapter exposes neither `DatabaseOptions` nor
`RetainedDatabaseSnapshotOptions`. It always interprets retained bytes through
the built-in default Engine reader/serializer composition and uses the default
retained-snapshot resource policy. The Engine reader version plus that fixed
composition define the bound source interpretation. Retained identity v1 does
not represent custom serializer, index, catalog, checksum, or function-provider
provenance, so custom provider configurations remain unsupported for this
adapter.

This completes the retained-source adapter and source-origin proof for local
physical CSharpDB tables. Final CSV/manifest publication, abrupt-power-loss
qualification of checkpoint namespace replacement, export/resume CLI wiring,
and cross-platform prepared-output support remain separate pending work.
