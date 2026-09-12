# Sharding Outstanding Work

Internal backlog based on the source review on 2026-09-06. This document tracks
unfinished implementation and validation; it is not public feature documentation.
The sections below describe future work, not features available today.

Directory mutation APIs, Admin route bindings, migration models and progress APIs,
Admin migration controls, read-only fan-out, and replica metadata already exist.
Extend those implementations rather than adding them again. Usage documentation
remains in the [client README](../src/CSharpDB.Client/README.md) and
[sharding sample](../samples/api-level-sharding/README.md).

## 1. Migration Recovery and Cutover Safety

Current gap: checkpoints are persisted, but resume and retry rerun the stored
migration plan with destination overwrite enabled. They do not continue from the
last verified step. Checkpoint write failures can become warnings, and migration
write fences live in the client process.

- [ ] Resume exact-key and bucket-range migrations from verified durable progress.
  Record enough information to recover partial table and collection copies,
  verification, source cleanup, directory repair, pending-map writes, and history.
  Retrying a completed step must not duplicate data or repeat unsafe deletion.
- [ ] Define required checkpoint persistence boundaries. A failed checkpoint write
  must block any later operation that would make recovery unsafe and produce an
  actionable recovery state.
- [ ] Persist migration ownership and write fences across process interruption.
  Reject conflicting exact-key and bucket-range jobs atomically, including overlaps
  between the two migration types. Enforce the same fences for every supported
  routed writer, rather than only the client running the migration.
- [ ] Coordinate fences, source cleanup, and pending-map activation. A migration
  awaiting activation must not allow new writes to make the verified copy stale
  or leave active routing pointing at data that has already been removed.
- [ ] Add explicit migration planning/preflight and job-start APIs, plus an
  abandonment operation with an operator comment and defined cleanup behavior.
  Preserve compatibility with the existing execution, progress, resume, and retry
  APIs and expose additions through REST and gRPC.
- [ ] Extend the existing Admin preview with server-side manifest validation and
  estimates of affected data. Its current preview summarizes routes and manifest
  object counts; it does not perform a full data preflight.
- [ ] Complete directory repair for interrupted and partially recovered migrations.
  Move unambiguous entries to the verified destination and pending map version;
  mark ambiguous entries stale for operator review.
- [ ] Preserve an audit trail of recoverable state transitions, operator actions,
  and recovery instructions. Extend the existing Admin progress/history views
  with the new preflight, recovery, and abandonment operations.

Completion criteria:

- Inject interruptions during partial copy, verification, source cleanup,
  directory repair, and catalog writes; reopen the client or daemon and recover
  without lost data, duplicate data, or premature ownership changes.
- Verify checkpoint storage failures and conflicting jobs cannot bypass recovery
  or fencing requirements.
- Exercise resume, retry, and abandonment over direct, REST, and gRPC clients,
  including recovery when catalog versions have changed.
- Verify Admin shows blocking preflight issues, actual progress, verification
  results, and the required operator action.

Implementation starting points:
[sharded client](../src/CSharpDB.Client/CSharpDbShardedClient.cs),
[sharding contracts](../src/CSharpDB.Client/CSharpDbSharding.cs), and
[Admin Sharding workspace](../src/CSharpDB.Admin/Components/Tabs/ShardingTab.razor).

## 2. Directory Management and Write Policies

Current gap: resolve, reserve, activate, upsert/repair, disable, delete, and
stale-marking APIs exist across direct, REST, and gRPC clients. Admin does not yet
have the dedicated directory management surface, and the proposed write-policy
abstraction is absent.

- [ ] Add filtered directory-entry listing and summary APIs for Admin, reusing
  directory definitions already available through map metadata.
- [ ] Add an explicit directory write-policy contract, proposed as
  `CSharpDbShardDirectoryWritePolicy`, for reserve-before-write and
  write-first-repair workflows. Define retry, duplicate-key, partial-failure,
  abandoned-reservation, and mutable-lookup-key behavior. Directory mutations and
  shard data writes must not be presented as an atomic cross-shard transaction.
- [ ] Add Admin views for directory definitions, entry counts, state filtering,
  alternate-key lookup, and the resolved route alongside route-key simulation.
- [ ] Add Admin reserve/activate, repair/upsert, disable, delete, and stale-marking
  flows using the existing APIs. Show active versus pending catalog state and
  required activation steps; keep hidden connection strings and API keys out of
  templates and previews.
- [ ] Provide a recovery workflow for incomplete reservations and missing or stale
  entries after partial writes or migrations, according to the selected policy.

Completion criteria:

- An operator can find, inspect, resolve, and repair entries through Admin over
  both local and remote connections.
- Policy tests cover duplicate submissions, conflicting reservations, interrupted
  writes, stale map versions, and retries without activating an incorrect route.
- Listing and UI tests cover filtering and active/pending state while retaining
  existing directory API regression coverage.

## 3. Split an Existing Database into Shards

Current gap: Admin can create a new sharded setup. Moving data between existing
shards does not provide the separate workflow needed to convert a populated,
unsharded database.

- [ ] Add a **Shard Existing Database** workflow that collects the source database,
  master database, target shards, keyspace, per-table and per-collection route-key
  mappings, batch size, verification mode, and cutover mode.
- [ ] Preflight schema and data ownership. Report unsupported objects, missing
  route-key mappings, and invalid target definitions before copying data.
- [ ] Copy schema and backfill rows/documents into their assigned shards using
  durable batch checkpoints and idempotent recovery. Reuse the hardened migration
  machinery where applicable.
- [ ] Verify counts and checksums before cutover. Define write fencing for the
  selected cutover mode and capture the final delta if source writes are allowed
  during backfill.
- [ ] Activate the master catalog only after verification and final synchronization
  succeed, then reopen Admin against the master database. Preserve the source and
  define safe recovery when backfill, verification, or cutover fails.
- [ ] Show preflight results, copy progress, verification, and cutover confirmation
  in Admin.

Completion criteria:

- A populated database containing route-owned tables and collections can be split
  and queried through the resulting master catalog with matching counts/checksums.
- Interruption and retry do not duplicate or lose data; failed verification leaves
  the source usable and does not activate an incomplete sharded setup.
- Cutover tests cover concurrent writes when that mode is supported and confirm
  that the final synchronized state is the state exposed through the master.

## 4. Replication and Manual Failover

Later work, after migration and catalog recovery are reliable. Existing replica
roles, relationships, promotion eligibility, and operator-reported lag are
metadata only; there is no shard replication worker or promotion/demotion API.

- [ ] Define and implement a replication protocol using suitable retained changes
  or WAL-derived primitives, including initial synchronization, durable positions,
  restart recovery, and handling of unavailable retained history.
- [ ] Add a worker that copies committed changes and reports measured lag, health,
  and last replicated position/time through the existing shard status surfaces.
- [ ] Add promotion-readiness, manual promotion, demotion/disable, and failover
  audit APIs for direct, REST, and gRPC clients. Use the master catalog and pending
  map activation rules for ownership changes.
- [ ] Validate eligibility, health, freshness, and lag before promotion. Define
  fencing of the old primary to prevent two writable owners. Forced promotion
  requires explicit acknowledgement of possible data loss.
- [ ] Extend Admin with readiness checks, measured replication status,
  promote/demote actions, confirmation, and failover audit history.

Completion criteria:

- Replicas converge after interruption and report measured lag rather than only
  operator-entered values.
- Unsafe promotion is rejected; successful promotion records ownership and audit
  state, while failed promotion preserves the prior valid ownership state.
- Failure and partition tests verify that promotion cannot create two writable
  primaries for the same ownership range.

Automatic failover remains deferred until manual failover has passed operational
validation.

## Validation Still Required

The 2026-09-06 review passed all 21 focused `CSharpDbShardedClientTests`. That result
validates existing scenarios; it does not establish the recovery or failover
criteria above.

Admin route bindings and fan-out controls already exist. Validate their complete
workflows rather than listing them as missing implementation: per-tab isolation,
child-tab route inheritance, missing-route guards, form/report/pipeline execution,
query designer and import/export routing, and visible per-shard failures. Add
coverage for gaps found during that validation and preserve unsharded behavior.

For each implemented section, run its focused tests and relevant direct/REST/gRPC
and Admin integration coverage. Before release, run the sharded-client, daemon,
ADO.NET route-context, Admin Forms, and Admin Reports suites and the full solution
build. Update public documentation only for functionality actually delivered.

## Constraints to Preserve

- The opened master database owns production sharding metadata.
- Catalog updates write pending maps; activation requires an explicit restart or
  client recreation rather than silently changing live routes.
- Admin route context remains per tab. Normal data operations use an explicit
  route; directory lookup supplies that route rather than moving data itself.
- Existing client APIs and REST/gRPC routes remain compatible.
- This backlog does not add cross-shard joins, distributed write transactions, or
  automatic SQL planner fan-out. Explicit diagnostic fan-out already exists.
- Optional aggregate helpers, such as named row-count or schema-drift operations,
  remain separate enhancements rather than prerequisites for these items.
