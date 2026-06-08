# Sharding Remaining Work Plan

Internal implementation plan for the remaining sharding work after the current
first slices. This file is a companion to `docs/sharding-roadmap-next-phases.md`;
the roadmap remains the historical planning record, while this document is the
execution plan for what is still left.

This document is intentionally kept under the top-level `docs/` folder and is
not part of the public `www/docs` site.

## Current Baseline

The current sharding implementation already includes the core route-aware client
surface, shard-admin control plane, catalog-backed pending maps, exact-key and
bucket-range migration first slices, read-only fan-out SQL, and replica metadata.

Implemented baseline to preserve:

- Route-key based routing through `CSharpDbShardedClient.ForRoute(...)`.
- Remote route context through `CSharpDbClientOptions.RouteContext`, HTTP
  headers, and gRPC metadata.
- Admin per-tab route state and route selector for query, data, and collection
  tabs.
- `ICSharpDbShardAdminClient` for map snapshots, route previews, shard status,
  execute-on-all-shards, catalog state, catalog validation/apply, migration
  execution, and migration history.
- REST and gRPC shard-admin endpoints for direct and remote Admin use.
- Catalog-backed pending map updates with validation and restart-required
  activation.
- Exact route-key and bucket-range migration first slices with manifests, write
  fences, copy, verification, pending map updates, history, and recovery
  metadata.
- Read-only fan-out SQL with per-shard results and no distributed SQL planner.
- Replica role metadata, primary relationships, lag metadata, and promotion
  eligibility flags.
- Admin can create sharding metadata for a new local direct master DB. Existing
  monolithic DBs still require a separate split/backfill/cutover workflow before
  the master catalog is seeded.

Non-negotiable rules:

- Sharding metadata must live in the opened master DB; app configuration only
  selects which master DB to open.
- Existing migration APIs must remain source-compatible.
- Applying catalog changes continues to write pending maps and does not silently
  mutate live routing in-process.
- No automatic cross-shard SQL inference, cross-shard writes, cross-shard joins,
  or distributed transactions are introduced by this plan.
- Desktop Admin continues to open one local direct database unless that opened
  master DB contains an active shard map.

## Milestones

### 1. Admin Route-Aware Completion

Goal: every Admin workflow that reads or writes shard-owned data must either use
a per-tab route-bound client or explicitly present itself as a shard-admin
fan-out/cluster operation.

Implementation work:

- Extend per-tab route context to forms, reports, pipelines, query designer,
  import/export, data-model launches, drilldowns, and any child tabs opened from
  route-aware parents.
- Reuse the existing `AdminRouteSelector` and `DatabaseClientHolder` route-bound
  client factory instead of adding another route model.
- Add a small Admin routing abstraction for feature packages that currently
  receive the singleton `ICSharpDbClient`; it must resolve the effective client
  from the active tab route when a route is required.
- Add route badges to every routed workflow. The badge must show keyspace, route
  key, shard id, bucket, token, and map version when available.
- Block route-required sharded workflows until a route is selected. Unsharded
  connections must not show new blocking states.
- Preserve tab isolation: changing a route in one tab must not alter another
  tab, even when both tabs point at the same table, form, report, or pipeline.
- When a parent tab opens a child tab, copy the parent's route snapshot into the
  child unless the child is explicitly a shard-admin workspace.

Acceptance criteria:

- Forms can browse, insert, update, and delete records on the selected route.
- Reports and report previews run against the selected route unless the report
  explicitly uses fan-out tooling.
- Pipelines run stored pipeline catalog operations and execution against the
  selected route when route-owned data is involved.
- Query designer SQL previews and generated-query launches inherit the active
  query tab route.
- Import/export uses the selected route for table archive operations.
- Existing unsharded Admin tests remain unchanged.

### 2. Shard Directory Workflows

Goal: make the global shard directory a real operator-managed lookup surface for
alternate keys that must resolve to a route context before querying shard-owned
data.

Public API work:

- Add `ICSharpDbShardDirectoryClient`.
- Add async methods for:
  - resolving a directory lookup key to `CSharpDbShardDirectoryResolution`;
  - reserving a directory entry;
  - activating a reserved entry;
  - idempotent upsert/repair of an entry;
  - disabling, deleting, or stale-marking an entry;
  - listing directory definitions and filtered entry summaries for Admin.
- Add `CSharpDbShardDirectoryWritePolicy` with explicit values for
  reserve-before-write and write-first-repair flows.
- Add REST endpoints under `/api/sharding/directories/...`.
- Add matching gRPC RPCs and protobuf messages.
- Keep directory APIs separate from normal table, SQL, and collection APIs.

Catalog and validation work:

- Persist directory entries in the catalog-backed JSON document.
- Keep directory metadata in the master catalog and require catalog writes for
  directory mutations.
- Validate directory entries against target keyspace, route key, resolved shard,
  map version, and state.
- Treat `Reserved`, `Active`, `Moving`, `Disabled`, and `Deleted` as the allowed
  states.
- Reject stale active entries unless the operator explicitly marks them
  historical, disabled, deleted, or stale.
- Make reserve and activate operations idempotent for the same directory,
  lookup key, target route, shard, and map version.

Admin work:

- Add a Sharding workspace directory panel for definitions, entry counts, lookup
  simulation, and entry state filtering.
- Add an operator flow to reserve and activate an entry for write-once workloads.
- Add a repair flow that can upsert or stale-mark entries after migration.
- Never display hidden connection strings or API keys in directory templates.

Acceptance criteria:

- An alternate key can resolve to a route context and then be used with
  `ForRoute(...)`.
- Duplicate active entries for the same directory and lookup key are rejected.
- Reserved entries can be safely retried.
- Deleted entries are not returned by normal resolution.
- Directory APIs work over direct, REST, and gRPC clients.

### 3. Migration Hardening

Goal: turn the current migration first slices into durable, observable,
resumable operator workflows.

Public API work:

- Add `CSharpDbShardMigrationPlan`.
- Add `CSharpDbShardMigrationCheckpoint`.
- Add `CSharpDbShardMigrationProgress`.
- Add APIs to create/preview a migration, start it, read progress, resume a
  failed or interrupted migration, retry a failed step, and abandon a migration
  with an operator comment.
- Keep `MigrateExactRouteKeyAsync(...)` and `MigrateBucketRangeAsync(...)`
  source-compatible. They may wrap the new job engine and block until the job
  reaches a terminal state.

Execution work:

- Persist durable step-level checkpoints in catalog mode.
- Checkpoint after validation, source scan, destination copy, verification,
  optional source delete, directory repair, catalog pending-map write, and final
  history write.
- Resume only from the last verified checkpoint.
- Make copy steps idempotent when overwrite is enabled.
- Keep the active map unchanged until verification succeeds and the catalog
  pending-map write succeeds.
- Preserve write fencing across active migration jobs and reject overlapping
  exact-key or bucket-range migrations.
- Add broader directory repair for exact-key and bucket-range migrations:
  matching active entries move to the destination shard and pending map version;
  ambiguous or partially repaired entries are marked stale for operator review.
- Include `RequiresOperatorRecovery` and `RecoveryAction` for all non-terminal
  or failed states that need manual intervention.

Admin work:

- Add migration preview in the Sharding workspace.
- Add manifest editing with validation before execution.
- Show source shard, destination shard, route key or bucket range, estimated
  affected data, progress, current checkpoint, verification output, and recovery
  action.
- Require confirmation before metadata-only recovery or catalog-only ownership
  changes.

Acceptance criteria:

- Exact-key and bucket-range migrations can resume after a process interruption.
- A failed verification leaves the active map unchanged and records recovery
  instructions.
- Retrying a partially copied migration does not duplicate rows/documents.
- Directory entries are updated or stale-marked consistently with migration
  outcome.
- Migration history includes enough information to audit every state transition.

### 4. Admin Fan-Out And Operator Tooling

Goal: expose explicit diagnostic fan-out and migration controls in Admin without
implying distributed SQL support.

Implementation work:

- Add a read-only fan-out SQL panel in the Sharding workspace.
- Show one result grid per shard with shard id, success/error state, duration if
  available, row count, and query output.
- Reject DDL/DML in the UI before submit when possible, while keeping server-side
  validation authoritative.
- Add optional explicit aggregate helpers only for named operations, such as
  per-shard row counts or schema drift checks.
- Add migration action panels for exact-key and bucket-range flows.
- Add route simulation and directory lookup side by side so operators can see
  route-key routing and alternate-key routing separately.

Acceptance criteria:

- Fan-out results are grouped by shard and partial failures do not hide
  successful shard results.
- Admin labels fan-out output as diagnostic read-only tooling.
- Migration controls can validate a manifest before starting a job.
- Recovery actions from migration history are visible without inspecting logs.

### 5. Replication And Failover

Goal: add high-availability primitives after catalog, directory, and migration
workflows are stable.

Replication work:

- Build replication from retained change feeds or WAL-derived primitives, not
  router retry behavior.
- Persist replica role, primary shard id, health, lag, last replicated time, and
  promotion eligibility in catalog-backed metadata.
- Add a replication worker that can copy committed changes from primary to
  replica and report lag.
- Add validation that replicas cannot own bucket ranges, exact-key pins, or
  active directory ownership.

Manual failover work:

- Add APIs to validate promotion readiness, promote a replica, demote or disable
  the old primary, and read failover audit history.
- Promotion must update catalog metadata and write a pending map when ownership
  changes require activation.
- Reject promotion when the replica is ineligible, unhealthy, stale, or behind
  the configured lag threshold unless the operator explicitly acknowledges a
  forced promotion.
- Keep automatic failover out of scope until manual failover is proven.

Admin work:

- Add replica health, lag, promotion readiness, promote/demote actions, and
  failover audit history to the Sharding workspace.
- Require operator confirmation for forced promotion and metadata-only recovery.

Acceptance criteria:

- Replica metadata still flows through direct, REST, and gRPC clients.
- Lag is reported from the replication worker, not only operator-entered
  metadata.
- Manual promotion updates catalog state and records audit history.
- Failed promotion leaves prior primary ownership intact.

### 6. Release Verification

Goal: make sharding release readiness repeatable.

Required verification commands:

```powershell
dotnet test tests\CSharpDB.Tests\CSharpDB.Tests.csproj --filter FullyQualifiedName~CSharpDbShardedClientTests
dotnet test tests\CSharpDB.Daemon.Tests\CSharpDB.Daemon.Tests.csproj --filter FullyQualifiedName~Shard
dotnet test tests\CSharpDB.Data.Tests\CSharpDB.Data.Tests.csproj --filter FullyQualifiedName~Shard
dotnet test tests\CSharpDB.Admin.Forms.Tests\CSharpDB.Admin.Forms.Tests.csproj
dotnet test tests\CSharpDB.Admin.Reports.Tests\CSharpDB.Admin.Reports.Tests.csproj
dotnet build CSharpDB.slnx
```

Acceptance gates:

- Focused sharded-client tests pass.
- Daemon REST/gRPC sharding tests pass.
- ADO.NET route context tests pass.
- Admin Forms and Admin Reports tests pass.
- Full solution build passes.
- Public docs are updated only for user-facing shipped features, not this
  internal plan.

## Public API Summary

Planned additive API surfaces:

- `ICSharpDbShardDirectoryClient`
- `CSharpDbShardDirectoryWritePolicy`
- `CSharpDbShardMigrationPlan`
- `CSharpDbShardMigrationCheckpoint`
- `CSharpDbShardMigrationProgress`
- Migration preview/start/progress/resume/retry/abandon APIs
- Failover readiness, promotion, demotion, and audit APIs

Future existing-database split workflow:

- Keep **Create Sharding** for new sharded setups.
- Add a separate **Shard Existing Database** workflow because existing data must
  be copied and verified before the master catalog becomes active.
- Collect source DB, master DB, shard definitions, route keyspace, per-object
  route-key mappings, batch size, verification mode, and cutover mode.
- Copy schema, backfill route-owned data with durable checkpoints, verify
  counts/checksums, fence writes, copy the final delta, seed the master catalog,
  and reopen Admin against the master DB.

Compatibility requirements:

- Existing sharded client APIs remain source-compatible.
- Existing REST/gRPC shard-admin routes remain stable.
- New REST/gRPC routes are explicit sharding control-plane routes.
- Normal `ICSharpDbClient` data operations remain route-bound and do not infer
  fan-out from SQL.

## Test Matrix

Admin route-aware completion:

- Each remaining route-aware tab stores an independent route snapshot.
- Route changes reload only the current tab.
- Child tabs inherit the parent route unless they are shard-admin tabs.
- Sharded route-required workflows block until a route is selected.
- Unsharded Admin behavior remains unchanged.

Directory workflows:

- Resolve active entries.
- Reject unknown directories, stale active entries, future map versions, invalid
  states, and route/shard mismatches.
- Reserve and activate entries idempotently.
- Disable, delete, and stale-mark entries.
- Validate direct, REST, and gRPC behavior.

Migration hardening:

- Exact-key and bucket-range migrations checkpoint each durable step.
- Resume after interruption.
- Retry partial copy with overwrite enabled.
- Preserve active map on failed verification.
- Repair or stale-mark directory entries after verified movement.
- Reject overlapping migration fences.

Admin fan-out and migration UX:

- Read-only fan-out returns one visible result set per shard.
- Partial shard failure is visible with successful shard results preserved.
- Migration manifest validation displays blocking issues before execution.
- Progress and recovery actions are visible from Admin.

Replication and failover:

- Replica metadata loads from the master catalog.
- Replication worker reports lag and health.
- Promotion readiness rejects unsafe promotion.
- Successful manual promotion records catalog and audit history.
- Failed promotion preserves the previous primary.

## Assumptions

- Work is planned in roadmap order with all remaining phases treated as active
  planning scope.
- The current implemented slices are retained and extended rather than replaced.
- Shard-directory indexes remain optional and external to normal in-shard table
  indexes.
- Resharding remains operator-controlled and manifest-driven.
- Cross-shard write support, cross-shard joins, cross-shard transactions, and an
  automatic distributed SQL planner remain out of scope.
- Automatic failover starts only after manual failover has shipped and passed
  operational validation.
