# CSharpDB.Migration

Shared, provider-neutral contracts for the CSharpDB migration assurance stack.
The project remains non-packable until its wire contracts and public SDK
interfaces complete an explicit freeze review.

The project contains the Phase 1 planning vertical slice and the
provider-neutral part of the Phase 2 staged-apply slice:

- versioned catalog and plan artifact formats;
- deterministic SHA-256 artifact envelopes;
- source-neutral catalog objects with explicit containment, set-like
  dependencies, ordered role-qualified schema members, and safe source identity;
- stable compatibility, evidence, diagnostic, and mapping states;
- an embedded, digested CSharpDB 4.2.0 capability catalog tied to the installed
  Migration and Primitives binaries;
- target plans bound to the source-catalog digest, capability digest, naming
  algorithm, and versioned mapping policy;
- detailed target-capability evaluation for columns, keys, foreign keys,
  checks, and indexes using the planned dependency types and source facets;
- `preserve`, `queryable`, and `custom` mapping policies with versioned
  conversion descriptors;
- shared versioned decimal and relational text codecs referenced by migration
  conversion descriptors, plus identifier rules reused by EF and migration;
- deterministic target naming with namespace flattening, case-insensitive
  collision handling, reserved-name protection, and the 128-character limit;
- profiling-coverage metadata that requires sample-derived mappings to be
  checked across the full apply stream;
- separate draft-plan and apply-readiness validation, including durable
  acceptance of both overrideable diagnostics and intentional exclusions;
- deterministic plan and converted-batch digests bound to source snapshot,
  cursor chain, ordered columns, target value tags, and exact payloads;
- strict apply-time conversion for every registered v1 mapping, including
  nullability, finite REAL values, decimal precision/scale, BLOB copying, and
  logical text codecs;
- the versioned `csharpdb-migration-fail-fast/v1` reject contract: canonical
  object/row/column ordering, safe first-error metadata, no submission of the
  failing prepared batch, and exact replay of prior transactional receipts;
- a bounded streaming apply coordinator with transactional-receipt resume
  verification and ordered schema-stage orchestration;
- provisional inspector, streaming source, target, snapshot, and validator
  interfaces while this project remains non-packable;
- an immutable awkward synthetic inspector, row source, and deterministic
  planner; and
- structural validation, duplicate-property rejection, unknown-member
  rejection, and defense-in-depth scanning for common credential shapes.

The scanner is not a substitute for provider-specific safe identity models.
Adapters must construct identities from non-secret fields and must never pass a
raw connection string into an artifact.

The v1 property order, property-presence rules, and ordering of set-like
collections are part of the wire contract and are protected by golden JSON,
digest, planner, and name-mapping vectors. A wire-shape change requires a new
artifact format version.

Provider packages and provider-specific execution code do not belong here.
CSV, SQLite, LiteDB, server, and Access dependencies remain in optional adapters described in
[`docs/migration-tooling-phase-0-decisions.md`](../../docs/migration-tooling-phase-0-decisions.md).

Phase 1 is complete for the in-repository planning slice. Exact decimal,
date/time, GUID, and identifier behavior is shared through
`CSharpDB.Primitives` and version-bound in migration conversion descriptors.
The target-specific implementation lives in `CSharpDB.Migration.CSharpDb`. It
creates a new staged database atomically, stores rows and receipts together,
and stops at `AwaitingValidation`; activation belongs to the validation phase.
The Phase 2 receipt schema intentionally supports deterministic fail-fast only.
`DeterministicRejects` is rejected before target creation because skip-and-record
would require rejected-row digests and records to commit atomically with each
batch receipt; it is not silently approximated with a sidecar log.

The current CLI proof surface is:

```text
csharpdb migrate inspect --source synthetic --out catalog.json
csharpdb migrate plan catalog.json --out plan.json [--profile preserve|queryable] [--accept-exclusions all|<id,...>]
csharpdb migrate preview plan.json --catalog catalog.json [--format text|json]
csharpdb migrate apply plan.json --catalog catalog.json --target staged.csdb --out run.json [--resume] [--format text|json]
```
