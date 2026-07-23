# CSharpDB.Migration

Shared, provider-neutral contracts for the CSharpDB migration assurance stack.
The project remains non-packable until its wire contracts and public SDK
interfaces complete an explicit freeze review.

The project contains the Phase 1 planning vertical slice, the provider-neutral
part of the Phase 2 staged-apply slice, and the Phase 3 validation core:

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
- the opt-in `csharpdb-migration-deterministic-rejects/v1` contract with
  capability-gated sources and targets, contiguous accepted/rejected outcome
  replay, canonical reject and batch digests, and separate replay counters;
- a bounded streaming apply coordinator with transactional-receipt resume
  verification and ordered schema-stage orchestration;
- the versioned `csharpdb-canon-v1` logical row codec, cross-platform golden
  vectors, plan-bound row/key projections, and a rename-stable native CSharpDB
  table contract used by archive restore;
- deterministic normalized-schema, 64-bit count, and 256-partition SHA-256
  validation with duplicate preservation and bounded spill/sort;
- self-digesting JSON and deterministic text validation reports containing
  identities, counts, and hashes but no raw row values;
- coherent-snapshot enforcement with `Inconclusive` outcomes when consistency
  cannot be established; and
- a report-before-activation contract that requires a published, canonical,
  semantically `Passed` report before the staged target can activate;
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
creates a new staged database atomically and stores accepted rows, canonical
reject-ledger entries, and v2 receipts in one transaction. The SDK apply runner
permits deterministic rejects only when the source advertises the exact contract
and complete rule registry and the target advertises the current digest and
authoritative ledger capabilities. Validation still stops this path before
report publication, and the CLI remains fail-fast until outcome comparison and
reject-artifact publication are qualified.

The current CLI proof surface is:

```text
csharpdb migrate inspect --source synthetic --out catalog.json
csharpdb migrate plan catalog.json --out plan.json [--profile preserve|queryable] [--accept-exclusions all|<id,...>]
csharpdb migrate preview plan.json --catalog catalog.json [--format text|json]
csharpdb migrate apply plan.json --catalog catalog.json --target staged.csdb --out run.json [--resume] [--format text|json]
csharpdb migrate validate plan.json --catalog catalog.json --target staged.csdb --out validation.json [--level schema|count|checksum] [--spill-dir directory] [--format text|json]
```

`validate` reads schema, counts, and rows from one source snapshot and one target
snapshot. Checksum validation uses bounded temporary spill space and removes its
owned workspace on success, cancellation, or failure. A passing report is
durably published and re-verified before activation; `Different`, `Error`, and
`Inconclusive` reports leave the target staged.
