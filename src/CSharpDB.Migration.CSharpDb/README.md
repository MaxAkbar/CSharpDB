# CSharpDB.Migration.CSharpDb

Target-specific staged execution adapter for `CSharpDB.Migration`.

The adapter creates a new database with atomic create-new semantics and holds
an exclusive same-directory migration lease for its lifetime. Its internal
tables bind the target to the plan/catalog/capability digests, source identity
and fingerprint, actual source snapshot, and a stable random target ID.
Staged files use the engine's write-optimized checkpoint policy with an
explicit 2,048-page LRU cache, so target residency is bounded independently of
the total source size.

Schema actions are applied in ordered, transactionally receipted stages. Each
prepared data batch is converted and digested by the provider-neutral runner;
the target then validates the binding, ordered columns, value tags,
nullability, finite REAL policy, and digest again. Accepted rows, canonical
reject-ledger entries, and their full v2 receipt commit in one explicit
transaction. Resume accepts only exact existing stage and batch receipts and
rejects changed identities, cursor chains, outcome order, or payloads.

`CSharpDbDdlPreviewBuilder` exposes the same ordered target-schema renderer as
an explicit, no-write preview. It reports plan readiness, separates SQL from
document-collection creation actions, and includes empty stages so the output
order is unambiguous. Its versioned digest covers only migrated schema actions,
not internal migration bookkeeping tables, and is independent of the plan
digest so it remains stable after attachment. A plan stores only the digest;
when present, the staged target recomputes and verifies it before creating or
resuming a target. `BuildAndAttachGeneratedDdlDigestBounded` performs one
authoritative bounded render and returns the sealed plan without retaining the
SQL. Legacy externally authored plans without a preview digest remain accepted
for compatibility, while newly CLI-authored plans are always sealed.
Operator-facing preview callers use `BuildBounded`; both bounded entry points
enforce configurable action, per-action SQL, and aggregate UTF-8 limits that
cannot exceed production ceilings. The renderer enforces those limits
incrementally before retaining each action and reports a stable, sanitized
limit kind without publishing SQL or names. Those are render-action limits,
not plan/catalog acquisition or preprocessing limits; callers must bound and
validate those inputs separately.

`CSharpDbDdlScratchValidator` verifies that reviewed preview with bounded
aggregate and parser limits, parses each SQL action once with `CSharpDB.Sql`,
and executes the same parsed statements in one committed, in-memory
transaction. It then reads the resulting catalog and compares it with the
plan's normalized expected schema. Its versioned report contains only digests,
stable stage/action/rule identifiers, readiness, and evidence level; it never
publishes rendered SQL, object names, ASTs, or parser/engine messages. A pass
is scratch-execution and schema-shape evidence only. It is not source semantic
equivalence and does not claim that view or trigger bodies were bound.

## Phase 6B.1: CSharpDB-ready DDL proof

`CSharpDbDdlCompatibilityAnalyzer` accepts one strict, bounded CSharpDB SQL
script. Its initial explicit allowlist is persistent `CREATE TABLE` plus
simple `CREATE INDEX`. Unsupported statements and unproven table or index
features produce stable, span-based diagnostics and stop before scratch
execution; supported statements are never extracted from an otherwise
unsupported script and proven in isolation.

For a completely supported script, the analyzer lowers the schema into the
migration catalog, evaluates the versioned target capability rules, renders
candidate CSharpDB DDL, parses it through `CSharpDB.Sql`, executes it only in a
new in-memory scratch database, and compares the resulting normalized schema
with the intended model. The deterministic report exposes only sanitized
digests, counts, source spans, stable rule identifiers, status, and evidence
level. It does not expose SQL, paths, object names, ASTs, or raw parser and
engine messages.

This analyzer has no existing-target or auto-apply path. It does not create a
database file, modify a migration plan, or install a generated rewrite. Phase
6B.2 will add a separately bounded T-SQL lowering subset; MySQL, SQLite, and
other source dialects remain fail-closed deferrals.

Apply stops at `awaiting-validation`. Activation accepts only a permit derived
from a coherent, published, passing validation report and persists its receipt
atomically. One immutable validation reader snapshot exposes the complete
ordered receipt and reject-ledger streams alongside schema, counts, and rows;
the SDK runner requires exact source-outcome agreement before it can publish a
deterministic-reject report. The project has no overwrite, merge, or replace
API. The provider-neutral SDK artifact writer projects those immutable streams
into canonical JSONL and publishes without overwriting a different file.

The adapter implements both deterministic fail-fast and the opt-in deterministic
reject contract. Conversion visits canonical source objects, rows, and columns
in order, while tolerant batches bind every accepted/rejected outcome to the
receipt and ledger. A cancellation observed before the final commit check rolls
back rows, rejects, and receipt; after that check commit is deliberately
non-cancellable and resume verifies whether it completed. Fresh-process fault
qualification covers accepted-only, mixed, and all-reject batches at every
transaction boundary.

The adapter also recognizes only the exact versioned JSON document-collection
contract. It creates a real Engine collection, stores generated ordinal keys
and already-canonical JSON through a migration-only direct-payload seam, and
refuses duplicate keys rather than updating them. The document writes and
batch receipt share one transaction. Physical `_col_` names are resolved for
resume, schema capture, counts, and validation while normalized evidence keeps
the logical `Collection` shape. Root-array and NDJSON qualification covers
exact bytes, every transaction fault cutoff, reopen/resume, checksums,
activation, and collection API access. See
[`migration-json-collection-projection.md`](../../docs/migration-json-collection-projection.md).

The bounded CLI can now request exact DDL or a separate sanitized scratch
report, but it cannot apply that DDL or promote plan readiness. Identity,
rowversion, and default lowering plus archive restoration remain explicit
follow-up work. SDK reject-artifact publication is qualified independently of
the CLI.
