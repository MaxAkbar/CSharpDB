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
resuming a target. Legacy plans without a preview digest remain accepted.

`CSharpDbDdlScratchValidator` verifies that reviewed preview with bounded
aggregate and parser limits, parses each SQL action once with `CSharpDB.Sql`,
and executes the same parsed statements in one committed, in-memory
transaction. It then reads the resulting catalog and compares it with the
plan's normalized expected schema. Its versioned report contains only digests,
stable stage/action/rule identifiers, readiness, and evidence level; it never
publishes rendered SQL, object names, ASTs, or parser/engine messages. A pass
is scratch-execution and schema-shape evidence only. It is not source semantic
equivalence and does not claim that view or trigger bodies were bound.

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

Identity/rowversion/default lowering, archive restoration, and bounded
operator-facing CLI integration remain explicit follow-up work. SDK
reject-artifact publication is qualified independently of the CLI.
