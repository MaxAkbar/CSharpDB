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

Apply stops at `awaiting-validation`. Activation accepts only a permit derived
from a coherent, published, passing validation report and persists its receipt
atomically. One immutable validation reader snapshot exposes the complete
ordered receipt and reject-ledger streams alongside schema, counts, and rows;
the SDK runner requires exact source-outcome agreement before it can publish a
deterministic-reject report. The project has no overwrite, merge, or replace
API.

The adapter implements both deterministic fail-fast and the opt-in deterministic
reject contract. Conversion visits canonical source objects, rows, and columns
in order, while tolerant batches bind every accepted/rejected outcome to the
receipt and ledger. A cancellation observed before the final commit check rolls
back rows, rejects, and receipt; after that check commit is deliberately
non-cancellable and resume verifies whether it completed. Fresh-process fault
qualification covers accepted-only, mixed, and all-reject batches at every
transaction boundary.

Identity/rowversion/default lowering, archive restoration, and bounded
reject-artifact publication remain explicit follow-up work.
