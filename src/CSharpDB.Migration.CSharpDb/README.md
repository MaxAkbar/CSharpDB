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
nullability, finite REAL policy, and digest again. Target rows and their full
receipt commit in one explicit transaction. Resume accepts only exact existing
stage and batch receipts and rejects changed identities, cursor chains, or
payloads.

The lifecycle ends at `awaiting-validation`. This project intentionally has no
activation, overwrite, merge, or replace API. Validation and activation gates
belong to later migration phases.

The current Phase 2 slice implements the versioned deterministic fail-fast
contract. Conversion visits canonical source objects, rows, and columns in
order, reports only stable first-error metadata, and never submits the failing
prepared batch. A cancellation observed before the final commit check rolls
back both rows and receipt; after that check commit is deliberately
non-cancellable and resume verifies whether it completed. Unsupported
skip-and-record mode is rejected before this adapter creates a target because
the current receipt schema cannot atomically bind durable reject records.

Identity/rowversion/default lowering, durable skip-and-record rejects, archive
restoration, process-crash test harnesses, and activation remain explicit
follow-up work.
