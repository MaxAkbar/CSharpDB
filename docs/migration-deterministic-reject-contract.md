# Deterministic Migration Reject Contract v1

Architecture decision for durable tolerant row handling in the staged migration
workflow. This contract extends, but does not replace, the existing
`csharpdb-migration-fail-fast/v1` behavior.

Implementation status: the bounded reject model, reject-set digest, v2 batch
digest, v2 receipt storage, and empty ledger schema are present. Execution is
still fail-fast; tolerant policy validation remains closed until the ledger
write, reject-aware validation, CSV evidence, and artifact phases below pass.

## Decision

`MigrationRejectMode.DeterministicRejects` will use
`csharpdb-migration-deterministic-rejects/v1`. For every source batch, the
target must commit these three things in one transaction:

1. all accepted target rows;
2. an ordered, target-owned reject ledger for rejected source rows; and
3. one receipt that binds the complete accepted/rejected outcome.

The target ledger and receipt are the durable authority. A user-facing reject
file is a deterministic, streaming projection of that ledger, written after
the transaction. It is never an append-only checkpoint and is not used to
decide what resume should skip.

Strict fail-fast remains the default. Tolerant mode is opt-in and applies only
to rules explicitly classified as row-rejectable by the source adapter.
Encoding failures, snapshot-integrity failures, resource-limit failures,
unrecoverable CSV syntax, cancellation, target failures, and unknown
exceptions remain fatal.

## Versioned Records

The tolerant path introduces these independently tagged records:

| Record | Contract tag | Purpose |
| --- | --- | --- |
| Reject behavior | `csharpdb-migration-deterministic-rejects/v1` | Selection and ordering of accepted and rejected outcomes |
| Batch receipt | `csharpdb-migration-batch-receipt/v2` | Counts, cursors, identities, and outcome digests |
| Reject ledger entry | `csharpdb-migration-reject-entry/v1` | Durable source position, diagnostic, and optional raw value |
| Reject artifact | `csharpdb-migration-reject-artifact/v1` | Canonical operator-facing serialization |

A v2 receipt contains the existing target, plan, catalog, source fingerprint,
snapshot, source-object, batch-ordinal, and cursor bindings plus the reject
contract version, reject-set digest, accepted row count, rejected row count,
and the v2 batch digest. The batch digest binds the accepted payload and the
ordered accepted/rejected outcome sequence. The input count is therefore the
checked sum of accepted and rejected counts rather than another independent
field. A nonempty source interval may have zero accepted rows; an all-reject
batch still commits a ledger and receipt so its cursor can advance exactly
once.

## Outcome and Digest Invariants

Each input row produces exactly one outcome in source-row order:

- **accepted** — the source ordinal and canonical typed target-row payload; or
- **rejected** — the source ordinal and one canonical reject entry.

Only the first row-rejectable failure in planned column order is recorded for
a row. Retrying a row with different transforms to search for additional
failures is outside v1.

The reject digest covers complete canonical reject entries in ledger order.
The v2 batch digest covers accepted rows in target insertion order, the reject
contract and reject digest, and the ordered sequence of source ordinal and
outcome kind. Counts or unordered aggregate digests alone are insufficient
because they would not detect an accepted/rejected reordering.

All digests use the migration canonical framing and SHA-256 conventions. They
bind the reject contract version directly and the plan-selected reject limits
through the bound plan digest.
Free-form exception messages, timestamps, output paths, and machine-specific
data never enter canonical bytes.

On replay, the runner must reproduce the batch from the pinned source snapshot
and policy, recompute every count and digest, and compare them with the stored
receipt before skipping the batch. Any difference is a hard receipt mismatch;
the runner does not repair, merge, or append to the existing outcome.

The public sealing order is reject digest first, then batch digest.
`MigrationBatchDigest.Compute(batch)` selects v2 and therefore rejects an
empty, malformed, or stale `RejectDigest`. The explicit v1 overload exists
only for a target advertising the legacy fail-fast receipt format.

## Cursor Invariants

Cursors describe contiguous **input** intervals, not accepted-row intervals.
Rejected rows therefore advance the cursor in the same way as accepted rows.

- Batch ordinals are contiguous from zero for each source object.
- A batch start cursor equals the preceding receipt's next cursor.
- Source-row ordinals are contiguous across both outcome kinds.
- `nextCursor = null` proves end of source; an all-reject terminal batch is
  still emitted and receipted.
- A cursor is bound to the source snapshot, projection, reader policy, reject
  policy, contract version, and all resource limits that can affect outcomes.
- Resume may optimize reading from the last receipt's next cursor, but it must
  first validate the complete stored receipt chain and must reproduce any
  receipt it elects to compare.

Changing the source, schema, projection, normalization, rule classification,
reject limit, or contract version changes the binding and cannot resume an
existing tolerant run.

## Reject Ledger Entry

Entries are ordered by source object, batch ordinal, source-row ordinal, then
reject ordinal. v1 produces one reject entry per rejected row. Each entry
contains:

- contract version, source object ID, batch ordinal, and source-row ordinal;
- stable `MIG-*` diagnostic rule ID;
- one-based CSV logical-record and data-record numbers;
- one-based start and end physical lines;
- zero-based column index and stable column object ID when known;
- field kind and whether the field was quoted;
- raw value when a field value is available; and
- the canonical entry byte count.

The diagnostic ID, not a localized message, identifies the failure. Structural
failures without a reliable record boundary are fatal and consequently do not
create a ledger entry. A future contract may add recoverable parser failures,
but v1 must not guess the next record boundary.

### CSV raw-value definition

For CSV, `rawValue` means the exact decoded logical field text produced by the
pinned reader after character decoding and RFC 4180 quote/escape removal, but
before null-token mapping, normalization, type inference conversion, or target
conversion. It is not the original lexical byte slice and does not include
surrounding quotes.

Empty fields use an empty string. A configured null token retains its decoded
text while its field kind records `Null`. A physically missing field has kind
`Missing` and `rawValue = null`. Embedded CR, LF, delimiters, and quotes are
preserved as characters; canonical JSON escaping changes only their serialized
representation. The line range and quoted flag let an operator locate the
source without pretending the reject artifact is a byte-exact source copy.

## Privacy and Resource Bounds

Reject ledgers and artifacts are sensitive because they may contain source
values. They follow stricter handling than migration reports:

- raw values never enter exceptions, console output, telemetry, ordinary run
  reports, receipts, diagnostic messages, or file names;
- the operator must explicitly select tolerant mode and a reject-artifact
  destination;
- artifact creation uses an exclusive file, rejects traversal and
  reparse-point destinations, writes a same-directory temporary file, and
  publishes by atomic replacement without widening the parent directory ACL;
- incomplete temporary artifacts are removed on retry; and
- retention and deletion are explicit operator responsibilities.

Digests provide integrity, not redaction. Receipt and ledger access remains
restricted with the staged target even though receipts contain only aggregate
digests and counts.

The plan must carry positive limits for rejected rows, raw UTF-8 bytes per
value, raw UTF-8 bytes per batch and run, and serialized artifact bytes. These
limits, their public absolute ceilings, and the CSV reader's field, record, and
column ceilings are validated before target creation and are included in the
plan digest. Accounting uses checked arithmetic. These plan-bound limits are a
remaining enablement item; the current core enforces absolute per-value,
per-row, per-batch, and reject-count ceilings while tolerant execution stays
disabled.

Reader resource-limit failures are fatal rather than rejects. If adding the
next reject would exceed a reject limit, the current batch is not committed;
previously receipted batches remain valid. Ledger reads and artifact generation
are streaming and bounded by batch; implementations must not collect the full
reject set or artifact in memory.

The artifact is canonical UTF-8 JSON Lines, ordered exactly like the ledger,
with a versioned binding header followed by canonical reject entries. It has
no timestamps or environment-specific paths. Re-materializing it from the same
target ledger produces identical bytes.

## Atomic Write Contract

The target accepts one prepared batch containing accepted rows and canonical
reject entries. Within one target transaction it:

1. verifies that no receipt already owns the batch ordinal;
2. inserts all accepted rows;
3. inserts every reject ledger entry;
4. inserts the v2 receipt with exact counts and digests; and
5. commits once using the existing indeterminate-commit handling.

Ledger rows have a uniqueness key scoped by target, plan, source object, batch,
and reject ordinal. A receipt and its ledger rows cannot exist independently.
When an existing receipt is found, the target validates the replayed batch
against it and performs no inserts.

## Crash and Cancellation Matrix

| Interruption point | Durable state | Required retry behavior |
| --- | --- | --- |
| Before transaction | No batch state | Rebuild and write the batch |
| After accepted rows, before ledger | Transaction uncommitted | Roll back all rows |
| After ledger, before receipt | Transaction uncommitted | Roll back rows and ledger |
| After receipt, before commit | Transaction uncommitted | Roll back all three records |
| Commit succeeds but acknowledgement is lost | All three records committed | Read receipt, reproduce outcome, validate, and skip |
| After commit, before artifact projection | All three records committed | Regenerate artifact from the ledger |
| During artifact write or publish | Target remains authoritative; temp may exist | Delete/replace temp and regenerate identical bytes |
| Cancellation before commit begins | No current-batch state | Honor cancellation |
| Cancellation after commit is invoked | Commit result may be indeterminate | Resolve through receipt lookup; never assume rollback |
| Source, policy, count, digest, or cursor mismatch | Prior receipts only | Stop without changing the target or artifact |

Tests must inject faults at every transaction boundary, include accepted-only,
mixed, and all-reject batches, and repeat each case across a fresh process.

## Pipeline Reuse Decision

`CSharpDB.Pipelines` supplies useful vocabulary for error mode, reject counts,
checkpoints, history, and metrics. Those concepts may be exposed through
adapters, but its current runtime and reject records are not reused as the
migration durability mechanism.

The pipeline runtime commits destination writes, reject logging, and checkpoint
updates in separate operations. It retries a failed batch one row at a time,
stores free-form exception messages and full payload JSON, and does not bind
rejects to a target-owned transactional receipt. Reusing it unchanged could
duplicate accepted rows, lose rejects, change failure selection after a crash,
and disclose unbounded source payloads.

Migration owns the canonical reject entry, ledger, receipt, and crash contract.
Pipeline adapters may translate final safe counts and stable diagnostic IDs
after commit; they must not become the authority for resume.

## Phased Enablement

1. Add provider-neutral v1 outcome/entry models, canonical codecs, digest test
   vectors, limit validation, and negative compatibility tests. Keep execution
   fail-fast.
2. Add the CSharpDB v2 receipt and reject-ledger schema plus one atomic target
   write API. Prove rollback and indeterminate-commit behavior with fault
   injection before allowing a source adapter to request it.
3. Add CSV row-outcome metadata for the explicit row-rejectable rule registry,
   then add bounded artifact materialization. Parser, integrity, and resource
   errors remain fatal.
4. Expose CLI tolerant mode only after cross-process resume, tamper, privacy,
   large-stream, all-reject, limit, cancellation, and artifact-regeneration
   tests pass. Strict mode remains the default and release baseline.

Later providers must pass the same contract suite; provider-specific exception
catching or sidecar logging is not sufficient.

## Compatibility

The existing `csharpdb-migration-fail-fast/v1` behavior remains the default,
but newly created targets use `csharpdb-migration-batch-receipt/v2` even while
all reject sets are empty. A v1 receipt does not prove a reject ledger or
ordered outcome digest and must never be interpreted as tolerant. This
implementation can resume a v1 staged target only under the fail-fast policy:
it reproduces the historical v1 batch digest, synthesizes the empty reject-set
digest in memory, and continues writing the 13-column v1 receipt shape. It
rejects invalid Unicode scalar data rather than reproducing v1 replacement
fallback collisions.

Deterministic rejects require a newly created target advertising the v2 receipt
and ledger capability. An in-progress v1 target is never upgraded in place and
cannot select tolerant handling. A target or source-object receipt chain cannot
mix receipt or reject contract versions. Unknown version tags fail closed.

Plans created before this feature continue to select fail-fast. Selecting
tolerant mode changes the canonical plan digest because it adds the reject
contract version, rule registry, limits, and artifact policy. This prevents an
old plan or receipt from silently acquiring skip semantics.
