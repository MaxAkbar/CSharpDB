# Migration CSV Data Source

This note records the fourth Phase 4A implementation slice. It turns one
immutable, schema-bound CSV snapshot into a repeatable `IMigrationDataSource`
without weakening sampled-schema or bounded-memory guarantees.

## Lifetime And Replay

`CsvMigrationDataSource.CreateAsync` accepts the exact
`CsvSchemaInferenceResult`, `CsvSourceSnapshot`, and generated
`MigrationCatalog` used during planning. It rejects a different snapshot or
catalog policy and verifies snapshot integrity before returning. Apply and
validation also compare the source's catalog digest with the plan before any
read.

The snapshot remains caller-owned; disposing the data source prevents new
reads but does not delete the private snapshot.

Every enumeration opens a new forward-only reader over that snapshot. Apply
can therefore replay already receipted batches, and validation can perform
separate count and row passes without depending on a single-use stream. CSV
does not imply a business key, so emitted rows deliberately use a null
`StableKey`; global row ordinals and target-owned transactional receipts carry
execution order and idempotency instead.

## Full-Stream Validation And Row Outcomes

Reads accept only the stable `csv:table:0` object and canonical ordinal column
IDs. Unknown, duplicate, noncanonical, or empty projections fail before a
reader is opened. Values are returned in the exact requested order, including
lexically sorted requests where `csv:column:10` precedes `csv:column:2`.

Every projected non-null scalar passes through
`CsvSchemaInferenceResult.TryNormalizeScalar`:

- text, empty text, and explicit numeric normalization retain scalar policy v1
  behavior;
- a late value that contradicts sampled or declared type evidence fails with
  `MIG-CSV-DATA-TYPE-001`;
- a missing projected field is never coerced to null or empty and fails with
  `MIG-CSV-DATA-MISSING-001`;
- a null in a declared non-nullable column fails with
  `MIG-CSV-DATA-NULL-001`;
- individual value and aggregate row overflows have separate stable size rule
  IDs.

Fail-fast reads raise `MigrationRowRejectedException.CreateForSource`, which has
no free-form message or caller-supplied inner exception and bounds its identifier
tokens. Under the opt-in deterministic-reject contract, the first
`MissingField`, `NullNotAllowed`, or `TypeMismatch` failure in projected column
order becomes a canonical rejected-row outcome. Its fixed evidence registry
records column and record coordinates, physical line span, field kind, quote
state, and the exact decoded text before null mapping or normalization. A null
token retains its text; a missing field records a null raw value.

Parser, encoding, snapshot-integrity, target-conversion, value/row-size,
cancellation, unknown, disallowed-rule, and reject-policy limit failures remain
fatal. Error messages contain no raw value, stable key, or cursor text.

## Batches And Cursors

`MigrationReadRequest` now carries the plan's `MaxBatchBytes` and
`MaxValueBytes`; apply and validation pass those values to every source read.
CSV batches split before either the requested outcome count or a conservative
source-canonical byte upper bound would be exceeded. Accepted and rejected rows
share the same contiguous input interval, so mixed and all-reject batches
advance cursors exactly once. Reject counts, sensitive evidence bytes, and
canonical artifact bytes use the same checked accounting as the target ledger.
The apply runner remains the final authority after target conversion. Fixed
adapter safety ceilings may split an unusually large direct request earlier,
keeping object overhead bounded independently of untrusted request values.

Cursor contract `csharpdb-csv-cursor-v1` records the next zero-based data-row
offset and global batch ordinal. Its position token binds those ordinals plus
the source fingerprint, snapshot, catalog, schema/scalar contracts, complete
inferred schema, ordered projection, row limit, and byte/value limits. A cursor
for deterministic rejects additionally binds the contract, complete allowed-rule
list, and every reject limit. Fail-fast cursor bytes are unchanged. A cursor
from a different snapshot or policy is rejected before scanning. Resume
reparses the immutable snapshot and discards the prefix until it proves the
exact batch boundary; it
does not seek by decoder byte offset, which would be unsafe for buffered,
multibyte, or multiline input.

The first batch has a null `StartCursor`. Nonterminal batches point to the next
boundary, while the terminal batch has `NextCursor = null` only after EOF is
confirmed. No empty batch is emitted. Buffering is limited to one bounded
batch plus the individually bounded logical record needed to decide the next
boundary.

## Deferred Work

The target-owned ledger and receipt, rather than an operator-facing file, are
the durable authority for skipped rows. Capability-qualified SDK validation
now compares the complete source replay with snapshot-scoped target receipts
and ledger entries before report publication. Bounded reject-artifact
publication remains gated, so the CLI still exposes fail-fast apply only. Typed
binary fields also remain deferred until a manifest declares base64 or hex
intent and a decoded-size bound. The durable
raw-snapshot and policy package used to cross process boundaries is described
in [`migration-csv-retained-package.md`](migration-csv-retained-package.md).
Large-stream behavior is qualified by the 50,000-row CI fixture and isolated
100K/1M retained-source benchmarks in
[`migration-csv-performance.md`](migration-csv-performance.md). Export and
typed binary declarations remain later Phase 4A work.
