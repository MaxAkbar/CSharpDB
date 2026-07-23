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

## Strict Full-Stream Validation

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

Failures use `MigrationRowRejectedException.CreateForSource`, which has no
free-form message or caller-supplied inner exception and bounds its identifier
tokens. The CSV adapter passes only constant rules and canonical object IDs, so
raw values, stable keys, and cursor text never enter its exception or failure
report.

## Batches And Cursors

`MigrationReadRequest` now carries the plan's `MaxBatchBytes` and
`MaxValueBytes`; apply and validation pass those values to every source read.
CSV batches split before either the requested row count or a conservative
source-canonical byte upper bound would be exceeded. The apply runner remains
the final authority after target conversion. Fixed adapter safety ceilings may
split an unusually large direct request earlier, keeping object overhead
bounded independently of untrusted request values.

Cursor contract `csharpdb-csv-cursor-v1` records the next zero-based data-row
offset and global batch ordinal. Its position token binds those ordinals plus
the source fingerprint, snapshot, catalog, schema/scalar contracts, complete
inferred schema, ordered projection, row limit, and byte/value limits. A cursor
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

This strict slice does not invent tolerant skip semantics. Durable reject
files require a row/reject/receipt transaction contract so a crash cannot
change which rows were skipped. Typed binary fields also remain deferred until
a manifest declares base64 or hex intent and a decoded-size bound. The durable
raw-snapshot and policy package used to cross process boundaries is described
in [`migration-csv-retained-package.md`](migration-csv-retained-package.md).
CLI wiring, export, typed binary declarations, and large-stream performance
qualification remain later Phase 4A work.
