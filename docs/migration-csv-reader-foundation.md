# Migration CSV Reader Foundation

This note records the first Phase 4A implementation slice. It is intentionally
smaller than the complete CSV import/export track: it establishes a safe CSV
lexical boundary before schema inference, migration planning, target writes,
resume, rejects, manifests, or export are added.

## Implemented Boundary

`CSharpDB.Migration.Files` owns the approved CsvHelper 33.1.0 dependency and
exposes no CsvHelper types. `CSharpDB.Migration` and the existing pipeline
packages remain provider-neutral.

`CsvStreamingReader` is a single-use, forward-only logical-record reader. It:

- parses RFC 4180 quoting, doubled quotes, and quoted multiline fields;
- accepts CRLF, LF, and CR input without normalizing field contents;
- preserves blank and duplicate headers for later inspection policy;
- preserves decoded whitespace and culture-looking lexemes exactly;
- distinguishes `Text`, `Empty`, `Null`, and `Missing` fields;
- preserves an empty final field through `PresentFieldCount`;
- performs strict configured decoding and strict UTF-8 by default;
- detects UTF-8, UTF-16, and UTF-32 byte-order marks without falling back to
  replacement characters;
- enforces configured field, logical-record, and field-count limits; and
- reports stable, value-free rule IDs for malformed data, encoding failures,
  limits, and record-shape failures.

One logical record is live at a time. A quote-aware bounded reader stops an
oversized record before CsvHelper can grow a proportional record buffer. The
parser may decode a fixed-size input buffer ahead, but it does not materialize
the source file or all records.

## Deliberate Semantics

- The current reader accepts one explicit delimiter character. Deterministic
  delimiter detection and confidence reporting belong to the inspection
  slice.
- A configured null token matches unquoted decoded values exactly. Quoting the
  token preserves literal text, which makes the default convention reversible.
  An explicit lossy option can also match quoted tokens. Empty null tokens are
  rejected so empty and null cannot be conflated.
- A header or explicit expected width establishes record width. For a
  headerless input without an explicit width, the first data record establishes
  it. Short records are padded with `Missing`; extra fields fail with a stable
  shape diagnostic.
- Missing fields are not converted to core migration null values. The future
  source adapter must reject them or apply an explicit mapping policy before it
  emits `MigrationDataRow` values.
- Cancellation is observed before, during, and after each logical-record read.
  CsvHelper 33.1.0 has no cancellation-token parser overload, so the adapter
  threads the active token through its bounded `TextReader` into the underlying
  stream read.
- Strict mode ships first. Tolerant skip-and-record behavior requires the
  durable reject/receipt design to preserve deterministic resume semantics.

## Next Phase 4A Slices

1. Add bounded delimiter and encoding inspection with confidence and immutable
   source fingerprint/snapshot metadata.
2. Add confidence-bearing schema inference and explicit column overrides.
3. Adapt logical records into `IMigrationDataSource`, including a deliberate
   missing-field policy and deterministic cursors.
4. Reuse staged prepared writes, transactional receipts, validation, and
   activation from Phases 2-3.
5. Add deterministic tolerant rejects only after their durable transaction
   contract is complete.
6. Add RFC 4180 export, typed manifests, checksums, and the separately named
   spreadsheet-safe lossy mode.
