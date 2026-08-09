# What's New

## CSharpDB 4.5.1

CSharpDB 4.5.1 is the first published release in the 4.5 line. The `v4.5.0`
Git tag is retained as release-attempt history, but it was not published as a
release. These notes therefore preserve the complete public change from
v4.4.0 to v4.5.1.

This release adds a persisted logical SQL type system across the engine,
providers, transports, migration tooling, archives, and public metadata. It
also adds bounded XPath 1.0 querying for XML values. Declared types, coercion,
materialization, and schema rendering now follow one contract end to end.

### Batch Insert Performance

- Removed an avoidable per-value allocation from successful declared-type
  assignment coercion. Qualified type-mismatch diagnostics are now formatted
  only on failure, preserving the same error messages and SQL semantics while
  improving in-memory SQL batch-insert throughput.

### Complete SQL Type System

- Added 25 logical SQL type kinds plus the special generated `ROWVERSION`
  declaration: `BOOLEAN`; `TINYINT`, `SMALLINT`, `INTEGER`, and `BIGINT`;
  `REAL`, `DOUBLE PRECISION`, and `DECIMAL`; `CHAR`, `VARCHAR`, and `TEXT`;
  `BINARY`, `VARBINARY`, and `BLOB`; `UUID`; `DATE`, `TIME`, `DATETIME2`, and
  `DATETIMEOFFSET`; both `INTERVAL` families; `JSON`; `XML`; `BIT(n)`; and
  `BIT VARYING`.
- Persisted canonical declarations and facets independently from CSharpDB's
  compact physical carriers. Catalogs, typed result metadata, schema scripts,
  migration plans, and archives now retain the declared SQL type instead of
  reducing every value to its storage carrier.
- Enforced declared ranges, lengths, precision and scale, fractional-seconds
  precision, UUID/document validation, exact bit lengths, assignment and cast
  rules, comparisons, aggregates, set operations, defaults, indexes, and table
  rewrites.
- Added a dedicated exact `DECIMAL` carrier with deterministic arithmetic,
  comparison, ordering, aggregation, precision/scale propagation, streaming,
  and overflow behavior. `DECIMAL` defaults to `(18,2)`, while `DECIMAL(p)`
  uses scale zero; assignment rejects excess fractional digits rather than
  rounding them.

The complete aliases, facets, CLR mappings, and physical carriers are listed
in the [SQL datatype reference](https://csharpdb.com/docs/sql-reference.html#data-types).
`NULL` remains a value and runtime tag, not a declarable column type.

### Integer, Boolean, Temporal, and Rowversion Semantics

- `INT`/`INTEGER` are now signed 32-bit values and `BIGINT` remains signed
  64-bit. `TINYINT` is unsigned 8-bit and `SMALLINT` is signed 16-bit. Literal
  inference, assignments, casts, defaults, updates, identities, arithmetic,
  aggregates, and rewrites enforce the declared range with checked overflow
  behavior. Potentially large counts, ranks, lengths, and identifiers remain
  64-bit.
- `BOOLEAN`, `BOOL`, and bare `BIT` are logical Booleans and materialize as
  `bool`. Numeric zero converts to false and any finite nonzero value converts
  to true. `BIT(n)` and `BIT VARYING`/`VARBIT` remain distinct packed
  bit-string types whose exact bit length survives storage and transport.
- Character and binary facets now apply Unicode-scalar or byte limits with
  fixed-width padding where required. UUID, temporal, interval, JSON, and XML
  values are validated and rendered canonically; temporal precision ranges
  from zero through seven fractional digits.
- Bare `TIMESTAMP` now aliases the generated eight-byte `ROWVERSION`
  concurrency token. Offset-free temporal values use `DATETIME2`/`DATETIME`;
  offset-aware values use `DATETIMEOFFSET` or `TIMESTAMP ... WITH TIME ZONE`.
- Rowversion allocation is database-wide and durable. Every inserted row and
  every successful update of a row containing a rowversion receives a new
  token, including no-op, raw-SQL, and trigger-issued updates. Allocation is
  covered by rollback, WAL recovery, checkpoint, reopen, and concurrent-write
  behavior.

### XML Query Support

- Added `XML_EXISTS`/`XMLEXISTS` and `XML_VALUE` with XPath 1.0 evaluation,
  NULL propagation, optional JSON namespace-prefix maps, XPath scalar
  conversion, and explicit diagnostics when `XML_VALUE` selects multiple
  nodes.
- XML parsing is bounded and secure. DTD declarations and external entities
  are rejected, and document depth and XPath length are limited.
- The supported surface is the documented function-style API. Standard
  `XMLEXISTS(... PASSING ...)`, `XML_TABLE`, and XML path indexes are not part
  of 4.5.1.

### Providers, Migration, and Interchange

- Updated ADO.NET and EF Core mappings plus HTTP, gRPC, native, Node, Admin,
  CLI, MCP, schema comparison, and import/export paths to preserve canonical
  logical metadata and typed values. This includes CLR Booleans and integer
  widths, exact decimals, temporal values, UUIDs, bit strings, and generated
  rowversion tokens.
- Added explicit SQL Server migration mappings for integer widths, `bit`,
  temporal types, and `timestamp`/`rowversion`. Migration plans now preserve
  the logical target SQL declaration separately from its physical carrier.
- Added a separately digested current `4.5.1` migration capability catalog
  while retaining the immutable 4.5.0, 4.4.0, and 4.3.0 catalogs for
  deterministic replay.
- Native table archive format v8 records the final 4.5 integer semantics and
  preserves every logical facet. CSV/JSON migration manifests and streaming
  exports retain exact decimals and logical target declarations.
- Expanded the public reference across all supported surfaces and added a
  regression guard that requires every logical type, accepted alias, and the
  special `ROWVERSION` declaration to remain documented.

### Upgrade and Compatibility Notes

- Applications that relied on `INTEGER` as a 64-bit declaration must use
  `BIGINT`. Descriptor-less integer columns from older databases remain
  `BIGINT`; preview metadata-version-10 and archive-v7 `INTEGER` declarations
  are also exposed as `BIGINT` so existing values are never silently narrowed.
- Bare `BIT` is Boolean. Use `BIT(n)` or `VARBIT(n)` for bit strings.
- Bare `TIMESTAMP` is rowversion. Use `DATETIME2` for offset-free date/time or
  `DATETIMEOFFSET` for offset-aware values. `ROWVERSION` and bare `TIMESTAMP`
  are generated column declarations, not cast or `ALTER COLUMN TYPE` targets.
- A table may contain one generated, nonnullable rowversion column. It cannot
  be assigned, defaulted, collated, used as an identity, or included in a key,
  foreign key, or index. The legacy `BLOB ROWVERSION NOT NULL` declaration
  remains accepted.
- On first 4.5 upgrade, legacy per-row rowversion values are regenerated and
  outstanding concurrency tokens are intentionally invalidated; rowversion
  values are opaque equality tokens. Keep the normal database backup before
  upgrading.
- Metadata version 11 records the final logical semantics without redesigning
  existing database pages. Existing physical values remain readable; 4.5 adds
  the declared-type metadata and exact-decimal representation needed for the
  new contract.
