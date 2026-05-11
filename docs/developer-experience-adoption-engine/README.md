# Developer Experience Adoption Engine Plan

This document captures the planned design for a Developer Experience Adoption
Engine in CSharpDB. The goal is to make common development tasks fast,
discoverable, and pleasant enough that developers want to use CSharpDB for new
projects, demos, tests, and local-first applications.

## Summary And Goals

The Adoption Engine should reduce setup friction and make CSharpDB feel helpful
during everyday development. V1 should focus on SQL-first capabilities that are
easy to explain and easy to surface through the existing CLI, Admin, EF Core,
MCP, and documentation paths:

- Schema-aware data seeding.
- Explicit query result caching.
- Computed columns.
- Simple analytics helpers.
- CLI and tooling improvements that make these features discoverable.

This plan builds on existing shipped work: the `csharpdb` CLI, VS Code
extension, EF Core provider, MCP server, generated collections, pipelines, and
Admin UI. The new work should connect those surfaces around developer workflows
instead of creating a parallel toolchain.

## Current Foundation

CSharpDB already has several adoption surfaces:

- `CSharpDB.Cli` provides the `csharpdb` executable, interactive SQL shell,
  meta-commands, file execution, maintenance commands, inspection commands, and
  ETL pipeline commands.
- `CSharpDB.EntityFrameworkCore` supports file-backed EF Core runtime and
  migrations, but computed columns are currently unsupported.
- `CSharpDB.Mcp` exposes schema, data, mutation, and SQL tools for local agent
  integrations.
- `CSharpDB.Generators` improves typed collection ergonomics with source
  generated collection descriptors.
- Admin and VS Code already provide interactive schema and query workflows.

The Adoption Engine should make these pieces feel like one developer story:
create a database, seed it, run useful queries, cache known-expensive reads,
inspect schema, and export examples or fixtures.

## V1 SQL Contract

Data seeding inserts schema-aware sample rows:

```sql
SEED Customers WITH 1000 ROWS;
```

Query result caching is explicit per query:

```sql
SELECT * FROM Customers CACHE 60s;
```

Computed columns can be declared from SQL expressions:

```sql
CREATE TABLE Customers (
    Id INTEGER PRIMARY KEY,
    FirstName TEXT,
    LastName TEXT,
    FullName AS FirstName + ' ' + LastName
);
```

Simple analytics helpers provide beginner-friendly grouped aggregates:

```sql
SELECT COUNT(*) BY Status FROM Bookings;
```

Default semantics:

- `SEED` uses the target table schema, column names, types, nullability,
  identity columns, and foreign keys to generate valid rows.
- `SEED` appends rows and returns inserted row count, skipped column count, and
  generation warnings.
- `CACHE <duration>` is opt-in and scoped to deterministic read-only `SELECT`
  statements.
- Cached query keys include normalized SQL text, parameter values, database
  identity, schema version, and table write versions.
- Computed columns are virtual in v1: values are evaluated on read and are not
  physically stored.
- Computed columns are read-only targets for `INSERT` and `UPDATE`.
- `SELECT COUNT(*) BY Status FROM Bookings` is parser sugar for `SELECT Status,
  COUNT(*) FROM Bookings GROUP BY Status`.

## Additional Adoption Areas

These areas came up while reviewing the existing repo and should be considered
part of the broader adoption story:

- Project bootstrap: `csharpdb init` to create a database, starter schema, seed
  script, and optional sample app.
- Fixture workflow: `csharpdb seed` and SQL `SEED` should support repeatable
  deterministic test data so integration tests can recreate the same rows.
- Schema diff and migration preview: complement EF Core migrations with a
  CSharpDB-native diff/preview path for SQL-first users.
- Query diagnostics: surface cache hits, seed timing, generated row counts, and
  computed-column evaluation costs through CLI timing and Admin query details.
- Snippets and recipes: ship copyable examples for Admin, CLI, VS Code, MCP,
  EF Core, and common app stacks.
- Agent/tooling flow: expose seeding, cache inspection, and schema summaries
  through `CSharpDB.Mcp` so local agents can prepare realistic databases.
- Package ergonomics: make `dotnet tool install csharpdb` and package
  references the recommended entry points when distribution is ready.

## Metadata And Execution Model

Data seeding should live in the SQL execution layer and reuse normal insert
paths. It should infer value generators from column metadata:

- Integer identity and primary-key columns use normal engine identity behavior
  unless an explicit generator is supplied in a future phase.
- Text columns use column-name-aware generators for common names such as
  `Name`, `Email`, `Phone`, `Status`, `City`, and `Description`.
- Foreign-key columns choose from existing parent keys; if no parent rows exist,
  v1 should report a generation warning rather than creating parent rows
  implicitly.
- Blob columns are skipped by default unless a future explicit generator is
  provided.

Query result caching should be an engine-level cache, not only an Admin or CLI
cache. It should be invalidated by writes to referenced tables and by schema
changes. The cache should be bounded by entry count and approximate memory size,
and `CACHE 0s` should bypass storage while preserving parse compatibility.

Computed columns should be stored as schema metadata with expression SQL,
result type, and dependency column names. V1 virtual computed columns should be
available in `SELECT`, `WHERE`, `ORDER BY`, Admin browsing, schema inspection,
and EF Core model validation. Indexing computed columns should remain future
work.

Analytics helpers should lower into ordinary aggregate queries during parsing
or planning. They should not introduce a separate execution engine. The helper
syntax should remain intentionally small in v1: `COUNT(*) BY <columns> FROM
<table> [WHERE ...] [ORDER BY ...] [LIMIT ...]`.

## CLI And Tooling Deliverables

The existing `csharpdb` CLI should become the primary adoption path:

- Add `.seed <TABLE> <COUNT>` as an interactive shortcut for SQL `SEED`.
- Add `csharpdb seed <dbfile> <table> --rows <n> [--json]` for scripts and CI.
- Add `.cache [status|clear]` and non-interactive `csharpdb cache` inspection
  commands once the engine cache exists.
- Add `.init` or `csharpdb init` for starter databases and example schemas.
- Add `.examples` to list local SQL snippets for seeding, computed columns,
  analytics helpers, pipelines, and external tables.
- Make CLI output include concise timing, cache-hit, and inserted-row summaries.

Other surfaces should use the same core behavior:

- Admin should expose seeding from table context menus and show cache hit/timing
  badges in query results.
- VS Code should add snippets for `SEED`, `CACHE`, computed columns, and
  analytics helpers.
- EF Core should map virtual computed columns once the SQL engine supports
  them.
- MCP should expose tools for seed execution and schema/example discovery.

## Non-Goals

- No automatic caching of every query in v1.
- No stored computed columns in v1.
- No indexing computed columns in v1.
- No faker dependency requirement in core engine v1.
- No automatic parent-row creation for foreign-key seeding in v1.
- No broad analytics language beyond simple grouped count helpers in v1.

## Phased Implementation Plan

Phase 1: CLI and seeding foundation.

- Add SQL parser and AST support for `SEED <table> WITH <n> ROWS`.
- Implement schema-aware seed execution through normal insert paths.
- Add CLI shortcuts and non-interactive seed command.
- Add tests for deterministic generation, identity columns, nullability, and
  foreign-key warnings.

Phase 2: computed columns.

- Add computed-column schema metadata and parser support in `CREATE TABLE`.
- Evaluate virtual computed values during reads.
- Reject writes targeting computed columns.
- Update schema surfaces, Admin browse views, CLI `.schema`, EF Core validation,
  and system catalog metadata.

Phase 3: explicit query result caching.

- Add parser support for `CACHE <duration>` on read-only `SELECT` statements.
- Add bounded engine cache with table-write/schema invalidation.
- Surface cache hit/miss and expiration diagnostics through CLI and Admin.
- Add cache inspection and clear commands.

Phase 4: analytics helpers and adoption polish.

- Lower `SELECT COUNT(*) BY ... FROM ...` into ordinary grouped aggregates.
- Add snippets, examples, `csharpdb init`, and MCP discovery tools.
- Add docs that connect CLI, Admin, EF Core, MCP, and VS Code workflows.

## Future Test Plan

- Parser tests for `SEED`, `CACHE`, computed columns, and analytics helper
  syntax, including invalid syntax.
- Seeding tests for all scalar types, identity columns, nullable columns,
  foreign keys with and without parent rows, and deterministic repeatability.
- Computed-column tests for projection, filtering, ordering, schema metadata,
  write rejection, and reopen persistence.
- Cache tests for TTL expiry, write invalidation, schema invalidation,
  parameterized query keys, memory bounds, and read-only enforcement.
- Analytics helper tests that compare helper output to equivalent `GROUP BY`
  SQL.
- CLI tests for `.seed`, non-interactive `seed`, cache inspection, examples,
  and JSON output.
- Surface tests for Admin, EF Core, MCP, and VS Code snippets as those phases
  are implemented.

