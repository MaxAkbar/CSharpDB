# CSharpDB migration compatibility services

This project contains read-only services that can be hosted by the CLI, an
Admin workflow, or another .NET application. It does not connect to a source
database, create a target database, apply rewrites, or execute queries.

## Data type mapping report

`DataTypeMappingReportService` consumes a validated `MigrationCatalog` and the
same `IDataTypeMappingProvider` contract used by `MigrationPlanner`. Its report
contains one deterministic entry for every catalog object with a native type,
including:

- source object identity, name, kind, native type, and logical type;
- requested and selected CSharpDB types;
- exact, lossless-reencoded, lossy, or unsupported classification;
- profile coverage and full-stream-validation requirements;
- versioned conversion parameters; and
- the stable mapping diagnostic, when one exists.

Use `CompatibilityReportFormatter.ToJson` or `ToText` for deterministic output.
The service supports the existing preserve, queryable, and per-object custom
mapping profiles. It does not change the catalog or create a migration plan.

The CLI custom-map file is a strict JSON object keyed by exact catalog
`objectId`. Each value is one case-insensitive persistent CSharpDB type:
`integer`, `real`, `text`, or `blob`.

```json
{
  "<exact-column-object-id-from-catalog>": "text",
  "<another-exact-column-object-id>": "real"
}
```

Unknown object IDs, duplicate properties, invalid types, comments, and trailing
commas fail closed. A requested mapping can still be classified as lossy or
unsupported; the file requests a report decision and does not approve it.

## Query compatibility report

`CSharpDB.Migration.Compatibility.QueryCompatibilityAnalyzer` is the base,
provider-neutral analyzer. It accepts a bounded query pack and analyzes
CSharpDB SQL with `CSharpDB.Sql`; T-SQL, MySQL, SQLite, and Access return
`Unknown` in this base implementation.

`CSharpDB.Migration.SqlServer.SqlServerQueryCompatibilityAnalyzer` is the
separately compiled T-SQL analyzer. It runs in the optional isolated SQL Server
worker, uses ScriptDom, and supports SQL Server compatibility levels 150, 160,
and 170.

The base compatibility and CLI dependency closures do not contain ScriptDom.
When the CLI receives `--dialect tsql`, it sends the bounded request to the
fixed SQL Server worker and sanitizes the returned report before publication.

MySQL, SQLite, and Access fail closed as `Unknown` until dialect-specific
parsers are added. One mechanical T-SQL rewrite is currently qualified: a
single root `TOP` with a non-negative integer literal can become CSharpDB
`LIMIT`. The generated candidate is parsed again and is never applied.

A `Conditional` result establishes parse-level evidence only. Schema and typed
parameter binding, scratch execution, and semantic equivalence are not claimed.
The analyzer emits explicit diagnostics for that missing evidence, as well as
state-changing statements, temporary objects, session-dependent behavior,
nondeterministic functions, and unordered `TOP`/`LIMIT`.
