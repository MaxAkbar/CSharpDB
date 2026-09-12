# What's New

## CSharpDB 4.6.4

CSharpDB 4.6.4 adds SQL Search and Dependency Explorer, a Database Documenter
with persistent descriptions, and repeatable test-data generation in Studio.
It also improves Data Modeler connector stability, SQL parameter binding,
and product documentation. These notes cover changes since v4.6.3 only.

### SQL Search and Dependency Explorer

- Search saved object names and definition text with object-type, case-sensitive,
  and whole-word filters, highlighted matches, and source navigation.
- Inspect tables, columns, constraints, indexes, views, triggers, procedures,
  saved queries, C# modules, forms, reports, pipeline revisions, saved data
  models, and registered external archive metadata.
- Follow **Uses**, **Used by**, and a dependency graph. Distinguish database
  references, diagram membership, saved proposed changes, and archive
  relationships instead of treating them as equivalent constraints.
- Assess column rename, drop, type, and nullability changes. Review confirmed
  references, downstream effects, engine restrictions, and references requiring
  manual review from table actions or the Data Modeler inspector.
- Share dependency findings with Data Modeler's **Known dependencies** view.
  Open related saved diagrams while preserving existing pending edits.
- Inspect definitions through direct, HTTP, and gRPC connections, scoped to the
  selected database and route. Background work supports cancellation and refresh;
  incomplete analysis and stale results are identified explicitly.

### Database Documenter

- Build a searchable data dictionary for tables, columns, keys, checks, defaults,
  indexes, foreign keys, views, triggers, and procedures. Filter by object type
  or missing descriptions, and follow declared relationship links.
- Save plain-text descriptions in the database for reuse across sessions.
  Stable table, column, and constraint identities preserve descriptions through
  supported renames. Changed definitions can require description review, and
  unmatched descriptions remain available as diagnostics.
- Check both description revisions and object definitions when saving so a
  stale edit cannot silently overwrite another user's changes. Failed saves
  retain the draft.
- Export the complete captured dictionary as one self-contained, searchable
  HTML file or linked Markdown, including descriptions, definitions, column
  and parameter details, relationships, and coverage diagnostics.
- Allow consecutive user-requested exports in the Windows desktop host.

### Repeatable Test Data Generator

- Configure row counts and field generators, preview data, and append to selected
  SQL tables from Studio. Download and reload versioned JSON profiles to reuse
  the same configuration.
- Generate person, contact, address, company, product, text, numeric, temporal,
  UUID, binary, sequence, constant, and value-list data with supported uniform,
  weighted, bounded-normal, recent-date, and hot-key distributions.
- Plan parent-before-child insertion using generated or existing parent keys,
  including supported composite, overlapping, one-to-one, and nullable
  relationships. Inspect referenced key tuples during preview.
- Validate the complete requested run against supported types, nullability,
  collations, primary and unique keys, relationships, checks, defaults, identity
  state, and configured limits before inserting.
- Recheck schema and existing key state when generation starts. Insert all
  selected tables in one transaction, preserve existing rows, verify counts
  and keys, and roll back on cancellation or failures before commit. An
  unconfirmed commit is reported as an unknown outcome without automatic retry.
- Share the generation engine with the developer DataGen CLI, including stable
  random streams, fixed reference dates, typed values, and improved schema
  inference. Row batches and statements have configurable size limits.

### Fixes and Documentation

- Bind SQL parameters using tokenizer spans in ADO.NET commands and Admin Forms
  SQL actions, leaving comments, string literals, and quoted identifiers intact.
- Keep unrelated Data Modeler connectors stable when another table moves;
  reroute when an actual obstacle affects a connector.
- Read archive definition metadata without scanning stored rows or index pages.
  Full archive integrity validation remains a separate operation.
- Add guides for SQL Search and dependencies, Test Data Generator, and form
  layout. Correct website examples and feature descriptions,
  improve shared navigation accessibility, and validate local links and anchors.

### Compatibility and Scope

- These changes do not introduce a database file-format revision. The first
  description save creates the internal `__documentation_annotations` table;
  opening, searching, and exporting a dictionary do not create it.
- SQL Search and dependency assessment inspect saved definitions without
  executing them. Impact findings are advisory: unresolved or dynamic references
  can require review, and engine enforcement remains authoritative.
- Database Documenter supports direct, HTTP, and gRPC connections. Older servers
  may provide a partial dictionary; description editing requires the new
  documentation metadata capability. Exports contain metadata, not user rows.
- Studio test-data generation supports one directly connected database,
  including direct hybrid incremental-durable mode. Remote, routed, and sharded
  targets, collections, and replace/truncate operations are outside its scope.
  Generated relationship cycles, INSERT-trigger tables, rowversion keys, and
  unsupported types or preflight expressions are rejected.
- Repeatability depends on the profile, seed, reference date, generator/provider
  version, schema identities, and existing key state. The hardened DataGen
  evaluator intentionally changes legacy generated output; old seeds alone do
  not reproduce bytes from earlier versions.
- `CSharpDB.DataGeneration` is currently consumed through project references by
  Studio and the developer CLI; it is not a separately published NuGet package
  or part of the `CSharpDB` umbrella package.
