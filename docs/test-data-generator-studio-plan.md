# Test Data Generator in Studio

**Status:** Implemented in the working tree for direct single-database connections; qualification and supported boundaries are documented in [the user guide](../www/docs/test-data-generator.html).

**Reviewed:** 2026-09-10, against commit `563202a2` on `version4.6.4`.

**Implementation:** Authorized after feasibility review and completed on 2026-09-11. The baseline assessment below records the original gaps; the six implementation phases now have code and focused verification. No commit or release is implied.

## Feasibility decision

Proceed. The repository already contains a reusable generation engine, realistic field providers, configurable rules, streaming row production, and Studio APIs for schema discovery and transactional writes. The requested workflow does not depend on a new storage engine or the proposed Admin plugin architecture.

This is a medium-sized product feature, with most complexity in correctness and execution rather than the screen. The existing developer utility is a useful foundation, but it does not yet provide reliable generation for arbitrary connected schemas. Relationship planning, repeatability across restarts, type handling, and append behavior must be completed before Studio exposes a Generate action.

The first release should cover selected SQL tables in one database, including existing data, with exact row counts, editable field rules, distributions, a saved seed, preview, and supported relationships. Unsupported schema shapes must produce a specific explanation before any write.

## Baseline foundations and gaps (before implementation)

| Area | Current evidence | Work needed for Studio |
| --- | --- | --- |
| Reusable engine | [SpecDataGenerator.cs](../src/CSharpDB.DataGeneration/Generators/SpecDataGenerator.cs) creates lazy row sources from a JSON specification. [DatasetSpecModels.cs](../src/CSharpDB.DataGeneration/Specs/DatasetSpecModels.cs) supports per-table count expressions and per-column rules. | Extract the reusable parts into a production library; retain the CLI as a consumer. Separate generation settings from file paths and CLI switches. |
| Realistic fields and distributions | The evaluator implements names, email, phone, selected address fields, product names, text, numbers, money, booleans, uniform choices, hot-key selection, and recent-date skew. | Add a discoverable provider catalog, validated options, weighted categories, and bounded normal numeric distributions. Expose only providers implemented by the evaluator. |
| Repeatability | `CreatePathRandom` and `CreateSeededRandom` use `System.HashCode` to derive random seeds. | Replace process-dependent hashing with a stable, versioned seed derivation. Microsoft documents that [HashCode is deterministic only within a process](https://learn.microsoft.com/en-us/dotnet/api/system.hashcode?view=netstandard-2.1); consequently the current code cannot support the README's byte-identical-across-runs promise. This conclusion is from source and the runtime contract, not a runtime reproduction. |
| Schema defaults | [SchemaInferredSpecBuilder.cs](../tests/CSharpDB.DataGen/Specs/SchemaInferredSpecBuilder.cs) infers rules from a database file. Some rules pass only literal seed parts such as `email`, `col`, or `fk`; `CreateSeededRandom` does not automatically include the row index. | Fix repeated non-null values across rows. Reconcile inferred providers such as `company.companyName`, `address.state`, and `internet.url` with the evaluator, which currently does not implement them. Validate inference against logical types before accepting a name heuristic. |
| Relationships | The built-in [relational spec](../tests/CSharpDB.DataGen/Specs/relational.dataset.json) coordinates a known order-entry schema using counts, row indexes, formulas, and shared seeds. Schema inference instead assigns guessed integer ranges to non-primary columns ending in `Id`. The spec model has no general foreign-key mapping. | Add explicit parent-key sources, dependency ordering, tuple mappings, and existing-key handling. A naming guess is a suggestion, not a relationship guarantee. |
| Rich schema metadata | [Client schema models](../src/CSharpDB.Client/Models/SchemaModels.cs) expose stable schema IDs, logical type facets, identity, rowversion, defaults, keys, foreign keys with ordered column lists, checks, and collations. | Use these models through the connected client. The old spec cannot preserve all this metadata. Its [SQL conversion helper](../tests/CSharpDB.DataGen/Specs/SqlSpecBuilder.cs) accepts only exact `INTEGER`, `REAL`, `TEXT`, and `BLOB` spellings, while inference emits richer logical types. |
| Database writes | [ICSharpDbClient](../src/CSharpDB.Client/ICSharpDbClient.cs) exposes transaction sessions. [TableImportExportService](../src/CSharpDB.Admin.ImportExport/Services/TableImportExportService.cs) demonstrates typed literal formatting, quoted identifiers, multi-row inserts, and rowversion omission. | Add a generator writer through the client. Reuse or extract suitable formatting helpers, rather than duplicating the entire import service. There is no general typed bulk-insert transaction API on this interface today. |
| CLI direct loader | [BinaryDirectLoader.cs](../tests/CSharpDB.DataGen/Output/BinaryDirectLoader.cs) creates schema, opens a database path directly, commits each batch, and can replace the target database file. | Keep this loader in the CLI. Studio needs append semantics through its existing connection and a transaction spanning the requested run. |
| Studio integration | Existing tools use a fixed tab kind, tab manager, layout switch, navigation, command palette, and scoped services. [DatabaseClientHolder.CaptureClient](../src/CSharpDB.Admin/Services/DatabaseClientHolder.cs) pins a client for a multi-call operation. | Follow these existing seams and pin the target throughout execution. There is no need to wait for plugin-host work. |

## User workflow

| Step | User experience |
| --- | --- |
| Open | Open **Test Data Generator** from database tools or the command palette. A table's **Generate Test Data...** action opens it with that table selected. |
| Select tables | Search and select writable SQL tables; enter an exact number of new rows per table. Show existing counts, dependent tables, and the total requested rows. Parents are suggested explicitly rather than silently added. |
| Configure fields | Show column name, type, constraints, suggested generator, and options. Offer names, related person fields, email, phone, address fields, company, product, number, decimal, date/time, boolean, UUID, text, sequence, constant, and value list where type-compatible. Unsupported suggestions are never selectable. |
| Configure distributions | Offer uniform, weighted category, bounded normal number, hot-key selection, recent-date skew, and null percentage where applicable. Show the relevant parameters beside the selected rule. |
| Resolve relationships | For each relationship, show the child columns, parent columns, and whether keys come from generated parents or existing parent rows. Offer explicit logical mappings for schemas without declared foreign keys. Show generation order and unresolved dependencies. |
| Preview | Preview a bounded number of rows per table, with field errors and relationship links. The seed, reference date, requested counts, and saved configuration determine the sample. Changing a setting invalidates the previous preview. |
| Generate | Show the target database, tables, new row counts, relationship choices, execution limits, and relevant defaults/triggers. Generate appends rows and reports progress by table; cancellation and failures report the transaction outcome. |
| Reuse | Save/load a versioned generation profile and download a run summary. Reopening a profile revalidates it against the current database. |

Related person fields should share a row context so, for example, an email template can use that row's first name and last name. Individual address generators provide plausible values; geographic consistency requires an explicit grouped address provider rather than independently selected city, region, and postal code.

## First-release boundaries

- **Target:** Append to existing writable SQL tables in one database. Start with Studio's direct connection; enable other single-database transports only after the same transaction, cancellation, and type checks pass. Cross-shard generation is deferred.
- **Counts:** Exact nonnegative new-row counts per table. Zero rows is valid, but cannot satisfy a required relationship to generated parents. Distribution percentages describe probabilities; they do not promise exact category counts in a small sample.
- **Relationships:** Acyclic generated-table dependencies, including composite foreign keys and references to unique keys; existing parent rows; and explicit logical relationships. Self-references may use an existing parent pool in this release. Cycles requiring newly generated keys, including generated self-references, are blocked with an explanation and deferred.
- **Types:** Cover common integer, real, decimal, text, boolean, date/time, UUID, and binary families with their declared limits. A provider/type compatibility matrix identifies any additional logical types requiring a constant or value list and blocks those that cannot be represented safely.
- **Constraints:** Preserve primary/unique keys, foreign keys, nullability, logical type bounds, lengths, precision/scale, and collation semantics. Support a documented subset of CHECK rules during preflight. Do not promise automatic satisfaction of arbitrary expressions or application business rules.
- **Database behavior:** Keep constraints, indexes, triggers, and host policies active. Rowversion fields are engine-generated. Defaults or trigger-generated values are labeled in preview; the exact-preview guarantee applies to generated input values. Block dependency plans whose parent keys would be changed by a default, trigger, or callback in a way the writer cannot resolve.
- **Limits:** Configure maximum rows, generated bytes, key-pool bytes, preview size, statement size, and transaction duration. Set initial defaults from qualification measurements. Refuse oversized runs before writes; do not silently change to partial commits.
- **Deferred:** Truncate/replace, new-database cloning, collection/document UI, cross-shard atomicity, resumable large runs with partial commits, arbitrary scripts, general constraint solving, cyclic insert/update strategies, and full export tooling. Existing CLI tracks remain available.

## Architecture

Create `src/CSharpDB.DataGeneration` as a reusable library. It owns versioned generation profiles, the provider catalog, rule validation, stable random streams, schema-independent planning, key-source abstractions, and lazy typed rows. Keep it independent of Razor, connection switching, and filesystem output. A schema snapshot supplied by an adapter should be sufficient to plan and preview data.

The existing `tests/CSharpDB.DataGen` executable consumes this library through an adapter for its current options and JSON specs. Retain its built-in relational, document, and time-series tracks. Version any behavior change caused by the seed fixes; do not promise compatibility with historical generated bytes.

Add Studio models and services under `src/CSharpDB.Admin` for the initial integration:

- `TestDataGenerationAdminService`: capture the target client, read a schema snapshot, resolve profiles, coordinate preflight, preview, execution, cancellation, and refresh.
- `TestDataGenerationWriter`: write typed rows using bounded multi-row statements inside one client transaction. Extract reusable typed SQL rendering where appropriate; validate identifiers and values using the existing rules.
- `TestDataGenerationModels`: table/column configuration, preview, validation messages, execution status, and run receipt.
- `TestDataGeneratorTab.razor` and scoped styles: render the workflow and retain tab-local editing state.

Wire the feature through [Program.cs](../src/CSharpDB.Admin/Program.cs), [TabDescriptor.cs](../src/CSharpDB.Admin/Models/TabDescriptor.cs), [TabManagerService.cs](../src/CSharpDB.Admin/Services/TabManagerService.cs), [MainLayout.razor](../src/CSharpDB.Admin/Components/Layout/MainLayout.razor), [NavMenu.razor](../src/CSharpDB.Admin/Components/Layout/NavMenu.razor), and [CommandPalette.razor](../src/CSharpDB.Admin/Components/Layout/CommandPalette.razor). Add the library to the solution and project references. Keep UI-specific behavior outside the shared generator so later modularization remains straightforward.

## Correctness contracts

### Repeatable generation

A seed alone is insufficient. A resolved run records the profile version/hash, generator algorithm version, provider/Bogus version, locale, fixed reference date, schema fingerprint, row counts, stable table/column identities, relationship settings, key allocations, and any existing-key snapshot fingerprints.

Use a specified stable hash over canonical typed inputs and a versioned random algorithm. Derive normal field streams from the seed, stable table/column/rule IDs, row ordinal, and operation. Model intentionally shared streams explicitly so the built-in relational formulas can still reproduce related values. Avoid process hash codes, global random state, current time during generation, and UI list position as seed inputs.

Preview and execution use the same resolved plan. Preview size, paging, table display order, batch size, and rendering must not alter a generated row. Existing-key selection uses a stable ordered snapshot. A rerun against changed existing data or a different identity allocation is a new resolved run; the UI must explain that difference rather than promise identical appended primary keys.

### Relationship and key planning

1. Read declared foreign keys first, preserving ordered composite column pairs. Offer name-based suggestions only where metadata is absent; the user explicitly adopts a logical mapping in the profile.
2. Resolve each parent source: generated rows or an existing key pool. A child-only selection can use existing parents without inserting more parent rows. Missing/empty required parent pools block execution; nullable references can use an explicit null policy.
3. Topologically order generated tables. Reject unsupported cycles before writing and identify the participating tables and relationships.
4. Allocate collision-free generated key domains. Use deterministic sequences or unique constructions when possible; inspect existing keys and identity state, detect overflow and insufficient value-list cardinality, and honor composite uniqueness and collations. Random retry alone is not a uniqueness guarantee.
5. Sample complete parent-key tuples. Preserve one-to-one uniqueness when the child key is unique, and enforce the capacity implications. Coordinate overlapping FK columns, including tenant IDs; reject conflicting mappings rather than generating each FK independently.
6. Resolve generated integer identity keys explicitly where supported and verify the engine's identity progression. Block unsupported key allocation cases instead of guessing IDs. Use compact key pools, deterministic key reconstruction, and configured memory limits rather than retaining every generated row.
7. Revalidate relevant schema, existing-key snapshots, and allocated ranges inside the write transaction. Concurrent changes that invalidate the plan require a fresh preview. After insertion, check configured relationship membership and counts before commit, including logical mappings without engine-enforced foreign keys.

### Preview and execution

Preview is read-only: it does not insert-and-rollback against the user's database and does not fire triggers. Generate preview rows from the full requested plan, not a smaller substitute dataset. Display referenced parent rows on demand, even when outside the parent's first preview page. Mark preview as a sample; successful preview is not proof that every row satisfies arbitrary CHECK expressions.

Before execution, validate the complete configuration, type compatibility, finite-domain capacity, supported constraints, relationship graph, target identity, and limits. For unsupported CHECK logic, require rules that can be validated by the supported preflight path or report the table as unsupported in the first release. Keep engine constraint enforcement authoritative.

Capture one database client for the operation. Start one transaction for all selected tables, revalidate the resolved inputs, and stream bounded insert batches in dependency order. Do not call per-row `InsertRowAsync` outside that transaction. Inspect both thrown exceptions and SQL result errors. Report processed rows as uncommitted until commit completes.

On a known failure or cancellation before commit, roll back the full transaction using a cleanup token independent of the cancelled generation token. Switching databases or closing the tab cancels the operation and awaits cleanup; it must never redirect later batches to another connection. A lost connection during commit is an unknown outcome until reconciled, and must not trigger an automatic retry. Nontransactional callback side effects cannot be advertised as rollback-safe; unsupported callback configurations must block generation.

After a confirmed commit, publish a run receipt with the seed, plan/version identifiers, target, requested and inserted counts, duration, warnings, and final status, and refresh affected Studio data views. Saving a reusable profile stores configuration; it should not silently embed existing database rows or credentials.

## Implementation sequence and exit criteria

### 1. Extract and harden the shared engine

- [x] Introduce the library and CLI adapter, preserving existing commands and tracks.
- [x] Replace process-dependent seed derivation; add explicit normal/shared seed contexts and a fixed reference-date setting.
- [x] Fix inferred rules that repeat values, unsupported inferred providers, and option/property mismatches.
- [x] Establish typed generator descriptors and profile/version validation. Implement missing providers required by the first-release UI.
- [x] Add weighted-category and bounded-normal rules with validated weights, bounds, finite values, and rounding behavior.

**Exit:** The CLI and library produce matching rows for the same resolved configuration. Independent processes reproduce identical canonical generated values, and inferred schemas exercise every advertised provider without unsupported-operation failures. Existing CLI dataset structures and relationships remain valid.

### 2. Build schema and relationship planning

- [x] Adapt connected-client metadata, including stable identities, all supported type facets, keys, indexes, checks, defaults, rowversion, and foreign keys.
- [x] Implement exact table counts, field-rule compatibility, existing-key pools, generated-key allocation, tuple mappings, and dependency ordering.
- [x] Validate nullability, uniqueness/collations, finite-domain capacity, overlapping FK columns, unsupported constraints, cycles, and execution limits.
- [x] Define the transport and schema support matrix, with specific preflight errors for unsupported cases.

**Exit:** Empty tables, populated tables, sparse/string parent keys, composite keys, and unequal parent/child counts produce valid resolved plans. Impossible plans fail before any write.

### 3. Implement preview and profile persistence

- [x] Add bounded preview generation and relationship navigation using the full resolved counts.
- [x] Save/load versioned profiles and show the seed, fixed reference date, validation findings, and schema changes.
- [x] Invalidate previews when generation settings, target identity, schema, or relevant existing-key state change.
- [x] Distinguish generated input values from engine-generated values and sample observations from full-plan validation.

**Exit:** Previewed generated values exactly match the corresponding execution input rows. Previewing different page sizes or tables does not affect output, and preview performs no database writes.

### 4. Implement transactional append

- [x] Capture the target client and revalidate schema/key state inside the transaction.
- [x] Add typed, quoted, bounded insert statements through the client transaction API.
- [x] Implement streamed generation, progress, cancellation, rollback, post-insert relationship/count checks, and commit outcome reporting.
- [x] Verify identity progression, rowversion omission, defaults, triggers, and callback support boundaries.
- [x] Measure representative runs and set row, byte, key-pool, statement, and transaction-duration limits.

**Exit:** Successful runs insert the exact requested direct rows and preserve configured relationships. A failure in a later table rolls back earlier inserts. The UI cannot switch the write target mid-run or claim success for an unknown commit outcome.

### 5. Integrate the Studio workspace

- [x] Add the tab, navigation entry, table action, command-palette item, scoped service registration, and database-change notifications.
- [x] Implement the table/count editor, field options, distribution controls, relationship review, preview grids, final run summary, and progress/cancel state.
- [x] Keep expensive planning and generation off synchronous UI event work; throttle progress and use bounded rendering.
- [x] Implement keyboard navigation, actionable field errors, empty/loading/error states, and cancellation on tab disposal or database change.

**Exit:** A user can configure, preview, generate, inspect the result, save a profile, and reopen it entirely in Studio without editing JSON or using the CLI.

### 6. Qualify and document the feature

- [x] Add focused tests in a new `tests/CSharpDB.DataGeneration.Tests` project and the appropriate existing Studio/client test projects.
- [x] Exercise real Studio flows against disposable fixtures and every enabled transport.
- [x] Document supported types/relationships, repeatability inputs, distribution semantics, transaction behavior, limits, and unsupported cases in Studio help and the generator README.
- [x] Update the current feature inventory only after implementation and qualification pass.

**Exit:** Focused acceptance checks and local browser qualification pass for the enabled direct connection modes. In-product help and the user guide describe the delivered support matrix, local measurements and deferred cases.

## Release acceptance cases

| Area | Required evidence |
| --- | --- |
| Repeatability | Same resolved profile in two fresh processes produces identical canonical data; a changed seed changes nonconstant fields. Preview, batch size, display ordering, and repeated generation do not perturb an unchanged field stream. Provider/version changes are detectable. |
| Realistic values | Person/email correlations work. Every suggested provider is executable and type-compatible. Inferred randomized fields vary across rows, with deliberate constants handled separately. |
| Distributions | Bounds, nullability, decimal scale, probabilities, and weight validation pass. Fixed-seed sufficiently sized samples match documented statistical tolerances; normal values stay within the configured range. |
| Relationships | Customer/order/item fixtures pass with unequal counts, child-only generation using existing parents, noncontiguous and text keys, composite keys, one-to-one mappings, nullable references, and multiple/overlapping foreign keys. |
| Invalid plans | Missing parents, empty required pools, duplicate finite domains, overflow, unsupported cycles, unsupported checks/types, and stale schema/key state produce clear errors and no writes. |
| Append correctness | Existing rows remain intact; generated PK/unique values do not collide; identity progression and collation-aware uniqueness are correct. Exact requested insert counts are distinct from trigger-created rows. |
| Failure behavior | Late constraint failure, cancellation, tab close, database switch, and supported transport failures exercise full rollback or an explicitly unresolved commit status. There is no automatic retry after an uncertain commit. |
| Preview | Preview performs no writes or trigger execution, references the complete parent domain, and matches generated execution inputs. Engine-generated values are visibly identified. |
| Responsiveness | Measured representative runs stay within configured memory/statement limits, keep Studio usable, and respond to cancellation. Record hardware, transport, row width, counts, and index/constraint shape with timings. |
| Regression | The existing CLI tracks still work through the extracted engine, supported generated values round-trip through the writer, and Studio navigation, connection switching, and other data tools remain functional. |

The minimum complete delivery is all six phases. A preview-only screen can be reviewed during development, but it is not completion of this feature.

## Delivered implementation decisions

- Shared evaluator/spec models moved to `src/CSharpDB.DataGeneration`; CLI commands remain consumers. Stable random streams, fixed temporal anchors, missing providers, typed conversions and schema inference were hardened. Legacy generated bytes intentionally change.
- Profiles, catalog, plan, validation, typed SQL helpers and receipts live in the shared library. Studio connection capture, snapshot reads, bounded writing and transaction coordination live together in `TestDataGenerationAdminService`; a separate writer class was unnecessary for this first adapter.
- The tab provides table selection, counts, fields, distributions, declared and logical relationships, paging, referenced parent keys, profile downloads/uploads, receipts and cancel behavior. Relationship preview discloses key tuples rather than loading complete existing parent records.
- Direct and direct hybrid incremental-durable modes are enabled. Remote/routed/sharded targets are blocked. All INSERT-trigger tables are conservatively blocked, not just triggers that might alter keys. Supported deterministic defaults are materialized in preview; rowversion remains engine-generated.
- The user guide records configuration defaults and a measured 10,000-row hybrid run. Size limits estimate generated values and key structures; they are not a process-memory ceiling. Larger configurable runs still require workload-specific qualification.
- Coverage includes CLI output across independent processes; distributions and logical types; composite, overlapping and one-to-one relationships; append preservation; stale profiles; late SQL failures; cancellation; database switching; tab disposal; and an injected lost commit acknowledgement. The broader repository suite was not run as a release qualification.

See [the feature guide](../www/docs/test-data-generator.html), [generator integration tests](../tests/CSharpDB.DataGeneration.Tests/GeneratorIntegrationTests.cs), and [Studio lifecycle tests](../tests/CSharpDB.Admin.Forms.Tests/Components/TestDataGeneratorTabTests.cs) for the delivered behavior and evidence.