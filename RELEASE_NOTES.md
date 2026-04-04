# What's New

## v2.8.0

### Admin Forms Designer and Entry Runtime

- Added an admin-only forms module with a visual form designer, runtime form entry, formula evaluation, validation inference, and JSON-backed form definitions.
- Integrated Forms into `CSharpDB.Admin` with sidebar navigation, designer/runtime tabs, table actions, and shared admin theming.
- Added database-backed form persistence through the internal `__forms` metadata table, including schema-signature tracking and active-database reload behavior.
- Added runtime usability improvements including paged record navigation, go-to-record by primary key, column-based search, manual child-table mapping, and checkbox/radio coercion for text and numeric boolean representations.
- Added a dedicated `CSharpDB.Admin.Forms.Tests` suite covering repository behavior, schema adaptation, generator/validation rules, runtime record services, serialization, and admin tab wiring.

### Admin Reports Designer, Preview, and Print

- Added an admin-only reports module with a banded report designer, preview/runtime rendering, grouping, sorting, totals, and report-only browser print/PDF output.
- Integrated Reports into `CSharpDB.Admin` with sidebar/object-explorer navigation, report actions, and shared admin-shell wiring.
- Added database-backed report persistence through `__reports` plus chunked `__report_definition_chunks` storage so larger layouts no longer depend on oversized single-row definitions.
- Added report usability features including schema-signature drift warnings, auto-fit column layout, and support for table, view, and saved-query report sources.
- Added a dedicated `CSharpDB.Admin.Reports.Tests` suite covering repository behavior, chunked-definition persistence, schema/source resolution, preview pagination/grouping, and layout helpers.

### Batch-First SQL Row Transport

- The SQL executor now uses an internal batch-first row transport foundation across batch-capable scans, joins, projections, filters, limits, and generic aggregate paths.
- Shared batch predicate and projection kernels reduce per-row overhead, preserve direct batch storage deeper into execution plans, and lay the groundwork for future vectorized execution work.
- Batch evaluation and expression-compiler coverage was expanded and hardened for compact-row shapes, numeric-expression predicates, and distinct aggregate fast paths.

### Docs, Site, and Admin Updates

- Added new design notes for compiled live queries and materialized join read models.
- Added blog and news pages to the website and refreshed roadmap content to reflect the current shipped surface.
- Refactored Admin navigation and title-bar plumbing, including a shared database client holder, improved modal input support, and the integrated Forms/Reports shell wiring.
