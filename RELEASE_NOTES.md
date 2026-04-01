# What's New

## v2.8.0

### Batch-First SQL Row Transport

- The SQL executor now uses an internal batch-first row transport foundation across batch-capable scans, joins, projections, filters, limits, and generic aggregate paths.
- Shared batch predicate and projection kernels reduce per-row overhead, preserve direct batch storage deeper into execution plans, and lay the groundwork for future vectorized execution work.
- Batch evaluation and expression-compiler coverage was expanded and hardened for compact-row shapes, numeric-expression predicates, and distinct aggregate fast paths.

### Docs, Site, and Admin Updates

- Added new design notes for compiled live queries and materialized join read models.
- Added blog and news pages to the website and refreshed roadmap content to reflect the current shipped surface.
- Refactored Admin navigation and title-bar plumbing, including a shared database client holder and improved modal input support.

