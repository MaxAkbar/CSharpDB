# What's New

## v2.9.0

### Planner Statistics and Durable Write Batching

- Added the next phase of statistics-guided planning with richer `ANALYZE` data, including histogram, heavy-hitter, and composite-prefix statistics used by the optimizer for better cardinality and join-cost estimates.
- Extended the planner to use bounded dynamic-programming join reordering on qualifying inner-join chains instead of relying only on simpler greedy choices.
- Added durable-write batching infrastructure and shared storage copy batching so WAL- and maintenance-adjacent copy paths can batch work more efficiently without changing default durability semantics.
- Added explicit `sys.table_stats.row_count_is_exact` semantics so planner costing can distinguish estimated row counts from exact counts and keep `COUNT(*)` fast paths honest.

### Planner and Runtime Stabilization

- Followed the main optimizer/storage work with stabilization updates across planner, checkpoint, and maintenance behavior after merge-time regressions surfaced in tests.
- Tightened planner behavior around batch evaluation, join planning, and checkpoint-related validation to keep the new stats-guided and batching paths correct under the existing test suite.
- Added and refreshed regression coverage across planner statistics, WAL behavior, integration scenarios, page-read buffering, and hybrid/local database paths.

### CLI Console Experience

- Migrated `CSharpDB.Cli` from the handwritten ANSI output layer to `Spectre.Console` so the shell, help text, query tables, schema panels, and status messages render with a more consistent console UI.
- Added a shared `CliConsole` helper with the new ASCII startup banner, left-anchored branding, richer prompt/status rendering, and reusable table/panel helpers for CLI commands.
- Added an interactive dot-command menu at the `csdb>` prompt. Pressing `.` as the first character at a fresh prompt now opens a keyboard-driven menu of supported dot commands with `Up`/`Down`, `Enter`, and `Esc`.
- Hardened the dot-command menu renderer so it clamps itself to the console buffer and scrolls the visible command window instead of crashing near the bottom of the terminal.

### Documentation Refresh

- Added contributor-facing documentation for configuration, the query execution pipeline, SQL surface behavior, and combined query/durable-write performance guidance.
- Refreshed the roadmap to reflect the current `v2.9.0` status of the optimizer, batch execution, and durable-write work.
- Validated the branch with `dotnet build CSharpDB.slnx`, `dotnet build src/CSharpDB.Cli/CSharpDB.Cli.csproj`, `dotnet test tests/CSharpDB.Cli.Tests/CSharpDB.Cli.Tests.csproj`, and `dotnet test CSharpDB.slnx`.
