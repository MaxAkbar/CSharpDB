# What's New

## v2.9.0

### Richer CLI Console Experience

- Migrated `CSharpDB.Cli` from the handwritten ANSI output layer to `Spectre.Console` so the shell, help text, query tables, schema panels, and status messages render with a more consistent console UI.
- Added a shared `CliConsole` helper with the new ASCII startup banner, left-anchored branding, richer prompt/status rendering, and reusable table/panel helpers for CLI commands.
- Upgraded inspector and maintenance summaries to structured console tables so database, page, WAL, index, vacuum, and reindex output is easier to scan in a terminal session.

### Interactive Dot-Command Menu

- Added an interactive dot-command menu at the `csdb>` prompt. Pressing `.` as the first character at a fresh prompt now opens a keyboard-driven menu of supported dot commands.
- Added arrow-key navigation with `Up` and `Down`, `Enter` to run the selected command, and `Esc` to cancel and return to the prompt.
- Preserved scripted and redirected-input behavior by falling back to normal dot-command handling when the shell is not running in an interactive console.

### Reliability and Test Coverage

- Hardened the dot-command menu renderer so it clamps itself to the visible console buffer and scrolls the visible command window instead of crashing near the bottom of the terminal.
- Added CLI coverage for redirected `.` handling and the menu layout edge cases around small or nearly full console buffers.
- Validated the current workspace with `dotnet build src/CSharpDB.Cli/CSharpDB.Cli.csproj`, `dotnet test tests/CSharpDB.Cli.Tests/CSharpDB.Cli.Tests.csproj`, and `dotnet test CSharpDB.slnx`.
