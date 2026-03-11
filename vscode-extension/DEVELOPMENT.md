# CSharpDB VS Code Extension Development

This document is for maintainers working on the extension in this repo.

The extension is implemented in [`vscode-extension/`](./), runs inside the VS Code extension host, and talks to CSharpDB through the NativeAOT library in [`src/CSharpDB.Native/`](../src/CSharpDB.Native/). There is no API server in the local extension path anymore.

## Prerequisites

- VS Code
- Node.js 18 or later
- npm
- .NET 10 SDK
- NativeAOT toolchain for your platform
  - Windows: Visual Studio or Build Tools with C++ workload
  - See [`src/CSharpDB.Native/README.md`](../src/CSharpDB.Native/README.md) for platform-specific details

## Repo Layout

Important folders:

- [`vscode-extension/src/extension.ts`](./src/extension.ts): activation, commands, status bar, database selection/creation, connect flow
- [`vscode-extension/src/api/client.ts`](./src/api/client.ts): high-level extension client used by panels/providers
- [`vscode-extension/src/api/nativeDatabase.ts`](./src/api/nativeDatabase.ts): direct database open/execute and JSON diagnostics bridge
- [`vscode-extension/src/native/nativeBindings.ts`](./src/native/nativeBindings.ts): `koffi` FFI bindings and native library discovery
- [`vscode-extension/src/providers/`](./src/providers): schema explorer, SQL completion, SQL hover
- [`vscode-extension/src/panels/`](./src/panels): webview panels and shared panel lifecycle helpers
- [`vscode-extension/src/panels/panelRegistry.ts`](./src/panels/panelRegistry.ts): tracks CSharpDB webviews for shared-tab behavior and disconnect cleanup
- [`vscode-extension/media/`](./media): panel-side JavaScript, CSS, and icons
- [`src/CSharpDB.Native/NativeExports.cs`](../src/CSharpDB.Native/NativeExports.cs): native exports consumed by the extension
- [`src/CSharpDB.Native/csharpdb.h`](../src/CSharpDB.Native/csharpdb.h): native header for exported functions
- [`src/CSharpDB.Native/NativeJsonContext.cs`](../src/CSharpDB.Native/NativeJsonContext.cs): source-generated JSON metadata for diagnostics/maintenance exports

## First-Time Setup

From the repo root:

```powershell
dotnet publish src\CSharpDB.Native\CSharpDB.Native.csproj -c Release -r win-x64
cd vscode-extension
npm install
npm run compile
```

That is enough for local development on Windows x64. The extension auto-discovers published NativeAOT libraries from common repo build locations under `src/CSharpDB.Native/bin/.../publish/`.

If you published the native library somewhere else, set the VS Code setting:

```json
"csharpdb.nativeLibraryPath": "C:\\path\\to\\CSharpDB.Native.dll"
```

You can also point that setting at a folder containing the library.

## Running The Extension

Two supported debug entry points exist:

1. Open the repo root and press `F5`
   - Uses [`.vscode/launch.json`](../.vscode/launch.json)
   - Launch config name: `Run CSharpDB Extension`
2. Open [`vscode-extension/`](./) directly and press `F5`
   - Uses [`vscode-extension/.vscode/launch.json`](./.vscode/launch.json)

Both paths start an Extension Development Host.

Recommended workflow:

1. Open the repo root in VS Code.
2. Press `F5`.
3. In the Extension Development Host window, open a workspace folder.
4. Open an existing `.db` file through `CSharpDB: Connect`, or create one through `CSharpDB: Create Database`.
5. Use the Schema Explorer title actions for connect/disconnect, refresh, storage diagnostics, and new procedure creation.
6. Create a `.csql` file and run queries with `Ctrl+Enter`.

## Debugging

### TypeScript changes

Compile once:

```powershell
cd vscode-extension
npm run compile
```

Or run watch mode in a terminal:

```powershell
cd vscode-extension
npm run watch
```

Then reload the Extension Development Host window after changes.

### Native changes

If you change anything under [`src/CSharpDB.Native/`](../src/CSharpDB.Native/) that affects exported functions or JSON payloads, republish the native project:

```powershell
dotnet publish src\CSharpDB.Native\CSharpDB.Native.csproj -c Release -r win-x64
```

For other targets:

```powershell
dotnet publish src\CSharpDB.Native\CSharpDB.Native.csproj -c Release -r win-arm64
```

After rebuilding the native library:

1. Stop the Extension Development Host.
2. Start `F5` again.

Restarting is important because the extension host keeps the native library loaded.

### Useful output locations

- `CSharpDB` output channel in VS Code
- Debug Console in the Extension Development Host
- Webpack compile output from the prelaunch task

## Current UX Behavior

- The connect flow supports three paths: choose a workspace `.db`, browse to an existing file, or create a new database file.
- The disconnected Schema Explorer shows both `Connect to CSharpDB` and `Create New Database`.
- The Schema Explorer title bar is the main toolbar for connect/disconnect, refresh, storage diagnostics, and new procedure.
- Tables and views open in the data browser. Views expose both data browsing and schema editing in the same panel.
- Indexes and triggers route to the owning table designer rather than opening separate editors.
- Procedures open in the dedicated procedure designer.
- By default, workbench-style CSharpDB panels reuse a single shared tab. This applies to the data browser, table designer, procedure designer, and diagnostics panel.
- Query results reuse a single auxiliary results panel.
- Disconnect closes all CSharpDB webviews so stale panels do not remain open against a closed database.

## Common Change Areas

### Commands, activation, connect flow

Edit [`vscode-extension/src/extension.ts`](./src/extension.ts).

Examples:

- add a command
- change auto-connect behavior
- change how `.db` files are selected or created
- update status bar behavior
- update disconnect cleanup behavior

### Schema explorer tree and menus

Edit:

- [`vscode-extension/src/providers/schemaTreeProvider.ts`](./src/providers/schemaTreeProvider.ts)
- [`vscode-extension/src/extension.ts`](./src/extension.ts)
- [`vscode-extension/package.json`](./package.json)

This area owns:

- disconnected-state actions like `Connect to CSharpDB` and `Create New Database`
- object click behavior for tables, views, indexes, triggers, and procedures
- explorer title bar commands
- context menu contributions and command visibility

### Database access and schema/data operations

Edit [`vscode-extension/src/api/client.ts`](./src/api/client.ts).

This file owns:

- table, view, trigger, index, and procedure operations
- query execution
- diagnostics and maintenance calls
- catalog initialization for `__procedures` and `__saved_queries`

Shared SQL helpers are in [`vscode-extension/src/api/sqlHelpers.ts`](./src/api/sqlHelpers.ts), and procedure metadata helpers are in [`vscode-extension/src/api/procedureHelpers.ts`](./src/api/procedureHelpers.ts).

### Native interop

Edit:

- [`vscode-extension/src/native/nativeBindings.ts`](./src/native/nativeBindings.ts)
- [`vscode-extension/src/api/nativeDatabase.ts`](./src/api/nativeDatabase.ts)
- [`src/CSharpDB.Native/NativeExports.cs`](../src/CSharpDB.Native/NativeExports.cs)
- [`src/CSharpDB.Native/csharpdb.h`](../src/CSharpDB.Native/csharpdb.h)

Rules:

- If you add a native export, update both the C# export and the header.
- If the export returns JSON, add/update source-generated metadata in [`src/CSharpDB.Native/NativeJsonContext.cs`](../src/CSharpDB.Native/NativeJsonContext.cs).
- Republish the native project before testing the extension.

### Webview panels

Each panel has two parts:

- extension-host side TypeScript in [`vscode-extension/src/panels/`](./src/panels)
- webview-side assets in [`vscode-extension/media/scripts/`](./media/scripts) and [`vscode-extension/media/styles/`](./media/styles)
- shared workbench-tab coordination in [`vscode-extension/src/panels/panelRegistry.ts`](./src/panels/panelRegistry.ts)

Common pairs:

- query results:
  - [`vscode-extension/src/panels/queryResultsPanel.ts`](./src/panels/queryResultsPanel.ts)
  - [`vscode-extension/media/scripts/grid.js`](./media/scripts/grid.js)
- data browser:
  - [`vscode-extension/src/panels/dataBrowserPanel.ts`](./src/panels/dataBrowserPanel.ts)
  - [`vscode-extension/media/scripts/dataBrowser.js`](./media/scripts/dataBrowser.js)
- table designer:
  - [`vscode-extension/src/panels/tableDesignerPanel.ts`](./src/panels/tableDesignerPanel.ts)
  - [`vscode-extension/media/scripts/tableDesigner.js`](./media/scripts/tableDesigner.js)
- procedure designer:
  - [`vscode-extension/src/panels/procedureDesignerPanel.ts`](./src/panels/procedureDesignerPanel.ts)
  - [`vscode-extension/media/scripts/procedureDesigner.js`](./media/scripts/procedureDesigner.js)
- diagnostics:
  - [`vscode-extension/src/panels/storageDiagnosticsPanel.ts`](./src/panels/storageDiagnosticsPanel.ts)
  - [`vscode-extension/media/scripts/storageDiagnostics.js`](./media/scripts/storageDiagnostics.js)

Notes:

- If panel reuse behavior changes, update both the individual panel class and [`vscode-extension/src/panels/panelRegistry.ts`](./src/panels/panelRegistry.ts).
- If disconnect should close or preserve a CSharpDB surface, update the connection-state handling in [`vscode-extension/src/extension.ts`](./src/extension.ts) and the panel registration logic together.

### Language features

Edit:

- [`vscode-extension/src/providers/sqlCompletionProvider.ts`](./src/providers/sqlCompletionProvider.ts)
- [`vscode-extension/src/providers/sqlHoverProvider.ts`](./src/providers/sqlHoverProvider.ts)
- [`vscode-extension/syntaxes/csharpdb-sql.tmLanguage.json`](./syntaxes/csharpdb-sql.tmLanguage.json)
- [`vscode-extension/language-configuration.json`](./language-configuration.json)

## Packaging

Build the production bundle:

```powershell
cd vscode-extension
npm run package
```

Create a VSIX:

```powershell
npx @vscode/vsce package
```

Notes:

- `webpack` externalizes `koffi`, so `node_modules` must be present when testing locally.
- The extension does not bundle a platform-specific NativeAOT binary yet.
- NativeAOT is platform-specific. For distributable builds, publish `src/CSharpDB.Native` per RID and package a matching VSIX per VS Code target.
- Typical targets are `win32-x64`/`win-x64`, `win32-arm64`/`win-arm64`, `darwin-x64`/`osx-x64`, `darwin-arm64`/`osx-arm64`, `linux-x64`/`linux-x64`, and `linux-arm64`/`linux-arm64`.
- In remote scenarios, the native binary must match the remote extension host, not necessarily the developer's local desktop OS.
- If you are not shipping per-target VSIX artifacts yet, provide a clear `csharpdb.nativeLibraryPath` setup path.

## Recommended Smoke Test

In the Extension Development Host:

1. Open a workspace folder.
2. If you do not already have a `.db` file, run `CSharpDB: Create Database` and save a new `smoke.db`.
3. Run `CSharpDB: Connect` if the extension does not auto-connect.
4. Create `smoke.csql`.
5. Execute:

```sql
CREATE TABLE demo (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO demo VALUES (1, 'Alice');
SELECT * FROM demo;
```

Verify:

- status bar shows the active database
- connect flow offers `Create New Database...`
- `demo` appears in Schema Explorer
- Schema Explorer title actions render for refresh, diagnostics, and new procedure
- query results panel renders rows
- `Browse Table` opens and loads data
- opening table data, table designer, procedure designer, and diagnostics reuses the shared CSharpDB workbench tab by default
- `New Procedure` opens the procedure designer in the shared CSharpDB workbench tab
- `Open Storage Diagnostics` renders without using the API server
- `Disconnect` closes CSharpDB webviews

## Troubleshooting

### `F5` asks for a debugger

Open the repo root or [`vscode-extension/`](./) and use the existing `Run CSharpDB Extension` launch config. The repo already contains the needed `.vscode` files.

### Native library not found

Do one of:

- run `dotnet publish src\CSharpDB.Native\CSharpDB.Native.csproj -c Release -r win-x64`
- set `csharpdb.nativeLibraryPath`
- point `csharpdb.nativeLibraryPath` at a published `win-x64` / `linux-x64` / `osx-*` output folder

### Native changes do not appear

The extension host keeps the native library loaded. Republish the native project and restart the Extension Development Host completely.

### `koffi` load errors

Reinstall extension dependencies:

```powershell
cd vscode-extension
npm install
```

### Query works in engine tests but not in the extension

Check whether the extension path is using:

- high-level SQL logic in [`vscode-extension/src/api/client.ts`](./src/api/client.ts)
- direct FFI behavior in [`vscode-extension/src/api/nativeDatabase.ts`](./src/api/nativeDatabase.ts)

The extension is not using the REST API path for local development anymore.
