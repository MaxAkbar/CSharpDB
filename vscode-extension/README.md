# CSharpDB VS Code Extension

This folder contains the first working implementation of the CSharpDB VS Code extension described in [`docs/vscode-extension/README.md`](../docs/vscode-extension/README.md).

## Features

- NativeAOT-backed local connection using the embedded C API
- Auto-connect support for workspace `.db` files
- Schema explorer for tables, columns, views, indexes, triggers, and procedures
- `.csql` language registration with syntax highlighting, completion, and hover help
- Query results panel
- Data browser panel with CRUD for tables and read-only browsing for views
- Table designer panel for create/alter flows
- Storage diagnostics panel with inspect, WAL, index checks, reindex, and vacuum

## Development

```bash
dotnet publish src/CSharpDB.Native/CSharpDB.Native.csproj -c Release -r win-x64
cd vscode-extension
npm install
npm run compile
```

The extension auto-discovers published NativeAOT libraries in common repo build locations. If you published it elsewhere, set `csharpdb.nativeLibraryPath` in VS Code settings to the `.dll`/`.so`/`.dylib` file or its containing folder.

Press `F5` from this folder or the repo root to open an Extension Development Host.

For the full maintainer workflow, see [DEVELOPMENT.md](./DEVELOPMENT.md).
