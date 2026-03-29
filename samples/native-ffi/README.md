# Native FFI Tutorials

These tutorials show how to use CSharpDB from non-.NET languages via the NativeAOT shared library (`CSharpDB.Native`). The native library exposes a C-compatible API that any language with FFI support can call.

## Prerequisites

1. **Build the native library** (requires .NET 10 SDK + C++ toolchain):

   ```bash
   # Windows
   dotnet publish src/CSharpDB.Native/CSharpDB.Native.csproj -c Release -r win-x64

   # Linux
   dotnet publish src/CSharpDB.Native/CSharpDB.Native.csproj -c Release -r linux-x64

   # macOS
   dotnet publish src/CSharpDB.Native/CSharpDB.Native.csproj -c Release -r osx-arm64
   ```

2. The published library will be at:
   ```
   src/CSharpDB.Native/bin/Release/net10.0/<rid>/publish/CSharpDB.Native.{dll,so,dylib}
   ```

## Tutorials

| Language | Directory | Dependencies |
|----------|-----------|-------------|
| [Python](python/) | `python/` | None (uses built-in `ctypes`) |
| [JavaScript / Node.js](javascript/) | `javascript/` | `koffi` (npm package) |

## Architecture

```
Your App (Python / Node.js / Go / Rust / ...)
    |
    | FFI call (ctypes / koffi / cgo / extern "C")
    v
CSharpDB.Native.dll (.so / .dylib)   <-- NativeAOT compiled, no .NET runtime
    |
    | Direct function calls
    v
CSharpDB Engine (embedded in the native binary)
    |
    v
Database file on disk (*.db)
```

Each tutorial includes:
- A **reusable wrapper module** that loads the native library and provides an idiomatic API
- **Runnable examples** covering CRUD operations, transactions, and queries
- A **README** with setup and usage instructions
