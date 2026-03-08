# Contributing to CSharpDB

Thanks for your interest in contributing.

## Ways to Contribute

- Report bugs
- Propose features and improvements
- Improve documentation
- Submit code changes and tests

## Before You Start

1. Search existing issues and pull requests to avoid duplicates.
2. If the change is large, open an issue first to align on scope.

## Development Setup

Prerequisite: [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)

```bash
dotnet --version
dotnet restore CSharpDB.slnx
dotnet build CSharpDB.slnx
```

> **Note:** The default `dotnet build` excludes `CSharpDB.Native` because it requires a native C++ toolchain (see below). Everything else builds with just the .NET SDK.

## Running Tests

Build validation:

```bash
dotnet build CSharpDB.slnx
```

Run test executables:

```bash
dotnet run --project tests/CSharpDB.Tests/CSharpDB.Tests.csproj --
dotnet run --project tests/CSharpDB.Data.Tests/CSharpDB.Data.Tests.csproj --
dotnet run --project tests/CSharpDB.Cli.Tests/CSharpDB.Cli.Tests.csproj --
```

## Working with CSharpDB.Native (NativeAOT)

The `CSharpDB.Native` project produces a standalone C-compatible shared library via NativeAOT. It is **excluded from the default solution build** because it requires a platform-specific C/C++ toolchain.

### Prerequisites

In addition to the .NET 10 SDK, you need a native linker:

| Platform | Requirement |
|----------|-------------|
| **Windows** | Visual Studio with **"Desktop development with C++"** workload, or VS Build Tools with C++ component |
| **Linux** | `clang` and `zlib1g-dev` (Ubuntu/Debian) or `clang` and `zlib-devel` (Fedora/RHEL) |
| **macOS** | Xcode command-line tools (`xcode-select --install`) |

### Building the native library

```bash
# Windows
dotnet publish src/CSharpDB.Native -c Release -r win-x64

# Linux
dotnet publish src/CSharpDB.Native -c Release -r linux-x64

# macOS (Apple Silicon)
dotnet publish src/CSharpDB.Native -c Release -r osx-arm64
```

Output goes to `src/CSharpDB.Native/bin/Release/net10.0/<rid>/publish/`:
- Windows: `CSharpDB.Native.dll`
- Linux: `CSharpDB.Native.so`
- macOS: `CSharpDB.Native.dylib`

The library is fully self-contained — no .NET runtime dependency at the call site.

### Including in solution build

To include the native project in a full solution build:

```bash
dotnet build CSharpDB.slnx -p:BuildNative=true
```

### Verifying exports

After publishing, verify the library exports the expected C symbols:

```bash
# Linux
nm -D src/CSharpDB.Native/bin/Release/net10.0/linux-x64/publish/CSharpDB.Native.so | grep csharpdb_

# macOS
nm -gU src/CSharpDB.Native/bin/Release/net10.0/osx-arm64/publish/CSharpDB.Native.dylib | grep csharpdb_

# Windows (Developer Command Prompt or PowerShell)
dumpbin /exports src\CSharpDB.Native\bin\Release\net10.0\win-x64\publish\CSharpDB.Native.dll | Select-String "csharpdb_"
```

You should see 20 exported functions prefixed with `csharpdb_`.

### Node.js client

The `clients/node/` directory contains a TypeScript package that wraps the native library via [koffi](https://koffi.dev/). To work with it:

```bash
cd clients/node
npm install
npm test
```

The client automatically locates the native library in `clients/node/native/`, the `CSHARPDB_NATIVE_PATH` environment variable, or the current working directory.

For more details, see the [Native Library Reference](src/CSharpDB.Native/README.md).

## Failure-Path Regression Checklist

When behavior changes touch connection lifecycle, argument binding, or execution flow, include failure-path coverage in addition to happy-path tests.

- Verify cancellation/error paths do not leave stale internal state (for example pooled connection state after `OpenAsync` failure).
- Verify unsupported/invalid argument types return structured errors instead of unhandled exceptions (for example procedure execution with unsupported parameter payloads).
- Verify non-`DbException` failures in service execution paths are handled according to API contract.

## Pull Request Guidelines

1. Keep PRs focused and reasonably small.
2. Add or update tests for behavior changes.
3. Update docs when user-facing behavior changes.
4. Ensure the solution builds cleanly before opening the PR.
5. Describe:
   - what changed
   - why it changed
   - how it was tested

## Coding Guidelines

- Follow existing code style and naming conventions.
- Prefer clear, maintainable changes over clever shortcuts.
- Avoid adding dependencies unless clearly justified.
- Preserve backward compatibility unless the PR explicitly documents a breaking change.

## Commit Messages

Use clear, imperative messages, for example:

- `Fix query parser for dotted table names`
- `Add system catalog tests for sys.tables`
- `Update admin README screenshots`

## Questions

If anything is unclear, open an issue or discussion and ask before implementing large changes.
