# User-Defined Functions (UDFs) for CSharpDB

Call C# libraries as SQL functions inside CSharpDB queries.

---

## Motivation

CSharpDB currently supports aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) evaluated inside the `AggregateOperator`, but has no scalar function infrastructure. Users cannot call functions like `UPPER(name)`, `ABS(balance)`, or register custom logic that runs per-row during query evaluation.

Adding user-defined functions (UDFs) enables:

- **Built-in scalar functions** — string, math, date, type-inspection utilities expected from any SQL database
- **Custom C# logic** — domain-specific transformations compiled as native plugins
- **Sandboxed UDFs** — safe execution of untrusted user-submitted functions via WebAssembly

### NativeAOT Constraint

CSharpDB targets NativeAOT (`PublishAot=true`) for its native FFI library. This eliminates traditional .NET dynamic code loading:

| Approach | Why it doesn't work |
|----------|-------------------|
| `Assembly.LoadFrom()` | No JIT compiler to execute loaded IL |
| `AssemblyLoadContext` | Requires JIT infrastructure |
| `Reflection.Emit` | Requires JIT |
| Roslyn `CSharpScript` | Compiles to in-memory assemblies, needs JIT |

CSharpDB uses approaches that are fully AOT-compatible: static registration, `NativeLibrary.Load()` for native shared libraries, and optionally Wasmtime for WASM sandboxing.

---

## Function Types

### Scalar Functions

Return a single value for each row. Called in `SELECT` lists, `WHERE` clauses, `ORDER BY`, etc.

```sql
SELECT UPPER(name), ABS(balance) FROM accounts WHERE LENGTH(name) > 3;
```

### Aggregate Functions

Accumulate state across rows and produce a final result. Used with `GROUP BY`.

```sql
SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department;
```

CSharpDB already supports `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`. The UDF system generalizes this to user-defined aggregates.

### Table-Valued Functions (Planned)

Return a result set (multiple rows and columns). Used in `FROM` clauses.

```sql
SELECT * FROM generate_series(1, 100) WHERE value % 2 = 0;
SELECT * FROM read_csv('data.csv') WHERE amount > 500;
```

---

## Usage

### Built-in Functions

Available out of the box with no registration required.

**String functions:**

```sql
SELECT UPPER('hello');              -- 'HELLO'
SELECT LOWER('World');              -- 'world'
SELECT LENGTH('CSharpDB');          -- 8
SELECT SUBSTR('CSharpDB', 2, 5);   -- 'Sharp'
SELECT TRIM('  hello  ');           -- 'hello'
SELECT REPLACE('foo bar', 'bar', 'baz');  -- 'foo baz'
SELECT INSTR('hello world', 'world');     -- 7
```

**Math functions:**

```sql
SELECT ABS(-42);          -- 42
SELECT ROUND(3.14159, 2); -- 3.14
SELECT SIGN(-7);          -- -1
SELECT CEIL(2.3);         -- 3
SELECT FLOOR(2.9);        -- 2
```

**Type and conditional functions:**

```sql
SELECT TYPEOF(42);                    -- 'integer'
SELECT TYPEOF('hello');               -- 'text'
SELECT COALESCE(NULL, NULL, 'found'); -- 'found'
SELECT IIF(score > 90, 'A', 'B') FROM grades;
SELECT NULLIF(a, b) FROM data;       -- NULL if a = b, else a
SELECT IFNULL(nickname, name) FROM users;
```

### Registering Custom Functions (C# API)

Register scalar functions from your application code:

```csharp
using CSharpDB.Engine;

var db = await Database.OpenAsync("mydata.db");

// Register a scalar function
db.RegisterScalarFunction("my_hash", argCount: 1, args =>
{
    if (args[0].IsNull) return DbValue.Null;
    var hash = args[0].AsText.GetHashCode();
    return DbValue.FromInteger(hash);
});

// Use it in queries
var result = await db.ExecuteAsync("SELECT my_hash(name) FROM users");
```

Register custom aggregate functions:

```csharp
// Custom aggregate: string concatenation with separator
db.RegisterAggregateFunction<List<string>>(
    name: "group_concat",
    argCount: 1,
    init: () => new List<string>(),
    step: (state, args) =>
    {
        if (!args[0].IsNull)
            state.Add(args[0].AsText);
        return state;
    },
    finalize: state => DbValue.FromText(string.Join(", ", state))
);

var result = await db.ExecuteAsync(
    "SELECT department, group_concat(name) FROM employees GROUP BY department");
```

### Registering via ADO.NET

```csharp
using var connection = new CSharpDbConnection("Data Source=mydata.db");
await connection.OpenAsync();

connection.RegisterScalarFunction("double_it", 1, args =>
    DbValue.FromInteger(args[0].AsInteger * 2));

using var cmd = connection.CreateCommand();
cmd.CommandText = "SELECT double_it(value) FROM numbers";
using var reader = await cmd.ExecuteReaderAsync();
```

### Loading Native Plugin Extensions

Load a NativeAOT-compiled shared library that registers functions:

```csharp
// C# API
db.LoadExtension("path/to/my_extension.dll");
```

```sql
-- SQL
LOAD EXTENSION 'path/to/my_extension.dll';
```

```
-- CLI meta-command
.load path/to/my_extension.dll
```

Extension loading is **disabled by default** and must be explicitly enabled:

```csharp
var db = await Database.OpenAsync("mydata.db", new DatabaseOptions
{
    AllowExtensionLoading = true,
    ExtensionSearchPath = "/usr/local/lib/csharpdb/extensions"
});
```

---

## Writing a Native Plugin

Native plugins are shared libraries (`.dll`/`.so`/`.dylib`) that export a C-compatible init function. They can be written in any language that produces native shared libraries — C#, C, C++, Rust, Go, etc.

### C# Plugin Example

**1. Create the project:**

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <PublishAot>true</PublishAot>
    <NativeLib>Shared</NativeLib>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="CSharpDB.Plugin.Sdk" Version="1.0.0" />
  </ItemGroup>
</Project>
```

**2. Implement the extension:**

```csharp
using CSharpDB.Plugin.Sdk;

public static class MyExtension
{
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_extension_init")]
    public static unsafe int Init(CSharpDbApi* api)
    {
        // Verify ABI compatibility
        if (api->Version < CSharpDbApi.MinSupportedVersion)
            return -1;

        // Register a scalar function
        fixed (byte* name = "rot13"u8)
        {
            api->RegisterScalar(name, 1, &Rot13, IntPtr.Zero);
        }

        return 0; // success
    }

    [UnmanagedCallersOnly]
    private static unsafe DbValueInterop Rot13(
        IntPtr userData, DbValueInterop* args, int argCount)
    {
        if (args[0].Type == DbType.Null)
            return DbValueInterop.Null;

        var text = args[0].GetText();
        var rotated = string.Create(text.Length, text, (span, src) =>
        {
            for (int i = 0; i < src.Length; i++)
            {
                char c = src[i];
                if (c is >= 'a' and <= 'z')
                    span[i] = (char)('a' + (c - 'a' + 13) % 26);
                else if (c is >= 'A' and <= 'Z')
                    span[i] = (char)('A' + (c - 'A' + 13) % 26);
                else
                    span[i] = c;
            }
        });

        return DbValueInterop.FromText(rotated);
    }
}
```

**3. Build and publish:**

```bash
dotnet publish -c Release
```

### Rust Plugin Example

```rust
use std::os::raw::c_int;

#[repr(C)]
pub struct CSharpDbApi {
    pub version: c_int,
    pub register_scalar: unsafe extern "C" fn(
        name: *const u8,
        arg_count: c_int,
        func: unsafe extern "C" fn(*mut (), *const DbValueInterop, c_int) -> DbValueInterop,
        user_data: *mut (),
    ) -> c_int,
}

#[no_mangle]
pub unsafe extern "C" fn csharpdb_extension_init(api: *const CSharpDbApi) -> c_int {
    let api = &*api;
    (api.register_scalar)(
        b"rust_hello\0".as_ptr(),
        0,
        rust_hello,
        std::ptr::null_mut(),
    );
    0
}

unsafe extern "C" fn rust_hello(
    _user_data: *mut (),
    _args: *const DbValueInterop,
    _arg_count: c_int,
) -> DbValueInterop {
    DbValueInterop::from_text("Hello from Rust!")
}
```

---

## Security Model

### Permission Levels

| Level | Description | Use Case |
|-------|-------------|----------|
| **Built-in only** | No external functions; only shipped built-ins | Production default |
| **Application-registered** | C# delegates registered via `Database` API | Application-specific logic |
| **Trusted extensions** | Native plugins from a configured path | Vetted third-party extensions |
| **Sandboxed** (future) | WASM modules with resource limits | Untrusted user-submitted code |

### Native Plugin Security

Native plugins run **in-process with full trust**. A malicious or buggy plugin can crash the engine, corrupt data, or access the filesystem.

Mitigations:
- Extension loading disabled by default
- Optional path restriction (`ExtensionSearchPath`)
- Future: cryptographic signature verification
- Future: WASM sandbox for untrusted code

### Resource Limits (WASM — Future)

```sql
CREATE FUNCTION expensive_udf(x REAL) RETURNS REAL
    LANGUAGE WASM
    FROM 'analysis.wasm'
    WITH (MAX_FUEL = 1000000, MAX_MEMORY = '4MB');
```

| Resource | Enforcement | Default |
|----------|------------|---------|
| CPU | Wasmtime fuel (instruction count) | 1M instructions |
| Memory | WASM linear memory cap | 4 MB |
| Time | CancellationToken deadline | Query timeout |
| I/O | No filesystem/network imports | Fully denied |

---

## Architecture

### Query Pipeline Integration

```
SQL: SELECT my_func(col) FROM t WHERE other_func(col) > 0
         │                              │
         ▼                              ▼
    Parser: FunctionCallExpression  FunctionCallExpression
         │                              │
         ▼                              ▼
    QueryPlanner: Is it aggregate? ──No──► Scalar path
         │                              │
         ▼                              ▼
    AggregateOperator              ExpressionEvaluator
    (step/finalize)                    │
                                       ▼
                                  FunctionRegistry.TryGetScalar("my_func")
                                       │
                                       ▼
                                  Invoke delegate(args) → DbValue
```

### Function Registry

```
FunctionRegistry
  ├─ Built-in scalars (UPPER, ABS, COALESCE, ...)
  ├─ Built-in aggregates (COUNT, SUM, AVG, ...)
  ├─ User-registered scalars (via Database API)
  ├─ User-registered aggregates (via Database API)
  ├─ Native plugin functions (via NativeLibrary.Load)
  └─ WASM functions (via Wasmtime, future)
```

All function types share the same `FunctionRegistry` and are invoked through the same code path in the expression evaluator. The caller does not know (or care) whether a function is built-in, user-registered, native, or WASM-based.

### NativeAOT Compatibility

| Component | AOT Strategy |
|-----------|-------------|
| Built-in functions | Static `Func<DbValue[], DbValue>` delegates |
| User-registered | Runtime delegate registration (no reflection) |
| Native plugins | `NativeLibrary.Load` + `GetExport` + `delegate* unmanaged` |
| WASM | Wasmtime native library via P/Invoke |

---

## Implementation Plan

### Phase 1 — Built-in Function Registry & Scalar Evaluation

**Goal:** Add a static function registry and wire scalar function calls through the query pipeline.

**Status:** Planned
**Estimated scope:** ~500–800 lines across 4–5 files

#### Function Registry (`CSharpDB.Execution`)

```csharp
public sealed class FunctionRegistry
{
    private readonly Dictionary<string, ScalarFunctionEntry> _scalars
        = new(StringComparer.OrdinalIgnoreCase);

    public void RegisterScalar(string name, int argCount, Func<DbValue[], DbValue> func);
    public bool TryGetScalar(string name, out ScalarFunctionEntry entry);
}

public readonly record struct ScalarFunctionEntry(
    string Name,
    int ArgCount,          // -1 = variadic
    Func<DbValue[], DbValue> Invoke);
```

The registry is populated at engine startup with built-in functions. It lives on `QueryPlanner` (or a shared context) so both the expression evaluator and compiled expression paths can resolve functions.

#### Expression Evaluator Integration

`ExpressionEvaluator.cs` currently does not handle `FunctionCallExpression` for non-aggregate calls. Add a case:

```
FunctionCallExpression
  ├─ Check FunctionRegistry for scalar match
  ├─ Evaluate each argument expression
  ├─ Invoke the registered delegate
  └─ Return DbValue result
```

`ExpressionCompiler.cs` gets a parallel `CompileFunctionCall` that bakes the delegate lookup into the compiled expression tree for cached plans.

#### Built-in Function Set (Initial)

| Category | Functions |
|----------|-----------|
| **String** | `UPPER`, `LOWER`, `LENGTH`, `TRIM`, `LTRIM`, `RTRIM`, `SUBSTR`, `REPLACE`, `INSTR`, `HEX`, `QUOTE`, `CHAR`, `UNICODE` |
| **Math** | `ABS`, `ROUND`, `MIN` (scalar), `MAX` (scalar), `RANDOM`, `SIGN`, `CEIL`, `FLOOR` |
| **Type** | `TYPEOF`, `CAST`, `COALESCE`, `NULLIF`, `IIF`, `IFNULL` |
| **Date/Time** | `DATE`, `TIME`, `DATETIME`, `STRFTIME`, `JULIANDAY` |
| **Aggregate** | (existing `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` stay in `AggregateOperator`) |

#### SQL Syntax

No new syntax required — `FunctionCallExpression` already exists in the AST (`Ast.cs:274`). The parser already recognizes `funcname(args...)` patterns. The only change is that the execution layer resolves these against the function registry instead of only checking for known aggregates.

#### Key Files Affected

| File | Change |
|------|--------|
| `CSharpDB.Execution/FunctionRegistry.cs` | New — static registry + built-in registration |
| `CSharpDB.Execution/ExpressionEvaluator.cs` | Add `FunctionCallExpression` scalar dispatch |
| `CSharpDB.Execution/ExpressionCompiler.cs` | Add `CompileFunctionCall` delegate binding |
| `CSharpDB.Execution/QueryPlanner.cs` | Pass registry to evaluator/compiler; distinguish aggregate vs. scalar calls |
| `CSharpDB.Execution/Operators.cs` | Minor — ensure `AggregateOperator` still handles aggregates, scalars delegated to evaluator |

#### Testing

- Unit tests for each built-in function (null handling, type coercion, edge cases)
- Integration tests: `SELECT UPPER(name) FROM users WHERE LENGTH(name) > 5`
- Verify cached plans correctly re-invoke functions
- Benchmark: function call overhead vs. raw column access

---

### Phase 2 — User-Registered Functions (In-Process C# API)

**Goal:** Allow C# application code to register custom scalar and aggregate functions at runtime before queries execute.

**Status:** Planned
**Estimated scope:** ~300–400 lines

#### Public API on `Database`

```csharp
public class Database
{
    // Scalar: receives argument values, returns one value
    public void RegisterScalarFunction(
        string name,
        int argCount,
        Func<DbValue[], DbValue> function);

    // Aggregate: step + finalize pattern
    public void RegisterAggregateFunction<TState>(
        string name,
        int argCount,
        Func<TState> init,
        Func<TState, DbValue[], TState> step,
        Func<TState, DbValue> finalize);
}
```

#### Client SDK Surface

`ICSharpDbClient` gains matching methods. For the `Direct` transport, these delegate straight to `Database`. Remote transports (future HTTP/gRPC) cannot register functions — this is an in-process-only capability.

#### Aggregate Function Generalization

Move the existing hardcoded `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` into the `FunctionRegistry` as registered aggregates, using the same `init`/`step`/`finalize` pattern. `AggregateOperator` becomes a generic aggregate executor driven by the registry rather than a switch statement.

#### `CREATE FUNCTION` DDL (SQL-level registration)

```sql
-- Register a scalar function backed by a .NET method
-- (only applicable to in-process usage via the C# API)
CREATE FUNCTION my_upper(text TEXT) RETURNS TEXT
    AS EXTERNAL NAME 'MyAssembly.MyClass.MyMethod';
```

This is syntactic sugar — it stores the binding in the schema catalog and resolves the delegate at plan time. Under NativeAOT, the referenced method must be statically compiled into the host application.

---

### Phase 3 — Native Plugin Extensions (Shared Library Model)

**Goal:** Load C# (or C/Rust/Go) functions from NativeAOT-compiled shared libraries at runtime, following the SQLite/DuckDB extension model.

**Status:** Research
**Estimated scope:** ~800–1200 lines (new `CSharpDB.Extensions` project)

#### Architecture

```
┌─────────────────────────────────────────────────┐
│ CSharpDB Engine                                 │
│                                                 │
│  NativeLibrary.Load("myext.dll")                │
│       │                                         │
│       ▼                                         │
│  GetExport("csharpdb_extension_init")           │
│       │                                         │
│       ▼                                         │
│  Call init(api_table*)                           │
│       │                                         │
│       │  ┌────────────────────────────┐         │
│       └──► Plugin calls:              │         │
│          │  api.RegisterScalar(...)   │         │
│          │  api.RegisterAggregate(...)│         │
│          │  api.RegisterTableFunc(...)│         │
│          └────────────────────────────┘         │
│                                                 │
│  Function pointers stored in FunctionRegistry   │
│  Invoked during query evaluation                │
└─────────────────────────────────────────────────┘
```

#### C ABI Contract

```csharp
// Exported by CSharpDB — passed to plugin init
[StructLayout(LayoutKind.Sequential)]
public unsafe struct CSharpDbApi
{
    public int Version;  // ABI version for compatibility
    public delegate* unmanaged<byte*, int,
        delegate* unmanaged<IntPtr, DbValueInterop*, int, DbValueInterop>,
        IntPtr, int> RegisterScalar;
    // ... RegisterAggregate, RegisterTableValuedFunction, etc.
}

// Interop value type (blittable, no GC references)
[StructLayout(LayoutKind.Sequential)]
public struct DbValueInterop
{
    public DbType Type;
    public long IntegerValue;
    public double RealValue;
    public IntPtr TextPtr;
    public int TextLength;
    public IntPtr BlobPtr;
    public int BlobLength;
}
```

#### ABI Versioning

The `CSharpDbApi.Version` field enables forward compatibility:
- Engine refuses plugins compiled against a newer ABI version
- Engine provides shims for older ABI versions when possible
- Breaking ABI changes increment the major version

#### Plugin SDK Package

Publish a `CSharpDB.Plugin.Sdk` NuGet package containing:
- ABI contract structs (`CSharpDbApi`, `DbValueInterop`)
- Helper utilities for marshaling `DbValue` ↔ `DbValueInterop`
- MSBuild targets for NativeAOT compilation with correct settings
- Template/example project

#### Key Files

| File | Change |
|------|--------|
| `CSharpDB.Extensions/ExtensionLoader.cs` | New — `NativeLibrary.Load`, init dispatch |
| `CSharpDB.Extensions/CSharpDbApi.cs` | New — ABI contract structs |
| `CSharpDB.Extensions/DbValueInterop.cs` | New — blittable value marshaling |
| `CSharpDB.Engine/Database.cs` | `LoadExtension(path)` method |
| `CSharpDB.Sql/Parser.cs` | Parse `LOAD EXTENSION` statement |
| `CSharpDB.Sql/Ast.cs` | `LoadExtensionStatement` node |
| `CSharpDB.Cli/Program.cs` | `.load` meta-command |

---

### Phase 4 — Table-Valued Functions

**Goal:** Support functions that return result sets (multiple rows/columns), enabling virtual table patterns.

**Status:** Research
**Estimated scope:** ~600–800 lines

#### Interface

```csharp
public interface ITableValuedFunction
{
    string Name { get; }
    ColumnDefinition[] OutputSchema(DbValue[] args);
    IAsyncEnumerable<DbValue[]> Execute(DbValue[] args, CancellationToken ct);
}
```

Table-valued functions integrate with the query planner as leaf operators in the operator tree, similar to `TableScanOperator` but driven by the function's async enumerable.

#### Registration

```csharp
database.RegisterTableFunction("generate_series", new GenerateSeriesFunction());
database.RegisterTableFunction("read_csv", new CsvReaderFunction());
```

Native plugins can register table-valued functions through the ABI as well, using callback-based iteration rather than `IAsyncEnumerable`.

---

### Phase 5 — WebAssembly Sandboxed UDFs (Optional)

**Goal:** Execute untrusted user-submitted UDFs in a WebAssembly sandbox with resource limits.

**Status:** Research
**Estimated scope:** New optional package `CSharpDB.Wasm`

#### Architecture

```
┌──────────────────────────────────┐
│ CSharpDB Engine                  │
│                                  │
│  Wasmtime Engine                 │
│    ├─ Module (compiled .wasm)    │
│    ├─ Linker (host functions)    │
│    │   ├─ dbvalue_get_type()     │
│    │   ├─ dbvalue_get_integer()  │
│    │   ├─ dbvalue_get_text()     │
│    │   ├─ dbvalue_set_result()   │
│    │   └─ ...                    │
│    └─ Instance                   │
│        └─ call exported func     │
│                                  │
│  Resource Limits:                │
│    ├─ Fuel (instruction budget)  │
│    ├─ Memory cap (linear memory) │
│    └─ No filesystem/network      │
└──────────────────────────────────┘
```

#### Execution Model

- **Module pre-compilation:** `.wasm` → compiled module cached in memory
- **Instance pooling:** Pre-warm a pool of module instances to avoid instantiation cost per call
- **Argument marshaling:** `DbValue[]` → WASM linear memory → call → read result from linear memory → `DbValue`
- **Resource enforcement:** Fuel-based CPU limiting, bounded linear memory, no imported I/O functions

#### Language Support

Since WASM is language-agnostic, UDFs can be authored in:
- **Rust** (first-class WASM target)
- **C/C++** (via Emscripten or wasi-sdk)
- **Go** (via TinyGo)
- **C#** (via `dotnet-wasi-sdk` / Blazor WASM toolchain)
- **AssemblyScript** (TypeScript-like, designed for WASM)

#### Security Comparison

| Property | Native Plugin (Phase 3) | WASM UDF (Phase 5) |
|----------|:-----------------------:|:-------------------:|
| Memory isolation | None (shared process) | Full (linear memory) |
| CPU limits | None | Fuel-based |
| File system access | Full | Denied by default |
| Network access | Full | Denied by default |
| Crash containment | Crashes engine | Trapped, engine continues |
| Call overhead | ~nanoseconds | ~microseconds |
| Language support | Any (via C ABI) | Any (via WASM) |
| Trust model | Trusted code only | Untrusted code safe |

---

## Implementation Priority & Dependencies

```
Phase 1: Built-in Function Registry
   │
   ▼
Phase 2: User-Registered Functions (C# API)
   │
   ├──────────────────────┐
   ▼                      ▼
Phase 3: Native Plugins   Phase 4: Table-Valued Functions
   │
   ▼
Phase 5: WASM Sandboxed UDFs (optional)
```

Phases 1 and 2 are prerequisites for all later phases. Phases 3, 4, and 5 can proceed independently once the function registry and evaluation pipeline are in place.

| Phase | Complexity | Prerequisites |
|-------|-----------|---------------|
| Phase 1 — Built-in functions | Low–Medium | None |
| Phase 2 — User-registered functions | Low | Phase 1 |
| Phase 3 — Native plugins | Medium–High | Phase 2 |
| Phase 4 — Table-valued functions | Medium | Phase 1 |
| Phase 5 — WASM sandboxing | High | Phase 2 |

---

## Prior Art & References

| Database | Extension Model | Sandboxing | Notes |
|----------|----------------|------------|-------|
| **SQLite** | C shared library via `dlopen` | None | Closest analog; CSharpDB Phase 3 follows this pattern |
| **DuckDB** | NativeAOT-style shared libraries + signed extensions | None | Added cryptographic signing for extension trust |
| **PostgreSQL** | C shared library via `dlopen` + PL/pgSQL, PL/Python, etc. | Process-level for PL languages | Most mature extension ecosystem |
| **SQL Server** | CLR hosted in-process | AppDomain + CAS (deprecated) | Requires full CLR; incompatible with NativeAOT |
| **ScyllaDB** | WebAssembly via Wasmtime | Full WASM sandbox | Pioneered WASM UDFs for databases |
| **SingleStore** | WebAssembly | Full WASM sandbox | Production WASM UDF system |
| **TiDB** | WebAssembly via Wasmer | Full WASM sandbox | Open-source WASM UDF implementation |

---

## Comparison with Other Databases

| Feature | CSharpDB (Planned) | SQLite | DuckDB | SQL Server |
|---------|:------------------:|:------:|:------:|:----------:|
| Built-in scalar functions | Phase 1 | Yes | Yes | Yes |
| User-registered functions | Phase 2 | Yes (C API) | Yes | Yes (CLR) |
| Native extension loading | Phase 3 | Yes (`dlopen`) | Yes (signed) | No |
| Table-valued functions | Phase 4 | Yes (vtable) | Yes | Yes (CLR) |
| Sandboxed execution | Phase 5 (WASM) | No | No | AppDomain (deprecated) |
| NativeAOT compatible | Yes | N/A (C) | N/A (C++) | No (requires CLR) |

---

## See Also

- [CSharpDB Roadmap](../roadmap.md) — Overall project roadmap
- [Architecture Guide](../architecture.md) — Engine architecture overview
- [Internals & Contributing](../internals.md) — How to extend the engine
- [Native FFI Tutorials](../tutorials/native-ffi/README.md) — Cross-language interop via NativeAOT
- [Storage Engine Guide](../storage/README.md) — Storage layer API reference
