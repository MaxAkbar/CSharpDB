# CSharpDB Native Library

A NativeAOT-compiled shared library that exposes the CSharpDB embedded database engine as a C-compatible API. This allows **any programming language** to use CSharpDB via FFI (Foreign Function Interface) — no .NET runtime required.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Building](#building)
  - [Windows](#windows)
  - [Linux](#linux)
  - [macOS](#macOS)
  - [Android](#android)
  - [iOS](#ios)
  - [Cross-Compilation](#cross-compilation)
- [Output](#output)
- [API Reference](#api-reference)
  - [Database Lifecycle](#database-lifecycle)
  - [SQL Execution](#sql-execution)
  - [Result Navigation](#result-navigation)
  - [Value Access](#value-access)
  - [Transactions](#transactions)
  - [Error Handling](#error-handling)
- [Type Codes](#type-codes)
- [Usage Examples](#usage-examples)
  - [C](#c)
  - [Python](#python)
  - [Go](#go)
  - [Rust](#rust)
  - [Swift](#swift)
  - [Kotlin (JNA)](#kotlin-jna)
  - [Dart / Flutter](#dart--flutter)
  - [Node.js](#nodejs)
- [Android Integration](#android-integration)
  - [Option A: .NET MAUI (No Native Build)](#option-a-net-maui-android-app-no-native-library-needed)
  - [Option B: Kotlin / Java / Flutter (Native .so)](#option-b-kotlin--java--flutter--react-native-native-so)
- [Language Client Packages](#language-client-packages)
- [Thread Safety](#thread-safety)
- [Memory Management](#memory-management)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

### All Platforms

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0) or later

### Windows

- **Visual Studio** with the **"Desktop development with C++"** workload installed, or
- **Visual Studio Build Tools** with the **C++ build tools** component

Install via Visual Studio Installer or command line:

```
vs_buildtools.exe --add Microsoft.VisualStudio.Workload.VCTools
```

### Linux

```bash
# Ubuntu / Debian
sudo apt install clang zlib1g-dev

# Fedora / RHEL
sudo dnf install clang zlib-devel

# Alpine
apk add clang build-base zlib-dev
```

### macOS

```bash
xcode-select --install
```

---

## Building

### Windows

```bash
dotnet publish src/CSharpDB.Native -c Release -r win-x64
```

Output: `src/CSharpDB.Native/bin/Release/net10.0/win-x64/publish/CSharpDB.Native.dll`

For ARM64 Windows:

```bash
dotnet publish src/CSharpDB.Native -c Release -r win-arm64
```

### Linux

```bash
dotnet publish src/CSharpDB.Native -c Release -r linux-x64
```

Output: `src/CSharpDB.Native/bin/Release/net10.0/linux-x64/publish/CSharpDB.Native.so`

For ARM64 (e.g., Raspberry Pi, AWS Graviton):

```bash
dotnet publish src/CSharpDB.Native -c Release -r linux-arm64
```

### macOS

```bash
# Apple Silicon
dotnet publish src/CSharpDB.Native -c Release -r osx-arm64

# Intel
dotnet publish src/CSharpDB.Native -c Release -r osx-x64
```

Output: `src/CSharpDB.Native/bin/Release/net10.0/osx-arm64/publish/CSharpDB.Native.dylib`

### Android

```bash
# ARM64 (most modern Android devices)
dotnet publish src/CSharpDB.Native -c Release -r linux-bionic-arm64

# x64 (emulators)
dotnet publish src/CSharpDB.Native -c Release -r linux-bionic-x64
```

### iOS

iOS requires building on macOS with Xcode installed:

```bash
dotnet publish src/CSharpDB.Native -c Release -r ios-arm64
```

> **Note:** iOS apps require static linking. You may need to set
> `<NativeLib>Static</NativeLib>` in the `.csproj` for iOS targets
> and link the resulting `.a` file into your Xcode project.

### Cross-Compilation

You can cross-compile from one platform to another if the appropriate toolchain is installed. For example, to build a Linux library from Windows using WSL or a Docker container:

```bash
docker run --rm -v $(pwd):/src -w /src mcr.microsoft.com/dotnet/sdk:10.0 \
    dotnet publish src/CSharpDB.Native -c Release -r linux-x64
```

---

## Output

After publishing, the output directory contains:

| File | Description |
|------|-------------|
| `CSharpDB.Native.dll` / `.so` / `.dylib` | The native shared library (this is all you need to ship) |

The library is fully self-contained. There is no dependency on the .NET runtime. Copy it alongside your application and load it like any other native library.

Typical sizes (Release, stripped):

| Platform | Approximate Size |
|----------|-----------------|
| Windows x64 | ~8-15 MB |
| Linux x64 | ~8-15 MB |
| macOS ARM64 | ~8-15 MB |

---

## API Reference

The C header file [`csharpdb.h`](csharpdb.h) defines the full API. All functions use C calling conventions and UTF-8 strings.

### Database Lifecycle

#### `csharpdb_open`

```c
csharpdb_t csharpdb_open(const char* path);
```

Open or create a database file at the given path.

- **path** — UTF-8 file path. Creates the file if it does not exist.
- **Returns** — Opaque database handle, or `NULL` on error.

#### `csharpdb_close`

```c
void csharpdb_close(csharpdb_t db);
```

Close the database and free all resources. Safe to call with `NULL`.

---

### SQL Execution

#### `csharpdb_execute`

```c
csharpdb_result_t csharpdb_execute(csharpdb_t db, const char* sql);
```

Execute a SQL statement (SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, etc.).

- **db** — Database handle from `csharpdb_open`.
- **sql** — UTF-8 SQL string.
- **Returns** — Result handle, or `NULL` on error. **Must** be freed with `csharpdb_result_free`.

---

### Result Navigation

#### `csharpdb_result_is_query`

```c
int csharpdb_result_is_query(csharpdb_result_t result);
```

Returns `1` if the result is from a SELECT query, `0` for DML/DDL statements.

#### `csharpdb_result_rows_affected`

```c
int csharpdb_result_rows_affected(csharpdb_result_t result);
```

Returns the number of rows affected by INSERT, UPDATE, or DELETE.

#### `csharpdb_result_column_count`

```c
int csharpdb_result_column_count(csharpdb_result_t result);
```

Returns the number of columns in the result set.

#### `csharpdb_result_column_name`

```c
const char* csharpdb_result_column_name(csharpdb_result_t result, int column_index);
```

Returns the name of a column (UTF-8). The pointer is valid until `csharpdb_result_free`.

#### `csharpdb_result_next`

```c
int csharpdb_result_next(csharpdb_result_t result);
```

Advance to the next row.

- Returns `1` if a row is available.
- Returns `0` at the end of results.
- Returns `-1` on error.

---

### Value Access

All value-access functions operate on the **current row** (the last row returned by `csharpdb_result_next`).

#### `csharpdb_result_column_type`

```c
int csharpdb_result_column_type(csharpdb_result_t result, int column_index);
```

Returns the [type code](#type-codes) of the value at the given column.

#### `csharpdb_result_is_null`

```c
int csharpdb_result_is_null(csharpdb_result_t result, int column_index);
```

Returns `1` if the value is NULL, `0` otherwise.

#### `csharpdb_result_get_int64`

```c
int64_t csharpdb_result_get_int64(csharpdb_result_t result, int column_index);
```

Read a 64-bit integer from the current row.

#### `csharpdb_result_get_double`

```c
double csharpdb_result_get_double(csharpdb_result_t result, int column_index);
```

Read a double-precision float from the current row.

#### `csharpdb_result_get_text`

```c
const char* csharpdb_result_get_text(csharpdb_result_t result, int column_index);
```

Read a UTF-8 text value. The pointer is valid until the next `csharpdb_result_next` or `csharpdb_result_free`.

#### `csharpdb_result_get_blob`

```c
const void* csharpdb_result_get_blob(csharpdb_result_t result, int column_index, int* out_size);
```

Read a blob. `out_size` receives the byte count (may be `NULL`). The pointer is valid until the next `csharpdb_result_next` or `csharpdb_result_free`.

#### `csharpdb_result_free`

```c
void csharpdb_result_free(csharpdb_result_t result);
```

Free a result handle and all associated resources. Safe to call with `NULL`.

---

### Transactions

#### `csharpdb_begin`

```c
int csharpdb_begin(csharpdb_t db);
```

Begin an explicit transaction. Returns `0` on success, `-1` on error.

#### `csharpdb_commit`

```c
int csharpdb_commit(csharpdb_t db);
```

Commit the current transaction. Returns `0` on success, `-1` on error.

#### `csharpdb_rollback`

```c
int csharpdb_rollback(csharpdb_t db);
```

Rollback the current transaction. Returns `0` on success, `-1` on error.

---

### Error Handling

Errors follow the `errno` pattern. After any function returns an error indicator (`NULL`, `-1`), call `csharpdb_last_error` to get the message.

#### `csharpdb_last_error`

```c
const char* csharpdb_last_error(void);
```

Returns the last error message (UTF-8), or `NULL` if no error. Valid until the next API call on the same thread.

#### `csharpdb_last_error_code`

```c
int csharpdb_last_error_code(void);
```

Returns the last error code. `0` = no error, `-1` = generic error. Positive values map to `CSharpDB.Primitives.ErrorCode`:

| Code | Name |
|------|------|
| 0 | Unknown |
| 1 | IoError |
| 2 | CorruptDatabase |
| 3 | TableNotFound |
| 4 | TableAlreadyExists |
| 5 | ColumnNotFound |
| 6 | TypeMismatch |
| 7 | SyntaxError |
| 8 | ConstraintViolation |
| 9 | JournalError |
| 10 | DuplicateKey |
| 11 | TriggerNotFound |
| 12 | TriggerAlreadyExists |
| 13 | WalError |
| 14 | Busy |

#### `csharpdb_clear_error`

```c
void csharpdb_clear_error(void);
```

Clear the error state for the current thread.

---

## Type Codes

| Code | Constant | Description |
|------|----------|-------------|
| 0 | `CSHARPDB_NULL` | SQL NULL |
| 1 | `CSHARPDB_INTEGER` | 64-bit signed integer |
| 2 | `CSHARPDB_REAL` | 64-bit IEEE 754 double |
| 3 | `CSHARPDB_TEXT` | UTF-8 text string |
| 4 | `CSHARPDB_BLOB` | Binary data |

---

## Usage Examples

### C

```c
#include <stdio.h>
#include "csharpdb.h"

int main() {
    csharpdb_t db = csharpdb_open("test.db");
    if (!db) {
        fprintf(stderr, "Error: %s\n", csharpdb_last_error());
        return 1;
    }

    // Create table
    csharpdb_result_t r = csharpdb_execute(db,
        "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
    csharpdb_result_free(r);

    // Insert data
    r = csharpdb_execute(db, "INSERT INTO users VALUES (1, 'Alice', 30)");
    printf("Rows inserted: %d\n", csharpdb_result_rows_affected(r));
    csharpdb_result_free(r);

    r = csharpdb_execute(db, "INSERT INTO users VALUES (2, 'Bob', 25)");
    csharpdb_result_free(r);

    // Query data
    r = csharpdb_execute(db, "SELECT id, name, age FROM users ORDER BY id");
    int cols = csharpdb_result_column_count(r);

    // Print column headers
    for (int i = 0; i < cols; i++) {
        printf("%-10s", csharpdb_result_column_name(r, i));
    }
    printf("\n");

    // Print rows
    while (csharpdb_result_next(r) == 1) {
        printf("%-10lld", (long long)csharpdb_result_get_int64(r, 0));
        printf("%-10s", csharpdb_result_get_text(r, 1));
        printf("%-10lld", (long long)csharpdb_result_get_int64(r, 2));
        printf("\n");
    }

    csharpdb_result_free(r);
    csharpdb_close(db);
    return 0;
}
```

Compile:

```bash
# Windows (MSVC)
cl example.c CSharpDB.Native.lib

# Linux / macOS
gcc example.c -L. -lcsharpdb -o example
```

---

### Python

No external packages required — uses the built-in `ctypes` module.

#### Quick start

```bash
pip install csharpdb  # not needed — ctypes is built into Python
```

#### Reusable wrapper class

```python
"""csharpdb.py — Python wrapper for the CSharpDB native library."""

import ctypes
import os
import sys

class CSharpDB:
    """Thin Python wrapper around the CSharpDB NativeAOT shared library."""

    # Map CSharpDB type codes to names
    NULL, INTEGER, REAL, TEXT, BLOB = 0, 1, 2, 3, 4

    def __init__(self, lib_path=None):
        if lib_path is None:
            if sys.platform == "win32":
                lib_path = "./CSharpDB.Native.dll"
            elif sys.platform == "darwin":
                lib_path = "./libCSharpDB.Native.dylib"
            else:
                lib_path = "./libCSharpDB.Native.so"

        self._lib = ctypes.CDLL(lib_path)
        self._setup_signatures()
        self._db = None

    def _setup_signatures(self):
        L = self._lib
        vp = ctypes.c_void_p

        L.csharpdb_open.restype = vp
        L.csharpdb_open.argtypes = [ctypes.c_char_p]
        L.csharpdb_close.restype = None
        L.csharpdb_close.argtypes = [vp]
        L.csharpdb_execute.restype = vp
        L.csharpdb_execute.argtypes = [vp, ctypes.c_char_p]

        L.csharpdb_result_is_query.restype = ctypes.c_int
        L.csharpdb_result_is_query.argtypes = [vp]
        L.csharpdb_result_next.restype = ctypes.c_int
        L.csharpdb_result_next.argtypes = [vp]
        L.csharpdb_result_rows_affected.restype = ctypes.c_int
        L.csharpdb_result_rows_affected.argtypes = [vp]
        L.csharpdb_result_column_count.restype = ctypes.c_int
        L.csharpdb_result_column_count.argtypes = [vp]
        L.csharpdb_result_column_name.restype = ctypes.c_char_p
        L.csharpdb_result_column_name.argtypes = [vp, ctypes.c_int]
        L.csharpdb_result_column_type.restype = ctypes.c_int
        L.csharpdb_result_column_type.argtypes = [vp, ctypes.c_int]
        L.csharpdb_result_is_null.restype = ctypes.c_int
        L.csharpdb_result_is_null.argtypes = [vp, ctypes.c_int]
        L.csharpdb_result_get_int64.restype = ctypes.c_int64
        L.csharpdb_result_get_int64.argtypes = [vp, ctypes.c_int]
        L.csharpdb_result_get_double.restype = ctypes.c_double
        L.csharpdb_result_get_double.argtypes = [vp, ctypes.c_int]
        L.csharpdb_result_get_text.restype = ctypes.c_char_p
        L.csharpdb_result_get_text.argtypes = [vp, ctypes.c_int]
        L.csharpdb_result_get_blob.restype = vp
        L.csharpdb_result_get_blob.argtypes = [vp, ctypes.c_int, ctypes.POINTER(ctypes.c_int)]
        L.csharpdb_result_free.restype = None
        L.csharpdb_result_free.argtypes = [vp]

        L.csharpdb_begin.restype = ctypes.c_int
        L.csharpdb_begin.argtypes = [vp]
        L.csharpdb_commit.restype = ctypes.c_int
        L.csharpdb_commit.argtypes = [vp]
        L.csharpdb_rollback.restype = ctypes.c_int
        L.csharpdb_rollback.argtypes = [vp]

        L.csharpdb_last_error.restype = ctypes.c_char_p
        L.csharpdb_last_error_code.restype = ctypes.c_int
        L.csharpdb_clear_error.restype = None

    def _check_error(self, context=""):
        err = self._lib.csharpdb_last_error()
        if err:
            raise RuntimeError(f"CSharpDB error{' (' + context + ')' if context else ''}: {err.decode()}")

    def open(self, path):
        self._db = self._lib.csharpdb_open(path.encode("utf-8"))
        if not self._db:
            self._check_error("open")

    def close(self):
        if self._db:
            self._lib.csharpdb_close(self._db)
            self._db = None

    def execute(self, sql):
        """Execute a non-query statement. Returns rows affected."""
        r = self._lib.csharpdb_execute(self._db, sql.encode("utf-8"))
        if not r:
            self._check_error("execute")
        affected = self._lib.csharpdb_result_rows_affected(r)
        self._lib.csharpdb_result_free(r)
        return affected

    def query(self, sql):
        """Execute a SELECT and return a list of dicts."""
        r = self._lib.csharpdb_execute(self._db, sql.encode("utf-8"))
        if not r:
            self._check_error("query")

        col_count = self._lib.csharpdb_result_column_count(r)
        columns = []
        for i in range(col_count):
            name = self._lib.csharpdb_result_column_name(r, i)
            columns.append(name.decode() if name else f"col_{i}")

        rows = []
        while self._lib.csharpdb_result_next(r) == 1:
            row = {}
            for i, col in enumerate(columns):
                if self._lib.csharpdb_result_is_null(r, i) == 1:
                    row[col] = None
                else:
                    col_type = self._lib.csharpdb_result_column_type(r, i)
                    if col_type == self.INTEGER:
                        row[col] = self._lib.csharpdb_result_get_int64(r, i)
                    elif col_type == self.REAL:
                        row[col] = self._lib.csharpdb_result_get_double(r, i)
                    elif col_type == self.TEXT:
                        val = self._lib.csharpdb_result_get_text(r, i)
                        row[col] = val.decode() if val else ""
                    else:
                        row[col] = None  # BLOB / unknown
            rows.append(row)

        self._lib.csharpdb_result_free(r)
        return rows

    def begin(self):
        if self._lib.csharpdb_begin(self._db) != 0:
            self._check_error("begin")

    def commit(self):
        if self._lib.csharpdb_commit(self._db) != 0:
            self._check_error("commit")

    def rollback(self):
        if self._lib.csharpdb_rollback(self._db) != 0:
            self._check_error("rollback")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
```

#### Usage

```python
from csharpdb import CSharpDB

with CSharpDB() as db:
    db.open("books.db")

    # Create table
    db.execute("CREATE TABLE IF NOT EXISTS books (id INTEGER PRIMARY KEY, title TEXT, year INTEGER)")

    # Insert rows inside a transaction
    db.begin()
    db.execute("INSERT INTO books VALUES (1, 'Dune', 1965)")
    db.execute("INSERT INTO books VALUES (2, 'Neuromancer', 1984)")
    db.execute("INSERT INTO books VALUES (3, 'Snow Crash', 1992)")
    db.commit()

    # Query — returns list of dicts with typed values
    for book in db.query("SELECT id, title, year FROM books ORDER BY year"):
        print(f"  {book['id']}: {book['title']} ({book['year']})")

    # Aggregates work too
    result = db.query("SELECT COUNT(*) as total, MIN(year) as oldest FROM books")
    print(f"  {result[0]['total']} books, oldest from {result[0]['oldest']}")
```

Output:

```
  1: Dune (1965)
  2: Neuromancer (1984)
  3: Snow Crash (1992)
  3 books, oldest from 1965
```

---

### Go

```go
package main

/*
#cgo LDFLAGS: -L. -lCSharpDB.Native
#include "csharpdb.h"
#include <stdlib.h>
*/
import "C"
import (
    "fmt"
    "unsafe"
)

func main() {
    path := C.CString("test.db")
    defer C.free(unsafe.Pointer(path))

    db := C.csharpdb_open(path)
    if db == nil {
        fmt.Printf("Error: %s\n", C.GoString(C.csharpdb_last_error()))
        return
    }
    defer C.csharpdb_close(db)

    // Create table
    sql := C.CString("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
    r := C.csharpdb_execute(db, sql)
    C.free(unsafe.Pointer(sql))
    C.csharpdb_result_free(r)

    // Insert
    sql = C.CString("INSERT INTO users VALUES (1, 'Alice')")
    r = C.csharpdb_execute(db, sql)
    C.free(unsafe.Pointer(sql))
    fmt.Printf("Inserted: %d row(s)\n", C.csharpdb_result_rows_affected(r))
    C.csharpdb_result_free(r)

    // Query
    sql = C.CString("SELECT id, name FROM users")
    r = C.csharpdb_execute(db, sql)
    C.free(unsafe.Pointer(sql))

    for C.csharpdb_result_next(r) == 1 {
        id := int64(C.csharpdb_result_get_int64(r, 0))
        name := C.GoString(C.csharpdb_result_get_text(r, 1))
        fmt.Printf("  %d: %s\n", id, name)
    }

    C.csharpdb_result_free(r)
}
```

Build:

```bash
CGO_ENABLED=1 go build -o example
```

---

### Rust

```rust
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};

// Declare the external functions
extern "C" {
    fn csharpdb_open(path: *const c_char) -> *mut c_void;
    fn csharpdb_close(db: *mut c_void);
    fn csharpdb_execute(db: *mut c_void, sql: *const c_char) -> *mut c_void;
    fn csharpdb_result_next(result: *mut c_void) -> c_int;
    fn csharpdb_result_rows_affected(result: *mut c_void) -> c_int;
    fn csharpdb_result_get_int64(result: *mut c_void, col: c_int) -> i64;
    fn csharpdb_result_get_text(result: *mut c_void, col: c_int) -> *const c_char;
    fn csharpdb_result_free(result: *mut c_void);
    fn csharpdb_last_error() -> *const c_char;
}

fn main() {
    unsafe {
        let path = CString::new("test.db").unwrap();
        let db = csharpdb_open(path.as_ptr());
        if db.is_null() {
            let err = CStr::from_ptr(csharpdb_last_error());
            eprintln!("Error: {}", err.to_str().unwrap());
            return;
        }

        // Create table
        let sql = CString::new(
            "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)"
        ).unwrap();
        let r = csharpdb_execute(db, sql.as_ptr());
        csharpdb_result_free(r);

        // Insert
        let sql = CString::new("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        let r = csharpdb_execute(db, sql.as_ptr());
        println!("Inserted: {} row(s)", csharpdb_result_rows_affected(r));
        csharpdb_result_free(r);

        // Query
        let sql = CString::new("SELECT id, name FROM users").unwrap();
        let r = csharpdb_execute(db, sql.as_ptr());

        while csharpdb_result_next(r) == 1 {
            let id = csharpdb_result_get_int64(r, 0);
            let name = CStr::from_ptr(csharpdb_result_get_text(r, 1));
            println!("  {}: {}", id, name.to_str().unwrap());
        }

        csharpdb_result_free(r);
        csharpdb_close(db);
    }
}
```

In `build.rs`:

```rust
fn main() {
    println!("cargo:rustc-link-search=native=.");
    println!("cargo:rustc-link-lib=dylib=CSharpDB.Native");
}
```

---

### Swift

```swift
import Foundation

// Assumes csharpdb.h is added to the bridging header or a module map

let db = csharpdb_open("test.db")
guard db != nil else {
    if let err = csharpdb_last_error() {
        print("Error: \(String(cString: err))")
    }
    exit(1)
}

// Create table
var r = csharpdb_execute(db,
    "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
csharpdb_result_free(r)

// Insert
r = csharpdb_execute(db, "INSERT INTO users VALUES (1, 'Alice')")
print("Inserted: \(csharpdb_result_rows_affected(r)) row(s)")
csharpdb_result_free(r)

// Query
r = csharpdb_execute(db, "SELECT id, name FROM users")
while csharpdb_result_next(r) == 1 {
    let id = csharpdb_result_get_int64(r, 0)
    let name = String(cString: csharpdb_result_get_text(r, 1))
    print("  \(id): \(name)")
}

csharpdb_result_free(r)
csharpdb_close(db)
```

For iOS/macOS, add the library to your Xcode project and create a module map:

```
// module.modulemap
module CSharpDB {
    header "csharpdb.h"
    link "CSharpDB.Native"
    export *
}
```

---

### Kotlin (JNA)

```kotlin
import com.sun.jna.Library
import com.sun.jna.Native
import com.sun.jna.Pointer

interface CSharpDB : Library {
    fun csharpdb_open(path: String): Pointer?
    fun csharpdb_close(db: Pointer)
    fun csharpdb_execute(db: Pointer, sql: String): Pointer?
    fun csharpdb_result_next(result: Pointer): Int
    fun csharpdb_result_rows_affected(result: Pointer): Int
    fun csharpdb_result_column_count(result: Pointer): Int
    fun csharpdb_result_get_int64(result: Pointer, col: Int): Long
    fun csharpdb_result_get_text(result: Pointer, col: Int): String?
    fun csharpdb_result_free(result: Pointer)
    fun csharpdb_last_error(): String?

    companion object {
        val INSTANCE: CSharpDB = Native.load("CSharpDB.Native", CSharpDB::class.java)
    }
}

fun main() {
    val lib = CSharpDB.INSTANCE
    val db = lib.csharpdb_open("test.db")
        ?: error("Failed to open: ${lib.csharpdb_last_error()}")

    // Create table
    lib.csharpdb_result_free(
        lib.csharpdb_execute(db,
            "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")!!)

    // Insert
    val r = lib.csharpdb_execute(db, "INSERT INTO users VALUES (1, 'Alice')")!!
    println("Inserted: ${lib.csharpdb_result_rows_affected(r)} row(s)")
    lib.csharpdb_result_free(r)

    // Query
    val result = lib.csharpdb_execute(db, "SELECT id, name FROM users")!!
    while (lib.csharpdb_result_next(result) == 1) {
        val id = lib.csharpdb_result_get_int64(result, 0)
        val name = lib.csharpdb_result_get_text(result, 1)
        println("  $id: $name")
    }

    lib.csharpdb_result_free(result)
    lib.csharpdb_close(db)
}
```

Add JNA dependency:

```kotlin
// build.gradle.kts
dependencies {
    implementation("net.java.dev.jna:jna:5.14.0")
}
```

For **Android**, use JNA or JNI and place the `.so` in `jniLibs/arm64-v8a/`.

---

### Dart / Flutter

```dart
import 'dart:ffi';
import 'dart:io';
import 'package:ffi/package:ffi.dart';

// Type definitions
typedef CsharpdbOpenNative = Pointer<Void> Function(Pointer<Utf8>);
typedef CsharpdbOpen = Pointer<Void> Function(Pointer<Utf8>);

typedef CsharpdbExecuteNative = Pointer<Void> Function(Pointer<Void>, Pointer<Utf8>);
typedef CsharpdbExecute = Pointer<Void> Function(Pointer<Void>, Pointer<Utf8>);

typedef CsharpdbResultNextNative = Int32 Function(Pointer<Void>);
typedef CsharpdbResultNext = int Function(Pointer<Void>);

typedef CsharpdbResultGetInt64Native = Int64 Function(Pointer<Void>, Int32);
typedef CsharpdbResultGetInt64 = int Function(Pointer<Void>, int);

typedef CsharpdbResultGetTextNative = Pointer<Utf8> Function(Pointer<Void>, Int32);
typedef CsharpdbResultGetText = Pointer<Utf8> Function(Pointer<Void>, int);

typedef CsharpdbResultFreeNative = Void Function(Pointer<Void>);
typedef CsharpdbResultFree = void Function(Pointer<Void>);

typedef CsharpdbCloseNative = Void Function(Pointer<Void>);
typedef CsharpdbClose = void Function(Pointer<Void>);

void main() {
  final lib = DynamicLibrary.open(
    Platform.isWindows ? 'CSharpDB.Native.dll' :
    Platform.isMacOS   ? 'libCSharpDB.Native.dylib' :
                         'libCSharpDB.Native.so'
  );

  final open = lib.lookupFunction<CsharpdbOpenNative, CsharpdbOpen>('csharpdb_open');
  final execute = lib.lookupFunction<CsharpdbExecuteNative, CsharpdbExecute>('csharpdb_execute');
  final next = lib.lookupFunction<CsharpdbResultNextNative, CsharpdbResultNext>('csharpdb_result_next');
  final getInt64 = lib.lookupFunction<CsharpdbResultGetInt64Native, CsharpdbResultGetInt64>('csharpdb_result_get_int64');
  final getText = lib.lookupFunction<CsharpdbResultGetTextNative, CsharpdbResultGetText>('csharpdb_result_get_text');
  final resultFree = lib.lookupFunction<CsharpdbResultFreeNative, CsharpdbResultFree>('csharpdb_result_free');
  final close = lib.lookupFunction<CsharpdbCloseNative, CsharpdbClose>('csharpdb_close');

  final path = 'test.db'.toNativeUtf8();
  final db = open(path);
  calloc.free(path);

  // Create table
  final createSql = 'CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)'.toNativeUtf8();
  var r = execute(db, createSql);
  calloc.free(createSql);
  resultFree(r);

  // Insert
  final insertSql = "INSERT INTO users VALUES (1, 'Alice')".toNativeUtf8();
  r = execute(db, insertSql);
  calloc.free(insertSql);
  resultFree(r);

  // Query
  final selectSql = 'SELECT id, name FROM users'.toNativeUtf8();
  r = execute(db, selectSql);
  calloc.free(selectSql);

  while (next(r) == 1) {
    final id = getInt64(r, 0);
    final name = getText(r, 1).toDartString();
    print('  $id: $name');
  }

  resultFree(r);
  close(db);
}
```

---

### Node.js

> **Dedicated TypeScript package available:** For a production-ready TypeScript/Node.js client with
> full type safety, generator-based iteration, and automatic library discovery, see
> [`clients/node/`](../../clients/node/) (`csharpdb` on npm).

Uses [koffi](https://koffi.dev/), a zero-dependency FFI library for Node.js.

#### Setup

```bash
npm init -y
npm install koffi
```

Copy `CSharpDB.Native.dll` (or `.so` / `.dylib`) into your project directory.

#### Reusable wrapper module

```javascript
// csharpdb.js — Node.js wrapper for the CSharpDB native library

const koffi = require("koffi");
const path = require("path");

// Type codes matching CSharpDB.Primitives.DbType
const DbType = { NULL: 0, INTEGER: 1, REAL: 2, TEXT: 3, BLOB: 4 };

function loadLibrary(libPath) {
  if (!libPath) {
    if (process.platform === "win32") libPath = "./CSharpDB.Native.dll";
    else if (process.platform === "darwin") libPath = "./libCSharpDB.Native.dylib";
    else libPath = "./libCSharpDB.Native.so";
  }
  return koffi.load(libPath);
}

class CSharpDB {
  constructor(libPath) {
    const lib = loadLibrary(libPath);

    // Bind all exported functions
    this._open = lib.func("csharpdb_open", "void*", ["str"]);
    this._close = lib.func("csharpdb_close", "void", ["void*"]);
    this._execute = lib.func("csharpdb_execute", "void*", ["void*", "str"]);

    this._resultIsQuery = lib.func("csharpdb_result_is_query", "int", ["void*"]);
    this._resultNext = lib.func("csharpdb_result_next", "int", ["void*"]);
    this._resultRowsAffected = lib.func("csharpdb_result_rows_affected", "int", ["void*"]);
    this._resultColumnCount = lib.func("csharpdb_result_column_count", "int", ["void*"]);
    this._resultColumnName = lib.func("csharpdb_result_column_name", "str", ["void*", "int"]);
    this._resultColumnType = lib.func("csharpdb_result_column_type", "int", ["void*", "int"]);
    this._resultIsNull = lib.func("csharpdb_result_is_null", "int", ["void*", "int"]);
    this._resultGetInt64 = lib.func("csharpdb_result_get_int64", "int64_t", ["void*", "int"]);
    this._resultGetDouble = lib.func("csharpdb_result_get_double", "double", ["void*", "int"]);
    this._resultGetText = lib.func("csharpdb_result_get_text", "str", ["void*", "int"]);
    this._resultFree = lib.func("csharpdb_result_free", "void", ["void*"]);

    this._begin = lib.func("csharpdb_begin", "int", ["void*"]);
    this._commit = lib.func("csharpdb_commit", "int", ["void*"]);
    this._rollback = lib.func("csharpdb_rollback", "int", ["void*"]);

    this._lastError = lib.func("csharpdb_last_error", "str", []);
    this._lastErrorCode = lib.func("csharpdb_last_error_code", "int", []);
    this._clearError = lib.func("csharpdb_clear_error", "void", []);

    this._db = null;
  }

  open(dbPath) {
    this._db = this._open(dbPath);
    if (!this._db) {
      throw new Error(`Failed to open database: ${this._lastError()}`);
    }
  }

  close() {
    if (this._db) {
      this._close(this._db);
      this._db = null;
    }
  }

  /** Execute a non-query statement (INSERT, UPDATE, DELETE, CREATE, etc.). Returns rows affected. */
  execute(sql) {
    const r = this._execute(this._db, sql);
    if (!r) throw new Error(`SQL error: ${this._lastError()}`);
    const affected = this._resultRowsAffected(r);
    this._resultFree(r);
    return affected;
  }

  /** Execute a SELECT and return an array of objects. */
  query(sql) {
    const r = this._execute(this._db, sql);
    if (!r) throw new Error(`Query error: ${this._lastError()}`);

    const colCount = this._resultColumnCount(r);
    const columns = [];
    for (let i = 0; i < colCount; i++) {
      columns.push(this._resultColumnName(r, i) || `col_${i}`);
    }

    const rows = [];
    while (this._resultNext(r) === 1) {
      const row = {};
      for (let i = 0; i < colCount; i++) {
        if (this._resultIsNull(r, i) === 1) {
          row[columns[i]] = null;
          continue;
        }
        const type = this._resultColumnType(r, i);
        switch (type) {
          case DbType.INTEGER:
            // koffi returns BigInt for int64 — convert to Number if safe
            const val = this._resultGetInt64(r, i);
            row[columns[i]] = typeof val === "bigint"
              ? (val >= -Number.MAX_SAFE_INTEGER && val <= Number.MAX_SAFE_INTEGER ? Number(val) : val)
              : val;
            break;
          case DbType.REAL:
            row[columns[i]] = this._resultGetDouble(r, i);
            break;
          case DbType.TEXT:
            row[columns[i]] = this._resultGetText(r, i) || "";
            break;
          default:
            row[columns[i]] = null;
        }
      }
      rows.add ? rows.push(row) : rows.push(row);
    }

    this._resultFree(r);
    return rows;
  }

  /** Execute a SELECT and return the first row, or null. */
  queryOne(sql) {
    const rows = this.query(sql);
    return rows.length > 0 ? rows[0] : null;
  }

  begin() {
    if (this._begin(this._db) !== 0) throw new Error(`Transaction error: ${this._lastError()}`);
  }

  commit() {
    if (this._commit(this._db) !== 0) throw new Error(`Commit error: ${this._lastError()}`);
  }

  rollback() {
    if (this._rollback(this._db) !== 0) throw new Error(`Rollback error: ${this._lastError()}`);
  }

  /** Run a function inside a transaction. Auto-commits on success, rolls back on error. */
  transaction(fn) {
    this.begin();
    try {
      const result = fn(this);
      this.commit();
      return result;
    } catch (err) {
      this.rollback();
      throw err;
    }
  }
}

module.exports = { CSharpDB, DbType };
```

#### Usage

```javascript
const { CSharpDB } = require("./csharpdb");

const db = new CSharpDB();
db.open("books.db");

// Create table
db.execute(`
  CREATE TABLE IF NOT EXISTS books (
    id INTEGER PRIMARY KEY,
    title TEXT,
    year INTEGER,
    rating REAL
  )
`);

// Insert rows inside a transaction
db.transaction(() => {
  db.execute("INSERT INTO books VALUES (1, 'Dune', 1965, 4.8)");
  db.execute("INSERT INTO books VALUES (2, 'Neuromancer', 1984, 4.5)");
  db.execute("INSERT INTO books VALUES (3, 'Snow Crash', 1992, 4.3)");
});

// Query — returns array of objects with typed values
const books = db.query("SELECT id, title, year, rating FROM books ORDER BY year");
for (const book of books) {
  console.log(`  ${book.id}: ${book.title} (${book.year}) — ${book.rating}/5`);
}

// Aggregates
const stats = db.queryOne("SELECT COUNT(*) as total, AVG(rating) as avg_rating FROM books");
console.log(`  ${stats.total} books, avg rating: ${stats.avg_rating.toFixed(1)}`);

db.close();
```

Output:

```
  1: Dune (1965) — 4.8/5
  2: Neuromancer (1984) — 4.5/5
  3: Snow Crash (1992) — 4.3/5
  3 books, avg rating: 4.5
```

#### Express.js REST API example

```javascript
const express = require("express");
const { CSharpDB } = require("./csharpdb");

const app = express();
app.use(express.json());

const db = new CSharpDB();
db.open("api.db");
db.execute(`
  CREATE TABLE IF NOT EXISTS todos (
    id INTEGER PRIMARY KEY,
    title TEXT,
    done INTEGER DEFAULT 0
  )
`);

app.get("/todos", (req, res) => {
  res.json(db.query("SELECT * FROM todos ORDER BY id"));
});

app.post("/todos", (req, res) => {
  const { title } = req.body;
  const count = db.queryOne("SELECT COALESCE(MAX(id), 0) + 1 as next_id FROM todos");
  db.execute(`INSERT INTO todos VALUES (${count.next_id}, '${title.replace(/'/g, "''")}', 0)`);
  res.json({ id: count.next_id, title, done: 0 });
});

app.listen(3000, () => console.log("Listening on :3000"));

process.on("SIGINT", () => { db.close(); process.exit(); });
```

---

## Android Integration

There are two ways to use CSharpDB on Android, depending on what your app is built with.

### Option A: .NET MAUI Android App (No Native Library Needed)

If your Android app uses .NET MAUI, reference the CSharpDB NuGet package directly — the database runs in-process with no FFI or native builds required:

```xml
<!-- In your MAUI .csproj -->
<PackageReference Include="CSharpDB" Version="1.7.0" />
```

```csharp
// Runs on-device, fully offline
var dbPath = Path.Combine(FileSystem.AppDataDirectory, "local.db");
await using var db = await Database.OpenAsync(dbPath);
await db.ExecuteAsync("CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, text TEXT)");
```

### Option B: Kotlin / Java / Flutter / React Native (Native .so)

For non-.NET Android apps, use the NativeAOT-compiled shared library.

#### Step 1: Build the .so for Android

```bash
# ARM64 — most real devices (Pixel, Samsung, etc.)
dotnet publish src/CSharpDB.Native/CSharpDB.Native.csproj -c Release -r linux-bionic-arm64

# x64 — Android emulators
dotnet publish src/CSharpDB.Native/CSharpDB.Native.csproj -c Release -r linux-bionic-x64
```

#### Step 2: Copy the .so into your Android project

Rename the output and place it in the standard `jniLibs` directory:

```
MyAndroidApp/
  app/
    src/main/
      jniLibs/
        arm64-v8a/
          libcsharpdb.so        <- from linux-bionic-arm64 publish
        x86_64/
          libcsharpdb.so        <- from linux-bionic-x64 publish
```

> **Important:** The file must be named `libcsharpdb.so` (with the `lib` prefix).
> Android's linker requires shared libraries to follow the `lib<name>.so` convention.

#### Step 3a: Kotlin / Java with JNA

Add the JNA dependency:

```kotlin
// build.gradle.kts
dependencies {
    implementation("net.java.dev.jna:jna:5.14.0@aar")
}
```

Define the interface:

```kotlin
import com.sun.jna.Library
import com.sun.jna.Native
import com.sun.jna.Pointer

interface CSharpDB : Library {
    companion object {
        val INSTANCE: CSharpDB = Native.load("csharpdb", CSharpDB::class.java)
    }

    fun csharpdb_open(path: String): Pointer?
    fun csharpdb_close(db: Pointer)
    fun csharpdb_execute(db: Pointer, sql: String): Pointer?
    fun csharpdb_result_next(result: Pointer): Int
    fun csharpdb_result_rows_affected(result: Pointer): Int
    fun csharpdb_result_column_count(result: Pointer): Int
    fun csharpdb_result_column_name(result: Pointer, col: Int): String?
    fun csharpdb_result_column_type(result: Pointer, col: Int): Int
    fun csharpdb_result_is_null(result: Pointer, col: Int): Int
    fun csharpdb_result_get_int64(result: Pointer, col: Int): Long
    fun csharpdb_result_get_double(result: Pointer, col: Int): Double
    fun csharpdb_result_get_text(result: Pointer, col: Int): String?
    fun csharpdb_result_free(result: Pointer)
    fun csharpdb_begin(db: Pointer): Int
    fun csharpdb_commit(db: Pointer): Int
    fun csharpdb_rollback(db: Pointer): Int
    fun csharpdb_last_error(): String?
    fun csharpdb_last_error_code(): Int
    fun csharpdb_clear_error()
}
```

Use it from an Activity, Fragment, or ViewModel:

```kotlin
class NotesRepository(private val context: Context) {
    private val lib = CSharpDB.INSTANCE
    private val db: Pointer

    init {
        val dbPath = "${context.filesDir}/notes.db"
        db = lib.csharpdb_open(dbPath)
            ?: error("Failed to open database: ${lib.csharpdb_last_error()}")

        val r = lib.csharpdb_execute(db,
            "CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
            ?: error("Failed to create table: ${lib.csharpdb_last_error()}")
        lib.csharpdb_result_free(r)
    }

    fun addNote(title: String, body: String) {
        // Use single quotes escaped for SQL; in production use parameterized queries
        val sql = "INSERT INTO notes (title, body) VALUES ('${title}', '${body}')"
        val r = lib.csharpdb_execute(db, sql)
            ?: error("Insert failed: ${lib.csharpdb_last_error()}")
        lib.csharpdb_result_free(r)
    }

    fun getAllNotes(): List<Triple<Long, String, String>> {
        val notes = mutableListOf<Triple<Long, String, String>>()
        val r = lib.csharpdb_execute(db, "SELECT id, title, body FROM notes ORDER BY id DESC")
            ?: error("Query failed: ${lib.csharpdb_last_error()}")

        while (lib.csharpdb_result_next(r) == 1) {
            val id = lib.csharpdb_result_get_int64(r, 0)
            val title = lib.csharpdb_result_get_text(r, 1) ?: ""
            val body = lib.csharpdb_result_get_text(r, 2) ?: ""
            notes.add(Triple(id, title, body))
        }

        lib.csharpdb_result_free(r)
        return notes
    }

    fun close() {
        lib.csharpdb_close(db)
    }
}
```

#### Step 3b: Flutter with dart:ffi

Place the `.so` file in your Flutter project:

```
my_flutter_app/
  android/
    app/
      src/main/
        jniLibs/
          arm64-v8a/
            libcsharpdb.so
          x86_64/
            libcsharpdb.so
```

Create a Dart wrapper:

```dart
import 'dart:ffi';
import 'dart:io';
import 'package:ffi/ffi.dart';

class CSharpDB {
  late final DynamicLibrary _lib;

  // Function typedefs
  late final Pointer<Void> Function(Pointer<Utf8>) _open;
  late final void Function(Pointer<Void>) _close;
  late final Pointer<Void> Function(Pointer<Void>, Pointer<Utf8>) _execute;
  late final int Function(Pointer<Void>) _resultNext;
  late final int Function(Pointer<Void>) _resultRowsAffected;
  late final int Function(Pointer<Void>) _resultColumnCount;
  late final Pointer<Utf8> Function(Pointer<Void>, int) _resultGetText;
  late final int Function(Pointer<Void>, int) _resultGetInt64;
  late final double Function(Pointer<Void>, int) _resultGetDouble;
  late final int Function(Pointer<Void>, int) _resultIsNull;
  late final void Function(Pointer<Void>) _resultFree;
  late final Pointer<Utf8> Function() _lastError;

  CSharpDB() {
    _lib = Platform.isAndroid
        ? DynamicLibrary.open('libcsharpdb.so')
        : DynamicLibrary.open('CSharpDB.Native.dll');

    _open = _lib.lookupFunction<Pointer<Void> Function(Pointer<Utf8>),
        Pointer<Void> Function(Pointer<Utf8>)>('csharpdb_open');
    _close = _lib.lookupFunction<Void Function(Pointer<Void>),
        void Function(Pointer<Void>)>('csharpdb_close');
    _execute = _lib.lookupFunction<
        Pointer<Void> Function(Pointer<Void>, Pointer<Utf8>),
        Pointer<Void> Function(Pointer<Void>, Pointer<Utf8>)>('csharpdb_execute');
    _resultNext = _lib.lookupFunction<Int32 Function(Pointer<Void>),
        int Function(Pointer<Void>)>('csharpdb_result_next');
    _resultRowsAffected = _lib.lookupFunction<Int32 Function(Pointer<Void>),
        int Function(Pointer<Void>)>('csharpdb_result_rows_affected');
    _resultColumnCount = _lib.lookupFunction<Int32 Function(Pointer<Void>),
        int Function(Pointer<Void>)>('csharpdb_result_column_count');
    _resultGetText = _lib.lookupFunction<
        Pointer<Utf8> Function(Pointer<Void>, Int32),
        Pointer<Utf8> Function(Pointer<Void>, int)>('csharpdb_result_get_text');
    _resultGetInt64 = _lib.lookupFunction<Int64 Function(Pointer<Void>, Int32),
        int Function(Pointer<Void>, int)>('csharpdb_result_get_int64');
    _resultGetDouble = _lib.lookupFunction<
        Double Function(Pointer<Void>, Int32),
        double Function(Pointer<Void>, int)>('csharpdb_result_get_double');
    _resultIsNull = _lib.lookupFunction<Int32 Function(Pointer<Void>, Int32),
        int Function(Pointer<Void>, int)>('csharpdb_result_is_null');
    _resultFree = _lib.lookupFunction<Void Function(Pointer<Void>),
        void Function(Pointer<Void>)>('csharpdb_result_free');
    _lastError = _lib.lookupFunction<Pointer<Utf8> Function(),
        Pointer<Utf8> Function()>('csharpdb_last_error');
  }

  late Pointer<Void> _db;

  void open(String path) {
    final pathPtr = path.toNativeUtf8();
    _db = _open(pathPtr);
    calloc.free(pathPtr);
    if (_db == nullptr) {
      throw Exception('Failed to open database: ${_lastError().toDartString()}');
    }
  }

  int execute(String sql) {
    final sqlPtr = sql.toNativeUtf8();
    final result = _execute(_db, sqlPtr);
    calloc.free(sqlPtr);
    if (result == nullptr) {
      throw Exception('SQL error: ${_lastError().toDartString()}');
    }
    final affected = _resultRowsAffected(result);
    _resultFree(result);
    return affected;
  }

  List<Map<String, dynamic>> query(String sql) {
    final sqlPtr = sql.toNativeUtf8();
    final result = _execute(_db, sqlPtr);
    calloc.free(sqlPtr);
    if (result == nullptr) {
      throw Exception('Query error: ${_lastError().toDartString()}');
    }

    final rows = <Map<String, dynamic>>[];
    final colCount = _resultColumnCount(result);

    while (_resultNext(result) == 1) {
      final row = <String, dynamic>{};
      for (var i = 0; i < colCount; i++) {
        if (_resultIsNull(result, i) == 1) {
          row['col_$i'] = null;
        } else {
          row['col_$i'] = _resultGetText(result, i).toDartString();
        }
      }
      rows.add(row);
    }

    _resultFree(result);
    return rows;
  }

  void close() => _close(_db);
}
```

Usage in Flutter:

```dart
final db = CSharpDB();
db.open('${(await getApplicationDocumentsDirectory()).path}/app.db');
db.execute('CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)');
db.execute("INSERT INTO items VALUES (1, 'Widget')");
final rows = db.query('SELECT * FROM items');
print(rows); // [{col_0: 1, col_1: Widget}]
db.close();
```

#### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        Android Device                           │
│                                                                 │
│  ┌──────────────────────┐     ┌──────────────────────────────┐  │
│  │ Kotlin / Flutter App │     │ jniLibs/arm64-v8a/           │  │
│  │                      │────►│   libcsharpdb.so             │  │
│  │ JNA / dart:ffi call  │     │   (NativeAOT, no .NET CLR)   │  │
│  └──────────────────────┘     └──────────────┬───────────────┘  │
│                                              │                  │
│                                              ▼                  │
│                                   ┌──────────────────┐          │
│                                   │ /data/data/app/  │          │
│                                   │   myapp.db       │          │
│                                   │   myapp.db.wal   │          │
│                                   └──────────────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

#### Notes

- The `.so` is self-contained — no .NET runtime is installed on the device.
- The database file is stored in the app's private data directory (`context.filesDir` or `getApplicationDocumentsDirectory()`).
- ARM64 covers virtually all modern Android phones (2017+). Add `x86_64` only if you need emulator support.
- The library size for Android ARM64 is typically 8-15 MB.

---

## Language Client Packages

Pre-built client packages that wrap the native library with idiomatic APIs:

| Language | Location | Features |
|----------|----------|----------|
| **Node.js / TypeScript** | [`clients/node/`](../../clients/node/) | Full TypeScript types, generator iteration, auto library discovery, transaction helper |

---

## Thread Safety

- **Different database handles** can be used concurrently from different threads.
- A **single database handle** must NOT be used from multiple threads simultaneously. The underlying engine uses single-writer semantics.
- **Error state** is per-thread (`csharpdb_last_error` will not be overwritten by another thread).
- For concurrent read access, open multiple database handles pointing to the same file. CSharpDB's WAL mode supports concurrent readers.

---

## Memory Management

**Rules:**

1. Every `csharpdb_open` must be paired with `csharpdb_close`.
2. Every `csharpdb_execute` must be paired with `csharpdb_result_free`.
3. String pointers from `csharpdb_result_get_text` and `csharpdb_result_column_name` are owned by the library. **Do not free them.**
4. Blob pointers from `csharpdb_result_get_blob` are owned by the library. **Do not free them.**
5. Text and blob pointers from row values are valid only until the next `csharpdb_result_next` call or `csharpdb_result_free`. Copy the data if you need it longer.
6. Column name pointers are valid until `csharpdb_result_free`.

---

## Troubleshooting

### "Platform linker not found" on Windows

Install the "Desktop development with C++" workload in Visual Studio:

```
vs_buildtools.exe --add Microsoft.VisualStudio.Workload.VCTools
```

### "clang not found" on Linux

```bash
sudo apt install clang   # Debian/Ubuntu
sudo dnf install clang   # Fedora/RHEL
```

### Library not found at runtime

Make sure the shared library is in one of:

- The same directory as your executable
- A directory in `LD_LIBRARY_PATH` (Linux) or `DYLD_LIBRARY_PATH` (macOS)
- A system library directory (`/usr/local/lib`, etc.)

On Windows, ensure the DLL is in the same folder as the `.exe` or in `PATH`.

### "Entry point not found"

Verify the library was published with NativeAOT:

```bash
# Linux/macOS
nm -D libCSharpDB.Native.so | grep csharpdb_

# Windows (from VS Developer Command Prompt)
dumpbin /exports CSharpDB.Native.dll
```

You should see all `csharpdb_*` symbols listed.

### Large binary size

The NativeAOT binary includes the .NET runtime and GC. To reduce size:

```bash
# Strip debug symbols (default in Release with StripSymbols=true)
dotnet publish -c Release -r linux-x64 -p:StripSymbols=true

# Aggressive trimming (may break reflection-dependent code)
dotnet publish -c Release -r linux-x64 -p:OptimizationPreference=Size
```
