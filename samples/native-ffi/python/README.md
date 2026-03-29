# Python FFI Tutorial

Use CSharpDB from Python via the built-in `ctypes` module. No external packages required.

## Files

| File | Description |
|------|-------------|
| `csharpdb.py` | Reusable wrapper module with typed query results, transactions, and context manager |
| `example_crud.py` | Create, read, update, delete operations |
| `example_transactions.py` | Explicit and automatic transactions, rollback on error |

## Prerequisites

1. Python 3.10+
2. The CSharpDB native library (see [build instructions](../README.md))

## Quick Start

1. **Set the library path** (or edit the `LIB_PATH` in the example files):

   ```bash
   # Windows
   set CSHARPDB_NATIVE_PATH=C:\path\to\CSharpDB.Native.dll

   # Linux / macOS
   export CSHARPDB_NATIVE_PATH=/path/to/CSharpDB.Native.so
   ```

2. **Run the examples**:

   ```bash
   cd samples/native-ffi/python

   python example_crud.py
   python example_transactions.py
   ```

## Using the Wrapper in Your Project

Copy `csharpdb.py` into your project and use it:

```python
from csharpdb import CSharpDB

with CSharpDB("/path/to/CSharpDB.Native.dll") as db:
    db.open("myapp.db")

    db.execute("CREATE TABLE items (id INTEGER, name TEXT, price REAL)")
    db.execute("INSERT INTO items VALUES (1, 'Widget', 9.99)")

    for row in db.query("SELECT * FROM items"):
        print(row)  # {'id': 1, 'name': 'Widget', 'price': 9.99}
```

## API Summary

| Method | Description |
|--------|-------------|
| `CSharpDB(lib_path)` | Load the native library |
| `db.open(path)` | Open or create a database |
| `db.close()` | Close the database |
| `db.execute(sql)` | Run INSERT/UPDATE/DELETE/DDL, returns rows affected |
| `db.query(sql)` | Run SELECT, returns `list[dict]` |
| `db.query_one(sql)` | Run SELECT, returns first row or `None` |
| `db.begin()` | Start a transaction |
| `db.commit()` | Commit the transaction |
| `db.rollback()` | Rollback the transaction |
| `db.transaction(fn)` | Run `fn` in a transaction with auto commit/rollback |

The class also supports `with` statements for automatic cleanup.
