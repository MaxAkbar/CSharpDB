# JavaScript / Node.js FFI Tutorial

Use CSharpDB from Node.js via [koffi](https://koffi.dev/), a zero-dependency FFI library.

## Files

| File | Description |
|------|-------------|
| `csharpdb.mjs` | Reusable ESM wrapper module with typed queries and transactions |
| `example_crud.mjs` | Create, read, update, delete operations |
| `example_transactions.mjs` | Explicit and automatic transactions, rollback on error |
| `package.json` | npm project with koffi dependency |

## Prerequisites

1. Node.js 18+
2. The CSharpDB native library (see [build instructions](../README.md))

## Quick Start

1. **Install dependencies**:

   ```bash
   cd samples/native-ffi/javascript
   npm install
   ```

2. **Set the library path** (or edit the `LIB_PATH` in the example files):

   ```bash
   # Windows
   set CSHARPDB_NATIVE_PATH=C:\path\to\CSharpDB.Native.dll

   # Linux / macOS
   export CSHARPDB_NATIVE_PATH=/path/to/CSharpDB.Native.so
   ```

3. **Run the examples**:

   ```bash
   node example_crud.mjs
   node example_transactions.mjs
   ```

   Or use npm scripts:

   ```bash
   npm run crud
   npm run transactions
   ```

## Using the Wrapper in Your Project

Copy `csharpdb.mjs` into your project and use it:

```javascript
import { CSharpDB } from './csharpdb.mjs';

const db = new CSharpDB('/path/to/CSharpDB.Native.dll');
db.open('myapp.db');

db.execute('CREATE TABLE items (id INTEGER, name TEXT, price REAL)');
db.execute("INSERT INTO items VALUES (1, 'Widget', 9.99)");

const rows = db.query('SELECT * FROM items');
console.log(rows);  // [{ id: 1, name: 'Widget', price: 9.99 }]

db.close();
```

For a production-ready TypeScript package with full type safety, see [`clients/node/`](../../../../clients/node/).

## API Summary

| Method | Description |
|--------|-------------|
| `new CSharpDB(libPath)` | Load the native library |
| `db.open(path)` | Open or create a database |
| `db.close()` | Close the database |
| `db.execute(sql)` | Run INSERT/UPDATE/DELETE/DDL, returns rows affected |
| `db.query(sql)` | Run SELECT, returns array of objects |
| `db.queryOne(sql)` | Run SELECT, returns first row or `null` |
| `db.begin()` | Start a transaction |
| `db.commit()` | Commit the transaction |
| `db.rollback()` | Rollback the transaction |
| `db.transaction(fn)` | Run `fn` in a transaction with auto commit/rollback |

## Value Types

| CSharpDB Type | JavaScript Type |
|---------------|-----------------|
| INTEGER | `Number` (auto-converted from BigInt if safe) |
| REAL | `Number` |
| TEXT | `String` |
| NULL | `null` |
