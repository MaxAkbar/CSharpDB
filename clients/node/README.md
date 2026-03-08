# csharpdb — Node.js Client

TypeScript/JavaScript client for [CSharpDB](https://github.com/MaxAkbar/CSharpDB) embedded database, powered by the NativeAOT shared library via FFI.

## Prerequisites

1. **Publish the native library** (requires .NET 10 SDK + C++ toolchain):

   ```bash
   # Windows
   dotnet publish src/CSharpDB.Native/CSharpDB.Native.csproj -c Release -r win-x64

   # Linux
   dotnet publish src/CSharpDB.Native/CSharpDB.Native.csproj -c Release -r linux-x64

   # macOS
   dotnet publish src/CSharpDB.Native/CSharpDB.Native.csproj -c Release -r osx-arm64
   ```

2. **Place the library** where the client can find it:

   ```bash
   mkdir native
   # Copy the published library to ./native/
   # Windows: CSharpDB.Native.dll
   # Linux:   CSharpDB.Native.so
   # macOS:   CSharpDB.Native.dylib
   ```

   Or set the `CSHARPDB_NATIVE_PATH` environment variable to the full path.

## Install

```bash
npm install csharpdb
```

## Quick Start

```ts
import { Database } from 'csharpdb';

const db = new Database('myapp.db');

// Create tables
db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');

// Insert data
db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')");

// Query rows (returns objects with typed values)
const users = db.query('SELECT * FROM users');
console.log(users);
// [{ id: 1n, name: 'Alice', email: 'alice@example.com' }]

// Query single row
const user = db.queryOne('SELECT * FROM users WHERE id = 1');
console.log(user?.name); // 'Alice'

db.close();
```

## API Reference

### `new Database(path, options?)`

Open or create a database file.

| Parameter | Type | Description |
|-----------|------|-------------|
| `path` | `string` | Database file path |
| `options.nativeLibraryPath` | `string?` | Explicit path to the native library |

### `db.execute(sql): ExecResult`

Execute a non-query statement (INSERT, UPDATE, DELETE, CREATE TABLE, etc.).

Returns `{ rowsAffected: number }`.

### `db.query(sql): Row[]`

Execute a SELECT and return all rows as an array of objects.

**Value type mapping:**

| CSharpDB Type | JavaScript Type |
|---------------|-----------------|
| INTEGER | `bigint` |
| REAL | `number` |
| TEXT | `string` |
| BLOB | `Buffer` |
| NULL | `null` |

### `db.queryOne(sql): Row | null`

Execute a SELECT and return the first row, or `null` if empty.

### `db.iterate(sql): Generator<Row>`

Execute a SELECT and yield rows one at a time via a generator. More memory-efficient for large result sets.

```ts
for (const row of db.iterate('SELECT * FROM large_table')) {
  process.stdout.write(`${row.id}\n`);
}
```

### `db.columns(sql): ColumnInfo[]`

Get column metadata without consuming rows.

### `db.transaction<T>(fn: () => T): T`

Run a function inside an explicit transaction. Commits on success, rolls back on error.

```ts
db.transaction(() => {
  db.execute("INSERT INTO orders VALUES (1, 'pending')");
  db.execute("INSERT INTO items VALUES (1, 1, 'Widget', 2)");
});
```

### `db.close()`

Close the database. Safe to call multiple times.

### `db.isOpen: boolean`

Returns `true` if the database connection is open.

## Error Handling

All errors throw `CSharpDBError` with a `code` property:

```ts
import { CSharpDBError } from 'csharpdb';

try {
  db.execute('INVALID SQL');
} catch (err) {
  if (err instanceof CSharpDBError) {
    console.error(`Error ${err.code}: ${err.message}`);
  }
}
```

## Native Library Search Order

1. Explicit path via `options.nativeLibraryPath`
2. `CSHARPDB_NATIVE_PATH` environment variable
3. `./native/CSharpDB.Native.{dll,so,dylib}`
4. Current working directory

## Building from Source

```bash
cd clients/node
npm install
npm run build    # Compile TypeScript
npm test         # Run tests (requires native library)
npm run example  # Run basic example
```

## License

MIT
