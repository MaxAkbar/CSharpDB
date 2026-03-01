# Getting Started with CSharpDB

This tutorial walks you through using CSharpDB from opening a database to running queries and transactions.

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)

## Setup

Add a project reference to `CSharpDB.Engine` (which transitively pulls in all other projects):

```bash
dotnet add reference path/to/src/CSharpDB.Engine/CSharpDB.Engine.csproj
```

Then add the using directive:

```csharp
using CSharpDB.Engine;
```

---

## 1. Opening a Database

`Database.OpenAsync` opens an existing database file or creates a new one if it doesn't exist:

```csharp
await using var db = await Database.OpenAsync("myapp.db");
```

The `await using` pattern ensures the database is properly closed when you're done. On open, if a WAL (Write-Ahead Log) file exists from a previous crash, the database automatically recovers committed data.

---

## 2. Creating Tables

```csharp
await db.ExecuteAsync(@"
    CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        price REAL,
        category TEXT
    )
");
```

Supported column types:
- `INTEGER` — 64-bit signed integer (`long`)
- `REAL` — 64-bit floating point (`double`)
- `TEXT` — UTF-8 string
- `BLOB` — raw byte array

Column constraints:
- `PRIMARY KEY` — marks the column as the primary key
- `NOT NULL` — disallows NULL values

Use `IF NOT EXISTS` to avoid errors when the table already exists:

```csharp
await db.ExecuteAsync("CREATE TABLE IF NOT EXISTS products (id INTEGER, name TEXT)");
```

---

## 3. Inserting Data

Insert with all columns:

```csharp
await db.ExecuteAsync("INSERT INTO products VALUES (1, 'Widget', 9.99, 'Hardware')");
await db.ExecuteAsync("INSERT INTO products VALUES (2, 'Gadget', 29.99, 'Electronics')");
await db.ExecuteAsync("INSERT INTO products VALUES (3, 'Doohickey', 4.99, 'Hardware')");
```

Insert with named columns (unspecified columns get NULL):

```csharp
await db.ExecuteAsync("INSERT INTO products (id, name) VALUES (4, 'Thingamajig')");
```

The return value of `ExecuteAsync` for DML statements includes a `RowsAffected` count:

```csharp
var result = await db.ExecuteAsync("INSERT INTO products VALUES (5, 'Gizmo', 14.99, 'Electronics')");
Console.WriteLine($"Inserted {result.RowsAffected} row(s)"); // Inserted 1 row(s)
```

---

## 4. Querying Data

### SELECT all rows

```csharp
await using var result = await db.ExecuteAsync("SELECT * FROM products");

await foreach (var row in result.GetRowsAsync())
{
    long id = row[0].AsInteger;
    string name = row[1].AsText;
    Console.WriteLine($"  {id}: {name}");
}
```

> **Important:** Use `await using` on SELECT results to properly dispose the underlying operator chain.

### Materialize all rows at once

If you want all rows in a list instead of streaming:

```csharp
await using var result = await db.ExecuteAsync("SELECT * FROM products");
var rows = await result.ToListAsync();

Console.WriteLine($"Got {rows.Count} products");
```

### WHERE clause

```csharp
await using var result = await db.ExecuteAsync(
    "SELECT * FROM products WHERE price > 10.0 AND category = 'Electronics'");
```

Supported operators in WHERE: `=`, `<>`, `<`, `>`, `<=`, `>=`, `AND`, `OR`, `NOT`, `+`, `-`, `*`, `/`, `LIKE`, `IN`, `BETWEEN`, `IS NULL`, `IS NOT NULL`

### LIKE pattern matching

```csharp
// % matches any sequence, _ matches any single character
await using var result = await db.ExecuteAsync(
    "SELECT * FROM products WHERE name LIKE 'Gad%'");
```

### IN lists

```csharp
await using var result = await db.ExecuteAsync(
    "SELECT * FROM products WHERE category IN ('Hardware', 'Electronics')");
```

### BETWEEN ranges

```csharp
await using var result = await db.ExecuteAsync(
    "SELECT * FROM products WHERE price BETWEEN 5.0 AND 20.0");
```

### ORDER BY

```csharp
await using var result = await db.ExecuteAsync(
    "SELECT * FROM products ORDER BY price DESC");
```

You can sort by multiple columns and mix `ASC`/`DESC`:

```csharp
await using var result = await db.ExecuteAsync(
    "SELECT * FROM products ORDER BY category ASC, price DESC");
```

### LIMIT and OFFSET

```csharp
await using var result = await db.ExecuteAsync(
    "SELECT * FROM products ORDER BY price DESC LIMIT 3");

// Skip the first 5, then take the next 10
await using var paged = await db.ExecuteAsync(
    "SELECT * FROM products ORDER BY id LIMIT 10 OFFSET 5");
```

### Select specific columns

```csharp
await using var result = await db.ExecuteAsync(
    "SELECT name, price FROM products WHERE price > 5.0");
```

### System catalog metadata (`sys.*`)

You can inspect tables, columns, indexes, views, and triggers with SQL:

```csharp
await using var tables = await db.ExecuteAsync(
    "SELECT table_name, column_count, primary_key_column FROM sys.tables ORDER BY table_name");

await using var columns = await db.ExecuteAsync(
    "SELECT column_name, data_type, is_nullable FROM sys.columns " +
    "WHERE table_name = 'products' ORDER BY ordinal_position");
```

Catalog sources:
- `sys.tables`
- `sys.columns`
- `sys.indexes`
- `sys.views`
- `sys.triggers`

Underscored aliases are also available (`sys_tables`, `sys_columns`, etc.).

---

## 5. Aggregate Functions

```csharp
// COUNT
await using var r1 = await db.ExecuteAsync("SELECT COUNT(*) FROM products");

// SUM, AVG, MIN, MAX
await using var r2 = await db.ExecuteAsync(
    "SELECT category, COUNT(*), AVG(price), MIN(price), MAX(price) FROM products GROUP BY category");

// HAVING
await using var r3 = await db.ExecuteAsync(
    "SELECT category, COUNT(*) as cnt FROM products GROUP BY category HAVING cnt > 1");
```

Supported aggregate functions: `COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)`, `SUM`, `AVG`, `MIN`, `MAX`

---

## 6. JOINs

```csharp
await db.ExecuteAsync("CREATE TABLE orders (id INTEGER, product_id INTEGER, qty INTEGER)");
await db.ExecuteAsync("INSERT INTO orders VALUES (1, 1, 10)");
await db.ExecuteAsync("INSERT INTO orders VALUES (2, 2, 5)");

// INNER JOIN
await using var result = await db.ExecuteAsync(@"
    SELECT p.name, o.qty
    FROM products p
    INNER JOIN orders o ON p.id = o.product_id");

// LEFT JOIN (all products, even those without orders)
await using var result2 = await db.ExecuteAsync(@"
    SELECT p.name, o.qty
    FROM products p
    LEFT JOIN orders o ON p.id = o.product_id");
```

Supported join types: `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `CROSS JOIN`

---

## 7. Updating Rows

```csharp
var result = await db.ExecuteAsync(
    "UPDATE products SET price = 12.99 WHERE name = 'Widget'");

Console.WriteLine($"Updated {result.RowsAffected} row(s)");
```

You can update multiple columns at once:

```csharp
await db.ExecuteAsync(
    "UPDATE products SET price = 19.99, category = 'Premium' WHERE id = 2");
```

---

## 8. Deleting Rows

```csharp
var result = await db.ExecuteAsync("DELETE FROM products WHERE category = 'Hardware'");
Console.WriteLine($"Deleted {result.RowsAffected} row(s)");
```

Delete all rows (no WHERE clause):

```csharp
await db.ExecuteAsync("DELETE FROM products");
```

---

## 9. Working with NULL

Insert a NULL value explicitly:

```csharp
await db.ExecuteAsync("INSERT INTO products VALUES (10, 'Mystery', NULL, NULL)");
```

Check for NULL when reading:

```csharp
await using var result = await db.ExecuteAsync("SELECT * FROM products");
await foreach (var row in result.GetRowsAsync())
{
    if (row[2].IsNull)
        Console.WriteLine($"{row[1].AsText}: no price set");
    else
        Console.WriteLine($"{row[1].AsText}: ${row[2].AsReal}");
}
```

Filter with IS NULL / IS NOT NULL:

```csharp
await using var result = await db.ExecuteAsync(
    "SELECT * FROM products WHERE price IS NOT NULL");
```

---

## 10. Transactions

By default, each DML/DDL statement auto-commits. For multi-statement atomicity, use explicit transactions:

```csharp
await db.BeginTransactionAsync();
try
{
    await db.ExecuteAsync("INSERT INTO products VALUES (20, 'Item A', 5.00, 'Batch')");
    await db.ExecuteAsync("INSERT INTO products VALUES (21, 'Item B', 7.50, 'Batch')");
    await db.ExecuteAsync("INSERT INTO products VALUES (22, 'Item C', 3.25, 'Batch')");

    await db.CommitAsync(); // All three inserts are now durable
}
catch
{
    await db.RollbackAsync(); // None of the inserts are persisted
    throw;
}
```

Transactions also improve performance for bulk inserts since dirty pages are written to the WAL only once rather than per-statement.

### Rollback example

```csharp
await db.BeginTransactionAsync();
await db.ExecuteAsync("DELETE FROM products"); // All rows deleted in memory
await db.RollbackAsync();                      // Changes discarded — all rows restored
```

---

## 11. Indexes

Create indexes to speed up equality lookups:

```csharp
// Regular index
await db.ExecuteAsync("CREATE INDEX idx_category ON products (category)");

// Unique index (enforces uniqueness)
await db.ExecuteAsync("CREATE UNIQUE INDEX idx_name ON products (name)");
```

When a WHERE clause contains `column = value` on an indexed column, the query planner automatically uses the index instead of a full table scan.

---

## 12. ALTER TABLE

```csharp
// Add a column
await db.ExecuteAsync("ALTER TABLE products ADD COLUMN weight REAL");

// Drop a column
await db.ExecuteAsync("ALTER TABLE products DROP COLUMN weight");

// Rename a column
await db.ExecuteAsync("ALTER TABLE products RENAME COLUMN category TO department");

// Rename a table
await db.ExecuteAsync("ALTER TABLE products RENAME TO inventory");
```

---

## 13. Views

Views are named, reusable queries:

```csharp
await db.ExecuteAsync(@"
    CREATE VIEW expensive_products AS
    SELECT name, price FROM products WHERE price > 20.0");

// Query the view like a table
await using var result = await db.ExecuteAsync("SELECT * FROM expensive_products");
```

---

## 14. Common Table Expressions (CTEs)

```csharp
await using var result = await db.ExecuteAsync(@"
    WITH high_value AS (
        SELECT * FROM products WHERE price > 10.0
    )
    SELECT name, price FROM high_value ORDER BY price DESC");
```

---

## 15. Triggers

Triggers execute SQL automatically when data changes:

```csharp
await db.ExecuteAsync("CREATE TABLE audit_log (action TEXT, product_name TEXT)");

await db.ExecuteAsync(@"
    CREATE TRIGGER log_insert
    AFTER INSERT ON products
    BEGIN
        INSERT INTO audit_log VALUES ('INSERT', NEW.name);
    END");

// Now inserting into products automatically logs to audit_log
await db.ExecuteAsync("INSERT INTO products VALUES (50, 'Auto-logged', 9.99, 'Test')");
```

Supported trigger types:
- `BEFORE INSERT`, `AFTER INSERT`
- `BEFORE UPDATE`, `AFTER UPDATE`
- `BEFORE DELETE`, `AFTER DELETE`

Use `NEW.column` in INSERT/UPDATE triggers and `OLD.column` in UPDATE/DELETE triggers.

---

## 16. Concurrent Readers

Create reader sessions for snapshot-isolated reads that don't block writes:

```csharp
// Take a snapshot of the current database state
using var reader = db.CreateReaderSession();

// Writer can continue modifying data — reader won't see changes
await db.ExecuteAsync("INSERT INTO products VALUES (99, 'New item', 1.0, 'Test')");

// Reader sees the database as it was when the snapshot was taken
await using var result = await reader.ExecuteReadAsync("SELECT COUNT(*) FROM products");
```

Multiple reader sessions can be active simultaneously.

---

## 17. Dropping Tables

```csharp
await db.ExecuteAsync("DROP TABLE products");
```

Use `IF EXISTS` to avoid errors:

```csharp
await db.ExecuteAsync("DROP TABLE IF EXISTS products");
```

---

## 18. Error Handling

CSharpDB throws `CSharpDbException` with a typed `ErrorCode`:

```csharp
using CSharpDB.Core;

try
{
    await db.ExecuteAsync("SELECT * FROM nonexistent");
}
catch (CSharpDbException ex) when (ex.Code == ErrorCode.TableNotFound)
{
    Console.WriteLine($"Table not found: {ex.Message}");
}
catch (CSharpDbException ex) when (ex.Code == ErrorCode.SyntaxError)
{
    Console.WriteLine($"SQL syntax error: {ex.Message}");
}
```

Error codes:

| Code | Meaning |
|------|---------|
| `TableNotFound` | Referenced table doesn't exist |
| `TableAlreadyExists` | CREATE TABLE on an existing table (without IF NOT EXISTS) |
| `ColumnNotFound` | Referenced column doesn't exist in the table |
| `DuplicateKey` | INSERT with a rowid that already exists |
| `SyntaxError` | Invalid SQL syntax |
| `TypeMismatch` | Value type doesn't match expected type |
| `ConstraintViolation` | NOT NULL, UNIQUE, or other constraint violated |
| `IoError` | File system read/write failure |
| `CorruptDatabase` | Database file structure is invalid |
| `WalError` | Error reading/writing the WAL file |
| `Busy` | Could not acquire write lock (another writer is active) |

---

## 19. Using the ADO.NET Provider

For standard .NET data access patterns, use the `CSharpDB.Data` package:

```csharp
using CSharpDB.Data;

await using var conn = new CSharpDbConnection("Data Source=myapp.db");
await conn.OpenAsync();

// Parameterized queries
using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT * FROM products WHERE price > @minPrice";
cmd.Parameters.AddWithValue("@minPrice", 10.0);

await using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    long id = reader.GetInt64(0);
    string name = reader.GetString(1);
    double price = reader.GetDouble(2);
    Console.WriteLine($"{id}: {name} (${price})");
}
```

### ExecuteScalar

```csharp
cmd.CommandText = "SELECT COUNT(*) FROM products";
var count = await cmd.ExecuteScalarAsync();
```

### ExecuteNonQuery

```csharp
cmd.CommandText = "INSERT INTO products VALUES (100, 'New', 5.99, 'Test')";
int rowsAffected = await cmd.ExecuteNonQueryAsync();
```

### Transactions via ADO.NET

```csharp
await using var txn = await conn.BeginTransactionAsync();
try
{
    using var cmd = conn.CreateCommand();
    cmd.Transaction = (CSharpDbTransaction)txn;
    cmd.CommandText = "INSERT INTO products VALUES (200, 'Txn Item', 1.0, 'Test')";
    await cmd.ExecuteNonQueryAsync();
    await txn.CommitAsync();
}
catch
{
    await txn.RollbackAsync();
    throw;
}
```

---

## 20. Data Persistence

Data survives application restarts — it's written to disk via the WAL on commit:

```csharp
// Session 1: Create and populate
await using (var db = await Database.OpenAsync("persistent.db"))
{
    await db.ExecuteAsync("CREATE TABLE notes (id INTEGER, text TEXT)");
    await db.ExecuteAsync("INSERT INTO notes VALUES (1, 'Remember this')");
}

// Session 2: Data is still there
await using (var db = await Database.OpenAsync("persistent.db"))
{
    await using var result = await db.ExecuteAsync("SELECT * FROM notes");
    var rows = await result.ToListAsync();
    Console.WriteLine(rows[0][1].AsText); // "Remember this"
}
```

---

## Next Steps

- [Architecture Guide](architecture.md) — How the engine works layer by layer
- [Internals & Contributing](internals.md) — How to extend the engine, testing strategy
- [REST API Reference](rest-api.md) — Use CSharpDB over HTTP from any language
- [MCP Server Reference](mcp-server.md) — Connect AI assistants to your database
- [CLI Reference](cli.md) — Interactive REPL with meta-commands
- [FAQ](faq.md) — Common setup and troubleshooting answers
- [Roadmap](roadmap.md) — Planned features and project direction
