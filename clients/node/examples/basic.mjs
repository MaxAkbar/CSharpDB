/**
 * Basic CSharpDB usage from Node.js.
 *
 * Prerequisites:
 *   1. Publish the native library:
 *      dotnet publish src/CSharpDB.Native/CSharpDB.Native.csproj -c Release -r win-x64
 *
 *   2. Copy the library to ./native/:
 *      mkdir -p native
 *      cp src/CSharpDB.Native/bin/Release/net10.0/win-x64/publish/CSharpDB.Native.dll native/
 *
 *   3. npm install && npm run build && node examples/basic.mjs
 */

import { Database } from "../dist/index.js";
import { unlinkSync } from "node:fs";

const DB_PATH = "example.db";

// Clean up from previous runs
try { unlinkSync(DB_PATH); } catch { /* ok */ }

// Open a database
const db = new Database(DB_PATH);
console.log("Opened database:", DB_PATH);

// Create a table
db.execute(`
  CREATE TABLE products (
    id    INTEGER PRIMARY KEY,
    name  TEXT,
    price REAL,
    stock INTEGER
  )
`);
console.log("Created products table");

// Insert rows inside a transaction
db.transaction(() => {
  db.execute("INSERT INTO products VALUES (1, 'Widget', 9.99, 100)");
  db.execute("INSERT INTO products VALUES (2, 'Gadget', 24.95, 50)");
  db.execute("INSERT INTO products VALUES (3, 'Doohickey', 3.50, 200)");
});
console.log("Inserted 3 products");

// Query all rows
console.log("\n--- All products ---");
const rows = db.query("SELECT * FROM products ORDER BY id");
for (const row of rows) {
  console.log(`  ${row.id}: ${row.name} - $${row.price} (${row.stock} in stock)`);
}

// Query one row
const cheapest = db.queryOne(
  "SELECT name, price FROM products ORDER BY price LIMIT 1"
);
console.log(`\nCheapest product: ${cheapest?.name} at $${cheapest?.price}`);

// Update with affected count
const result = db.execute("UPDATE products SET stock = stock - 10 WHERE price > 5");
console.log(`\nUpdated ${result.rowsAffected} products`);

// Iterate using generator (memory efficient for large result sets)
console.log("\n--- Iterating with generator ---");
for (const row of db.iterate("SELECT name, stock FROM products")) {
  console.log(`  ${row.name}: ${row.stock} units`);
}

// Column metadata
const cols = db.columns("SELECT id, name, price FROM products");
console.log("\nColumns:", cols.map((c) => c.name).join(", "));

// Cleanup
db.close();
console.log("\nDatabase closed.");

// Remove temp file
try { unlinkSync(DB_PATH); } catch { /* ok */ }
