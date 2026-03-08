/**
 * Example: Transactions with CSharpDB from Node.js.
 *
 * Demonstrates:
 *   - Explicit begin/commit/rollback
 *   - transaction() helper for automatic commit/rollback
 *   - Rollback on error
 *
 * Run:
 *   npm install
 *   node example_transactions.mjs
 */

import { CSharpDB } from "./csharpdb.mjs";
import { unlinkSync, existsSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

const LIB_PATH =
  process.env.CSHARPDB_NATIVE_PATH ||
  resolve(
    __dirname,
    "..", "..", "..", "..",
    "src", "CSharpDB.Native", "bin", "Release",
    "net10.0", "win-x64", "publish", "CSharpDB.Native.dll"
  );

const DB_FILE = "tutorial_transactions.db";

if (existsSync(DB_FILE)) unlinkSync(DB_FILE);

console.log("=== CSharpDB Node.js Tutorial: Transactions ===\n");

const db = new CSharpDB(LIB_PATH);
db.open(DB_FILE);

// Setup
db.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT, balance REAL)");
db.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000.00)");
db.execute("INSERT INTO accounts VALUES (2, 'Bob', 500.00)");

function showBalances() {
  const rows = db.query("SELECT name, balance FROM accounts ORDER BY id");
  for (const r of rows) {
    console.log(`    ${r.name}: $${r.balance.toFixed(2)}`);
  }
}

console.log("Initial balances:");
showBalances();

// --- Example 1: Successful transaction (explicit) ---
console.log("\n--- Example 1: Successful transfer (explicit begin/commit) ---");
db.begin();
db.execute("UPDATE accounts SET balance = balance - 200 WHERE name = 'Alice'");
db.execute("UPDATE accounts SET balance = balance + 200 WHERE name = 'Bob'");
db.commit();
console.log("  Transferred $200 from Alice to Bob");
showBalances();

// --- Example 2: Rolled-back transaction ---
console.log("\n--- Example 2: Rolled-back transfer ---");
db.begin();
db.execute("UPDATE accounts SET balance = balance - 9999 WHERE name = 'Alice'");
console.log("  (Oops, transferring $9999 - let's rollback)");
db.rollback();
console.log("  Rolled back!");
showBalances();

// --- Example 3: transaction() helper with auto-commit ---
console.log("\n--- Example 3: transaction() helper ---");
db.transaction(() => {
  db.execute("UPDATE accounts SET balance = balance - 100 WHERE name = 'Bob'");
  db.execute("UPDATE accounts SET balance = balance + 100 WHERE name = 'Alice'");
});
console.log("  Auto-committed $100 transfer from Bob to Alice");
showBalances();

// --- Example 4: transaction() helper with auto-rollback on error ---
console.log("\n--- Example 4: transaction() with error (auto-rollback) ---");
try {
  db.transaction(() => {
    db.execute("UPDATE accounts SET balance = balance - 50 WHERE name = 'Alice'");
    throw new Error("Something went wrong mid-transfer!");
  });
} catch (err) {
  console.log(`  Caught error: ${err.message}`);
  console.log("  Transaction was automatically rolled back");
}
showBalances();

// --- Example 5: Batch insert in transaction ---
console.log("\n--- Example 5: Batch insert in a transaction ---");
db.execute("CREATE TABLE logs (id INTEGER PRIMARY KEY, message TEXT)");

db.transaction(() => {
  for (let i = 1; i <= 5; i++) {
    db.execute(`INSERT INTO logs VALUES (${i}, 'Log entry ${i}')`);
  }
});
const count = db.queryOne("SELECT COUNT(*) as cnt FROM logs");
console.log(`  Inserted ${count.cnt} log entries in a single transaction`);

// Cleanup
db.close();
console.log(`\nCleaning up ${DB_FILE}...`);
unlinkSync(DB_FILE);
console.log("Done!");
