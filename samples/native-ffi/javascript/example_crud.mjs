/**
 * Example: Basic CRUD operations with CSharpDB from Node.js.
 *
 * Run:
 *   npm install
 *   node example_crud.mjs
 */

import { CSharpDB } from "./csharpdb.mjs";
import { unlinkSync, existsSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

// Adjust this path to your published CSharpDB.Native library
const LIB_PATH =
  process.env.CSHARPDB_NATIVE_PATH ||
  resolve(
    __dirname,
    "..", "..", "..", "..",
    "src", "CSharpDB.Native", "bin", "Release",
    "net10.0", "win-x64", "publish", "CSharpDB.Native.dll"
  );

const DB_FILE = "tutorial_crud.db";

// Clean up from previous runs
if (existsSync(DB_FILE)) unlinkSync(DB_FILE);

console.log("=== CSharpDB Node.js Tutorial: CRUD Operations ===\n");

const db = new CSharpDB(LIB_PATH);
db.open(DB_FILE);
console.log(`Opened database: ${DB_FILE}`);

// --- CREATE ---
console.log("\n--- CREATE TABLE ---");
db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)");
console.log("Created 'users' table");

// --- INSERT ---
console.log("\n--- INSERT ---");
db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 30)");
db.execute("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 25)");
db.execute("INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com', 35)");
console.log("Inserted 3 users");

// --- READ (SELECT all) ---
console.log("\n--- SELECT * FROM users ---");
const allUsers = db.query("SELECT * FROM users ORDER BY id");
for (const row of allUsers) {
  console.log(`  id=${row.id}, name=${row.name}, email=${row.email}, age=${row.age}`);
}

// --- READ (SELECT with filter) ---
console.log("\n--- SELECT users older than 28 ---");
const olderUsers = db.query("SELECT name, age FROM users WHERE age > 28 ORDER BY age");
for (const row of olderUsers) {
  console.log(`  ${row.name} (age ${row.age})`);
}

// --- READ (single row) ---
console.log("\n--- SELECT single user ---");
const user = db.queryOne("SELECT name, email FROM users WHERE id = 2");
if (user) {
  console.log(`  User 2: ${user.name} <${user.email}>`);
}

// --- UPDATE ---
console.log("\n--- UPDATE ---");
const updated = db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'");
console.log(`  Updated ${updated} row(s)`);

const alice = db.queryOne("SELECT name, age FROM users WHERE name = 'Alice'");
console.log(`  Alice's age is now: ${alice.age}`);

// --- DELETE ---
console.log("\n--- DELETE ---");
const deleted = db.execute("DELETE FROM users WHERE name = 'Charlie'");
console.log(`  Deleted ${deleted} row(s)`);

const remaining = db.query("SELECT name FROM users ORDER BY name");
console.log(`  Remaining users: ${remaining.map((r) => r.name).join(", ")}`);

// --- Aggregates ---
console.log("\n--- AGGREGATES ---");
const stats = db.queryOne("SELECT COUNT(*) as total, AVG(age) as avg_age FROM users");
console.log(`  ${stats.total} users, average age: ${stats.avg_age.toFixed(1)}`);

// Cleanup
db.close();
console.log(`\nDatabase closed. Cleaning up ${DB_FILE}...`);
unlinkSync(DB_FILE);
console.log("Done!");
