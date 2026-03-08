"""Example: Basic CRUD operations with CSharpDB from Python.

Run:
    python example_crud.py

Requires the native library path — set LIB_PATH below or pass via command line.
"""

import os
import sys

# Adjust this path to your published CSharpDB.Native library
LIB_PATH = os.environ.get(
    "CSHARPDB_NATIVE_PATH",
    os.path.join(
        os.path.dirname(__file__),
        "..", "..", "..", "..",
        "src", "CSharpDB.Native", "bin", "Release",
        "net10.0", "win-x64", "publish", "CSharpDB.Native.dll",
    ),
)

from csharpdb import CSharpDB

DB_FILE = "tutorial_crud.db"

# Clean up from previous runs
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)

print("=== CSharpDB Python Tutorial: CRUD Operations ===\n")

with CSharpDB(LIB_PATH) as db:
    db.open(DB_FILE)
    print(f"Opened database: {DB_FILE}")

    # --- CREATE ---
    print("\n--- CREATE TABLE ---")
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)")
    print("Created 'users' table")

    # --- INSERT ---
    print("\n--- INSERT ---")
    db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 30)")
    db.execute("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 25)")
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com', 35)")
    print("Inserted 3 users")

    # --- READ (SELECT all) ---
    print("\n--- SELECT * FROM users ---")
    rows = db.query("SELECT * FROM users ORDER BY id")
    for row in rows:
        print(f"  id={row['id']}, name={row['name']}, email={row['email']}, age={row['age']}")

    # --- READ (SELECT with filter) ---
    print("\n--- SELECT users older than 28 ---")
    rows = db.query("SELECT name, age FROM users WHERE age > 28 ORDER BY age")
    for row in rows:
        print(f"  {row['name']} (age {row['age']})")

    # --- READ (single row) ---
    print("\n--- SELECT single user ---")
    user = db.query_one("SELECT name, email FROM users WHERE id = 2")
    if user:
        print(f"  User 2: {user['name']} <{user['email']}>")

    # --- UPDATE ---
    print("\n--- UPDATE ---")
    affected = db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")
    print(f"  Updated {affected} row(s)")

    updated = db.query_one("SELECT name, age FROM users WHERE name = 'Alice'")
    print(f"  Alice's age is now: {updated['age']}")

    # --- DELETE ---
    print("\n--- DELETE ---")
    affected = db.execute("DELETE FROM users WHERE name = 'Charlie'")
    print(f"  Deleted {affected} row(s)")

    remaining = db.query("SELECT name FROM users ORDER BY name")
    print(f"  Remaining users: {', '.join(r['name'] for r in remaining)}")

    # --- Aggregates ---
    print("\n--- AGGREGATES ---")
    stats = db.query_one("SELECT COUNT(*) as total, AVG(age) as avg_age FROM users")
    print(f"  {stats['total']} users, average age: {stats['avg_age']:.1f}")

print(f"\nDatabase closed. Cleaning up {DB_FILE}...")
os.remove(DB_FILE)
print("Done!")
