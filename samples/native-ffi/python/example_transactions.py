"""Example: Transactions with CSharpDB from Python.

Demonstrates:
  - Explicit begin/commit/rollback
  - transaction() helper for automatic commit/rollback
  - Rollback on error

Run:
    python example_transactions.py
"""

import os
import sys

LIB_PATH = os.environ.get(
    "CSHARPDB_NATIVE_PATH",
    os.path.join(
        os.path.dirname(__file__),
        "..", "..", "..", "..",
        "src", "CSharpDB.Native", "bin", "Release",
        "net10.0", "win-x64", "publish", "CSharpDB.Native.dll",
    ),
)

from csharpdb import CSharpDB, CSharpDBError

DB_FILE = "tutorial_transactions.db"

if os.path.exists(DB_FILE):
    os.remove(DB_FILE)

print("=== CSharpDB Python Tutorial: Transactions ===\n")

with CSharpDB(LIB_PATH) as db:
    db.open(DB_FILE)

    # Setup
    db.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT, balance REAL)")
    db.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000.00)")
    db.execute("INSERT INTO accounts VALUES (2, 'Bob', 500.00)")

    def show_balances():
        rows = db.query("SELECT name, balance FROM accounts ORDER BY id")
        for r in rows:
            print(f"    {r['name']}: ${r['balance']:.2f}")

    print("Initial balances:")
    show_balances()

    # --- Example 1: Successful transaction (explicit) ---
    print("\n--- Example 1: Successful transfer (explicit begin/commit) ---")
    db.begin()
    db.execute("UPDATE accounts SET balance = balance - 200 WHERE name = 'Alice'")
    db.execute("UPDATE accounts SET balance = balance + 200 WHERE name = 'Bob'")
    db.commit()
    print("  Transferred $200 from Alice to Bob")
    show_balances()

    # --- Example 2: Rolled-back transaction ---
    print("\n--- Example 2: Rolled-back transfer ---")
    db.begin()
    db.execute("UPDATE accounts SET balance = balance - 9999 WHERE name = 'Alice'")
    print("  (Oops, transferring $9999 — let's rollback)")
    db.rollback()
    print("  Rolled back!")
    show_balances()

    # --- Example 3: transaction() helper with auto-commit ---
    print("\n--- Example 3: transaction() helper ---")

    def transfer():
        db.execute("UPDATE accounts SET balance = balance - 100 WHERE name = 'Bob'")
        db.execute("UPDATE accounts SET balance = balance + 100 WHERE name = 'Alice'")

    db.transaction(transfer)
    print("  Auto-committed $100 transfer from Bob to Alice")
    show_balances()

    # --- Example 4: transaction() helper with auto-rollback on error ---
    print("\n--- Example 4: transaction() with error (auto-rollback) ---")
    try:
        def bad_transfer():
            db.execute("UPDATE accounts SET balance = balance - 50 WHERE name = 'Alice'")
            raise ValueError("Something went wrong mid-transfer!")

        db.transaction(bad_transfer)
    except ValueError as e:
        print(f"  Caught error: {e}")
        print("  Transaction was automatically rolled back")
    show_balances()

    # --- Example 5: Batch insert in transaction ---
    print("\n--- Example 5: Batch insert in a transaction ---")
    db.execute("CREATE TABLE logs (id INTEGER PRIMARY KEY, message TEXT)")

    def batch_insert():
        for i in range(1, 6):
            db.execute(f"INSERT INTO logs VALUES ({i}, 'Log entry {i}')")

    db.transaction(batch_insert)
    count = db.query_one("SELECT COUNT(*) as cnt FROM logs")
    print(f"  Inserted {count['cnt']} log entries in a single transaction")

print(f"\nCleaning up {DB_FILE}...")
os.remove(DB_FILE)
print("Done!")
