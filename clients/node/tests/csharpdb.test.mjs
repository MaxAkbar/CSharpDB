/**
 * CSharpDB Node.js client tests.
 *
 * Requires the native library to be published and available.
 * Run with: node --test tests/csharpdb.test.mjs
 */

import { describe, it, before, after, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { unlinkSync, existsSync } from "node:fs";
import { Database, CSharpDBError, ColumnType } from "../dist/index.js";

const TEST_DB = "test_csharpdb.db";

function cleanup() {
  try {
    if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
  } catch {
    /* ok */
  }
}

describe("Database", () => {
  /** @type {Database} */
  let db;

  before(() => {
    cleanup();
  });

  after(() => {
    cleanup();
  });

  beforeEach(() => {
    cleanup();
    db = new Database(TEST_DB);
  });

  after(() => {
    if (db?.isOpen) db.close();
  });

  describe("open/close", () => {
    it("should open a new database", () => {
      assert.equal(db.isOpen, true);
      db.close();
      assert.equal(db.isOpen, false);
    });

    it("should allow closing multiple times", () => {
      db.close();
      db.close(); // should not throw
    });

    it("should throw on operations after close", () => {
      db.close();
      assert.throws(() => db.execute("SELECT 1"), CSharpDBError);
    });
  });

  describe("execute", () => {
    it("should create a table", () => {
      const result = db.execute("CREATE TABLE t (id INTEGER, name TEXT)");
      assert.equal(result.rowsAffected, 0);
    });

    it("should insert rows and return affected count", () => {
      db.execute("CREATE TABLE t (id INTEGER, name TEXT)");
      const r1 = db.execute("INSERT INTO t VALUES (1, 'hello')");
      assert.equal(r1.rowsAffected, 1);
    });

    it("should throw on invalid SQL", () => {
      assert.throws(() => db.execute("INVALID SQL"), CSharpDBError);
    });
  });

  describe("query", () => {
    it("should return rows as objects", () => {
      db.execute("CREATE TABLE t (id INTEGER, name TEXT)");
      db.execute("INSERT INTO t VALUES (1, 'Alice')");
      db.execute("INSERT INTO t VALUES (2, 'Bob')");

      const rows = db.query("SELECT id, name FROM t ORDER BY id");
      assert.equal(rows.length, 2);
      assert.equal(rows[0].id, 1n); // bigint
      assert.equal(rows[0].name, "Alice");
      assert.equal(rows[1].id, 2n);
      assert.equal(rows[1].name, "Bob");
    });

    it("should return empty array for no matches", () => {
      db.execute("CREATE TABLE t (id INTEGER)");
      const rows = db.query("SELECT * FROM t");
      assert.deepEqual(rows, []);
    });

    it("should handle NULL values", () => {
      db.execute("CREATE TABLE t (id INTEGER, val TEXT)");
      db.execute("INSERT INTO t VALUES (1, NULL)");

      const rows = db.query("SELECT * FROM t");
      assert.equal(rows[0].val, null);
    });

    it("should handle REAL values", () => {
      db.execute("CREATE TABLE t (val REAL)");
      db.execute("INSERT INTO t VALUES (3.14)");

      const rows = db.query("SELECT val FROM t");
      assert.ok(Math.abs(Number(rows[0].val) - 3.14) < 0.001);
    });
  });

  describe("queryOne", () => {
    it("should return single row", () => {
      db.execute("CREATE TABLE t (id INTEGER, name TEXT)");
      db.execute("INSERT INTO t VALUES (1, 'Alice')");

      const row = db.queryOne("SELECT * FROM t WHERE id = 1");
      assert.notEqual(row, null);
      assert.equal(row?.name, "Alice");
    });

    it("should return null when no rows", () => {
      db.execute("CREATE TABLE t (id INTEGER)");
      const row = db.queryOne("SELECT * FROM t");
      assert.equal(row, null);
    });
  });

  describe("iterate", () => {
    it("should yield rows via generator", () => {
      db.execute("CREATE TABLE t (id INTEGER)");
      db.execute("INSERT INTO t VALUES (1)");
      db.execute("INSERT INTO t VALUES (2)");
      db.execute("INSERT INTO t VALUES (3)");

      const ids = [];
      for (const row of db.iterate("SELECT id FROM t ORDER BY id")) {
        ids.push(Number(row.id));
      }
      assert.deepEqual(ids, [1, 2, 3]);
    });

    it("should handle early break", () => {
      db.execute("CREATE TABLE t (id INTEGER)");
      for (let i = 0; i < 100; i++) {
        db.execute(`INSERT INTO t VALUES (${i})`);
      }

      let count = 0;
      for (const _row of db.iterate("SELECT id FROM t")) {
        count++;
        if (count === 5) break;
      }
      assert.equal(count, 5);
    });
  });

  describe("columns", () => {
    it("should return column metadata", () => {
      db.execute("CREATE TABLE t (id INTEGER, name TEXT, score REAL)");
      const cols = db.columns("SELECT id, name, score FROM t");
      assert.equal(cols.length, 3);
      assert.equal(cols[0].name, "id");
      assert.equal(cols[1].name, "name");
      assert.equal(cols[2].name, "score");
    });
  });

  describe("transaction", () => {
    it("should commit on success", () => {
      db.execute("CREATE TABLE t (id INTEGER)");

      db.transaction(() => {
        db.execute("INSERT INTO t VALUES (1)");
        db.execute("INSERT INTO t VALUES (2)");
      });

      const rows = db.query("SELECT * FROM t");
      assert.equal(rows.length, 2);
    });

    it("should rollback on error", () => {
      db.execute("CREATE TABLE t (id INTEGER)");
      db.execute("INSERT INTO t VALUES (0)");

      assert.throws(() => {
        db.transaction(() => {
          db.execute("INSERT INTO t VALUES (1)");
          throw new Error("abort!");
        });
      });

      const rows = db.query("SELECT * FROM t");
      assert.equal(rows.length, 1);
      assert.equal(rows[0].id, 0n);
    });

    it("should return the function result", () => {
      db.execute("CREATE TABLE t (id INTEGER)");
      const result = db.transaction(() => {
        db.execute("INSERT INTO t VALUES (42)");
        return "done";
      });
      assert.equal(result, "done");
    });
  });
});
