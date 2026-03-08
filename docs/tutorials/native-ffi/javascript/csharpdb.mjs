/**
 * csharpdb.mjs — Node.js wrapper for the CSharpDB NativeAOT shared library.
 *
 * Usage:
 *   import { CSharpDB } from './csharpdb.mjs';
 *
 *   const db = new CSharpDB('path/to/CSharpDB.Native.dll');
 *   db.open('mydata.db');
 *   db.execute("CREATE TABLE t (id INTEGER, name TEXT)");
 *   const rows = db.query("SELECT * FROM t");
 *   db.close();
 */

import koffi from "koffi";

// Type codes matching CSharpDB.Core.DbType
export const DbType = { NULL: 0, INTEGER: 1, REAL: 2, TEXT: 3, BLOB: 4 };

export class CSharpDB {
  /**
   * Load the native library.
   * @param {string} libPath - Path to CSharpDB.Native.dll / .so / .dylib
   */
  constructor(libPath) {
    if (!libPath) {
      if (process.platform === "win32") libPath = "./CSharpDB.Native.dll";
      else if (process.platform === "darwin") libPath = "./CSharpDB.Native.dylib";
      else libPath = "./CSharpDB.Native.so";
    }

    const lib = koffi.load(libPath);

    // Bind all exported functions
    this._open = lib.func("csharpdb_open", "void*", ["str"]);
    this._close = lib.func("csharpdb_close", "void", ["void*"]);
    this._execute = lib.func("csharpdb_execute", "void*", ["void*", "str"]);

    this._resultIsQuery = lib.func("csharpdb_result_is_query", "int", ["void*"]);
    this._resultNext = lib.func("csharpdb_result_next", "int", ["void*"]);
    this._resultRowsAffected = lib.func("csharpdb_result_rows_affected", "int", ["void*"]);
    this._resultColumnCount = lib.func("csharpdb_result_column_count", "int", ["void*"]);
    this._resultColumnName = lib.func("csharpdb_result_column_name", "str", ["void*", "int"]);
    this._resultColumnType = lib.func("csharpdb_result_column_type", "int", ["void*", "int"]);
    this._resultIsNull = lib.func("csharpdb_result_is_null", "int", ["void*", "int"]);
    this._resultGetInt64 = lib.func("csharpdb_result_get_int64", "int64_t", ["void*", "int"]);
    this._resultGetDouble = lib.func("csharpdb_result_get_double", "double", ["void*", "int"]);
    this._resultGetText = lib.func("csharpdb_result_get_text", "str", ["void*", "int"]);
    this._resultFree = lib.func("csharpdb_result_free", "void", ["void*"]);

    this._begin = lib.func("csharpdb_begin", "int", ["void*"]);
    this._commit = lib.func("csharpdb_commit", "int", ["void*"]);
    this._rollback = lib.func("csharpdb_rollback", "int", ["void*"]);

    this._lastError = lib.func("csharpdb_last_error", "str", []);
    this._lastErrorCode = lib.func("csharpdb_last_error_code", "int", []);
    this._clearError = lib.func("csharpdb_clear_error", "void", []);

    this._db = null;
  }

  /** Open or create a database file. */
  open(dbPath) {
    this._db = this._open(dbPath);
    if (!this._db) {
      throw new Error(`Failed to open database: ${this._lastError()}`);
    }
  }

  /** Close the database. Safe to call multiple times. */
  close() {
    if (this._db) {
      this._close(this._db);
      this._db = null;
    }
  }

  /**
   * Execute a non-query statement (INSERT, UPDATE, DELETE, DDL).
   * @returns {number} Number of rows affected.
   */
  execute(sql) {
    const r = this._execute(this._db, sql);
    if (!r) throw new Error(`SQL error: ${this._lastError()}`);
    const affected = this._resultRowsAffected(r);
    this._resultFree(r);
    return affected;
  }

  /**
   * Execute a SELECT and return an array of objects.
   * Values: INTEGER -> BigInt (auto-converted to Number if safe), REAL -> Number, TEXT -> String.
   */
  query(sql) {
    const r = this._execute(this._db, sql);
    if (!r) throw new Error(`Query error: ${this._lastError()}`);

    const colCount = this._resultColumnCount(r);
    const columns = [];
    for (let i = 0; i < colCount; i++) {
      columns.push(this._resultColumnName(r, i) || `col_${i}`);
    }

    const rows = [];
    while (this._resultNext(r) === 1) {
      const row = {};
      for (let i = 0; i < colCount; i++) {
        if (this._resultIsNull(r, i) === 1) {
          row[columns[i]] = null;
          continue;
        }
        const type = this._resultColumnType(r, i);
        switch (type) {
          case DbType.INTEGER: {
            const val = this._resultGetInt64(r, i);
            // Convert BigInt to Number if it fits safely
            row[columns[i]] =
              typeof val === "bigint" &&
              val >= -Number.MAX_SAFE_INTEGER &&
              val <= Number.MAX_SAFE_INTEGER
                ? Number(val)
                : val;
            break;
          }
          case DbType.REAL:
            row[columns[i]] = this._resultGetDouble(r, i);
            break;
          case DbType.TEXT:
            row[columns[i]] = this._resultGetText(r, i) || "";
            break;
          default:
            row[columns[i]] = null;
        }
      }
      rows.push(row);
    }

    this._resultFree(r);
    return rows;
  }

  /** Execute a SELECT and return the first row, or null. */
  queryOne(sql) {
    const rows = this.query(sql);
    return rows.length > 0 ? rows[0] : null;
  }

  /** Begin an explicit transaction. */
  begin() {
    if (this._begin(this._db) !== 0)
      throw new Error(`Transaction error: ${this._lastError()}`);
  }

  /** Commit the current transaction. */
  commit() {
    if (this._commit(this._db) !== 0)
      throw new Error(`Commit error: ${this._lastError()}`);
  }

  /** Rollback the current transaction. */
  rollback() {
    if (this._rollback(this._db) !== 0)
      throw new Error(`Rollback error: ${this._lastError()}`);
  }

  /**
   * Run a function inside a transaction.
   * Auto-commits on success, rolls back on error.
   */
  transaction(fn) {
    this.begin();
    try {
      const result = fn(this);
      this.commit();
      return result;
    } catch (err) {
      this.rollback();
      throw err;
    }
  }
}
