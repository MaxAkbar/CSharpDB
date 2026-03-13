/**
 * CSharpDB Node.js client — high-level TypeScript API over the NativeAOT shared library.
 *
 * @example
 * ```ts
 * import { Database } from 'csharpdb';
 *
 * const db = new Database('mydata.db');
 * db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
 * db.execute("INSERT INTO users VALUES (1, 'Alice')");
 *
 * for (const row of db.query('SELECT * FROM users')) {
 *   console.log(row);  // { id: 1n, name: 'Alice' }
 * }
 *
 * db.close();
 * ```
 */

import koffi from "koffi";
import { loadNativeLibrary, type NativeBindings, type DbHandle, type ResultHandle } from "./native.js";

// ---------- Column type constants (matches CSharpDB.Primitives.DbType) ----------

/** Column type codes returned by the native library. */
export const ColumnType = {
  NULL: 0,
  INTEGER: 1,
  REAL: 2,
  TEXT: 3,
  BLOB: 4,
} as const;

export type ColumnTypeValue = (typeof ColumnType)[keyof typeof ColumnType];

// ---------- Public types ----------

/** A single row returned by a query, mapping column names to JS values. */
export type Row = Record<string, bigint | number | string | Buffer | null>;

/** Column metadata from a query result. */
export interface ColumnInfo {
  name: string;
  index: number;
}

/** Result of a non-query (INSERT/UPDATE/DELETE/DDL) execution. */
export interface ExecResult {
  rowsAffected: number;
}

/** Options for opening a database. */
export interface DatabaseOptions {
  /** Path to the CSharpDB NativeAOT shared library. Auto-detected if omitted. */
  nativeLibraryPath?: string;
}

// ---------- CSharpDB Error ----------

export class CSharpDBError extends Error {
  constructor(
    message: string,
    public readonly code: number
  ) {
    super(message);
    this.name = "CSharpDBError";
  }
}

// ---------- Database class ----------

export class Database {
  private _native: NativeBindings;
  private _handle: DbHandle | null;

  /**
   * Open or create a CSharpDB database.
   * @param path File path for the database (created if it doesn't exist).
   * @param options Optional configuration.
   */
  constructor(path: string, options?: DatabaseOptions) {
    this._native = loadNativeLibrary(options?.nativeLibraryPath);
    this._handle = this._native.csharpdb_open(path);
    if (!this._handle) {
      throw this._makeError();
    }
  }

  /** Returns true if the database is open. */
  get isOpen(): boolean {
    return this._handle !== null;
  }

  /**
   * Execute a SQL statement that does not return rows (INSERT, UPDATE, DELETE, DDL).
   * @returns Number of rows affected.
   */
  execute(sql: string): ExecResult {
    const result = this._exec(sql);
    try {
      const rowsAffected = this._native.csharpdb_result_rows_affected(result);
      return { rowsAffected };
    } finally {
      this._native.csharpdb_result_free(result);
    }
  }

  /**
   * Execute a SELECT query and return all matching rows as an array.
   * Each row is an object mapping column name -> value.
   *
   * Values are returned as:
   * - INTEGER -> bigint
   * - REAL -> number
   * - TEXT -> string
   * - BLOB -> Buffer
   * - NULL -> null
   */
  query(sql: string): Row[] {
    const result = this._exec(sql);
    try {
      return this._readRows(result);
    } finally {
      this._native.csharpdb_result_free(result);
    }
  }

  /**
   * Execute a SELECT query and return the first row, or null if empty.
   */
  queryOne(sql: string): Row | null {
    const result = this._exec(sql);
    try {
      const columns = this._getColumns(result);
      const status = this._native.csharpdb_result_next(result);
      if (status === -1) throw this._makeError();
      if (status === 0) return null;
      return this._readCurrentRow(result, columns);
    } finally {
      this._native.csharpdb_result_free(result);
    }
  }

  /**
   * Iterate over query results row-by-row using a generator.
   * More memory-efficient for large result sets.
   *
   * **Important:** The generator holds a native result handle. Always consume it
   * fully or use a `for...of` loop which handles cleanup via `.return()`.
   */
  *iterate(sql: string): Generator<Row, void, undefined> {
    const result = this._exec(sql);
    try {
      const columns = this._getColumns(result);
      while (true) {
        const status = this._native.csharpdb_result_next(result);
        if (status === -1) throw this._makeError();
        if (status === 0) break;
        yield this._readCurrentRow(result, columns);
      }
    } finally {
      this._native.csharpdb_result_free(result);
    }
  }

  /**
   * Get column metadata for a query without consuming rows.
   */
  columns(sql: string): ColumnInfo[] {
    const result = this._exec(sql);
    try {
      return this._getColumns(result);
    } finally {
      this._native.csharpdb_result_free(result);
    }
  }

  /**
   * Run a function inside an explicit transaction.
   * Automatically commits on success or rolls back on error.
   */
  transaction<T>(fn: () => T): T {
    this._ensureOpen();
    if (this._native.csharpdb_begin(this._handle!) === -1) {
      throw this._makeError();
    }
    try {
      const result = fn();
      if (this._native.csharpdb_commit(this._handle!) === -1) {
        throw this._makeError();
      }
      return result;
    } catch (err) {
      this._native.csharpdb_rollback(this._handle!);
      throw err;
    }
  }

  /**
   * Close the database and release all native resources.
   * Safe to call multiple times.
   */
  close(): void {
    if (this._handle) {
      this._native.csharpdb_close(this._handle);
      this._handle = null;
    }
  }

  /** Alias for close() — enables use with explicit cleanup patterns. */
  [Symbol.dispose](): void {
    this.close();
  }

  // ---------- Private helpers ----------

  private _ensureOpen(): void {
    if (!this._handle) {
      throw new CSharpDBError("Database is closed", 0);
    }
  }

  private _exec(sql: string): ResultHandle {
    this._ensureOpen();
    const result = this._native.csharpdb_execute(this._handle!, sql);
    if (!result) {
      throw this._makeError();
    }
    return result;
  }

  private _makeError(): CSharpDBError {
    const msg = this._native.csharpdb_last_error() ?? "Unknown CSharpDB error";
    const code = this._native.csharpdb_last_error_code();
    return new CSharpDBError(msg, code);
  }

  private _getColumns(result: ResultHandle): ColumnInfo[] {
    const count = this._native.csharpdb_result_column_count(result);
    const columns: ColumnInfo[] = [];
    for (let i = 0; i < count; i++) {
      columns.push({
        name: this._native.csharpdb_result_column_name(result, i) ?? `col${i}`,
        index: i,
      });
    }
    return columns;
  }

  private _readCurrentRow(result: ResultHandle, columns: ColumnInfo[]): Row {
    const row: Row = {};
    for (const col of columns) {
      if (this._native.csharpdb_result_is_null(result, col.index) === 1) {
        row[col.name] = null;
        continue;
      }
      const type = this._native.csharpdb_result_column_type(result, col.index);
      switch (type) {
        case ColumnType.INTEGER:
          row[col.name] = this._native.csharpdb_result_get_int64(result, col.index);
          break;
        case ColumnType.REAL:
          row[col.name] = this._native.csharpdb_result_get_double(result, col.index);
          break;
        case ColumnType.TEXT:
          row[col.name] = this._native.csharpdb_result_get_text(result, col.index) ?? "";
          break;
        case ColumnType.BLOB: {
          const sizeBuffer = Buffer.alloc(4);
          const ptr = this._native.csharpdb_result_get_blob(result, col.index, sizeBuffer);
          const size = sizeBuffer.readInt32LE();
          // koffi returns the blob data — we copy it into a Node Buffer
          if (ptr && size > 0) {
            row[col.name] = Buffer.from(koffi_decode(ptr, size));
          } else {
            row[col.name] = Buffer.alloc(0);
          }
          break;
        }
        default:
          row[col.name] = null;
      }
    }
    return row;
  }

  private _readRows(result: ResultHandle): Row[] {
    const columns = this._getColumns(result);
    const rows: Row[] = [];
    while (true) {
      const status = this._native.csharpdb_result_next(result);
      if (status === -1) throw this._makeError();
      if (status === 0) break;
      rows.push(this._readCurrentRow(result, columns));
    }
    return rows;
  }
}

// koffi helper for decoding raw pointer to Buffer (blob support)
function koffi_decode(ptr: unknown, size: number): Uint8Array {
  return koffi.decode(ptr, "uint8_t", size) as Uint8Array;
}

// ---------- Exports ----------

export { loadNativeLibrary } from "./native.js";
export type { NativeBindings, DbHandle, ResultHandle } from "./native.js";
