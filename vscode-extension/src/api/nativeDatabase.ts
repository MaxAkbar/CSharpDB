import * as path from "path";
import {
  DbHandle,
  NativeBindings,
  NativeStringHandle,
  ResultHandle,
  decodeOwnedUtf8String,
  loadNativeLibrary
} from "../native/nativeBindings";

const COLUMN_TYPE = {
  null: 0,
  integer: 1,
  real: 2,
  text: 3,
  blob: 4
} as const;

export interface NativeQueryResult {
  isQuery: boolean;
  columnNames: string[];
  rows: Array<Record<string, unknown>>;
  rowsAffected: number;
}

export class NativeDatabaseError extends Error {
  readonly code: number;

  constructor(message: string, code = -1) {
    super(message);
    this.name = "NativeDatabaseError";
    this.code = code;
  }
}

export class NativeDatabase {
  private nativeLibraryPath = "";
  private resolvedNativeLibraryPath = "";
  private native?: NativeBindings;
  private dbHandle?: DbHandle;
  private databasePath?: string;

  constructor(nativeLibraryPath = "") {
    this.setNativeLibraryPath(nativeLibraryPath);
  }

  get isOpen(): boolean {
    return this.dbHandle !== undefined;
  }

  setNativeLibraryPath(value: string | undefined): void {
    this.nativeLibraryPath = (value ?? "").trim();
  }

  getNativeLibraryPath(): string {
    return this.nativeLibraryPath;
  }

  getResolvedNativeLibraryPath(): string {
    return this.resolvedNativeLibraryPath;
  }

  getDatabasePath(): string | undefined {
    return this.databasePath;
  }

  async open(databasePath: string): Promise<void> {
    const normalizedPath = path.resolve(databasePath);
    await this.close();

    const handle = this.getNative().csharpdb_open(normalizedPath);
    if (!handle) {
      throw this.readLastError();
    }

    this.dbHandle = handle;
    this.databasePath = normalizedPath;
  }

  async close(): Promise<void> {
    if (this.dbHandle && this.native) {
      this.native.csharpdb_close(this.dbHandle);
    }

    this.dbHandle = undefined;
    this.databasePath = undefined;
  }

  execute(sql: string): NativeQueryResult {
    const native = this.getNative();
    const result = native.csharpdb_execute(this.requireHandle(), sql);
    if (!result) {
      throw this.readLastError();
    }

    try {
      const isQuery = native.csharpdb_result_is_query(result) === 1;
      if (!isQuery) {
        return {
          isQuery: false,
          columnNames: [],
          rows: [],
          rowsAffected: native.csharpdb_result_rows_affected(result)
        };
      }

      const columnNames = readColumnNames(native, result);
      const rows: Array<Record<string, unknown>> = [];
      while (true) {
        const status = native.csharpdb_result_next(result);
        if (status === -1) {
          throw this.readLastError();
        }

        if (status === 0) {
          break;
        }

        rows.push(readCurrentRow(native, result, columnNames));
      }

      return {
        isQuery: true,
        columnNames,
        rows,
        rowsAffected: rows.length
      };
    } finally {
      native.csharpdb_result_free(result);
    }
  }

  readJsonResult<T>(invoke: (native: NativeBindings, databasePath: string) => NativeStringHandle): T {
    const databasePath = this.requireDatabasePath();
    return this.readJsonResultForPath(databasePath, invoke);
  }

  readJsonResultForPath<T>(
    databasePath: string,
    invoke: (native: NativeBindings, resolvedPath: string) => NativeStringHandle
  ): T {
    const native = this.getNative();
    const handle = invoke(native, path.resolve(databasePath));
    if (!handle) {
      throw this.readLastError();
    }

    const json = decodeOwnedUtf8String(native, handle);
    return JSON.parse(json) as T;
  }

  async withClosedDatabase<T>(operation: (databasePath: string) => Promise<T>): Promise<T> {
    const databasePath = this.requireDatabasePath();
    const nativeLibraryPath = this.getNativeLibraryPath();
    const wasOpen = this.isOpen;

    if (wasOpen) {
      await this.close();
    }

    try {
      return await operation(databasePath);
    } finally {
      if (wasOpen) {
        this.setNativeLibraryPath(nativeLibraryPath);
        await this.open(databasePath);
      }
    }
  }

  executeInTransaction(statements: string[]): void {
    const native = this.getNative();
    const handle = this.requireHandle();
    if (native.csharpdb_begin(handle) === -1) {
      throw this.readLastError();
    }

    try {
      for (const statement of statements) {
        void this.execute(statement);
      }

      if (native.csharpdb_commit(handle) === -1) {
        throw this.readLastError();
      }
    } catch (error) {
      native.csharpdb_rollback(handle);
      throw error;
    }
  }

  private getNative(): NativeBindings {
    if (!this.native) {
      const loaded = loadNativeLibrary(this.nativeLibraryPath || undefined);
      this.native = loaded.bindings;
      this.resolvedNativeLibraryPath = loaded.libraryPath;
    }

    return this.native;
  }

  private requireHandle(): DbHandle {
    if (!this.dbHandle) {
      throw new Error("No database is open.");
    }

    return this.dbHandle;
  }

  private requireDatabasePath(): string {
    if (!this.databasePath) {
      throw new Error("No database is open.");
    }

    return this.databasePath;
  }

  private readLastError(): NativeDatabaseError {
    const native = this.getNative();
    const message = native.csharpdb_last_error() ?? "Unknown CSharpDB error";
    const code = native.csharpdb_last_error_code();
    native.csharpdb_clear_error();
    return new NativeDatabaseError(message, code);
  }
}

function readColumnNames(native: NativeBindings, result: ResultHandle): string[] {
  const count = native.csharpdb_result_column_count(result);
  const columnNames: string[] = [];
  for (let index = 0; index < count; index++) {
    columnNames.push(native.csharpdb_result_column_name(result, index) ?? `column_${index}`);
  }

  return columnNames;
}

function readCurrentRow(native: NativeBindings, result: ResultHandle, columnNames: string[]): Record<string, unknown> {
  const row: Record<string, unknown> = {};
  for (let index = 0; index < columnNames.length; index++) {
    if (native.csharpdb_result_is_null(result, index) === 1) {
      row[columnNames[index]] = null;
      continue;
    }

    const type = native.csharpdb_result_column_type(result, index);
    switch (type) {
      case COLUMN_TYPE.integer:
        row[columnNames[index]] = normalizeInteger(native.csharpdb_result_get_int64(result, index));
        break;
      case COLUMN_TYPE.real:
        row[columnNames[index]] = native.csharpdb_result_get_double(result, index);
        break;
      case COLUMN_TYPE.text:
        row[columnNames[index]] = native.csharpdb_result_get_text(result, index) ?? "";
        break;
      case COLUMN_TYPE.blob:
        row[columnNames[index]] = formatBlobPlaceholder(native, result, index);
        break;
      default:
        row[columnNames[index]] = null;
        break;
    }
  }

  return row;
}

function formatBlobPlaceholder(native: NativeBindings, result: ResultHandle, index: number): string {
  const sizeBuffer = Buffer.alloc(4);
  void native.csharpdb_result_get_blob(result, index, sizeBuffer);
  const size = sizeBuffer.readInt32LE(0);
  return `[${Math.max(0, size)} bytes]`;
}

function normalizeInteger(value: bigint): number | string {
  if (value <= BigInt(Number.MAX_SAFE_INTEGER) && value >= BigInt(Number.MIN_SAFE_INTEGER)) {
    return Number(value);
  }

  return value.toString();
}
