/**
 * Low-level FFI bindings to the CSharpDB NativeAOT shared library.
 * Uses koffi to load and call the C API functions.
 */

import koffi from "koffi";
import { platform } from "node:os";
import { resolve, dirname } from "node:path";
import { existsSync } from "node:fs";

// ---------- Type aliases matching csharpdb.h ----------

/** Opaque database handle (void*) */
export type DbHandle = unknown;
/** Opaque result handle (void*) */
export type ResultHandle = unknown;

// ---------- Library loader ----------

function getDefaultLibraryName(): string {
  switch (platform()) {
    case "win32":
      return "CSharpDB.Native.dll";
    case "darwin":
      return "CSharpDB.Native.dylib";
    default:
      return "CSharpDB.Native.so";
  }
}

function findLibrary(customPath?: string): string {
  if (customPath) {
    if (!existsSync(customPath)) {
      throw new Error(`CSharpDB native library not found at: ${customPath}`);
    }
    return customPath;
  }

  const libName = getDefaultLibraryName();

  // Search order:
  // 1. CSHARPDB_NATIVE_PATH env var
  // 2. ./native/<libName>  (next to consuming app)
  // 3. Same directory as this module
  const searchPaths = [
    process.env["CSHARPDB_NATIVE_PATH"],
    resolve(process.cwd(), "native", libName),
    resolve(process.cwd(), libName),
    resolve(dirname(new URL(import.meta.url).pathname.replace(/^\/([A-Z]:)/, "$1")), "..", "native", libName),
  ].filter(Boolean) as string[];

  for (const p of searchPaths) {
    if (existsSync(p)) return p;
  }

  throw new Error(
    `CSharpDB native library not found. Searched:\n${searchPaths.map((p) => `  - ${p}`).join("\n")}\n\n` +
      `Set CSHARPDB_NATIVE_PATH or place ${libName} in ./native/`
  );
}

// ---------- FFI declarations ----------

export interface NativeBindings {
  // Database lifecycle
  csharpdb_open(path: string): DbHandle;
  csharpdb_close(db: DbHandle): void;

  // SQL execution
  csharpdb_execute(db: DbHandle, sql: string): ResultHandle;

  // Result metadata
  csharpdb_result_is_query(result: ResultHandle): number;
  csharpdb_result_rows_affected(result: ResultHandle): number;
  csharpdb_result_column_count(result: ResultHandle): number;
  csharpdb_result_column_name(result: ResultHandle, index: number): string | null;

  // Row iteration
  csharpdb_result_next(result: ResultHandle): number;
  csharpdb_result_column_type(result: ResultHandle, index: number): number;
  csharpdb_result_is_null(result: ResultHandle, index: number): number;

  // Value access
  csharpdb_result_get_int64(result: ResultHandle, index: number): bigint;
  csharpdb_result_get_double(result: ResultHandle, index: number): number;
  csharpdb_result_get_text(result: ResultHandle, index: number): string | null;
  csharpdb_result_get_blob(result: ResultHandle, index: number, outSize: Buffer): unknown;

  // Result cleanup
  csharpdb_result_free(result: ResultHandle): void;

  // Transactions
  csharpdb_begin(db: DbHandle): number;
  csharpdb_commit(db: DbHandle): number;
  csharpdb_rollback(db: DbHandle): number;

  // Error handling
  csharpdb_last_error(): string | null;
  csharpdb_last_error_code(): number;
  csharpdb_clear_error(): void;
}

export function loadNativeLibrary(libraryPath?: string): NativeBindings {
  const libPath = findLibrary(libraryPath);
  const lib = koffi.load(libPath);

  return {
    csharpdb_open: lib.func("csharpdb_open", "void*", ["str"]),
    csharpdb_close: lib.func("csharpdb_close", "void", ["void*"]),
    csharpdb_execute: lib.func("csharpdb_execute", "void*", ["void*", "str"]),
    csharpdb_result_is_query: lib.func("csharpdb_result_is_query", "int", ["void*"]),
    csharpdb_result_rows_affected: lib.func("csharpdb_result_rows_affected", "int", ["void*"]),
    csharpdb_result_column_count: lib.func("csharpdb_result_column_count", "int", ["void*"]),
    csharpdb_result_column_name: lib.func("csharpdb_result_column_name", "str", ["void*", "int"]),
    csharpdb_result_next: lib.func("csharpdb_result_next", "int", ["void*"]),
    csharpdb_result_column_type: lib.func("csharpdb_result_column_type", "int", ["void*", "int"]),
    csharpdb_result_is_null: lib.func("csharpdb_result_is_null", "int", ["void*", "int"]),
    csharpdb_result_get_int64: lib.func("csharpdb_result_get_int64", "int64_t", ["void*", "int"]),
    csharpdb_result_get_double: lib.func("csharpdb_result_get_double", "double", ["void*", "int"]),
    csharpdb_result_get_text: lib.func("csharpdb_result_get_text", "str", ["void*", "int"]),
    csharpdb_result_get_blob: lib.func("csharpdb_result_get_blob", "void*", ["void*", "int", "uint8_t*"]),
    csharpdb_result_free: lib.func("csharpdb_result_free", "void", ["void*"]),
    csharpdb_begin: lib.func("csharpdb_begin", "int", ["void*"]),
    csharpdb_commit: lib.func("csharpdb_commit", "int", ["void*"]),
    csharpdb_rollback: lib.func("csharpdb_rollback", "int", ["void*"]),
    csharpdb_last_error: lib.func("csharpdb_last_error", "str", []),
    csharpdb_last_error_code: lib.func("csharpdb_last_error_code", "int", []),
    csharpdb_clear_error: lib.func("csharpdb_clear_error", "void", []),
  };
}
