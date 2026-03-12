import * as fs from "fs";
import * as path from "path";
import koffi from "koffi";

export type DbHandle = unknown;
export type ResultHandle = unknown;
export type NativeStringHandle = unknown;

export interface NativeBindings {
  csharpdb_open(path: string): DbHandle;
  csharpdb_close(db: DbHandle): void;
  csharpdb_execute(db: DbHandle, sql: string): ResultHandle;
  csharpdb_result_is_query(result: ResultHandle): number;
  csharpdb_result_rows_affected(result: ResultHandle): number;
  csharpdb_result_column_count(result: ResultHandle): number;
  csharpdb_result_column_name(result: ResultHandle, index: number): string | null;
  csharpdb_result_next(result: ResultHandle): number;
  csharpdb_result_column_type(result: ResultHandle, index: number): number;
  csharpdb_result_is_null(result: ResultHandle, index: number): number;
  csharpdb_result_get_int64(result: ResultHandle, index: number): bigint;
  csharpdb_result_get_double(result: ResultHandle, index: number): number;
  csharpdb_result_get_text(result: ResultHandle, index: number): string | null;
  csharpdb_result_get_blob(result: ResultHandle, index: number, outSize: Buffer): unknown;
  csharpdb_result_free(result: ResultHandle): void;
  csharpdb_begin(db: DbHandle): number;
  csharpdb_commit(db: DbHandle): number;
  csharpdb_rollback(db: DbHandle): number;
  csharpdb_last_error(): string | null;
  csharpdb_last_error_code(): number;
  csharpdb_clear_error(): void;
  csharpdb_inspect_storage_json(databasePath: string, includePages: number): NativeStringHandle;
  csharpdb_inspect_wal_json(databasePath: string): NativeStringHandle;
  csharpdb_inspect_page_json(databasePath: string, pageId: number, includeHex: number): NativeStringHandle;
  csharpdb_check_indexes_json(databasePath: string, indexName: string, sampleSize: number): NativeStringHandle;
  csharpdb_get_maintenance_report_json(databasePath: string): NativeStringHandle;
  csharpdb_reindex_json(databasePath: string, scope: number, name: string): NativeStringHandle;
  csharpdb_vacuum_json(databasePath: string): NativeStringHandle;
  csharpdb_string_free(value: NativeStringHandle): void;
  csharpdb_string_length(value: NativeStringHandle): number;
}

export interface LoadedNativeLibrary {
  bindings: NativeBindings;
  libraryPath: string;
}

const bindingCache = new Map<string, NativeBindings>();

export function loadNativeLibrary(customPath?: string): LoadedNativeLibrary {
  const libraryPath = findLibrary(customPath);
  const cached = bindingCache.get(libraryPath);
  if (cached) {
    return { bindings: cached, libraryPath };
  }

  let bindings: NativeBindings;
  try {
    const library = koffi.load(libraryPath);
    bindings = {
      csharpdb_open: library.func("csharpdb_open", "void*", ["str"]),
      csharpdb_close: library.func("csharpdb_close", "void", ["void*"]),
      csharpdb_execute: library.func("csharpdb_execute", "void*", ["void*", "str"]),
      csharpdb_result_is_query: library.func("csharpdb_result_is_query", "int", ["void*"]),
      csharpdb_result_rows_affected: library.func("csharpdb_result_rows_affected", "int", ["void*"]),
      csharpdb_result_column_count: library.func("csharpdb_result_column_count", "int", ["void*"]),
      csharpdb_result_column_name: library.func("csharpdb_result_column_name", "str", ["void*", "int"]),
      csharpdb_result_next: library.func("csharpdb_result_next", "int", ["void*"]),
      csharpdb_result_column_type: library.func("csharpdb_result_column_type", "int", ["void*", "int"]),
      csharpdb_result_is_null: library.func("csharpdb_result_is_null", "int", ["void*", "int"]),
      csharpdb_result_get_int64: library.func("csharpdb_result_get_int64", "int64_t", ["void*", "int"]),
      csharpdb_result_get_double: library.func("csharpdb_result_get_double", "double", ["void*", "int"]),
      csharpdb_result_get_text: library.func("csharpdb_result_get_text", "str", ["void*", "int"]),
      csharpdb_result_get_blob: library.func("csharpdb_result_get_blob", "void*", ["void*", "int", "uint8_t*"]),
      csharpdb_result_free: library.func("csharpdb_result_free", "void", ["void*"]),
      csharpdb_begin: library.func("csharpdb_begin", "int", ["void*"]),
      csharpdb_commit: library.func("csharpdb_commit", "int", ["void*"]),
      csharpdb_rollback: library.func("csharpdb_rollback", "int", ["void*"]),
      csharpdb_last_error: library.func("csharpdb_last_error", "str", []),
      csharpdb_last_error_code: library.func("csharpdb_last_error_code", "int", []),
      csharpdb_clear_error: library.func("csharpdb_clear_error", "void", []),
      csharpdb_inspect_storage_json: library.func("csharpdb_inspect_storage_json", "void*", ["str", "int"]),
      csharpdb_inspect_wal_json: library.func("csharpdb_inspect_wal_json", "void*", ["str"]),
      csharpdb_inspect_page_json: library.func("csharpdb_inspect_page_json", "void*", ["str", "uint32_t", "int"]),
      csharpdb_check_indexes_json: library.func("csharpdb_check_indexes_json", "void*", ["str", "str", "int"]),
      csharpdb_get_maintenance_report_json: library.func("csharpdb_get_maintenance_report_json", "void*", ["str"]),
      csharpdb_reindex_json: library.func("csharpdb_reindex_json", "void*", ["str", "int", "str"]),
      csharpdb_vacuum_json: library.func("csharpdb_vacuum_json", "void*", ["str"]),
      csharpdb_string_free: library.func("csharpdb_string_free", "void", ["void*"]),
      csharpdb_string_length: library.func("csharpdb_string_length", "int", ["void*"])
    };
  } catch (error) {
    throw formatLoadError(libraryPath, error);
  }

  bindingCache.set(libraryPath, bindings);
  return { bindings, libraryPath };
}

export function decodeOwnedUtf8String(bindings: NativeBindings, handle: NativeStringHandle): string {
  if (!handle) {
    return "";
  }

  const length = bindings.csharpdb_string_length(handle);
  try {
    if (length <= 0) {
      return "";
    }

    const bytes = koffi.decode(handle, "uint8_t", length) as Uint8Array;
    return Buffer.from(bytes).toString("utf8");
  } finally {
    bindings.csharpdb_string_free(handle);
  }
}

function findLibrary(customPath?: string): string {
  const libraryName = getLibraryName();
  const explicit = resolveCandidate(customPath, libraryName);
  if (explicit) {
    if (!fs.existsSync(explicit)) {
      throw new Error(`CSharpDB native library not found at: ${explicit}`);
    }

    return explicit;
  }

  const candidates = [
    resolveCandidate(process.env.CSHARPDB_NATIVE_PATH, libraryName),
    path.resolve(__dirname, "..", "native", libraryName),
    path.resolve(__dirname, "..", libraryName),
    path.resolve(process.cwd(), "native", libraryName),
    path.resolve(process.cwd(), libraryName)
  ].filter((value): value is string => typeof value === "string");

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }

  const publishSearchRoots = [
    path.resolve(__dirname, "..", "..", "src", "CSharpDB.Native", "bin"),
    path.resolve(process.cwd(), "src", "CSharpDB.Native", "bin")
  ];

  const publishedMatches = publishSearchRoots
    .flatMap((root) => findLibrariesRecursively(root, libraryName))
    .filter((candidate) => candidate.toLowerCase().includes(`${path.sep}publish${path.sep}`.toLowerCase()));

  const preferredPublishedMatch = chooseBestLibraryMatch(publishedMatches);
  if (preferredPublishedMatch) {
    return preferredPublishedMatch;
  }

  const packagedMatches = [
    ...findLibrariesRecursively(path.resolve(__dirname, "..", "native"), libraryName),
    ...findLibrariesRecursively(path.resolve(process.cwd(), "native"), libraryName)
  ];

  const preferredPackagedMatch = chooseBestLibraryMatch(packagedMatches);
  if (preferredPackagedMatch) {
    return preferredPackagedMatch;
  }

  throw new Error(
    [
      "Unable to locate the CSharpDB NativeAOT library.",
      "Run a NativeAOT publish, for example:",
      suggestedPublishCommand(),
      `Then set csharpdb.nativeLibraryPath or place ${libraryName} under a native/ folder next to the extension.`
    ].join(" ")
  );
}

function getLibraryName(): string {
  switch (process.platform) {
    case "win32":
      return "CSharpDB.Native.dll";
    case "darwin":
      return "CSharpDB.Native.dylib";
    default:
      return "CSharpDB.Native.so";
  }
}

function resolveCandidate(candidate: string | undefined, libraryName: string): string | undefined {
  if (!candidate || candidate.trim().length === 0) {
    return undefined;
  }

  const resolved = path.resolve(candidate);
  try {
    if (fs.statSync(resolved).isDirectory()) {
      return path.join(resolved, libraryName);
    }
  } catch {
    // Fall through and treat the value as a file path.
  }

  return resolved;
}

function findLibrariesRecursively(root: string, libraryName: string): string[] {
  if (!fs.existsSync(root)) {
    return [];
  }

  const matches: string[] = [];
  const pending = [root];
  while (pending.length > 0) {
    const current = pending.pop();
    if (!current) {
      continue;
    }

    let entries: fs.Dirent[];
    try {
      entries = fs.readdirSync(current, { withFileTypes: true });
    } catch {
      continue;
    }

    for (const entry of entries) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        pending.push(fullPath);
        continue;
      }

      if (entry.isFile() && entry.name === libraryName) {
        matches.push(fullPath);
      }
    }
  }

  return matches;
}

function chooseBestLibraryMatch(matches: string[]): string | undefined {
  if (matches.length === 0) {
    return undefined;
  }

  return [...new Set(matches)]
    .sort((left, right) => scoreLibraryMatch(right) - scoreLibraryMatch(left))[0];
}

function scoreLibraryMatch(candidate: string): number {
  const normalized = candidate.toLowerCase();
  let score = 0;

  if (normalized.includes(`${path.sep}publish${path.sep}`)) {
    score += 100;
  }

  if (normalized.includes(`${path.sep}release${path.sep}`)) {
    score += 25;
  }

  if (normalized.includes(`${path.sep}debug${path.sep}`)) {
    score -= 25;
  }

  const preferredRid = getPreferredRid();
  if (preferredRid && normalized.includes(`${path.sep}${preferredRid}${path.sep}`)) {
    score += 20;
  }

  score -= normalized.split(path.sep).length;
  return score;
}

function getPreferredRid(): string | undefined {
  switch (process.platform) {
    case "win32":
      return process.arch === "arm64" ? "win-arm64" : "win-x64";
    case "darwin":
      return process.arch === "arm64" ? "osx-arm64" : "osx-x64";
    default:
      return process.arch === "arm64" ? "linux-arm64" : "linux-x64";
  }
}

function formatLoadError(libraryPath: string, error: unknown): Error {
  const message = error instanceof Error ? error.message : String(error);
  if (message.includes("Cannot find function 'csharpdb_open'")) {
    return new Error(
      [
        `The library at '${libraryPath}' is not a NativeAOT publish output.`,
        "It is usually a regular `dotnet build` assembly, which does not export the C symbols used by the extension.",
        "Publish the native library instead:",
        suggestedPublishCommand()
      ].join(" ")
    );
  }

  return error instanceof Error ? error : new Error(message);
}

function suggestedPublishCommand(): string {
  switch (getPreferredRid()) {
    case "win-arm64":
      return "dotnet publish src\\CSharpDB.Native\\CSharpDB.Native.csproj -c Release -r win-arm64";
    case "win-x64":
      return "dotnet publish src\\CSharpDB.Native\\CSharpDB.Native.csproj -c Release -r win-x64";
    case "osx-arm64":
      return "dotnet publish src/CSharpDB.Native/CSharpDB.Native.csproj -c Release -r osx-arm64";
    case "osx-x64":
      return "dotnet publish src/CSharpDB.Native/CSharpDB.Native.csproj -c Release -r osx-x64";
    case "linux-arm64":
      return "dotnet publish src/CSharpDB.Native/CSharpDB.Native.csproj -c Release -r linux-arm64";
    default:
      return "dotnet publish src/CSharpDB.Native/CSharpDB.Native.csproj -c Release -r linux-x64";
  }
}
