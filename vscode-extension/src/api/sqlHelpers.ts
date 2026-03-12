export const PROCEDURE_TABLE_NAME = "__procedures";
export const SAVED_QUERY_TABLE_NAME = "__saved_queries";
export const PROCEDURE_ENABLED_INDEX_NAME = "idx___procedures_is_enabled";
export const SAVED_QUERY_NAME_INDEX_NAME = "idx___saved_queries_name";

export function normalizePage(page: number): number {
  return page < 1 ? 1 : page;
}

export function normalizePageSize(pageSize: number): number {
  if (pageSize < 1) {
    return 50;
  }

  return Math.min(pageSize, 1000);
}

export function requireNonEmpty(value: string, message: string): string {
  if (value.trim().length === 0) {
    throw new Error(message);
  }

  return value.trim();
}

export function quoteIdentifier(identifier: string): string {
  const normalized = requireNonEmpty(identifier, "Identifier is required.");
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(normalized)
    ? normalized
    : `"${normalized.replaceAll("\"", "\"\"")}"`;
}

export function normalizeEmbeddedSql(sql: string): string {
  const trimmed = requireNonEmpty(sql, "SQL is required.");
  return trimmed.replace(/;+$/u, "").trimEnd();
}

export function normalizeTypeName(type: string): string {
  return requireNonEmpty(type, "Column type is required.").toUpperCase();
}

export function buildCreateIndexSql(indexName: string, tableName: string, columnName: string, isUnique: boolean): string {
  const unique = isUnique ? "UNIQUE " : "";
  return `CREATE ${unique}INDEX ${quoteIdentifier(indexName)} ON ${quoteIdentifier(tableName)} (${quoteIdentifier(columnName)})`;
}

export function buildCreateTriggerSql(
  triggerName: string,
  tableName: string,
  timing: string,
  event: string,
  bodySql: string
): string {
  return [
    "CREATE TRIGGER",
    quoteIdentifier(triggerName),
    requireNonEmpty(timing, "Trigger timing is required.").toUpperCase(),
    requireNonEmpty(event, "Trigger event is required.").toUpperCase(),
    "ON",
    quoteIdentifier(tableName),
    "BEGIN",
    normalizeEmbeddedSql(bodySql),
    "END"
  ].join(" ");
}

export function formatSqlLiteral(value: unknown): string {
  if (value === null || value === undefined) {
    return "NULL";
  }

  if (typeof value === "bigint") {
    return value.toString();
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? String(value) : "NULL";
  }

  if (typeof value === "boolean") {
    return value ? "1" : "0";
  }

  if (typeof value === "string") {
    return `'${value.replaceAll("'", "''")}'`;
  }

  if (Buffer.isBuffer(value)) {
    return `X'${value.toString("hex")}'`;
  }

  if (value instanceof Uint8Array) {
    return `X'${Buffer.from(value).toString("hex")}'`;
  }

  return `'${JSON.stringify(value).replaceAll("'", "''")}'`;
}

export function asString(value: unknown): string | undefined {
  return typeof value === "string" ? value : value === undefined || value === null ? undefined : String(value);
}

export function asNullableString(value: unknown): string | null | undefined {
  return value === null || value === undefined ? value as null | undefined : String(value);
}

export function asBoolean(value: unknown): boolean {
  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    return value !== 0;
  }

  if (typeof value === "string") {
    return value !== "0" && value.toLowerCase() !== "false";
  }

  return false;
}

export function asNumber(value: unknown): number | undefined {
  if (typeof value === "number") {
    return value;
  }

  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }

  return undefined;
}

export function isInternalTable(tableName: string): boolean {
  return tableName.startsWith("_col_") ||
    tableName.localeCompare(PROCEDURE_TABLE_NAME, undefined, { sensitivity: "base" }) === 0 ||
    tableName.localeCompare(SAVED_QUERY_TABLE_NAME, undefined, { sensitivity: "base" }) === 0;
}

export function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
