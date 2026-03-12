import {
  AddColumnRequest,
  BrowseResponse,
  CreateIndexRequest,
  CreateProcedureRequest,
  CreateTriggerRequest,
  CreateViewRequest,
  DatabaseInfoResponse,
  DatabaseInspectReport,
  DatabaseMaintenanceReport,
  ExecuteProcedureRequest,
  IndexInspectReport,
  IndexResponse,
  MutationResponse,
  PageInspectReport,
  ProblemDetails,
  ProcedureDetailResponse,
  ProcedureExecutionResponse,
  ProcedureSummaryResponse,
  ReindexRequest,
  ReindexResult,
  RenameColumnRequest,
  RenameTableRequest,
  RowCountResponse,
  SqlResultResponse,
  TableSchemaResponse,
  TriggerResponse,
  UpdateIndexRequest,
  UpdateProcedureRequest,
  UpdateTriggerRequest,
  UpdateViewRequest,
  ViewResponse,
  VacuumResult,
  WalInspectReport
} from "./types";
import { NativeDatabase, NativeDatabaseError } from "./nativeDatabase";
import { normalizeProcedureParameters, parseProcedureParameters, serializeProcedureParameters } from "./procedureHelpers";
import {
  PROCEDURE_ENABLED_INDEX_NAME,
  PROCEDURE_TABLE_NAME,
  SAVED_QUERY_NAME_INDEX_NAME,
  SAVED_QUERY_TABLE_NAME,
  asBoolean,
  asNullableString,
  asNumber,
  asString,
  buildCreateIndexSql,
  buildCreateTriggerSql,
  formatSqlLiteral,
  isInternalTable,
  normalizeEmbeddedSql,
  normalizePage,
  normalizePageSize,
  normalizeTypeName,
  quoteIdentifier,
  requireNonEmpty,
  toErrorMessage
} from "./sqlHelpers";
import { splitExecutableStatements } from "../utils/sqlScriptSplitter";

export class CSharpDbApiError extends Error {
  readonly status: number;
  readonly code: number;
  readonly problem?: ProblemDetails;

  constructor(message: string, code = -1, problem?: ProblemDetails) {
    super(message);
    this.name = "CSharpDbApiError";
    this.status = code;
    this.code = code;
    this.problem = problem;
  }
}

export class CSharpDbApiClient {
  private readonly database: NativeDatabase;
  private catalogsInitialized = false;

  constructor(nativeLibraryPath = "") {
    this.database = new NativeDatabase(nativeLibraryPath);
  }

  setBaseUrl(value: string): void {
    this.setNativeLibraryPath(value);
  }

  getBaseUrl(): string {
    return this.getNativeLibraryPath();
  }

  setNativeLibraryPath(value: string | undefined): void {
    this.database.setNativeLibraryPath(value);
  }

  getNativeLibraryPath(): string {
    return this.database.getNativeLibraryPath();
  }

  getResolvedNativeLibraryPath(): string {
    return this.database.getResolvedNativeLibraryPath();
  }

  getDatabasePath(): string | undefined {
    return this.database.getDatabasePath();
  }

  get isConnected(): boolean {
    return this.database.isOpen;
  }

  async open(databasePath: string): Promise<DatabaseInfoResponse> {
    try {
      await this.database.open(databasePath);
      this.catalogsInitialized = false;
      await this.ensureCatalogsInitialized();
      return await this.getDatabaseInfo();
    } catch (error) {
      throw wrapError(error);
    }
  }

  async close(): Promise<void> {
    await this.database.close();
    this.catalogsInitialized = false;
  }

  async getTableNames(): Promise<string[]> {
    return this.queryRows(
      "SELECT table_name FROM sys.tables ORDER BY table_name;"
    )
      .map((row) => asString(row.table_name))
      .filter((name): name is string => typeof name === "string" && !isInternalTable(name));
  }

  async getTableSchema(name: string): Promise<TableSchemaResponse> {
    const tableName = requireNonEmpty(name, "Table name is required.");
    const rows = this.queryRows(
      `SELECT column_name, ordinal_position, data_type, is_nullable, is_primary_key, is_identity
       FROM sys.columns
       WHERE table_name = ${formatSqlLiteral(tableName)}
       ORDER BY ordinal_position;`
    );

    if (rows.length === 0) {
      throw new Error(`Table '${tableName}' was not found.`);
    }

    return {
      tableName,
      columns: rows.map((row) => ({
        name: asString(row.column_name) ?? "",
        type: (asString(row.data_type) ?? "TEXT").toUpperCase(),
        nullable: asBoolean(row.is_nullable),
        isPrimaryKey: asBoolean(row.is_primary_key),
        isIdentity: asBoolean(row.is_identity)
      }))
    };
  }

  async getRowCount(name: string): Promise<RowCountResponse> {
    const tableName = requireNonEmpty(name, "Table name is required.");
    const result = this.queryRows(`SELECT COUNT(*) AS count FROM ${quoteIdentifier(tableName)};`);
    return {
      tableName,
      count: asNumber(result[0]?.count) ?? 0
    };
  }

  async dropTable(name: string): Promise<void> {
    this.executeStatement(`DROP TABLE ${quoteIdentifier(name)};`);
  }

  async renameTable(name: string, request: RenameTableRequest): Promise<void> {
    this.executeStatement(`ALTER TABLE ${quoteIdentifier(name)} RENAME TO ${quoteIdentifier(request.newName)};`);
  }

  async addColumn(name: string, request: AddColumnRequest): Promise<void> {
    const parts = [
      "ALTER TABLE",
      quoteIdentifier(name),
      "ADD COLUMN",
      quoteIdentifier(request.columnName),
      normalizeTypeName(request.type)
    ];

    if (request.notNull) {
      parts.push("NOT NULL");
    }

    this.executeStatement(`${parts.join(" ")};`);
  }

  async dropColumn(name: string, columnName: string): Promise<void> {
    this.executeStatement(`ALTER TABLE ${quoteIdentifier(name)} DROP COLUMN ${quoteIdentifier(columnName)};`);
  }

  async renameColumn(name: string, columnName: string, request: RenameColumnRequest): Promise<void> {
    this.executeStatement(
      `ALTER TABLE ${quoteIdentifier(name)} RENAME COLUMN ${quoteIdentifier(columnName)} TO ${quoteIdentifier(request.newName)};`
    );
  }

  async browseRows(name: string, page = 1, pageSize = 50): Promise<BrowseResponse> {
    return this.browseObjectRows(name, page, pageSize);
  }

  async getRowByPk(name: string, pkValue: string | number, pkColumn?: string): Promise<Record<string, unknown>> {
    const schema = await this.getTableSchema(name);
    const primaryKeyColumn = pkColumn ?? schema.columns.find((column) => column.isPrimaryKey)?.name;
    if (!primaryKeyColumn) {
      throw new Error(`Table '${name}' has no primary key.`);
    }

    const rows = this.queryRows(
      `SELECT * FROM ${quoteIdentifier(name)} WHERE ${quoteIdentifier(primaryKeyColumn)} = ${formatSqlLiteral(pkValue)};`
    );

    if (rows.length === 0) {
      throw new Error(`Row '${pkValue}' was not found in '${name}'.`);
    }

    return rows[0];
  }

  async insertRow(name: string, values: Record<string, unknown>): Promise<MutationResponse> {
    const entries = Object.entries(values);
    if (entries.length === 0) {
      throw new Error("Insert requires at least one value.");
    }

    const rowsAffected = this.executeNonQuery(
      `INSERT INTO ${quoteIdentifier(name)} (${entries.map(([key]) => quoteIdentifier(key)).join(", ")})
       VALUES (${entries.map(([, value]) => formatSqlLiteral(value)).join(", ")});`
    );

    return { rowsAffected };
  }

  async updateRow(
    name: string,
    pkValue: string | number,
    values: Record<string, unknown>,
    pkColumn?: string
  ): Promise<MutationResponse> {
    const schema = await this.getTableSchema(name);
    const primaryKeyColumn = pkColumn ?? schema.columns.find((column) => column.isPrimaryKey)?.name;
    if (!primaryKeyColumn) {
      throw new Error(`Table '${name}' has no primary key.`);
    }

    const rowsAffected = this.executeNonQuery(
      `UPDATE ${quoteIdentifier(name)}
       SET ${Object.entries(values).map(([key, value]) => `${quoteIdentifier(key)} = ${formatSqlLiteral(value)}`).join(", ")}
       WHERE ${quoteIdentifier(primaryKeyColumn)} = ${formatSqlLiteral(pkValue)};`
    );

    return { rowsAffected };
  }

  async deleteRow(name: string, pkValue: string | number, pkColumn?: string): Promise<MutationResponse> {
    const schema = await this.getTableSchema(name);
    const primaryKeyColumn = pkColumn ?? schema.columns.find((column) => column.isPrimaryKey)?.name;
    if (!primaryKeyColumn) {
      throw new Error(`Table '${name}' has no primary key.`);
    }

    const rowsAffected = this.executeNonQuery(
      `DELETE FROM ${quoteIdentifier(name)}
       WHERE ${quoteIdentifier(primaryKeyColumn)} = ${formatSqlLiteral(pkValue)};`
    );

    return { rowsAffected };
  }

  async getIndexes(): Promise<IndexResponse[]> {
    const rows = this.queryRows(
      `SELECT index_name, table_name, column_name, ordinal_position, is_unique
       FROM sys.indexes
       ORDER BY index_name, ordinal_position;`
    );

    const grouped = new Map<string, IndexResponse>();
    for (const row of rows) {
      const indexName = asString(row.index_name);
      const tableName = asString(row.table_name);
      const columnName = asString(row.column_name);
      if (!indexName || !tableName || !columnName || isInternalTable(tableName)) {
        continue;
      }

      const existing = grouped.get(indexName);
      if (existing) {
        existing.columns.push(columnName);
      } else {
        grouped.set(indexName, {
          indexName,
          tableName,
          columns: [columnName],
          isUnique: asBoolean(row.is_unique)
        });
      }
    }

    return [...grouped.values()].sort((left, right) =>
      left.indexName.localeCompare(right.indexName, undefined, { sensitivity: "base" })
    );
  }

  async createIndex(request: CreateIndexRequest): Promise<IndexResponse> {
    this.executeStatement(`${buildCreateIndexSql(request.indexName, request.tableName, request.columnName, request.isUnique ?? false)};`);
    return {
      indexName: request.indexName,
      tableName: request.tableName,
      columns: [request.columnName],
      isUnique: request.isUnique ?? false
    };
  }

  async updateIndex(name: string, request: UpdateIndexRequest): Promise<IndexResponse> {
    this.executeStatementsInTransaction([
      `DROP INDEX ${quoteIdentifier(name)};`,
      `${buildCreateIndexSql(request.newIndexName, request.tableName, request.columnName, request.isUnique ?? false)};`
    ]);

    return {
      indexName: request.newIndexName,
      tableName: request.tableName,
      columns: [request.columnName],
      isUnique: request.isUnique ?? false
    };
  }

  async dropIndex(name: string): Promise<void> {
    this.executeStatement(`DROP INDEX ${quoteIdentifier(name)};`);
  }

  async getViews(): Promise<ViewResponse[]> {
    return this.queryRows("SELECT view_name, sql FROM sys.views ORDER BY view_name;").map((row) => ({
      viewName: asString(row.view_name) ?? "",
      sql: asString(row.sql) ?? ""
    }));
  }

  async getView(name: string): Promise<ViewResponse> {
    const rows = this.queryRows(
      `SELECT view_name, sql FROM sys.views WHERE view_name = ${formatSqlLiteral(name)};`
    );

    if (rows.length === 0) {
      throw new Error(`View '${name}' was not found.`);
    }

    return {
      viewName: asString(rows[0].view_name) ?? name,
      sql: asString(rows[0].sql) ?? ""
    };
  }

  async browseViewRows(name: string, page = 1, pageSize = 50): Promise<BrowseResponse> {
    return this.browseObjectRows(name, page, pageSize);
  }

  async createView(request: CreateViewRequest): Promise<ViewResponse> {
    const sql = normalizeEmbeddedSql(request.selectSql);
    this.executeStatement(`CREATE VIEW ${quoteIdentifier(request.viewName)} AS ${sql};`);
    return { viewName: request.viewName, sql };
  }

  async updateView(name: string, request: UpdateViewRequest): Promise<ViewResponse> {
    const sql = normalizeEmbeddedSql(request.selectSql);
    this.executeStatementsInTransaction([
      `DROP VIEW ${quoteIdentifier(name)};`,
      `CREATE VIEW ${quoteIdentifier(request.newViewName)} AS ${sql};`
    ]);

    return { viewName: request.newViewName, sql };
  }

  async dropView(name: string): Promise<void> {
    this.executeStatement(`DROP VIEW ${quoteIdentifier(name)};`);
  }

  async getTriggers(): Promise<TriggerResponse[]> {
    return this.queryRows(
      `SELECT trigger_name, table_name, timing, event, body_sql
       FROM sys.triggers
       ORDER BY trigger_name;`
    ).map((row) => ({
      triggerName: asString(row.trigger_name) ?? "",
      tableName: asString(row.table_name) ?? "",
      timing: asString(row.timing) ?? "",
      event: asString(row.event) ?? "",
      bodySql: asString(row.body_sql) ?? ""
    }));
  }

  async createTrigger(request: CreateTriggerRequest): Promise<TriggerResponse> {
    this.executeStatement(`${buildCreateTriggerSql(request.triggerName, request.tableName, request.timing, request.event, request.bodySql)};`);
    return {
      triggerName: request.triggerName,
      tableName: request.tableName,
      timing: request.timing,
      event: request.event,
      bodySql: request.bodySql
    };
  }

  async updateTrigger(name: string, request: UpdateTriggerRequest): Promise<TriggerResponse> {
    this.executeStatementsInTransaction([
      `DROP TRIGGER ${quoteIdentifier(name)};`,
      `${buildCreateTriggerSql(request.newTriggerName, request.tableName, request.timing, request.event, request.bodySql)};`
    ]);

    return {
      triggerName: request.newTriggerName,
      tableName: request.tableName,
      timing: request.timing,
      event: request.event,
      bodySql: request.bodySql
    };
  }

  async dropTrigger(name: string): Promise<void> {
    this.executeStatement(`DROP TRIGGER ${quoteIdentifier(name)};`);
  }

  async getProcedures(includeDisabled = true): Promise<ProcedureSummaryResponse[]> {
    await this.ensureCatalogsInitialized();
    const whereClause = includeDisabled ? "" : " WHERE is_enabled = 1";
    return this.queryRows(
      `SELECT name, description, is_enabled, created_utc, updated_utc
       FROM ${PROCEDURE_TABLE_NAME}${whereClause}
       ORDER BY name;`
    ).map((row) => ({
      name: asString(row.name) ?? "",
      description: asNullableString(row.description),
      isEnabled: asBoolean(row.is_enabled),
      createdUtc: asString(row.created_utc) ?? "",
      updatedUtc: asString(row.updated_utc) ?? ""
    }));
  }

  async getProcedure(name: string): Promise<ProcedureDetailResponse> {
    await this.ensureCatalogsInitialized();
    const rows = this.queryRows(
      `SELECT name, body_sql, params_json, description, is_enabled, created_utc, updated_utc
       FROM ${PROCEDURE_TABLE_NAME}
       WHERE name = ${formatSqlLiteral(name)};`
    );

    if (rows.length === 0) {
      throw new Error(`Procedure '${name}' was not found.`);
    }

    return {
      name: asString(rows[0].name) ?? "",
      bodySql: asString(rows[0].body_sql) ?? "",
      parameters: normalizeProcedureParameters(parseProcedureParameters(rows[0].params_json)),
      description: asNullableString(rows[0].description),
      isEnabled: asBoolean(rows[0].is_enabled),
      createdUtc: asString(rows[0].created_utc) ?? "",
      updatedUtc: asString(rows[0].updated_utc) ?? ""
    };
  }

  async createProcedure(request: CreateProcedureRequest): Promise<ProcedureDetailResponse> {
    await this.ensureCatalogsInitialized();
    const timestamp = new Date().toISOString();
    const bodySql = normalizeEmbeddedSql(request.bodySql);
    this.executeStatement(
      `INSERT INTO ${PROCEDURE_TABLE_NAME}
         (name, body_sql, params_json, description, is_enabled, created_utc, updated_utc)
       VALUES (
         ${formatSqlLiteral(request.name)},
         ${formatSqlLiteral(bodySql)},
         ${formatSqlLiteral(serializeProcedureParameters(request.parameters ?? []))},
         ${formatSqlLiteral(request.description ?? null)},
         ${(request.isEnabled ?? true) ? 1 : 0},
         ${formatSqlLiteral(timestamp)},
         ${formatSqlLiteral(timestamp)}
       );`
    );

    return {
      name: request.name,
      bodySql,
      parameters: normalizeProcedureParameters(request.parameters ?? []),
      description: request.description ?? null,
      isEnabled: request.isEnabled ?? true,
      createdUtc: timestamp,
      updatedUtc: timestamp
    };
  }

  async updateProcedure(name: string, request: UpdateProcedureRequest): Promise<ProcedureDetailResponse> {
    await this.ensureCatalogsInitialized();
    const existing = await this.getProcedure(name);
    const updatedUtc = new Date().toISOString();
    const bodySql = normalizeEmbeddedSql(request.bodySql);
    this.executeStatement(
      `UPDATE ${PROCEDURE_TABLE_NAME}
       SET name = ${formatSqlLiteral(request.newName)},
           body_sql = ${formatSqlLiteral(bodySql)},
           params_json = ${formatSqlLiteral(serializeProcedureParameters(request.parameters ?? []))},
           description = ${formatSqlLiteral(request.description ?? null)},
           is_enabled = ${(request.isEnabled ?? true) ? 1 : 0},
           created_utc = ${formatSqlLiteral(existing.createdUtc)},
           updated_utc = ${formatSqlLiteral(updatedUtc)}
       WHERE name = ${formatSqlLiteral(name)};`
    );

    return {
      name: request.newName,
      bodySql,
      parameters: normalizeProcedureParameters(request.parameters ?? []),
      description: request.description ?? null,
      isEnabled: request.isEnabled ?? true,
      createdUtc: existing.createdUtc,
      updatedUtc
    };
  }

  async deleteProcedure(name: string): Promise<void> {
    await this.ensureCatalogsInitialized();
    const rowsAffected = this.executeNonQuery(
      `DELETE FROM ${PROCEDURE_TABLE_NAME} WHERE name = ${formatSqlLiteral(name)};`
    );

    if (rowsAffected === 0) {
      throw new Error(`Procedure '${name}' was not found.`);
    }
  }

  async executeProcedure(_name: string, _request: ExecuteProcedureRequest): Promise<ProcedureExecutionResponse> {
    throw new Error("Structured procedure execution is not supported by the NativeAOT extension client.");
  }

  async executeSql(sql: string): Promise<SqlResultResponse> {
    const startedAt = Date.now();
    const statements = splitExecutableStatements(sql);
    if (statements.length === 0) {
      return { isQuery: false, rowsAffected: 0, elapsedMs: Date.now() - startedAt };
    }

    let lastResult: ReturnType<NativeDatabase["execute"]> | undefined;
    let totalRowsAffected = 0;

    for (let index = 0; index < statements.length; index++) {
      try {
        const result = this.database.execute(statements[index]);
        lastResult = result;
        if (!result.isQuery) {
          totalRowsAffected += result.rowsAffected;
        }
      } catch (error) {
        const prefix = statements.length > 1 ? `Statement ${index + 1} failed: ` : "";
        return {
          isQuery: false,
          rowsAffected: totalRowsAffected,
          error: `${prefix}${toErrorMessage(wrapError(error))}`,
          elapsedMs: Date.now() - startedAt
        };
      }
    }

    if (!lastResult) {
      return { isQuery: false, rowsAffected: 0, elapsedMs: Date.now() - startedAt };
    }

    if (lastResult.isQuery) {
      return {
        isQuery: true,
        columnNames: lastResult.columnNames,
        rows: lastResult.rows,
        rowsAffected: lastResult.rows.length,
        elapsedMs: Date.now() - startedAt
      };
    }

    return {
      isQuery: false,
      rowsAffected: totalRowsAffected,
      elapsedMs: Date.now() - startedAt
    };
  }

  async getDatabaseInfo(): Promise<DatabaseInfoResponse> {
    const [tables, indexes, views, triggers, procedures] = await Promise.all([
      this.getTableNames(),
      this.getIndexes(),
      this.getViews(),
      this.getTriggers(),
      this.getProcedures()
    ]);

    const dataSource = this.database.getDatabasePath();
    if (!dataSource) {
      throw new Error("No database is open.");
    }

    return {
      dataSource,
      tableCount: tables.length,
      indexCount: indexes.length,
      viewCount: views.length,
      triggerCount: triggers.length,
      procedureCount: procedures.length
    };
  }

  async inspectStorage(includePages = false, databasePath?: string): Promise<DatabaseInspectReport> {
    return this.readJsonResult(databasePath, (resolvedPath) =>
      this.database.readJsonResultForPath(resolvedPath, (native, dbPath) => native.csharpdb_inspect_storage_json(dbPath, includePages ? 1 : 0))
    );
  }

  async inspectWal(databasePath?: string): Promise<WalInspectReport> {
    return this.readJsonResult(databasePath, (resolvedPath) =>
      this.database.readJsonResultForPath(resolvedPath, (native, dbPath) => native.csharpdb_inspect_wal_json(dbPath))
    );
  }

  async inspectPage(pageId: number, includeHex = false, databasePath?: string): Promise<PageInspectReport> {
    return this.readJsonResult(databasePath, (resolvedPath) =>
      this.database.readJsonResultForPath(resolvedPath, (native, dbPath) => native.csharpdb_inspect_page_json(dbPath, pageId, includeHex ? 1 : 0))
    );
  }

  async checkIndexes(index?: string, sample?: number, databasePath?: string): Promise<IndexInspectReport> {
    return this.readJsonResult(databasePath, (resolvedPath) =>
      this.database.readJsonResultForPath(
        resolvedPath,
        (native, dbPath) => native.csharpdb_check_indexes_json(dbPath, index?.trim() ?? "", sample ?? 0)
      )
    );
  }

  async getMaintenanceReport(): Promise<DatabaseMaintenanceReport> {
    return this.database.readJsonResult((native, dbPath) => native.csharpdb_get_maintenance_report_json(dbPath));
  }

  async reindex(request: ReindexRequest): Promise<ReindexResult> {
    const scopeMap: Record<string, number> = { all: 0, table: 1, index: 2 };
    return this.database.withClosedDatabase(async (databasePath) =>
      this.database.readJsonResultForPath(
        databasePath,
        (native, dbPath) => native.csharpdb_reindex_json(dbPath, scopeMap[request.scope ?? "all"] ?? 0, request.name?.trim() ?? "")
      )
    );
  }

  async vacuum(): Promise<VacuumResult> {
    return this.database.withClosedDatabase(async (databasePath) =>
      this.database.readJsonResultForPath(databasePath, (native, dbPath) => native.csharpdb_vacuum_json(dbPath))
    );
  }

  private queryRows(sql: string): Array<Record<string, unknown>> {
    try {
      return this.database.execute(sql).rows;
    } catch (error) {
      throw wrapError(error);
    }
  }

  private executeStatement(sql: string): void {
    try {
      void this.database.execute(sql);
    } catch (error) {
      throw wrapError(error);
    }
  }

  private executeNonQuery(sql: string): number {
    try {
      return this.database.execute(sql).rowsAffected;
    } catch (error) {
      throw wrapError(error);
    }
  }

  private async browseObjectRows(name: string, page: number, pageSize: number): Promise<BrowseResponse> {
    const normalizedPage = normalizePage(page);
    const normalizedPageSize = normalizePageSize(pageSize);
    const result = this.database.execute(`SELECT * FROM ${quoteIdentifier(name)};`);
    const skip = (normalizedPage - 1) * normalizedPageSize;
    const totalRows = result.rows.length;
    return {
      columnNames: result.columnNames,
      rows: result.rows.slice(skip, skip + normalizedPageSize),
      totalRows,
      page: normalizedPage,
      pageSize: normalizedPageSize,
      totalPages: Math.max(1, Math.ceil(totalRows / normalizedPageSize))
    };
  }

  private async ensureCatalogsInitialized(): Promise<void> {
    if (this.catalogsInitialized) {
      return;
    }

    this.executeStatement(
      `CREATE TABLE IF NOT EXISTS ${PROCEDURE_TABLE_NAME} (
         name TEXT PRIMARY KEY,
         body_sql TEXT NOT NULL,
         params_json TEXT NOT NULL,
         description TEXT,
         is_enabled INTEGER NOT NULL,
         created_utc TEXT NOT NULL,
         updated_utc TEXT NOT NULL
       );`
    );

    this.executeStatement(
      `CREATE INDEX IF NOT EXISTS ${PROCEDURE_ENABLED_INDEX_NAME}
       ON ${PROCEDURE_TABLE_NAME} (is_enabled);`
    );

    this.executeStatement(
      `CREATE TABLE IF NOT EXISTS ${SAVED_QUERY_TABLE_NAME} (
         id INTEGER PRIMARY KEY IDENTITY,
         name TEXT NOT NULL,
         sql_text TEXT NOT NULL,
         created_utc TEXT NOT NULL,
         updated_utc TEXT NOT NULL
       );`
    );

    this.executeStatement(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${SAVED_QUERY_NAME_INDEX_NAME}
       ON ${SAVED_QUERY_TABLE_NAME} (name);`
    );

    this.catalogsInitialized = true;
  }

  private readJsonResult<T>(databasePath: string | undefined, reader: (resolvedPath: string) => T): T {
    try {
      const resolvedPath = databasePath?.trim().length
        ? requireNonEmpty(databasePath, "Database path is required.")
        : (() => {
            const current = this.database.getDatabasePath();
            if (!current) {
              throw new Error("No database is open.");
            }

            return current;
          })();

      return reader(resolvedPath);
    } catch (error) {
      throw wrapError(error);
    }
  }

  private executeStatementsInTransaction(statements: string[]): void {
    try {
      this.database.executeInTransaction(statements);
    } catch (error) {
      throw wrapError(error);
    }
  }
}

function wrapError(error: unknown): CSharpDbApiError {
  if (error instanceof CSharpDbApiError) {
    return error;
  }

  if (error instanceof NativeDatabaseError) {
    return new CSharpDbApiError(error.message, error.code);
  }

  return new CSharpDbApiError(toErrorMessage(error));
}
