export interface ProblemDetails {
  title?: string;
  status?: number;
  detail?: string;
}

export interface ColumnResponse {
  name: string;
  type: string;
  nullable: boolean;
  isPrimaryKey: boolean;
  isIdentity: boolean;
}

export interface TableSchemaResponse {
  tableName: string;
  columns: ColumnResponse[];
}

export interface BrowseResponse {
  columnNames: string[];
  rows: Array<Record<string, unknown>>;
  totalRows: number;
  page: number;
  pageSize: number;
  totalPages: number;
}

export interface RowCountResponse {
  tableName: string;
  count: number;
}

export interface MutationResponse {
  rowsAffected: number;
}

export interface IndexResponse {
  indexName: string;
  tableName: string;
  columns: string[];
  isUnique: boolean;
}

export interface ViewResponse {
  viewName: string;
  sql: string;
}

export interface TriggerResponse {
  triggerName: string;
  tableName: string;
  timing: string;
  event: string;
  bodySql: string;
}

export interface SqlResultResponse {
  isQuery: boolean;
  columnNames?: string[] | null;
  rows?: Array<Record<string, unknown>> | null;
  rowsAffected: number;
  error?: string | null;
  elapsedMs: number;
}

export interface DatabaseInfoResponse {
  dataSource: string;
  tableCount: number;
  indexCount: number;
  viewCount: number;
  triggerCount: number;
  procedureCount: number;
}

export interface RenameTableRequest {
  newName: string;
}

export interface AddColumnRequest {
  columnName: string;
  type: string;
  notNull?: boolean;
}

export interface RenameColumnRequest {
  newName: string;
}

export interface InsertRowRequest {
  values: Record<string, unknown>;
}

export interface UpdateRowRequest {
  values: Record<string, unknown>;
}

export interface CreateIndexRequest {
  indexName: string;
  tableName: string;
  columnName: string;
  isUnique?: boolean;
}

export interface UpdateIndexRequest {
  newIndexName: string;
  tableName: string;
  columnName: string;
  isUnique?: boolean;
}

export interface CreateViewRequest {
  viewName: string;
  selectSql: string;
}

export interface UpdateViewRequest {
  newViewName: string;
  selectSql: string;
}

export interface CreateTriggerRequest {
  triggerName: string;
  tableName: string;
  timing: string;
  event: string;
  bodySql: string;
}

export interface UpdateTriggerRequest {
  newTriggerName: string;
  tableName: string;
  timing: string;
  event: string;
  bodySql: string;
}

export interface ExecuteSqlRequest {
  sql: string;
}

export interface ProcedureParameterRequest {
  name: string;
  type: string;
  required: boolean;
  default?: unknown;
  description?: string;
}

export interface CreateProcedureRequest {
  name: string;
  bodySql: string;
  parameters?: ProcedureParameterRequest[] | null;
  description?: string | null;
  isEnabled?: boolean;
}

export interface UpdateProcedureRequest {
  newName: string;
  bodySql: string;
  parameters?: ProcedureParameterRequest[] | null;
  description?: string | null;
  isEnabled?: boolean;
}

export interface ExecuteProcedureRequest {
  args?: Record<string, unknown> | null;
}

export interface ProcedureParameterResponse {
  name: string;
  type: string;
  required: boolean;
  default?: unknown;
  description?: string | null;
}

export interface ProcedureSummaryResponse {
  name: string;
  description?: string | null;
  isEnabled: boolean;
  createdUtc: string;
  updatedUtc: string;
}

export interface ProcedureDetailResponse {
  name: string;
  bodySql: string;
  parameters: ProcedureParameterResponse[];
  description?: string | null;
  isEnabled: boolean;
  createdUtc: string;
  updatedUtc: string;
}

export interface ProcedureStatementResultResponse {
  statementIndex: number;
  statementText: string;
  isQuery: boolean;
  columnNames?: string[] | null;
  rows?: Array<Record<string, unknown>> | null;
  rowsAffected: number;
  elapsedMs: number;
}

export interface ProcedureExecutionResponse {
  procedureName: string;
  succeeded: boolean;
  statements: ProcedureStatementResultResponse[];
  error?: string | null;
  failedStatementIndex?: number | null;
  elapsedMs: number;
}

export type InspectSeverity = "info" | "warning" | "error";

export interface IntegrityIssue {
  code: string;
  severity: InspectSeverity;
  message: string;
  pageId?: number | null;
  offset?: number | null;
}

export interface FileHeaderReport {
  fileLengthBytes: number;
  physicalPageCount: number;
  magic: string;
  magicValid: boolean;
  version: number;
  versionValid: boolean;
  pageSize: number;
  pageSizeValid: boolean;
  declaredPageCount: number;
  declaredPageCountMatchesPhysical: boolean;
  schemaRootPage: number;
  freelistHead: number;
  changeCounter: number;
}

export interface LeafCellReport {
  cellIndex: number;
  cellOffset: number;
  headerBytes: number;
  cellTotalBytes: number;
  key?: number | null;
  payloadBytes: number;
}

export interface InteriorCellReport {
  cellIndex: number;
  cellOffset: number;
  headerBytes: number;
  cellTotalBytes: number;
  leftChildPage?: number | null;
  key?: number | null;
}

export interface PageReport {
  pageId: number;
  pageTypeCode: number;
  pageTypeName: string;
  baseOffset: number;
  cellCount: number;
  cellContentStart: number;
  rightChildOrNextLeaf: number;
  freeSpaceBytes: number;
  cellOffsets: number[];
  leafCells?: LeafCellReport[] | null;
  interiorCells?: InteriorCellReport[] | null;
}

export interface DatabaseInspectReport {
  schemaVersion: string;
  databasePath: string;
  header: FileHeaderReport;
  pageTypeHistogram: Record<string, number>;
  pageCountScanned: number;
  pages?: PageReport[] | null;
  issues: IntegrityIssue[];
}

export interface WalInspectReport {
  schemaVersion: string;
  databasePath: string;
  walPath: string;
  exists: boolean;
  fileLengthBytes: number;
  fullFrameCount: number;
  commitFrameCount: number;
  trailingBytes: number;
  magic: string;
  magicValid: boolean;
  version: number;
  versionValid: boolean;
  pageSize: number;
  pageSizeValid: boolean;
  salt1: number;
  salt2: number;
  issues: IntegrityIssue[];
}

export interface PageInspectReport {
  schemaVersion: string;
  databasePath: string;
  pageId: number;
  exists: boolean;
  page?: PageReport | null;
  hexDump?: string | null;
  issues: IntegrityIssue[];
}

export interface IndexCheckItem {
  indexName: string;
  tableName: string;
  columns: string[];
  rootPage: number;
  rootPageValid: boolean;
  tableExists: boolean;
  columnsExistInTable: boolean;
  rootTreeReachable: boolean;
}

export interface IndexInspectReport {
  schemaVersion: string;
  databasePath: string;
  requestedIndexName?: string | null;
  sampleSize: number;
  indexes: IndexCheckItem[];
  issues: IntegrityIssue[];
}

export type ReindexScope = "all" | "table" | "index";

export interface ReindexRequest {
  scope?: ReindexScope;
  name?: string | null;
}

export interface SpaceUsageReport {
  databaseFileBytes: number;
  walFileBytes: number;
  pageSizeBytes: number;
  physicalPageCount: number;
  declaredPageCount: number;
  freelistPageCount: number;
  freelistBytes: number;
}

export interface FragmentationReport {
  bTreeFreeBytes: number;
  pagesWithFreeSpace: number;
  tailFreelistPageCount: number;
  tailFreelistBytes: number;
}

export interface DatabaseMaintenanceReport {
  schemaVersion: string;
  databasePath: string;
  spaceUsage: SpaceUsageReport;
  fragmentation: FragmentationReport;
  pageTypeHistogram: Record<string, number>;
}

export interface ReindexResult {
  scope: ReindexScope;
  name?: string | null;
  rebuiltIndexCount: number;
}

export interface VacuumResult {
  databaseFileBytesBefore: number;
  databaseFileBytesAfter: number;
  physicalPageCountBefore: number;
  physicalPageCountAfter: number;
}
