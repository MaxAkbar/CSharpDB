import * as vscode from "vscode";
import { CSharpDbApiClient } from "../api/client";
import { BrowseResponse, ColumnResponse, ViewResponse } from "../api/types";
import { renderWebviewPage } from "../utils/htmlBuilder";
import { closeWorkbenchPanels, registerCSharpDbPanel, unregisterCSharpDbPanel } from "./panelRegistry";

type BrowserObjectType = "table" | "view";

interface BrowserColumnDescriptor {
  name: string;
  type?: string;
  isPrimaryKey: boolean;
  nullable?: boolean;
  isIdentity?: boolean;
}

interface BrowserRow {
  id: string;
  originalPk?: unknown;
  values: Record<string, unknown>;
}

interface BrowserState {
  objectType: BrowserObjectType;
  objectName: string;
  readOnly: boolean;
  primaryKeyColumn?: string;
  columns: BrowserColumnDescriptor[];
  rows: BrowserRow[];
  page: number;
  pageSize: number;
  totalPages: number;
  totalRows: number;
  viewDefinition?: ViewResponse;
  statusMessage?: string;
}

interface SaveChangesPayload {
  newRows: Array<Record<string, unknown>>;
  updatedRows: Array<{ originalPk: unknown; values: Record<string, unknown> }>;
  deletedRows: Array<{ originalPk: unknown }>;
}

export class DataBrowserPanel implements vscode.Disposable {
  private static readonly panels = new Map<string, DataBrowserPanel>();
  private static sharedPanel?: DataBrowserPanel;

  static createOrReveal(
    extensionUri: vscode.Uri,
    client: CSharpDbApiClient,
    objectType: BrowserObjectType,
    objectName: string
  ): void {
    if (vscode.workspace.getConfiguration("csharpdb").get<boolean>("reuseDataBrowserTab", true)) {
      if (DataBrowserPanel.sharedPanel) {
        closeWorkbenchPanels(DataBrowserPanel.sharedPanel.panel);
        DataBrowserPanel.sharedPanel.reveal();
        void DataBrowserPanel.sharedPanel.openObject(objectType, objectName);
        return;
      }

      closeWorkbenchPanels();
      const panel = DataBrowserPanel.createPanel(`${objectName} Data`, vscode.ViewColumn.Active);
      DataBrowserPanel.sharedPanel = new DataBrowserPanel(panel, extensionUri, client, objectType, objectName, true);
      return;
    }

    const key = `${objectType}:${objectName}`;
    const existing = DataBrowserPanel.panels.get(key);
    if (existing) {
      existing.reveal();
      void existing.loadAndRender();
      return;
    }

    const panel = DataBrowserPanel.createPanel(`${objectName} Data`, vscode.ViewColumn.Active);

    const instance = new DataBrowserPanel(panel, extensionUri, client, objectType, objectName, false);
    DataBrowserPanel.panels.set(key, instance);
  }

  private static createPanel(title: string, viewColumn: vscode.ViewColumn): vscode.WebviewPanel {
    return vscode.window.createWebviewPanel(
      "csharpdb.dataBrowser",
      title,
      viewColumn,
      {
        enableScripts: true,
        retainContextWhenHidden: true
      }
    );
  }

  private readonly disposables: vscode.Disposable[] = [];
  private readonly shared: boolean;
  private loadVersion = 0;
  private page = 1;
  private pageSize = 25;
  private lastStatusMessage?: string;

  private constructor(
    private readonly panel: vscode.WebviewPanel,
    private readonly extensionUri: vscode.Uri,
    private readonly client: CSharpDbApiClient,
    private objectType: BrowserObjectType,
    private objectName: string,
    shared: boolean
  ) {
    this.shared = shared;
    registerCSharpDbPanel(panel, "workbench");

    this.disposables.push(
      panel.onDidDispose(() => {
        unregisterCSharpDbPanel(panel);
        if (this.shared) {
          DataBrowserPanel.sharedPanel = undefined;
        } else {
          DataBrowserPanel.panels.delete(this.key);
        }
        this.dispose();
      }),
      panel.webview.onDidReceiveMessage(async (message) => {
        switch (message?.type) {
          case "ready":
          case "refresh":
            await this.loadAndRender();
            break;
          case "changePage":
            this.page = Number(message.page) || 1;
            await this.loadAndRender();
            break;
          case "setPageSize":
            this.pageSize = Number(message.pageSize) || 25;
            this.page = 1;
            await this.loadAndRender();
            break;
          case "saveChanges":
            await this.saveChanges(message.payload as SaveChangesPayload);
            break;
          case "saveView":
            await this.saveView(message.payload as { viewName: string; sql: string });
            break;
          case "dropView":
            await this.dropView();
            break;
          default:
            break;
        }
      })
    );

    this.panel.webview.html = renderWebviewPage({
      title: this.title,
      body: '<div id="app"></div>',
      extensionUri,
      webview: panel.webview,
      styles: ["media/styles/panel.css", "media/styles/grid.css"],
      scripts: ["media/scripts/dataBrowser.js"],
      inlineScript: "window.__CDB_BROWSER_INITIAL_STATE__ = null;"
    });

    void this.loadAndRender();
  }

  dispose(): void {
    while (this.disposables.length > 0) {
      this.disposables.pop()?.dispose();
    }
  }

  private get key(): string {
    return `${this.objectType}:${this.objectName}`;
  }

  private get title(): string {
    return `${this.objectName} Data`;
  }

  private reveal(): void {
    this.panel.reveal(this.panel.viewColumn ?? vscode.ViewColumn.Active, true);
  }

  private async openObject(objectType: BrowserObjectType, objectName: string): Promise<void> {
    const objectChanged = this.objectType !== objectType || this.objectName !== objectName;
    this.objectType = objectType;
    this.objectName = objectName;
    if (objectChanged) {
      this.page = 1;
      this.lastStatusMessage = undefined;
    }

    this.panel.title = this.title;
    await this.loadAndRender();
  }

  private async loadAndRender(): Promise<void> {
    const loadVersion = ++this.loadVersion;

    try {
      const state = await this.loadState();
      if (loadVersion !== this.loadVersion) {
        return;
      }

      await this.panel.webview.postMessage({ type: "state", state });
      this.lastStatusMessage = undefined;
    } catch (error) {
      if (loadVersion !== this.loadVersion) {
        return;
      }

      await this.panel.webview.postMessage({
        type: "state",
        state: {
          objectType: this.objectType,
          objectName: this.objectName,
          readOnly: true,
          columns: [],
          rows: [],
          page: this.page,
          pageSize: this.pageSize,
          totalPages: 1,
          totalRows: 0,
          statusMessage: error instanceof Error ? error.message : String(error)
        } satisfies BrowserState
      });
    }
  }

  private async loadState(): Promise<BrowserState> {
    if (this.objectType === "table") {
      const [schema, browse] = await Promise.all([
        this.client.getTableSchema(this.objectName),
        this.client.browseRows(this.objectName, this.page, this.pageSize)
      ]);

      const primaryKeyColumn = schema.columns.find((column) => column.isPrimaryKey)?.name;
      const readOnly = !primaryKeyColumn;
      return this.toState({
        objectType: "table",
        objectName: this.objectName,
        columns: schema.columns.map(mapColumn),
        browse,
        readOnly,
        primaryKeyColumn,
        statusMessage: readOnly ? "This table has no primary key, so editing is disabled." : this.lastStatusMessage
      });
    }

    const [browse, viewDefinition] = await Promise.all([
      this.client.browseViewRows(this.objectName, this.page, this.pageSize),
      this.client.getView(this.objectName)
    ]);
    return this.toState({
      objectType: "view",
      objectName: this.objectName,
      columns: browse.columnNames.map((name) => ({
        name,
        isPrimaryKey: false
      })),
      browse,
      readOnly: true,
      primaryKeyColumn: undefined,
      viewDefinition,
      statusMessage: this.lastStatusMessage
    });
  }

  private toState(input: {
    objectType: BrowserObjectType;
    objectName: string;
    columns: BrowserColumnDescriptor[];
    browse: BrowseResponse;
    readOnly: boolean;
    primaryKeyColumn?: string;
    viewDefinition?: ViewResponse;
    statusMessage?: string;
  }): BrowserState {
    return {
      objectType: input.objectType,
      objectName: input.objectName,
      readOnly: input.readOnly,
      primaryKeyColumn: input.primaryKeyColumn,
      columns: input.columns,
      rows: input.browse.rows.map((row, index) => ({
        id: `${input.objectName}:${this.page}:${index}`,
        originalPk: input.primaryKeyColumn ? row[input.primaryKeyColumn] : undefined,
        values: row
      })),
      page: input.browse.page,
      pageSize: input.browse.pageSize,
      totalPages: input.browse.totalPages,
      totalRows: input.browse.totalRows,
      viewDefinition: input.viewDefinition,
      statusMessage: input.statusMessage
    };
  }

  private async saveChanges(payload: SaveChangesPayload): Promise<void> {
    if (this.objectType !== "table") {
      return;
    }

    const schema = await this.client.getTableSchema(this.objectName);
    const primaryKeyColumn = schema.columns.find((column) => column.isPrimaryKey)?.name;
    if (!primaryKeyColumn) {
      throw new Error(`Table '${this.objectName}' has no primary key.`);
    }

    for (const row of payload.newRows) {
      await this.client.insertRow(this.objectName, sanitizeValues(row));
    }

    for (const row of payload.updatedRows) {
      await this.client.updateRow(this.objectName, coercePk(row.originalPk), sanitizeValues(row.values), primaryKeyColumn);
    }

    for (const row of payload.deletedRows) {
      await this.client.deleteRow(this.objectName, coercePk(row.originalPk), primaryKeyColumn);
    }

    const changedCount = payload.newRows.length + payload.updatedRows.length + payload.deletedRows.length;
    this.lastStatusMessage = changedCount > 0 ? `Saved ${changedCount} change(s).` : "No changes to save.";
    void vscode.window.showInformationMessage(this.lastStatusMessage);
    await vscode.commands.executeCommand("csharpdb.refresh");
    await this.loadAndRender();
  }

  private async saveView(payload: { viewName: string; sql: string }): Promise<void> {
    if (this.objectType !== "view") {
      return;
    }

    const nextName = payload.viewName?.trim();
    if (!nextName) {
      throw new Error("View name is required.");
    }

    const previousKey = this.key;
    const result = await this.client.updateView(this.objectName, {
      newViewName: nextName,
      selectSql: payload.sql
    });

    this.objectName = result.viewName;
    this.updatePanelRegistration(previousKey);
    this.panel.title = this.title;
    this.lastStatusMessage = `Saved view '${result.viewName}'.`;
    void vscode.window.showInformationMessage(this.lastStatusMessage);
    await vscode.commands.executeCommand("csharpdb.refresh");
    await this.loadAndRender();
  }

  private async dropView(): Promise<void> {
    if (this.objectType !== "view") {
      return;
    }

    const viewName = this.objectName;
    await this.client.dropView(viewName);
    void vscode.window.showInformationMessage(`Dropped view '${viewName}'.`);
    await vscode.commands.executeCommand("csharpdb.refresh");
    this.panel.dispose();
  }

  private updatePanelRegistration(previousKey: string): void {
    if (this.shared || previousKey === this.key) {
      return;
    }

    DataBrowserPanel.panels.delete(previousKey);
    DataBrowserPanel.panels.set(this.key, this);
  }
}

function mapColumn(column: ColumnResponse): BrowserColumnDescriptor {
  return {
    name: column.name,
    type: column.type,
    isPrimaryKey: column.isPrimaryKey,
    nullable: column.nullable,
    isIdentity: column.isIdentity
  };
}

function sanitizeValues(values: Record<string, unknown>): Record<string, unknown> {
  const sanitized: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(values)) {
    sanitized[key] = value === undefined ? null : value;
  }

  return sanitized;
}

function coercePk(value: unknown): string | number {
  return typeof value === "number" ? value : String(value);
}
