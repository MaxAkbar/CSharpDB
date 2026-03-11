import * as vscode from "vscode";
import { CSharpDbApiClient } from "../api/client";
import { ColumnResponse, IndexResponse, TriggerResponse } from "../api/types";
import { renderWebviewPage } from "../utils/htmlBuilder";
import { closeWorkbenchPanels, registerCSharpDbPanel, unregisterCSharpDbPanel } from "./panelRegistry";

interface DesignerCreateColumn {
  name: string;
  type: string;
  primaryKey: boolean;
  notNull: boolean;
  identity: boolean;
}

interface DesignerState {
  mode: "create" | "alter";
  tableName?: string;
  columns: ColumnResponse[];
  indexes: IndexResponse[];
  triggers: TriggerResponse[];
  createColumns: DesignerCreateColumn[];
  statusMessage?: string;
}

export class TableDesignerPanel implements vscode.Disposable {
  private static readonly panels = new Map<string, TableDesignerPanel>();
  private static sharedPanel?: TableDesignerPanel;

  static createOrReveal(extensionUri: vscode.Uri, client: CSharpDbApiClient, tableName?: string): void {
    if (vscode.workspace.getConfiguration("csharpdb").get<boolean>("reuseDesignerTab", true)) {
      if (TableDesignerPanel.sharedPanel) {
        closeWorkbenchPanels(TableDesignerPanel.sharedPanel.panel);
        TableDesignerPanel.sharedPanel.reveal();
        void TableDesignerPanel.sharedPanel.openTable(tableName);
        return;
      }

      closeWorkbenchPanels();
      const panel = TableDesignerPanel.createPanel(tableName ? `Design ${tableName}` : "Create Table", vscode.ViewColumn.Active);
      TableDesignerPanel.sharedPanel = new TableDesignerPanel(panel, extensionUri, client, tableName, true);
      return;
    }

    const key = tableName ?? "__new__";
    const existing = TableDesignerPanel.panels.get(key);
    if (existing) {
      existing.reveal();
      void existing.loadAndRender();
      return;
    }

    const panel = TableDesignerPanel.createPanel(
      tableName ? `Design ${tableName}` : "Create Table",
      vscode.ViewColumn.Active
    );

    const instance = new TableDesignerPanel(panel, extensionUri, client, tableName, false);
    TableDesignerPanel.panels.set(key, instance);
  }

  private static createPanel(title: string, viewColumn: vscode.ViewColumn): vscode.WebviewPanel {
    return vscode.window.createWebviewPanel(
      "csharpdb.tableDesigner",
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
  private statusMessage?: string;

  private constructor(
    private readonly panel: vscode.WebviewPanel,
    private readonly extensionUri: vscode.Uri,
    private readonly client: CSharpDbApiClient,
    private tableName: string | undefined,
    shared: boolean
  ) {
    this.shared = shared;
    registerCSharpDbPanel(panel, "workbench");

    this.disposables.push(
      panel.onDidDispose(() => {
        unregisterCSharpDbPanel(panel);
        if (this.shared) {
          TableDesignerPanel.sharedPanel = undefined;
        } else {
          for (const [key, value] of TableDesignerPanel.panels.entries()) {
            if (value === this) {
              TableDesignerPanel.panels.delete(key);
            }
          }
        }

        this.dispose();
      }),
      panel.webview.onDidReceiveMessage(async (message) => {
        switch (message?.type) {
          case "ready":
          case "refresh":
            await this.loadAndRender();
            break;
          case "createTable":
            await this.createTable(message.payload);
            break;
          case "renameTable":
            await this.renameTable(String(message.payload?.newName ?? ""));
            break;
          case "addColumn":
            await this.addColumn(message.payload);
            break;
          case "dropColumn":
            await this.dropColumn(String(message.payload?.columnName ?? ""));
            break;
          case "renameColumn":
            await this.renameColumn(String(message.payload?.oldName ?? ""), String(message.payload?.newName ?? ""));
            break;
          case "createIndex":
            await this.createIndex(message.payload);
            break;
          case "dropIndex":
            await this.dropIndex(String(message.payload?.indexName ?? ""));
            break;
          case "createTrigger":
            await this.createTrigger(message.payload);
            break;
          case "dropTrigger":
            await this.dropTrigger(String(message.payload?.triggerName ?? ""));
            break;
          default:
            break;
        }
      })
    );

    this.panel.webview.html = renderWebviewPage({
      title: "CSharpDB Table Designer",
      body: '<div id="app"></div>',
      extensionUri,
      webview: panel.webview,
      styles: ["media/styles/panel.css", "media/styles/grid.css"],
      scripts: ["media/scripts/tableDesigner.js"],
      inlineScript: "window.__CDB_TABLE_DESIGNER_INITIAL_STATE__ = null;"
    });

    void this.loadAndRender();
  }

  dispose(): void {
    while (this.disposables.length > 0) {
      this.disposables.pop()?.dispose();
    }
  }

  private get key(): string {
    return this.tableName ?? "__new__";
  }

  private get title(): string {
    return this.tableName ? `Design ${this.tableName}` : "Create Table";
  }

  private reveal(): void {
    this.panel.reveal(this.panel.viewColumn ?? vscode.ViewColumn.Active, true);
  }

  private async openTable(tableName?: string): Promise<void> {
    this.tableName = tableName;
    this.statusMessage = undefined;
    this.panel.title = this.title;
    await this.loadAndRender();
  }

  private async loadAndRender(): Promise<void> {
    const loadVersion = ++this.loadVersion;
    const state = await this.loadState();
    if (loadVersion !== this.loadVersion) {
      return;
    }

    this.panel.title = this.title;
    await this.panel.webview.postMessage({ type: "state", state });
  }

  private async loadState(): Promise<DesignerState> {
    if (!this.tableName) {
      return {
        mode: "create",
        columns: [],
        indexes: [],
        triggers: [],
        createColumns: [
          { name: "id", type: "INTEGER", primaryKey: true, notNull: true, identity: true },
          { name: "name", type: "TEXT", primaryKey: false, notNull: true, identity: false }
        ],
        statusMessage: this.statusMessage
      };
    }

    const [schema, indexes, triggers] = await Promise.all([
      this.client.getTableSchema(this.tableName),
      this.client.getIndexes(),
      this.client.getTriggers()
    ]);

    return {
      mode: "alter",
      tableName: this.tableName,
      columns: schema.columns,
      indexes: indexes.filter((index) => equalsIgnoreCase(index.tableName, this.tableName!)),
      triggers: triggers.filter((trigger) => equalsIgnoreCase(trigger.tableName, this.tableName!)),
      createColumns: [],
      statusMessage: this.statusMessage
    };
  }

  private async createTable(payload: { tableName: string; columns: DesignerCreateColumn[] }): Promise<void> {
    const tableName = payload.tableName?.trim();
    if (!tableName) {
      throw new Error("Table name is required.");
    }

    const columns = payload.columns.filter((column) => column.name?.trim());
    if (columns.length === 0) {
      throw new Error("At least one column is required.");
    }

    const definitions = columns.map((column) => {
      const parts = [quoteIdentifier(column.name), column.type.toUpperCase()];
      if (column.primaryKey) {
        parts.push("PRIMARY KEY");
      }

      if (column.identity) {
        parts.push("IDENTITY");
      }

      if (column.notNull) {
        parts.push("NOT NULL");
      }

      return parts.join(" ");
    });

    const result = await this.client.executeSql(`CREATE TABLE ${quoteIdentifier(tableName)} (${definitions.join(", ")});`);
    if (result.error) {
      throw new Error(result.error);
    }

    const previousKey = this.key;
    this.tableName = tableName;
    this.updatePanelRegistration(previousKey);
    this.statusMessage = `Created table '${tableName}'.`;
    void vscode.window.showInformationMessage(this.statusMessage);
    await vscode.commands.executeCommand("csharpdb.refresh");
    await this.loadAndRender();
  }

  private async renameTable(newName: string): Promise<void> {
    if (!this.tableName) {
      return;
    }

    const previousKey = this.key;
    await this.client.renameTable(this.tableName, { newName });
    this.tableName = newName;
    this.updatePanelRegistration(previousKey);
    this.statusMessage = `Renamed table to '${newName}'.`;
    await vscode.commands.executeCommand("csharpdb.refresh");
    await this.loadAndRender();
  }

  private async addColumn(payload: { columnName: string; type: string; notNull: boolean }): Promise<void> {
    if (!this.tableName) {
      return;
    }

    await this.client.addColumn(this.tableName, {
      columnName: payload.columnName,
      type: payload.type,
      notNull: payload.notNull
    });
    this.statusMessage = `Added column '${payload.columnName}'.`;
    await vscode.commands.executeCommand("csharpdb.refresh");
    await this.loadAndRender();
  }

  private async dropColumn(columnName: string): Promise<void> {
    if (!this.tableName) {
      return;
    }

    await this.client.dropColumn(this.tableName, columnName);
    this.statusMessage = `Dropped column '${columnName}'.`;
    await vscode.commands.executeCommand("csharpdb.refresh");
    await this.loadAndRender();
  }

  private async renameColumn(oldName: string, newName: string): Promise<void> {
    if (!this.tableName) {
      return;
    }

    await this.client.renameColumn(this.tableName, oldName, { newName });
    this.statusMessage = `Renamed column '${oldName}' to '${newName}'.`;
    await vscode.commands.executeCommand("csharpdb.refresh");
    await this.loadAndRender();
  }

  private async createIndex(payload: { indexName: string; columnName: string; isUnique: boolean }): Promise<void> {
    if (!this.tableName) {
      return;
    }

    await this.client.createIndex({
      indexName: payload.indexName,
      tableName: this.tableName,
      columnName: payload.columnName,
      isUnique: payload.isUnique
    });
    this.statusMessage = `Created index '${payload.indexName}'.`;
    await vscode.commands.executeCommand("csharpdb.refresh");
    await this.loadAndRender();
  }

  private async dropIndex(indexName: string): Promise<void> {
    await this.client.dropIndex(indexName);
    this.statusMessage = `Dropped index '${indexName}'.`;
    await vscode.commands.executeCommand("csharpdb.refresh");
    await this.loadAndRender();
  }

  private async createTrigger(payload: { triggerName: string; timing: string; event: string; bodySql: string }): Promise<void> {
    if (!this.tableName) {
      return;
    }

    await this.client.createTrigger({
      triggerName: payload.triggerName,
      tableName: this.tableName,
      timing: payload.timing,
      event: payload.event,
      bodySql: payload.bodySql
    });
    this.statusMessage = `Created trigger '${payload.triggerName}'.`;
    await vscode.commands.executeCommand("csharpdb.refresh");
    await this.loadAndRender();
  }

  private async dropTrigger(triggerName: string): Promise<void> {
    await this.client.dropTrigger(triggerName);
    this.statusMessage = `Dropped trigger '${triggerName}'.`;
    await vscode.commands.executeCommand("csharpdb.refresh");
    await this.loadAndRender();
  }

  private updatePanelRegistration(previousKey: string): void {
    if (this.shared || previousKey === this.key) {
      return;
    }

    TableDesignerPanel.panels.delete(previousKey);
    TableDesignerPanel.panels.set(this.key, this);
  }
}

function quoteIdentifier(identifier: string): string {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(identifier)
    ? identifier
    : `"${identifier.replaceAll("\"", "\"\"")}"`;
}

function equalsIgnoreCase(left: string, right: string): boolean {
  return left.localeCompare(right, undefined, { sensitivity: "base" }) === 0;
}
