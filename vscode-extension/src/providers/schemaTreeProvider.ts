import * as path from "path";
import * as vscode from "vscode";
import { CSharpDbApiClient } from "../api/client";
import {
  DatabaseInfoResponse,
  IndexResponse,
  ProcedureSummaryResponse,
  TableSchemaResponse,
  TriggerResponse,
  ViewResponse
} from "../api/types";
import { ConnectionManager } from "../server/connectionManager";

type SchemaNodeType =
  | "status"
  | "database"
  | "group"
  | "table"
  | "column"
  | "view"
  | "index"
  | "trigger"
  | "procedure";

export interface SchemaSnapshot {
  info: DatabaseInfoResponse;
  tables: TableSchemaResponse[];
  views: ViewResponse[];
  indexes: IndexResponse[];
  triggers: TriggerResponse[];
  procedures: ProcedureSummaryResponse[];
}

export class SchemaTreeItem extends vscode.TreeItem {
  readonly nodeType: SchemaNodeType;
  readonly resourceName?: string;
  readonly queryText?: string;

  constructor(
    nodeType: SchemaNodeType,
    label: string,
    collapsibleState: vscode.TreeItemCollapsibleState,
    options: {
      resourceName?: string;
      description?: string;
      tooltip?: string;
      iconName?: string;
      contextValue?: string;
      command?: vscode.Command;
      queryText?: string;
    } = {}
  ) {
    super(label, collapsibleState);
    this.nodeType = nodeType;
    this.resourceName = options.resourceName;
    this.queryText = options.queryText;
    this.description = options.description;
    this.tooltip = options.tooltip;
    this.contextValue = options.contextValue ?? `csharpdb.${nodeType}`;
    this.command = options.command;
    if (options.iconName) {
      const iconPath = vscode.Uri.file(path.join(__dirname, "..", "media", "icons", `${options.iconName}.svg`));
      this.iconPath = {
        light: iconPath,
        dark: iconPath
      };
    }
  }
}

export class SchemaTreeProvider implements vscode.TreeDataProvider<SchemaTreeItem>, vscode.Disposable {
  private readonly emitter = new vscode.EventEmitter<SchemaTreeItem | undefined>();
  private snapshot?: SchemaSnapshot;

  readonly onDidChangeTreeData = this.emitter.event;

  constructor(
    private readonly client: CSharpDbApiClient,
    private readonly connectionManager: ConnectionManager
  ) {
    this.connectionManager.onConnectionChanged((state) => {
      if (!state.connected) {
        this.snapshot = undefined;
      }

      void this.refresh();
    });
  }

  getSnapshot(): SchemaSnapshot | undefined {
    return this.snapshot;
  }

  dispose(): void {
    this.emitter.dispose();
  }

  getTableSchema(name: string): TableSchemaResponse | undefined {
    return this.snapshot?.tables.find((table) => equalsIgnoreCase(table.tableName, name));
  }

  async refresh(): Promise<void> {
    if (!this.connectionManager.currentState.connected) {
      this.snapshot = undefined;
      this.emitter.fire(undefined);
      return;
    }

    try {
      const [info, tableNames, views, indexes, triggers, procedures] = await Promise.all([
        this.client.getDatabaseInfo(),
        this.client.getTableNames(),
        this.client.getViews(),
        this.client.getIndexes(),
        this.client.getTriggers(),
        this.client.getProcedures()
      ]);

      const tables = await Promise.all(
        tableNames
          .sort((left, right) => left.localeCompare(right, undefined, { sensitivity: "base" }))
          .map((name) => this.client.getTableSchema(name))
      );

      this.snapshot = {
        info,
        tables,
        views: sortByName(views, (view) => view.viewName),
        indexes: sortByName(indexes, (index) => index.indexName),
        triggers: sortByName(triggers, (trigger) => trigger.triggerName),
        procedures: sortByName(procedures, (procedure) => procedure.name)
      };
    } catch {
      this.snapshot = undefined;
    }

    this.emitter.fire(undefined);
  }

  getTreeItem(element: SchemaTreeItem): vscode.TreeItem {
    return element;
  }

  getChildren(element?: SchemaTreeItem): vscode.ProviderResult<SchemaTreeItem[]> {
    if (!this.connectionManager.currentState.connected) {
      if (element) {
        return [];
      }

      return [
        new SchemaTreeItem("status", "Connect to CSharpDB", vscode.TreeItemCollapsibleState.None, {
          description: this.connectionManager.currentState.lastError,
          tooltip: this.connectionManager.currentState.lastError ?? "Connect to start exploring the database.",
          iconName: "database",
          command: {
            command: "csharpdb.connect",
            title: "Connect"
          }
        }),
        new SchemaTreeItem("status", "Create New Database", vscode.TreeItemCollapsibleState.None, {
          description: "Choose a path and open a new .db file",
          tooltip: "Create a new CSharpDB database file and connect to it.",
          iconName: "database",
          command: {
            command: "csharpdb.createDatabase",
            title: "Create New Database"
          }
        })
      ];
    }

    if (!this.snapshot) {
      return [];
    }

    if (!element) {
      return [this.createDatabaseRoot(this.snapshot.info)];
    }

    switch (element.nodeType) {
      case "database":
        return this.createGroupItems();
      case "group":
        return this.getGroupChildren(element.resourceName ?? "");
      case "table":
        return this.getColumnItems(element.resourceName ?? "");
      default:
        return [];
    }
  }

  private createDatabaseRoot(info: DatabaseInfoResponse): SchemaTreeItem {
    const dataSourceLabel = path.basename(info.dataSource) || info.dataSource;
    return new SchemaTreeItem("database", dataSourceLabel, vscode.TreeItemCollapsibleState.Expanded, {
      description: `${info.tableCount} tables`,
      tooltip: info.dataSource,
      iconName: "database"
    });
  }

  private createGroupItems(): SchemaTreeItem[] {
    if (!this.snapshot) {
      return [];
    }

    return [
      new SchemaTreeItem("group", "Tables", vscode.TreeItemCollapsibleState.Expanded, {
        resourceName: "tables",
        description: String(this.snapshot.tables.length),
        iconName: "table",
        contextValue: "csharpdb.group"
      }),
      new SchemaTreeItem("group", "Views", vscode.TreeItemCollapsibleState.Collapsed, {
        resourceName: "views",
        description: String(this.snapshot.views.length),
        iconName: "view",
        contextValue: "csharpdb.group"
      }),
      new SchemaTreeItem("group", "Indexes", vscode.TreeItemCollapsibleState.Collapsed, {
        resourceName: "indexes",
        description: String(this.snapshot.indexes.length),
        iconName: "index",
        contextValue: "csharpdb.group"
      }),
      new SchemaTreeItem("group", "Triggers", vscode.TreeItemCollapsibleState.Collapsed, {
        resourceName: "triggers",
        description: String(this.snapshot.triggers.length),
        iconName: "trigger",
        contextValue: "csharpdb.group"
      }),
      new SchemaTreeItem("group", "Procedures", vscode.TreeItemCollapsibleState.Collapsed, {
        resourceName: "procedures",
        description: String(this.snapshot.procedures.length),
        iconName: "procedure",
        contextValue: "csharpdb.group"
      })
    ];
  }

  private getGroupChildren(groupName: string): SchemaTreeItem[] {
    if (!this.snapshot) {
      return [];
    }

    switch (groupName) {
      case "tables":
        return this.snapshot.tables.map((table) => new SchemaTreeItem("table", table.tableName, vscode.TreeItemCollapsibleState.Collapsed, {
          resourceName: table.tableName,
          description: `${table.columns.length} cols`,
          tooltip: `Browse ${table.tableName}`,
          iconName: "table",
          command: {
            command: "csharpdb.browseTable",
            title: "Browse Table",
            arguments: [table.tableName]
          }
        }));
      case "views":
        return this.snapshot.views.map((view) => new SchemaTreeItem("view", view.viewName, vscode.TreeItemCollapsibleState.None, {
          resourceName: view.viewName,
          description: "view",
          tooltip: view.sql,
          iconName: "view",
          queryText: `SELECT * FROM ${view.viewName} LIMIT 100;`,
          command: {
            command: "csharpdb.browseView",
            title: "Browse View",
            arguments: [view.viewName]
          }
        }));
      case "indexes":
        return this.snapshot.indexes.map((index) => new SchemaTreeItem("index", index.indexName, vscode.TreeItemCollapsibleState.None, {
          resourceName: index.indexName,
          description: index.tableName,
          tooltip: `${index.tableName} (${index.columns.join(", ")})`,
          iconName: "index",
          queryText: `SELECT * FROM sys.indexes WHERE index_name = '${index.indexName.replaceAll("'", "''")}';`,
          command: {
            command: "csharpdb.openTableDesigner",
            title: "Open Table Designer",
            arguments: [index.tableName]
          }
        }));
      case "triggers":
        return this.snapshot.triggers.map((trigger) => new SchemaTreeItem("trigger", trigger.triggerName, vscode.TreeItemCollapsibleState.None, {
          resourceName: trigger.triggerName,
          description: `${trigger.timing} ${trigger.event}`,
          tooltip: trigger.bodySql,
          iconName: "trigger",
          queryText: `SELECT * FROM sys.triggers WHERE trigger_name = '${trigger.triggerName.replaceAll("'", "''")}';`,
          command: {
            command: "csharpdb.openTableDesigner",
            title: "Open Table Designer",
            arguments: [trigger.tableName]
          }
        }));
      case "procedures":
        return this.snapshot.procedures.map((procedure) => new SchemaTreeItem("procedure", procedure.name, vscode.TreeItemCollapsibleState.None, {
          resourceName: procedure.name,
          description: procedure.isEnabled ? "enabled" : "disabled",
          tooltip: procedure.description ?? procedure.name,
          iconName: "procedure",
          queryText: `EXEC ${procedure.name};`,
          command: {
            command: "csharpdb.openProcedureDesigner",
            title: "Open Procedure Designer",
            arguments: [procedure.name]
          }
        }));
      default:
        return [];
    }
  }

  private getColumnItems(tableName: string): SchemaTreeItem[] {
    const table = this.getTableSchema(tableName);
    if (!table) {
      return [];
    }

    return table.columns.map((column) => {
      const badges = [
        column.type.toUpperCase(),
        column.isPrimaryKey ? "PK" : undefined,
        column.isIdentity ? "IDENTITY" : undefined,
        column.nullable ? undefined : "NOT NULL"
      ].filter(Boolean);

      return new SchemaTreeItem("column", column.name, vscode.TreeItemCollapsibleState.None, {
        resourceName: column.name,
        description: badges.join(" · "),
        tooltip: `${column.name} ${badges.join(" ")}`,
        iconName: "column"
      });
    });
  }
}

function sortByName<T>(items: T[], selector: (value: T) => string): T[] {
  return [...items].sort((left, right) => selector(left).localeCompare(selector(right), undefined, { sensitivity: "base" }));
}

function equalsIgnoreCase(left: string, right: string): boolean {
  return left.localeCompare(right, undefined, { sensitivity: "base" }) === 0;
}
