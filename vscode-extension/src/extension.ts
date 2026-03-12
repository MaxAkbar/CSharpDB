import * as path from "path";
import * as vscode from "vscode";
import { CSharpDbApiClient } from "./api/client";
import { ConnectionManager } from "./server/connectionManager";
import { SchemaTreeItem, SchemaTreeProvider } from "./providers/schemaTreeProvider";
import { SqlCompletionProvider } from "./providers/sqlCompletionProvider";
import { SqlHoverProvider } from "./providers/sqlHoverProvider";
import { QueryResultsPanel } from "./panels/queryResultsPanel";
import { DataBrowserPanel } from "./panels/dataBrowserPanel";
import { TableDesignerPanel } from "./panels/tableDesignerPanel";
import { StorageDiagnosticsPanel } from "./panels/storageDiagnosticsPanel";
import { ProcedureDesignerPanel } from "./panels/procedureDesignerPanel";
import { closeAllCSharpDbPanels } from "./panels/panelRegistry";

export async function activate(context: vscode.ExtensionContext): Promise<void> {
  const output = vscode.window.createOutputChannel("CSharpDB");
  const client = new CSharpDbApiClient(getNativeLibraryPath());
  const connectionManager = new ConnectionManager(client, output);
  const schemaProvider = new SchemaTreeProvider(client, connectionManager);
  const schemaTreeView = vscode.window.createTreeView("csharpdb.schemaExplorer", {
    treeDataProvider: schemaProvider,
    showCollapseAll: true
  });
  const statusBar = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 100);

  statusBar.name = "CSharpDB";
  statusBar.show();

  void vscode.commands.executeCommand("setContext", "csharpdb.connected", connectionManager.currentState.connected);

  const refreshAll = async (): Promise<void> => {
    await connectionManager.refresh();
    await schemaProvider.refresh();
  };

  const ensureConnected = async (interactive = true): Promise<boolean> => {
    if (connectionManager.currentState.connected) {
      return true;
    }

    const databasePath = await pickDatabaseFile(interactive);
    if (!databasePath) {
      return false;
    }

    await connectionManager.connect({ databasePath });
    await schemaProvider.refresh();
    return true;
  };

  connectionManager.onConnectionChanged((state) => {
    void vscode.commands.executeCommand("setContext", "csharpdb.connected", state.connected);

    if (state.connecting) {
      statusBar.text = "$(sync~spin) CSharpDB: Opening";
      statusBar.command = "csharpdb.connect";
      statusBar.tooltip = state.databasePath ?? "Opening local database";
      return;
    }

    if (state.connected && state.info) {
      statusBar.text = `$(database) CSharpDB: ${path.basename(state.info.dataSource)}`;
      statusBar.command = "csharpdb.disconnect";
      statusBar.tooltip = [
        state.info.dataSource,
        state.resolvedNativeLibraryPath || state.nativeLibraryPath || "NativeAOT"
      ].join("\n");
      return;
    }

    if (!state.connected) {
      closeAllCSharpDbPanels();
    }

    statusBar.text = "$(plug) CSharpDB: Disconnected";
    statusBar.command = "csharpdb.connect";
    statusBar.tooltip = state.lastError ?? "Open a .db file to connect through NativeAOT.";
  });

  context.subscriptions.push(
    output,
    connectionManager,
    schemaProvider,
    schemaTreeView,
    statusBar,
    vscode.languages.registerCompletionItemProvider("csharpdb-sql", new SqlCompletionProvider(schemaProvider), ".", " "),
    vscode.languages.registerHoverProvider("csharpdb-sql", new SqlHoverProvider(schemaProvider))
  );

  const watcher = vscode.workspace.createFileSystemWatcher("**/*.db");
  watcher.onDidCreate(() => {
    if (!connectionManager.currentState.connected && vscode.workspace.getConfiguration("csharpdb").get<boolean>("autoConnect", true)) {
      void ensureConnected(false).catch(() => undefined);
    }
  });
  context.subscriptions.push(watcher);

  context.subscriptions.push(
    vscode.commands.registerCommand("csharpdb.connect", async () => {
      try {
        await ensureConnected(true);
      } catch (error) {
        void vscode.window.showErrorMessage(toErrorMessage(error));
      }
    }),
    vscode.commands.registerCommand("csharpdb.createDatabase", async () => {
      try {
        const databasePath = await createDatabaseFileDialog();
        if (!databasePath) {
          return;
        }

        await connectionManager.connect({ databasePath });
        closeAllCSharpDbPanels();
        await schemaProvider.refresh();
      } catch (error) {
        void vscode.window.showErrorMessage(toErrorMessage(error));
      }
    }),
    vscode.commands.registerCommand("csharpdb.disconnect", async () => {
      await connectionManager.disconnect();
      await schemaProvider.refresh();
    }),
    vscode.commands.registerCommand("csharpdb.refresh", async () => {
      await refreshAll();
    }),
    vscode.commands.registerCommand("csharpdb.newQuery", async (initialText?: string) => {
      await openQueryDocument(initialText ?? "");
    }),
    vscode.commands.registerCommand("csharpdb.newQueryWithText", async (arg?: string | SchemaTreeItem) => {
      const text = typeof arg === "string" ? arg : arg?.queryText ?? "";
      await openQueryDocument(text);
    }),
    vscode.commands.registerCommand("csharpdb.executeQuery", async () => {
      const editor = vscode.window.activeTextEditor;
      if (!editor) {
        return;
      }

      const sql = editor.selection.isEmpty ? editor.document.getText() : editor.document.getText(editor.selection);
      if (!sql.trim()) {
        void vscode.window.showWarningMessage("There is no SQL to execute.");
        return;
      }

      try {
        if (!await ensureConnected(true)) {
          return;
        }

        const result = await client.executeSql(sql);
        QueryResultsPanel.show(context.extensionUri, sql, result);
        if (!result.error && !result.isQuery) {
          await schemaProvider.refresh();
        }
      } catch (error) {
        void vscode.window.showErrorMessage(toErrorMessage(error));
      }
    }),
    vscode.commands.registerCommand("csharpdb.browseTable", async (arg?: string | SchemaTreeItem) => {
      try {
        if (!await ensureConnected(true)) {
          return;
        }

        const tableName = resolveResourceName(arg);
        if (!tableName) {
          return;
        }

        DataBrowserPanel.createOrReveal(context.extensionUri, client, "table", tableName);
      } catch (error) {
        void vscode.window.showErrorMessage(toErrorMessage(error));
      }
    }),
    vscode.commands.registerCommand("csharpdb.browseView", async (arg?: string | SchemaTreeItem) => {
      try {
        if (!await ensureConnected(true)) {
          return;
        }

        const viewName = resolveResourceName(arg);
        if (!viewName) {
          return;
        }

        DataBrowserPanel.createOrReveal(context.extensionUri, client, "view", viewName);
      } catch (error) {
        void vscode.window.showErrorMessage(toErrorMessage(error));
      }
    }),
    vscode.commands.registerCommand("csharpdb.openTableDesigner", async (arg?: string | SchemaTreeItem) => {
      try {
        if (!await ensureConnected(true)) {
          return;
        }

        TableDesignerPanel.createOrReveal(context.extensionUri, client, resolveResourceName(arg));
      } catch (error) {
        void vscode.window.showErrorMessage(toErrorMessage(error));
      }
    }),
    vscode.commands.registerCommand("csharpdb.openProcedureDesigner", async (arg?: string | SchemaTreeItem) => {
      try {
        if (!await ensureConnected(true)) {
          return;
        }

        ProcedureDesignerPanel.createOrReveal(context.extensionUri, client, resolveResourceName(arg));
      } catch (error) {
        void vscode.window.showErrorMessage(toErrorMessage(error));
      }
    }),
    vscode.commands.registerCommand("csharpdb.newProcedure", async () => {
      try {
        if (!await ensureConnected(true)) {
          return;
        }

        ProcedureDesignerPanel.createOrReveal(context.extensionUri, client);
      } catch (error) {
        void vscode.window.showErrorMessage(toErrorMessage(error));
      }
    }),
    vscode.commands.registerCommand("csharpdb.openDiagnostics", async () => {
      try {
        if (!await ensureConnected(true)) {
          return;
        }

        StorageDiagnosticsPanel.createOrReveal(context.extensionUri, client);
      } catch (error) {
        void vscode.window.showErrorMessage(toErrorMessage(error));
      }
    }),
    vscode.commands.registerCommand("csharpdb.renameTable", async (arg?: string | SchemaTreeItem) => {
      const tableName = resolveResourceName(arg);
      if (!tableName) {
        return;
      }

      const newName = await vscode.window.showInputBox({
        title: "Rename Table",
        value: tableName,
        validateInput: (value) => value.trim().length === 0 ? "A new name is required." : undefined
      });

      if (!newName || newName === tableName) {
        return;
      }

      try {
        if (!await ensureConnected(true)) {
          return;
        }

        await client.renameTable(tableName, { newName });
        await schemaProvider.refresh();
      } catch (error) {
        void vscode.window.showErrorMessage(toErrorMessage(error));
      }
    }),
    vscode.commands.registerCommand("csharpdb.dropObject", async (arg?: SchemaTreeItem) => {
      if (!arg?.resourceName) {
        return;
      }

      const confirmed = await vscode.window.showWarningMessage(
        `Drop ${arg.nodeType} '${arg.resourceName}'?`,
        { modal: true },
        "Drop"
      );

      if (confirmed !== "Drop") {
        return;
      }

      try {
        if (!await ensureConnected(true)) {
          return;
        }

        switch (arg.nodeType) {
          case "table":
            await client.dropTable(arg.resourceName);
            break;
          case "view":
            await client.dropView(arg.resourceName);
            break;
          case "index":
            await client.dropIndex(arg.resourceName);
            break;
          case "trigger":
            await client.dropTrigger(arg.resourceName);
            break;
          case "procedure":
            await client.deleteProcedure(arg.resourceName);
            break;
          default:
            return;
        }

        await schemaProvider.refresh();
      } catch (error) {
        void vscode.window.showErrorMessage(toErrorMessage(error));
      }
    })
  );

  void tryAutoConnect();

  async function tryAutoConnect(): Promise<void> {
    try {
      if (vscode.workspace.getConfiguration("csharpdb").get<boolean>("autoConnect", true)) {
        const connected = await ensureConnected(false);
        if (connected) {
          return;
        }
      }

      await refreshAll();
    } catch (error) {
      output.appendLine(`[activate] ${toErrorMessage(error)}`);
    }
  }

  async function openQueryDocument(text: string): Promise<void> {
    const document = await vscode.workspace.openTextDocument({
      language: "csharpdb-sql",
      content: text
    });
    await vscode.window.showTextDocument(document, vscode.ViewColumn.Active);
  }

  async function pickDatabaseFile(interactive: boolean): Promise<string | undefined> {
    const files = await vscode.workspace.findFiles("**/*.db", "**/{bin,obj,node_modules,.git}/**", 50);
    const sorted = files.sort((left, right) => left.fsPath.localeCompare(right.fsPath, undefined, { sensitivity: "base" }));

    if (!interactive) {
      if (sorted.length === 0) {
        return undefined;
      }

      return sorted[0].fsPath;
    }

    const picked = await vscode.window.showQuickPick(
      [
        ...sorted.map((uri) => ({
          label: path.basename(uri.fsPath),
          description: vscode.workspace.asRelativePath(uri),
          value: uri.fsPath
        })),
        {
          label: "Create New Database...",
          description: "Choose a path for a new CSharpDB database",
          value: "__create__"
        },
        {
          label: "Browse...",
          description: "Open a database file from disk",
          value: "__browse__"
        }
      ],
      {
        title: sorted.length === 0 ? "Open or Create a Database" : "Select a Database File"
      }
    );

    if (!picked) {
      return undefined;
    }

    if (picked.value === "__create__") {
      return createDatabaseFileDialog();
    }

    return picked.value === "__browse__" ? openDatabaseFileDialog() : picked.value;
  }

  async function openDatabaseFileDialog(): Promise<string | undefined> {
    const picked = await vscode.window.showOpenDialog({
      canSelectFiles: true,
      canSelectFolders: false,
      canSelectMany: false,
      filters: {
        "CSharpDB Database": ["db"]
      },
      openLabel: "Open Database"
    });

    return picked?.[0]?.fsPath;
  }

  async function createDatabaseFileDialog(): Promise<string | undefined> {
    const defaultDirectory = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
    const defaultUri = defaultDirectory
      ? vscode.Uri.file(path.join(defaultDirectory, "database.db"))
      : undefined;

    const picked = await vscode.window.showSaveDialog({
      defaultUri,
      filters: {
        "CSharpDB Database": ["db"]
      },
      saveLabel: "Create Database",
      title: "Create a New CSharpDB Database"
    });

    if (!picked) {
      return undefined;
    }

    return ensureDatabaseExtension(picked.fsPath);
  }
}

export function deactivate(): void {
  // VS Code disposes extension resources automatically.
}

function resolveResourceName(arg?: string | SchemaTreeItem): string | undefined {
  if (!arg) {
    return undefined;
  }

  return typeof arg === "string" ? arg : arg.resourceName;
}

function getNativeLibraryPath(): string {
  return vscode.workspace.getConfiguration("csharpdb").get<string>("nativeLibraryPath", "");
}

function ensureDatabaseExtension(filePath: string): string {
  return path.extname(filePath).length > 0 ? filePath : `${filePath}.db`;
}

function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
