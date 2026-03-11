import * as vscode from "vscode";
import { CSharpDbApiClient } from "../api/client";
import {
  DatabaseInspectReport,
  DatabaseMaintenanceReport,
  IndexInspectReport,
  PageInspectReport,
  WalInspectReport
} from "../api/types";
import { renderWebviewPage } from "../utils/htmlBuilder";
import { closeWorkbenchPanels, registerCSharpDbPanel, unregisterCSharpDbPanel } from "./panelRegistry";

interface DiagnosticsState {
  databaseReport: DatabaseInspectReport;
  walReport: WalInspectReport;
  indexReport: IndexInspectReport;
  maintenanceReport: DatabaseMaintenanceReport;
  pageReport?: PageInspectReport;
  statusMessage?: string;
}

export class StorageDiagnosticsPanel implements vscode.Disposable {
  private static panel?: StorageDiagnosticsPanel;

  static createOrReveal(extensionUri: vscode.Uri, client: CSharpDbApiClient): void {
    if (StorageDiagnosticsPanel.panel) {
      closeWorkbenchPanels(StorageDiagnosticsPanel.panel.panel);
      StorageDiagnosticsPanel.panel.panel.reveal(
        StorageDiagnosticsPanel.panel.panel.viewColumn ?? vscode.ViewColumn.Active,
        true
      );
      void StorageDiagnosticsPanel.panel.loadAndRender();
      return;
    }

    closeWorkbenchPanels();
    const panel = vscode.window.createWebviewPanel(
      "csharpdb.storageDiagnostics",
      "CSharpDB Diagnostics",
      vscode.ViewColumn.Active,
      {
        enableScripts: true,
        retainContextWhenHidden: true
      }
    );

    StorageDiagnosticsPanel.panel = new StorageDiagnosticsPanel(panel, extensionUri, client);
  }

  private readonly disposables: vscode.Disposable[] = [];
  private pageReport?: PageInspectReport;
  private statusMessage?: string;

  private constructor(
    private readonly panel: vscode.WebviewPanel,
    private readonly extensionUri: vscode.Uri,
    private readonly client: CSharpDbApiClient
  ) {
    registerCSharpDbPanel(panel, "workbench");

    this.disposables.push(
      panel.onDidDispose(() => {
        unregisterCSharpDbPanel(panel);
        StorageDiagnosticsPanel.panel = undefined;
        this.dispose();
      }),
      panel.webview.onDidReceiveMessage(async (message) => {
        switch (message?.type) {
          case "ready":
          case "refresh":
            await this.loadAndRender();
            break;
          case "inspectPage":
            await this.inspectPage(Number(message.payload?.pageId), Boolean(message.payload?.includeHex));
            break;
          case "reindex":
            await this.reindex(String(message.payload?.scope ?? "all"), message.payload?.name);
            break;
          case "vacuum":
            await this.vacuum();
            break;
          default:
            break;
        }
      })
    );

    this.panel.webview.html = renderWebviewPage({
      title: "CSharpDB Diagnostics",
      body: '<div id="app"></div>',
      extensionUri,
      webview: panel.webview,
      styles: ["media/styles/panel.css", "media/styles/grid.css"],
      scripts: ["media/scripts/storageDiagnostics.js"],
      inlineScript: "window.__CDB_DIAGNOSTICS_INITIAL_STATE__ = null;"
    });

    void this.loadAndRender();
  }

  dispose(): void {
    while (this.disposables.length > 0) {
      this.disposables.pop()?.dispose();
    }
  }

  private async loadAndRender(): Promise<void> {
    const state = await this.loadState();
    await this.panel.webview.postMessage({ type: "state", state });
  }

  private async loadState(): Promise<DiagnosticsState> {
    const [databaseReport, walReport, indexReport, maintenanceReport] = await Promise.all([
      this.client.inspectStorage(false),
      this.client.inspectWal(),
      this.client.checkIndexes(),
      this.client.getMaintenanceReport()
    ]);

    return {
      databaseReport,
      walReport,
      indexReport,
      maintenanceReport,
      pageReport: this.pageReport,
      statusMessage: this.statusMessage
    };
  }

  private async inspectPage(pageId: number, includeHex: boolean): Promise<void> {
    this.pageReport = await this.client.inspectPage(pageId, includeHex);
    this.statusMessage = `Inspected page ${pageId}.`;
    await this.loadAndRender();
  }

  private async reindex(scope: string, name?: string): Promise<void> {
    const normalizedScope = scope === "table" || scope === "index" ? scope : "all";
    const result = await this.client.reindex({
      scope: normalizedScope,
      name: name || undefined
    });
    this.statusMessage = `Reindexed ${result.rebuiltIndexCount} index(es).`;
    await this.loadAndRender();
  }

  private async vacuum(): Promise<void> {
    const result = await this.client.vacuum();
    this.statusMessage = `Vacuum complete: ${result.databaseFileBytesBefore} -> ${result.databaseFileBytesAfter} bytes.`;
    await this.loadAndRender();
  }
}
