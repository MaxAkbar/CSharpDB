import * as vscode from "vscode";
import { CSharpDbApiClient } from "../api/client";
import { ProcedureParameterResponse, ProcedureParameterRequest } from "../api/types";
import { renderWebviewPage } from "../utils/htmlBuilder";
import { closeWorkbenchPanels, registerCSharpDbPanel, unregisterCSharpDbPanel } from "./panelRegistry";

interface ProcedureParameterDraft {
  name: string;
  type: string;
  required: boolean;
  defaultText: string;
  description: string;
}

interface ProcedureDesignerState {
  mode: "create" | "edit";
  procedureName?: string;
  description: string;
  isEnabled: boolean;
  bodySql: string;
  parameters: ProcedureParameterDraft[];
  createdUtc?: string;
  updatedUtc?: string;
  executionSupported: boolean;
  statusMessage?: string;
}

interface SaveProcedurePayload {
  name: string;
  description: string;
  isEnabled: boolean;
  bodySql: string;
  parameters: ProcedureParameterDraft[];
}

export class ProcedureDesignerPanel implements vscode.Disposable {
  private static readonly panels = new Map<string, ProcedureDesignerPanel>();
  private static sharedPanel?: ProcedureDesignerPanel;

  static createOrReveal(extensionUri: vscode.Uri, client: CSharpDbApiClient, procedureName?: string): void {
    if (vscode.workspace.getConfiguration("csharpdb").get<boolean>("reuseDesignerTab", true)) {
      if (ProcedureDesignerPanel.sharedPanel) {
        closeWorkbenchPanels(ProcedureDesignerPanel.sharedPanel.panel);
        ProcedureDesignerPanel.sharedPanel.reveal();
        void ProcedureDesignerPanel.sharedPanel.openProcedure(procedureName);
        return;
      }

      closeWorkbenchPanels();
      const panel = ProcedureDesignerPanel.createPanel(
        procedureName ? `Procedure ${procedureName}` : "New Procedure",
        vscode.ViewColumn.Active
      );
      ProcedureDesignerPanel.sharedPanel = new ProcedureDesignerPanel(panel, extensionUri, client, procedureName, true);
      return;
    }

    const key = procedureName ?? "__new__";
    const existing = ProcedureDesignerPanel.panels.get(key);
    if (existing) {
      existing.reveal();
      void existing.loadAndRender();
      return;
    }

    const panel = ProcedureDesignerPanel.createPanel(
      procedureName ? `Procedure ${procedureName}` : "New Procedure",
      vscode.ViewColumn.Active
    );

    const instance = new ProcedureDesignerPanel(panel, extensionUri, client, procedureName, false);
    ProcedureDesignerPanel.panels.set(key, instance);
  }

  private static createPanel(title: string, viewColumn: vscode.ViewColumn): vscode.WebviewPanel {
    return vscode.window.createWebviewPanel(
      "csharpdb.procedureDesigner",
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
    private procedureName: string | undefined,
    shared: boolean
  ) {
    this.shared = shared;
    registerCSharpDbPanel(panel, "workbench");

    this.disposables.push(
      panel.onDidDispose(() => {
        unregisterCSharpDbPanel(panel);
        if (this.shared) {
          ProcedureDesignerPanel.sharedPanel = undefined;
        } else {
          for (const [key, value] of ProcedureDesignerPanel.panels.entries()) {
            if (value === this) {
              ProcedureDesignerPanel.panels.delete(key);
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
          case "saveProcedure":
            await this.saveProcedure(message.payload as SaveProcedurePayload);
            break;
          case "deleteProcedure":
            await this.deleteProcedure();
            break;
          default:
            break;
        }
      })
    );

    this.panel.webview.html = renderWebviewPage({
      title: "CSharpDB Procedure Designer",
      body: '<div id="app"></div>',
      extensionUri,
      webview: panel.webview,
      styles: ["media/styles/panel.css", "media/styles/grid.css"],
      scripts: ["media/scripts/procedureDesigner.js"],
      inlineScript: "window.__CDB_PROCEDURE_INITIAL_STATE__ = null;"
    });

    void this.loadAndRender();
  }

  dispose(): void {
    while (this.disposables.length > 0) {
      this.disposables.pop()?.dispose();
    }
  }

  private get key(): string {
    return this.procedureName ?? "__new__";
  }

  private get title(): string {
    return this.procedureName ? `Procedure ${this.procedureName}` : "New Procedure";
  }

  private reveal(): void {
    this.panel.reveal(this.panel.viewColumn ?? vscode.ViewColumn.Active, true);
  }

  private async openProcedure(procedureName?: string): Promise<void> {
    this.procedureName = procedureName;
    this.statusMessage = undefined;
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

      this.panel.title = this.title;
      await this.panel.webview.postMessage({ type: "state", state });
    } catch (error) {
      if (loadVersion !== this.loadVersion) {
        return;
      }

      await this.panel.webview.postMessage({
        type: "state",
        state: {
          mode: this.procedureName ? "edit" : "create",
          procedureName: this.procedureName,
          description: "",
          isEnabled: true,
          bodySql: "",
          parameters: [],
          executionSupported: false,
          statusMessage: error instanceof Error ? error.message : String(error)
        } satisfies ProcedureDesignerState
      });
    }
  }

  private async loadState(): Promise<ProcedureDesignerState> {
    if (!this.procedureName) {
      return {
        mode: "create",
        description: "",
        isEnabled: true,
        bodySql: "",
        parameters: [],
        executionSupported: false,
        statusMessage: this.statusMessage
      };
    }

    const procedure = await this.client.getProcedure(this.procedureName);
    return {
      mode: "edit",
      procedureName: procedure.name,
      description: procedure.description ?? "",
      isEnabled: procedure.isEnabled,
      bodySql: procedure.bodySql,
      parameters: procedure.parameters.map(mapProcedureParameter),
      createdUtc: procedure.createdUtc,
      updatedUtc: procedure.updatedUtc,
      executionSupported: false,
      statusMessage: this.statusMessage
    };
  }

  private async saveProcedure(payload: SaveProcedurePayload): Promise<void> {
    const name = payload.name?.trim();
    if (!name) {
      throw new Error("Procedure name is required.");
    }

    const parameters = payload.parameters
      .filter((parameter) => parameter.name?.trim().length > 0)
      .map((parameter) => toProcedureParameterRequest(parameter));

    const previousKey = this.key;
    if (!this.procedureName) {
      const created = await this.client.createProcedure({
        name,
        description: payload.description?.trim() || null,
        isEnabled: payload.isEnabled,
        bodySql: payload.bodySql,
        parameters
      });

      this.procedureName = created.name;
      this.statusMessage = `Created procedure '${created.name}'.`;
    } else {
      const updated = await this.client.updateProcedure(this.procedureName, {
        newName: name,
        description: payload.description?.trim() || null,
        isEnabled: payload.isEnabled,
        bodySql: payload.bodySql,
        parameters
      });

      this.procedureName = updated.name;
      this.statusMessage = `Saved procedure '${updated.name}'.`;
    }

    this.updatePanelRegistration(previousKey);
    this.panel.title = this.title;
    void vscode.window.showInformationMessage(this.statusMessage);
    await vscode.commands.executeCommand("csharpdb.refresh");
    await this.loadAndRender();
  }

  private async deleteProcedure(): Promise<void> {
    if (!this.procedureName) {
      this.panel.dispose();
      return;
    }

    const procedureName = this.procedureName;
    await this.client.deleteProcedure(procedureName);
    void vscode.window.showInformationMessage(`Deleted procedure '${procedureName}'.`);
    await vscode.commands.executeCommand("csharpdb.refresh");
    this.panel.dispose();
  }

  private updatePanelRegistration(previousKey: string): void {
    if (this.shared || previousKey === this.key) {
      return;
    }

    ProcedureDesignerPanel.panels.delete(previousKey);
    ProcedureDesignerPanel.panels.set(this.key, this);
  }
}

function mapProcedureParameter(parameter: ProcedureParameterResponse): ProcedureParameterDraft {
  return {
    name: parameter.name,
    type: parameter.type.toUpperCase(),
    required: parameter.required,
    defaultText: formatDefaultValue(parameter.default),
    description: parameter.description ?? ""
  };
}

function toProcedureParameterRequest(parameter: ProcedureParameterDraft): ProcedureParameterRequest {
  return {
    name: parameter.name.trim(),
    type: parameter.type.trim().toUpperCase(),
    required: Boolean(parameter.required),
    default: parseDefaultValue(parameter.defaultText),
    description: parameter.description.trim() || undefined
  };
}

function parseDefaultValue(value: string): unknown {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return undefined;
  }

  try {
    return JSON.parse(trimmed);
  } catch {
    return trimmed;
  }
}

function formatDefaultValue(value: unknown): string {
  if (value === undefined || value === null) {
    return "";
  }

  return typeof value === "string" ? value : JSON.stringify(value);
}
