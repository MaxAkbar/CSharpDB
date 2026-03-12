import * as vscode from "vscode";

type CSharpDbPanelScope = "workbench" | "auxiliary";

const panelScopes = new Map<vscode.WebviewPanel, CSharpDbPanelScope>();

export function registerCSharpDbPanel(panel: vscode.WebviewPanel, scope: CSharpDbPanelScope): void {
  panelScopes.set(panel, scope);
}

export function unregisterCSharpDbPanel(panel: vscode.WebviewPanel): void {
  panelScopes.delete(panel);
}

export function closeWorkbenchPanels(exceptPanel?: vscode.WebviewPanel): void {
  closePanels((panel, scope) => scope === "workbench" && panel !== exceptPanel);
}

export function closeAllCSharpDbPanels(): void {
  closePanels(() => true);
}

function closePanels(predicate: (panel: vscode.WebviewPanel, scope: CSharpDbPanelScope) => boolean): void {
  for (const [panel, scope] of [...panelScopes.entries()]) {
    if (predicate(panel, scope)) {
      panel.dispose();
    }
  }
}
