import * as vscode from "vscode";
import { SqlResultResponse } from "../api/types";
import { escapeHtml, renderWebviewPage } from "../utils/htmlBuilder";
import { registerCSharpDbPanel, unregisterCSharpDbPanel } from "./panelRegistry";

export class QueryResultsPanel {
  private static panel?: vscode.WebviewPanel;

  static show(extensionUri: vscode.Uri, sql: string, result: SqlResultResponse): void {
    if (QueryResultsPanel.panel) {
      QueryResultsPanel.panel.reveal(QueryResultsPanel.panel.viewColumn ?? vscode.ViewColumn.Active, true);
    } else {
      QueryResultsPanel.panel = vscode.window.createWebviewPanel(
        "csharpdb.queryResults",
        "CSharpDB Query Results",
        vscode.ViewColumn.Active,
        {
          enableScripts: true,
          retainContextWhenHidden: true
        }
      );
      registerCSharpDbPanel(QueryResultsPanel.panel, "auxiliary");

      QueryResultsPanel.panel.onDidDispose(() => {
        unregisterCSharpDbPanel(QueryResultsPanel.panel!);
        QueryResultsPanel.panel = undefined;
      });
    }

    QueryResultsPanel.panel.webview.html = QueryResultsPanel.render(extensionUri, QueryResultsPanel.panel.webview, sql, result);
  }

  private static render(extensionUri: vscode.Uri, webview: vscode.Webview, sql: string, result: SqlResultResponse): string {
    const title = result.error ? "Query Error" : result.isQuery ? "Query Result" : "Statement Result";
    const meta = `
      <div class="meta-row">
        <span class="badge">${result.isQuery ? "Query" : "Mutation"}</span>
        <span>${result.elapsedMs.toFixed(1)} ms</span>
        <span>${result.isQuery ? `${result.rows?.length ?? 0} rows` : `${result.rowsAffected} rows affected`}</span>
      </div>`;

    let body = `
      <div class="panel-shell">
        <div class="panel-header">
          <div>
            <h1 class="panel-title">${title}</h1>
            <div class="panel-subtitle">Executed against the active CSharpDB connection.</div>
          </div>
        </div>
        ${meta}
        <div class="card">
          <h3>SQL</h3>
          <pre class="mono">${escapeHtml(sql)}</pre>
        </div>`;

    if (result.error) {
      body += `<div class="status-banner error">${escapeHtml(result.error)}</div>`;
    } else if (!result.isQuery) {
      body += `<div class="status-banner">${result.rowsAffected} row(s) affected.</div>`;
    } else {
      body += `
        <div class="card">
          <h3>Rows</h3>
          <div class="results-scroll">
            ${renderGrid(result)}
          </div>
        </div>`;
    }

    body += "</div>";

    return renderWebviewPage({
      title: "CSharpDB Query Results",
      body,
      extensionUri,
      webview,
      styles: ["media/styles/panel.css", "media/styles/grid.css"],
      scripts: ["media/scripts/grid.js"]
    });
  }
}

function renderGrid(result: SqlResultResponse): string {
  const columns = result.columnNames ?? [];
  const rows = result.rows ?? [];

  if (columns.length === 0) {
    return "<p>No result columns were returned.</p>";
  }

  const header = columns
    .map((column, index) => `<th data-sortable="true" data-column-index="${index}">${escapeHtml(column)}</th>`)
    .join("");

  const bodyRows = rows
    .map((row) => {
      const cells = columns.map((column) => renderCell(row[column]));
      return `<tr>${cells.join("")}</tr>`;
    })
    .join("");

  return `<table class="data-grid"><thead><tr>${header}</tr></thead><tbody>${bodyRows}</tbody></table>`;
}

function renderCell(value: unknown): string {
  if (value === null || value === undefined) {
    return `<td class="null-cell" data-value="">NULL</td>`;
  }

  if (typeof value === "object") {
    const serialized = JSON.stringify(value);
    return `<td class="blob-cell" data-value="${escapeHtml(serialized)}" data-copy="${escapeHtml(serialized)}">${escapeHtml(serialized)}</td>`;
  }

  const text = String(value);
  return `<td data-value="${escapeHtml(text)}" data-copy="${escapeHtml(text)}">${escapeHtml(text)}</td>`;
}
