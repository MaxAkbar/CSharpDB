import * as vscode from "vscode";
import { ColumnResponse } from "../api/types";
import { SchemaTreeProvider } from "./schemaTreeProvider";

export class SqlHoverProvider implements vscode.HoverProvider {
  constructor(private readonly schemaProvider: SchemaTreeProvider) {}

  provideHover(document: vscode.TextDocument, position: vscode.Position): vscode.ProviderResult<vscode.Hover> {
    const snapshot = this.schemaProvider.getSnapshot();
    if (!snapshot) {
      return undefined;
    }

    const range = document.getWordRangeAtPosition(position, /[A-Za-z_][A-Za-z0-9_]*/);
    if (!range) {
      return undefined;
    }

    const word = document.getText(range);
    const table = snapshot.tables.find((entry) => equalsIgnoreCase(entry.tableName, word));
    if (table) {
      const markdown = new vscode.MarkdownString(undefined, true);
      markdown.appendMarkdown(`### Table \`${table.tableName}\`\n`);
      for (const column of table.columns) {
        markdown.appendMarkdown(`- \`${column.name}\` ${formatColumn(column)}\n`);
      }

      return new vscode.Hover(markdown, range);
    }

    const view = snapshot.views.find((entry) => equalsIgnoreCase(entry.viewName, word));
    if (view) {
      const markdown = new vscode.MarkdownString(undefined, true);
      markdown.appendMarkdown(`### View \`${view.viewName}\`\n`);
      markdown.appendCodeblock(view.sql, "sql");
      return new vscode.Hover(markdown, range);
    }

    const linePrefix = document.lineAt(position).text.slice(0, position.character);
    const qualifiedMatch = /([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$/.exec(linePrefix);
    if (qualifiedMatch) {
      const tableName = qualifiedMatch[1];
      const columnName = qualifiedMatch[2];
      const tableSchema = snapshot.tables.find((entry) => equalsIgnoreCase(entry.tableName, tableName));
      const column = tableSchema?.columns.find((entry) => equalsIgnoreCase(entry.name, columnName));
      if (column) {
        return new vscode.Hover(renderColumnHover(tableName, column), range);
      }
    }

    const matches = snapshot.tables.flatMap((tableSchema) =>
      tableSchema.columns
        .filter((column) => equalsIgnoreCase(column.name, word))
        .map((column) => ({ tableName: tableSchema.tableName, column }))
    );

    if (matches.length === 1) {
      return new vscode.Hover(renderColumnHover(matches[0].tableName, matches[0].column), range);
    }

    if (matches.length > 1) {
      const markdown = new vscode.MarkdownString(undefined, true);
      markdown.appendMarkdown(`### Column \`${word}\`\n`);
      markdown.appendMarkdown(matches.map((match) => `- \`${match.tableName}.${match.column.name}\` ${formatColumn(match.column)}`).join("\n"));
      return new vscode.Hover(markdown, range);
    }

    return undefined;
  }
}

function renderColumnHover(tableName: string, column: ColumnResponse): vscode.MarkdownString {
  const markdown = new vscode.MarkdownString(undefined, true);
  markdown.appendMarkdown(`### Column \`${tableName}.${column.name}\`\n`);
  markdown.appendMarkdown(`- Type: \`${column.type.toUpperCase()}\`\n`);
  markdown.appendMarkdown(`- Nullable: ${column.nullable ? "Yes" : "No"}\n`);
  markdown.appendMarkdown(`- Primary key: ${column.isPrimaryKey ? "Yes" : "No"}\n`);
  markdown.appendMarkdown(`- Identity: ${column.isIdentity ? "Yes" : "No"}\n`);
  return markdown;
}

function formatColumn(column: ColumnResponse): string {
  const parts = [column.type.toUpperCase()];
  if (column.isPrimaryKey) {
    parts.push("PK");
  }

  if (column.isIdentity) {
    parts.push("IDENTITY");
  }

  if (!column.nullable) {
    parts.push("NOT NULL");
  }

  return parts.join(" ");
}

function equalsIgnoreCase(left: string, right: string): boolean {
  return left.localeCompare(right, undefined, { sensitivity: "base" }) === 0;
}
