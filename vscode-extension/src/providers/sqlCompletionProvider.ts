import * as vscode from "vscode";
import { SchemaTreeProvider } from "./schemaTreeProvider";

const KEYWORDS = [
  "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
  "DELETE", "CREATE", "ALTER", "DROP", "TABLE", "INDEX", "VIEW", "TRIGGER", "JOIN", "INNER", "LEFT", "RIGHT", "OUTER",
  "CROSS", "ON", "ORDER", "BY", "ASC", "DESC", "GROUP", "HAVING", "LIMIT", "OFFSET", "AS", "DISTINCT", "ALL", "EXISTS",
  "BETWEEN", "LIKE", "UNION", "BEGIN", "END", "COMMIT", "ROLLBACK", "TRANSACTION", "PRIMARY", "KEY", "UNIQUE", "DEFAULT",
  "CHECK", "FOREIGN", "REFERENCES", "IF", "ELSE", "CASE", "WHEN", "THEN", "COLUMN", "ADD", "RENAME", "TO", "BEFORE",
  "AFTER", "FOR", "EACH", "ROW", "IDENTITY", "AUTOINCREMENT", "INTEGER", "TEXT", "REAL", "BLOB", "BOOLEAN", "INT",
  "VARCHAR", "CHAR", "TRUE", "FALSE", "EXEC", "EXECUTE"
];

const FUNCTIONS = [
  "COUNT", "SUM", "AVG", "MIN", "MAX", "CAST", "COALESCE", "IFNULL", "UPPER", "LOWER", "LENGTH", "SUBSTR", "TRIM",
  "REPLACE", "ABS", "ROUND", "DATE", "TIME", "DATETIME", "TYPEOF", "TOTAL", "GROUP_CONCAT"
];

export class SqlCompletionProvider implements vscode.CompletionItemProvider {
  constructor(private readonly schemaProvider: SchemaTreeProvider) {}

  provideCompletionItems(document: vscode.TextDocument, position: vscode.Position): vscode.ProviderResult<vscode.CompletionItem[]> {
    const snapshot = this.schemaProvider.getSnapshot();
    const linePrefix = document.lineAt(position).text.slice(0, position.character);
    const items: vscode.CompletionItem[] = [];

    if (snapshot) {
      const qualifiedMatch = /([A-Za-z_][A-Za-z0-9_]*)\.\w*$/.exec(linePrefix);
      if (qualifiedMatch) {
        const targetName = qualifiedMatch[1];
        const table = snapshot.tables.find((entry) => equalsIgnoreCase(entry.tableName, targetName));
        if (table) {
          return table.columns.map((column) => createColumnCompletion(column.name, column.type));
        }

        const view = snapshot.views.find((entry) => equalsIgnoreCase(entry.viewName, targetName));
        if (view) {
          const schema = this.schemaProvider.getTableSchema(view.viewName);
          if (schema) {
            return schema.columns.map((column) => createColumnCompletion(column.name, column.type));
          }
        }
      }

      if (/(?:from|join|into|update|table|view|trigger)\s+[A-Za-z_0-9]*$/i.test(linePrefix)) {
        items.push(
          ...snapshot.tables.map((table) => createObjectCompletion(table.tableName, vscode.CompletionItemKind.Class, "table")),
          ...snapshot.views.map((view) => createObjectCompletion(view.viewName, vscode.CompletionItemKind.Interface, "view"))
        );
      }

      items.push(
        ...snapshot.tables.map((table) => createObjectCompletion(table.tableName, vscode.CompletionItemKind.Class, "table")),
        ...snapshot.views.map((view) => createObjectCompletion(view.viewName, vscode.CompletionItemKind.Interface, "view"))
      );
    }

    items.push(...KEYWORDS.map((keyword) => createKeywordCompletion(keyword)));
    items.push(...FUNCTIONS.map((fn) => createFunctionCompletion(fn)));
    items.push(createSnippetCompletion("SELECT template", "SELECT ${1:*}\nFROM ${2:table}\nWHERE ${3:condition};"));
    items.push(createSnippetCompletion("CREATE TABLE template", "CREATE TABLE ${1:table_name} (\n\t${2:id} INTEGER PRIMARY KEY IDENTITY,\n\t${3:name} TEXT NOT NULL\n);"));

    return items;
  }
}

function createKeywordCompletion(keyword: string): vscode.CompletionItem {
  const item = new vscode.CompletionItem(keyword, vscode.CompletionItemKind.Keyword);
  item.insertText = keyword;
  return item;
}

function createFunctionCompletion(name: string): vscode.CompletionItem {
  const item = new vscode.CompletionItem(name, vscode.CompletionItemKind.Function);
  item.insertText = new vscode.SnippetString(`${name}($1)`);
  return item;
}

function createObjectCompletion(name: string, kind: vscode.CompletionItemKind, detail: string): vscode.CompletionItem {
  const item = new vscode.CompletionItem(name, kind);
  item.detail = detail;
  return item;
}

function createColumnCompletion(name: string, type: string): vscode.CompletionItem {
  const item = new vscode.CompletionItem(name, vscode.CompletionItemKind.Field);
  item.detail = type.toUpperCase();
  return item;
}

function createSnippetCompletion(label: string, snippet: string): vscode.CompletionItem {
  const item = new vscode.CompletionItem(label, vscode.CompletionItemKind.Snippet);
  item.insertText = new vscode.SnippetString(snippet);
  return item;
}

function equalsIgnoreCase(left: string, right: string): boolean {
  return left.localeCompare(right, undefined, { sensitivity: "base" }) === 0;
}
