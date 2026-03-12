import * as vscode from "vscode";

export interface WebviewPageOptions {
  title: string;
  body: string;
  extensionUri: vscode.Uri;
  webview: vscode.Webview;
  styles?: string[];
  scripts?: string[];
  inlineScript?: string;
}

export function getNonce(): string {
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let nonce = "";
  for (let i = 0; i < 32; i += 1) {
    nonce += alphabet.charAt(Math.floor(Math.random() * alphabet.length));
  }

  return nonce;
}

export function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

export function toWebviewUri(webview: vscode.Webview, extensionUri: vscode.Uri, relativePath: string): string {
  return webview.asWebviewUri(vscode.Uri.joinPath(extensionUri, relativePath)).toString();
}

export function renderWebviewPage(options: WebviewPageOptions): string {
  const nonce = getNonce();
  const styleUris = (options.styles ?? []).map((stylePath) => toWebviewUri(options.webview, options.extensionUri, stylePath));
  const scriptUris = (options.scripts ?? []).map((scriptPath) => toWebviewUri(options.webview, options.extensionUri, scriptPath));
  const csp = [
    "default-src 'none'",
    `img-src ${options.webview.cspSource} https: data:`,
    `font-src ${options.webview.cspSource}`,
    `style-src ${options.webview.cspSource} 'unsafe-inline'`,
    `script-src 'nonce-${nonce}'`
  ].join("; ");

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta http-equiv="Content-Security-Policy" content="${csp}" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>${escapeHtml(options.title)}</title>
  ${styleUris.map((href) => `<link rel="stylesheet" href="${href}" />`).join("\n  ")}
</head>
<body>
  ${options.body}
  ${scriptUris.map((src) => `<script nonce="${nonce}" src="${src}"></script>`).join("\n  ")}
  ${options.inlineScript ? `<script nonce="${nonce}">${options.inlineScript}</script>` : ""}
</body>
</html>`;
}
