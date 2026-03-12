(function () {
  const vscode = acquireVsCodeApi();
  const root = document.getElementById("app");
  let state = window.__CDB_DIAGNOSTICS_INITIAL_STATE__;

  function renderTable(rows) {
    return `
      <table class="data-grid">
        <tbody>
          ${rows.map(([label, value]) => `<tr><th>${escapeHtml(label)}</th><td>${escapeHtml(value)}</td></tr>`).join("")}
        </tbody>
      </table>`;
  }

  function renderIssues(issues) {
    if (!issues.length) {
      return "<p>No warnings or errors detected.</p>";
    }

    return `
      <table class="data-grid">
        <thead><tr><th>Severity</th><th>Code</th><th>Message</th><th>Page</th></tr></thead>
        <tbody>
          ${issues.map((issue) => `<tr><td>${escapeHtml(issue.severity)}</td><td>${escapeHtml(issue.code)}</td><td>${escapeHtml(issue.message)}</td><td>${escapeHtml(issue.pageId ?? "-")}</td></tr>`).join("")}
        </tbody>
      </table>`;
  }

  function renderPageHistogram(histogram) {
    return `
      <table class="data-grid">
        <thead><tr><th>Page Type</th><th>Count</th></tr></thead>
        <tbody>
          ${Object.entries(histogram).sort(([a], [b]) => a.localeCompare(b)).map(([key, value]) => `<tr><td>${escapeHtml(key)}</td><td>${escapeHtml(value)}</td></tr>`).join("")}
        </tbody>
      </table>`;
  }

  function setState(nextState) {
    state = nextState;
    render();
  }

  function render() {
    if (!state) {
      root.innerHTML = "";
      return;
    }

    const combinedIssues = [
      ...state.databaseReport.issues,
      ...state.walReport.issues,
      ...state.indexReport.issues
    ];

    const indexOptions = state.indexReport.indexes.map((index) => `<option value="${escapeHtml(index.indexName)}">${escapeHtml(index.indexName)}</option>`).join("");
    const tableOptions = [...new Set(state.indexReport.indexes.map((index) => index.tableName))].map((tableName) => `<option value="${escapeHtml(tableName)}">${escapeHtml(tableName)}</option>`).join("");

    root.innerHTML = `
      <div class="panel-shell">
        ${state.statusMessage ? `<div class="status-banner">${escapeHtml(state.statusMessage)}</div>` : ""}
        <div class="panel-header">
          <div>
            <h1 class="panel-title">Storage Diagnostics</h1>
            <div class="panel-subtitle mono">${escapeHtml(state.databaseReport.databasePath)}</div>
          </div>
          <button data-action="refresh">Refresh</button>
        </div>
        <div class="grid-layout">
          <div class="card">
            <h3>Database Header</h3>
            ${renderTable([
              ["File Length", state.databaseReport.header.fileLengthBytes],
              ["Physical Pages", state.databaseReport.header.physicalPageCount],
              ["Declared Pages", state.databaseReport.header.declaredPageCount],
              ["Magic", `${state.databaseReport.header.magic} (${state.databaseReport.header.magicValid ? "ok" : "bad"})`],
              ["Version", `${state.databaseReport.header.version} (${state.databaseReport.header.versionValid ? "ok" : "bad"})`],
              ["Page Size", `${state.databaseReport.header.pageSize} (${state.databaseReport.header.pageSizeValid ? "ok" : "bad"})`],
              ["Schema Root", state.databaseReport.header.schemaRootPage],
              ["Freelist Head", state.databaseReport.header.freelistHead]
            ])}
          </div>
          <div class="card">
            <h3>WAL</h3>
            ${renderTable([
              ["Path", state.walReport.walPath],
              ["Exists", state.walReport.exists ? "Yes" : "No"],
              ["File Length", state.walReport.fileLengthBytes],
              ["Frames", state.walReport.fullFrameCount],
              ["Commit Frames", state.walReport.commitFrameCount],
              ["Trailing Bytes", state.walReport.trailingBytes]
            ])}
          </div>
          <div class="card">
            <h3>Space Usage</h3>
            ${renderTable([
              ["Database File", state.maintenanceReport.spaceUsage.databaseFileBytes],
              ["WAL File", state.maintenanceReport.spaceUsage.walFileBytes],
              ["Page Size", state.maintenanceReport.spaceUsage.pageSizeBytes],
              ["Freelist Pages", state.maintenanceReport.spaceUsage.freelistPageCount],
              ["Freelist Bytes", state.maintenanceReport.spaceUsage.freelistBytes]
            ])}
          </div>
          <div class="card">
            <h3>Fragmentation</h3>
            ${renderTable([
              ["B+tree Free Bytes", state.maintenanceReport.fragmentation.bTreeFreeBytes],
              ["Pages With Free Space", state.maintenanceReport.fragmentation.pagesWithFreeSpace],
              ["Tail Freelist Pages", state.maintenanceReport.fragmentation.tailFreelistPageCount],
              ["Tail Freelist Bytes", state.maintenanceReport.fragmentation.tailFreelistBytes]
            ])}
          </div>
        </div>
        <div class="grid-layout">
          <div class="card">
            <h3>Maintenance</h3>
            <div class="toolbar">
              <select id="reindex-scope">
                <option value="all">All</option>
                <option value="table">Table</option>
                <option value="index">Index</option>
              </select>
              <select id="reindex-name">
                <option value="">(all)</option>
                ${tableOptions}
                ${indexOptions}
              </select>
              <button data-action="reindex">Reindex</button>
              <button class="danger" data-action="vacuum">Vacuum</button>
            </div>
          </div>
          <div class="card">
            <h3>Page Drill-Down</h3>
            <div class="toolbar">
              <input id="page-id" class="input" type="number" min="0" value="${escapeHtml(state.pageReport?.pageId ?? 0)}" />
              <label><input type="checkbox" id="include-hex" /> Include Hex</label>
              <button data-action="inspectPage">Inspect</button>
            </div>
            ${state.pageReport ? renderTable([
              ["Exists", state.pageReport.exists ? "Yes" : "No"],
              ["Page Type", state.pageReport.page?.pageTypeName ?? "-"],
              ["Cell Count", state.pageReport.page?.cellCount ?? "-"],
              ["Free Space", state.pageReport.page?.freeSpaceBytes ?? "-"]
            ]) : "<p>Inspect a page to load page-level details.</p>"}
            ${state.pageReport?.hexDump ? `<pre class="mono">${escapeHtml(state.pageReport.hexDump)}</pre>` : ""}
          </div>
        </div>
        <div class="card">
          <h3>Page Type Histogram</h3>
          ${renderPageHistogram(state.databaseReport.pageTypeHistogram)}
        </div>
        <div class="card">
          <h3>Index Checks</h3>
          <table class="data-grid">
            <thead><tr><th>Index</th><th>Table</th><th>Root OK</th><th>Table OK</th><th>Columns OK</th><th>Reachable</th></tr></thead>
            <tbody>
              ${state.indexReport.indexes.map((index) => `<tr><td>${escapeHtml(index.indexName)}</td><td>${escapeHtml(index.tableName)}</td><td>${index.rootPageValid ? "Yes" : "No"}</td><td>${index.tableExists ? "Yes" : "No"}</td><td>${index.columnsExistInTable ? "Yes" : "No"}</td><td>${index.rootTreeReachable ? "Yes" : "No"}</td></tr>`).join("")}
            </tbody>
          </table>
        </div>
        <div class="card">
          <h3>Integrity Issues</h3>
          ${renderIssues(combinedIssues)}
        </div>
      </div>`;
  }

  document.addEventListener("click", (event) => {
    const actionTarget = event.target.closest("[data-action]");
    if (!actionTarget) {
      return;
    }

    switch (actionTarget.dataset.action) {
      case "refresh":
        vscode.postMessage({ type: "refresh" });
        return;
      case "inspectPage":
        vscode.postMessage({
          type: "inspectPage",
          payload: {
            pageId: Number(document.getElementById("page-id").value),
            includeHex: document.getElementById("include-hex").checked
          }
        });
        return;
      case "reindex":
        vscode.postMessage({
          type: "reindex",
          payload: {
            scope: document.getElementById("reindex-scope").value,
            name: document.getElementById("reindex-name").value
          }
        });
        return;
      case "vacuum":
        if (window.confirm("Run a full vacuum rewrite?")) {
          vscode.postMessage({ type: "vacuum" });
        }
        return;
      default:
        return;
    }
  });

  window.addEventListener("message", (event) => {
    if (event.data?.type === "state") {
      setState(event.data.state);
    }
  });

  vscode.postMessage({ type: "ready" });

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }
})();
