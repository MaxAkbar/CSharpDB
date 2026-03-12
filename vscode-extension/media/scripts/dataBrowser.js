(function () {
  const vscode = acquireVsCodeApi();
  const root = document.getElementById("app");
  let state = window.__CDB_BROWSER_INITIAL_STATE__;
  let drafts = [];
  let filters = {};
  let sortColumn = null;
  let sortAscending = true;
  let selectedIds = new Set();
  let newRowCounter = 0;
  let activePane = "data";
  let viewDraft = null;

  function normalizeValue(value) {
    if (value === "") {
      return null;
    }

    return value;
  }

  function cloneRows(rows) {
    return rows.map((row) => ({
      id: row.id,
      originalPk: row.originalPk,
      originalValues: { ...row.values },
      currentValues: { ...row.values },
      status: "clean"
    }));
  }

  function cloneViewDefinition(viewDefinition) {
    if (!viewDefinition) {
      return null;
    }

    return {
      viewName: viewDefinition.viewName,
      sql: viewDefinition.sql
    };
  }

  function setState(nextState) {
    const sameObject = state &&
      state.objectType === nextState.objectType &&
      state.objectName === nextState.objectName;

    state = nextState;
    drafts = cloneRows(nextState.rows);
    filters = {};
    selectedIds = new Set();
    sortColumn = null;
    sortAscending = true;
    viewDraft = cloneViewDefinition(nextState.viewDefinition);
    activePane = nextState.objectType === "view" && sameObject ? activePane : "data";
    render();
  }

  function getDirtyCount() {
    return drafts.filter((row) => row.status !== "clean").length;
  }

  function getViewDirty() {
    if (!state || state.objectType !== "view" || !state.viewDefinition || !viewDraft) {
      return false;
    }

    return state.viewDefinition.viewName !== viewDraft.viewName ||
      state.viewDefinition.sql !== viewDraft.sql;
  }

  function ensureCanDiscard() {
    if (getDirtyCount() === 0) {
      return true;
    }

    return window.confirm("Discard unsaved changes?");
  }

  function ensureCanDiscardViewChanges() {
    if (!getViewDirty()) {
      return true;
    }

    return window.confirm("Discard unsaved view changes?");
  }

  function addRow() {
    const values = {};
    state.columns.forEach((column) => {
      values[column.name] = null;
    });

    drafts.unshift({
      id: `new:${newRowCounter++}`,
      originalPk: undefined,
      originalValues: { ...values },
      currentValues: { ...values },
      status: "new"
    });
    render();
  }

  function deleteSelected() {
    drafts = drafts
      .map((row) => {
        if (!selectedIds.has(row.id)) {
          return row;
        }

        if (row.status === "new") {
          return null;
        }

        return { ...row, status: "deleted" };
      })
      .filter(Boolean);

    selectedIds = new Set();
    render();
  }

  function discardChanges() {
    drafts = cloneRows(state.rows);
    selectedIds = new Set();
    render();
  }

  function resetViewDraft() {
    viewDraft = cloneViewDefinition(state.viewDefinition);
    render();
  }

  function saveChanges() {
    const payload = {
      newRows: drafts
        .filter((row) => row.status === "new")
        .map((row) => ({ ...row.currentValues })),
      updatedRows: drafts
        .filter((row) => row.status === "modified")
        .map((row) => ({ originalPk: row.originalPk, values: { ...row.currentValues } })),
      deletedRows: drafts
        .filter((row) => row.status === "deleted")
        .map((row) => ({ originalPk: row.originalPk }))
    };

    vscode.postMessage({ type: "saveChanges", payload });
  }

  function saveView() {
    if (!viewDraft) {
      return;
    }

    vscode.postMessage({
      type: "saveView",
      payload: {
        viewName: viewDraft.viewName,
        sql: viewDraft.sql
      }
    });
  }

  function dropView() {
    if (!state || state.objectType !== "view") {
      return;
    }

    if (!window.confirm(`Drop view '${state.objectName}'?`)) {
      return;
    }

    vscode.postMessage({ type: "dropView" });
  }

  function applyFiltersAndSort(rows) {
    let result = rows.filter((row) => {
      if (row.status === "deleted") {
        return true;
      }

      return Object.entries(filters).every(([columnName, filterValue]) => {
        if (!filterValue) {
          return true;
        }

        const cellValue = row.currentValues[columnName];
        return String(cellValue ?? "NULL").toLowerCase().includes(filterValue.toLowerCase());
      });
    });

    if (sortColumn) {
      result = [...result].sort((left, right) => {
        const leftValue = left.currentValues[sortColumn];
        const rightValue = right.currentValues[sortColumn];
        const comparison = compareValues(leftValue, rightValue);
        return sortAscending ? comparison : -comparison;
      });
    }

    return result;
  }

  function compareValues(left, right) {
    if (left === right) {
      return 0;
    }

    if (left == null) {
      return -1;
    }

    if (right == null) {
      return 1;
    }

    const leftNumber = Number(left);
    const rightNumber = Number(right);
    if (!Number.isNaN(leftNumber) && !Number.isNaN(rightNumber)) {
      return leftNumber - rightNumber;
    }

    return String(left).localeCompare(String(right), undefined, { sensitivity: "base" });
  }

  function toggleSort(columnName) {
    if (sortColumn === columnName) {
      sortAscending = !sortAscending;
    } else {
      sortColumn = columnName;
      sortAscending = true;
    }

    render();
  }

  function renderCell(column, row) {
    const value = row.currentValues[column.name];
    const isExistingPrimaryKey = column.isPrimaryKey && row.status !== "new";
    const editable = !state.readOnly && !isExistingPrimaryKey && row.status !== "deleted";
    const textValue = value == null ? "" : String(value);

    if (!editable) {
      return `<td class="editable-cell">${escapeHtml(value == null ? "NULL" : textValue)}</td>`;
    }

    return `<td><input class="input editable-cell-input" data-row-id="${row.id}" data-column-name="${column.name}" value="${escapeHtml(textValue)}" placeholder="NULL" /></td>`;
  }

  function renderModeSwitcher() {
    if (!state || state.objectType !== "view") {
      return "";
    }

    return `
      <div class="mode-switcher">
        <button data-action="showData" class="${activePane === "data" ? "active" : "secondary"}">Data</button>
        <button data-action="showSchema" class="${activePane === "schema" ? "active" : "secondary"}">Schema</button>
      </div>`;
  }

  function renderToolbar(dirtyCount) {
    if (state.objectType === "table") {
      return `
        <div class="toolbar">
          <button data-action="refresh" class="secondary">Refresh</button>
          <button data-action="addRow" ${state.readOnly ? "disabled" : ""}>Add Row</button>
          <button data-action="deleteSelected" class="danger" ${state.readOnly || selectedIds.size === 0 ? "disabled" : ""}>Delete Selected</button>
          <button data-action="save" ${state.readOnly || dirtyCount === 0 ? "disabled" : ""}>Save</button>
          <button data-action="discard" class="secondary" ${state.readOnly || dirtyCount === 0 ? "disabled" : ""}>Discard</button>
          <div class="toolbar-spacer"></div>
          <label>Page size</label>
          <select data-page-size>
            ${renderPageSizes()}
          </select>
          <button data-action="prevPage" class="secondary" ${state.page <= 1 ? "disabled" : ""}>Prev</button>
          <button data-action="nextPage" class="secondary" ${state.page >= state.totalPages ? "disabled" : ""}>Next</button>
        </div>`;
    }

    if (activePane === "schema") {
      return `
        <div class="toolbar">
          ${renderModeSwitcher()}
          <button data-action="refresh" class="secondary">Refresh</button>
          <button data-action="saveView">Save View</button>
          <button data-action="resetView" class="secondary">Reset</button>
          <button data-action="dropView" class="danger">Drop View</button>
          <div class="toolbar-spacer"></div>
        </div>`;
    }

    return `
      <div class="toolbar">
        ${renderModeSwitcher()}
        <button data-action="refresh" class="secondary">Refresh</button>
        <div class="toolbar-spacer"></div>
        <label>Page size</label>
        <select data-page-size>
          ${renderPageSizes()}
        </select>
        <button data-action="prevPage" class="secondary" ${state.page <= 1 ? "disabled" : ""}>Prev</button>
        <button data-action="nextPage" class="secondary" ${state.page >= state.totalPages ? "disabled" : ""}>Next</button>
      </div>`;
  }

  function renderPageSizes() {
    return [10, 25, 50, 100]
      .map((size) => `<option value="${size}" ${state.pageSize === size ? "selected" : ""}>${size}</option>`)
      .join("");
  }

  function renderGrid() {
    const visibleRows = applyFiltersAndSort(drafts);
    const tableHeaders = state.columns
      .map((column) => {
        const badge = column.type ? `<span class="badge">${escapeHtml(column.type.toUpperCase())}</span>` : "";
        const sortMark = sortColumn === column.name ? (sortAscending ? " ↑" : " ↓") : "";
        return `<th class="sortable" data-sort-column="${column.name}">${escapeHtml(column.name)}${sortMark}<div>${badge}</div></th>`;
      })
      .join("");

    const filterCells = state.columns
      .map((column) => `<td><input class="input" data-filter-column="${column.name}" value="${escapeHtml(filters[column.name] ?? "")}" placeholder="Filter..." /></td>`)
      .join("");

    const bodyRows = visibleRows.length > 0
      ? visibleRows.map((row) => {
        const classes = [
          row.status === "new" ? "row-new" : "",
          row.status === "modified" ? "row-modified" : "",
          row.status === "deleted" ? "row-deleted" : "",
          selectedIds.has(row.id) ? "row-selected" : ""
        ].join(" ").trim();
        const rowLabel = row.status === "new" ? "new" : row.status === "modified" ? "edited" : row.status === "deleted" ? "deleted" : "";
        return `
          <tr class="${classes}">
            <td><input type="checkbox" data-select-row="${row.id}" ${selectedIds.has(row.id) ? "checked" : ""} /></td>
            <td>${escapeHtml(rowLabel)}</td>
            ${state.columns.map((column) => renderCell(column, row)).join("")}
          </tr>`;
      }).join("")
      : `<tr><td colspan="${state.columns.length + 2}">No rows on this page.</td></tr>`;

    return `
      <div class="card results-scroll">
        <table class="data-grid">
          <thead>
            <tr>
              <th></th>
              <th>Status</th>
              ${tableHeaders}
            </tr>
            <tr class="filter-row">
              <td></td>
              <td></td>
              ${filterCells}
            </tr>
          </thead>
          <tbody>${bodyRows}</tbody>
        </table>
      </div>`;
  }

  function renderViewSchema() {
    const currentView = viewDraft || { viewName: state.objectName, sql: "" };
    return `
      <div class="card form-card">
        <div class="form-grid">
          <div class="field-group">
            <label>View Name</label>
            <input class="input mono" data-view-name value="${escapeHtml(currentView.viewName)}" />
          </div>
          <div class="field-group">
            <label>Status</label>
            <div class="meta-row">
              <span class="badge">${getViewDirty() ? "unsaved changes" : "saved"}</span>
              <span class="badge">read-only data</span>
            </div>
          </div>
        </div>
        <div class="field-group">
          <label>SELECT SQL</label>
          <textarea class="input mono editor-textarea" data-view-sql placeholder="SELECT ...">${escapeHtml(currentView.sql)}</textarea>
          <div class="help-text">Enter only the SELECT body. The extension will rebuild the view definition.</div>
        </div>
      </div>`;
  }

  function render() {
    if (!state) {
      root.innerHTML = "";
      return;
    }

    const dirtyCount = getDirtyCount();
    const showingSchema = state.objectType === "view" && activePane === "schema";
    const subtitle = state.objectType === "table"
      ? "Table browser"
      : showingSchema ? "View schema" : "View browser";
    const metaBadges = showingSchema
      ? `
          <span class="badge">${getViewDirty() ? "unsaved" : "saved"}</span>
          <span class="badge">schema</span>`
      : `
          <span class="badge">${state.totalRows} rows</span>
          <span class="badge">${state.page} / ${state.totalPages}</span>
          ${state.readOnly ? '<span class="badge">read-only</span>' : `<span class="badge">${dirtyCount} dirty</span>`}`;

    root.innerHTML = `
      <div class="panel-shell">
        <div class="panel-header">
          <div>
            <h1 class="panel-title">${escapeHtml(state.objectName)}</h1>
            <div class="panel-subtitle">${subtitle}</div>
          </div>
          <div class="meta-row">
            ${metaBadges}
          </div>
        </div>
        ${state.statusMessage ? `<div class="status-banner">${escapeHtml(state.statusMessage)}</div>` : ""}
        ${renderToolbar(dirtyCount)}
        ${showingSchema ? renderViewSchema() : renderGrid()}
      </div>`;
  }

  function updateCell(rowId, columnName, value) {
    const row = drafts.find((entry) => entry.id === rowId);
    if (!row || row.status === "deleted") {
      return;
    }

    row.currentValues[columnName] = normalizeValue(value);
    row.status = row.status === "new" ? "new" : "modified";
  }

  function changePage(page) {
    if (!ensureCanDiscard()) {
      return;
    }

    vscode.postMessage({ type: "changePage", page });
  }

  function refresh() {
    if (state.objectType === "view" && activePane === "schema" && !ensureCanDiscardViewChanges()) {
      return;
    }

    if (state.objectType === "table" && !ensureCanDiscard()) {
      return;
    }

    vscode.postMessage({ type: "refresh" });
  }

  document.addEventListener("click", (event) => {
    const actionTarget = event.target.closest("[data-action]");
    if (actionTarget) {
      switch (actionTarget.dataset.action) {
        case "refresh":
          refresh();
          return;
        case "addRow":
          addRow();
          return;
        case "deleteSelected":
          deleteSelected();
          return;
        case "save":
          saveChanges();
          return;
        case "discard":
          discardChanges();
          return;
        case "prevPage":
          changePage(state.page - 1);
          return;
        case "nextPage":
          changePage(state.page + 1);
          return;
        case "showData":
          activePane = "data";
          render();
          return;
        case "showSchema":
          activePane = "schema";
          render();
          return;
        case "saveView":
          saveView();
          return;
        case "resetView":
          resetViewDraft();
          return;
        case "dropView":
          dropView();
          return;
        default:
          return;
      }
    }

    const sortTarget = event.target.closest("[data-sort-column]");
    if (sortTarget) {
      toggleSort(sortTarget.dataset.sortColumn);
    }
  });

  document.addEventListener("input", (event) => {
    const editTarget = event.target.closest("[data-row-id][data-column-name]");
    if (editTarget) {
      updateCell(editTarget.dataset.rowId, editTarget.dataset.columnName, editTarget.value);
      return;
    }

    const viewNameTarget = event.target.closest("[data-view-name]");
    if (viewNameTarget && viewDraft) {
      viewDraft.viewName = viewNameTarget.value;
      return;
    }

    const viewSqlTarget = event.target.closest("[data-view-sql]");
    if (viewSqlTarget && viewDraft) {
      viewDraft.sql = viewSqlTarget.value;
    }
  });

  document.addEventListener("change", (event) => {
    const filterTarget = event.target.closest("[data-filter-column]");
    if (filterTarget) {
      filters[filterTarget.dataset.filterColumn] = filterTarget.value;
      render();
      return;
    }

    const editTarget = event.target.closest("[data-row-id][data-column-name]");
    if (editTarget) {
      updateCell(editTarget.dataset.rowId, editTarget.dataset.columnName, editTarget.value);
      render();
      return;
    }

    const pageSizeTarget = event.target.closest("[data-page-size]");
    if (pageSizeTarget) {
      if (!ensureCanDiscard()) {
        pageSizeTarget.value = String(state.pageSize);
        return;
      }

      vscode.postMessage({ type: "setPageSize", pageSize: Number(pageSizeTarget.value) });
      return;
    }

    const selectTarget = event.target.closest("[data-select-row]");
    if (!selectTarget) {
      return;
    }

    if (selectTarget.checked) {
      selectedIds.add(selectTarget.dataset.selectRow);
    } else {
      selectedIds.delete(selectTarget.dataset.selectRow);
    }

    render();
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
