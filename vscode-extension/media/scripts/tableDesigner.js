(function () {
  const vscode = acquireVsCodeApi();
  const root = document.getElementById("app");
  let state = window.__CDB_TABLE_DESIGNER_INITIAL_STATE__;
  let createColumns = [];

  function setState(nextState) {
    state = nextState;
    createColumns = nextState.mode === "create"
      ? nextState.createColumns.map((column) => ({ ...column }))
      : [];
    render();
  }

  function renderCreateMode() {
    const preview = buildCreateSql();
    return `
      <div class="card">
        <h2>Create Table</h2>
        <div class="toolbar">
          <input id="create-table-name" class="input" placeholder="table_name" />
          <button data-action="createTable">Create</button>
        </div>
        <div class="results-scroll">
          <table class="data-grid">
            <thead>
              <tr>
                <th>Name</th>
                <th>Type</th>
                <th>PK</th>
                <th>NOT NULL</th>
                <th>IDENTITY</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              ${createColumns.map((column, index) => `
                <tr>
                  <td><input class="input" data-create-name="${index}" value="${escapeHtml(column.name)}" /></td>
                  <td>
                    <select data-create-type="${index}">
                      ${["INTEGER", "REAL", "TEXT", "BLOB"].map((type) => `<option value="${type}" ${column.type === type ? "selected" : ""}>${type}</option>`).join("")}
                    </select>
                  </td>
                  <td><input type="checkbox" data-create-pk="${index}" ${column.primaryKey ? "checked" : ""} /></td>
                  <td><input type="checkbox" data-create-not-null="${index}" ${column.notNull ? "checked" : ""} /></td>
                  <td><input type="checkbox" data-create-identity="${index}" ${column.identity ? "checked" : ""} /></td>
                  <td><button class="danger" data-remove-column="${index}">Remove</button></td>
                </tr>
              `).join("")}
            </tbody>
          </table>
        </div>
        <div class="toolbar">
          <button class="secondary" data-action="addCreateColumn">Add Column</button>
        </div>
        <div class="card">
          <h3>SQL Preview</h3>
          <pre class="mono">${escapeHtml(preview)}</pre>
        </div>
      </div>`;
  }

  function renderAlterMode() {
    return `
      <div class="card">
        <h2>${escapeHtml(state.tableName)}</h2>
        <div class="toolbar">
          <button class="secondary" data-action="refresh">Refresh</button>
        </div>
        <div class="results-scroll">
          <table class="data-grid">
            <thead>
              <tr><th>Name</th><th>Type</th><th>PK</th><th>Identity</th><th>Nullable</th></tr>
            </thead>
            <tbody>
              ${state.columns.map((column) => `
                <tr>
                  <td>${escapeHtml(column.name)}</td>
                  <td>${escapeHtml(column.type.toUpperCase())}</td>
                  <td>${column.isPrimaryKey ? "Yes" : ""}</td>
                  <td>${column.isIdentity ? "Yes" : ""}</td>
                  <td>${column.nullable ? "Yes" : "No"}</td>
                </tr>
              `).join("")}
            </tbody>
          </table>
        </div>
      </div>
      <div class="split-layout">
        <div>
          <div class="card">
            <h3>Rename Table</h3>
            <div class="toolbar">
              <input id="rename-table-input" class="input" placeholder="new_table_name" />
              <button data-action="renameTable">Rename</button>
            </div>
          </div>
          <div class="card">
            <h3>Add Column</h3>
            <div class="toolbar">
              <input id="add-column-name" class="input" placeholder="column_name" />
              <select id="add-column-type">
                ${["INTEGER", "REAL", "TEXT", "BLOB"].map((type) => `<option value="${type}">${type}</option>`).join("")}
              </select>
              <label><input type="checkbox" id="add-column-not-null" /> NOT NULL</label>
              <button data-action="addColumn">Add</button>
            </div>
          </div>
          <div class="card">
            <h3>Rename Column</h3>
            <div class="toolbar">
              <select id="rename-column-old">
                ${state.columns.map((column) => `<option value="${escapeHtml(column.name)}">${escapeHtml(column.name)}</option>`).join("")}
              </select>
              <input id="rename-column-new" class="input" placeholder="new_name" />
              <button data-action="renameColumn">Rename</button>
            </div>
          </div>
          <div class="card">
            <h3>Drop Column</h3>
            <div class="toolbar">
              <select id="drop-column-name">
                ${state.columns.map((column) => `<option value="${escapeHtml(column.name)}">${escapeHtml(column.name)}</option>`).join("")}
              </select>
              <button class="danger" data-action="dropColumn">Drop</button>
            </div>
          </div>
        </div>
        <div>
          <div class="card">
            <h3>Indexes</h3>
            <div class="toolbar">
              <input id="index-name" class="input" placeholder="index_name" />
              <select id="index-column">
                ${state.columns.map((column) => `<option value="${escapeHtml(column.name)}">${escapeHtml(column.name)}</option>`).join("")}
              </select>
              <label><input type="checkbox" id="index-unique" /> UNIQUE</label>
              <button data-action="createIndex">Create</button>
            </div>
            <ul>
              ${state.indexes.map((index) => `<li>${escapeHtml(index.indexName)} (${escapeHtml(index.columns.join(", "))}) <button class="danger" data-drop-index="${escapeHtml(index.indexName)}">Drop</button></li>`).join("") || "<li>No indexes</li>"}
            </ul>
          </div>
          <div class="card">
            <h3>Triggers</h3>
            <div class="toolbar">
              <input id="trigger-name" class="input" placeholder="trigger_name" />
              <select id="trigger-timing">
                <option value="Before">BEFORE</option>
                <option value="After">AFTER</option>
              </select>
              <select id="trigger-event">
                <option value="Insert">INSERT</option>
                <option value="Update">UPDATE</option>
                <option value="Delete">DELETE</option>
              </select>
            </div>
            <textarea id="trigger-body" class="input" placeholder="SQL body..."></textarea>
            <div class="toolbar">
              <button data-action="createTrigger">Create Trigger</button>
            </div>
            <ul>
              ${state.triggers.map((trigger) => `<li>${escapeHtml(trigger.triggerName)} (${escapeHtml(`${trigger.timing} ${trigger.event}`)}) <button class="danger" data-drop-trigger="${escapeHtml(trigger.triggerName)}">Drop</button></li>`).join("") || "<li>No triggers</li>"}
            </ul>
          </div>
        </div>
      </div>`;
  }

  function render() {
    if (!state) {
      root.innerHTML = "";
      return;
    }

    root.innerHTML = `
      <div class="panel-shell">
        ${state.statusMessage ? `<div class="status-banner">${escapeHtml(state.statusMessage)}</div>` : ""}
        ${state.mode === "create" ? renderCreateMode() : renderAlterMode()}
      </div>`;
  }

  function buildCreateSql() {
    const tableName = document.getElementById("create-table-name")?.value || "table_name";
    const definitions = createColumns
      .filter((column) => column.name)
      .map((column) => {
        const parts = [column.name, column.type];
        if (column.primaryKey) parts.push("PRIMARY KEY");
        if (column.identity) parts.push("IDENTITY");
        if (column.notNull) parts.push("NOT NULL");
        return parts.join(" ");
      });
    return `CREATE TABLE ${tableName} (${definitions.join(", ")});`;
  }

  function collectCreatePayload() {
    return {
      tableName: document.getElementById("create-table-name").value,
      columns: createColumns
    };
  }

  document.addEventListener("click", (event) => {
    const actionTarget = event.target.closest("[data-action]");
    if (actionTarget) {
      switch (actionTarget.dataset.action) {
        case "refresh":
          vscode.postMessage({ type: "refresh" });
          return;
        case "addCreateColumn":
          createColumns.push({ name: "", type: "TEXT", primaryKey: false, notNull: false, identity: false });
          render();
          return;
        case "createTable":
          vscode.postMessage({ type: "createTable", payload: collectCreatePayload() });
          return;
        case "renameTable":
          vscode.postMessage({ type: "renameTable", payload: { newName: document.getElementById("rename-table-input").value } });
          return;
        case "addColumn":
          vscode.postMessage({
            type: "addColumn",
            payload: {
              columnName: document.getElementById("add-column-name").value,
              type: document.getElementById("add-column-type").value,
              notNull: document.getElementById("add-column-not-null").checked
            }
          });
          return;
        case "renameColumn":
          vscode.postMessage({
            type: "renameColumn",
            payload: {
              oldName: document.getElementById("rename-column-old").value,
              newName: document.getElementById("rename-column-new").value
            }
          });
          return;
        case "dropColumn":
          if (window.confirm("Drop the selected column?")) {
            vscode.postMessage({ type: "dropColumn", payload: { columnName: document.getElementById("drop-column-name").value } });
          }
          return;
        case "createIndex":
          vscode.postMessage({
            type: "createIndex",
            payload: {
              indexName: document.getElementById("index-name").value,
              columnName: document.getElementById("index-column").value,
              isUnique: document.getElementById("index-unique").checked
            }
          });
          return;
        case "createTrigger":
          vscode.postMessage({
            type: "createTrigger",
            payload: {
              triggerName: document.getElementById("trigger-name").value,
              timing: document.getElementById("trigger-timing").value,
              event: document.getElementById("trigger-event").value,
              bodySql: document.getElementById("trigger-body").value
            }
          });
          return;
        default:
          return;
      }
    }

    const removeColumnTarget = event.target.closest("[data-remove-column]");
    if (removeColumnTarget) {
      createColumns.splice(Number(removeColumnTarget.dataset.removeColumn), 1);
      render();
      return;
    }

    const dropIndexTarget = event.target.closest("[data-drop-index]");
    if (dropIndexTarget && window.confirm(`Drop index '${dropIndexTarget.dataset.dropIndex}'?`)) {
      vscode.postMessage({ type: "dropIndex", payload: { indexName: dropIndexTarget.dataset.dropIndex } });
      return;
    }

    const dropTriggerTarget = event.target.closest("[data-drop-trigger]");
    if (dropTriggerTarget && window.confirm(`Drop trigger '${dropTriggerTarget.dataset.dropTrigger}'?`)) {
      vscode.postMessage({ type: "dropTrigger", payload: { triggerName: dropTriggerTarget.dataset.dropTrigger } });
    }
  });

  document.addEventListener("change", (event) => {
    const nameTarget = event.target.closest("[data-create-name]");
    if (nameTarget) {
      createColumns[Number(nameTarget.dataset.createName)].name = nameTarget.value;
      render();
      return;
    }

    const typeTarget = event.target.closest("[data-create-type]");
    if (typeTarget) {
      createColumns[Number(typeTarget.dataset.createType)].type = typeTarget.value;
      render();
      return;
    }

    const pkTarget = event.target.closest("[data-create-pk]");
    if (pkTarget) {
      createColumns[Number(pkTarget.dataset.createPk)].primaryKey = pkTarget.checked;
      render();
      return;
    }

    const notNullTarget = event.target.closest("[data-create-not-null]");
    if (notNullTarget) {
      createColumns[Number(notNullTarget.dataset.createNotNull)].notNull = notNullTarget.checked;
      render();
      return;
    }

    const identityTarget = event.target.closest("[data-create-identity]");
    if (identityTarget) {
      createColumns[Number(identityTarget.dataset.createIdentity)].identity = identityTarget.checked;
      render();
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
