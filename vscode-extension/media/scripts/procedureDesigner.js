(function () {
  const vscode = acquireVsCodeApi();
  const root = document.getElementById("app");
  let state = window.__CDB_PROCEDURE_INITIAL_STATE__;
  let draft = null;

  function cloneDraft(nextState) {
    return {
      name: nextState.procedureName ?? "",
      description: nextState.description ?? "",
      isEnabled: Boolean(nextState.isEnabled),
      bodySql: nextState.bodySql ?? "",
      parameters: (nextState.parameters ?? []).map((parameter) => ({ ...parameter }))
    };
  }

  function setState(nextState) {
    state = nextState;
    draft = cloneDraft(nextState);
    render();
  }

  function addParameter() {
    draft.parameters.push({
      name: "",
      type: "TEXT",
      required: false,
      defaultText: "",
      description: ""
    });
    render();
  }

  function removeParameter(index) {
    draft.parameters.splice(index, 1);
    render();
  }

  function saveProcedure() {
    vscode.postMessage({
      type: "saveProcedure",
      payload: {
        name: draft.name,
        description: draft.description,
        isEnabled: draft.isEnabled,
        bodySql: draft.bodySql,
        parameters: draft.parameters
      }
    });
  }

  function deleteProcedure() {
    const name = state.procedureName || draft.name || "this procedure";
    if (!window.confirm(`Delete procedure '${name}'?`)) {
      return;
    }

    vscode.postMessage({ type: "deleteProcedure" });
  }

  function renderParameterRows() {
    if (!draft.parameters.length) {
      return '<div class="empty-state">No parameters defined.</div>';
    }

    return `
      <table class="data-grid">
        <thead>
          <tr>
            <th>Name</th>
            <th>Type</th>
            <th>Required</th>
            <th>Default</th>
            <th>Description</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          ${draft.parameters.map((parameter, index) => `
            <tr>
              <td><input class="input mono" data-param-index="${index}" data-param-field="name" value="${escapeHtml(parameter.name)}" /></td>
              <td>
                <select class="input" data-param-index="${index}" data-param-field="type">
                  ${["INTEGER", "REAL", "TEXT", "BLOB"].map((type) => `<option value="${type}" ${parameter.type === type ? "selected" : ""}>${type}</option>`).join("")}
                </select>
              </td>
              <td class="checkbox-cell">
                <input type="checkbox" data-param-index="${index}" data-param-field="required" ${parameter.required ? "checked" : ""} />
              </td>
              <td><input class="input mono" data-param-index="${index}" data-param-field="defaultText" value="${escapeHtml(parameter.defaultText)}" /></td>
              <td><input class="input" data-param-index="${index}" data-param-field="description" value="${escapeHtml(parameter.description)}" /></td>
              <td><button data-action="removeParameter" data-param-index="${index}" class="danger compact-button">Remove</button></td>
            </tr>`).join("")}
        </tbody>
      </table>`;
  }

  function render() {
    if (!state || !draft) {
      root.innerHTML = "";
      return;
    }

    root.innerHTML = `
      <div class="panel-shell">
        <div class="panel-header">
          <div>
            <h1 class="panel-title">${escapeHtml(state.procedureName ?? "New Procedure")}</h1>
            <div class="panel-subtitle">${state.mode === "create" ? "Create stored procedure" : "Edit stored procedure"}</div>
          </div>
          <div class="meta-row">
            <span class="badge">${draft.isEnabled ? "enabled" : "disabled"}</span>
            ${state.updatedUtc ? `<span class="badge">updated ${escapeHtml(state.updatedUtc)}</span>` : ""}
          </div>
        </div>
        ${state.statusMessage ? `<div class="status-banner">${escapeHtml(state.statusMessage)}</div>` : ""}
        ${state.executionSupported ? "" : '<div class="status-banner warning">Procedure execution is not supported by the NativeAOT extension client yet. Editing is supported.</div>'}
        <div class="toolbar">
          <button data-action="saveProcedure">Save</button>
          ${state.mode === "edit" ? '<button data-action="deleteProcedure" class="danger">Delete</button>' : ""}
          <button data-action="refresh" class="secondary">Refresh</button>
          <div class="toolbar-spacer"></div>
        </div>
        <div class="card form-card">
          <div class="form-grid">
            <div class="field-group">
              <label>Name</label>
              <input class="input mono" data-field="name" value="${escapeHtml(draft.name)}" />
            </div>
            <div class="field-group">
              <label>Description</label>
              <input class="input" data-field="description" value="${escapeHtml(draft.description)}" />
            </div>
            <div class="field-group">
              <label>State</label>
              <label class="inline-checkbox">
                <input type="checkbox" data-field="isEnabled" ${draft.isEnabled ? "checked" : ""} />
                Enabled
              </label>
            </div>
          </div>
          <div class="field-group">
            <label>Body SQL</label>
            <textarea class="input mono editor-textarea" data-field="bodySql" placeholder="SQL body with @param references...">${escapeHtml(draft.bodySql)}</textarea>
            <div class="help-text">Use @parameter placeholders that match the parameter list below.</div>
          </div>
        </div>
        <div class="card">
          <div class="card-header">
            <div>
              <h2>Parameters</h2>
              <div class="help-text">Default values are parsed as JSON when possible; otherwise they are stored as text.</div>
            </div>
            <button data-action="addParameter" class="secondary">Add Parameter</button>
          </div>
          ${renderParameterRows()}
        </div>
      </div>`;
  }

  document.addEventListener("click", (event) => {
    const actionTarget = event.target.closest("[data-action]");
    if (!actionTarget) {
      return;
    }

    switch (actionTarget.dataset.action) {
      case "saveProcedure":
        saveProcedure();
        return;
      case "deleteProcedure":
        deleteProcedure();
        return;
      case "refresh":
        vscode.postMessage({ type: "refresh" });
        return;
      case "addParameter":
        addParameter();
        return;
      case "removeParameter":
        removeParameter(Number(actionTarget.dataset.paramIndex));
        return;
      default:
        return;
    }
  });

  document.addEventListener("input", (event) => {
    const fieldTarget = event.target.closest("[data-field]");
    if (fieldTarget) {
      switch (fieldTarget.dataset.field) {
        case "name":
          draft.name = fieldTarget.value;
          break;
        case "description":
          draft.description = fieldTarget.value;
          break;
        case "bodySql":
          draft.bodySql = fieldTarget.value;
          break;
        default:
          break;
      }
      return;
    }

    const parameterTarget = event.target.closest("[data-param-index][data-param-field]");
    if (!parameterTarget) {
      return;
    }

    const parameter = draft.parameters[Number(parameterTarget.dataset.paramIndex)];
    if (!parameter) {
      return;
    }

    parameter[parameterTarget.dataset.paramField] = parameterTarget.value;
  });

  document.addEventListener("change", (event) => {
    const fieldTarget = event.target.closest("[data-field]");
    if (fieldTarget && fieldTarget.dataset.field === "isEnabled") {
      draft.isEnabled = fieldTarget.checked;
      return;
    }

    const parameterTarget = event.target.closest("[data-param-index][data-param-field]");
    if (!parameterTarget) {
      return;
    }

    const parameter = draft.parameters[Number(parameterTarget.dataset.paramIndex)];
    if (!parameter) {
      return;
    }

    if (parameterTarget.dataset.paramField === "required") {
      parameter.required = parameterTarget.checked;
      return;
    }

    parameter[parameterTarget.dataset.paramField] = parameterTarget.value;
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
