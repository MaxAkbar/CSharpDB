(function () {
  function compareValues(left, right) {
    if (left === right) {
      return 0;
    }

    if (left === null || left === "") {
      return -1;
    }

    if (right === null || right === "") {
      return 1;
    }

    const leftNumber = Number(left);
    const rightNumber = Number(right);
    if (!Number.isNaN(leftNumber) && !Number.isNaN(rightNumber)) {
      return leftNumber - rightNumber;
    }

    return String(left).localeCompare(String(right), undefined, { sensitivity: "base" });
  }

  function sortTable(table, columnIndex, ascending) {
    const body = table.tBodies[0];
    if (!body) {
      return;
    }

    const rows = Array.from(body.rows);
    rows.sort((left, right) => {
      const leftCell = left.cells[columnIndex];
      const rightCell = right.cells[columnIndex];
      const result = compareValues(leftCell?.dataset.value ?? leftCell?.innerText ?? "", rightCell?.dataset.value ?? rightCell?.innerText ?? "");
      return ascending ? result : -result;
    });

    rows.forEach((row) => body.appendChild(row));
  }

  document.addEventListener("click", (event) => {
    const header = event.target.closest("th[data-sortable='true']");
    if (header) {
      const table = header.closest("table");
      if (!table) {
        return;
      }

      const columnIndex = Number(header.dataset.columnIndex);
      const ascending = header.dataset.sortDirection !== "asc";
      table.querySelectorAll("th[data-sortable='true']").forEach((th) => {
        th.dataset.sortDirection = "";
      });
      header.dataset.sortDirection = ascending ? "asc" : "desc";
      sortTable(table, columnIndex, ascending);
      return;
    }

    const copyTarget = event.target.closest("[data-copy]");
    if (copyTarget) {
      navigator.clipboard?.writeText(copyTarget.dataset.copy ?? "");
    }
  });
})();
