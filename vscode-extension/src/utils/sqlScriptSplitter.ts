export function splitExecutableStatements(sql: string): string[] {
  if (sql.trim().length === 0) {
    return [];
  }

  const statements: string[] = [];
  let statementStart = 0;
  let atStatementStart = true;
  let createSeen = false;
  let createTrigger = false;
  let triggerBeginDepth = 0;

  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inBracketIdentifier = false;
  let inLineComment = false;
  let inBlockComment = false;

  for (let index = 0; index < sql.length;) {
    const current = sql[index];
    const next = index + 1 < sql.length ? sql[index + 1] : "";

    if (inLineComment) {
      if (current === "\n") {
        inLineComment = false;
      }

      index++;
      continue;
    }

    if (inBlockComment) {
      if (current === "*" && next === "/") {
        inBlockComment = false;
        index += 2;
        continue;
      }

      index++;
      continue;
    }

    if (inSingleQuote) {
      if (current === "'" && next === "'") {
        index += 2;
        continue;
      }

      if (current === "'") {
        inSingleQuote = false;
      }

      index++;
      continue;
    }

    if (inDoubleQuote) {
      if (current === "\"" && next === "\"") {
        index += 2;
        continue;
      }

      if (current === "\"") {
        inDoubleQuote = false;
      }

      index++;
      continue;
    }

    if (inBracketIdentifier) {
      if (current === "]") {
        inBracketIdentifier = false;
      }

      index++;
      continue;
    }

    if (current === "-" && next === "-") {
      inLineComment = true;
      index += 2;
      continue;
    }

    if (current === "/" && next === "*") {
      inBlockComment = true;
      index += 2;
      continue;
    }

    if (current === "'") {
      inSingleQuote = true;
      if (atStatementStart) {
        atStatementStart = false;
        createSeen = false;
        createTrigger = false;
        triggerBeginDepth = 0;
      }

      index++;
      continue;
    }

    if (current === "\"") {
      inDoubleQuote = true;
      if (atStatementStart) {
        atStatementStart = false;
        createSeen = false;
        createTrigger = false;
        triggerBeginDepth = 0;
      }

      index++;
      continue;
    }

    if (current === "[") {
      inBracketIdentifier = true;
      if (atStatementStart) {
        atStatementStart = false;
        createSeen = false;
        createTrigger = false;
        triggerBeginDepth = 0;
      }

      index++;
      continue;
    }

    if (current === ";") {
      if (atStatementStart) {
        statementStart = index + 1;
        index++;
        continue;
      }

      if (!createTrigger || triggerBeginDepth === 0) {
        const statement = sql.slice(statementStart, index + 1).trim();
        if (statement.length > 0) {
          statements.push(statement);
        }

        statementStart = index + 1;
        atStatementStart = true;
        createSeen = false;
        createTrigger = false;
        triggerBeginDepth = 0;
      }

      index++;
      continue;
    }

    if (isWhitespace(current)) {
      index++;
      continue;
    }

    if (isIdentifierStart(current)) {
      const wordStart = index;
      index++;
      while (index < sql.length && isIdentifierPart(sql[index])) {
        index++;
      }

      const word = sql.slice(wordStart, index).toUpperCase();
      if (atStatementStart) {
        atStatementStart = false;
        createSeen = word === "CREATE";
        createTrigger = false;
        triggerBeginDepth = 0;
      } else if (createSeen && !createTrigger && word === "TRIGGER") {
        createTrigger = true;
      }

      if (createTrigger) {
        if (word === "BEGIN") {
          triggerBeginDepth++;
        } else if (word === "END" && triggerBeginDepth > 0) {
          triggerBeginDepth--;
        }
      }

      continue;
    }

    if (atStatementStart) {
      atStatementStart = false;
      createSeen = false;
      createTrigger = false;
      triggerBeginDepth = 0;
    }

    index++;
  }

  const trailing = sql.slice(statementStart).trim();
  if (trailing.length > 0 && containsExecutableContent(trailing)) {
    statements.push(trailing);
  }

  return statements;
}

function containsExecutableContent(sql: string): boolean {
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inBracketIdentifier = false;
  let inLineComment = false;
  let inBlockComment = false;

  for (let index = 0; index < sql.length; index++) {
    const current = sql[index];
    const next = index + 1 < sql.length ? sql[index + 1] : "";

    if (inLineComment) {
      if (current === "\n") {
        inLineComment = false;
      }

      continue;
    }

    if (inBlockComment) {
      if (current === "*" && next === "/") {
        inBlockComment = false;
        index++;
      }

      continue;
    }

    if (inSingleQuote) {
      if (current === "'" && next === "'") {
        index++;
        continue;
      }

      if (current === "'") {
        inSingleQuote = false;
      }

      continue;
    }

    if (inDoubleQuote) {
      if (current === "\"" && next === "\"") {
        index++;
        continue;
      }

      if (current === "\"") {
        inDoubleQuote = false;
      }

      continue;
    }

    if (inBracketIdentifier) {
      if (current === "]") {
        inBracketIdentifier = false;
      }

      continue;
    }

    if (current === "-" && next === "-") {
      inLineComment = true;
      index++;
      continue;
    }

    if (current === "/" && next === "*") {
      inBlockComment = true;
      index++;
      continue;
    }

    if (current === "'") {
      inSingleQuote = true;
      continue;
    }

    if (current === "\"") {
      inDoubleQuote = true;
      continue;
    }

    if (current === "[") {
      inBracketIdentifier = true;
      continue;
    }

    if (!isWhitespace(current)) {
      return true;
    }
  }

  return false;
}

function isWhitespace(value: string): boolean {
  return value === " " || value === "\t" || value === "\n" || value === "\r" || value === "\f";
}

function isIdentifierStart(value: string): boolean {
  return /[A-Za-z_]/.test(value);
}

function isIdentifierPart(value: string): boolean {
  return /[A-Za-z0-9_]/.test(value);
}
