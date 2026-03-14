using CSharpDB.Primitives;

namespace CSharpDB.Sql;

public static class SqlScriptSplitter
{
    public static bool TrySplitCompleteStatements(
        string sql,
        out List<string> statements,
        out string remainder,
        out string? error)
    {
        ArgumentNullException.ThrowIfNull(sql);

        statements = new List<string>();
        remainder = sql;
        error = null;

        try
        {
            SplitCore(sql, statements, out remainder, includeTrailingStatement: false);
            ValidateStatements(statements);
            return true;
        }
        catch (CSharpDbException ex)
        {
            statements = new List<string>();
            remainder = sql;
            error = ex.Message;
            return false;
        }
    }

    public static IReadOnlyList<string> SplitExecutableStatements(string sql)
    {
        ArgumentNullException.ThrowIfNull(sql);

        if (string.IsNullOrWhiteSpace(sql))
            return Array.Empty<string>();

        var statements = new List<string>();
        SplitCore(sql, statements, out _, includeTrailingStatement: true);
        return statements;
    }

    private static void ValidateStatements(IEnumerable<string> statements)
    {
        foreach (var statement in statements)
            _ = Parser.Parse(statement);
    }

    private static void SplitCore(string sql, List<string> statements, out string remainder, bool includeTrailingStatement)
    {
        var tokens = new Tokenizer(sql).Tokenize();
        int statementStart = 0;
        bool atStatementStart = true;
        bool createSeen = false;
        bool createTrigger = false;
        int triggerBeginDepth = 0;

        foreach (var token in tokens)
        {
            if (token.Type == TokenType.Eof)
                break;

            if (atStatementStart)
            {
                if (token.Type == TokenType.Semicolon)
                {
                    statementStart = token.Position + 1;
                    continue;
                }

                atStatementStart = false;
                createSeen = token.Type == TokenType.Create;
                createTrigger = false;
                triggerBeginDepth = 0;
            }
            else if (createSeen && !createTrigger && token.Type == TokenType.Trigger)
            {
                createTrigger = true;
            }

            if (createTrigger)
            {
                if (token.Type == TokenType.Begin)
                    triggerBeginDepth++;
                else if (token.Type == TokenType.End && triggerBeginDepth > 0)
                    triggerBeginDepth--;
            }

            if (token.Type == TokenType.Semicolon)
            {
                bool isStatementTerminator = !createTrigger || triggerBeginDepth == 0;
                if (!isStatementTerminator)
                    continue;

                int statementEnd = token.Position + 1;
                if (statementEnd > statementStart)
                {
                    string statement = sql[statementStart..statementEnd].Trim();
                    if (statement.Length > 0)
                        statements.Add(statement);
                }

                statementStart = statementEnd;
                atStatementStart = true;
                createSeen = false;
                createTrigger = false;
                triggerBeginDepth = 0;
            }
        }

        remainder = statementStart < sql.Length ? sql[statementStart..] : string.Empty;

        if (!includeTrailingStatement)
            return;

        string trailingStatement = remainder.Trim();
        if (trailingStatement.Length > 0 && ContainsExecutableTokens(trailingStatement))
            statements.Add(trailingStatement);

        remainder = string.Empty;
    }

    private static bool ContainsExecutableTokens(string sql)
    {
        var tokens = new Tokenizer(sql).Tokenize();
        return tokens.Any(token => token.Type != TokenType.Eof);
    }
}
