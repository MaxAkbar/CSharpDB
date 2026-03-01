using CSharpDB.Core;
using CSharpDB.Sql;

namespace CSharpDB.Cli;

internal static class SqlScriptParser
{
    public static bool TrySplitCompleteStatements(
        string sql,
        out List<string> statements,
        out string remainder,
        out string? error)
    {
        statements = new List<string>();
        remainder = sql;
        error = null;

        try
        {
            SplitCore(sql, statements, out remainder);
            return true;
        }
        catch (CSharpDbException ex)
        {
            error = ex.Message;
            return false;
        }
    }

    public static IReadOnlyList<string> SplitAllStatements(string sql)
    {
        var statements = new List<string>();
        SplitCore(sql, statements, out string remainder);

        if (!string.IsNullOrWhiteSpace(remainder))
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                "SQL script ended with an incomplete statement (missing semicolon).");
        }

        return statements;
    }

    private static void SplitCore(string sql, List<string> statements, out string remainder)
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
    }
}
