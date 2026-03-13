namespace CSharpDB.Sql;

public readonly record struct SqlStatementClassification(Statement Statement, bool IsReadOnly)
{
    public bool IsMutating => !IsReadOnly;
    public bool IsQuery => IsReadOnly;
}

public static class SqlStatementClassifier
{
    public static SqlStatementClassification Classify(string sql)
    {
        ArgumentNullException.ThrowIfNull(sql);

        if (Parser.TryParseSimpleSelect(sql, out var fastStatement))
            return new SqlStatementClassification(fastStatement, IsReadOnly(fastStatement));

        var statement = Parser.Parse(sql);
        return new SqlStatementClassification(statement, IsReadOnly(statement));
    }

    public static bool IsReadOnly(Statement statement)
    {
        ArgumentNullException.ThrowIfNull(statement);
        return statement is QueryStatement or WithStatement;
    }
}
