using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class SqlScriptSplitterTests
{
    [Fact]
    public void TrySplitCompleteStatements_KeepsTriggerBodyAsSingleStatement()
    {
        string sql = """
            CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER);
            CREATE TRIGGER tr AFTER INSERT ON t BEGIN
                INSERT INTO t VALUES (NEW.id + 100, NEW.n);
            END;
            INSERT INTO t VALUES (1, 10);
            """;

        bool ok = SqlScriptSplitter.TrySplitCompleteStatements(sql, out var statements, out var remainder, out var error);

        Assert.True(ok);
        Assert.Null(error);
        Assert.Equal(string.Empty, remainder);
        Assert.Equal(3, statements.Count);
        Assert.Contains("CREATE TRIGGER tr AFTER INSERT ON t BEGIN", statements[1], StringComparison.OrdinalIgnoreCase);
        Assert.Contains("END;", statements[1], StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TrySplitCompleteStatements_IgnoresEmptyStatements()
    {
        string sql = ";;CREATE TABLE t (id INTEGER PRIMARY KEY);;;;INSERT INTO t VALUES (1);;";

        bool ok = SqlScriptSplitter.TrySplitCompleteStatements(sql, out var statements, out var remainder, out var error);

        Assert.True(ok);
        Assert.Null(error);
        Assert.Equal(string.Empty, remainder);
        Assert.Equal(2, statements.Count);
    }

    [Fact]
    public void TrySplitCompleteStatements_ReturnsIncompleteTrailingRemainder()
    {
        string sql = """
            CREATE TABLE t (id INTEGER PRIMARY KEY);
            INSERT INTO t VALUES (1
            """;

        bool ok = SqlScriptSplitter.TrySplitCompleteStatements(sql, out var statements, out var remainder, out var error);

        Assert.True(ok);
        Assert.Null(error);
        Assert.Single(statements);
        Assert.Contains("INSERT INTO t VALUES (1", remainder, StringComparison.Ordinal);
    }

    [Fact]
    public void TrySplitCompleteStatements_ReturnsParserErrorForInvalidCompleteStatement()
    {
        string sql = "SELECT FROM t;";

        bool ok = SqlScriptSplitter.TrySplitCompleteStatements(sql, out var statements, out var remainder, out var error);

        Assert.False(ok);
        Assert.Empty(statements);
        Assert.Equal(sql, remainder);
        Assert.NotNull(error);
        Assert.Contains("Syntax error", error, StringComparison.Ordinal);
    }

    [Fact]
    public void SplitExecutableStatements_AllowsFinalStatementWithoutSemicolon()
    {
        string sql = "CREATE TABLE t (id INTEGER PRIMARY KEY); INSERT INTO t VALUES (1)";

        var statements = SqlScriptSplitter.SplitExecutableStatements(sql);

        Assert.Equal(2, statements.Count);
        Assert.Equal("INSERT INTO t VALUES (1)", statements[1]);
    }

    [Fact]
    public void SplitExecutableStatements_IgnoresTrailingCommentOnlyRemainder()
    {
        string sql = """
            SELECT * FROM t;
            -- Admin-only examples can live here without becoming executable statements
            -- EXEC ExampleProc arg=1;
            """;

        var statements = SqlScriptSplitter.SplitExecutableStatements(sql);

        Assert.Single(statements);
        Assert.Equal("SELECT * FROM t;", statements[0]);
    }

    [Theory]
    [InlineData("SELECT * FROM t;", true)]
    [InlineData("INSERT INTO t VALUES (1);", false)]
    [InlineData("UPDATE t SET n = 1;", false)]
    [InlineData("DELETE FROM t WHERE id = 1;", false)]
    [InlineData("CREATE TABLE t (id INTEGER PRIMARY KEY);", false)]
    [InlineData("CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO log VALUES (1); END;", false)]
    public void Classify_ReturnsExpectedReadOnlyState(string sql, bool expectedReadOnly)
    {
        var classification = SqlStatementClassifier.Classify(sql);

        Assert.Equal(expectedReadOnly, classification.IsReadOnly);
        Assert.Equal(!expectedReadOnly, classification.IsMutating);
        Assert.Equal(expectedReadOnly, classification.IsQuery);
    }
}
