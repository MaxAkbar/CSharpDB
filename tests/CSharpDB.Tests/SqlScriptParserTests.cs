using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class SqlScriptParserTests
{
    [Theory]
    [InlineData("SELECT * FROM first_table; DROP TABLE first_table;", "DROP")]
    [InlineData("INSERT INTO first_table VALUES (1); DELETE FROM first_table;", "DELETE")]
    [InlineData("CREATE TABLE first_table (id INTEGER); DROP TABLE first_table;", "DROP")]
    public void ParserParse_RejectsExecutableTokensAfterOptionalSemicolon(
        string sql,
        string trailingKeyword)
    {
        var error = Assert.Throws<CSharpDbException>(() => Parser.Parse(sql));

        Assert.Equal(ErrorCode.SyntaxError, error.Code);
        Assert.Contains(
            $"position {sql.IndexOf(trailingKeyword, StringComparison.Ordinal)}",
            error.Message,
            StringComparison.Ordinal);
        Assert.Contains("after statement", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Parse_ReturnsExactAbsoluteSpansAndExcludesCommentsAndEmptyStatements()
    {
        const string first = "INSERT INTO t VALUES ('it''s;ok');";
        const string second = "CREATE TABLE \"semi;table\" (id INTEGER)";
        string script =
            "-- heading ;\r\n" +
            ";; /* ignored ; */\r\n" +
            $"  {first}\r\n" +
            "/* gap ; */\r\n" +
            $"  {second}\r\n" +
            "-- trailing comment ;";

        IReadOnlyList<SqlScriptStatement> statements = SqlScriptParser.Parse(
            script,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, statements.Count);
        AssertStatement(statements[0], 0, first, script.IndexOf(first, StringComparison.Ordinal), 3, 3);
        AssertStatement(statements[1], 1, second, script.IndexOf(second, StringComparison.Ordinal), 5, 3);
        Assert.IsType<InsertStatement>(statements[0].Statement);
        Assert.IsType<CreateTableStatement>(statements[1].Statement);
    }

    [Fact]
    public void Parse_KeepsTriggerAndConditionalBodiesWhole()
    {
        string script = """
            CREATE TRIGGER tr AFTER INSERT ON items BEGIN
                INSERT INTO audit VALUES ('body;value');
            END;
            IF NOT EXISTS (SELECT 1 FROM audit) BEGIN
                INSERT INTO audit VALUES ('conditional');
            END
            """;

        IReadOnlyList<SqlScriptStatement> statements = SqlScriptParser.Parse(
            script,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, statements.Count);
        Assert.IsType<CreateTriggerStatement>(statements[0].Statement);
        Assert.IsType<ConditionalStatement>(statements[1].Statement);
        Assert.EndsWith("END;", statements[0].Text, StringComparison.Ordinal);
        Assert.EndsWith("END", statements[1].Text, StringComparison.Ordinal);
    }

    [Fact]
    public void Parse_ReportsSyntaxErrorsWithAbsoluteSpanAndRule()
    {
        const string script = "-- preface\n  SELECT FROM t;";
        int expectedStart = script.IndexOf("FROM", StringComparison.Ordinal);

        var error = Assert.Throws<SqlScriptParseException>(
            () => SqlScriptParser.Parse(
                script,
                cancellationToken: TestContext.Current.CancellationToken));

        Assert.Equal(SqlScriptParseErrorCategory.Syntax, error.Category);
        Assert.Equal("statement.syntax", error.Rule);
        Assert.Equal(new SqlSourceSpan(expectedStart, 4, 2, 10), error.Span);
    }

    [Fact]
    public void Parse_EnforcesCharacterUtf8StatementTokenAndNestingLimits()
    {
        AssertLimit(
            "SELECT 1",
            SqlScriptParserOptions.Default with { MaxScriptCharacters = 5 },
            "script.max-characters",
            expectedStart: 5);

        AssertLimit(
            "é",
            SqlScriptParserOptions.Default with { MaxScriptUtf8Bytes = 1 },
            "script.max-utf8-bytes",
            expectedStart: 0);

        AssertLimit(
            "SELECT 1; SELECT 2;",
            SqlScriptParserOptions.Default with { MaxStatementCount = 1 },
            "script.max-statements",
            expectedStart: 10);

        AssertLimit(
            "SELECT 1;",
            SqlScriptParserOptions.Default with { MaxStatementCharacters = 8 },
            "statement.max-characters",
            expectedStart: 0);

        AssertLimit(
            "SELECT 1 FROM t",
            SqlScriptParserOptions.Default with { MaxTokenCount = 2 },
            "script.max-tokens",
            expectedStart: 9);

        AssertLimit(
            "SELECT (((1)))",
            SqlScriptParserOptions.Default with { MaxNestingDepth = 2 },
            "statement.max-nesting",
            expectedStart: 9);
    }

    [Fact]
    public void Parse_ObservesPreCanceledToken()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.Throws<OperationCanceledException>(
            () => SqlScriptParser.Parse("SELECT 1;", cancellationToken: cancellation.Token));
    }

    [Theory]
    [InlineData(nameof(SqlScriptParserOptions.MaxScriptCharacters))]
    [InlineData(nameof(SqlScriptParserOptions.MaxScriptUtf8Bytes))]
    [InlineData(nameof(SqlScriptParserOptions.MaxStatementCount))]
    [InlineData(nameof(SqlScriptParserOptions.MaxStatementCharacters))]
    [InlineData(nameof(SqlScriptParserOptions.MaxTokenCount))]
    [InlineData(nameof(SqlScriptParserOptions.MaxNestingDepth))]
    public void Parse_RejectsLimitsAboveProductionCeilings(string optionName)
    {
        SqlScriptParserOptions options = optionName switch
        {
            nameof(SqlScriptParserOptions.MaxScriptCharacters) =>
                SqlScriptParserOptions.Default with
                {
                    MaxScriptCharacters = SqlScriptParserOptions.HardMaxScriptCharacters + 1,
                },
            nameof(SqlScriptParserOptions.MaxScriptUtf8Bytes) =>
                SqlScriptParserOptions.Default with
                {
                    MaxScriptUtf8Bytes = SqlScriptParserOptions.HardMaxScriptUtf8Bytes + 1,
                },
            nameof(SqlScriptParserOptions.MaxStatementCount) =>
                SqlScriptParserOptions.Default with
                {
                    MaxStatementCount = SqlScriptParserOptions.HardMaxStatementCount + 1,
                },
            nameof(SqlScriptParserOptions.MaxStatementCharacters) =>
                SqlScriptParserOptions.Default with
                {
                    MaxStatementCharacters = SqlScriptParserOptions.HardMaxStatementCharacters + 1,
                },
            nameof(SqlScriptParserOptions.MaxTokenCount) =>
                SqlScriptParserOptions.Default with
                {
                    MaxTokenCount = SqlScriptParserOptions.HardMaxTokenCount + 1,
                },
            nameof(SqlScriptParserOptions.MaxNestingDepth) =>
                SqlScriptParserOptions.Default with
                {
                    MaxNestingDepth = SqlScriptParserOptions.HardMaxNestingDepth + 1,
                },
            _ => throw new InvalidOperationException($"Unexpected option '{optionName}'."),
        };

        var error = Assert.Throws<ArgumentOutOfRangeException>(
            () => SqlScriptParser.Parse(
                "SELECT 1;",
                options,
                TestContext.Current.CancellationToken));

        Assert.Equal(optionName, error.ParamName);
    }

    private static void AssertStatement(
        SqlScriptStatement statement,
        int expectedIndex,
        string expectedText,
        int expectedStart,
        int expectedLine,
        int expectedColumn)
    {
        Assert.Equal(expectedIndex, statement.Index);
        Assert.Equal(expectedText, statement.Text);
        Assert.Equal(
            new SqlSourceSpan(
                expectedStart,
                expectedText.Length,
                expectedLine,
                expectedColumn),
            statement.Span);
    }

    private static void AssertLimit(
        string script,
        SqlScriptParserOptions options,
        string expectedRule,
        int expectedStart)
    {
        var error = Assert.Throws<SqlScriptParseException>(
            () => SqlScriptParser.Parse(
                script,
                options,
                TestContext.Current.CancellationToken));

        Assert.Equal(SqlScriptParseErrorCategory.Limit, error.Category);
        Assert.Equal(expectedRule, error.Rule);
        Assert.Equal(expectedStart, error.Span.Start);
    }
}
