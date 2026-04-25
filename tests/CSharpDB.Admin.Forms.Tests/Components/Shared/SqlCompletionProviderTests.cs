using CSharpDB.Admin.Helpers;

namespace CSharpDB.Admin.Forms.Tests.Components.Shared;

public sealed class SqlCompletionProviderTests
{
    [Fact]
    public void GetCompletions_TypedS_SuggestsSelect()
    {
        var result = SqlCompletionProvider.GetCompletions("s", 1, CreateCatalog());

        var suggestion = Assert.Single(result.Suggestions, s => s.Label == "SELECT");
        Assert.Equal(0, suggestion.ReplacementStart);
        Assert.Equal(1, suggestion.ReplacementEnd);
        Assert.Equal("SELECT ", suggestion.InsertText);
    }

    [Fact]
    public void GetCompletions_TypedLi_SuggestsLimit()
    {
        var result = SqlCompletionProvider.GetCompletions("li", 2, CreateCatalog());

        var suggestion = Assert.Single(result.Suggestions, s => s.Label == "LIMIT");
        Assert.Equal(0, suggestion.ReplacementStart);
        Assert.Equal(2, suggestion.ReplacementEnd);
        Assert.Equal("LIMIT ", suggestion.InsertText);
    }

    [Fact]
    public void GetCompletions_AfterUpdateTableTypingSe_SuggestsSet()
    {
        var result = SqlCompletionProvider.GetCompletions("UPDATE Customers se", "UPDATE Customers se".Length, CreateCatalog());

        var suggestion = Assert.Single(result.Suggestions, s => s.Label == "SET");
        Assert.Equal("SET ", suggestion.InsertText);
    }

    [Fact]
    public void GetCompletions_AfterUpdateTableWithTrailingSpace_SuggestsSet()
    {
        string sql = "UPDATE Customers ";

        var result = SqlCompletionProvider.GetCompletions(sql, sql.Length, CreateCatalog());

        var suggestion = Assert.Single(result.Suggestions, s => s.Label == "SET");
        Assert.Equal("SET ", suggestion.InsertText);
    }

    [Fact]
    public void GetCompletions_InsideInsertColumnList_SuggestsColumns()
    {
        string sql = "INSERT INTO Customers (";

        var result = SqlCompletionProvider.GetCompletions(sql, sql.Length, CreateCatalog());

        Assert.DoesNotContain(result.Suggestions, s => s.Label == "*");
        Assert.Contains(result.Suggestions, s => s.Label == "CustomerId");
        Assert.Contains(result.Suggestions, s => s.Label == "Name");
        Assert.Contains(result.Suggestions, s => s.Label == "Email");
    }

    [Fact]
    public void GetCompletions_AfterInsertColumnComma_SuggestsColumns()
    {
        string sql = "INSERT INTO Customers (CustomerId, ";

        var result = SqlCompletionProvider.GetCompletions(sql, sql.Length, CreateCatalog());

        Assert.Contains(result.Suggestions, s => s.Label == "Name");
        Assert.Contains(result.Suggestions, s => s.Label == "Email");
    }

    [Fact]
    public void GetCompletions_AfterInsertColumnListWithTrailingSpace_SuggestsValues()
    {
        string sql = "INSERT INTO Customers (CustomerId, Name) ";

        var result = SqlCompletionProvider.GetCompletions(sql, sql.Length, CreateCatalog());

        var suggestion = Assert.Single(result.Suggestions, s => s.Label == "VALUES");
        Assert.Equal("VALUES ", suggestion.InsertText);
    }

    [Fact]
    public void GetCompletions_AfterInsertColumnListTypingVa_SuggestsValues()
    {
        string sql = "INSERT INTO Customers (CustomerId, Name) va";

        var result = SqlCompletionProvider.GetCompletions(sql, sql.Length, CreateCatalog());

        var suggestion = Assert.Single(result.Suggestions, s => s.Label == "VALUES");
        Assert.Equal("VALUES ", suggestion.InsertText);
    }

    [Fact]
    public void GetCompletions_AfterSelectWithoutSource_SuggestsSources()
    {
        var result = SqlCompletionProvider.GetCompletions("SELECT ", 7, CreateCatalog());

        Assert.Contains(result.Suggestions, s => s.Label == "Customers" && s.Detail == "table");
        Assert.Contains(result.Suggestions, s => s.Label == "CustomerSummary" && s.Detail == "view");
    }

    [Fact]
    public void GetCompletions_SelectSourceSuggestionInsertsFromAndLeavesCaretInSelectList()
    {
        var result = SqlCompletionProvider.GetCompletions("SELECT ", 7, CreateCatalog());

        var suggestion = Assert.Single(result.Suggestions, s => s.Label == "Customers");
        Assert.Equal($"{Environment.NewLine}FROM Customers", suggestion.InsertText);
        Assert.Equal(7, suggestion.CaretPosition);
    }

    [Fact]
    public void GetCompletions_SelectListWithSourceAfterCaret_SuggestsColumns()
    {
        string sql = $"SELECT {Environment.NewLine}FROM Customers";

        var result = SqlCompletionProvider.GetCompletions(sql, "SELECT ".Length, CreateCatalog());

        Assert.Contains(result.Suggestions, s => s.Label == "*");
        Assert.Contains(result.Suggestions, s => s.Label == "CustomerId" && s.Detail == "Customers - INTEGER");
        Assert.Contains(result.Suggestions, s => s.Label == "Name" && s.Detail == "Customers - TEXT");
    }

    [Fact]
    public void GetCompletions_FromContextFiltersSources()
    {
        var result = SqlCompletionProvider.GetCompletions("SELECT * FROM Cu", "SELECT * FROM Cu".Length, CreateCatalog());

        Assert.Contains(result.Suggestions, s => s.Label == "Customers");
        Assert.Contains(result.Suggestions, s => s.Label == "CustomerSummary");
        Assert.DoesNotContain(result.Suggestions, s => s.Label == "Orders");
    }

    [Fact]
    public void GetCompletions_ExecContextSuggestsProcedures()
    {
        var result = SqlCompletionProvider.GetCompletions("EXEC Re", "EXEC Re".Length, CreateCatalog());

        var suggestion = Assert.Single(result.Suggestions);
        Assert.Equal("RefreshCustomerStats", suggestion.Label);
        Assert.Equal(SqlCompletionSuggestionKind.Procedure, suggestion.Kind);
    }

    [Fact]
    public void GetCompletions_QualifiedAliasSuggestsAliasColumns()
    {
        var result = SqlCompletionProvider.GetCompletions("SELECT c. FROM Customers c", "SELECT c.".Length, CreateCatalog());

        Assert.Contains(result.Suggestions, s => s.Label == "CustomerId");
        Assert.Contains(result.Suggestions, s => s.Label == "Name");
    }

    [Fact]
    public void GetCompletions_AfterSetInUpdate_SuggestsColumnsForUpdateSource()
    {
        var result = SqlCompletionProvider.GetCompletions("UPDATE Customers SET ", "UPDATE Customers SET ".Length, CreateCatalog());

        Assert.Contains(result.Suggestions, s => s.Label == "CustomerId");
        Assert.Contains(result.Suggestions, s => s.Label == "Name");
        Assert.Contains(result.Suggestions, s => s.Label == "Email");
    }

    [Fact]
    public void GetCompletions_AfterWhereInUpdate_SuggestsColumnsForUpdateSource()
    {
        string sql = "UPDATE Customers SET Name = 'Alice' WHERE ";

        var result = SqlCompletionProvider.GetCompletions(sql, sql.Length, CreateCatalog());

        Assert.Contains(result.Suggestions, s => s.Label == "CustomerId");
        Assert.Contains(result.Suggestions, s => s.Label == "Name");
        Assert.Contains(result.Suggestions, s => s.Label == "Email");
    }

    [Fact]
    public void GetCompletions_ExactColumnMatch_DoesNotKeepPopupOpen()
    {
        string sql = "SELECT Name FROM Customers";

        var result = SqlCompletionProvider.GetCompletions(sql, "SELECT Name".Length, CreateCatalog());

        Assert.Empty(result.Suggestions);
    }

    [Fact]
    public void GetCompletions_ExactSourceMatch_DoesNotKeepPopupOpen()
    {
        string sql = "SELECT * FROM Customers";

        var result = SqlCompletionProvider.GetCompletions(sql, sql.Length, CreateCatalog());

        Assert.Empty(result.Suggestions);
    }

    [Fact]
    public void GetCompletions_ExplicitTriggerStillShowsSuggestionsForExactMatch()
    {
        string sql = "SELECT Name FROM Customers";

        var result = SqlCompletionProvider.GetCompletions(sql, "SELECT Name".Length, CreateCatalog(), explicitTrigger: true);

        var suggestion = Assert.Single(result.Suggestions);
        Assert.Equal("Name", suggestion.Label);
    }

    [Fact]
    public void GetCompletions_ExplicitTriggerWithEmptyPrefix_IncludesLimit()
    {
        var result = SqlCompletionProvider.GetCompletions("SELECT * FROM Customers ", "SELECT * FROM Customers ".Length, CreateCatalog(), explicitTrigger: true);

        Assert.Contains(result.Suggestions, s => s.Label == "LIMIT");
        Assert.Contains(result.Suggestions, s => s.Label == "OFFSET");
    }

    private static SqlCompletionCatalog CreateCatalog()
        => new()
        {
            Sources =
            [
                new SqlCompletionSource("Customers", SqlCompletionSourceKind.Table),
                new SqlCompletionSource("Orders", SqlCompletionSourceKind.Table),
                new SqlCompletionSource("CustomerSummary", SqlCompletionSourceKind.View),
                new SqlCompletionSource("sys.tables", SqlCompletionSourceKind.SystemCatalog),
            ],
            ColumnsBySource = new Dictionary<string, IReadOnlyList<SqlCompletionColumn>>(StringComparer.OrdinalIgnoreCase)
            {
                ["Customers"] =
                [
                    new SqlCompletionColumn("CustomerId", "INTEGER", "Customers"),
                    new SqlCompletionColumn("Name", "TEXT", "Customers"),
                    new SqlCompletionColumn("Email", "TEXT", "Customers"),
                ],
                ["Orders"] =
                [
                    new SqlCompletionColumn("OrderId", "INTEGER", "Orders"),
                    new SqlCompletionColumn("CustomerId", "INTEGER", "Orders"),
                    new SqlCompletionColumn("Total", "REAL", "Orders"),
                ],
            },
            Procedures = ["RefreshCustomerStats"],
        };
}
