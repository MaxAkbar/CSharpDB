using CSharpDB.Admin.Helpers;
using CSharpDB.Sql;

namespace CSharpDB.Admin.Forms.Tests.Components.Shared;

public sealed class SqlKeywordCompletionTests
{
    public static IEnumerable<object[]> ReservedKeywordCases()
        => Tokenizer.ReservedKeywords.Select(static keyword => new object[] { keyword });

    public static IEnumerable<object[]> EditorKeywordCases()
        => SqlKeywordCatalog.CompletionKeywords.Select(static keyword => new object[] { keyword });

    [Theory]
    [MemberData(nameof(EditorKeywordCases))]
    public void GetCompletions_CoversEntireEditorVocabulary(string keyword)
    {
        var result = SqlCompletionProvider.GetCompletions(keyword, keyword.Length, Catalog, explicitTrigger: true);
        Assert.Contains(result.Suggestions, suggestion => suggestion.Label == keyword);
    }

    [Theory]
    [MemberData(nameof(ReservedKeywordCases))]
    public void GetCompletions_CoversTokenizerVocabulary(string keyword)
    {
        string sql = "SELECT * FROM Customers WHERE Email " + keyword.ToLowerInvariant();

        var result = SqlCompletionProvider.GetCompletions(sql, sql.Length, Catalog, explicitTrigger: true);

        // The tokenizer recognizes RECURSIVE solely to reject recursive CTEs.
        if (keyword == "RECURSIVE")
            Assert.DoesNotContain(result.Suggestions, suggestion => suggestion.Label == keyword);
        else
            Assert.Contains(result.Suggestions, suggestion => suggestion.Label == keyword);
    }

    [Theory]
    [InlineData("SELECT dis", "DISTINCT")]
    [InlineData("SELECT * FROM Customers WHERE ex", "EXISTS")]
    [InlineData("SELECT * FROM Customers WHERE no", "NOT")]
    [InlineData("SELECT * FROM Customers ORDER BY Email de", "DESC")]
    [InlineData("SELECT * FROM Customers WHERE Email LIKE 'x%' esc", "ESCAPE")]
    [InlineData("SELECT * FROM Customers un", "UNION")]
    [InlineData("SELECT SUM(Total) ov", "OVER")]
    [InlineData("SELECT SUM(Total) OVER (par", "PARTITION")]
    [InlineData("SELECT SUM(Total) OVER (ORDER BY Email ROWS UNBOUNDED pre", "PRECEDING")]
    [InlineData("CREATE TABLE Customers (amount dec", "DECIMAL")]
    [InlineData("CREATE TABLE Customers (version rowv", "ROWVERSION")]
    [InlineData("ALTER TABLE Customers re", "RENAME")]
    [InlineData("ALTER TABLE Customers rese", "RESEED")]
    [InlineData("exe", "EXECUTE")]
    public void GetCompletions_SuggestsKeywordsInRelevantContexts(string sql, string keyword)
    {
        var result = SqlCompletionProvider.GetCompletions(sql, sql.Length, Catalog);

        Assert.Contains(result.Suggestions, suggestion => suggestion.Label == keyword);
    }

    [Theory]
    [InlineData("CREATE", "CREATE TABLE")]
    [InlineData("ORDER", "ORDER BY")]
    [InlineData("GROUP", "GROUP BY")]
    [InlineData("INSERT", "INSERT INTO")]
    [InlineData("LEFT", "LEFT JOIN")]
    public void GetCompletions_CompleteFirstWord_StillOffersLongerPhrase(string sql, string phrase)
    {
        var result = SqlCompletionProvider.GetCompletions(sql, sql.Length, Catalog);

        Assert.Contains(result.Suggestions, suggestion => suggestion.Label == phrase);
    }

    [Fact]
    public void GetCompletions_SelectListColumnMatch_DoesNotHideDistinct()
    {
        const string sql = "SELECT dis FROM Customers";
        var catalog = CreateCatalog([new SqlCompletionColumn("Discount", "REAL", "Customers")]);

        var result = SqlCompletionProvider.GetCompletions(sql, "SELECT dis".Length, catalog);

        Assert.Contains(result.Suggestions, suggestion => suggestion.Label == "Discount");
        Assert.Contains(result.Suggestions, suggestion => suggestion.Label == "DISTINCT");
    }

    [Theory]
    [InlineData("exists", "EXISTS")]
    [InlineData("distinc", "DISTINCT")]
    public void GetCompletions_ManyMatchingColumns_LeavesRoomForKeywords(string prefix, string keyword)
    {
        string sql = "SELECT * FROM Customers WHERE " + prefix;
        var columns = Enumerable.Range(1, 20)
            .Select(index => new SqlCompletionColumn(prefix + index, "TEXT", "Customers"))
            .ToArray();

        var result = SqlCompletionProvider.GetCompletions(sql, sql.Length, CreateCatalog(columns), explicitTrigger: true);

        Assert.Contains(result.Suggestions, suggestion => suggestion.Label == keyword);
        Assert.Contains(result.Suggestions, suggestion => suggestion.Kind == SqlCompletionSuggestionKind.Column);
        Assert.True(result.Suggestions.Count <= 12);
    }

    [Theory]
    [InlineData("DATE", "da")]
    [InlineData("TIME", "ti")]
    [InlineData("DATETIME", "dateti")]
    public void GetCompletions_TemporalTypes_OfferBareTypeAlongsideFunction(string type, string prefix)
    {
        string sql = "CREATE TABLE Customers (created " + prefix;

        var result = SqlCompletionProvider.GetCompletions(sql, sql.Length, Catalog);

        Assert.Contains(result.Suggestions, suggestion =>
            suggestion.Label == type && suggestion.Kind == SqlCompletionSuggestionKind.Keyword && suggestion.InsertText == type + " ");
        Assert.Contains(result.Suggestions, suggestion =>
            suggestion.Label == type && suggestion.Kind == SqlCompletionSuggestionKind.Function && suggestion.InsertText == type + "()");
    }

    [Fact]
    public void GetCompletions_Cast_PreservesFunctionInsertion()
    {
        const string sql = "SELECT ca";

        var result = SqlCompletionProvider.GetCompletions(sql, sql.Length, Catalog);

        var suggestion = Assert.Single(result.Suggestions, suggestion => suggestion.Label == "CAST");
        Assert.Equal(SqlCompletionSuggestionKind.Function, suggestion.Kind);
        Assert.Equal("CAST()", suggestion.InsertText);
        Assert.Equal("SELECT CAST(".Length, suggestion.CaretPosition);
    }

    [Theory]
    [InlineData("CASE")]
    [InlineData("TRY_CAST")]
    [InlineData("UPSERT")]
    [InlineData("RETURNING")]
    [InlineData("RANGE")]
    [InlineData("COMMIT")]
    public void GetCompletions_DoesNotAdvertiseUnsupportedConstructs(string word)
    {
        string sql = "SELECT * FROM Customers WHERE Email " + word;

        var result = SqlCompletionProvider.GetCompletions(sql, sql.Length, Catalog, explicitTrigger: true);

        Assert.DoesNotContain(result.Suggestions, suggestion => suggestion.Label == word);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void GetCompletions_InsideLikePattern_DoesNotOfferKeywords(bool explicitTrigger)
    {
        const string sql = "SELECT * FROM Customers WHERE Email LIKE 'esc";

        var result = SqlCompletionProvider.GetCompletions(sql, sql.Length, Catalog, explicitTrigger);

        Assert.Empty(result.Suggestions);
    }

    private static SqlCompletionCatalog CreateCatalog(IReadOnlyList<SqlCompletionColumn> columns) => new()
    {
        Sources = [new SqlCompletionSource("Customers", SqlCompletionSourceKind.Table)],
        ColumnsBySource = new Dictionary<string, IReadOnlyList<SqlCompletionColumn>>(StringComparer.OrdinalIgnoreCase)
        {
            ["Customers"] = columns,
        },
    };

    private static readonly SqlCompletionCatalog Catalog = CreateCatalog(
    [
        new SqlCompletionColumn("Email", "TEXT", "Customers"),
        new SqlCompletionColumn("Note", "TEXT", "Customers"),
        new SqlCompletionColumn("Total", "REAL", "Customers"),
    ]);
}
