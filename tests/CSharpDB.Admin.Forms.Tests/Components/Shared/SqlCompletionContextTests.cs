using CSharpDB.Admin.Helpers;

namespace CSharpDB.Admin.Forms.Tests.Components.Shared;

public sealed class SqlCompletionContextTests
{
    [Theory]
    [InlineData("SELECT * FROM Customers WHERE |")]
    [InlineData("select * from Customers\nwhere |")]
    [InlineData("SELECT * FROM \"Customers\" WHERE |")]
    [InlineData("SELECT * FROM Customers WHERE (|")]
    [InlineData("SELECT * FROM Customers WHERE NOT |")]
    [InlineData("SELECT * FROM Customers WHERE Name = 'A' AND |")]
    [InlineData("SELECT * FROM Customers WHERE Name = 'A' OR |")]
    [InlineData("SELECT * FROM Customers WHERE CustomerId = |")]
    [InlineData("SELECT * FROM Customers GROUP BY Name, |")]
    [InlineData("SELECT * FROM Customers GROUP BY Name HAVING |")]
    [InlineData("SELECT * FROM Customers ORDER BY Name, |")]
    [InlineData("UPDATE Customers SET Name = 'A', |")]
    [InlineData("UPDATE \"Customers\" SET |")]
    [InlineData("DELETE FROM Customers WHERE |")]
    [InlineData("SELECT DISTINCT | FROM Customers")]
    [InlineData("SELECT COUNT(*) OVER (PARTITION BY |) FROM Customers")]
    [InlineData("SELECT * FROM Customers WHERE /* FROM Orders */ |")]
    [InlineData("SELECT * FROM Customers WHERE Name = '; FROM Orders' AND |")]
    [InlineData("-- SELECT * FROM Orders;\nSELECT * FROM Customers WHERE |")]
    [InlineData("SELECT * FROM Orders; SELECT * FROM Customers WHERE |")]
    [InlineData("SELECT * FROM Orders WHERE EXISTS (SELECT * FROM Customers WHERE |)")]
    [InlineData("SELECT * FROM Orders UNION SELECT * FROM Customers WHERE |")]
    [InlineData("FIND DUPLICATES IN Customers ON |")]
    [InlineData("DEDUP Customers ON |")]
    [InlineData("MERGE DUPLICATES Customers ON |")]
    [InlineData("CREATE INDEX ix ON Customers (|")]
    public void ColumnContexts_OfferSourceColumns(string markedSql)
    {
        var result = Complete(markedSql);
        Assert.Contains(result.Suggestions, s => s.Label == "Email" && s.Kind == SqlCompletionSuggestionKind.Column);
        Assert.DoesNotContain(result.Suggestions, s => s.Label == "Total");
    }

    [Theory]
    [InlineData("SELECT * FROM Customers WHERE |")]
    [InlineData("UPDATE Customers SET |")]
    [InlineData("SELECT * FROM Customers ORDER BY |")]
    [InlineData("SELECT * FROM Customers GROUP BY |")]
    public void NonProjectionContexts_DoNotOfferWildcard(string sql)
        => Assert.DoesNotContain(Complete(sql).Suggestions, s => s.Label == "*");

    [Theory]
    [InlineData("SELECT * FROM |")]
    [InlineData("SELECT * FROM Customers JOIN |")]
    [InlineData("SELECT * FROM Customers LEFT OUTER JOIN |")]
    [InlineData("INSERT INTO |")]
    [InlineData("UPDATE |")]
    [InlineData("DELETE FROM |")]
    [InlineData("ALTER TABLE |")]
    [InlineData("DROP TABLE |")]
    [InlineData("VALIDATE TABLE |")]
    [InlineData("ANALYZE |")]
    [InlineData("FIND DUPLICATES IN |")]
    [InlineData("FIND ORPHANS IN |")]
    [InlineData("DEDUP |")]
    [InlineData("MERGE DUPLICATES |")]
    [InlineData("CREATE INDEX ix ON |")]
    [InlineData("CREATE TABLE Child (ParentId INTEGER REFERENCES |")]
    public void SourceContexts_OfferSources(string sql)
        => Assert.Contains(Complete(sql).Suggestions, s => s.Label == "Customers" && s.Kind == SqlCompletionSuggestionKind.Source);

    [Theory]
    [InlineData("SELECT * FROM Customers ORDER |", "BY")]
    [InlineData("SELECT * FROM Customers GROUP |", "BY")]
    [InlineData("SELECT COUNT(*) OVER (PARTITION |) FROM Customers", "BY")]
    [InlineData("SELECT * FROM Customers INNER |", "JOIN")]
    [InlineData("SELECT * FROM Customers CROSS |", "JOIN")]
    [InlineData("SELECT * FROM Customers LEFT |", "JOIN")]
    [InlineData("SELECT * FROM Customers RIGHT OUTER |", "JOIN")]
    [InlineData("SELECT * FROM Customers WHERE Email IS |", "NULL")]
    [InlineData("SELECT * FROM Customers WHERE Email IS NOT |", "NULL")]
    [InlineData("DEDUP Customers ON Email KEEP |", "FIRST")]
    [InlineData("INSERT |", "INTO")]
    [InlineData("DELETE |", "FROM")]
    [InlineData("FIND |", "ORPHANS")]
    [InlineData("FIND DUPLICATES |", "IN")]
    [InlineData("FIND ORPHANS |", "IN")]
    [InlineData("MERGE |", "DUPLICATES")]
    [InlineData("VALIDATE |", "TABLE")]
    public void KeywordTransitions_OfferTheNextClauseWord(string sql, string expected)
        => Assert.Contains(Complete(sql).Suggestions, s => s.Label == expected && s.Kind == SqlCompletionSuggestionKind.Keyword);

    [Theory]
    [InlineData("SELECT * FROM Customers c JOIN Orders o ON |", "Total")]
    [InlineData("SELECT * FROM Customers c JOIN Orders o ON c.CustomerId = o.CustomerId WHERE |", "Total")]
    [InlineData("SELECT * FROM Customers c JOIN Orders o ON c.CustomerId = |", "o.CustomerId")]
    [InlineData("SELECT * FROM \"Customers\" AS \"c\" WHERE c.|", "Email")]
    [InlineData("UPDATE Customers AS c SET Name = 'A' WHERE c.|", "Email")]
    public void JoinedAndAliasedSources_ResolveColumns(string sql, string expected)
        => Assert.Contains(Complete(sql).Suggestions, s => s.Label == expected && s.Kind == SqlCompletionSuggestionKind.Column);

    [Theory]
    [InlineData("SELECT * FROM Customers WHERE Name = 'AND |")]
    [InlineData("SELECT * FROM Customers -- WHERE |")]
    [InlineData("SELECT * FROM Customers /* WHERE | */")]
    [InlineData("SELECT * FROM Customers WHERE Name = @|")]
    public void LiteralsCommentsAndParameters_DoNotOfferCompletions(string sql)
        => Assert.Empty(Complete(sql, explicitTrigger: true).Suggestions);

    [Theory]
    [InlineData("SELECT * FROM Customers WHERE Em|", "Email")]
    [InlineData("SELECT * FROM \"Customers\" WHERE \"Em|", "\"Email\"")]
    public void ColumnInsertion_PreservesSqlAndCaret(string markedSql, string insertion)
    {
        int caret = markedSql.IndexOf('|');
        string sql = markedSql.Replace("|", "");
        var suggestion = Assert.Single(Complete(markedSql).Suggestions, s => s.Label == "Email");
        Assert.Equal(insertion, suggestion.InsertText);
        Assert.Equal(caret, suggestion.ReplacementEnd);
        Assert.Equal(suggestion.ReplacementStart + insertion.Length, suggestion.CaretPosition);
        Assert.EndsWith("WHERE " + insertion, sql[..suggestion.ReplacementStart] + suggestion.InsertText + sql[suggestion.ReplacementEnd..]);
    }

    [Fact]
    public void EmptyEditor_DoesNotThrow()
        => Assert.Contains(Complete("|", explicitTrigger: true).Suggestions, s => s.Label == "SELECT");

    [Fact]
    public void QuotedAmbiguousColumn_InsertsTheQualifierSeparately()
    {
        var result = Complete("SELECT * FROM Customers c JOIN Orders o ON c.CustomerId = o.CustomerId WHERE \"Cu|");
        Assert.Equal("c.\"CustomerId\"", Assert.Single(result.Suggestions, s => s.Label == "c.CustomerId").InsertText);
        Assert.Equal("o.\"CustomerId\"", Assert.Single(result.Suggestions, s => s.Label == "o.CustomerId").InsertText);
    }

    private static SqlCompletionResult Complete(string markedSql, bool explicitTrigger = false)
        => SqlCompletionProvider.GetCompletions(markedSql.Replace("|", ""), markedSql.IndexOf('|'), Catalog, explicitTrigger);

    private static readonly SqlCompletionCatalog Catalog = new()
    {
        Sources = [new("Customers", SqlCompletionSourceKind.Table), new("Orders", SqlCompletionSourceKind.Table)],
        ColumnsBySource = new Dictionary<string, IReadOnlyList<SqlCompletionColumn>>(StringComparer.OrdinalIgnoreCase)
        {
            ["Customers"] = [new("CustomerId", "INTEGER", "Customers"), new("Name", "TEXT", "Customers"), new("Email", "TEXT", "Customers")],
            ["Orders"] = [new("OrderId", "INTEGER", "Orders"), new("CustomerId", "INTEGER", "Orders"), new("Total", "REAL", "Orders")],
        },
    };
}
