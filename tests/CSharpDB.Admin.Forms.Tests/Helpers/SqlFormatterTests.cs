using CSharpDB.Admin.Helpers;
using CSharpDB.Sql;

namespace CSharpDB.Admin.Forms.Tests.Helpers;

public sealed class SqlFormatterTests
{
    [Fact]
    public void Format_EveryTokenizerReservedKeyword_IsUppercased()
    {
        foreach (string keyword in Tokenizer.ReservedKeywords)
            Assert.Equal(keyword.ToUpperInvariant(), SqlFormatter.Format(keyword.ToLowerInvariant()));
    }

    [Theory]
    [InlineData("with recursive escape intersect except", "WITH RECURSIVE ESCAPE INTERSECT EXCEPT")]
    [InlineData("over partition rows unbounded preceding following current window", "OVER PARTITION ROWS UNBOUNDED PRECEDING FOLLOWING CURRENT WINDOW")]
    [InlineData("bigint decimal numeric nvarchar datetime2 rowversion", "BIGINT DECIMAL NUMERIC NVARCHAR DATETIME2 ROWVERSION")]
    public void Format_UppercasesSharedKeywordVocabulary(string sql, string expected)
    {
        Assert.Equal(expected, SqlFormatter.Format(sql));
    }

    [Fact]
    public void Format_ExistingClauseLayout_RemainsUnchanged()
    {
        const string sql = "select id, name from items where id = 1 and name like 'a%' order by name limit 10";
        const string expected = "SELECT id, name\nFROM items\nWHERE id = 1\nAND name LIKE 'a%'\nORDER BY name\nLIMIT 10";

        Assert.Equal(expected, SqlFormatter.Format(sql));
    }

    [Theory]
    [InlineData("'with recursive intersect except escape'")]
    [InlineData("'it''s a -- where /* with */ value'")]
    [InlineData("\"with\"")]
    [InlineData("\"with \"\"recursive\"\" escape\"")]
    [InlineData("/* with recursive\n  escape intersect except */")]
    public void Format_ProtectedTokens_PreservesTheirExactText(string token)
    {
        Assert.Equal($"SELECT {token}\nFROM items", SqlFormatter.Format($"select {token} from items"));
    }

    [Theory]
    [InlineData("\n")]
    [InlineData("\r\n")]
    public void Format_LineCommentTerminator_KeepsFollowingSqlOutsideComment(string newline)
    {
        string sql = "select -- with recursive escape" + newline + "1 from items";

        string formatted = SqlFormatter.Format(sql);

        Assert.Equal("SELECT -- with recursive escape" + newline + "1\nFROM items", formatted);
        var tokens = new Tokenizer(formatted).Tokenize();
        Assert.Contains(tokens, token => token.Value == "1");
        Assert.Contains(tokens, token => token.Type == TokenType.From);
    }

    [Fact]
    public void Format_ClauseFollowingLineComment_DoesNotAddBlankLine()
    {
        Assert.Equal("SELECT 1 -- with recursive\nFROM items", SqlFormatter.Format("select 1 -- with recursive\nfrom items"));
    }

    [Theory]
    [InlineData("'with recursive  ")]
    [InlineData("\"with recursive  ")]
    [InlineData("/* with recursive  ")]
    [InlineData("-- with recursive  ")]
    public void Format_IncompleteOpaqueToken_DoesNotTrimItsContent(string token)
    {
        Assert.Equal("SELECT " + token, SqlFormatter.Format("select " + token));
    }
}
