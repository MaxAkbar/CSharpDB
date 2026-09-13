using CSharpDB.Admin.Helpers;
using CSharpDB.Sql;

namespace CSharpDB.Admin.Forms.Tests.Helpers;

public sealed class SqlHighlighterTests
{
    [Fact]
    public void Highlight_EveryTokenizerReservedKeyword_HasKeywordOrFunctionStyle()
    {
        foreach (string keyword in Tokenizer.ReservedKeywords)
        {
            string text = keyword.ToLowerInvariant();
            string highlighted = SqlHighlighter.Highlight(text);

            Assert.True(
                highlighted == $"<span class=\"hl-keyword\">{text}</span>" ||
                highlighted == $"<span class=\"hl-function\">{text}</span>",
                $"Reserved word '{keyword}' was not highlighted as a keyword or function.");
        }
    }

    [Theory]
    [InlineData("with")]
    [InlineData("ReCuRsIvE")]
    [InlineData("escape")]
    [InlineData("intersect")]
    [InlineData("except")]
    [InlineData("over")]
    [InlineData("partition")]
    [InlineData("rows")]
    [InlineData("unbounded")]
    [InlineData("preceding")]
    [InlineData("following")]
    [InlineData("current")]
    [InlineData("window")]
    [InlineData("bigint")]
    [InlineData("decimal")]
    [InlineData("nvarchar")]
    [InlineData("datetime2")]
    [InlineData("rowversion")]
    public void Highlight_ReservedAndContextualKeywords_UsesKeywordStyleWithoutChangingCase(string keyword)
    {
        Assert.Equal($"<span class=\"hl-keyword\">{keyword}</span>", SqlHighlighter.Highlight(keyword));
    }

    [Theory]
    [InlineData("count")]
    [InlineData("SuM")]
    [InlineData("avg")]
    [InlineData("min")]
    [InlineData("max")]
    [InlineData("coalesce")]
    public void Highlight_AggregateAndScalarFunctions_KeepFunctionStyle(string function)
    {
        Assert.Equal($"<span class=\"hl-function\">{function}</span>(amount)", SqlHighlighter.Highlight(function + "(amount)"));
    }

    [Fact]
    public void Highlight_QuotedIdentifier_KeepsKeywordsAndFunctionsOpaqueAndEscapesHtml()
    {
        const string sql = "\"with \"\"recursive\"\" count < & >\"";

        string highlighted = SqlHighlighter.Highlight(sql);

        Assert.Equal("&quot;with &quot;&quot;recursive&quot;&quot; count &lt; &amp; &gt;&quot;", highlighted);
    }

    [Theory]
    [InlineData("'with ''recursive'' < & > escape'", "hl-string", "'with ''recursive'' &lt; &amp; &gt; escape'")]
    [InlineData("-- with recursive < & > escape", "hl-comment", "-- with recursive &lt; &amp; &gt; escape")]
    [InlineData("/* with\nrecursive < & > escape */", "hl-comment", "/* with\nrecursive &lt; &amp; &gt; escape */")]
    public void Highlight_StringsAndComments_RemainSingleProtectedSpans(string sql, string style, string escaped)
    {
        Assert.Equal($"<span class=\"{style}\">{escaped}</span>", SqlHighlighter.Highlight(sql));
    }

    [Theory]
    [InlineData("\"with recursive  ", "&quot;with recursive  ")]
    [InlineData("'with recursive  ", "<span class=\"hl-string\">'with recursive  </span>")]
    [InlineData("/* with recursive  ", "<span class=\"hl-comment\">/* with recursive  </span>")]
    public void Highlight_IncompleteOpaqueTokens_PreservesRemainingEditorText(string sql, string expected)
    {
        Assert.Equal(expected, SqlHighlighter.Highlight(sql));
    }
}
