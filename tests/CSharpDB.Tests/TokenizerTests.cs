using CSharpDB.Sql;

namespace CSharpDB.Tests;

public class TokenizerTests
{
    [Fact]
    public void Tokenize_CreateTable()
    {
        var tokens = new Tokenizer("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").Tokenize();
        Assert.Equal(TokenType.Create, tokens[0].Type);
        Assert.Equal(TokenType.Table, tokens[1].Type);
        Assert.Equal(TokenType.Identifier, tokens[2].Type);
        Assert.Equal("users", tokens[2].Value);
        Assert.Equal(TokenType.LeftParen, tokens[3].Type);
    }

    [Fact]
    public void Tokenize_StringLiteral_WithEscapes()
    {
        var tokens = new Tokenizer("'it''s'").Tokenize();
        Assert.Equal(TokenType.StringLiteral, tokens[0].Type);
        Assert.Equal("it's", tokens[0].Value);
    }

    [Fact]
    public void Tokenize_Operators()
    {
        var tokens = new Tokenizer("a <= b <> c >= d != e").Tokenize();
        Assert.Equal(TokenType.Identifier, tokens[0].Type);
        Assert.Equal(TokenType.LessOrEqual, tokens[1].Type);
        Assert.Equal(TokenType.NotEquals, tokens[3].Type);
        Assert.Equal(TokenType.GreaterOrEqual, tokens[5].Type);
        Assert.Equal(TokenType.NotEquals, tokens[7].Type);
    }

    [Fact]
    public void Tokenize_Numbers()
    {
        var tokens = new Tokenizer("42 3.14").Tokenize();
        Assert.Equal(TokenType.IntegerLiteral, tokens[0].Type);
        Assert.Equal("42", tokens[0].Value);
        Assert.Equal(TokenType.RealLiteral, tokens[1].Type);
        Assert.Equal("3.14", tokens[1].Value);
    }

    [Fact]
    public void Tokenize_Parameter()
    {
        var tokens = new Tokenizer("SELECT * FROM t WHERE id = @id").Tokenize();
        Assert.Contains(tokens, t => t.Type == TokenType.Parameter && t.Value == "id");
    }

    [Fact]
    public void Tokenize_Comment()
    {
        var tokens = new Tokenizer("SELECT -- this is a comment\n*").Tokenize();
        Assert.Equal(TokenType.Select, tokens[0].Type);
        Assert.Equal(TokenType.Star, tokens[1].Type);
        Assert.Equal(TokenType.Eof, tokens[2].Type);
    }

    [Theory]
    [InlineData("LIKE", TokenType.Like)]
    [InlineData("IN", TokenType.In)]
    [InlineData("BETWEEN", TokenType.Between)]
    [InlineData("ESCAPE", TokenType.Escape)]
    [InlineData("IS", TokenType.Is)]
    [InlineData("like", TokenType.Like)]
    [InlineData("in", TokenType.In)]
    [InlineData("GROUP", TokenType.Group)]
    [InlineData("HAVING", TokenType.Having)]
    [InlineData("AS", TokenType.As)]
    [InlineData("DISTINCT", TokenType.Distinct)]
    [InlineData("COUNT", TokenType.Count)]
    [InlineData("SUM", TokenType.Sum)]
    [InlineData("AVG", TokenType.Avg)]
    [InlineData("MIN", TokenType.Min)]
    [InlineData("MAX", TokenType.Max)]
    [InlineData("JOIN", TokenType.Join)]
    [InlineData("INNER", TokenType.Inner)]
    [InlineData("LEFT", TokenType.Left)]
    [InlineData("RIGHT", TokenType.Right)]
    [InlineData("OUTER", TokenType.Outer)]
    [InlineData("CROSS", TokenType.Cross)]
    [InlineData("ON", TokenType.On)]
    [InlineData("join", TokenType.Join)]
    [InlineData("left", TokenType.Left)]
    [InlineData("ALTER", TokenType.Alter)]
    [InlineData("ADD", TokenType.Add)]
    [InlineData("COLUMN", TokenType.Column)]
    [InlineData("RENAME", TokenType.Rename)]
    [InlineData("TO", TokenType.To)]
    [InlineData("alter", TokenType.Alter)]
    [InlineData("add", TokenType.Add)]
    [InlineData("INDEX", TokenType.Index)]
    [InlineData("UNIQUE", TokenType.Unique)]
    [InlineData("VIEW", TokenType.View)]
    [InlineData("index", TokenType.Index)]
    [InlineData("unique", TokenType.Unique)]
    [InlineData("view", TokenType.View)]
    public void Tokenize_NewKeywords(string keyword, TokenType expected)
    {
        var tokens = new Tokenizer(keyword).Tokenize();
        Assert.Equal(expected, tokens[0].Type);
    }

    [Fact]
    public void Tokenize_DotToken()
    {
        var tokens = new Tokenizer("t.col").Tokenize();
        Assert.Equal(TokenType.Identifier, tokens[0].Type);
        Assert.Equal("t", tokens[0].Value);
        Assert.Equal(TokenType.Dot, tokens[1].Type);
        Assert.Equal(TokenType.Identifier, tokens[2].Type);
        Assert.Equal("col", tokens[2].Value);
    }

    [Theory]
    [InlineData("WITH", TokenType.With)]
    [InlineData("RECURSIVE", TokenType.Recursive)]
    [InlineData("with", TokenType.With)]
    [InlineData("recursive", TokenType.Recursive)]
    public void Tokenize_CteKeywords(string keyword, TokenType expected)
    {
        var tokens = new Tokenizer(keyword).Tokenize();
        Assert.Equal(expected, tokens[0].Type);
    }

    [Theory]
    [InlineData("TRIGGER", TokenType.Trigger)]
    [InlineData("BEFORE", TokenType.Before)]
    [InlineData("AFTER", TokenType.After)]
    [InlineData("FOR", TokenType.For)]
    [InlineData("EACH", TokenType.Each)]
    [InlineData("ROW", TokenType.Row)]
    [InlineData("BEGIN", TokenType.Begin)]
    [InlineData("END", TokenType.End)]
    [InlineData("NEW", TokenType.New)]
    [InlineData("OLD", TokenType.Old)]
    public void Tokenize_TriggerKeywords(string keyword, TokenType expected)
    {
        var tokens = new Tokenizer(keyword).Tokenize();
        Assert.Equal(expected, tokens[0].Type);
    }
}
