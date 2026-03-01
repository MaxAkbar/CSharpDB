namespace CSharpDB.Sql;

public readonly struct Token
{
    public TokenType Type { get; init; }
    public string Value { get; init; }
    public int Position { get; init; }

    public Token(TokenType type, string value, int position)
    {
        Type = type;
        Value = value;
        Position = position;
    }

    public override string ToString() => $"{Type}({Value}) @{Position}";
}
