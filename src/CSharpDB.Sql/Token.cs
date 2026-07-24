namespace CSharpDB.Sql;

public readonly struct Token
{
    public TokenType Type { get; init; }
    public string Value { get; init; }
    public int Position { get; init; }
    public int Length { get; init; }

    public Token(TokenType type, string value, int position)
        : this(type, value, position, value.Length)
    {
    }

    public Token(TokenType type, string value, int position, int length)
    {
        Type = type;
        Value = value;
        Position = position;
        Length = length;
    }

    public override string ToString() => $"{Type}({Value}) @{Position}";
}
