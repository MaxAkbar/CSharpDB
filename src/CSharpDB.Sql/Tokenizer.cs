using CSharpDB.Primitives;

namespace CSharpDB.Sql;

public sealed class Tokenizer
{
    private static readonly Dictionary<string, TokenType> Keywords = new(StringComparer.OrdinalIgnoreCase)
    {
        ["CREATE"] = TokenType.Create,
        ["TABLE"] = TokenType.Table,
        ["INSERT"] = TokenType.Insert,
        ["INTO"] = TokenType.Into,
        ["VALUES"] = TokenType.Values,
        ["SELECT"] = TokenType.Select,
        ["FROM"] = TokenType.From,
        ["WHERE"] = TokenType.Where,
        ["AND"] = TokenType.And,
        ["OR"] = TokenType.Or,
        ["NOT"] = TokenType.Not,
        ["LIMIT"] = TokenType.Limit,
        ["OFFSET"] = TokenType.Offset,
        ["ORDER"] = TokenType.Order,
        ["BY"] = TokenType.By,
        ["ASC"] = TokenType.Asc,
        ["DESC"] = TokenType.Desc,
        ["NULL"] = TokenType.Null,
        ["DELETE"] = TokenType.Delete,
        ["UPDATE"] = TokenType.Update,
        ["SET"] = TokenType.Set,
        ["DROP"] = TokenType.Drop,
        ["INTEGER"] = TokenType.Integer,
        ["INT"] = TokenType.Integer,
        ["REAL"] = TokenType.Real,
        ["FLOAT"] = TokenType.Real,
        ["DOUBLE"] = TokenType.Real,
        ["TEXT"] = TokenType.Text,
        ["VARCHAR"] = TokenType.Text,
        ["BLOB"] = TokenType.Blob,
        ["PRIMARY"] = TokenType.Primary,
        ["KEY"] = TokenType.Key,
        ["FOREIGN"] = TokenType.Foreign,
        ["REFERENCES"] = TokenType.References,
        ["IDENTITY"] = TokenType.Identity,
        ["AUTOINCREMENT"] = TokenType.Autoincrement,
        ["CASCADE"] = TokenType.Cascade,
        ["IF"] = TokenType.If,
        ["EXISTS"] = TokenType.Exists,
        ["LIKE"] = TokenType.Like,
        ["IN"] = TokenType.In,
        ["BETWEEN"] = TokenType.Between,
        ["ESCAPE"] = TokenType.Escape,
        ["IS"] = TokenType.Is,
        ["GROUP"] = TokenType.Group,
        ["HAVING"] = TokenType.Having,
        ["AS"] = TokenType.As,
        ["DISTINCT"] = TokenType.Distinct,
        ["COUNT"] = TokenType.Count,
        ["SUM"] = TokenType.Sum,
        ["AVG"] = TokenType.Avg,
        ["MIN"] = TokenType.Min,
        ["MAX"] = TokenType.Max,
        ["JOIN"] = TokenType.Join,
        ["INNER"] = TokenType.Inner,
        ["LEFT"] = TokenType.Left,
        ["RIGHT"] = TokenType.Right,
        ["OUTER"] = TokenType.Outer,
        ["CROSS"] = TokenType.Cross,
        ["ON"] = TokenType.On,
        ["UNION"] = TokenType.Union,
        ["INTERSECT"] = TokenType.Intersect,
        ["EXCEPT"] = TokenType.Except,
        ["ALTER"] = TokenType.Alter,
        ["ADD"] = TokenType.Add,
        ["COLUMN"] = TokenType.Column,
        ["CONSTRAINT"] = TokenType.Constraint,
        ["COLLATE"] = TokenType.Collate,
        ["RENAME"] = TokenType.Rename,
        ["TO"] = TokenType.To,
        ["INDEX"] = TokenType.Index,
        ["UNIQUE"] = TokenType.Unique,
        ["VIEW"] = TokenType.View,
        ["WITH"] = TokenType.With,
        ["RECURSIVE"] = TokenType.Recursive,
        ["ANALYZE"] = TokenType.Analyze,
        ["TRIGGER"] = TokenType.Trigger,
        ["BEFORE"] = TokenType.Before,
        ["AFTER"] = TokenType.After,
        ["FOR"] = TokenType.For,
        ["EACH"] = TokenType.Each,
        ["ROW"] = TokenType.Row,
        ["BEGIN"] = TokenType.Begin,
        ["END"] = TokenType.End,
        ["NEW"] = TokenType.New,
        ["OLD"] = TokenType.Old,
    };

    private readonly string _input;
    private int _pos;

    public Tokenizer(string input)
    {
        _input = input;
        _pos = 0;
    }

    public List<Token> Tokenize()
    {
        var tokens = new List<Token>();

        while (_pos < _input.Length)
        {
            SkipWhitespace();
            if (_pos >= _input.Length) break;

            char c = _input[_pos];

            // Single-line comment
            if (c == '-' && _pos + 1 < _input.Length && _input[_pos + 1] == '-')
            {
                while (_pos < _input.Length && _input[_pos] != '\n') _pos++;
                continue;
            }

            if ((c == 'X' || c == 'x') &&
                _pos + 1 < _input.Length &&
                _input[_pos + 1] == '\'')
            {
                tokens.Add(ReadBlobLiteral());
            }
            else if (char.IsLetter(c) || c == '_')
            {
                tokens.Add(ReadIdentifierOrKeyword());
            }
            else if (c == '@')
            {
                tokens.Add(ReadParameter());
            }
            else if (char.IsDigit(c))
            {
                tokens.Add(ReadNumber());
            }
            else if (c == '\'')
            {
                tokens.Add(ReadString());
            }
            else
            {
                tokens.Add(ReadOperatorOrPunctuation());
            }
        }

        tokens.Add(new Token(TokenType.Eof, "", _pos));
        return tokens;
    }

    private void SkipWhitespace()
    {
        while (_pos < _input.Length && char.IsWhiteSpace(_input[_pos]))
            _pos++;
    }

    private Token ReadIdentifierOrKeyword()
    {
        int start = _pos;
        while (_pos < _input.Length && (char.IsLetterOrDigit(_input[_pos]) || _input[_pos] == '_'))
            _pos++;

        string value = _input[start.._pos];
        var type = Keywords.TryGetValue(value, out var kw) ? kw : TokenType.Identifier;
        return new Token(type, value, start);
    }

    private Token ReadNumber()
    {
        int start = _pos;
        bool hasDot = false;

        while (_pos < _input.Length && (char.IsDigit(_input[_pos]) || _input[_pos] == '.'))
        {
            if (_input[_pos] == '.')
            {
                if (hasDot) break;
                hasDot = true;
            }
            _pos++;
        }

        string value = _input[start.._pos];
        return new Token(hasDot ? TokenType.RealLiteral : TokenType.IntegerLiteral, value, start);
    }

    private Token ReadParameter()
    {
        int start = _pos;
        _pos++; // skip '@'
        if (_pos >= _input.Length || !(char.IsLetter(_input[_pos]) || _input[_pos] == '_'))
            throw new CSharpDbException(ErrorCode.SyntaxError, $"Invalid parameter name at position {start}.");

        int nameStart = _pos;
        while (_pos < _input.Length && (char.IsLetterOrDigit(_input[_pos]) || _input[_pos] == '_'))
            _pos++;

        string value = _input[nameStart.._pos];
        return new Token(TokenType.Parameter, value, start);
    }

    private Token ReadString()
    {
        int start = _pos;
        _pos++; // skip opening quote
        var sb = new System.Text.StringBuilder();

        while (_pos < _input.Length)
        {
            char c = _input[_pos];
            if (c == '\'')
            {
                // Check for escaped quote ''
                if (_pos + 1 < _input.Length && _input[_pos + 1] == '\'')
                {
                    sb.Append('\'');
                    _pos += 2;
                }
                else
                {
                    _pos++; // skip closing quote
                    return new Token(TokenType.StringLiteral, sb.ToString(), start);
                }
            }
            else
            {
                sb.Append(c);
                _pos++;
            }
        }

        throw new CSharpDbException(ErrorCode.SyntaxError, $"Unterminated string literal at position {start}.");
    }

    private Token ReadBlobLiteral()
    {
        int start = _pos;
        _pos++; // skip X/x
        _pos++; // skip opening quote

        int hexStart = _pos;
        while (_pos < _input.Length && _input[_pos] != '\'')
        {
            if (!IsHexDigit(_input[_pos]))
            {
                throw new CSharpDbException(
                    ErrorCode.SyntaxError,
                    $"Invalid blob literal at position {start}. Expected hexadecimal characters.");
            }

            _pos++;
        }

        if (_pos >= _input.Length)
            throw new CSharpDbException(ErrorCode.SyntaxError, $"Unterminated blob literal at position {start}.");

        int hexLength = _pos - hexStart;
        if ((hexLength & 1) != 0)
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                $"Blob literal at position {start} must contain an even number of hexadecimal characters.");
        }

        string hex = _input[hexStart.._pos];
        _pos++; // skip closing quote
        return new Token(TokenType.BlobLiteral, hex, start);
    }

    private Token ReadOperatorOrPunctuation()
    {
        int start = _pos;
        char c = _input[_pos++];

        switch (c)
        {
            case '=':
                return new Token(TokenType.Equals, "=", start);
            case '<':
                if (_pos < _input.Length)
                {
                    if (_input[_pos] == '=') { _pos++; return new Token(TokenType.LessOrEqual, "<=", start); }
                    if (_input[_pos] == '>') { _pos++; return new Token(TokenType.NotEquals, "<>", start); }
                }
                return new Token(TokenType.LessThan, "<", start);
            case '>':
                if (_pos < _input.Length && _input[_pos] == '=') { _pos++; return new Token(TokenType.GreaterOrEqual, ">=", start); }
                return new Token(TokenType.GreaterThan, ">", start);
            case '!':
                if (_pos < _input.Length && _input[_pos] == '=') { _pos++; return new Token(TokenType.NotEquals, "!=", start); }
                throw new CSharpDbException(ErrorCode.SyntaxError, $"Unexpected character '!' at position {start}.");
            case '+': return new Token(TokenType.Plus, "+", start);
            case '-': return new Token(TokenType.Minus, "-", start);
            case '*': return new Token(TokenType.Star, "*", start);
            case '/': return new Token(TokenType.Slash, "/", start);
            case ',': return new Token(TokenType.Comma, ",", start);
            case ':': return new Token(TokenType.Colon, ":", start);
            case '.': return new Token(TokenType.Dot, ".", start);
            case '(': return new Token(TokenType.LeftParen, "(", start);
            case ')': return new Token(TokenType.RightParen, ")", start);
            case ';': return new Token(TokenType.Semicolon, ";", start);
            default:
                throw new CSharpDbException(ErrorCode.SyntaxError, $"Unexpected character '{c}' at position {start}.");
        }
    }

    private static bool IsHexDigit(char c)
        => c is >= '0' and <= '9'
        or >= 'a' and <= 'f'
        or >= 'A' and <= 'F';
}
