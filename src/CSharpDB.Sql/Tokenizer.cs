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
        ["EXTERNAL"] = TokenType.External,
        ["TEMP"] = TokenType.Temp,
        ["TEMPORARY"] = TokenType.Temporary,
        ["PERSIST"] = TokenType.Persist,
        ["WITH"] = TokenType.With,
        ["RECURSIVE"] = TokenType.Recursive,
        ["ANALYZE"] = TokenType.Analyze,
        ["EXPLAIN"] = TokenType.Explain,
        ["ESTIMATE"] = TokenType.Estimate,
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
        ["FIND"] = TokenType.Find,
        ["DUPLICATES"] = TokenType.Duplicates,
        ["DEDUP"] = TokenType.Dedup,
        ["KEEP"] = TokenType.Keep,
        ["FIRST"] = TokenType.First,
        ["LAST"] = TokenType.Last,
        ["MERGE"] = TokenType.Merge,
        ["VALIDATION"] = TokenType.Validation,
        ["RULE"] = TokenType.Rule,
        ["MESSAGE"] = TokenType.Message,
        ["VALIDATE"] = TokenType.Validate,
        ["ORPHANS"] = TokenType.Orphans,
    };

    private readonly string _input;
    private int _pos;

    internal static bool TryGetKeyword(
        ReadOnlySpan<char> value,
        out TokenType tokenType)
        => Keywords
            .GetAlternateLookup<ReadOnlySpan<char>>()
            .TryGetValue(value, out tokenType);

    public Tokenizer(string input)
    {
        _input = input;
        _pos = 0;
    }

    public List<Token> Tokenize()
    {
        try
        {
            return Tokenize(int.MaxValue, CancellationToken.None);
        }
        catch (SqlTokenizerException ex)
        {
            // Preserve the exact exception type exposed by the original public API.
            throw new CSharpDbException(ex.Code, ex.Message);
        }
    }

    internal List<Token> Tokenize(int maxTokenCount, CancellationToken cancellationToken)
    {
        if (maxTokenCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxTokenCount));

        var tokens = new List<Token>();

        while (_pos < _input.Length)
        {
            cancellationToken.ThrowIfCancellationRequested();
            SkipWhitespace();
            if (_pos >= _input.Length) break;

            char c = _input[_pos];
            Token token;

            // Single-line comment
            if (c == '-' && _pos + 1 < _input.Length && _input[_pos + 1] == '-')
            {
                while (_pos < _input.Length && _input[_pos] != '\n') _pos++;
                continue;
            }

            // Block comment
            if (c == '/' && _pos + 1 < _input.Length && _input[_pos + 1] == '*')
            {
                int start = _pos;
                _pos += 2;
                while (_pos + 1 < _input.Length &&
                       !(_input[_pos] == '*' && _input[_pos + 1] == '/'))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    _pos++;
                }

                if (_pos + 1 >= _input.Length)
                {
                    throw new SqlTokenizerException(
                        start,
                        _input.Length - start,
                        "token.block-comment",
                        $"Unterminated block comment at position {start}.");
                }

                _pos += 2;
                continue;
            }

            if ((c == 'X' || c == 'x') &&
                _pos + 1 < _input.Length &&
                _input[_pos + 1] == '\'')
            {
                token = ReadBlobLiteral();
            }
            else if (char.IsLetter(c) || c == '_')
            {
                token = ReadIdentifierOrKeyword();
            }
            else if (c == '"')
            {
                token = ReadQuotedIdentifier();
            }
            else if (c == '@')
            {
                token = ReadParameter();
            }
            else if (char.IsDigit(c))
            {
                token = ReadNumber();
            }
            else if (c == '\'')
            {
                token = ReadString();
            }
            else
            {
                token = ReadOperatorOrPunctuation();
            }

            if (tokens.Count == maxTokenCount)
                throw new SqlTokenLimitExceededException(token.Position, token.Length);

            tokens.Add(token);
        }

        tokens.Add(new Token(TokenType.Eof, "", _pos, 0));
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
        try
        {
            SqlIdentifierRules.Validate(value);
        }
        catch (CSharpDbException ex)
        {
            throw new SqlTokenizerException(
                start,
                _pos - start,
                "token.identifier",
                ex.Message,
                ex);
        }

        var type = Keywords.TryGetValue(value, out var kw) ? kw : TokenType.Identifier;
        return new Token(type, value, start, _pos - start);
    }

    private Token ReadQuotedIdentifier()
    {
        int start = _pos;
        _pos++; // skip opening double quote
        var builder = new System.Text.StringBuilder();

        while (_pos < _input.Length)
        {
            char c = _input[_pos++];
            if (c != '"')
            {
                builder.Append(c);
                continue;
            }

            if (_pos < _input.Length && _input[_pos] == '"')
            {
                builder.Append('"');
                _pos++;
                continue;
            }

            string value = builder.ToString();
            try
            {
                SqlIdentifierRules.Validate(value, "Quoted identifier");
            }
            catch (CSharpDbException ex)
            {
                throw new SqlTokenizerException(
                    start,
                    _pos - start,
                    "token.quoted-identifier",
                    ex.Message,
                    ex);
            }

            return new Token(TokenType.Identifier, value, start, _pos - start);
        }

        throw new SqlTokenizerException(
            start,
            _input.Length - start,
            "token.quoted-identifier",
            $"Unterminated quoted identifier at position {start}.");
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
        return new Token(hasDot ? TokenType.RealLiteral : TokenType.IntegerLiteral, value, start, _pos - start);
    }

    private Token ReadParameter()
    {
        int start = _pos;
        _pos++; // skip '@'
        if (_pos >= _input.Length || !(char.IsLetter(_input[_pos]) || _input[_pos] == '_'))
        {
            throw new SqlTokenizerException(
                start,
                1,
                "token.parameter",
                $"Invalid parameter name at position {start}.");
        }

        int nameStart = _pos;
        while (_pos < _input.Length && (char.IsLetterOrDigit(_input[_pos]) || _input[_pos] == '_'))
            _pos++;

        string value = _input[nameStart.._pos];
        return new Token(TokenType.Parameter, value, start, _pos - start);
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
                    return new Token(TokenType.StringLiteral, sb.ToString(), start, _pos - start);
                }
            }
            else
            {
                sb.Append(c);
                _pos++;
            }
        }

        throw new SqlTokenizerException(
            start,
            _input.Length - start,
            "token.string-literal",
            $"Unterminated string literal at position {start}.");
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
                throw new SqlTokenizerException(
                    start,
                    Math.Max(1, _pos - start + 1),
                    "token.blob-literal",
                    $"Invalid blob literal at position {start}. Expected hexadecimal characters.");
            }

            _pos++;
        }

        if (_pos >= _input.Length)
        {
            throw new SqlTokenizerException(
                start,
                _input.Length - start,
                "token.blob-literal",
                $"Unterminated blob literal at position {start}.");
        }

        int hexLength = _pos - hexStart;
        if ((hexLength & 1) != 0)
        {
            throw new SqlTokenizerException(
                start,
                _pos - start + 1,
                "token.blob-literal",
                $"Blob literal at position {start} must contain an even number of hexadecimal characters.");
        }

        string hex = _input[hexStart.._pos];
        _pos++; // skip closing quote
        return new Token(TokenType.BlobLiteral, hex, start, _pos - start);
    }

    private Token ReadOperatorOrPunctuation()
    {
        int start = _pos;
        char c = _input[_pos++];

        switch (c)
        {
            case '=':
                return new Token(TokenType.Equals, "=", start, _pos - start);
            case '<':
                if (_pos < _input.Length)
                {
                    if (_input[_pos] == '=') { _pos++; return new Token(TokenType.LessOrEqual, "<=", start, _pos - start); }
                    if (_input[_pos] == '>') { _pos++; return new Token(TokenType.NotEquals, "<>", start, _pos - start); }
                }
                return new Token(TokenType.LessThan, "<", start, _pos - start);
            case '>':
                if (_pos < _input.Length && _input[_pos] == '=') { _pos++; return new Token(TokenType.GreaterOrEqual, ">=", start, _pos - start); }
                return new Token(TokenType.GreaterThan, ">", start, _pos - start);
            case '!':
                if (_pos < _input.Length && _input[_pos] == '=') { _pos++; return new Token(TokenType.NotEquals, "!=", start, _pos - start); }
                throw new SqlTokenizerException(
                    start,
                    1,
                    "token.character",
                    $"Unexpected character '!' at position {start}.");
            case '+': return new Token(TokenType.Plus, "+", start, _pos - start);
            case '-': return new Token(TokenType.Minus, "-", start, _pos - start);
            case '*': return new Token(TokenType.Star, "*", start, _pos - start);
            case '/': return new Token(TokenType.Slash, "/", start, _pos - start);
            case ',': return new Token(TokenType.Comma, ",", start, _pos - start);
            case ':': return new Token(TokenType.Colon, ":", start, _pos - start);
            case '.': return new Token(TokenType.Dot, ".", start, _pos - start);
            case '(': return new Token(TokenType.LeftParen, "(", start, _pos - start);
            case ')': return new Token(TokenType.RightParen, ")", start, _pos - start);
            case ';': return new Token(TokenType.Semicolon, ";", start, _pos - start);
            default:
                throw new SqlTokenizerException(
                    start,
                    1,
                    "token.character",
                    $"Unexpected character '{c}' at position {start}.");
        }
    }

    private static bool IsHexDigit(char c)
        => c is >= '0' and <= '9'
        or >= 'a' and <= 'f'
        or >= 'A' and <= 'F';
}

internal sealed class SqlTokenLimitExceededException : Exception
{
    public int Position { get; }
    public int TokenLength { get; }

    public SqlTokenLimitExceededException(int position, int tokenLength)
    {
        Position = position;
        TokenLength = tokenLength;
    }
}

internal sealed class SqlTokenizerException : CSharpDbException
{
    public int Position { get; }
    public int TokenLength { get; }
    public string Rule { get; }

    public SqlTokenizerException(
        int position,
        int tokenLength,
        string rule,
        string message)
        : base(ErrorCode.SyntaxError, message)
    {
        Position = position;
        TokenLength = tokenLength;
        Rule = rule;
    }

    public SqlTokenizerException(
        int position,
        int tokenLength,
        string rule,
        string message,
        Exception innerException)
        : base(ErrorCode.SyntaxError, message, innerException)
    {
        Position = position;
        TokenLength = tokenLength;
        Rule = rule;
    }
}
