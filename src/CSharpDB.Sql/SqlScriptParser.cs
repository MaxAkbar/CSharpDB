using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Sql;

/// <summary>
/// An absolute source span. Offsets and lengths are UTF-16 code units; lines and
/// columns are one-based.
/// </summary>
public readonly record struct SqlSourceSpan(int Start, int Length, int Line, int Column)
{
    public int End => checked(Start + Length);
}

public enum SqlScriptParseErrorCategory
{
    Limit,
    Lexical,
    Syntax,
}

public sealed class SqlScriptParseException : CSharpDbException
{
    public SqlScriptParseErrorCategory Category { get; }
    public string Rule { get; }
    public SqlSourceSpan Span { get; }

    internal SqlScriptParseException(
        SqlScriptParseErrorCategory category,
        string rule,
        SqlSourceSpan span,
        string detail)
        : base(ErrorCode.SyntaxError, FormatMessage(category, rule, span, detail))
    {
        Category = category;
        Rule = rule;
        Span = span;
    }

    internal SqlScriptParseException(
        SqlScriptParseErrorCategory category,
        string rule,
        SqlSourceSpan span,
        string detail,
        Exception innerException)
        : base(ErrorCode.SyntaxError, FormatMessage(category, rule, span, detail), innerException)
    {
        Category = category;
        Rule = rule;
        Span = span;
    }

    private static string FormatMessage(
        SqlScriptParseErrorCategory category,
        string rule,
        SqlSourceSpan span,
        string detail)
        => $"SQL script {category.ToString().ToLowerInvariant()} error [{rule}] " +
           $"at line {span.Line}, column {span.Column} (offset {span.Start}): {detail}";
}

public sealed record SqlScriptParserOptions
{
    public const int HardMaxScriptCharacters = 4 * 1024 * 1024;
    public const int HardMaxScriptUtf8Bytes = 16 * 1024 * 1024;
    public const int HardMaxStatementCount = 4096;
    public const int HardMaxStatementCharacters = 1024 * 1024;
    public const int HardMaxTokenCount = 250_000;
    public const int HardMaxNestingDepth = 128;

    public static SqlScriptParserOptions Default { get; } = new();

    public int MaxScriptCharacters { get; init; } = HardMaxScriptCharacters;
    public int MaxScriptUtf8Bytes { get; init; } = HardMaxScriptUtf8Bytes;
    public int MaxStatementCount { get; init; } = HardMaxStatementCount;
    public int MaxStatementCharacters { get; init; } = HardMaxStatementCharacters;
    public int MaxTokenCount { get; init; } = HardMaxTokenCount;
    public int MaxNestingDepth { get; init; } = HardMaxNestingDepth;

    internal void Validate()
    {
        ValidateLimit(MaxScriptCharacters, HardMaxScriptCharacters, nameof(MaxScriptCharacters));
        ValidateLimit(MaxScriptUtf8Bytes, HardMaxScriptUtf8Bytes, nameof(MaxScriptUtf8Bytes));
        ValidateLimit(MaxStatementCount, HardMaxStatementCount, nameof(MaxStatementCount));
        ValidateLimit(MaxStatementCharacters, HardMaxStatementCharacters, nameof(MaxStatementCharacters));
        ValidateLimit(MaxTokenCount, HardMaxTokenCount, nameof(MaxTokenCount));
        ValidateLimit(MaxNestingDepth, HardMaxNestingDepth, nameof(MaxNestingDepth));
    }

    private static void ValidateLimit(int value, int hardMaximum, string name)
    {
        if (value <= 0 || value > hardMaximum)
        {
            throw new ArgumentOutOfRangeException(
                name,
                value,
                $"SQL script parser limits must be between 1 and the production ceiling of {hardMaximum}.");
        }
    }
}

public sealed class SqlScriptStatement
{
    internal SqlScriptStatement(int index, string text, Statement statement, SqlSourceSpan span)
    {
        Index = index;
        Text = text;
        Statement = statement;
        Span = span;
    }

    public int Index { get; }
    public string Text { get; }
    public Statement Statement { get; }
    public SqlSourceSpan Span { get; }
}

/// <summary>
/// Parses a bounded SQL script into independently executable statements while
/// retaining their exact absolute source locations.
/// </summary>
public static class SqlScriptParser
{
    public static IReadOnlyList<SqlScriptStatement> Parse(
        string script,
        SqlScriptParserOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(script);
        options ??= SqlScriptParserOptions.Default;
        options.Validate();
        cancellationToken.ThrowIfCancellationRequested();

        ValidateSourceEncodingAndSize(script, options, cancellationToken);
        var sourceMap = new SourceMap(script);

        List<Token> tokens;
        try
        {
            tokens = new Tokenizer(script).Tokenize(options.MaxTokenCount, cancellationToken);
        }
        catch (SqlTokenLimitExceededException ex)
        {
            throw CreateError(
                SqlScriptParseErrorCategory.Limit,
                "script.max-tokens",
                sourceMap,
                ex.Position,
                ex.TokenLength,
                $"Script exceeds the maximum of {options.MaxTokenCount} tokens.",
                ex);
        }
        catch (SqlTokenizerException ex)
        {
            throw CreateError(
                SqlScriptParseErrorCategory.Lexical,
                ex.Rule,
                sourceMap,
                ex.Position,
                ex.TokenLength,
                ex.Message,
                ex);
        }

        ValidateNesting(tokens, options, sourceMap, cancellationToken);
        List<StatementRange> ranges = FindStatements(
            tokens,
            options,
            sourceMap,
            cancellationToken);

        if (ranges.Count == 0)
            return Array.Empty<SqlScriptStatement>();

        var statements = new SqlScriptStatement[ranges.Count];
        for (int i = 0; i < ranges.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            StatementRange range = ranges[i];
            string text = script.Substring(range.Start, range.Length);
            Statement statement;
            try
            {
                statement = Parser.Parse(text);
            }
            catch (CSharpDbException ex)
            {
                int absolutePosition = range.Start;
                int tokenLength = range.Length;
                if (TryReadParserPosition(ex.Message, out int relativePosition))
                {
                    absolutePosition = checked(range.Start + relativePosition);
                    tokenLength = FindTokenLength(tokens, absolutePosition);
                }

                throw CreateError(
                    SqlScriptParseErrorCategory.Syntax,
                    "statement.syntax",
                    sourceMap,
                    absolutePosition,
                    tokenLength,
                    ex.Message,
                    ex);
            }

            statements[i] = new SqlScriptStatement(
                i,
                text,
                statement,
                sourceMap.CreateSpan(range.Start, range.Length));
        }

        return statements;
    }

    private static void ValidateSourceEncodingAndSize(
        string script,
        SqlScriptParserOptions options,
        CancellationToken cancellationToken)
    {
        if (script.Length > options.MaxScriptCharacters)
        {
            int start = options.MaxScriptCharacters;
            throw new SqlScriptParseException(
                SqlScriptParseErrorCategory.Limit,
                "script.max-characters",
                CreateSpanByScanning(script, start, 1),
                $"Script exceeds the maximum of {options.MaxScriptCharacters} characters.");
        }

        long utf8Bytes = 0;
        int position = 0;
        while (position < script.Length)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!Rune.TryGetRuneAt(script, position, out Rune rune))
            {
                throw new SqlScriptParseException(
                    SqlScriptParseErrorCategory.Lexical,
                    "script.valid-utf16",
                    CreateSpanByScanning(script, position, 1),
                    "Script contains an unpaired UTF-16 surrogate.");
            }

            utf8Bytes += rune.Utf8SequenceLength;
            if (utf8Bytes > options.MaxScriptUtf8Bytes)
            {
                throw new SqlScriptParseException(
                    SqlScriptParseErrorCategory.Limit,
                    "script.max-utf8-bytes",
                    CreateSpanByScanning(script, position, rune.Utf16SequenceLength),
                    $"Script exceeds the maximum of {options.MaxScriptUtf8Bytes} UTF-8 bytes.");
            }

            position += rune.Utf16SequenceLength;
        }
    }

    private static void ValidateNesting(
        IReadOnlyList<Token> tokens,
        SqlScriptParserOptions options,
        SourceMap sourceMap,
        CancellationToken cancellationToken)
    {
        int parenthesisDepth = 0;
        int blockDepth = 0;
        int unaryDepth = 0;

        foreach (Token token in tokens)
        {
            cancellationToken.ThrowIfCancellationRequested();
            switch (token.Type)
            {
                case TokenType.LeftParen:
                    parenthesisDepth++;
                    unaryDepth = 0;
                    break;
                case TokenType.RightParen:
                    if (parenthesisDepth > 0)
                        parenthesisDepth--;
                    unaryDepth = 0;
                    break;
                case TokenType.Begin:
                    blockDepth++;
                    unaryDepth = 0;
                    break;
                case TokenType.End:
                    if (blockDepth > 0)
                        blockDepth--;
                    unaryDepth = 0;
                    break;
                case TokenType.Not:
                case TokenType.Plus:
                case TokenType.Minus:
                    unaryDepth++;
                    break;
                default:
                    unaryDepth = 0;
                    break;
            }

            int depth = parenthesisDepth + blockDepth + unaryDepth;
            if (depth > options.MaxNestingDepth)
            {
                throw CreateError(
                    SqlScriptParseErrorCategory.Limit,
                    "statement.max-nesting",
                    sourceMap,
                    token.Position,
                    token.Length,
                    $"Statement exceeds the maximum nesting depth of {options.MaxNestingDepth}.");
            }
        }
    }

    private static List<StatementRange> FindStatements(
        IReadOnlyList<Token> tokens,
        SqlScriptParserOptions options,
        SourceMap sourceMap,
        CancellationToken cancellationToken)
    {
        var ranges = new List<StatementRange>(Math.Min(options.MaxStatementCount, 16));
        int statementStart = -1;
        int lastTokenEnd = -1;
        bool createSeen = false;
        bool createTrigger = false;
        int triggerBeginDepth = 0;
        bool conditionalStatement = false;
        int conditionalBeginDepth = 0;

        foreach (Token token in tokens)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (token.Type == TokenType.Eof)
                break;

            if (statementStart < 0)
            {
                if (token.Type == TokenType.Semicolon)
                    continue;

                statementStart = token.Position;
                createSeen = token.Type == TokenType.Create;
                createTrigger = false;
                triggerBeginDepth = 0;
                conditionalStatement = token.Type == TokenType.If;
                conditionalBeginDepth = 0;
            }
            else if (createSeen && !createTrigger && token.Type == TokenType.Trigger)
            {
                createTrigger = true;
            }

            lastTokenEnd = checked(token.Position + token.Length);
            if (createTrigger)
            {
                if (token.Type == TokenType.Begin)
                    triggerBeginDepth++;
                else if (token.Type == TokenType.End && triggerBeginDepth > 0)
                    triggerBeginDepth--;
            }
            else if (conditionalStatement)
            {
                if (token.Type == TokenType.Begin)
                    conditionalBeginDepth++;
                else if (token.Type == TokenType.End && conditionalBeginDepth > 0)
                    conditionalBeginDepth--;
            }

            if (token.Type != TokenType.Semicolon)
                continue;

            bool terminatesStatement =
                (!createTrigger || triggerBeginDepth == 0)
                && (!conditionalStatement || conditionalBeginDepth == 0);
            if (!terminatesStatement)
                continue;

            AddRange(
                ranges,
                new StatementRange(statementStart, lastTokenEnd - statementStart),
                options,
                sourceMap);
            statementStart = -1;
            lastTokenEnd = -1;
            createSeen = false;
            createTrigger = false;
            triggerBeginDepth = 0;
            conditionalStatement = false;
            conditionalBeginDepth = 0;
        }

        if (statementStart >= 0)
        {
            AddRange(
                ranges,
                new StatementRange(statementStart, lastTokenEnd - statementStart),
                options,
                sourceMap);
        }

        return ranges;
    }

    private static void AddRange(
        List<StatementRange> ranges,
        StatementRange range,
        SqlScriptParserOptions options,
        SourceMap sourceMap)
    {
        if (range.Length > options.MaxStatementCharacters)
        {
            throw CreateError(
                SqlScriptParseErrorCategory.Limit,
                "statement.max-characters",
                sourceMap,
                range.Start,
                range.Length,
                $"Statement exceeds the maximum of {options.MaxStatementCharacters} characters.");
        }

        if (ranges.Count == options.MaxStatementCount)
        {
            throw CreateError(
                SqlScriptParseErrorCategory.Limit,
                "script.max-statements",
                sourceMap,
                range.Start,
                range.Length,
                $"Script exceeds the maximum of {options.MaxStatementCount} statements.");
        }

        ranges.Add(range);
    }

    private static SqlScriptParseException CreateError(
        SqlScriptParseErrorCategory category,
        string rule,
        SourceMap sourceMap,
        int start,
        int length,
        string detail,
        Exception? innerException = null)
    {
        SqlSourceSpan span = sourceMap.CreateSpan(start, length);
        return innerException is null
            ? new SqlScriptParseException(category, rule, span, detail)
            : new SqlScriptParseException(category, rule, span, detail, innerException);
    }

    private static bool TryReadParserPosition(string message, out int position)
    {
        const string prefix = "Syntax error at position ";
        position = 0;
        if (!message.StartsWith(prefix, StringComparison.Ordinal))
            return false;

        int end = message.IndexOf(':', prefix.Length);
        return end > prefix.Length &&
               int.TryParse(
                   message.AsSpan(prefix.Length, end - prefix.Length),
                   out position);
    }

    private static int FindTokenLength(IReadOnlyList<Token> tokens, int absolutePosition)
    {
        foreach (Token token in tokens)
        {
            if (token.Position == absolutePosition)
                return token.Length;
            if (token.Position > absolutePosition)
                break;
        }

        return 0;
    }

    private static SqlSourceSpan CreateSpanByScanning(string source, int start, int length)
    {
        int line = 1;
        int column = 1;
        int index = 0;
        while (index < start)
        {
            char current = source[index++];
            if (current == '\r')
            {
                if (index < start && source[index] == '\n')
                    index++;
                line++;
                column = 1;
            }
            else if (current == '\n')
            {
                line++;
                column = 1;
            }
            else
            {
                column++;
            }
        }

        return new SqlSourceSpan(start, length, line, column);
    }

    private readonly record struct StatementRange(int Start, int Length);

    private sealed class SourceMap
    {
        private readonly int[] _lineStarts;
        private readonly int _sourceLength;

        public SourceMap(string source)
        {
            _sourceLength = source.Length;
            var lineStarts = new List<int> { 0 };
            for (int i = 0; i < source.Length; i++)
            {
                if (source[i] == '\r')
                {
                    if (i + 1 < source.Length && source[i + 1] == '\n')
                        i++;
                    lineStarts.Add(i + 1);
                }
                else if (source[i] == '\n')
                {
                    lineStarts.Add(i + 1);
                }
            }

            _lineStarts = lineStarts.ToArray();
        }

        public SqlSourceSpan CreateSpan(int start, int length)
        {
            start = Math.Clamp(start, 0, _sourceLength);
            length = Math.Clamp(length, 0, _sourceLength - start);

            int lineIndex = Array.BinarySearch(_lineStarts, start);
            if (lineIndex < 0)
                lineIndex = ~lineIndex - 1;

            return new SqlSourceSpan(
                start,
                length,
                lineIndex + 1,
                start - _lineStarts[lineIndex] + 1);
        }
    }
}
