using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Observability;
using CSharpDB.Primitives;

namespace CSharpDB.Sql;

/// <summary>
/// Tokenizer-backed SQL normalization and fingerprinting. Literal contents and
/// parameter names are replaced before either normalized text or hash input is
/// constructed.
/// </summary>
public sealed class SqlQueryFingerprintProvider : IQueryFingerprintProvider
{
    private const int MaximumTokenCount = 100_000;
    private const int MaximumSqlLength = 1_048_576;
    private const int MaximumNormalizedTextLength = 32_768;
    private const int FastFingerprintBufferSize = 1024;
    private static readonly byte[] FingerprintPreamble =
        Encoding.UTF8.GetBytes(QueryFingerprint.Algorithm + "\0");
    private static readonly string[] CanonicalTokenText =
        CreateCanonicalTokenText();

    public static SqlQueryFingerprintProvider Instance { get; } = new();

    public QueryFingerprint CreateFingerprint(
        string sql,
        CancellationToken cancellationToken = default)
        => TryCreateFingerprintWithoutTokenMaterialization(
            sql,
            cancellationToken,
            out QueryFingerprint? fingerprint)
                ? fingerprint!
                : Process(sql, includeNormalizedText: false, cancellationToken).Fingerprint;

    public QueryFingerprintResult NormalizeAndFingerprint(
        string sql,
        CancellationToken cancellationToken = default)
        => Process(sql, includeNormalizedText: true, cancellationToken);

    private static QueryFingerprintResult Process(
        string sql,
        bool includeNormalizedText,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        if (sql.Length > MaximumSqlLength)
        {
            throw new CSharpDbException(
                ErrorCode.ResourceLimitExceeded,
                $"SQL text exceeds the supported fingerprinting limit of {MaximumSqlLength} characters.");
        }

        List<Token> tokens;
        try
        {
            tokens = new Tokenizer(sql).Tokenize(MaximumTokenCount, cancellationToken);
        }
        catch (SqlTokenLimitExceededException ex)
        {
            throw new CSharpDbException(
                ErrorCode.ResourceLimitExceeded,
                $"SQL token count exceeds the supported limit of {MaximumTokenCount}.",
                ex);
        }

        StringBuilder? normalized = includeNormalizedText
            ? new StringBuilder(Math.Min(sql.Length, 4096))
            : null;
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(FingerprintPreamble);

        bool first = true;
        foreach (Token token in tokens)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (token.Type == TokenType.Eof)
                break;

            string canonical = GetCanonicalToken(token);
            if (normalized is not null)
            {
                int addedLength = canonical.Length + (first ? 0 : 1);
                if (normalized.Length + addedLength > MaximumNormalizedTextLength)
                {
                    throw new CSharpDbException(
                        ErrorCode.ResourceLimitExceeded,
                        $"Normalized SQL exceeds the supported limit of {MaximumNormalizedTextLength} characters.");
                }

                if (!first)
                    normalized.Append(' ');
                normalized.Append(canonical);
            }
            first = false;

            AppendFrame(hash, canonical);
        }

        string digest = Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant();
        return new QueryFingerprintResult(
            normalized?.ToString() ?? string.Empty,
            new QueryFingerprint($"{QueryFingerprint.Algorithm}:{digest}"));
    }

    /// <summary>
    /// Computes the same versioned digest directly from the common ASCII SQL
    /// lexical stream. Query execution already tokenizes or fast-parses the
    /// statement, so history-only capture must not allocate a second token
    /// list and one substring per token before normal execution can begin.
    /// Quoted or non-ASCII identifiers retain the full tokenizer-backed path.
    /// </summary>
    private static bool TryCreateFingerprintWithoutTokenMaterialization(
        string sql,
        CancellationToken cancellationToken,
        out QueryFingerprint? fingerprint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        if (sql.Length > MaximumSqlLength)
        {
            throw new CSharpDbException(
                ErrorCode.ResourceLimitExceeded,
                $"SQL text exceeds the supported fingerprinting limit of {MaximumSqlLength} characters.");
        }

        Span<byte> canonicalBytes = stackalloc byte[FastFingerprintBufferSize];
        FingerprintPreamble.CopyTo(canonicalBytes);
        int canonicalLength = FingerprintPreamble.Length;

        int position = 0;
        int tokenCount = 0;
        while (position < sql.Length)
        {
            cancellationToken.ThrowIfCancellationRequested();
            while (position < sql.Length && char.IsWhiteSpace(sql[position]))
                position++;
            if (position >= sql.Length)
                break;

            char current = sql[position];
            if (current == '-' && position + 1 < sql.Length && sql[position + 1] == '-')
            {
                position += 2;
                while (position < sql.Length && sql[position] != '\n')
                    position++;
                continue;
            }

            if (current == '/' && position + 1 < sql.Length && sql[position + 1] == '*')
            {
                position += 2;
                while (position + 1 < sql.Length &&
                       (sql[position] != '*' || sql[position + 1] != '/'))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    position++;
                }

                if (position + 1 >= sql.Length)
                {
                    fingerprint = null;
                    return false;
                }

                position += 2;
                continue;
            }

            if (++tokenCount > MaximumTokenCount)
            {
                throw new CSharpDbException(
                    ErrorCode.ResourceLimitExceeded,
                    $"SQL token count exceeds the supported limit of {MaximumTokenCount}.");
            }

            if ((current == 'X' || current == 'x') &&
                position + 1 < sql.Length && sql[position + 1] == '\'')
            {
                position += 2;
                int hexLength = 0;
                while (position < sql.Length && sql[position] != '\'')
                {
                    if (!IsHexDigit(sql[position]))
                    {
                        fingerprint = null;
                        return false;
                    }

                    position++;
                    hexLength++;
                }

                if (position >= sql.Length || (hexLength & 1) != 0)
                {
                    fingerprint = null;
                    return false;
                }

                position++;
                if (!TryAppendFrame(canonicalBytes, ref canonicalLength, "?"))
                {
                    fingerprint = null;
                    return false;
                }
                continue;
            }

            if (char.IsLetter(current) || current == '_')
            {
                int start = position++;
                bool ascii = current <= 0x7f;
                while (position < sql.Length &&
                       (char.IsLetterOrDigit(sql[position]) || sql[position] == '_'))
                {
                    ascii &= sql[position] <= 0x7f;
                    position++;
                }

                ReadOnlySpan<char> word = sql.AsSpan(start, position - start);
                if (!ascii || word.Length > SqlIdentifierRules.MaxLength)
                {
                    fingerprint = null;
                    return false;
                }

                if (Tokenizer.TryGetKeyword(word, out TokenType keyword))
                {
                    if (!TryAppendFrame(
                            canonicalBytes,
                            ref canonicalLength,
                            keyword == TokenType.Null
                                ? "?"
                                : CanonicalTokenText[(int)keyword]))
                    {
                        fingerprint = null;
                        return false;
                    }
                }
                else if (!TryAppendAsciiIdentifierFrame(
                             canonicalBytes,
                             ref canonicalLength,
                             word))
                {
                    fingerprint = null;
                    return false;
                }

                continue;
            }

            if (current == '"')
            {
                fingerprint = null;
                return false;
            }

            if (current == '@')
            {
                position++;
                if (position >= sql.Length ||
                    (!char.IsLetter(sql[position]) && sql[position] != '_'))
                {
                    fingerprint = null;
                    return false;
                }

                position++;
                while (position < sql.Length &&
                       (char.IsLetterOrDigit(sql[position]) || sql[position] == '_'))
                {
                    position++;
                }

                if (!TryAppendFrame(canonicalBytes, ref canonicalLength, "?"))
                {
                    fingerprint = null;
                    return false;
                }
                continue;
            }

            if (char.IsDigit(current))
            {
                bool hasDot = false;
                do
                {
                    if (sql[position] == '.')
                    {
                        if (hasDot)
                            break;
                        hasDot = true;
                    }

                    position++;
                }
                while (position < sql.Length &&
                       (char.IsDigit(sql[position]) || sql[position] == '.'));

                if (!TryAppendFrame(canonicalBytes, ref canonicalLength, "?"))
                {
                    fingerprint = null;
                    return false;
                }
                continue;
            }

            if (current == '\'')
            {
                position++;
                bool terminated = false;
                while (position < sql.Length)
                {
                    if (sql[position] != '\'')
                    {
                        position++;
                        continue;
                    }

                    if (position + 1 < sql.Length && sql[position + 1] == '\'')
                    {
                        position += 2;
                        continue;
                    }

                    position++;
                    terminated = true;
                    break;
                }

                if (!terminated)
                {
                    fingerprint = null;
                    return false;
                }

                if (!TryAppendFrame(canonicalBytes, ref canonicalLength, "?"))
                {
                    fingerprint = null;
                    return false;
                }
                continue;
            }

            string? canonical = current switch
            {
                '=' => "=",
                '+' => "+",
                '-' => "-",
                '*' => "*",
                '/' => "/",
                ',' => ",",
                ':' => ":",
                '.' => ".",
                '(' => "(",
                ')' => ")",
                ';' => ";",
                _ => null,
            };
            position++;
            if (canonical is not null)
            {
                if (!TryAppendFrame(canonicalBytes, ref canonicalLength, canonical))
                {
                    fingerprint = null;
                    return false;
                }
                continue;
            }

            if (current == '<')
            {
                if (position < sql.Length && sql[position] == '=')
                {
                    position++;
                    canonical = "<=";
                }
                else if (position < sql.Length && sql[position] == '>')
                {
                    position++;
                    canonical = "<>";
                }
                else
                {
                    canonical = "<";
                }

                if (!TryAppendFrame(canonicalBytes, ref canonicalLength, canonical))
                {
                    fingerprint = null;
                    return false;
                }

                continue;
            }

            if (current == '>')
            {
                if (position < sql.Length && sql[position] == '=')
                {
                    position++;
                    canonical = ">=";
                }
                else
                {
                    canonical = ">";
                }

                if (!TryAppendFrame(canonicalBytes, ref canonicalLength, canonical))
                {
                    fingerprint = null;
                    return false;
                }

                continue;
            }

            if (current == '!' && position < sql.Length && sql[position] == '=')
            {
                position++;
                if (!TryAppendFrame(canonicalBytes, ref canonicalLength, "<>"))
                {
                    fingerprint = null;
                    return false;
                }
                continue;
            }

            fingerprint = null;
            return false;
        }

        Span<byte> digest = stackalloc byte[32];
        SHA256.HashData(canonicalBytes[..canonicalLength], digest);
        fingerprint = new QueryFingerprint(digest);
        return true;
    }

    private static bool TryAppendAsciiIdentifierFrame(
        Span<byte> destination,
        ref int position,
        ReadOnlySpan<char> identifier)
    {
        int byteCount = identifier.Length + 2;
        if (destination.Length - position < sizeof(int) + byteCount)
            return false;

        BinaryPrimitives.WriteInt32LittleEndian(destination[position..], byteCount);
        position += sizeof(int);

        Span<byte> canonical = destination.Slice(position, byteCount);
        canonical[0] = (byte)'"';
        for (int index = 0; index < identifier.Length; index++)
        {
            char character = identifier[index];
            canonical[index + 1] = (byte)(character is >= 'a' and <= 'z'
                ? character - ('a' - 'A')
                : character);
        }

        canonical[^1] = (byte)'"';
        position += byteCount;
        return true;
    }

    private static bool TryAppendFrame(
        Span<byte> destination,
        ref int position,
        string canonical)
    {
        int byteCount = Encoding.UTF8.GetByteCount(canonical);
        if (destination.Length - position < sizeof(int) + byteCount)
            return false;

        BinaryPrimitives.WriteInt32LittleEndian(destination[position..], byteCount);
        position += sizeof(int);
        position += Encoding.UTF8.GetBytes(canonical, destination[position..]);
        return true;
    }

    private static string[] CreateCanonicalTokenText()
    {
        TokenType[] values = Enum.GetValues<TokenType>();
        var canonical = new string[values.Length];
        foreach (TokenType value in values)
            canonical[(int)value] = value.ToString().ToUpperInvariant();
        return canonical;
    }

    private static bool IsHexDigit(char character)
        => character is >= '0' and <= '9' or
            >= 'a' and <= 'f' or
            >= 'A' and <= 'F';

    private static string GetCanonicalToken(Token token)
        => token.Type switch
        {
            TokenType.IntegerLiteral or
            TokenType.RealLiteral or
            TokenType.StringLiteral or
            TokenType.BlobLiteral or
            TokenType.Parameter or
            TokenType.Null => "?",

            TokenType.Identifier =>
                $"\"{token.Value.ToUpperInvariant().Replace("\"", "\"\"", StringComparison.Ordinal)}\"",

            TokenType.Equals => "=",
            TokenType.NotEquals => "<>",
            TokenType.LessThan => "<",
            TokenType.GreaterThan => ">",
            TokenType.LessOrEqual => "<=",
            TokenType.GreaterOrEqual => ">=",
            TokenType.Plus => "+",
            TokenType.Minus => "-",
            TokenType.Star => "*",
            TokenType.Slash => "/",
            TokenType.Comma => ",",
            TokenType.Colon => ":",
            TokenType.Dot => ".",
            TokenType.LeftParen => "(",
            TokenType.RightParen => ")",
            TokenType.Semicolon => ";",

            _ => token.Type.ToString().ToUpperInvariant(),
        };

    private static void AppendFrame(IncrementalHash hash, string canonical)
    {
        int byteCount = Encoding.UTF8.GetByteCount(canonical);
        Span<byte> length = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(length, byteCount);
        hash.AppendData(length);

        if (byteCount <= 512)
        {
            Span<byte> bytes = stackalloc byte[byteCount];
            Encoding.UTF8.GetBytes(canonical, bytes);
            hash.AppendData(bytes);
            return;
        }

        byte[] rented = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            int written = Encoding.UTF8.GetBytes(canonical, rented);
            hash.AppendData(rented.AsSpan(0, written));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented, clearArray: true);
        }
    }
}

public static class SqlQueryNormalizer
{
    public static QueryFingerprintResult NormalizeAndFingerprint(
        string sql,
        CancellationToken cancellationToken = default)
        => SqlQueryFingerprintProvider.Instance.NormalizeAndFingerprint(
            sql,
            cancellationToken);

    public static QueryFingerprint CreateFingerprint(
        string sql,
        CancellationToken cancellationToken = default)
        => SqlQueryFingerprintProvider.Instance.CreateFingerprint(
            sql,
            cancellationToken);
}
