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
    private static readonly byte[] FingerprintPreamble =
        Encoding.UTF8.GetBytes(QueryFingerprint.Algorithm + "\0");

    public static SqlQueryFingerprintProvider Instance { get; } = new();

    public QueryFingerprint CreateFingerprint(
        string sql,
        CancellationToken cancellationToken = default)
        => Process(sql, includeNormalizedText: false, cancellationToken).Fingerprint;

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
