namespace CSharpDB.Observability;

public sealed record QueryFingerprint
{
    public const string Algorithm = "csharpdb-sql-v1";
    private const int Sha256HexLength = 64;

    public QueryFingerprint(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);

        string prefix = Algorithm + ":";
        if (!value.StartsWith(prefix, StringComparison.Ordinal) ||
            value.Length != prefix.Length + Sha256HexLength ||
            !value.AsSpan(prefix.Length).ContainsOnlyHexDigits())
        {
            throw new ArgumentException(
                $"A query fingerprint must use the '{Algorithm}' contract and a full SHA-256 digest.",
                nameof(value));
        }

        Value = value;
    }

    public string Value { get; }

    public override string ToString() => Value;
}

public sealed record QueryFingerprintResult(
    string NormalizedText,
    QueryFingerprint Fingerprint);

public interface IQueryFingerprintProvider
{
    QueryFingerprint CreateFingerprint(
        string sql,
        CancellationToken cancellationToken = default);

    QueryFingerprintResult NormalizeAndFingerprint(
        string sql,
        CancellationToken cancellationToken = default);
}

file static class HexSpanExtensions
{
    internal static bool ContainsOnlyHexDigits(this ReadOnlySpan<char> value)
    {
        foreach (char character in value)
        {
            if (character is not (>= '0' and <= '9') and
                not (>= 'a' and <= 'f'))
            {
                return false;
            }
        }

        return true;
    }
}
