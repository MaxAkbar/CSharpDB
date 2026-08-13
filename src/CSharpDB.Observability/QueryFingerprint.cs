using System.Buffers.Binary;

namespace CSharpDB.Observability;

public sealed record QueryFingerprint
{
    public const string Algorithm = "csharpdb-sql-v1";
    private const int Sha256HexLength = 64;
    private readonly ulong _digest0;
    private readonly ulong _digest1;
    private readonly ulong _digest2;
    private readonly ulong _digest3;
    private string? _value;

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

        byte[] digest = Convert.FromHexString(value.AsSpan(prefix.Length));
        _digest0 = BinaryPrimitives.ReadUInt64LittleEndian(digest);
        _digest1 = BinaryPrimitives.ReadUInt64LittleEndian(digest.AsSpan(8));
        _digest2 = BinaryPrimitives.ReadUInt64LittleEndian(digest.AsSpan(16));
        _digest3 = BinaryPrimitives.ReadUInt64LittleEndian(digest.AsSpan(24));
        _value = value;
    }

    internal QueryFingerprint(ReadOnlySpan<byte> sha256Digest)
    {
        if (sha256Digest.Length != 32)
            throw new ArgumentException("A full SHA-256 digest is required.", nameof(sha256Digest));

        _digest0 = BinaryPrimitives.ReadUInt64LittleEndian(sha256Digest);
        _digest1 = BinaryPrimitives.ReadUInt64LittleEndian(sha256Digest[8..]);
        _digest2 = BinaryPrimitives.ReadUInt64LittleEndian(sha256Digest[16..]);
        _digest3 = BinaryPrimitives.ReadUInt64LittleEndian(sha256Digest[24..]);
    }

    public string Value
    {
        get
        {
            string? value = Volatile.Read(ref _value);
            if (value is not null)
                return value;

            value = string.Create(
                Algorithm.Length + 1 + Sha256HexLength,
                (_digest0, _digest1, _digest2, _digest3),
                static (destination, digest) =>
                {
                    Algorithm.AsSpan().CopyTo(destination);
                    destination[Algorithm.Length] = ':';
                    Span<byte> bytes = stackalloc byte[32];
                    BinaryPrimitives.WriteUInt64LittleEndian(bytes, digest._digest0);
                    BinaryPrimitives.WriteUInt64LittleEndian(bytes[8..], digest._digest1);
                    BinaryPrimitives.WriteUInt64LittleEndian(bytes[16..], digest._digest2);
                    BinaryPrimitives.WriteUInt64LittleEndian(bytes[24..], digest._digest3);
                    Span<char> hex = destination[(Algorithm.Length + 1)..];
                    for (int index = 0; index < bytes.Length; index++)
                    {
                        byte current = bytes[index];
                        hex[index * 2] = ToLowerHex(current >> 4);
                        hex[(index * 2) + 1] = ToLowerHex(current & 0x0f);
                    }
                });
            Interlocked.CompareExchange(ref _value, value, null);
            return _value;
        }
    }

    public bool Equals(QueryFingerprint? other)
        => other is not null &&
           _digest0 == other._digest0 &&
           _digest1 == other._digest1 &&
           _digest2 == other._digest2 &&
           _digest3 == other._digest3;

    public override int GetHashCode()
        => HashCode.Combine(_digest0, _digest1, _digest2, _digest3);

    public override string ToString() => Value;

    private static char ToLowerHex(int value)
        => (char)(value < 10 ? '0' + value : 'a' + value - 10);
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
