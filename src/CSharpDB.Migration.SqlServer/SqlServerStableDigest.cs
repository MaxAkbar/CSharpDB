using System.Security.Cryptography;
using System.Text;

namespace CSharpDB.Migration.SqlServer;

internal static class SqlServerStableDigest
{
    private static readonly UTF8Encoding s_utf8 =
        new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public static string Text(string domain, params string?[] values)
    {
        ArgumentNullException.ThrowIfNull(values);
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Append(hasher, domain);
        foreach (string? value in values)
            Append(hasher, value);
        return Convert.ToHexString(hasher.GetHashAndReset()).ToLowerInvariant();
    }

    public static string Sequence(string domain, IEnumerable<string?> values)
    {
        ArgumentNullException.ThrowIfNull(values);
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Append(hasher, domain);
        foreach (string? value in values)
            Append(hasher, value);
        return Convert.ToHexString(hasher.GetHashAndReset()).ToLowerInvariant();
    }

    private static void Append(IncrementalHash hasher, string? value)
    {
        Span<byte> presence = stackalloc byte[1];
        presence[0] = value is null ? (byte)0 : (byte)1;
        hasher.AppendData(presence);
        if (value is null)
            return;

        byte[] bytes = s_utf8.GetBytes(value);
        Span<byte> length = stackalloc byte[sizeof(int)];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(length, bytes.Length);
        hasher.AppendData(length);
        hasher.AppendData(bytes);
    }
}
