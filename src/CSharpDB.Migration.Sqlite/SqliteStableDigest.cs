using System.Security.Cryptography;
using System.Text;

namespace CSharpDB.Migration.Sqlite;

internal static class SqliteStableDigest
{
    public static string Text(string domain, params string?[] values)
    {
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Append(hasher, domain);
        foreach (string? value in values)
            Append(hasher, value);
        return Convert.ToHexString(hasher.GetHashAndReset()).ToLowerInvariant();
    }

    public static async ValueTask<string> FileAsync(
        string path,
        CancellationToken cancellationToken)
    {
        await using var stream = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 128 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        byte[] digest = await SHA256.HashDataAsync(stream, cancellationToken).ConfigureAwait(false);
        return "sha256:" + Convert.ToHexString(digest).ToLowerInvariant();
    }

    private static void Append(IncrementalHash hasher, string? value)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(value ?? string.Empty);
        Span<byte> length = stackalloc byte[sizeof(int)];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(length, bytes.Length);
        hasher.AppendData(length);
        hasher.AppendData(bytes);
    }
}
