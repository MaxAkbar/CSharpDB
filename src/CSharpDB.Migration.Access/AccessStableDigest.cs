using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace CSharpDB.Migration.Access;

internal static class AccessStableDigest
{
    private static readonly UTF8Encoding StrictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);

    internal static string Text(
        string domain,
        params string?[] values)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(domain);
        using IncrementalHash hash =
            IncrementalHash.CreateHash(
                HashAlgorithmName.SHA256);
        Append(hash, domain);
        foreach (string? value in values)
            Append(hash, value);
        return "sha256:" +
            Convert.ToHexString(
                    hash.GetHashAndReset())
                .ToLowerInvariant();
    }

    internal static async ValueTask<string> FileAsync(
        Stream stream,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (!stream.CanRead || !stream.CanSeek)
            throw new ArgumentException(
                "The Access source digest requires a readable, seekable stream.",
                nameof(stream));
        stream.Position = 0;
        using IncrementalHash hash =
            IncrementalHash.CreateHash(
                HashAlgorithmName.SHA256);
        byte[] buffer =
            GC.AllocateUninitializedArray<byte>(
                1024 * 1024);
        while (true)
        {
            int read = await stream.ReadAsync(
                    buffer,
                    cancellationToken)
                .ConfigureAwait(false);
            if (read == 0)
                break;
            hash.AppendData(buffer, 0, read);
        }
        return "sha256:" +
            Convert.ToHexString(
                    hash.GetHashAndReset())
                .ToLowerInvariant();
    }

    private static void Append(
        IncrementalHash hash,
        string? value)
    {
        Span<byte> length =
            stackalloc byte[sizeof(int)];
        if (value is null)
        {
            BinaryPrimitives.WriteInt32BigEndian(
                length,
                -1);
            hash.AppendData(length);
            return;
        }
        byte[] bytes = StrictUtf8.GetBytes(value);
        BinaryPrimitives.WriteInt32BigEndian(
            length,
            bytes.Length);
        hash.AppendData(length);
        hash.AppendData(bytes);
    }
}
