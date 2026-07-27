using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Computes an unambiguous, length-prefixed digest for JSON adapter policy
/// components. Null and empty components remain distinct.
/// </summary>
internal static class JsonStableDigest
{
    internal static string Compute(params string?[] components)
    {
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> lengthBytes = stackalloc byte[4];
        foreach (string? component in components)
        {
            if (component is null)
            {
                BinaryPrimitives.WriteInt32BigEndian(lengthBytes, -1);
                hasher.AppendData(lengthBytes);
                continue;
            }

            byte[] bytes = Encoding.UTF8.GetBytes(component);
            BinaryPrimitives.WriteInt32BigEndian(lengthBytes, bytes.Length);
            hasher.AppendData(lengthBytes);
            hasher.AppendData(bytes);
            CryptographicOperations.ZeroMemory(bytes);
        }

        return "sha256:" +
            Convert.ToHexString(hasher.GetHashAndReset()).ToLowerInvariant();
    }
}
