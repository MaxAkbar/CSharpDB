using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration.Canonicalization;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Duplicate- and order-preserving logical checksum used by JSON export v1.
/// The hash input is the unique ASCII domain, the 32-byte canonical codec
/// contract hash, each 32-byte canonical row hash in emitted order, and the
/// final unsigned 64-bit big-endian row count.
/// </summary>
public sealed class JsonExportOrderedContentDigest : IDisposable
{
    private readonly IncrementalHash hash =
        IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

    private ulong rowCount;
    private bool completed;
    private bool disposed;

    public JsonExportOrderedContentDigest()
    {
        hash.AppendData(
            Encoding.ASCII.GetBytes(
                JsonExportContracts.OrderedContentDigestDomain));
        hash.AppendData(
            Convert.FromHexString(
                CanonicalRowCodec.ContractHashHex));
    }

    public long RowCount => checked((long)rowCount);

    public void AppendRow(
        IReadOnlyList<CanonicalValue> fields)
    {
        ArgumentNullException.ThrowIfNull(fields);
        byte[] rowHash =
            CanonicalRowCodec.ComputeRowHashBytes(fields);
        try
        {
            AppendRowHash(rowHash);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(rowHash);
        }
    }

    public void AppendRowHash(
        ReadOnlySpan<byte> canonicalRowHash)
    {
        ThrowIfUnavailable();
        if (canonicalRowHash.Length != SHA256.HashSizeInBytes)
        {
            throw new ArgumentException(
                "A canonical JSON export row hash must contain exactly 32 bytes.",
                nameof(canonicalRowHash));
        }
        if (rowCount == long.MaxValue)
        {
            throw new OverflowException(
                "JSON export row count exceeds the signed 64-bit contract.");
        }

        hash.AppendData(canonicalRowHash);
        rowCount++;
    }

    /// <summary>
    /// Returns the logical prefix accumulated so far, before the final
    /// row-count suffix. This does not mutate the active digest.
    /// </summary>
    public JsonExportHashManifest GetCurrentPrefixDigest()
    {
        ThrowIfUnavailable();
        byte[] current = hash.GetCurrentHash();
        try
        {
            return CreateHash(current);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(current);
        }
    }

    public JsonExportHashManifest Complete()
    {
        ThrowIfUnavailable();
        Span<byte> countBytes =
            stackalloc byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64BigEndian(
            countBytes,
            rowCount);
        hash.AppendData(countBytes);

        byte[] current = hash.GetCurrentHash();
        try
        {
            completed = true;
            return CreateHash(current);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(current);
            CryptographicOperations.ZeroMemory(countBytes);
        }
    }

    public void Dispose()
    {
        if (disposed)
            return;

        disposed = true;
        hash.Dispose();
        GC.SuppressFinalize(this);
    }

    private static JsonExportHashManifest CreateHash(
        ReadOnlySpan<byte> digest) =>
        new()
        {
            Algorithm =
                JsonExportHashManifest.Sha256Algorithm,
            Value = Convert.ToHexString(digest)
                .ToLowerInvariant(),
        };

    private void ThrowIfUnavailable()
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        if (completed)
        {
            throw new InvalidOperationException(
                "The JSON export logical checksum is already complete.");
        }
    }
}
