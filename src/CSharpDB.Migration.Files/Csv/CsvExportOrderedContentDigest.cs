using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration.Canonicalization;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Duplicate- and order-preserving logical checksum used by CSV export v1.
/// The hash input is the ASCII domain, the 32-byte canonical codec contract
/// hash, each 32-byte canonical row hash in emitted order, and the final
/// unsigned 64-bit big-endian row count.
/// </summary>
public sealed class CsvExportOrderedContentDigest : IDisposable
{
    private readonly IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
    private ulong rowCount;
    private bool completed;
    private bool disposed;

    public CsvExportOrderedContentDigest()
    {
        hash.AppendData(Encoding.ASCII.GetBytes(CsvExportContracts.OrderedContentDigestDomain));
        hash.AppendData(Convert.FromHexString(CanonicalRowCodec.ContractHashHex));
    }

    public long RowCount => checked((long)rowCount);

    public void AppendRow(IReadOnlyList<CanonicalValue> fields)
    {
        ArgumentNullException.ThrowIfNull(fields);
        AppendRowHash(CanonicalRowCodec.ComputeRowHashBytes(fields));
    }

    public void AppendRowHash(ReadOnlySpan<byte> canonicalRowHash)
    {
        ThrowIfUnavailable();
        if (canonicalRowHash.Length != SHA256.HashSizeInBytes)
        {
            throw new ArgumentException(
                "A canonical CSV export row hash must contain exactly 32 bytes.",
                nameof(canonicalRowHash));
        }
        if (rowCount == long.MaxValue)
            throw new OverflowException("CSV export row count exceeds the signed 64-bit contract.");

        hash.AppendData(canonicalRowHash);
        rowCount++;
    }

    /// <summary>
    /// Returns the SHA-256 of the ordered-content prefix accumulated so far,
    /// before the final row-count suffix is appended. The operation does not
    /// mutate or reset the digest and may be repeated while it remains active.
    /// </summary>
    public CsvExportHashManifest GetCurrentPrefixDigest()
    {
        ThrowIfUnavailable();
        string value = Convert.ToHexString(hash.GetCurrentHash()).ToLowerInvariant();
        return new CsvExportHashManifest
        {
            Algorithm = CsvExportHashManifest.Sha256Algorithm,
            Value = value,
        };
    }

    public CsvExportHashManifest Complete()
    {
        ThrowIfUnavailable();
        Span<byte> countBytes = stackalloc byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64BigEndian(countBytes, rowCount);
        hash.AppendData(countBytes);
        string value = Convert.ToHexString(hash.GetCurrentHash()).ToLowerInvariant();
        completed = true;
        return new CsvExportHashManifest
        {
            Algorithm = CsvExportHashManifest.Sha256Algorithm,
            Value = value,
        };
    }

    public void Dispose()
    {
        if (disposed)
            return;
        disposed = true;
        hash.Dispose();
        GC.SuppressFinalize(this);
    }

    private void ThrowIfUnavailable()
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        if (completed)
            throw new InvalidOperationException("The CSV export logical checksum is already complete.");
    }
}
