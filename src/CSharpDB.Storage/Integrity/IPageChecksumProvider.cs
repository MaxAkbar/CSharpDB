namespace CSharpDB.Storage.Integrity;

/// <summary>
/// Computes checksums for WAL headers/page payloads.
/// </summary>
public interface IPageChecksumProvider
{
    uint Compute(ReadOnlySpan<byte> data);
}
