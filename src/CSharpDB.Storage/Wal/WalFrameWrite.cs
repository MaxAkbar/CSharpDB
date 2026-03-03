namespace CSharpDB.Storage.Wal;

/// <summary>
/// Represents a WAL frame payload to append during a write transaction.
/// </summary>
public readonly record struct WalFrameWrite(uint PageId, ReadOnlyMemory<byte> PageData);
