namespace CSharpDB.Storage.Wal;

/// <summary>
/// Optional built-in WAL capability for validating snapshot safety before changing
/// the database file. Preparing freezes the exact remaining copy targets; later
/// commits must not change those targets until this checkpoint finishes.
/// </summary>
/// <remarks>
/// The caller holds the checkpoint coordinator's admission lock across preparation,
/// validation, and copying. WAL providers without this capability defer copying
/// while readers are active.
/// </remarks>
internal interface IWalCheckpointCopyPlan
{
    ValueTask<ReadOnlyMemory<KeyValuePair<uint, long>>> PrepareCheckpointCopyAsync(
        uint pageCount, CancellationToken ct);
}
