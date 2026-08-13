using CSharpDB.Storage.Diagnostics;

namespace CSharpDB.Storage.Wal;

internal interface IWalCheckpointRuntimeSnapshotProvider
{
    bool TryGetCheckpointProgressSnapshot(
        out WalCheckpointProgressRawSnapshot snapshot);
}
