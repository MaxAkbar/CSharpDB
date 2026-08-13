using CSharpDB.Storage.Diagnostics;

namespace CSharpDB.Storage.Wal;

internal interface IWalRecoveryRuntimeSnapshotProvider
{
    bool TryGetRecoveryRuntimeSnapshot(
        out StorageRecoveryRuntimeRawSnapshot snapshot);
}
