using CSharpDB.Storage.Diagnostics;

namespace CSharpDB.Storage.Wal;

/// <summary>
/// Optional live gauges exposed by built-in WAL implementations. This is kept
/// separate from resettable benchmark diagnostics and from public WAL extension
/// contracts.
/// </summary>
internal interface ILiveWalRuntimeSnapshotProvider
{
    bool TryGetLiveRuntimeDiagnosticsSnapshot(out WalRuntimeRawSnapshot snapshot);
}
