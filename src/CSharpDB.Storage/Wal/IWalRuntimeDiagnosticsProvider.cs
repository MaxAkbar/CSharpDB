namespace CSharpDB.Storage.Wal;

internal interface IWalRuntimeDiagnosticsProvider
{
    WalFlushDiagnosticsSnapshot GetWalFlushDiagnosticsSnapshot();
    void ResetWalFlushDiagnostics();
}
