namespace CSharpDB.Storage.Paging;

internal interface ICommitPathDiagnosticsProvider
{
    CommitPathDiagnosticsSnapshot GetCommitPathDiagnosticsSnapshot();
    void ResetCommitPathDiagnostics();
}
