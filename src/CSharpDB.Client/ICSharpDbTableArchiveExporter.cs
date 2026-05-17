using CSharpDB.Client.Models;

namespace CSharpDB.Client;

public interface ICSharpDbTableArchiveExporter
{
    bool SupportsTableArchiveExport { get; }

    Task<TableArchiveExportResult> ExportTableArchiveAsync(
        string tableName,
        string path,
        CancellationToken ct = default);
}

public interface ICSharpDbTableArchiveProgressExporter : ICSharpDbTableArchiveExporter
{
    Task<TableArchiveExportResult> ExportTableArchiveAsync(
        string tableName,
        string path,
        IProgress<TableArchiveExportProgress>? progress,
        CancellationToken ct = default);
}
