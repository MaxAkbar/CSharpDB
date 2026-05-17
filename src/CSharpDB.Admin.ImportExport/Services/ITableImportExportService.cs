using CSharpDB.Admin.ImportExport.Contracts;

namespace CSharpDB.Admin.ImportExport.Services;

public interface ITableImportExportService
{
    Task<string> GetDefaultServerExportPathAsync(string tableName, CancellationToken ct = default);
    Task<IReadOnlyList<ExternalTableRegistrationInfo>> GetExternalTablesAsync(CancellationToken ct = default);
    Task<TableExportResult> ExportTableAsync(
        TableExportRequest request,
        IProgress<TableExportProgress>? progress = null,
        CancellationToken ct = default);
    Task RegisterExternalTableAsync(ExternalTableRegistrationRequest request, CancellationToken ct = default);
    Task RegisterExternalTableAsync(
        ExternalTableRegistrationRequest request,
        IProgress<TableExportProgress>? progress,
        CancellationToken ct = default);
    Task DropExternalTableAsync(string tableName, CancellationToken ct = default);
    Task<RestoreTableResult> RestoreTableAsync(RestoreTableRequest request, CancellationToken ct = default);
}
