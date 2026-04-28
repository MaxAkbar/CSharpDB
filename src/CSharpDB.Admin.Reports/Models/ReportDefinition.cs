namespace CSharpDB.Admin.Reports.Models;

public sealed record ReportDefinition(
    string ReportId,
    string Name,
    ReportSourceReference Source,
    int DefinitionVersion,
    string SourceSchemaSignature,
    ReportPageSettings PageSettings,
    IReadOnlyList<ReportGroupDefinition> Groups,
    IReadOnlyList<ReportSortDefinition> Sorts,
    IReadOnlyList<ReportBandDefinition> Bands,
    IReadOnlyDictionary<string, object?>? RendererHints = null,
    IReadOnlyList<ReportEventBinding>? EventBindings = null);
