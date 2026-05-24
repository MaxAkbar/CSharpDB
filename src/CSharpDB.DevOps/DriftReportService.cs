namespace CSharpDB.DevOps;

public sealed class DriftReportService
{
    private readonly SchemaComparisonService _schemaComparison = new();
    private readonly DataComparisonService _dataComparison = new();

    public async Task<DriftReport> CreateAsync(
        ISchemaCompareTarget baselineSchema,
        ISchemaCompareTarget currentSchema,
        IDataCompareTarget? baselineData = null,
        IDataCompareTarget? currentData = null,
        DriftReportOptions? options = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(baselineSchema);
        ArgumentNullException.ThrowIfNull(currentSchema);

        options ??= new DriftReportOptions();
        SchemaDiffReport schemaReport = await _schemaComparison.CompareAsync(baselineSchema, currentSchema, ct);

        var dataReports = new List<DataDiffReport>();
        if (options.DataTables.Count > 0)
        {
            if (baselineData is null || currentData is null)
                throw new InvalidOperationException("Data drift tables require data compare targets.");

            foreach (DataCompareOptions table in options.DataTables)
                dataReports.Add(await _dataComparison.CompareAsync(baselineData, currentData, table, ct));
        }

        int dataRowsDifferent = dataReports.Sum(report =>
            report.Summary.SourceOnlyRows + report.Summary.TargetOnlyRows + report.Summary.ChangedRows + report.Summary.UnmatchedRows);

        return new DriftReport
        {
            Baseline = baselineSchema.Descriptor,
            Current = currentSchema.Descriptor,
            Schema = schemaReport,
            Data = dataReports,
            Summary = new DriftSummary
            {
                HasDrift = schemaReport.Summary.TotalChanges > 0 || dataRowsDifferent > 0,
                SchemaChanges = schemaReport.Summary.TotalChanges,
                DestructiveSchemaChanges = schemaReport.Summary.DestructiveChanges,
                DataTablesCompared = dataReports.Count,
                DataTablesWithDifferences = dataReports.Count(report => report.Summary.HasDifferences),
                DataRowsDifferent = dataRowsDifferent,
            },
        };
    }
}
