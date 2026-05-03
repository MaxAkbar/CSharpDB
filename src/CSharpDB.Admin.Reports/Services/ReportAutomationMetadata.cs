using CSharpDB.Admin.Reports.Models;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Reports.Services;

public static class ReportAutomationMetadata
{
    private const string Surface = "admin.reports";
    private static readonly string[] IgnoredFormulaFunctions = ["SUM", "COUNT", "AVG", "MIN", "MAX"];

    public static ReportDefinition NormalizeForExport(ReportDefinition report)
    {
        ArgumentNullException.ThrowIfNull(report);

        DbAutomationMetadata metadata = Build(report);
        return report with { Automation = metadata.IsEmpty ? null : metadata };
    }

    public static DbAutomationMetadata Build(ReportDefinition report)
    {
        ArgumentNullException.ThrowIfNull(report);

        var builder = new DbAutomationMetadataBuilder();
        foreach (ReportEventBinding binding in report.EventBindings ?? [])
            builder.AddCommand(binding.CommandName, Surface, $"report.events.{binding.Event}");

        foreach (ReportBandDefinition band in report.Bands)
        {
            foreach (ReportControlDefinition control in band.Controls)
                AddScalarFunctions(builder, control.Expression, $"bands.{band.BandId}.controls.{control.ControlId}.expression");
        }

        return builder.Build();
    }

    private static void AddScalarFunctions(DbAutomationMetadataBuilder builder, string? expression, string location)
    {
        foreach (DbAutomationScalarFunctionCall call in
            DbAutomationExpressionInspector.FindScalarFunctionCalls(expression, IgnoredFormulaFunctions))
        {
            builder.AddScalarFunction(call.Name, call.Arity, Surface, location);
        }
    }
}
