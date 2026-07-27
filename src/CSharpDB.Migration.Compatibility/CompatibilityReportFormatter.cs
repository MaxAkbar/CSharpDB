using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CSharpDB.Migration.Compatibility;

public static class CompatibilityReportFormatter
{
    private static readonly JsonSerializerOptions s_jsonOptions = CreateJsonOptions();

    public static string ToJson(
        DataTypeMappingReport report,
        bool writeIndented = true)
    {
        ArgumentNullException.ThrowIfNull(report);
        return NormalizeNewlines(
            JsonSerializer.Serialize(
                report,
                writeIndented
                    ? s_jsonOptions
                    : new JsonSerializerOptions(s_jsonOptions)
                    {
                        WriteIndented = false,
                    }));
    }

    public static string ToJson(
        QueryCompatibilityReport report,
        bool writeIndented = true)
    {
        ArgumentNullException.ThrowIfNull(report);
        return NormalizeNewlines(
            JsonSerializer.Serialize(
                report,
                writeIndented
                    ? s_jsonOptions
                    : new JsonSerializerOptions(s_jsonOptions)
                    {
                        WriteIndented = false,
                    }));
    }

    public static string ToText(DataTypeMappingReport report)
    {
        ArgumentNullException.ThrowIfNull(report);
        var builder = new StringBuilder();
        builder.Append("Data type mapping report").Append('\n')
            .Append("Format: ").Append(report.Format).Append('\n')
            .Append("Source: ").Append(Token(report.SourceKind)).Append('\n')
            .Append("Target: CSharpDB ").Append(Escape(report.TargetCSharpDbVersion)).Append('\n')
            .Append("Catalog: ").Append(report.CatalogDigest).Append('\n')
            .Append("Policy: ").Append(Escape(report.MappingPolicyId))
            .Append("/v").Append(report.MappingPolicyVersion).Append('\n')
            .Append("Profile: ").Append(Token(report.Profile)).Append('\n')
            .Append("Summary: total=").Append(report.Summary.Total)
            .Append(" exact=").Append(report.Summary.Exact)
            .Append(" losslessReencoded=").Append(report.Summary.LosslessReencoded)
            .Append(" lossy=").Append(report.Summary.Lossy)
            .Append(" unsupported=").Append(report.Summary.Unsupported)
            .Append(" fullStreamValidation=")
            .Append(report.Summary.RequiresFullStreamValidation)
            .Append('\n');

        foreach (DataTypeMappingReportEntry entry in report.Entries)
        {
            builder.Append('\n')
                .Append('[').Append(Escape(entry.SourceObjectId)).Append("] ")
                .Append(Escape(entry.SourceName)).Append('\n')
                .Append("  kind: ").Append(Token(entry.SourceObjectKind)).Append('\n')
                .Append("  source: ").Append(Escape(entry.SourceNativeType))
                .Append(" (logical=").Append(Escape(entry.SourceLogicalType)).Append(')')
                .Append('\n')
                .Append("  target: ")
                .Append(entry.TargetType is null ? "none" : Token(entry.TargetType.Value))
                .Append('\n')
                .Append("  classification: ").Append(Token(entry.Classification))
                .Append('\n')
                .Append("  coverage: ").Append(Token(entry.Coverage.Kind))
                .Append(" values=").Append(entry.Coverage.ValuesExamined);
            if (entry.Coverage.TotalValues is long total)
                builder.Append('/').Append(total);
            builder.Append(" fullStreamValidation=")
                .Append(entry.Coverage.RequiresFullStreamValidation ? "true" : "false")
                .Append('\n');

            if (entry.Conversion is not null)
            {
                builder.Append("  conversion: ")
                    .Append(Escape(entry.Conversion.ConversionId))
                    .Append("/v").Append(entry.Conversion.Version)
                    .Append('\n');
                foreach (MigrationCatalogFacet parameter in entry.Conversion.Parameters)
                {
                    builder.Append("    ")
                        .Append(Escape(parameter.Name))
                        .Append('=')
                        .Append(Escape(parameter.Value ?? "null"))
                        .Append('\n');
                }
            }

            if (entry.Diagnostic is not null)
            {
                builder.Append("  diagnostic: ")
                    .Append(Escape(entry.Diagnostic.DiagnosticId))
                    .Append(' ')
                    .Append(Token(entry.Diagnostic.Status))
                    .Append(" - ")
                    .Append(Escape(entry.Diagnostic.Summary))
                    .Append('\n');
            }
        }

        return builder.ToString();
    }

    public static string ToText(QueryCompatibilityReport report)
    {
        ArgumentNullException.ThrowIfNull(report);
        var builder = new StringBuilder();
        builder.Append("Query compatibility report").Append('\n')
            .Append("Format: ").Append(report.Format).Append('\n')
            .Append("Target: CSharpDB ").Append(Escape(report.TargetCSharpDbVersion)).Append('\n')
            .Append("Capability: ").Append(report.CapabilityDigest).Append('\n')
            .Append("Summary: total=").Append(report.Summary.Total)
            .Append(" conditional=").Append(report.Summary.Conditional)
            .Append(" unsupported=").Append(report.Summary.Unsupported)
            .Append(" unknown=").Append(report.Summary.Unknown)
            .Append('\n');

        foreach (QueryCompatibilityResult result in report.Results)
        {
            builder.Append('\n')
                .Append('[').Append(Escape(result.QueryId)).Append("] ")
                .Append(Token(result.SourceDialect)).Append(' ')
                .Append(Token(result.Status)).Append('\n')
                .Append("  sourceDigest: ").Append(result.SourceDigest).Append('\n')
                .Append("  sourceParsed: ")
                .Append(result.SourceParsed ? "true" : "false")
                .Append('\n')
                .Append("  targetParsed: ")
                .Append(result.TargetParsed ? "true" : "false")
                .Append('\n')
                .Append("  readOnly: ")
                .Append(result.IsReadOnly switch
                {
                    true => "true",
                    false => "false",
                    null => "unknown",
                })
                .Append('\n');

            if (result.Rewrite is not null)
            {
                builder.Append("  rewrite: ")
                    .Append(Escape(result.Rewrite.RewriteId))
                    .Append(" candidateDigest=")
                    .Append(result.Rewrite.CandidateDigest)
                    .Append('\n');
            }

            foreach (MigrationDiagnostic diagnostic in result.Diagnostics)
            {
                builder.Append("  ")
                    .Append(Token(diagnostic.Severity))
                    .Append(' ')
                    .Append(Escape(diagnostic.RuleId))
                    .Append(": ")
                    .Append(Escape(diagnostic.Summary))
                    .Append('\n');
            }
        }

        return builder.ToString();
    }

    private static JsonSerializerOptions CreateJsonOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = true,
        };
        options.Converters.Add(
            new JsonStringEnumConverter(JsonNamingPolicy.CamelCase));
        return options;
    }

    private static string NormalizeNewlines(string value) =>
        value.Replace("\r\n", "\n", StringComparison.Ordinal);

    private static string Token<T>(T value)
        where T : struct, Enum
    {
        string name = value.ToString();
        if (name.Length == 0)
            return name;
        return char.ToLowerInvariant(name[0]) + name[1..];
    }

    private static string Escape(string value) =>
        value.Replace("\\", "\\\\", StringComparison.Ordinal)
            .Replace("\r", "\\r", StringComparison.Ordinal)
            .Replace("\n", "\\n", StringComparison.Ordinal)
            .Replace("\t", "\\t", StringComparison.Ordinal);
}
