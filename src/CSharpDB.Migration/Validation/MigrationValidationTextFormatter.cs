using System.Globalization;
using System.Text;

namespace CSharpDB.Migration;

/// <summary>
/// Deterministic, value-free human-readable projection of a validation report.
/// </summary>
public static class MigrationValidationTextFormatter
{
    public static string Format(MigrationValidationReport report)
    {
        MigrationValidationReport normalized = MigrationValidationReportSerializer.Normalize(report);
        string reportDigest = MigrationValidationReportSerializer.ComputeDigest(normalized);
        var text = new StringBuilder();
        Line(text, $"Status: {normalized.Outcome.ToString().ToUpperInvariant()}");
        Line(text, $"Level: {normalized.Level.ToString().ToLowerInvariant()}");
        Line(text, $"Report digest: {reportDigest}");
        Line(text, $"Target CSharpDB version: {normalized.Binding.TargetCSharpDbVersion}");
        Line(text, $"Plan digest: {normalized.Binding.PlanDigest}");
        Line(text, $"Catalog digest: {normalized.Binding.CatalogDigest}");
        Line(text, $"Target: {normalized.Binding.TargetIdentity}");
        Line(text, $"Source snapshot: {normalized.Binding.SourceSnapshotIdentity}");
        Line(text, $"Target snapshot: {normalized.Binding.TargetSnapshotIdentity}");
        Line(text, $"Snapshot consistency: {Token(normalized.SnapshotConsistency.Status)}");
        Line(
            text,
            $"Schema: {Token(normalized.Schema.Status)} source={normalized.Schema.SourceSchemaDigest} target={normalized.Schema.TargetSchemaDigest}");

        foreach (MigrationSchemaDifferenceEvidence difference in normalized.Schema.Differences)
        {
            Line(
                text,
                $"  schema-difference object={difference.ObjectId} kind={Token(difference.Kind)} source={difference.SourceDefinitionDigest ?? "-"} target={difference.TargetDefinitionDigest ?? "-"}");
        }

        foreach (MigrationObjectValidationEvidence item in normalized.Objects)
        {
            Line(
                text,
                $"Object {item.SourceObjectId} -> {item.TargetObjectId}: {Token(item.Status)} sourceRows={Count(item.SourceRowCount)} targetRows={Count(item.TargetRowCount)} sourceChecksum={item.SourceChecksum ?? "-"} targetChecksum={item.TargetChecksum ?? "-"}");
            foreach (MigrationValidationPartitionEvidence partition in item.Partitions
                         .Where(partition => partition.Status != MigrationValidationStatus.Passed ||
                             partition.Mismatches.Count > 0))
            {
                Line(
                    text,
                    $"  partition {partition.PartitionId.ToString("D3", CultureInfo.InvariantCulture)}: {Token(partition.Status)} sourceRows={partition.SourceRowCount.ToString(CultureInfo.InvariantCulture)} targetRows={partition.TargetRowCount.ToString(CultureInfo.InvariantCulture)} sourceDigest={partition.SourceDigest} targetDigest={partition.TargetDigest}");
                foreach (MigrationValidationMismatchEvidence mismatch in partition.Mismatches)
                {
                    Line(
                        text,
                        $"    mismatch kind={Token(mismatch.Kind)} key={mismatch.KeyHash ?? "-"} sourceRow={mismatch.SourceRowHash ?? "-"} targetRow={mismatch.TargetRowHash ?? "-"} sourceMultiplicity={mismatch.SourceMultiplicity.ToString(CultureInfo.InvariantCulture)} targetMultiplicity={mismatch.TargetMultiplicity.ToString(CultureInfo.InvariantCulture)}");
                }
            }
        }

        foreach (MigrationValidationDiagnosticEvidence diagnostic in normalized.Diagnostics)
        {
            Line(
                text,
                $"Diagnostic {diagnostic.DiagnosticId}: rule={diagnostic.RuleId} severity={Token(diagnostic.Severity)} status={Token(diagnostic.Status)} object={diagnostic.ObjectId ?? "-"} partition={(diagnostic.PartitionId?.ToString(CultureInfo.InvariantCulture) ?? "-")}");
        }

        return text.ToString();
    }

    private static string Count(long? value) =>
        value?.ToString(CultureInfo.InvariantCulture) ?? "-";

    private static string Token<T>(T value) where T : struct, Enum =>
        value.ToString().ToLowerInvariant();

    private static void Line(StringBuilder text, string value) => text.Append(value).Append('\n');
}
