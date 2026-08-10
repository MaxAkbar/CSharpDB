using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace CSharpDB.Observability;

[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    GenerationMode = JsonSourceGenerationMode.Metadata,
    UseStringEnumConverter = true)]
[JsonSerializable(typeof(CSharpDbObservabilityOptions))]
[JsonSerializable(typeof(CSharpDbLoggingOptions))]
[JsonSerializable(typeof(CSharpDbHistoryOptions))]
[JsonSerializable(typeof(CSharpDbOpenTelemetryOptions))]
[JsonSerializable(typeof(CSharpDbOtlpOptions))]
[JsonSerializable(typeof(CSharpDbPrometheusOptions))]
[JsonSerializable(typeof(CSharpDbHealthOptions))]
[JsonSerializable(typeof(QueryFingerprint))]
[JsonSerializable(typeof(QueryFingerprintResult))]
[JsonSerializable(typeof(OpaqueDiagnosticsId))]
[JsonSerializable(typeof(DiagnosticsTraceId))]
[JsonSerializable(typeof(SafeErrorProjection))]
[JsonSerializable(typeof(DiagnosticsSnapshotMetadata))]
[JsonSerializable(typeof(RuntimeDiagnosticsSnapshot))]
[JsonSerializable(typeof(QueryDiagnosticsSummary))]
[JsonSerializable(typeof(ActiveQuerySnapshot))]
[JsonSerializable(typeof(ActiveQuerySnapshot[]))]
[JsonSerializable(typeof(BoundedDiagnosticsSnapshot<ActiveQuerySnapshot>))]
[JsonSerializable(typeof(RecentQuerySnapshot))]
[JsonSerializable(typeof(RecentQuerySnapshot[]))]
[JsonSerializable(typeof(BoundedDiagnosticsSnapshot<RecentQuerySnapshot>))]
[JsonSerializable(typeof(QueryDetailSnapshot))]
[JsonSerializable(typeof(QueryPlanDiagnosticsSnapshot))]
[JsonSerializable(typeof(ConnectionDiagnosticsSnapshot))]
[JsonSerializable(typeof(SessionDiagnosticsSnapshot))]
[JsonSerializable(typeof(SessionDiagnosticsSnapshot[]))]
[JsonSerializable(typeof(BoundedDiagnosticsSnapshot<SessionDiagnosticsSnapshot>))]
[JsonSerializable(typeof(StorageRuntimeDiagnosticsSnapshot))]
[JsonSerializable(typeof(WalRuntimeDiagnosticsSnapshot))]
[JsonSerializable(typeof(MaintenanceOperationSnapshot))]
[JsonSerializable(typeof(MaintenanceOperationSnapshot[]))]
[JsonSerializable(typeof(BoundedDiagnosticsSnapshot<MaintenanceOperationSnapshot>))]
[JsonSerializable(typeof(HealthDiagnosticsSnapshot))]
[JsonSerializable(typeof(CSharpDbHostStateSnapshot))]
[JsonSerializable(typeof(DiagnosticsSection<QueryDiagnosticsSummary>))]
[JsonSerializable(typeof(DiagnosticsSection<ConnectionDiagnosticsSnapshot>))]
[JsonSerializable(typeof(DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>))]
[JsonSerializable(typeof(DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>))]
[JsonSerializable(typeof(DiagnosticsSection<MaintenanceOperationSnapshot>))]
[JsonSerializable(typeof(DiagnosticsSection<HealthDiagnosticsSnapshot>))]
public sealed partial class CSharpDbObservabilityJsonContext : JsonSerializerContext
{
    // The .NET 10 string-enum reader uses Regex on its deserialization path.
    // Preserve that optional framework dependency when this context is used
    // from a fully trimmed managed application.
    [DynamicDependency(nameof(Regex.InfiniteMatchTimeout), typeof(Regex))]
    static CSharpDbObservabilityJsonContext()
    {
    }
}
