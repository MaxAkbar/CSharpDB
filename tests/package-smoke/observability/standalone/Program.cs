using System.Text.Json;
using CSharpDB.Observability;

var options = new CSharpDbObservabilityOptions
{
    Enabled = true,
    DatabaseAlias = "package-smoke",
};
options.Validate();

var fingerprint = new QueryFingerprint(
    $"{QueryFingerprint.Algorithm}:{new string('a', 64)}");
var metadata = DiagnosticsSnapshotMetadata.Create(
    serverInstanceId: new string('b', 32),
    counterEpoch: 7,
    scope: DiagnosticsScope.Instance,
    availability: DiagnosticsAvailability.Available,
    source: DiagnosticsSource.Client,
    databaseAlias: "package-smoke");
var health = new HealthDiagnosticsSnapshot(
    metadata,
    CSharpDbHostLifecyclePhase.Running,
    CSharpDbHealthStatus.Healthy,
    CSharpDbHealthStatus.Healthy,
    CSharpDbReadinessReason.None,
    metadata.CapturedAtUtc,
    Error: null);
var runtime = new RuntimeDiagnosticsSnapshot(
    metadata,
    DiagnosticsSection<QueryDiagnosticsSummary>.WithoutValue(
        DiagnosticsAvailability.Disabled),
    DiagnosticsSection<ConnectionDiagnosticsSnapshot>.WithoutValue(
        DiagnosticsAvailability.NotApplicable),
    DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.WithoutValue(
        DiagnosticsAvailability.Unavailable),
    DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.WithoutValue(
        DiagnosticsAvailability.Unavailable),
    DiagnosticsSection<MaintenanceOperationSnapshot>.WithoutValue(
        DiagnosticsAvailability.Disabled),
    DiagnosticsSection<HealthDiagnosticsSnapshot>.Available(health));
var topology = new DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>(
    runtime,
    shards: null,
    shardCapacity: null,
    droppedShardCount: null,
    shardsTruncated: null);

string optionsJson = JsonSerializer.Serialize(
    options,
    CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions);
string fingerprintJson = JsonSerializer.Serialize(
    fingerprint,
    CSharpDbObservabilityJsonContext.Default.QueryFingerprint);
string topologyJson = JsonSerializer.Serialize(
    topology,
    CSharpDbObservabilityJsonContext.Default
        .DiagnosticsTopologySnapshotRuntimeDiagnosticsSnapshot);

if (!optionsJson.Contains("package-smoke", StringComparison.Ordinal) ||
    !fingerprintJson.Contains(QueryFingerprint.Algorithm, StringComparison.Ordinal) ||
    topology.Metadata != metadata ||
    topology.Aggregate.Health.Value?.Readiness != CSharpDbHealthStatus.Healthy ||
    !topologyJson.Contains("package-smoke", StringComparison.Ordinal) ||
    topologyJson.Contains("Data Source", StringComparison.OrdinalIgnoreCase))
{
    throw new InvalidOperationException("The standalone observability package contract was not usable.");
}

Console.WriteLine("Standalone CSharpDB.Observability package smoke passed.");
