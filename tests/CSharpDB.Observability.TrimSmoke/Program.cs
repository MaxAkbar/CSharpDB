using System.Text.Json;
using System.Diagnostics.Metrics;
using CSharpDB.Observability;
using CSharpDB.Sql;
using CSharpDB.Testing;

const string secret = "observability-native-aot-canary";

var options = new CSharpDbObservabilityOptions
{
    Enabled = true,
    DatabaseAlias = "native-aot-smoke",
};
options.Logging.SlowQueryThresholdOverrides[CSharpDbOperationClass.Query] =
    TimeSpan.FromMilliseconds(250);
options.Validate();

QueryFingerprintResult result = SqlQueryNormalizer.NormalizeAndFingerprint(
    $"SELECT value FROM observations WHERE secret = '{secret}' AND id = 42");

string optionsJson = JsonSerializer.Serialize(
    options,
    CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions);
string fingerprintJson = JsonSerializer.Serialize(
    result,
    CSharpDbObservabilityJsonContext.Default.QueryFingerprintResult);
DiagnosticsSnapshotMetadata metadata = DiagnosticsSnapshotMetadata.Create(
    CSharpDbDiagnostics.CreateServerInstanceId(),
    counterEpoch: 0,
    DiagnosticsScope.Instance,
    DiagnosticsAvailability.Available,
    DiagnosticsSource.Engine,
    databaseAlias: "native-aot-smoke");
var activeQuery = new ActiveQuerySnapshot(
    metadata,
    OpaqueDiagnosticsId.Create(),
    ParentOperationId: null,
    CSharpDbOperationClass.Query,
    CSharpDbOperationRole.Root,
    QueryExecutionPhase.Executing,
    DateTimeOffset.UtcNow,
    TimeSpan.FromMilliseconds(1),
    result.Fingerprint,
    CSharpDbTransport.Direct,
    TraceId: new DiagnosticsTraceId("0123456789abcdef0123456789abcdef"),
    SessionId: null);
var activeQueries = new BoundedDiagnosticsSnapshot<ActiveQuerySnapshot>(
    [activeQuery],
    droppedCount: 0,
    isTruncated: false);
var activeQueryCollection = new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
    metadata,
    [activeQuery],
    capacity: 1_000,
    retention: null,
    droppedCount: 0,
    isTruncated: false);
string activeQueriesJson = JsonSerializer.Serialize(
    activeQueries,
    CSharpDbObservabilityJsonContext.Default.GetTypeInfo(
        typeof(BoundedDiagnosticsSnapshot<ActiveQuerySnapshot>))!);
string activeQueryCollectionJson = JsonSerializer.Serialize(
    activeQueryCollection,
    CSharpDbObservabilityJsonContext.Default.GetTypeInfo(
        typeof(DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>))!);
var planValue = new DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>(
    metadata,
    new QueryPlanDiagnosticsSnapshot(
        metadata,
        activeQuery.OperationId,
        result.Fingerprint,
        QueryAccessPathCategory.PrimaryKeyLookup,
        PlanCacheHit: true,
        Reoptimized: false,
        EstimatedRows: 1,
        ActualRows: 1,
        PlanNodeCount: null,
        PlanTruncated: false));
string planValueJson = JsonSerializer.Serialize(
    planValue,
    CSharpDbObservabilityJsonContext.Default
        .DiagnosticsValueSnapshotQueryPlanDiagnosticsSnapshot);
var queryDetail = new QueryDetailSnapshot(
    metadata,
    activeQuery.OperationId,
    result.Fingerprint,
    SqlTextCaptureMode.Normalized,
    result.NormalizedText);
var queryDetailValue = new DiagnosticsValueSnapshot<QueryDetailSnapshot>(
    metadata,
    queryDetail);
var queryDetailTopology = new DiagnosticsTopologySnapshot<
    DiagnosticsValueSnapshot<QueryDetailSnapshot>>(
    queryDetailValue,
    shards: null,
    shardCapacity: null,
    droppedShardCount: null,
    shardsTruncated: null);
string queryDetailTopologyJson = JsonSerializer.Serialize(
    queryDetailTopology,
    CSharpDbObservabilityJsonContext.Default
        .DiagnosticsTopologySnapshotDiagnosticsValueSnapshotQueryDetailSnapshot);
DiagnosticsTransportFixture transportFixture = DiagnosticsTransportFixture.Create();
string runtimeTopologyJson = RoundTrip(
    transportFixture.Runtime,
    CSharpDbObservabilityJsonContext.Default
        .DiagnosticsTopologySnapshotRuntimeDiagnosticsSnapshot,
    "runtime topology");
string storageTopologyJson = RoundTrip(
    transportFixture.Storage,
    CSharpDbObservabilityJsonContext.Default
        .DiagnosticsTopologySnapshotDiagnosticsValueSnapshotStorageRuntimeDiagnosticsSnapshot,
    "storage topology");
string walTopologyJson = RoundTrip(
    transportFixture.Wal,
    CSharpDbObservabilityJsonContext.Default
        .DiagnosticsTopologySnapshotDiagnosticsValueSnapshotWalRuntimeDiagnosticsSnapshot,
    "WAL topology");
string activeTopologyJson = RoundTrip(
    transportFixture.ActiveQueries,
    CSharpDbObservabilityJsonContext.Default
        .DiagnosticsTopologySnapshotDiagnosticsCollectionSnapshotActiveQuerySnapshot,
    "active-query topology");
string recentTopologyJson = RoundTrip(
    transportFixture.RecentQueries,
    CSharpDbObservabilityJsonContext.Default
        .DiagnosticsTopologySnapshotDiagnosticsCollectionSnapshotRecentQuerySnapshot,
    "recent-query topology");
string planTopologyJson = RoundTrip(
    transportFixture.QueryPlan,
    CSharpDbObservabilityJsonContext.Default
        .DiagnosticsTopologySnapshotDiagnosticsValueSnapshotQueryPlanDiagnosticsSnapshot,
    "query-plan topology");
string sessionTopologyJson = RoundTrip(
    transportFixture.Sessions,
    CSharpDbObservabilityJsonContext.Default
        .DiagnosticsTopologySnapshotDiagnosticsCollectionSnapshotSessionDiagnosticsSnapshot,
    "session topology");
string activeMaintenanceTopologyJson = RoundTrip(
    transportFixture.ActiveMaintenance,
    CSharpDbObservabilityJsonContext.Default
        .DiagnosticsTopologySnapshotDiagnosticsCollectionSnapshotMaintenanceOperationSnapshot,
    "active-maintenance topology");
string recentMaintenanceTopologyJson = RoundTrip(
    transportFixture.RecentMaintenance,
    CSharpDbObservabilityJsonContext.Default
        .DiagnosticsTopologySnapshotDiagnosticsCollectionSnapshotMaintenanceOperationSnapshot,
    "recent-maintenance topology");
string fullQueryDetailTopologyJson = RoundTrip(
    transportFixture.QueryDetail,
    CSharpDbObservabilityJsonContext.Default
        .DiagnosticsTopologySnapshotDiagnosticsValueSnapshotQueryDetailSnapshot,
    "query-detail topology");
var hostState = new CSharpDbHostState();
CSharpDbHostStateSnapshot failedHost = hostState.MarkFailed(
    SafeErrorProjector.Project(SafeErrorKind.DatabaseIo));
string hostJson = JsonSerializer.Serialize(
    failedHost,
    CSharpDbObservabilityJsonContext.Default.CSharpDbHostStateSnapshot);
hostState.MarkRecovering();
CSharpDbHostStateSnapshot readOnlyHost = hostState.MarkRunning(
    CSharpDbReadinessReason.ReadOnly);
using CSharpDbHealthMetricSource healthMetricSource =
    CSharpDbHealthMetricSource.TryCreate(hostState, "native-aot-smoke") ??
    throw new InvalidOperationException(
        "The health metric source could not be registered.");
var healthTransitionEvent = new CSharpDbHealthTransitionEvent(readOnlyHost);
string healthTransitionJson = JsonSerializer.Serialize(
    healthTransitionEvent,
    CSharpDbObservabilityJsonContext.Default.CSharpDbHealthTransitionEvent);
var operation = CSharpDbOperationContext.CreateRoot(
    CSharpDbOperationClass.Query,
    CSharpDbTransport.Direct,
    "native-aot-smoke",
    queryFingerprint: result.Fingerprint);
var queryEvent = new CSharpDbQueryFailedEvent(
    operation,
    DateTimeOffset.UtcNow,
    totalDuration: TimeSpan.FromMilliseconds(500),
    timeToFirstResult: null,
    queueDuration: TimeSpan.FromMilliseconds(10),
    executionAndConsumptionDuration: TimeSpan.FromMilliseconds(100),
    rowsProduced: 0,
    rowsAffected: 0,
    SafeErrorProjector.Project(SafeErrorKind.DatabaseOperation));
string queryEventJson = JsonSerializer.Serialize(
    queryEvent,
    CSharpDbObservabilityJsonContext.Default.CSharpDbQueryFailedEvent);
var longRunningEvent = new CSharpDbLongRunningQueryEvent(
    operation,
    DateTimeOffset.UtcNow,
    elapsed: TimeSpan.FromSeconds(1),
    longRunningQueryThreshold: TimeSpan.FromMilliseconds(500),
    QueryExecutionPhase.Executing);
string longRunningEventJson = JsonSerializer.Serialize(
    longRunningEvent,
    CSharpDbObservabilityJsonContext.Default.CSharpDbLongRunningQueryEvent);

OpaqueDiagnosticsId boundarySessionId = OpaqueDiagnosticsId.Create();
using (CSharpDbOperationScope.EnterBoundary(
           CSharpDbTransport.Grpc,
           boundarySessionId))
{
    if (CSharpDbOperationScope.Current is not null ||
        CSharpDbOperationScope.CurrentTransport != CSharpDbTransport.Grpc ||
        CSharpDbOperationScope.CurrentSessionId != boundarySessionId)
    {
        throw new InvalidOperationException("The ambient boundary operation scope was not established.");
    }

    using (CSharpDbOperationScope.SuppressDiagnostics())
    {
        if (!CSharpDbOperationScope.IsDiagnosticsSuppressed)
            throw new InvalidOperationException("Diagnostics suppression was not established.");
    }
}

if (!result.Fingerprint.Value.StartsWith(
        QueryFingerprint.Algorithm + ":",
        StringComparison.Ordinal))
{
    throw new InvalidOperationException("The SQL fingerprint did not use the versioned contract.");
}

if (result.NormalizedText.Contains(secret, StringComparison.Ordinal) ||
    fingerprintJson.Contains(secret, StringComparison.Ordinal) ||
    optionsJson.Contains(secret, StringComparison.Ordinal) ||
    activeQueriesJson.Contains(secret, StringComparison.Ordinal) ||
    activeQueryCollectionJson.Contains(secret, StringComparison.Ordinal) ||
    planValueJson.Contains(secret, StringComparison.Ordinal) ||
    queryDetailTopologyJson.Contains(secret, StringComparison.Ordinal) ||
    runtimeTopologyJson.Contains(secret, StringComparison.Ordinal) ||
    storageTopologyJson.Contains(secret, StringComparison.Ordinal) ||
    walTopologyJson.Contains(secret, StringComparison.Ordinal) ||
    activeTopologyJson.Contains(secret, StringComparison.Ordinal) ||
    recentTopologyJson.Contains(secret, StringComparison.Ordinal) ||
    planTopologyJson.Contains(secret, StringComparison.Ordinal) ||
    sessionTopologyJson.Contains(secret, StringComparison.Ordinal) ||
    activeMaintenanceTopologyJson.Contains(secret, StringComparison.Ordinal) ||
    recentMaintenanceTopologyJson.Contains(secret, StringComparison.Ordinal) ||
    fullQueryDetailTopologyJson.Contains(secret, StringComparison.Ordinal) ||
    healthTransitionJson.Contains(secret, StringComparison.Ordinal) ||
    queryEventJson.Contains(secret, StringComparison.Ordinal) ||
    longRunningEventJson.Contains(secret, StringComparison.Ordinal))
{
    throw new InvalidOperationException("Sensitive SQL literal content escaped normalization.");
}

if (!activeQueriesJson.Contains(CSharpDbDiagnostics.SchemaVersion, StringComparison.Ordinal))
    throw new InvalidOperationException("The bounded active-query envelope was not serialized.");

if (!queryEventJson.Contains("csharpdb.operation_failed", StringComparison.Ordinal) ||
    !queryEventJson.Contains(result.Fingerprint.Value, StringComparison.Ordinal))
{
    throw new InvalidOperationException("The typed query event was not source-generated safely.");
}

if (!longRunningEventJson.Contains("Executing", StringComparison.Ordinal) ||
    longRunningEventJson.Contains("capturedSql", StringComparison.OrdinalIgnoreCase))
{
    throw new InvalidOperationException("The long-running query event was not source-generated safely.");
}

if (JsonSerializer.Deserialize(
        optionsJson,
        CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions) is null ||
    JsonSerializer.Deserialize(
        fingerprintJson,
        CSharpDbObservabilityJsonContext.Default.QueryFingerprintResult) is null ||
    JsonSerializer.Deserialize(
        hostJson,
        CSharpDbObservabilityJsonContext.Default.CSharpDbHostStateSnapshot) is null ||
    JsonSerializer.Deserialize(
        healthTransitionJson,
        CSharpDbObservabilityJsonContext.Default.CSharpDbHealthTransitionEvent)?
        .State.ReadinessReason != CSharpDbReadinessReason.ReadOnly ||
    JsonSerializer.Deserialize(
        optionsJson,
        CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions)?
        .Logging.GetSlowQueryThreshold(CSharpDbOperationClass.Query) != TimeSpan.FromMilliseconds(250))
{
    throw new InvalidOperationException("A source-generated observability contract did not round-trip.");
}

var activeQueriesTypeInfo = CSharpDbObservabilityJsonContext.Default.GetTypeInfo(
    typeof(BoundedDiagnosticsSnapshot<ActiveQuerySnapshot>))!;
if (JsonSerializer.Deserialize(activeQueriesJson, activeQueriesTypeInfo) is not
    BoundedDiagnosticsSnapshot<ActiveQuerySnapshot> deserializedActiveQueries ||
    deserializedActiveQueries.Records.Count != 1 ||
    deserializedActiveQueries.Records[0].TraceId?.Value !=
        "0123456789abcdef0123456789abcdef")
{
    throw new InvalidOperationException("The bounded active-query contract did not round-trip.");
}

var activeQueryCollectionTypeInfo = CSharpDbObservabilityJsonContext.Default.GetTypeInfo(
    typeof(DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>))!;
if (JsonSerializer.Deserialize(activeQueryCollectionJson, activeQueryCollectionTypeInfo) is not
    DiagnosticsCollectionSnapshot<ActiveQuerySnapshot> deserializedCollection ||
    deserializedCollection.Records?.Count != 1 ||
    deserializedCollection.Capacity != 1_000 ||
    deserializedCollection.Metadata.ServerInstanceId != metadata.ServerInstanceId)
{
    throw new InvalidOperationException("The identified diagnostics collection did not round-trip.");
}

if (JsonSerializer.Deserialize(
        planValueJson,
        CSharpDbObservabilityJsonContext.Default
            .DiagnosticsValueSnapshotQueryPlanDiagnosticsSnapshot) is not
    DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot> deserializedPlan ||
    deserializedPlan.Value?.OperationId != activeQuery.OperationId ||
    deserializedPlan.Metadata != deserializedPlan.Value.Metadata)
{
    throw new InvalidOperationException("The identified diagnostics value did not round-trip.");
}

if (JsonSerializer.Deserialize(
        queryDetailTopologyJson,
        CSharpDbObservabilityJsonContext.Default
            .DiagnosticsTopologySnapshotDiagnosticsValueSnapshotQueryDetailSnapshot) is not
    DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>
        deserializedDetailTopology ||
    deserializedDetailTopology.Aggregate.Value?.OperationId != activeQuery.OperationId ||
    deserializedDetailTopology.Aggregate.Value.CaptureMode != SqlTextCaptureMode.Normalized ||
    deserializedDetailTopology.Aggregate.Value.CapturedSqlText != result.NormalizedText ||
    CSharpDbObservabilityJsonContext.Default.GetTypeInfo(
        typeof(ShardDiagnosticsSection<RuntimeDiagnosticsSnapshot>)) is null)
{
    throw new InvalidOperationException(
        "The bounded query-detail topology did not round-trip through the trimmed context.");
}

bool meterInstrumentPublished = false;
bool meterMeasurementObserved = false;
bool healthGaugePublished = false;
bool healthGaugeObserved = false;
using (var meterListener = new MeterListener())
{
    const string canaryName = "csharpdb.trim_smoke.measurements";
    meterListener.InstrumentPublished = (instrument, listener) =>
    {
        if (instrument.Meter.Name == CSharpDbDiagnostics.MeterName &&
            instrument.Name == canaryName)
        {
            meterInstrumentPublished = true;
            listener.EnableMeasurementEvents(instrument);
        }
        else if (instrument.Meter.Name == CSharpDbDiagnostics.MeterName &&
                 instrument.Name == CSharpDbMetricInstrumentNames.HealthStatus)
        {
            healthGaugePublished = instrument is ObservableGauge<long> &&
                instrument.Unit == CSharpDbMetricUnits.Status;
            listener.EnableMeasurementEvents(instrument);
        }
    };
    meterListener.SetMeasurementEventCallback<long>(
        (instrument, measurement, tags, _) =>
        {
            if (instrument.Name == canaryName &&
                measurement == 1 &&
                tags.Length == 1 &&
                tags[0].Key == CSharpDbMetricTagNames.DatabaseAlias &&
                Equals(tags[0].Value, "native-aot-smoke"))
            {
                meterMeasurementObserved = true;
            }

            if (instrument.Name == CSharpDbMetricInstrumentNames.HealthStatus &&
                measurement == 1 &&
                HasTag(
                    tags,
                    CSharpDbMetricTagNames.CheckKind,
                    "readiness") &&
                HasTag(
                    tags,
                    CSharpDbMetricTagNames.Status,
                    "unhealthy") &&
                HasTag(
                    tags,
                    CSharpDbMetricTagNames.DatabaseAlias,
                    "native-aot-smoke"))
            {
                healthGaugeObserved = true;
            }
        });
    meterListener.Start();
    Counter<long> canary = CSharpDbDiagnostics.Meter.CreateCounter<long>(
        canaryName,
        "{measurement}",
        "Trim and NativeAOT MeterListener canary.");
    canary.Add(
        1,
        new KeyValuePair<string, object?>(
            CSharpDbMetricTagNames.DatabaseAlias,
            "native-aot-smoke"));
    meterListener.RecordObservableInstruments();
}

if (!meterInstrumentPublished || !meterMeasurementObserved ||
    !healthGaugePublished || !healthGaugeObserved)
{
    throw new InvalidOperationException(
        "The trimmed MeterListener and health gauge did not publish the expected measurements.");
}

Console.WriteLine("All diagnostics topologies, observability JSON, SQL fingerprint, health state, and MeterListener trim/NativeAOT smoke passed.");

static string RoundTrip<T>(
    T value,
    System.Text.Json.Serialization.Metadata.JsonTypeInfo<T> typeInfo,
    string contractName)
    where T : class
{
    string json = JsonSerializer.Serialize(value, typeInfo);
    T roundTrip = JsonSerializer.Deserialize(json, typeInfo) ??
        throw new InvalidOperationException(
            $"The source-generated {contractName} returned null after deserialization.");
    string roundTripJson = JsonSerializer.Serialize(roundTrip, typeInfo);
    if (!string.Equals(json, roundTripJson, StringComparison.Ordinal))
    {
        throw new InvalidOperationException(
            $"The source-generated {contractName} changed during round-trip serialization.");
    }

    return json;
}

static bool HasTag(
    ReadOnlySpan<KeyValuePair<string, object?>> tags,
    string name,
    string value)
{
    foreach (KeyValuePair<string, object?> tag in tags)
    {
        if (tag.Key == name && Equals(tag.Value, value))
            return true;
    }

    return false;
}
