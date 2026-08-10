using System.Text.Json;
using CSharpDB.Observability;
using CSharpDB.Sql;

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
string activeQueriesJson = JsonSerializer.Serialize(
    activeQueries,
    CSharpDbObservabilityJsonContext.Default.GetTypeInfo(
        typeof(BoundedDiagnosticsSnapshot<ActiveQuerySnapshot>))!);
var hostState = new CSharpDbHostState();
CSharpDbHostStateSnapshot failedHost = hostState.MarkFailed(
    SafeErrorProjector.Project(SafeErrorKind.DatabaseIo));
string hostJson = JsonSerializer.Serialize(
    failedHost,
    CSharpDbObservabilityJsonContext.Default.CSharpDbHostStateSnapshot);
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
    queryEventJson.Contains(secret, StringComparison.Ordinal))
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

Console.WriteLine("Observability source-generated JSON and SQL fingerprint trim/NativeAOT smoke passed.");
