using System.Text.Json;
using CSharpDB.Observability;
using CSharpDB.Sql;

const string secret = "observability-native-aot-canary";

var options = new CSharpDbObservabilityOptions
{
    Enabled = true,
    DatabaseAlias = "native-aot-smoke",
};
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

if (!result.Fingerprint.Value.StartsWith(
        QueryFingerprint.Algorithm + ":",
        StringComparison.Ordinal))
{
    throw new InvalidOperationException("The SQL fingerprint did not use the versioned contract.");
}

if (result.NormalizedText.Contains(secret, StringComparison.Ordinal) ||
    fingerprintJson.Contains(secret, StringComparison.Ordinal) ||
    optionsJson.Contains(secret, StringComparison.Ordinal) ||
    activeQueriesJson.Contains(secret, StringComparison.Ordinal))
{
    throw new InvalidOperationException("Sensitive SQL literal content escaped normalization.");
}

if (!activeQueriesJson.Contains(CSharpDbDiagnostics.SchemaVersion, StringComparison.Ordinal))
    throw new InvalidOperationException("The bounded active-query envelope was not serialized.");

if (JsonSerializer.Deserialize(
        optionsJson,
        CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions) is null ||
    JsonSerializer.Deserialize(
        fingerprintJson,
        CSharpDbObservabilityJsonContext.Default.QueryFingerprintResult) is null ||
    JsonSerializer.Deserialize(
        hostJson,
        CSharpDbObservabilityJsonContext.Default.CSharpDbHostStateSnapshot) is null)
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
