using System.Collections.Frozen;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace CSharpDB.Observability;

/// <summary>
/// Stable source names and schema constants for CSharpDB instrumentation.
/// </summary>
public static class CSharpDbDiagnostics
{
    public const string SchemaVersion = "1.0";
    public const string InstrumentationVersion = "1.0.0";
    public const string ActivitySourceName = "CSharpDB";
    public const string MeterName = "CSharpDB";
    public const int MaximumDatabaseAliasLength = 64;
    public const int MaximumConfiguredDatabaseAliases = 64;
    public const int OpaqueIdentifierHexLength = 32;

    public static ActivitySource ActivitySource { get; } =
        new(ActivitySourceName, InstrumentationVersion);

    public static Meter Meter { get; } =
        new(MeterName, InstrumentationVersion);

    public static string CreateServerInstanceId()
        => Guid.NewGuid().ToString("N");

    public static string CreateOpaqueIdentifier()
        => Guid.NewGuid().ToString("N");

    public static bool IsValidOpaqueIdentifier(string? value)
    {
        if (value is null || value.Length != OpaqueIdentifierHexLength)
            return false;

        foreach (char character in value)
        {
            if (character is not (>= '0' and <= '9') and
                not (>= 'a' and <= 'f'))
            {
                return false;
            }
        }

        return true;
    }
}

/// <summary>
/// Stable counter semantics. Production cumulative counters never reset;
/// resettable diagnostic/benchmark families must advance CounterEpoch first.
/// </summary>
public static class CSharpDbCounterSemantics
{
    public static FrozenDictionary<string, CounterSemantics> Instruments { get; } =
        new Dictionary<string, CounterSemantics>(StringComparer.Ordinal)
        {
            ["csharpdb.requests"] = CounterSemantics.Cumulative,
            ["csharpdb.statements"] = CounterSemantics.Cumulative,
            ["csharpdb.rows.produced"] = CounterSemantics.Cumulative,
            ["csharpdb.rows.affected"] = CounterSemantics.Cumulative,
            ["csharpdb.storage.bytes.read"] = CounterSemantics.Cumulative,
            ["csharpdb.storage.bytes.written"] = CounterSemantics.Cumulative,
            ["csharpdb.queries.active"] = CounterSemantics.Gauge,
            ["csharpdb.connections.available"] = CounterSemantics.Gauge,
            ["csharpdb.wal.logical_bytes"] = CounterSemantics.Gauge,
            ["csharpdb.wal.frame_count"] = CounterSemantics.Gauge,
        }.ToFrozenDictionary(StringComparer.Ordinal);
}

/// <summary>
/// The complete allowlist of metric tag keys. Operation identifiers, query
/// fingerprints, SQL, object names, sessions, paths, and error messages are
/// intentionally absent.
/// </summary>
public static class CSharpDbMetricTagNames
{
    public const string OperationClass = "csharpdb.operation.class";
    public const string Outcome = "csharpdb.operation.outcome";
    public const string Transport = "csharpdb.transport";
    public const string DatabaseAlias = "csharpdb.database.alias";
    public const string CheckKind = "csharpdb.health.check";
    public const string Status = "csharpdb.status";

    public static FrozenSet<string> Allowed { get; } = new[]
    {
        OperationClass,
        Outcome,
        Transport,
        DatabaseAlias,
        CheckKind,
        Status,
    }.ToFrozenSet(StringComparer.Ordinal);

    public static bool IsAllowed(string tagName)
        => Allowed.Contains(tagName);

    public static bool IsAllowedValue(string tagName, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return false;

        return tagName switch
        {
            OperationClass => IsDefined<CSharpDbOperationClass>(value, CSharpDbOperationClass.Unknown),
            Outcome => IsDefined<CSharpDbOperationOutcome>(value, CSharpDbOperationOutcome.Unknown),
            Transport => IsDefined<CSharpDbTransport>(value, CSharpDbTransport.Unknown),
            DatabaseAlias => CSharpDbObservabilityOptions.IsValidDatabaseAlias(value),
            CheckKind => IsDefined<CSharpDbHealthCheckKind>(value, CSharpDbHealthCheckKind.Unknown),
            Status => IsDefined<CSharpDbHealthStatus>(value, CSharpDbHealthStatus.Unknown),
            _ => false,
        };
    }

    private static bool IsDefined<TEnum>(string value, TEnum unknown)
        where TEnum : struct, Enum
        => Enum.TryParse(value, ignoreCase: true, out TEnum parsed) &&
           Enum.IsDefined(parsed) &&
           !EqualityComparer<TEnum>.Default.Equals(parsed, unknown);
}

/// <summary>
/// Reserved structured-log event identifiers. Each subsystem owns a range of
/// one hundred identifiers so future additions do not renumber existing events.
/// </summary>
public static class CSharpDbLogEventIds
{
    public const int HostStarting = 1000;
    public const int DatabaseOpened = 1001;
    public const int DatabaseClosed = 1002;

    public const int QueryCompleted = 2000;
    public const int SlowQuery = 2001;
    public const int QueryFailed = 2002;
    public const int QueryCanceled = 2003;

    public const int TransactionCompleted = 3000;

    public const int CheckpointCompleted = 4000;
    public const int RecoveryCompleted = 4001;

    public const int BackupCompleted = 5000;
    public const int RestoreCompleted = 5001;
    public const int MaintenanceCompleted = 5002;

    public const int HealthTransition = 6000;

    public const int ApiRequestRejected = 7000;
    public const int ApiUnhandledError = 7001;
}
