using CSharpDB.Observability;

namespace CSharpDB.Admin.Models;

internal sealed record AdminObservabilityScopeOption(
    string? Value,
    string Label,
    DiagnosticsAvailability Availability);

internal sealed record AdminObservabilityValue<T>(
    DiagnosticsAvailability Availability,
    T? Value,
    bool FieldsTruncated,
    string StatusText)
    where T : class;

internal sealed record AdminObservabilityCollection<T>(
    DiagnosticsAvailability Availability,
    IReadOnlyList<T> Records,
    bool IsTruncated,
    long DroppedCount,
    string StatusText)
    where T : class
{
    public bool FieldsTruncated { get; init; }
}

internal sealed record AdminObservabilityMetricSample(
    DateTimeOffset CapturedAtUtc,
    double? QueryRatePerSecond,
    double? ErrorRatePerSecond,
    double? AverageLatencyMilliseconds,
    double? WalGrowthBytesPerSecond);

internal sealed record AdminObservabilityViewState
{
    public bool IsLoading { get; init; }
    public bool IsRefreshing { get; init; }
    public bool IsPaused { get; init; }
    public bool IsStale { get; init; }
    public TimeSpan RefreshInterval { get; init; }
    public TimeSpan MaximumRefreshInterval { get; init; }
    public DateTimeOffset? LastSuccessfulRefreshUtc { get; init; }
    public DateTimeOffset? SnapshotCapturedAtUtc { get; init; }
    public TimeSpan? SnapshotAge { get; init; }
    public string StatusText { get; init; } = "Inactive";
    public bool HasUnavailableSections { get; init; }
    public string? SelectedScope { get; init; }
    public string? ScopeNotice { get; init; }
    public int? ShardCapacity { get; init; }
    public long DroppedShardCount { get; init; }
    public bool ShardsTruncated { get; init; }
    public IReadOnlyList<AdminObservabilityScopeOption> ScopeOptions { get; init; } =
        Array.Empty<AdminObservabilityScopeOption>();
    public AdminObservabilityValue<RuntimeDiagnosticsSnapshot> Runtime { get; init; } =
        EmptyValue<RuntimeDiagnosticsSnapshot>();
    public AdminObservabilityValue<StorageRuntimeDiagnosticsSnapshot> Storage { get; init; } =
        EmptyValue<StorageRuntimeDiagnosticsSnapshot>();
    public AdminObservabilityValue<WalRuntimeDiagnosticsSnapshot> Wal { get; init; } =
        EmptyValue<WalRuntimeDiagnosticsSnapshot>();
    public AdminObservabilityCollection<ActiveQuerySnapshot> ActiveQueries { get; init; } =
        EmptyCollection<ActiveQuerySnapshot>();
    public AdminObservabilityCollection<RecentQuerySnapshot> RecentQueries { get; init; } =
        EmptyCollection<RecentQuerySnapshot>();
    public AdminObservabilityCollection<SessionDiagnosticsSnapshot> Sessions { get; init; } =
        EmptyCollection<SessionDiagnosticsSnapshot>();
    public AdminObservabilityCollection<MaintenanceOperationSnapshot> ActiveMaintenance { get; init; } =
        EmptyCollection<MaintenanceOperationSnapshot>();
    public AdminObservabilityCollection<MaintenanceOperationSnapshot> RecentMaintenance { get; init; } =
        EmptyCollection<MaintenanceOperationSnapshot>();
    public AdminObservabilityValue<QueryPlanDiagnosticsSnapshot> SelectedPlan { get; init; } =
        EmptyValue<QueryPlanDiagnosticsSnapshot>(DiagnosticsAvailability.NotApplicable, "Not requested");
    public bool HasPlanRequest { get; init; }
    public AdminObservabilityValue<QueryDetailSnapshot> RevealedDetail { get; init; } =
        EmptyValue<QueryDetailSnapshot>(DiagnosticsAvailability.NotApplicable, "Not requested");
    public bool HasDetailRequest { get; init; }
    public IReadOnlyList<AdminObservabilityMetricSample> Samples { get; init; } =
        Array.Empty<AdminObservabilityMetricSample>();

    internal static AdminObservabilityValue<T> EmptyValue<T>(
        DiagnosticsAvailability availability = DiagnosticsAvailability.Unavailable,
        string statusText = "Not loaded") where T : class
        => new(availability, null, false, statusText);

    internal static AdminObservabilityCollection<T> EmptyCollection<T>(
        DiagnosticsAvailability availability = DiagnosticsAvailability.Unavailable,
        string statusText = "Not loaded") where T : class
        => new(availability, Array.Empty<T>(), false, 0, statusText);
}
