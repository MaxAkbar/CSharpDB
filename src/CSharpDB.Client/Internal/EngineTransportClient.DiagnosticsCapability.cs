using CSharpDB.Engine;
using CSharpDB.Observability;

namespace CSharpDB.Client.Internal;

internal sealed partial class EngineTransportClient
{
    internal static Action<int>? RuntimeDiagnosticsCaptureCompletedForTests;

    public Task<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>
        GetRuntimeDiagnosticsAsync(CancellationToken ct = default)
    {
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> result =
            CaptureStableRuntimeDiagnosticsTopology(
                CreateRuntimeDiagnosticsTopology,
                static identity => CreateIdentityUnstableRuntimeTopology(identity),
                ct);
        return Task.FromResult(result);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<
        StorageRuntimeDiagnosticsSnapshot>>>
        GetStorageDiagnosticsAsync(CancellationToken ct = default)
    {
        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<
            StorageRuntimeDiagnosticsSnapshot>> result =
                CaptureStableRuntimeDiagnosticsTopology(
                    CreateStorageDiagnosticsTopology,
                    static identity => CreateIdentityUnstableValueTopology<
                        StorageRuntimeDiagnosticsSnapshot>(identity),
                    ct);
        return Task.FromResult(result);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<
        WalRuntimeDiagnosticsSnapshot>>>
        GetWalDiagnosticsAsync(CancellationToken ct = default)
    {
        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<
            WalRuntimeDiagnosticsSnapshot>> result =
                CaptureStableRuntimeDiagnosticsTopology(
                    CreateWalDiagnosticsTopology,
                    static identity => CreateIdentityUnstableValueTopology<
                        WalRuntimeDiagnosticsSnapshot>(identity),
                    ct);
        return Task.FromResult(result);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
        MaintenanceOperationSnapshot>>>
        GetActiveMaintenanceOperationsAsync(
            int maximumRecords,
            CancellationToken ct = default)
        => GetMaintenanceOperationsAsync(
            maximumRecords,
            recent: false,
            ct);

    public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
        MaintenanceOperationSnapshot>>>
        GetRecentMaintenanceOperationsAsync(
            int maximumRecords,
            CancellationToken ct = default)
        => GetMaintenanceOperationsAsync(
            maximumRecords,
            recent: true,
            ct);

    private Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
        MaintenanceOperationSnapshot>>>
        GetMaintenanceOperationsAsync(
            int maximumRecords,
            bool recent,
            CancellationToken ct)
    {
        ValidateDiagnosticsMaximumRecords(maximumRecords);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
            MaintenanceOperationSnapshot>> result =
                CaptureStableRuntimeDiagnosticsTopology(
                    families => CreateMaintenanceTopology(
                        families,
                        maximumRecords,
                        recent),
                    static identity => CreateIdentityUnstableCollectionTopology<
                        MaintenanceOperationSnapshot>(identity),
                    ct);
        return Task.FromResult(result);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>
        GetActiveQueriesAsync(int maximumRecords, CancellationToken ct = default)
    {
        ValidateDiagnosticsMaximumRecords(maximumRecords);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>> result =
            CaptureStableRuntimeDiagnosticsTopology(
                families => CreateActiveQueryTopology(families, maximumRecords),
                static identity => CreateIdentityUnstableCollectionTopology<
                    ActiveQuerySnapshot>(identity),
                ct);
        return Task.FromResult(result);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>>
        GetRecentQueriesAsync(int maximumRecords, CancellationToken ct = default)
    {
        ValidateDiagnosticsMaximumRecords(maximumRecords);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>> result =
            CaptureStableRuntimeDiagnosticsTopology(
                families => CreateRecentQueryTopology(families, maximumRecords),
                static identity => CreateIdentityUnstableCollectionTopology<
                    RecentQuerySnapshot>(identity),
                ct);
        return Task.FromResult(result);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>>
        GetQueryPlanDiagnosticsAsync(
            OpaqueDiagnosticsId operationId,
            CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operationId);
        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>> result =
            CaptureStableRuntimeDiagnosticsTopology(
                families => CreateQueryPlanTopology(families, operationId),
                static identity => CreateIdentityUnstableValueTopology<
                    QueryPlanDiagnosticsSnapshot>(identity),
                ct);
        return Task.FromResult(result);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>>
        GetSessionsAsync(int maximumRecords, CancellationToken ct = default)
    {
        ValidateDiagnosticsMaximumRecords(maximumRecords);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>> result =
            CaptureStableRuntimeDiagnosticsTopology(
                families => CreateSessionTopology(
                    families.Current,
                    maximumRecords),
                static identity => CreateIdentityUnstableCollectionTopology<
                    SessionDiagnosticsSnapshot>(identity),
                ct);
        return Task.FromResult(result);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>>
        GetQueryDetailAsync(
            OpaqueDiagnosticsId operationId,
            CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operationId);
        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>> result =
            CaptureStableRuntimeDiagnosticsTopology(
                families => CreateQueryDetailTopology(families, operationId),
                static identity => CreateIdentityUnstableValueTopology<
                    QueryDetailSnapshot>(identity),
                ct);
        return Task.FromResult(result);
    }

    private DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>
        CreateRuntimeDiagnosticsTopology(RuntimeDiagnosticsFamilyLease families)
    {
        if (!families.Current.IsEnabled &&
            families.States.Count == 1 &&
            ReferenceEquals(families.States[0], families.Current) &&
            families.DroppedCount == 0)
        {
            RuntimeDiagnosticsSnapshot disabled = CreateDisabledRuntimeSnapshot(
                families.Current,
                DiagnosticsScope.Instance,
                DiagnosticsSource.Client);
            return CreateInstanceTopology(disabled);
        }

        if (families.States.Count == 1 &&
            ReferenceEquals(families.States[0], families.Current) &&
            families.DroppedCount == 0)
        {
            ConnectionCapture connectionCapture = CaptureConnections();
            MaintenanceRuntimeDiagnosticsCapture maintenanceCapture =
                CaptureMaintenance(
                    families,
                    families.Current,
                    includeClientRegistry: true,
                    maximumActiveRecords: 1,
                    maximumRecentRecords: 0);
            bool maintenanceTruncated = IsMaintenanceCaptureTruncated(
                maintenanceCapture);
            DiagnosticsSnapshotMetadata provisionalMetadata =
                families.Current.CreateMetadata(
                DiagnosticsScope.Instance,
                DiagnosticsAvailability.Available,
                DiagnosticsSource.Client,
                recordsTruncated: maintenanceTruncated,
                fieldsTruncated: connectionCapture.FieldsTruncated);
            QueryDiagnosticsSummary raw = QueryRuntimeDiagnostics
                .GetOrCreate(families.Current)
                .GetSummary();
            StorageRuntimeDiagnosticsCapture storage =
                CaptureStorageWithDetailOverlay(
                    families,
                    families.Current,
                    provisionalMetadata);
            DiagnosticsSnapshotMetadata metadata = RebuildMetadata(
                provisionalMetadata,
                DiagnosticsAvailability.Available,
                maintenanceTruncated,
                connectionCapture.FieldsTruncated || storage.FieldsTruncated);
            QueryDiagnosticsSummary queries = raw with { Metadata = metadata };
            ConnectionDiagnosticsSnapshot connections = CreateConnectionSnapshot(
                metadata,
                connectionCapture);
            var snapshot = new RuntimeDiagnosticsSnapshot(
                metadata,
                DiagnosticsSection<QueryDiagnosticsSummary>.Available(queries),
                DiagnosticsSection<ConnectionDiagnosticsSnapshot>.Available(connections),
                ReprojectStorageSection(storage.Storage, metadata),
                ReprojectWalSection(storage.Wal, metadata),
                CreateMaintenanceSection(maintenanceCapture, metadata),
                DiagnosticsSection<HealthDiagnosticsSnapshot>.WithoutValue(
                    DiagnosticsAvailability.Unavailable));
            return CreateInstanceTopology(snapshot);
        }

        bool hasEnabledFamily = families.States.Any(static state => state.IsEnabled);
        RuntimeDiagnosticsFamilySection<RuntimeDiagnosticsSnapshot>[] exactFamilies =
            families.States
                .Select(state => CreateExactRuntimeSummaryFamily(families, state))
                .ToArray();
        if (!hasEnabledFamily)
        {
            RuntimeDiagnosticsSnapshot disabledAggregate = CreateDisabledRuntimeSnapshot(
                families.Current,
                DiagnosticsScope.Aggregate,
                DiagnosticsSource.Client);
            return CreateAggregateTopology(
                disabledAggregate,
                exactFamilies,
                families.DroppedCount);
        }

        ConnectionCapture aggregateConnections = CaptureConnections();
        MaintenanceRuntimeDiagnosticsCapture aggregateMaintenance =
            CaptureAggregateMaintenance(
                families,
                maximumActiveRecords: 1,
                maximumRecentRecords: 0);
        DiagnosticsSnapshotMetadata aggregateMetadata = families.Current.CreateMetadata(
            DiagnosticsScope.Aggregate,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Client,
            recordsTruncated: IsMaintenanceCaptureTruncated(
                aggregateMaintenance),
            fieldsTruncated: aggregateConnections.FieldsTruncated);
        var aggregateSnapshot = new RuntimeDiagnosticsSnapshot(
            aggregateMetadata,
            DiagnosticsSection<QueryDiagnosticsSummary>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<ConnectionDiagnosticsSnapshot>.Available(
                CreateConnectionSnapshot(aggregateMetadata, aggregateConnections)),
            DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            CreateMaintenanceSection(
                aggregateMaintenance,
                aggregateMetadata),
            DiagnosticsSection<HealthDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable));
        return CreateAggregateTopology(
            aggregateSnapshot,
            exactFamilies,
            families.DroppedCount);
    }

    private RuntimeDiagnosticsFamilySection<RuntimeDiagnosticsSnapshot>
        CreateExactRuntimeSummaryFamily(
            RuntimeDiagnosticsFamilyLease families,
            CSharpDbRuntimeDiagnosticsState state)
    {
        if (!state.IsEnabled)
        {
            return new RuntimeDiagnosticsFamilySection<RuntimeDiagnosticsSnapshot>(
                state.DatabaseAlias,
                CreateDisabledRuntimeSnapshot(
                    state,
                    DiagnosticsScope.Instance,
                    DiagnosticsSource.Engine));
        }

        MaintenanceRuntimeDiagnosticsCapture maintenanceCapture =
            CaptureMaintenance(
                families,
                state,
                includeClientRegistry: false,
                maximumActiveRecords: 1,
                maximumRecentRecords: 0);
        bool maintenanceTruncated = IsMaintenanceCaptureTruncated(
            maintenanceCapture);
        DiagnosticsSnapshotMetadata provisionalMetadata = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine,
            recordsTruncated: maintenanceTruncated);
        QueryDiagnosticsSummary raw = QueryRuntimeDiagnostics
            .GetOrCreate(state)
            .GetSummary();
        StorageRuntimeDiagnosticsCapture storage =
            CaptureStorageWithDetailOverlay(
                families,
                state,
                provisionalMetadata);
        DiagnosticsSnapshotMetadata metadata = RebuildMetadata(
            provisionalMetadata,
            DiagnosticsAvailability.Available,
            maintenanceTruncated,
            storage.FieldsTruncated);
        var snapshot = new RuntimeDiagnosticsSnapshot(
            metadata,
            DiagnosticsSection<QueryDiagnosticsSummary>.Available(
                raw with { Metadata = metadata }),
            DiagnosticsSection<ConnectionDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.NotApplicable),
            ReprojectStorageSection(storage.Storage, metadata),
            ReprojectWalSection(storage.Wal, metadata),
            CreateMaintenanceSection(maintenanceCapture, metadata),
            DiagnosticsSection<HealthDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable));
        return new RuntimeDiagnosticsFamilySection<RuntimeDiagnosticsSnapshot>(
            state.DatabaseAlias,
            snapshot);
    }

    private MaintenanceRuntimeDiagnosticsCapture CaptureAggregateMaintenance(
        RuntimeDiagnosticsFamilyLease families,
        int maximumActiveRecords,
        int maximumRecentRecords)
    {
        var sources = new List<MaintenanceRuntimeDiagnostics?>(
            families.States.Count + 2)
        {
            TryGetClientMaintenanceDiagnostics(),
        };
        foreach (CSharpDbRuntimeDiagnosticsState state in families.States)
            sources.Add(MaintenanceRuntimeDiagnostics.TryGet(state));
        if (!families.States.Contains(families.Current))
            sources.Add(MaintenanceRuntimeDiagnostics.TryGet(families.Current));

        return MaintenanceRuntimeDiagnostics.Merge(
            sources,
            maximumActiveRecords,
            maximumRecentRecords,
            sourcesTruncated: families.DroppedCount > 0);
    }

    private MaintenanceRuntimeDiagnosticsCapture CaptureMaintenance(
        RuntimeDiagnosticsFamilyLease families,
        CSharpDbRuntimeDiagnosticsState state,
        bool includeClientRegistry,
        int maximumActiveRecords,
        int maximumRecentRecords)
    {
        MaintenanceRuntimeDiagnostics? stateDiagnostics =
            MaintenanceRuntimeDiagnostics.TryGet(state);
        CSharpDbRuntimeDiagnosticsState? overlayState =
            GetSameIdentityOverlayState(families, state);
        MaintenanceRuntimeDiagnostics? overlayDiagnostics =
            overlayState is null
                ? null
                : MaintenanceRuntimeDiagnostics.TryGet(overlayState);
        return MaintenanceRuntimeDiagnostics.Merge(
            includeClientRegistry
                ? new[]
                {
                    TryGetClientMaintenanceDiagnostics(),
                    stateDiagnostics,
                    overlayDiagnostics,
                }
                : new[]
                {
                    stateDiagnostics,
                    overlayDiagnostics,
                },
            maximumActiveRecords,
            maximumRecentRecords);
    }

    private static bool IsMaintenanceCaptureTruncated(
        MaintenanceRuntimeDiagnosticsCapture capture)
        => capture.CaptureFailed ||
           capture.ActiveRecordsTruncated;

    private static DiagnosticsSection<MaintenanceOperationSnapshot>
        CreateMaintenanceSection(
            MaintenanceRuntimeDiagnosticsCapture capture,
            DiagnosticsSnapshotMetadata metadata)
    {
        if (capture.CaptureFailed ||
            capture.Active.Length == 0 && capture.ActiveRecordsTruncated)
        {
            return DiagnosticsSection<MaintenanceOperationSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable);
        }

        if (capture.Active.Length == 0)
        {
            return DiagnosticsSection<MaintenanceOperationSnapshot>.WithoutValue(
                DiagnosticsAvailability.NotApplicable);
        }

        return DiagnosticsSection<MaintenanceOperationSnapshot>.Available(
            capture.Active[0].ToSnapshot(metadata));
    }

    private DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<
        StorageRuntimeDiagnosticsSnapshot>> CreateStorageDiagnosticsTopology(
            RuntimeDiagnosticsFamilyLease families)
    {
        if (IsSingleCurrentFamily(families))
        {
            return CreateInstanceTopology(
                CreateExactStorageValue(
                    families,
                    families.Current,
                    DiagnosticsSource.Client));
        }

        RuntimeDiagnosticsFamilySection<DiagnosticsValueSnapshot<
            StorageRuntimeDiagnosticsSnapshot>>[] exactFamilies = families.States
                .Select(state => new RuntimeDiagnosticsFamilySection<
                    DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot>>(
                    state.DatabaseAlias,
                    CreateExactStorageValue(
                        families,
                        state,
                        DiagnosticsSource.Engine)))
                .ToArray();
        DiagnosticsAvailability availability = families.States.Any(
            static state => state.IsEnabled)
                ? DiagnosticsAvailability.Unavailable
                : DiagnosticsAvailability.Disabled;
        DiagnosticsSnapshotMetadata metadata = families.Current.CreateMetadata(
            DiagnosticsScope.Aggregate,
            availability,
            DiagnosticsSource.Client);
        var aggregate = new DiagnosticsValueSnapshot<
            StorageRuntimeDiagnosticsSnapshot>(metadata, value: null);
        return CreateAggregateTopology(
            aggregate,
            exactFamilies,
            families.DroppedCount);
    }

    private DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<
        WalRuntimeDiagnosticsSnapshot>> CreateWalDiagnosticsTopology(
            RuntimeDiagnosticsFamilyLease families)
    {
        if (IsSingleCurrentFamily(families))
        {
            return CreateInstanceTopology(
                CreateExactWalValue(
                    families,
                    families.Current,
                    DiagnosticsSource.Client));
        }

        RuntimeDiagnosticsFamilySection<DiagnosticsValueSnapshot<
            WalRuntimeDiagnosticsSnapshot>>[] exactFamilies = families.States
                .Select(state => new RuntimeDiagnosticsFamilySection<
                    DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot>>(
                    state.DatabaseAlias,
                    CreateExactWalValue(
                        families,
                        state,
                        DiagnosticsSource.Engine)))
                .ToArray();
        DiagnosticsAvailability availability = families.States.Any(
            static state => state.IsEnabled)
                ? DiagnosticsAvailability.Unavailable
                : DiagnosticsAvailability.Disabled;
        DiagnosticsSnapshotMetadata metadata = families.Current.CreateMetadata(
            DiagnosticsScope.Aggregate,
            availability,
            DiagnosticsSource.Client);
        var aggregate = new DiagnosticsValueSnapshot<
            WalRuntimeDiagnosticsSnapshot>(metadata, value: null);
        return CreateAggregateTopology(
            aggregate,
            exactFamilies,
            families.DroppedCount);
    }

    private DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot>
        CreateExactStorageValue(
            RuntimeDiagnosticsFamilyLease families,
            CSharpDbRuntimeDiagnosticsState state,
            DiagnosticsSource source)
    {
        if (!state.IsEnabled)
        {
            DiagnosticsSnapshotMetadata disabled = state.CreateMetadata(
                DiagnosticsScope.Instance,
                DiagnosticsAvailability.Disabled,
                source);
            return new DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot>(
                disabled,
                value: null);
        }

        DiagnosticsSnapshotMetadata provisional = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            source);
        StorageRuntimeDiagnosticsCapture capture =
            CaptureStorageWithDetailOverlay(families, state, provisional);
        DiagnosticsAvailability availability = capture.Storage.Availability;
        DiagnosticsSnapshotMetadata metadata = RebuildMetadata(
            provisional,
            availability,
            recordsTruncated: false,
            fieldsTruncated: false);
        return new DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot>(
            metadata,
            capture.Storage.Value is { } value
                ? ReprojectStorageSnapshot(value, metadata)
                : null);
    }

    private DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot>
        CreateExactWalValue(
            RuntimeDiagnosticsFamilyLease families,
            CSharpDbRuntimeDiagnosticsState state,
            DiagnosticsSource source)
    {
        if (!state.IsEnabled)
        {
            DiagnosticsSnapshotMetadata disabled = state.CreateMetadata(
                DiagnosticsScope.Instance,
                DiagnosticsAvailability.Disabled,
                source);
            return new DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot>(
                disabled,
                value: null);
        }

        DiagnosticsSnapshotMetadata provisional = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            source);
        StorageRuntimeDiagnosticsCapture capture =
            CaptureStorageWithDetailOverlay(families, state, provisional);
        DiagnosticsAvailability availability = capture.Wal.Availability;
        DiagnosticsSnapshotMetadata metadata = RebuildMetadata(
            provisional,
            availability,
            recordsTruncated: false,
            fieldsTruncated: capture.Wal.Value is not null &&
                capture.FieldsTruncated);
        return new DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot>(
            metadata,
            capture.Wal.Value is { } value
                ? ReprojectWalSnapshot(value, metadata)
                : null);
    }

    private DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
        MaintenanceOperationSnapshot>> CreateMaintenanceTopology(
            RuntimeDiagnosticsFamilyLease families,
            int maximumRecords,
            bool recent)
    {
        if (IsSingleCurrentFamily(families))
        {
            MaintenanceRuntimeDiagnosticsCapture capture = CaptureMaintenance(
                families,
                families.Current,
                includeClientRegistry: true,
                recent ? 0 : maximumRecords,
                recent ? maximumRecords : 0);
            return CreateInstanceTopology(CreateMaintenanceCollection(
                families.Current,
                capture,
                maximumRecords,
                recent,
                DiagnosticsScope.Instance,
                DiagnosticsSource.Client));
        }

        int familyCount = families.States.Count;
        RuntimeDiagnosticsFamilySection<DiagnosticsCollectionSnapshot<
            MaintenanceOperationSnapshot>>[] exactFamilies = families.States
                .Select((state, index) =>
                {
                    int budget = GetPartitionBudget(
                        maximumRecords,
                        familyCount,
                        index);
                    MaintenanceRuntimeDiagnosticsCapture capture =
                        CaptureMaintenance(
                            families,
                            state,
                            includeClientRegistry: false,
                            recent ? 0 : budget,
                            recent ? budget : 0);
                    DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>
                        collection = CreateMaintenanceCollection(
                            state,
                            capture,
                            maximumRecords,
                            recent,
                            DiagnosticsScope.Instance,
                            DiagnosticsSource.Engine);
                    return new RuntimeDiagnosticsFamilySection<
                        DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>(
                        state.DatabaseAlias,
                        collection);
                })
                .ToArray();

        if (!families.States.Any(static state => state.IsEnabled))
        {
            DiagnosticsSnapshotMetadata disabled = families.Current.CreateMetadata(
                DiagnosticsScope.Aggregate,
                DiagnosticsAvailability.Disabled,
                DiagnosticsSource.Client);
            var aggregateDisabled = new DiagnosticsCollectionSnapshot<
                MaintenanceOperationSnapshot>(
                disabled, null, null, null, null, null);
            return CreateAggregateTopology(
                aggregateDisabled,
                exactFamilies,
                families.DroppedCount);
        }

        MaintenanceRuntimeDiagnosticsCapture aggregateCapture =
            CaptureAggregateMaintenance(
                families,
                recent ? 0 : maximumRecords,
                recent ? maximumRecords : 0);
        DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot> aggregate =
            CreateMaintenanceCollection(
                families.Current,
                aggregateCapture,
                maximumRecords,
                recent,
                DiagnosticsScope.Aggregate,
                DiagnosticsSource.Client);
        return CreateAggregateTopology(
            aggregate,
            exactFamilies,
            families.DroppedCount);
    }

    private static DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>
        CreateMaintenanceCollection(
            CSharpDbRuntimeDiagnosticsState state,
            MaintenanceRuntimeDiagnosticsCapture capture,
            int maximumRecords,
            bool recent,
            DiagnosticsScope scope,
            DiagnosticsSource source)
    {
        if (!state.IsEnabled)
        {
            DiagnosticsSnapshotMetadata disabled = state.CreateMetadata(
                scope,
                DiagnosticsAvailability.Disabled,
                source);
            return new DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>(
                disabled, null, null, null, null, null);
        }

        if (capture.CaptureFailed)
        {
            DiagnosticsSnapshotMetadata unavailable = state.CreateMetadata(
                scope,
                DiagnosticsAvailability.Unavailable,
                source);
            return new DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>(
                unavailable, null, null, null, null, null);
        }

        MaintenanceRuntimeRecord[] selected = recent
            ? capture.Recent
            : capture.Active;
        bool truncated = recent
            ? capture.RecentRecordsTruncated
            : capture.ActiveRecordsTruncated;
        long droppedCount = recent
            ? capture.RecentDroppedCount
            : capture.ActiveRejectedCount;
        DiagnosticsSnapshotMetadata metadata = state.CreateMetadata(
            scope,
            DiagnosticsAvailability.Available,
            source,
            recordsTruncated: truncated);
        MaintenanceOperationSnapshot[] records = selected
            .Select(record => record.ToSnapshot(metadata))
            .ToArray();
        int configuredCapacity = capture.Capacity > 0
            ? capture.Capacity
            : state.RecentOperationCapacity;
        TimeSpan configuredRetention = capture.Retention > TimeSpan.Zero
            ? capture.Retention
            : state.RecentOperationRetention;
        return new DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>(
            metadata,
            records,
            configuredCapacity,
            recent ? configuredRetention : null,
            droppedCount,
            truncated);
    }

    private static bool IsSingleCurrentFamily(
        RuntimeDiagnosticsFamilyLease families)
        => families.States.Count == 1 &&
           ReferenceEquals(families.States[0], families.Current) &&
           families.DroppedCount == 0;

    private static int GetPartitionBudget(
        int totalBudget,
        int partitionCount,
        int partitionIndex)
    {
        if (partitionCount <= 0)
            return 0;
        int quotient = totalBudget / partitionCount;
        int remainder = totalBudget % partitionCount;
        return quotient + (partitionIndex < remainder ? 1 : 0);
    }

    private static CSharpDbRuntimeDiagnosticsState? GetSameIdentityOverlayState(
        RuntimeDiagnosticsFamilyLease families,
        CSharpDbRuntimeDiagnosticsState established)
    {
        CSharpDbRuntimeDiagnosticsState current = families.Current;
        return !ReferenceEquals(current, established) &&
            families.HaveSameCapturedIdentity(current, established)
                ? current
                : null;
    }

    private static StorageRuntimeDiagnosticsCapture
        CaptureStorageWithDetailOverlay(
            RuntimeDiagnosticsFamilyLease families,
            CSharpDbRuntimeDiagnosticsState state,
            DiagnosticsSnapshotMetadata metadata)
    {
        StorageRuntimeDiagnosticsCapture established =
            StorageRuntimeDiagnostics.Capture(state, metadata);
        CSharpDbRuntimeDiagnosticsState? overlayState =
            GetSameIdentityOverlayState(families, state);
        if (overlayState is null || !overlayState.IsEnabled ||
            established.Wal.Availability == DiagnosticsAvailability.Unsupported)
        {
            return established;
        }

        StorageRuntimeDiagnosticsCapture overlay =
            StorageRuntimeDiagnostics.Capture(overlayState, metadata);
        try
        {
            return OverlayStorageRuntimeDetails(
                established,
                overlay,
                metadata);
        }
        catch
        {
            // Replacement detail is optional. A clock or detail-construction
            // failure must not invalidate established storage/WAL gauges.
            return established with
            {
                FieldsTruncated = established.FieldsTruncated ||
                    overlay.FieldsTruncated,
            };
        }
    }

    private static StorageRuntimeDiagnosticsCapture OverlayStorageRuntimeDetails(
        StorageRuntimeDiagnosticsCapture established,
        StorageRuntimeDiagnosticsCapture overlay,
        DiagnosticsSnapshotMetadata metadata)
    {
        WalRuntimeDiagnosticsSnapshot? establishedWal = established.Wal.Value;
        WalRuntimeDiagnosticsSnapshot? overlayWal = overlay.Wal.Value;
        if (overlayWal is null)
        {
            return established with
            {
                FieldsTruncated = established.FieldsTruncated ||
                    overlay.FieldsTruncated,
            };
        }

        WalRecoveryDiagnosticsSnapshot? recovery = SelectRecoveryDetail(
            establishedWal?.Recovery.Value,
            overlayWal.Recovery.Value,
            out bool competingRecovery);
        CheckpointDiagnosticsSnapshot? establishedCheckpoint =
            establishedWal?.Checkpoint.Value;
        CheckpointDiagnosticsSnapshot? overlayCheckpoint =
            overlayWal.Checkpoint.Value;
        CheckpointPhase authoritativeCheckpointPhase =
            SelectAuthoritativeCheckpointPhase(
                establishedWal?.CheckpointPhase,
                overlayWal.CheckpointPhase);
        long activeCheckpointCount = SaturatingAdd(
            establishedCheckpoint?.ActiveCount ?? 0,
            overlayCheckpoint?.ActiveCount ?? 0);
        bool checkpointDetailIncompatible = !IsCheckpointDetailCoherent(
            establishedCheckpoint,
            overlayCheckpoint,
            authoritativeCheckpointPhase);
        bool fieldsTruncated = established.FieldsTruncated ||
            overlay.FieldsTruncated ||
            competingRecovery ||
            activeCheckpointCount > 1 ||
            checkpointDetailIncompatible;
        DiagnosticsSnapshotMetadata detailMetadata = RebuildMetadata(
            metadata,
            DiagnosticsAvailability.Available,
            metadata.RecordsTruncated,
            metadata.FieldsTruncated || fieldsTruncated);

        DiagnosticsSection<WalRecoveryDiagnosticsSnapshot> recoverySection;
        try
        {
            recoverySection = recovery is null
                ? DiagnosticsSection<WalRecoveryDiagnosticsSnapshot>.WithoutValue(
                    CombineDetailAvailability(
                        establishedWal?.Recovery.Availability,
                        overlayWal.Recovery.Availability))
                : DiagnosticsSection<WalRecoveryDiagnosticsSnapshot>.Available(
                    ReprojectWalRecoverySnapshot(recovery, detailMetadata));
        }
        catch
        {
            recoverySection = TryReprojectDetailOrUnavailable(
                establishedWal?.Recovery,
                detailMetadata,
                ReprojectWalRecoverySnapshot);
        }

        DiagnosticsSection<CheckpointDiagnosticsSnapshot> checkpointSection;
        try
        {
            CheckpointDiagnosticsSnapshot? checkpoint =
                MergeCheckpointDetails(
                    establishedCheckpoint,
                    overlayCheckpoint,
                    authoritativeCheckpointPhase,
                    detailMetadata);
            checkpointSection = checkpoint is null
                ? DiagnosticsSection<CheckpointDiagnosticsSnapshot>.WithoutValue(
                    checkpointDetailIncompatible
                        ? DiagnosticsAvailability.Unavailable
                        : CombineDetailAvailability(
                            establishedWal?.Checkpoint.Availability,
                            overlayWal.Checkpoint.Availability))
                : DiagnosticsSection<CheckpointDiagnosticsSnapshot>.Available(
                    checkpoint);
        }
        catch
        {
            checkpointSection = DiagnosticsSection<CheckpointDiagnosticsSnapshot>
                .WithoutValue(DiagnosticsAvailability.Unavailable);
            fieldsTruncated = true;
        }

        bool hasDetail = recoverySection.Value is not null ||
            checkpointSection.Value is not null;
        if (establishedWal is null && !hasDetail)
        {
            return established with
            {
                FieldsTruncated = fieldsTruncated,
            };
        }

        CheckpointDiagnosticsSnapshot? checkpointValue = checkpointSection.Value;
        DateTimeOffset? lastSuccessfulCheckpoint = Maximum(
            establishedWal?.LastSuccessfulCheckpointAtUtc,
            checkpointValue?.LastSuccessfulAtUtc);
        SafeErrorProjection? derivedError = checkpointValue?.LastError ??
            recoverySection.Value?.Error ??
            recoverySection.Value?.LastRetryError;
        var wal = new WalRuntimeDiagnosticsSnapshot(
            detailMetadata,
            establishedWal?.LogicalBytes,
            establishedWal?.AllocatedBytes,
            establishedWal?.CommittedFrameBytes,
            establishedWal?.RetainedBytes,
            establishedWal?.FrameCount,
            establishedWal?.FlushCount,
            establishedWal?.BytesWritten,
            establishedWal?.PendingCommitCount,
            authoritativeCheckpointPhase,
            establishedWal?.LastSuccessfulFlushAtUtc,
            lastSuccessfulCheckpoint,
            overlayWal.LastError ?? establishedWal?.LastError ?? derivedError)
        {
            FlushedCommitCount = establishedWal?.FlushedCommitCount,
            DurableFlushCount = establishedWal?.DurableFlushCount,
            LastSuccessfulDurableFlushAtUtc =
                establishedWal?.LastSuccessfulDurableFlushAtUtc,
            GroupCommitBatchCount = establishedWal?.GroupCommitBatchCount,
            GroupCommitCount = establishedWal?.GroupCommitCount,
            LastSuccessfulGroupCommitAtUtc =
                establishedWal?.LastSuccessfulGroupCommitAtUtc,
            Recovery = recoverySection,
            Checkpoint = checkpointSection,
        };
        return new StorageRuntimeDiagnosticsCapture(
            established.Storage,
            DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.Available(wal),
            fieldsTruncated);
    }

    private static WalRecoveryDiagnosticsSnapshot? SelectRecoveryDetail(
        WalRecoveryDiagnosticsSnapshot? established,
        WalRecoveryDiagnosticsSnapshot? overlay,
        out bool competingActive)
    {
        competingActive = false;
        if (established is null)
            return overlay;
        if (overlay is null)
            return established;

        bool establishedActive = established.Phase != WalRecoveryPhase.Completed;
        bool overlayActive = overlay.Phase != WalRecoveryPhase.Completed;
        if (establishedActive != overlayActive)
            return establishedActive ? established : overlay;
        if (establishedActive)
        {
            competingActive = true;
            int started = overlay.StartedAtUtc.CompareTo(established.StartedAtUtc);
            if (started != 0)
                return started < 0 ? overlay : established;
            return string.CompareOrdinal(
                    overlay.OperationId.Value,
                    established.OperationId.Value) < 0
                ? overlay
                : established;
        }

        int completed = Nullable.Compare(
            overlay.CompletedAtUtc,
            established.CompletedAtUtc);
        if (completed != 0)
            return completed > 0 ? overlay : established;
        int terminalStarted = overlay.StartedAtUtc.CompareTo(
            established.StartedAtUtc);
        if (terminalStarted != 0)
            return terminalStarted > 0 ? overlay : established;
        return string.CompareOrdinal(
                overlay.OperationId.Value,
                established.OperationId.Value) > 0
            ? overlay
            : established;
    }

    private static CheckpointDiagnosticsSnapshot? MergeCheckpointDetails(
        CheckpointDiagnosticsSnapshot? established,
        CheckpointDiagnosticsSnapshot? overlay,
        CheckpointPhase authoritativePhase,
        DiagnosticsSnapshotMetadata metadata)
    {
        if (!IsCheckpointDetailCoherent(
                established,
                overlay,
                authoritativePhase))
        {
            return null;
        }
        if (established is null)
        {
            return overlay is null
                ? null
                : ReprojectCheckpointSnapshot(overlay, metadata);
        }
        if (overlay is null)
            return ReprojectCheckpointSnapshot(established, metadata);

        long activeCount = SaturatingAdd(
            established.ActiveCount,
            overlay.ActiveCount);
        CheckpointDiagnosticsSnapshot? representative =
            SelectActiveCheckpointRepresentative(established, overlay);
        CheckpointDiagnosticsSnapshot phaseSource = representative ??
            SelectTerminalCheckpointUnit(established, overlay);
        DateTimeOffset? lastSuccessfulAtUtc = Maximum(
            established.LastSuccessfulAtUtc,
            overlay.LastSuccessfulAtUtc);
        CheckpointDiagnosticsSnapshot? latestFailure =
            SelectLatestCheckpointFailure(established, overlay);
        DateTimeOffset? lastFailedAtUtc = latestFailure?.LastFailedAtUtc;
        SafeErrorProjection? lastError = latestFailure?.LastError;
        return new CheckpointDiagnosticsSnapshot(
            metadata,
            representative?.OperationId,
            phaseSource.Phase,
            phaseSource.Origin,
            representative?.StartedAtUtc,
            representative?.Elapsed,
            representative?.CompletedPageCount,
            representative?.TotalPageCount,
            phaseSource.Phase == CheckpointPhase.Idle
                ? CheckpointRetentionReason.None
                : phaseSource.RetentionReason,
            phaseSource.LastStartedAtUtc,
            lastSuccessfulAtUtc,
            lastFailedAtUtc,
            phaseSource.LastElapsed,
            activeCount,
            SaturatingAdd(established.AttemptCount, overlay.AttemptCount),
            SaturatingAdd(established.SuccessCount, overlay.SuccessCount),
            SaturatingAdd(established.FailureCount, overlay.FailureCount),
            SaturatingAdd(established.CanceledCount, overlay.CanceledCount),
            lastError);
    }

    private static bool IsCheckpointDetailCoherent(
        CheckpointDiagnosticsSnapshot? established,
        CheckpointDiagnosticsSnapshot? overlay,
        CheckpointPhase authoritativePhase)
    {
        long activeCount = SaturatingAdd(
            established?.ActiveCount ?? 0,
            overlay?.ActiveCount ?? 0);
        if (activeCount > 0)
        {
            if (authoritativePhase is CheckpointPhase.Idle or
                CheckpointPhase.Faulted)
            {
                return false;
            }

            return established is { ActiveCount: > 0 } &&
                    established.Phase == authoritativePhase ||
                overlay is { ActiveCount: > 0 } &&
                    overlay.Phase == authoritativePhase;
        }

        if (authoritativePhase == CheckpointPhase.Requested)
            return false;

        if (established is null && overlay is null)
            return authoritativePhase == CheckpointPhase.Idle;
        return established?.Phase == authoritativePhase ||
            overlay?.Phase == authoritativePhase;
    }

    private static CheckpointPhase SelectAuthoritativeCheckpointPhase(
        CheckpointPhase? established,
        CheckpointPhase overlay)
        => established is null ||
            GetCheckpointPhaseRank(overlay) >
                GetCheckpointPhaseRank(established.Value)
            ? overlay
            : established.Value;

    private static CheckpointDiagnosticsSnapshot?
        SelectActiveCheckpointRepresentative(
            CheckpointDiagnosticsSnapshot left,
            CheckpointDiagnosticsSnapshot right)
    {
        bool leftActive = left.ActiveCount > 0;
        bool rightActive = right.ActiveCount > 0;
        if (!leftActive)
            return rightActive ? right : null;
        if (!rightActive)
            return left;
        int phase = GetCheckpointPhaseRank(right.Phase).CompareTo(
            GetCheckpointPhaseRank(left.Phase));
        if (phase != 0)
            return phase > 0 ? right : left;
        int started = Nullable.Compare(right.StartedAtUtc, left.StartedAtUtc);
        if (started != 0)
            return started < 0 ? right : left;
        return string.CompareOrdinal(
                right.OperationId!.Value,
                left.OperationId!.Value) < 0
            ? right
            : left;
    }

    private static CheckpointDiagnosticsSnapshot SelectTerminalCheckpointUnit(
        CheckpointDiagnosticsSnapshot left,
        CheckpointDiagnosticsSnapshot right)
    {
        int phase = GetCheckpointPhaseRank(right.Phase).CompareTo(
            GetCheckpointPhaseRank(left.Phase));
        if (phase != 0)
            return phase > 0 ? right : left;

        DateTimeOffset? leftTerminal = GetCompatibleCheckpointTerminalTimestamp(
            left);
        DateTimeOffset? rightTerminal = GetCompatibleCheckpointTerminalTimestamp(
            right);
        int terminal = Nullable.Compare(rightTerminal, leftTerminal);
        if (terminal != 0)
            return terminal > 0 ? right : left;
        int started = Nullable.Compare(
            right.LastStartedAtUtc,
            left.LastStartedAtUtc);
        if (started != 0)
            return started > 0 ? right : left;
        int origin = right.Origin.CompareTo(left.Origin);
        return origin < 0 ? right : left;
    }

    private static DateTimeOffset? GetCompatibleCheckpointTerminalTimestamp(
        CheckpointDiagnosticsSnapshot value)
        => value.Phase switch
        {
            CheckpointPhase.Faulted => value.LastFailedAtUtc,
            CheckpointPhase.Idle => value.LastSuccessfulAtUtc,
            _ => Maximum(
                value.LastSuccessfulAtUtc,
                value.LastFailedAtUtc),
        };

    private static CheckpointDiagnosticsSnapshot? SelectLatestCheckpointFailure(
        CheckpointDiagnosticsSnapshot left,
        CheckpointDiagnosticsSnapshot right)
    {
        bool leftHasFailure = left.LastFailedAtUtc is not null;
        bool rightHasFailure = right.LastFailedAtUtc is not null;
        if (!leftHasFailure)
            return rightHasFailure ? right : null;
        if (!rightHasFailure)
            return left;
        return right.LastFailedAtUtc > left.LastFailedAtUtc ? right : left;
    }

    private static int GetCheckpointPhaseRank(CheckpointPhase phase)
        => phase switch
        {
            CheckpointPhase.Idle => 0,
            CheckpointPhase.Requested => 1,
            CheckpointPhase.Copying => 2,
            CheckpointPhase.CopyCompleteAwaitingReaders => 3,
            CheckpointPhase.Finalizing => 4,
            CheckpointPhase.Faulted => 5,
            _ => -1,
        };

    private static DiagnosticsAvailability CombineDetailAvailability(
        DiagnosticsAvailability? left,
        DiagnosticsAvailability right)
    {
        if (left is null)
            return right;
        if (left == right)
            return right;
        if (left == DiagnosticsAvailability.Unsupported ||
            right == DiagnosticsAvailability.Unsupported)
        {
            return DiagnosticsAvailability.Unsupported;
        }
        return DiagnosticsAvailability.Unavailable;
    }

    private static DiagnosticsSection<T> TryReprojectDetailOrUnavailable<T>(
        DiagnosticsSection<T>? section,
        DiagnosticsSnapshotMetadata metadata,
        Func<T, DiagnosticsSnapshotMetadata, T> projector)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        try
        {
            return section?.Value is { } value
                ? DiagnosticsSection<T>.Available(projector(value, metadata))
                : DiagnosticsSection<T>.WithoutValue(
                    section?.Availability ?? DiagnosticsAvailability.Unavailable);
        }
        catch
        {
            return DiagnosticsSection<T>.WithoutValue(
                DiagnosticsAvailability.Unavailable);
        }
    }

    private static DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>
        ReprojectStorageSection(
            DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot> section,
            DiagnosticsSnapshotMetadata metadata)
        => section.Value is { } value
            ? DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.Available(
                ReprojectStorageSnapshot(value, metadata))
            : DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.WithoutValue(
                section.Availability);

    private static StorageRuntimeDiagnosticsSnapshot ReprojectStorageSnapshot(
        StorageRuntimeDiagnosticsSnapshot value,
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            value.LogicalDatabaseBytes,
            value.AllocatedDatabaseBytes,
            value.PageCount,
            value.PageReads,
            value.PageWrites,
            value.BytesRead,
            value.BytesWritten,
            value.CacheHits,
            value.CacheMisses,
            value.DirtyPages,
            value.ActiveReaders,
            value.ActiveWriters,
            value.CommitCount,
            value.ConflictCount)
        {
            Cache = TryReprojectDetailOrUnavailable(
                value.Cache,
                metadata,
                ReprojectStorageCacheSnapshot),
            PhysicalIo = TryReprojectDetailOrUnavailable(
                value.PhysicalIo,
                metadata,
                ReprojectStorageDeviceIoSnapshot),
        };

    private static StorageCacheDiagnosticsSnapshot
        ReprojectStorageCacheSnapshot(
            StorageCacheDiagnosticsSnapshot value,
            DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            value.SharedResidentPages,
            value.SharedCapacityPages,
            value.WalResidentPages,
            value.WalCapacityPages);

    private static StorageDeviceIoDiagnosticsSnapshot
        ReprojectStorageDeviceIoSnapshot(
            StorageDeviceIoDiagnosticsSnapshot value,
            DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            value.ReadCount,
            value.BytesRead,
            value.WriteCount,
            value.BytesWritten,
            value.FlushCount,
            value.ResizeCount,
            value.SequentialReadCount,
            value.SequentialBytesRead,
            value.MemoryMappedPageExposureCount,
            value.MemoryMappedBytesExposed);

    private static DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>
        ReprojectWalSection(
            DiagnosticsSection<WalRuntimeDiagnosticsSnapshot> section,
            DiagnosticsSnapshotMetadata metadata)
        => section.Value is { } value
            ? DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.Available(
                ReprojectWalSnapshot(value, metadata))
            : DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.WithoutValue(
                section.Availability);

    private static WalRuntimeDiagnosticsSnapshot ReprojectWalSnapshot(
        WalRuntimeDiagnosticsSnapshot value,
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            value.LogicalBytes,
            value.AllocatedBytes,
            value.CommittedFrameBytes,
            value.RetainedBytes,
            value.FrameCount,
            value.FlushCount,
            value.BytesWritten,
            value.PendingCommitCount,
            value.CheckpointPhase,
            value.LastSuccessfulFlushAtUtc,
            value.LastSuccessfulCheckpointAtUtc,
            value.LastError)
        {
            FlushedCommitCount = value.FlushedCommitCount,
            DurableFlushCount = value.DurableFlushCount,
            LastSuccessfulDurableFlushAtUtc =
                value.LastSuccessfulDurableFlushAtUtc,
            GroupCommitBatchCount = value.GroupCommitBatchCount,
            GroupCommitCount = value.GroupCommitCount,
            LastSuccessfulGroupCommitAtUtc =
                value.LastSuccessfulGroupCommitAtUtc,
            Recovery = value.Recovery.Value is { } recovery
                ? DiagnosticsSection<WalRecoveryDiagnosticsSnapshot>.Available(
                    ReprojectWalRecoverySnapshot(recovery, metadata))
                : DiagnosticsSection<WalRecoveryDiagnosticsSnapshot>.WithoutValue(
                    value.Recovery.Availability),
            Checkpoint = value.Checkpoint.Value is { } checkpoint
                ? DiagnosticsSection<CheckpointDiagnosticsSnapshot>.Available(
                    ReprojectCheckpointSnapshot(checkpoint, metadata))
                : DiagnosticsSection<CheckpointDiagnosticsSnapshot>.WithoutValue(
                    value.Checkpoint.Availability),
        };

    private static WalRecoveryDiagnosticsSnapshot ReprojectWalRecoverySnapshot(
        WalRecoveryDiagnosticsSnapshot value,
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            value.OperationId,
            value.Phase,
            value.StartedAtUtc,
            value.CompletedAtUtc,
            value.Elapsed,
            value.Outcome,
            value.ScannedFrameCount,
            value.ScannedBytes,
            value.RecoveredFrameCount,
            value.RecoveredBytes,
            value.DiscardedFrameCount,
            value.DiscardedBytes,
            value.TruncationReason,
            value.AttemptCount,
            value.RetryCount,
            value.LastRetryError,
            value.Error);

    private static CheckpointDiagnosticsSnapshot ReprojectCheckpointSnapshot(
        CheckpointDiagnosticsSnapshot value,
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            value.OperationId,
            value.Phase,
            value.Origin,
            value.StartedAtUtc,
            value.Elapsed,
            value.CompletedPageCount,
            value.TotalPageCount,
            value.RetentionReason,
            value.LastStartedAtUtc,
            value.LastSuccessfulAtUtc,
            value.LastFailedAtUtc,
            value.LastElapsed,
            value.ActiveCount,
            value.AttemptCount,
            value.SuccessCount,
            value.FailureCount,
            value.CanceledCount,
            value.LastError);

    private static DiagnosticsSnapshotMetadata RebuildMetadata(
        DiagnosticsSnapshotMetadata metadata,
        DiagnosticsAvailability availability,
        bool recordsTruncated,
        bool fieldsTruncated)
        => new(
            metadata.SchemaVersion,
            metadata.CapturedAtUtc,
            metadata.ServerInstanceId,
            metadata.CounterEpoch,
            metadata.Scope,
            availability,
            metadata.Source,
            metadata.DatabaseAlias,
            recordsTruncated,
            fieldsTruncated);

    private static DateTimeOffset? Maximum(
        DateTimeOffset? left,
        DateTimeOffset? right)
        => left is null
            ? right
            : right is null || left >= right
                ? left
                : right;

    private static RuntimeDiagnosticsSnapshot CreateDisabledRuntimeSnapshot(
        CSharpDbRuntimeDiagnosticsState state,
        DiagnosticsScope scope,
        DiagnosticsSource source)
    {
        DiagnosticsSnapshotMetadata metadata = state.CreateMetadata(
            scope,
            DiagnosticsAvailability.Disabled,
            source);
        return new RuntimeDiagnosticsSnapshot(
            metadata,
            DiagnosticsSection<QueryDiagnosticsSummary>.WithoutValue(
                DiagnosticsAvailability.Disabled),
            DiagnosticsSection<ConnectionDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Disabled),
            DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Disabled),
            DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Disabled),
            DiagnosticsSection<MaintenanceOperationSnapshot>.WithoutValue(
                DiagnosticsAvailability.Disabled),
            DiagnosticsSection<HealthDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Disabled));
    }

    private static DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>
        CreateActiveQueryTopology(
            RuntimeDiagnosticsFamilyLease families,
            int maximumRecords)
    {
        DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>[] exact = families.States
            .Select(state => CreateExactActiveQueryCollection(state, maximumRecords))
            .ToArray();
        if (exact.Length == 1 &&
            ReferenceEquals(families.States[0], families.Current) &&
            families.DroppedCount == 0)
            return CreateInstanceTopology(exact[0]);

        RuntimeDiagnosticsFamilySection<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>[]
            exactFamilies = exact
                .Select(collection => new RuntimeDiagnosticsFamilySection<
                    DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
                    collection.Metadata.DatabaseAlias,
                    collection))
                .ToArray();
        if (!families.States.Any(static state => state.IsEnabled))
        {
            DiagnosticsSnapshotMetadata disabledMetadata = families.Current.CreateMetadata(
                DiagnosticsScope.Aggregate,
                DiagnosticsAvailability.Disabled,
                DiagnosticsSource.Client);
            var disabledAggregate = new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
                disabledMetadata, null, null, null, null, null);
            return CreateAggregateTopology(
                disabledAggregate,
                exactFamilies,
                families.DroppedCount);
        }

        ActiveQuerySnapshot[] copied = exact
            .SelectMany(static collection => collection.Records ?? [])
            .OrderBy(static record => record.StartedAtUtc)
            .ThenBy(static record => record.OperationId.Value, StringComparer.Ordinal)
            .ToArray();
        bool truncated = families.DroppedCount > 0 ||
            exact.Any(static collection => collection.IsTruncated == true) ||
            copied.Length > maximumRecords;
        DiagnosticsSnapshotMetadata metadata = families.Current.CreateMetadata(
            DiagnosticsScope.Aggregate,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Client,
            recordsTruncated: truncated);
        ActiveQuerySnapshot[] aggregateRecords = copied
            .Take(maximumRecords)
            .Select(record => record with { Metadata = metadata })
            .ToArray();
        long aggregateDroppedCount = exact.Aggregate(
            0L,
            static (total, collection) => SaturatingAdd(
                total,
                collection.DroppedCount ?? 0));
        var aggregate = new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            metadata,
            aggregateRecords,
            maximumRecords,
            retention: null,
            droppedCount: aggregateDroppedCount,
            isTruncated: truncated);
        return CreateAggregateTopology(aggregate, exactFamilies, families.DroppedCount);
    }

    private static DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>
        CreateExactActiveQueryCollection(
            CSharpDbRuntimeDiagnosticsState state,
            int maximumRecords)
    {
        if (state.IsEnabled)
        {
            return QueryRuntimeDiagnostics
                .GetOrCreate(state)
                .GetActiveCollectionSnapshot(maximumRecords);
        }

        DiagnosticsSnapshotMetadata metadata = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Disabled,
            DiagnosticsSource.Engine);
        return new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            metadata, null, null, null, null, null);
    }

    private static DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>
        CreateRecentQueryTopology(
            RuntimeDiagnosticsFamilyLease families,
            int maximumRecords)
    {
        DiagnosticsCollectionSnapshot<RecentQuerySnapshot>[] exact = families.States
            .Select(state => CreateExactRecentQueryCollection(state, maximumRecords))
            .ToArray();
        if (exact.Length == 1 &&
            ReferenceEquals(families.States[0], families.Current) &&
            families.DroppedCount == 0)
            return CreateInstanceTopology(exact[0]);

        RuntimeDiagnosticsFamilySection<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>[]
            exactFamilies = exact
                .Select(collection => new RuntimeDiagnosticsFamilySection<
                    DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>(
                    collection.Metadata.DatabaseAlias,
                    collection))
                .ToArray();
        if (!families.States.Any(static state => state.IsEnabled))
        {
            DiagnosticsSnapshotMetadata disabledMetadata = families.Current.CreateMetadata(
                DiagnosticsScope.Aggregate,
                DiagnosticsAvailability.Disabled,
                DiagnosticsSource.Client);
            var disabledAggregate = new DiagnosticsCollectionSnapshot<RecentQuerySnapshot>(
                disabledMetadata, null, null, null, null, null);
            return CreateAggregateTopology(
                disabledAggregate,
                exactFamilies,
                families.DroppedCount);
        }

        RecentQuerySnapshot[] copied = exact
            .SelectMany(static collection => collection.Records ?? [])
            .OrderByDescending(static record => record.CompletedAtUtc)
            .ThenBy(static record => record.OperationId.Value, StringComparer.Ordinal)
            .ToArray();
        bool truncated = families.DroppedCount > 0 ||
            exact.Any(static collection => collection.IsTruncated == true) ||
            copied.Length > maximumRecords;
        DiagnosticsSnapshotMetadata metadata = families.Current.CreateMetadata(
            DiagnosticsScope.Aggregate,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Client,
            recordsTruncated: truncated);
        RecentQuerySnapshot[] aggregateRecords = copied
            .Take(maximumRecords)
            .Select(record => record with { Metadata = metadata })
            .ToArray();
        long aggregateDroppedCount = exact.Aggregate(
            0L,
            static (total, collection) => SaturatingAdd(
                total,
                collection.DroppedCount ?? 0));
        var aggregate = new DiagnosticsCollectionSnapshot<RecentQuerySnapshot>(
            metadata,
            aggregateRecords,
            maximumRecords,
            retention: null,
            droppedCount: aggregateDroppedCount,
            isTruncated: truncated);
        return CreateAggregateTopology(aggregate, exactFamilies, families.DroppedCount);
    }

    private static DiagnosticsCollectionSnapshot<RecentQuerySnapshot>
        CreateExactRecentQueryCollection(
            CSharpDbRuntimeDiagnosticsState state,
            int maximumRecords)
    {
        if (state.IsEnabled)
        {
            return QueryRuntimeDiagnostics
                .GetOrCreate(state)
                .GetRecentCollectionSnapshot(maximumRecords);
        }

        DiagnosticsSnapshotMetadata metadata = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Disabled,
            DiagnosticsSource.Engine);
        return new DiagnosticsCollectionSnapshot<RecentQuerySnapshot>(
            metadata, null, null, null, null, null);
    }

    private static DiagnosticsTopologySnapshot<
        DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>
        CreateQueryPlanTopology(
            RuntimeDiagnosticsFamilyLease families,
            OpaqueDiagnosticsId operationId)
    {
        DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>[] exact = families.States
            .Select(state =>
            {
                QueryPlanDiagnosticsSnapshot? plan = state.IsEnabled
                    ? QueryRuntimeDiagnostics.GetOrCreate(state).GetPlanSnapshot(operationId)
                    : null;
                DiagnosticsAvailability availability = !state.IsEnabled
                    ? DiagnosticsAvailability.Disabled
                    : plan is null
                        ? DiagnosticsAvailability.Unavailable
                        : DiagnosticsAvailability.Available;
                DiagnosticsSnapshotMetadata metadata = plan?.Metadata ?? state.CreateMetadata(
                    DiagnosticsScope.Instance,
                    availability,
                    DiagnosticsSource.Engine);
                return new DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>(
                    metadata,
                    plan);
            })
            .ToArray();
        if (exact.Length == 1 &&
            ReferenceEquals(families.States[0], families.Current) &&
            families.DroppedCount == 0)
            return CreateInstanceTopology(exact[0]);

        QueryPlanDiagnosticsSnapshot? selected = exact
            .Select(static envelope => envelope.Value)
            .FirstOrDefault(static plan => plan is not null);
        DiagnosticsAvailability aggregateAvailability = selected is not null
            ? DiagnosticsAvailability.Available
            : families.States.Any(static state => state.IsEnabled)
                ? DiagnosticsAvailability.Unavailable
                : DiagnosticsAvailability.Disabled;
        DiagnosticsSnapshotMetadata aggregateMetadata = families.Current.CreateMetadata(
            DiagnosticsScope.Aggregate,
            aggregateAvailability,
            DiagnosticsSource.Client);
        var aggregate = new DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>(
            aggregateMetadata,
            selected is null ? null : selected with { Metadata = aggregateMetadata });
        RuntimeDiagnosticsFamilySection<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>[]
            exactFamilies = exact
                .Select(envelope => new RuntimeDiagnosticsFamilySection<
                    DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>(
                    envelope.Metadata.DatabaseAlias,
                    envelope))
                .ToArray();
        return CreateAggregateTopology(aggregate, exactFamilies, families.DroppedCount);
    }

    private static DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>
        CreateQueryDetailTopology(
            RuntimeDiagnosticsFamilyLease families,
            OpaqueDiagnosticsId operationId)
    {
        DiagnosticsValueSnapshot<QueryDetailSnapshot>[] exact = families.States
            .Select(state =>
            {
                QueryDetailSnapshot? detail = state.IsEnabled
                    ? QueryRuntimeDiagnostics
                        .GetOrCreate(state, startSweepTimer: false)
                        .GetQueryDetailSnapshot(operationId)
                    : null;
                DiagnosticsAvailability availability = detail is not null
                    ? DiagnosticsAvailability.Available
                    : state.IsEnabled
                        ? DiagnosticsAvailability.Unavailable
                        : DiagnosticsAvailability.Disabled;
                DiagnosticsSnapshotMetadata metadata = detail?.Metadata ??
                    state.CreateMetadata(
                        DiagnosticsScope.Instance,
                        availability,
                        DiagnosticsSource.Engine);
                return new DiagnosticsValueSnapshot<QueryDetailSnapshot>(
                    metadata,
                    detail);
            })
            .ToArray();
        if (exact.Length == 1 &&
            ReferenceEquals(families.States[0], families.Current) &&
            families.DroppedCount == 0)
            return CreateInstanceTopology(exact[0]);

        QueryDetailSnapshot? selected = exact
            .Select(static envelope => envelope.Value)
            .FirstOrDefault(static detail => detail is not null);
        DiagnosticsAvailability aggregateAvailability = selected is not null
            ? DiagnosticsAvailability.Available
            : families.States.Any(static state => state.IsEnabled)
                ? DiagnosticsAvailability.Unavailable
                : DiagnosticsAvailability.Disabled;
        DiagnosticsSnapshotMetadata aggregateMetadata = families.Current.CreateMetadata(
            DiagnosticsScope.Aggregate,
            aggregateAvailability,
            DiagnosticsSource.Client,
            fieldsTruncated: selected?.Metadata.FieldsTruncated == true);
        var aggregate = new DiagnosticsValueSnapshot<QueryDetailSnapshot>(
            aggregateMetadata,
            selected is null
                ? null
                : selected with { Metadata = aggregateMetadata });
        RuntimeDiagnosticsFamilySection<DiagnosticsValueSnapshot<QueryDetailSnapshot>>[]
            exactFamilies = exact
                .Select(envelope => new RuntimeDiagnosticsFamilySection<
                    DiagnosticsValueSnapshot<QueryDetailSnapshot>>(
                    envelope.Metadata.DatabaseAlias,
                    envelope))
                .ToArray();
        return CreateAggregateTopology(aggregate, exactFamilies, families.DroppedCount);
    }

    private DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>
        CreateSessionTopology(
            CSharpDbRuntimeDiagnosticsState state,
            int maximumRecords)
    {
        if (!state.IsEnabled)
        {
            DiagnosticsSnapshotMetadata disabledMetadata = state.CreateMetadata(
                DiagnosticsScope.Instance,
                DiagnosticsAvailability.Disabled,
                DiagnosticsSource.Client);
            return CreateInstanceTopology(
                new DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>(
                    disabledMetadata, null, null, null, null, null));
        }

        // Sessions are a client-scoped view rather than an engine-family
        // partition. Follow the current family's enablement policy and omit
        // retained sessions that were created without diagnostic identity.
        ClientTransactionSession[] capturedTransactions = _transactions.Values
            .Take(CSharpDbObservabilityOptions.MaximumHistoryCapacity + 1)
            .ToArray();
        bool transactionCaptureTruncated =
            capturedTransactions.Length > CSharpDbObservabilityOptions.MaximumHistoryCapacity;
        if (transactionCaptureTruncated)
        {
            capturedTransactions =
                capturedTransactions[..CSharpDbObservabilityOptions.MaximumHistoryCapacity];
        }

        ClientTransactionSession[] transactionSessions = capturedTransactions
            .Where(static session => session.CanPublishDiagnostics)
            .ToArray();

        int observedCount = transactionSessions.Length + 1;
        bool truncated = transactionCaptureTruncated || observedCount > maximumRecords;
        DiagnosticsSnapshotMetadata metadata = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Client,
            recordsTruncated: truncated);
        DirectDiagnosticsSession direct = GetOrCreateDirectDiagnosticsSession();
        int directReaders = TryGetCachedDatabaseActiveReaderCount();
        var records = new List<SessionDiagnosticsSnapshot>(observedCount)
        {
            new(
                metadata,
                direct.SessionId,
                direct.CreatedAtUtc,
                direct.LastActiveAtUtc,
                CurrentOperationId: null,
                HasActiveReader: directReaders > 0,
                HasActiveTransaction: false,
                CSharpDB.Observability.CSharpDbTransport.Direct)
            {
                State = directReaders > 0
                    ? DiagnosticsSessionState.SnapshotReader
                    : DiagnosticsSessionState.Idle,
            },
        };
        foreach (ClientTransactionSession session in transactionSessions)
            records.Add(session.CreateDiagnosticsSnapshot(metadata, metadata.CapturedAtUtc));

        SessionDiagnosticsSnapshot[] selected = records
            .OrderBy(GetSessionSelectionPriority)
            .ThenBy(static record => record.CreatedAtUtc)
            .ThenBy(static record => record.SessionId.Value, StringComparer.Ordinal)
            .Take(maximumRecords)
            .ToArray();
        var collection = new DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>(
            metadata,
            selected,
            maximumRecords,
            retention: null,
            droppedCount: 0,
            isTruncated: truncated);
        return CreateInstanceTopology(collection);
    }

    private static int GetSessionSelectionPriority(SessionDiagnosticsSnapshot record)
    {
        if (record.CurrentOperationId is not null)
            return 0;
        if (record.HasActiveReader)
            return 1;
        if (record.State == DiagnosticsSessionState.Abandoned)
            return 3;
        if (record.HasActiveTransaction)
            return 2;
        return 4;
    }

    private ConnectionCapture CaptureConnections()
    {
        ClientTransactionSession[] sessions = _transactions.Values
            .Take(CSharpDbObservabilityOptions.MaximumHistoryCapacity + 1)
            .ToArray();
        bool fieldsTruncated =
            sessions.Length > CSharpDbObservabilityOptions.MaximumHistoryCapacity;
        if (fieldsTruncated)
            sessions = sessions[..CSharpDbObservabilityOptions.MaximumHistoryCapacity];
        return new ConnectionCapture(
            sessions,
            fieldsTruncated ? null : sessions.Length,
            TryGetCachedDatabaseActiveReaderCount(),
            fieldsTruncated,
            Volatile.Read(ref _exclusiveMaintenanceActive) != 0);
    }

    private static ConnectionDiagnosticsSnapshot CreateConnectionSnapshot(
        DiagnosticsSnapshotMetadata metadata,
        ConnectionCapture capture)
    {
        SessionDiagnosticsSnapshot[] sessions = capture.Sessions
            .Where(static session => session.CanPublishDiagnostics)
            .Select(session => session.CreateDiagnosticsSnapshot(
                metadata,
                metadata.CapturedAtUtc))
            .ToArray();
        int? activeReaders = capture.FieldsTruncated
            ? null
            : SaturatingAdd(
                capture.CachedActiveReaders,
                capture.Sessions.Count(static session => session.HasActiveDiagnosticsReader));
        bool incompleteSessionIdentity = sessions.Length != capture.Sessions.Length;
        TimeSpan? oldestAge =
            capture.FieldsTruncated || incompleteSessionIdentity || sessions.Length == 0
            ? null
            : capture.Sessions.Max(static session => session.GetDiagnosticsAge());
        return new ConnectionDiagnosticsSnapshot(
            metadata,
            PoolCapacity: null,
            AvailableSlots: null,
            WaiterCount: null,
            ActiveLogicalSessions: capture.TransactionCount is int transactionCount
                ? transactionCount == int.MaxValue
                    ? int.MaxValue
                    : transactionCount + 1
                : null,
            ActiveReaders: activeReaders,
            ActiveTransactions: capture.TransactionCount,
            RetiredPoolCount: null,
            PoisonedPoolCount: null,
            OldestTransactionAge: oldestAge)
        {
            TransactionOwnerSessionId = capture.TransactionCount == 1 && sessions.Length == 1
                ? sessions[0].SessionId
                : null,
            PoolState = ConnectionPoolLifecycleState.Unknown,
            ExclusiveMaintenanceActive = capture.ExclusiveMaintenanceActive,
        };
    }

    private int TryGetCachedDatabaseActiveReaderCount()
    {
        Task<Database>? databaseTask;
        lock (_databaseGate)
            databaseTask = _databaseTask;
        if (databaseTask?.IsCompletedSuccessfully != true)
            return 0;

        try
        {
            return databaseTask.GetAwaiter().GetResult().ActiveReaderCount;
        }
        catch
        {
            return 0;
        }
    }

    private DiagnosticsTopologySnapshot<T> CaptureStableRuntimeDiagnosticsTopology<T>(
        Func<RuntimeDiagnosticsFamilyLease, DiagnosticsTopologySnapshot<T>> capture,
        Func<RuntimeDiagnosticsCaptureIdentity, DiagnosticsTopologySnapshot<T>>
            createIdentityUnstable,
        CancellationToken ct)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        const int maximumAttempts = 3;
        RuntimeDiagnosticsCaptureIdentity fallbackIdentity = default;
        for (int attempt = 1; attempt <= maximumAttempts; attempt++)
        {
            ct.ThrowIfCancellationRequested();
            using RuntimeDiagnosticsFamilyLease families =
                AcquireRuntimeDiagnosticsFamilyLease();
            fallbackIdentity = families.CaptureIdentity;
            try
            {
                DiagnosticsTopologySnapshot<T> result = capture(families);
                Volatile.Read(ref RuntimeDiagnosticsCaptureCompletedForTests)
                    ?.Invoke(attempt);
                ct.ThrowIfCancellationRequested();
                if (families.IsIdentityStable())
                    return result;
            }
            catch (ArgumentException) when (!families.IsIdentityStable())
            {
                // Promotion raced the capture. Discard every field and child
                // produced from the mixed identity and regroup from scratch.
            }
        }

        ct.ThrowIfCancellationRequested();
        return createIdentityUnstable(fallbackIdentity);
    }

    private static DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>
        CreateIdentityUnstableRuntimeTopology(
            RuntimeDiagnosticsCaptureIdentity identity)
    {
        DiagnosticsSnapshotMetadata metadata =
            CreateIdentityUnstableMetadata(identity);
        var snapshot = new RuntimeDiagnosticsSnapshot(
            metadata,
            DiagnosticsSection<QueryDiagnosticsSummary>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<ConnectionDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<MaintenanceOperationSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<HealthDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable));
        return CreateInstanceTopology(snapshot);
    }

    private static DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<T>>
        CreateIdentityUnstableValueTopology<T>(
            RuntimeDiagnosticsCaptureIdentity identity)
        where T : class, IRuntimeDiagnosticsSnapshot
        => CreateInstanceTopology(new DiagnosticsValueSnapshot<T>(
            CreateIdentityUnstableMetadata(identity),
            value: null));

    private static DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<T>>
        CreateIdentityUnstableCollectionTopology<T>(
            RuntimeDiagnosticsCaptureIdentity identity)
        where T : class, IRuntimeDiagnosticsSnapshot
        => CreateInstanceTopology(new DiagnosticsCollectionSnapshot<T>(
            CreateIdentityUnstableMetadata(identity),
            records: null,
            capacity: null,
            retention: null,
            droppedCount: null,
            isTruncated: null));

    private static DiagnosticsSnapshotMetadata CreateIdentityUnstableMetadata(
        RuntimeDiagnosticsCaptureIdentity identity)
        => DiagnosticsSnapshotMetadata.Create(
            identity.ServerInstanceId,
            identity.CounterEpoch,
            DiagnosticsScope.Aggregate,
            DiagnosticsAvailability.Unavailable,
            DiagnosticsSource.Client,
            identity.DatabaseAlias,
            timeProvider: identity.TimeProvider);

    private RuntimeDiagnosticsFamilyLease AcquireRuntimeDiagnosticsFamilyLease()
    {
        lock (_runtimeDiagnosticsLifetimeGate)
        {
            ObjectDisposedException.ThrowIf(
                Volatile.Read(ref _disposeStarted),
                this);
            CSharpDbRuntimeDiagnosticsState? publishedCurrent =
                CurrentRuntimeDiagnosticsState;
            CSharpDbRuntimeDiagnosticsState current = publishedCurrent ??
                GetOrCreateDisabledRuntimeDiagnosticsStateLocked();
            var unique = new HashSet<CSharpDbRuntimeDiagnosticsState> { current };
            if (_runtimeDiagnosticsSessionOwners is not null)
                unique.UnionWith(_runtimeDiagnosticsSessionOwners.Keys);
            if (_retiredRuntimeDiagnosticsStates is not null)
                unique.UnionWith(_retiredRuntimeDiagnosticsStates);
            Dictionary<CSharpDbRuntimeDiagnosticsState, long> capturedEpochs =
                unique.ToDictionary(
                    static state => state,
                    static state => state.CounterEpoch);
            (string ServerInstanceId, long CounterEpoch) currentIdentity =
                (current.ServerInstanceId, capturedEpochs[current]);
            CSharpDbRuntimeDiagnosticsState[] logicalFamilies = unique
                .GroupBy(state =>
                    (state.ServerInstanceId, capturedEpochs[state]))
                .Select(group =>
                {
                    CSharpDbRuntimeDiagnosticsState[] candidates = group.ToArray();
                    bool isCurrentIdentity = group.Key == currentIdentity;
                    if (isCurrentIdentity)
                    {
                        // A replacement is published before its first
                        // successful physical open advances the epoch. During
                        // that window, retain the established family's exact
                        // payload rather than publishing two children with one
                        // identity or hiding its still-active transaction work.
                        CSharpDbRuntimeDiagnosticsState? established = candidates
                            .FirstOrDefault(state => !ReferenceEquals(state, current));
                        if (established is not null)
                            return established;
                    }

                    return candidates
                        .OrderByDescending(static state => state.IsEnabled)
                        .ThenBy(static state => state.DatabaseAlias, StringComparer.Ordinal)
                        .First();
                })
                .OrderByDescending(state =>
                    (state.ServerInstanceId, capturedEpochs[state]) == currentIdentity)
                .ThenByDescending(state => capturedEpochs[state])
                .ThenBy(static state => state.DatabaseAlias, StringComparer.Ordinal)
                .ThenBy(static state => state.ServerInstanceId, StringComparer.Ordinal)
                .ToArray();
            CSharpDbRuntimeDiagnosticsState[] selected = logicalFamilies
                .Take(CSharpDbDiagnostics.MaximumRuntimeDiagnosticsFamilies)
                .ToArray();
            CSharpDbRuntimeDiagnosticsState[] retained = selected
                .Append(current)
                .Distinct()
                .ToArray();
            foreach (CSharpDbRuntimeDiagnosticsState state in retained)
                RetainRuntimeDiagnosticsStateLocked(state);

            return new RuntimeDiagnosticsFamilyLease(
                this,
                publishedCurrent,
                current,
                selected,
                retained,
                retained.Select(state => new RuntimeDiagnosticsStateEpoch(
                    state,
                    capturedEpochs[state])).ToArray(),
                Math.Max(0, logicalFamilies.Length - selected.Length),
                new RuntimeDiagnosticsCaptureIdentity(
                    current.ServerInstanceId,
                    capturedEpochs[current],
                    current.DatabaseAlias,
                    current.TimeProvider));
        }
    }

    private CSharpDbRuntimeDiagnosticsState GetOrCreateDisabledRuntimeDiagnosticsStateLocked()
        => _disabledRuntimeDiagnosticsState ??= new CSharpDbRuntimeDiagnosticsState(
            _directDatabaseOptions.ObservabilityOptions,
            _observabilityTimeProvider);

    private static void ValidateDiagnosticsMaximumRecords(int maximumRecords)
    {
        if (maximumRecords <= 0 ||
            maximumRecords > CSharpDbObservabilityOptions.MaximumHistoryCapacity)
        {
            throw new ArgumentOutOfRangeException(nameof(maximumRecords));
        }
    }

    private static DiagnosticsTopologySnapshot<T> CreateInstanceTopology<T>(T aggregate)
        where T : class, IRuntimeDiagnosticsSnapshot
        => new(
            aggregate,
            shards: null,
            shardCapacity: null,
            droppedShardCount: null,
            shardsTruncated: null);

    private static DiagnosticsTopologySnapshot<T> CreateAggregateTopology<T>(
        T aggregate,
        IReadOnlyList<RuntimeDiagnosticsFamilySection<T>> families,
        int droppedFamilyCount)
        where T : class, IRuntimeDiagnosticsSnapshot
        => new(
            aggregate,
            shards: null,
            shardCapacity: null,
            droppedShardCount: null,
            shardsTruncated: null,
            runtimeFamilies: families,
            runtimeFamilyCapacity: CSharpDbDiagnostics.MaximumRuntimeDiagnosticsFamilies,
            droppedRuntimeFamilyCount: droppedFamilyCount,
            runtimeFamiliesTruncated: droppedFamilyCount > 0);

    private static int SaturatingAdd(int left, int right)
        => left >= int.MaxValue - right ? int.MaxValue : left + right;

    private static long SaturatingAdd(long left, long right)
        => right <= 0
            ? left
            : left >= long.MaxValue - right
                ? long.MaxValue
                : left + right;

    private sealed record ConnectionCapture(
        ClientTransactionSession[] Sessions,
        int? TransactionCount,
        int CachedActiveReaders,
        bool FieldsTruncated,
        bool ExclusiveMaintenanceActive);

    private readonly record struct RuntimeDiagnosticsCaptureIdentity(
        string ServerInstanceId,
        long CounterEpoch,
        string DatabaseAlias,
        TimeProvider TimeProvider);

    private readonly record struct RuntimeDiagnosticsStateEpoch(
        CSharpDbRuntimeDiagnosticsState State,
        long CounterEpoch);

    private sealed class RuntimeDiagnosticsFamilyLease : IDisposable
    {
        private EngineTransportClient? _owner;
        private readonly CSharpDbRuntimeDiagnosticsState? _publishedCurrent;
        private readonly IReadOnlyList<RuntimeDiagnosticsStateEpoch>
            _capturedEpochs;

        // Narrow compatibility seam for the deterministic dropped-family
        // topology canary. Production acquisition always uses the fully
        // captured constructor below.
        internal RuntimeDiagnosticsFamilyLease(
            EngineTransportClient owner,
            CSharpDbRuntimeDiagnosticsState current,
            CSharpDbRuntimeDiagnosticsState[] states,
            CSharpDbRuntimeDiagnosticsState[] retainedStates,
            int droppedCount)
            : this(
                owner,
                current,
                current,
                states,
                retainedStates,
                states
                    .Append(current)
                    .Distinct()
                    .Select(static state => new RuntimeDiagnosticsStateEpoch(
                        state,
                        state.CounterEpoch))
                    .ToArray(),
                droppedCount,
                new RuntimeDiagnosticsCaptureIdentity(
                    current.ServerInstanceId,
                    current.CounterEpoch,
                    current.DatabaseAlias,
                    current.TimeProvider))
        {
        }

        internal RuntimeDiagnosticsFamilyLease(
            EngineTransportClient owner,
            CSharpDbRuntimeDiagnosticsState? publishedCurrent,
            CSharpDbRuntimeDiagnosticsState current,
            CSharpDbRuntimeDiagnosticsState[] states,
            CSharpDbRuntimeDiagnosticsState[] retainedStates,
            RuntimeDiagnosticsStateEpoch[] capturedEpochs,
            int droppedCount,
            RuntimeDiagnosticsCaptureIdentity captureIdentity)
        {
            _owner = owner;
            _publishedCurrent = publishedCurrent;
            _capturedEpochs = capturedEpochs;
            Current = current;
            States = states;
            RetainedStates = retainedStates;
            DroppedCount = droppedCount;
            CaptureIdentity = captureIdentity;
        }

        internal CSharpDbRuntimeDiagnosticsState Current { get; }
        internal IReadOnlyList<CSharpDbRuntimeDiagnosticsState> States { get; }
        private IReadOnlyList<CSharpDbRuntimeDiagnosticsState> RetainedStates { get; }
        internal int DroppedCount { get; }
        internal RuntimeDiagnosticsCaptureIdentity CaptureIdentity { get; }

        internal bool HaveSameCapturedIdentity(
            CSharpDbRuntimeDiagnosticsState left,
            CSharpDbRuntimeDiagnosticsState right)
        {
            if (!string.Equals(
                    left.ServerInstanceId,
                    right.ServerInstanceId,
                    StringComparison.Ordinal))
            {
                return false;
            }

            long? leftEpoch = null;
            long? rightEpoch = null;
            foreach (RuntimeDiagnosticsStateEpoch capture in _capturedEpochs)
            {
                if (ReferenceEquals(capture.State, left))
                    leftEpoch = capture.CounterEpoch;
                if (ReferenceEquals(capture.State, right))
                    rightEpoch = capture.CounterEpoch;
            }

            return leftEpoch.HasValue &&
                rightEpoch.HasValue &&
                leftEpoch.Value == rightEpoch.Value;
        }

        internal bool IsIdentityStable()
        {
            EngineTransportClient? owner = Volatile.Read(ref _owner);
            if (owner is null ||
                !ReferenceEquals(
                    owner.CurrentRuntimeDiagnosticsState,
                    _publishedCurrent))
            {
                return false;
            }

            foreach (RuntimeDiagnosticsStateEpoch capture in _capturedEpochs)
            {
                if (capture.State.CounterEpoch != capture.CounterEpoch)
                    return false;
            }

            return true;
        }

        public void Dispose()
        {
            EngineTransportClient? owner = Interlocked.Exchange(ref _owner, null);
            if (owner is null)
                return;

            foreach (CSharpDbRuntimeDiagnosticsState state in RetainedStates)
                owner.ReleaseRuntimeDiagnosticsStateOwnership(state);
        }
    }
}
