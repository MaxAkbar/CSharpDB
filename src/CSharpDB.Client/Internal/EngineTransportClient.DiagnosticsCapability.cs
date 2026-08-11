using CSharpDB.Engine;
using CSharpDB.Observability;

namespace CSharpDB.Client.Internal;

internal sealed partial class EngineTransportClient
{
    public Task<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>
        GetRuntimeDiagnosticsAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        using RuntimeDiagnosticsFamilyLease families =
            AcquireRuntimeDiagnosticsFamilyLease();
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> result =
            CreateRuntimeDiagnosticsTopology(families);
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(result);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>
        GetActiveQueriesAsync(int maximumRecords, CancellationToken ct = default)
    {
        ValidateDiagnosticsMaximumRecords(maximumRecords);
        ct.ThrowIfCancellationRequested();
        using RuntimeDiagnosticsFamilyLease families =
            AcquireRuntimeDiagnosticsFamilyLease();
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>> result =
            CreateActiveQueryTopology(families, maximumRecords);
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(result);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>>
        GetRecentQueriesAsync(int maximumRecords, CancellationToken ct = default)
    {
        ValidateDiagnosticsMaximumRecords(maximumRecords);
        ct.ThrowIfCancellationRequested();
        using RuntimeDiagnosticsFamilyLease families =
            AcquireRuntimeDiagnosticsFamilyLease();
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>> result =
            CreateRecentQueryTopology(families, maximumRecords);
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(result);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>>
        GetQueryPlanDiagnosticsAsync(
            OpaqueDiagnosticsId operationId,
            CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operationId);
        ct.ThrowIfCancellationRequested();
        using RuntimeDiagnosticsFamilyLease families =
            AcquireRuntimeDiagnosticsFamilyLease();
        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>> result =
            CreateQueryPlanTopology(families, operationId);
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(result);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>>
        GetSessionsAsync(int maximumRecords, CancellationToken ct = default)
    {
        ValidateDiagnosticsMaximumRecords(maximumRecords);
        ct.ThrowIfCancellationRequested();
        using RuntimeDiagnosticsFamilyLease families =
            AcquireRuntimeDiagnosticsFamilyLease();
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>> result =
            CreateSessionTopology(families.Current, maximumRecords);
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(result);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>>
        GetQueryDetailAsync(
            OpaqueDiagnosticsId operationId,
            CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operationId);
        ct.ThrowIfCancellationRequested();
        using RuntimeDiagnosticsFamilyLease families =
            AcquireRuntimeDiagnosticsFamilyLease();
        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>> result =
            CreateQueryDetailTopology(families, operationId);
        ct.ThrowIfCancellationRequested();
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
            DiagnosticsSnapshotMetadata metadata = families.Current.CreateMetadata(
                DiagnosticsScope.Instance,
                DiagnosticsAvailability.Available,
                DiagnosticsSource.Client,
                fieldsTruncated: connectionCapture.FieldsTruncated);
            QueryDiagnosticsSummary raw = QueryRuntimeDiagnostics
                .GetOrCreate(families.Current)
                .GetSummary();
            QueryDiagnosticsSummary queries = raw with { Metadata = metadata };
            ConnectionDiagnosticsSnapshot connections = CreateConnectionSnapshot(
                metadata,
                connectionCapture);
            var snapshot = new RuntimeDiagnosticsSnapshot(
                metadata,
                DiagnosticsSection<QueryDiagnosticsSummary>.Available(queries),
                DiagnosticsSection<ConnectionDiagnosticsSnapshot>.Available(connections),
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

        bool hasEnabledFamily = families.States.Any(static state => state.IsEnabled);
        RuntimeDiagnosticsFamilySection<RuntimeDiagnosticsSnapshot>[] exactFamilies =
            families.States
                .Select(CreateExactRuntimeSummaryFamily)
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
        DiagnosticsSnapshotMetadata aggregateMetadata = families.Current.CreateMetadata(
            DiagnosticsScope.Aggregate,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Client,
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
            DiagnosticsSection<MaintenanceOperationSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<HealthDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable));
        return CreateAggregateTopology(
            aggregateSnapshot,
            exactFamilies,
            families.DroppedCount);
    }

    private static RuntimeDiagnosticsFamilySection<RuntimeDiagnosticsSnapshot>
        CreateExactRuntimeSummaryFamily(CSharpDbRuntimeDiagnosticsState state)
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

        DiagnosticsSnapshotMetadata metadata = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine);
        QueryDiagnosticsSummary raw = QueryRuntimeDiagnostics
            .GetOrCreate(state)
            .GetSummary();
        var snapshot = new RuntimeDiagnosticsSnapshot(
            metadata,
            DiagnosticsSection<QueryDiagnosticsSummary>.Available(
                raw with { Metadata = metadata }),
            DiagnosticsSection<ConnectionDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.NotApplicable),
            DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<MaintenanceOperationSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<HealthDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable));
        return new RuntimeDiagnosticsFamilySection<RuntimeDiagnosticsSnapshot>(
            state.DatabaseAlias,
            snapshot);
    }

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

    private RuntimeDiagnosticsFamilyLease AcquireRuntimeDiagnosticsFamilyLease()
    {
        lock (_runtimeDiagnosticsLifetimeGate)
        {
            ObjectDisposedException.ThrowIf(
                Volatile.Read(ref _disposeStarted),
                this);
            CSharpDbRuntimeDiagnosticsState current =
                CurrentRuntimeDiagnosticsState ??
                GetOrCreateDisabledRuntimeDiagnosticsStateLocked();
            var unique = new HashSet<CSharpDbRuntimeDiagnosticsState> { current };
            if (_runtimeDiagnosticsSessionOwners is not null)
                unique.UnionWith(_runtimeDiagnosticsSessionOwners.Keys);
            if (_retiredRuntimeDiagnosticsStates is not null)
                unique.UnionWith(_retiredRuntimeDiagnosticsStates);
            (string ServerInstanceId, long CounterEpoch) currentIdentity =
                (current.ServerInstanceId, current.CounterEpoch);
            CSharpDbRuntimeDiagnosticsState[] logicalFamilies = unique
                .GroupBy(static state =>
                    (state.ServerInstanceId, state.CounterEpoch))
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
                    (state.ServerInstanceId, state.CounterEpoch) == currentIdentity)
                .ThenByDescending(static state => state.CounterEpoch)
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
                current,
                selected,
                retained,
                Math.Max(0, logicalFamilies.Length - selected.Length));
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

    private sealed class RuntimeDiagnosticsFamilyLease : IDisposable
    {
        private EngineTransportClient? _owner;

        internal RuntimeDiagnosticsFamilyLease(
            EngineTransportClient owner,
            CSharpDbRuntimeDiagnosticsState current,
            CSharpDbRuntimeDiagnosticsState[] states,
            CSharpDbRuntimeDiagnosticsState[] retainedStates,
            int droppedCount)
        {
            _owner = owner;
            Current = current;
            States = states;
            RetainedStates = retainedStates;
            DroppedCount = droppedCount;
        }

        internal CSharpDbRuntimeDiagnosticsState Current { get; }
        internal IReadOnlyList<CSharpDbRuntimeDiagnosticsState> States { get; }
        private IReadOnlyList<CSharpDbRuntimeDiagnosticsState> RetainedStates { get; }
        internal int DroppedCount { get; }

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
