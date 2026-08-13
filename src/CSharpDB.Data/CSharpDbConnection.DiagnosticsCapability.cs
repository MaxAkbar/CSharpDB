using System.Data;
using System.Runtime.CompilerServices;
using CSharpDB.Client;
using CSharpDB.Engine;
using CSharpDB.Observability;

namespace CSharpDB.Data;

public sealed partial class CSharpDbConnection : ICSharpDbObservabilityClient
{
    private static readonly ConditionalWeakTable<object, DiagnosticsOwnerIdentity>
        s_runtimeDiagnosticsOwnerIdentities = new();

    public Task<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>
        GetRuntimeDiagnosticsAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        DiagnosticsTarget target = CaptureDiagnosticsTarget();
        if (target.RemoteClient is { } remote)
        {
            return DelegateRemoteAsync(
                () => remote.GetRuntimeDiagnosticsAsync(ct));
        }

        return CaptureEmbeddedRuntimeDiagnosticsAsync(target, ct);
    }

    public async Task<DiagnosticsTopologySnapshot<
        DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot>>>
        GetStorageDiagnosticsAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        DiagnosticsTarget target = CaptureDiagnosticsTarget();
        if (target.RemoteClient is { } remote)
        {
            return await DelegateRemoteAsync(
                () => remote.GetStorageDiagnosticsAsync(ct));
        }

        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> runtime =
            await CaptureEmbeddedRuntimeDiagnosticsAsync(target, ct);
        DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot> value =
            CreateValueFromRuntimeSection(
                runtime.Aggregate.Metadata,
                runtime.Aggregate.Storage,
                ProjectStorageSnapshot);
        ct.ThrowIfCancellationRequested();
        return CreateInstanceTopology(value);
    }

    public async Task<DiagnosticsTopologySnapshot<
        DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot>>>
        GetWalDiagnosticsAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        DiagnosticsTarget target = CaptureDiagnosticsTarget();
        if (target.RemoteClient is { } remote)
        {
            return await DelegateRemoteAsync(
                () => remote.GetWalDiagnosticsAsync(ct));
        }

        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> runtime =
            await CaptureEmbeddedRuntimeDiagnosticsAsync(target, ct);
        DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot> value =
            CreateValueFromRuntimeSection(
                runtime.Aggregate.Metadata,
                runtime.Aggregate.Wal,
                ProjectWalSnapshot);
        ct.ThrowIfCancellationRequested();
        return CreateInstanceTopology(value);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>
        GetActiveQueriesAsync(
            int maximumRecords,
            CancellationToken ct = default)
    {
        ValidateMaximumRecords(maximumRecords);
        ct.ThrowIfCancellationRequested();
        DiagnosticsTarget target = CaptureDiagnosticsTarget();
        if (target.RemoteClient is { } remote)
        {
            return DelegateRemoteAsync(
                () => remote.GetActiveQueriesAsync(maximumRecords, ct));
        }

        DiagnosticsCollectionSnapshot<ActiveQuerySnapshot> collection =
            CaptureQueryCollection(
                target,
                diagnostics => diagnostics.GetActiveCollectionSnapshot(maximumRecords));
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(CreateInstanceTopology(collection));
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>>
        GetRecentQueriesAsync(
            int maximumRecords,
            CancellationToken ct = default)
    {
        ValidateMaximumRecords(maximumRecords);
        ct.ThrowIfCancellationRequested();
        DiagnosticsTarget target = CaptureDiagnosticsTarget();
        if (target.RemoteClient is { } remote)
        {
            return DelegateRemoteAsync(
                () => remote.GetRecentQueriesAsync(maximumRecords, ct));
        }

        DiagnosticsCollectionSnapshot<RecentQuerySnapshot> collection =
            CaptureQueryCollection(
                target,
                diagnostics => diagnostics.GetRecentCollectionSnapshot(maximumRecords));
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(CreateInstanceTopology(collection));
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>>
        GetQueryPlanDiagnosticsAsync(
            OpaqueDiagnosticsId operationId,
            CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operationId);
        ct.ThrowIfCancellationRequested();
        DiagnosticsTarget target = CaptureDiagnosticsTarget();
        if (target.RemoteClient is { } remote)
        {
            return DelegateRemoteAsync(
                () => remote.GetQueryPlanDiagnosticsAsync(operationId, ct));
        }

        DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot> value =
            CaptureQueryValue(
                target,
                diagnostics => diagnostics.GetPlanSnapshot(operationId));
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(CreateInstanceTopology(value));
    }

    public async Task<DiagnosticsTopologySnapshot<
        DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>>
        GetSessionsAsync(
            int maximumRecords,
            CancellationToken ct = default)
    {
        ValidateMaximumRecords(maximumRecords);
        ct.ThrowIfCancellationRequested();
        DiagnosticsTarget target = CaptureDiagnosticsTarget();
        if (target.RemoteClient is { } remote)
        {
            return await DelegateRemoteAsync(
                () => remote.GetSessionsAsync(maximumRecords, ct));
        }

        DiagnosticsAvailability availability = GetEmbeddedAvailability(target);
        if (availability != DiagnosticsAvailability.Available)
        {
            DiagnosticsSnapshotMetadata metadata = CreateMetadata(
                target,
                availability,
                DiagnosticsSource.Client);
            return CreateInstanceTopology(
                CreateUnavailableCollection<SessionDiagnosticsSnapshot>(metadata));
        }

        DataConnectionDiagnosticsRawSnapshot? raw;
        try
        {
            raw = await target.Contributor!.CaptureRuntimeDiagnosticsAsync(
                CSharpDbObservabilityOptions.MaximumActiveOperationCapacity,
                ct);
        }
        catch (ObjectDisposedException)
        {
            return CreateInstanceTopology(
                CreateUnavailableCollection<SessionDiagnosticsSnapshot>(
                    CreateMetadata(
                        target,
                        DiagnosticsAvailability.Unavailable,
                        DiagnosticsSource.Client)));
        }

        ct.ThrowIfCancellationRequested();
        if (raw is null)
        {
            return CreateInstanceTopology(
                CreateUnavailableCollection<SessionDiagnosticsSnapshot>(
                    CreateMetadata(
                        target,
                        DiagnosticsAvailability.Unavailable,
                        DiagnosticsSource.Client)));
        }

        DataSessionDiagnosticsRawSnapshot[] ordered = raw.Sessions
            .OrderBy(GetSessionSelectionPriority)
            .ThenBy(static session => session.CreatedAtUtc)
            .ThenBy(static session => session.SessionId.Value, StringComparer.Ordinal)
            .ToArray();
        int take = Math.Min(maximumRecords, ordered.Length);
        long dropped = Math.Max(0, raw.DroppedSessionCount);
        bool truncated = raw.SessionsTruncated || ordered.Length > take || dropped > 0;
        DiagnosticsSnapshotMetadata availableMetadata = CreateMetadata(
            target,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Client,
            recordsTruncated: truncated);
        SessionDiagnosticsSnapshot[] sessions = ordered
            .Take(take)
            .Select(session => CreateSessionSnapshot(availableMetadata, session))
            .ToArray();
        var collection = new DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>(
            availableMetadata,
            sessions,
            maximumRecords,
            retention: null,
            dropped,
            truncated);
        return CreateInstanceTopology(collection);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>>
        GetQueryDetailAsync(
            OpaqueDiagnosticsId operationId,
            CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operationId);
        ct.ThrowIfCancellationRequested();
        DiagnosticsTarget target = CaptureDiagnosticsTarget();
        if (target.RemoteClient is { } remote)
        {
            return DelegateRemoteAsync(
                () => remote.GetQueryDetailAsync(operationId, ct));
        }

        DiagnosticsValueSnapshot<QueryDetailSnapshot> value =
            CaptureQueryValue(
                target,
                diagnostics => diagnostics.GetQueryDetailSnapshot(operationId));
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(CreateInstanceTopology(value));
    }

    public Task<DiagnosticsTopologySnapshot<
        DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>>
        GetActiveMaintenanceOperationsAsync(
            int maximumRecords,
            CancellationToken ct = default)
        => GetMaintenanceOperationsAsync(
            maximumRecords,
            recent: false,
            ct);

    public Task<DiagnosticsTopologySnapshot<
        DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>>
        GetRecentMaintenanceOperationsAsync(
            int maximumRecords,
            CancellationToken ct = default)
        => GetMaintenanceOperationsAsync(
            maximumRecords,
            recent: true,
            ct);

    private Task<DiagnosticsTopologySnapshot<
        DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>>
        GetMaintenanceOperationsAsync(
            int maximumRecords,
            bool recent,
            CancellationToken ct)
    {
        ValidateMaximumRecords(maximumRecords);
        ct.ThrowIfCancellationRequested();
        DiagnosticsTarget target = CaptureDiagnosticsTarget();
        if (target.RemoteClient is { } remote)
        {
            return DelegateRemoteAsync(
                () => recent
                    ? remote.GetRecentMaintenanceOperationsAsync(
                        maximumRecords,
                        ct)
                    : remote.GetActiveMaintenanceOperationsAsync(
                        maximumRecords,
                        ct));
        }

        if (GetEmbeddedHistoryAvailability(target) ==
            DiagnosticsAvailability.Disabled)
        {
            DiagnosticsSnapshotMetadata metadata = CreateMetadata(
                target,
                DiagnosticsAvailability.Disabled,
                DiagnosticsSource.Engine);
            return Task.FromResult(CreateInstanceTopology(
                CreateUnavailableCollection<MaintenanceOperationSnapshot>(
                    metadata)));
        }

        throw new CSharpDbObservabilityNotSupportedException();
    }

    private async Task<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>
        CaptureEmbeddedRuntimeDiagnosticsAsync(
            DiagnosticsTarget target,
            CancellationToken ct)
    {
        DiagnosticsAvailability availability = GetEmbeddedAvailability(target);
        if (availability != DiagnosticsAvailability.Available)
        {
            return CreateInstanceTopology(
                CreateRuntimeWithoutValues(
                    CreateMetadata(
                        target,
                        availability,
                        DiagnosticsSource.Client)));
        }

        QueryDiagnosticsSummary? querySummary = null;
        if (target.RuntimeState!.HistoryEnabled)
        {
            try
            {
                querySummary = QueryRuntimeDiagnostics
                    .GetOrCreate(target.RuntimeState)
                    .GetSummary();
            }
            catch (ObjectDisposedException)
            {
                return CreateInstanceTopology(
                    CreateRuntimeWithoutValues(
                        CreateMetadata(
                            target,
                            DiagnosticsAvailability.Unavailable,
                            DiagnosticsSource.Client)));
            }
        }

        DataConnectionDiagnosticsRawSnapshot? raw;
        try
        {
            raw = await target.Contributor!.CaptureRuntimeDiagnosticsAsync(
                CSharpDbObservabilityOptions.MaximumActiveOperationCapacity,
                ct);
        }
        catch (ObjectDisposedException)
        {
            return CreateInstanceTopology(
                CreateRuntimeWithoutValues(
                    CreateMetadata(
                        target,
                        DiagnosticsAvailability.Unavailable,
                        DiagnosticsSource.Client)));
        }

        ct.ThrowIfCancellationRequested();
        DiagnosticsSnapshotMetadata metadata = CreateMetadata(
            target,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Client);
        QueryDiagnosticsSummary? projectedQuerySummary =
            querySummary is null
                ? null
                : querySummary with { Metadata = metadata };
        DiagnosticsSection<ConnectionDiagnosticsSnapshot> connectionSection =
            raw is null
                ? DiagnosticsSection<ConnectionDiagnosticsSnapshot>.WithoutValue(
                    DiagnosticsAvailability.Unavailable)
                : DiagnosticsSection<ConnectionDiagnosticsSnapshot>.Available(
                    CreateConnectionSnapshot(metadata, raw));
        var snapshot = new RuntimeDiagnosticsSnapshot(
            metadata,
            projectedQuerySummary is null
                ? DiagnosticsSection<QueryDiagnosticsSummary>.WithoutValue(
                    DiagnosticsAvailability.Disabled)
                : DiagnosticsSection<QueryDiagnosticsSummary>.Available(
                    projectedQuerySummary),
            connectionSection,
            DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<MaintenanceOperationSnapshot>.WithoutValue(
                target.RuntimeState.HistoryEnabled
                    ? DiagnosticsAvailability.Unavailable
                    : DiagnosticsAvailability.Disabled),
            DiagnosticsSection<HealthDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable));
        return CreateInstanceTopology(snapshot);
    }

    private static DiagnosticsCollectionSnapshot<T> CaptureQueryCollection<T>(
        DiagnosticsTarget target,
        Func<QueryRuntimeDiagnostics, DiagnosticsCollectionSnapshot<T>> capture)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        DiagnosticsAvailability availability =
            GetEmbeddedHistoryAvailability(target);
        if (availability != DiagnosticsAvailability.Available)
        {
            return CreateUnavailableCollection<T>(
                CreateMetadata(
                    target,
                    availability,
                    DiagnosticsSource.Engine));
        }

        try
        {
            return capture(QueryRuntimeDiagnostics.GetOrCreate(target.RuntimeState!));
        }
        catch (ObjectDisposedException)
        {
            return CreateUnavailableCollection<T>(
                CreateMetadata(
                    target,
                    DiagnosticsAvailability.Unavailable,
                    DiagnosticsSource.Engine));
        }
    }

    private static DiagnosticsValueSnapshot<T> CaptureQueryValue<T>(
        DiagnosticsTarget target,
        Func<QueryRuntimeDiagnostics, T?> capture)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        DiagnosticsAvailability availability =
            GetEmbeddedHistoryAvailability(target);
        if (availability != DiagnosticsAvailability.Available)
        {
            return new DiagnosticsValueSnapshot<T>(
                CreateMetadata(
                    target,
                    availability,
                    DiagnosticsSource.Engine),
                value: null);
        }

        try
        {
            T? value = capture(QueryRuntimeDiagnostics.GetOrCreate(target.RuntimeState!));
            if (value is not null)
                return new DiagnosticsValueSnapshot<T>(value.Metadata, value);
        }
        catch (ObjectDisposedException)
        {
            // The exact physical owner retired between target capture and the
            // component read. Retain its opaque identity but publish no value.
        }

        return new DiagnosticsValueSnapshot<T>(
            CreateMetadata(
                target,
                DiagnosticsAvailability.Unavailable,
                DiagnosticsSource.Engine),
            value: null);
    }

    private static DiagnosticsValueSnapshot<T> CreateValueFromRuntimeSection<T>(
        DiagnosticsSnapshotMetadata runtimeMetadata,
        DiagnosticsSection<T> section,
        Func<T, DiagnosticsSnapshotMetadata, T> project)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        ArgumentNullException.ThrowIfNull(runtimeMetadata);
        ArgumentNullException.ThrowIfNull(section);
        DiagnosticsSnapshotMetadata metadata = new(
            runtimeMetadata.SchemaVersion,
            runtimeMetadata.CapturedAtUtc,
            runtimeMetadata.ServerInstanceId,
            runtimeMetadata.CounterEpoch,
            runtimeMetadata.Scope,
            section.Availability,
            runtimeMetadata.Source,
            runtimeMetadata.DatabaseAlias,
            recordsTruncated: false,
            fieldsTruncated: section.Value is not null &&
                runtimeMetadata.FieldsTruncated);
        return new DiagnosticsValueSnapshot<T>(
            metadata,
            section.Value is { } value ? project(value, metadata) : null);
    }

    private static StorageRuntimeDiagnosticsSnapshot ProjectStorageSnapshot(
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
            Cache = ProjectDetailSection(
                value.Cache,
                metadata,
                ProjectStorageCacheSnapshot),
            PhysicalIo = ProjectDetailSection(
                value.PhysicalIo,
                metadata,
                ProjectStorageDeviceIoSnapshot),
        };

    private static StorageCacheDiagnosticsSnapshot ProjectStorageCacheSnapshot(
        StorageCacheDiagnosticsSnapshot value,
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            value.SharedResidentPages,
            value.SharedCapacityPages,
            value.WalResidentPages,
            value.WalCapacityPages);

    private static StorageDeviceIoDiagnosticsSnapshot
        ProjectStorageDeviceIoSnapshot(
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

    private static WalRuntimeDiagnosticsSnapshot ProjectWalSnapshot(
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
            Recovery = ProjectDetailSection(
                value.Recovery,
                metadata,
                ProjectWalRecoverySnapshot),
            Checkpoint = ProjectDetailSection(
                value.Checkpoint,
                metadata,
                ProjectCheckpointSnapshot),
        };

    private static WalRecoveryDiagnosticsSnapshot ProjectWalRecoverySnapshot(
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

    private static CheckpointDiagnosticsSnapshot ProjectCheckpointSnapshot(
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

    private static DiagnosticsSection<T> ProjectDetailSection<T>(
        DiagnosticsSection<T> section,
        DiagnosticsSnapshotMetadata metadata,
        Func<T, DiagnosticsSnapshotMetadata, T> project)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        try
        {
            return section.Value is null
                ? DiagnosticsSection<T>.WithoutValue(section.Availability)
                : DiagnosticsSection<T>.Available(
                    project(section.Value, metadata));
        }
        catch
        {
            return DiagnosticsSection<T>.WithoutValue(
                DiagnosticsAvailability.Unavailable);
        }
    }

    private DiagnosticsTarget CaptureDiagnosticsTarget()
    {
        ICSharpDbSession? session = Volatile.Read(ref _session);
        if (session is null || _state != ConnectionState.Open)
            throw new InvalidOperationException("Connection is not open.");

        ICSharpDbObservabilityClient? remoteClient =
            session.RemoteObservabilityClient;
        IDataRuntimeDiagnosticsContributor? contributor =
            session.RuntimeDiagnosticsContributor;
        object? identityKey = session.RuntimeDiagnosticsIdentityKey;
        if (contributor is null)
        {
            if (remoteClient is null)
                throw new CSharpDbObservabilityNotSupportedException();

            return new DiagnosticsTarget(
                remoteClient,
                IdentityKey: null,
                Contributor: null,
                Options: null,
                RuntimeState: null);
        }

        if (identityKey is null)
            throw new CSharpDbObservabilityNotSupportedException();

        CSharpDbObservabilityOptions? options = session.ObservabilityOptionsSnapshot;
        CSharpDbRuntimeDiagnosticsState? runtimeState;
        try
        {
            runtimeState = session.RuntimeDiagnosticsState;
        }
        catch (ObjectDisposedException)
        {
            runtimeState = null;
        }

        var target = new DiagnosticsTarget(
            RemoteClient: null,
            IdentityKey: identityKey,
            Contributor: contributor,
            Options: options,
            RuntimeState: runtimeState);
        if (runtimeState is not null)
        {
            _ = s_runtimeDiagnosticsOwnerIdentities.GetValue(
                identityKey,
                _ => new DiagnosticsOwnerIdentity(runtimeState));
        }
        return target;
    }

    private static DiagnosticsAvailability GetEmbeddedAvailability(
        DiagnosticsTarget target)
    {
        if (target.RuntimeState is { } state)
        {
            return state.IsEnabled
                ? DiagnosticsAvailability.Available
                : DiagnosticsAvailability.Disabled;
        }

        return target.Options?.Enabled == true
            ? DiagnosticsAvailability.Unavailable
            : DiagnosticsAvailability.Disabled;
    }

    private static DiagnosticsAvailability GetEmbeddedHistoryAvailability(
        DiagnosticsTarget target)
    {
        if (target.RuntimeState is { } state)
        {
            return state.HistoryEnabled
                ? DiagnosticsAvailability.Available
                : DiagnosticsAvailability.Disabled;
        }

        return target.Options?.Enabled == true &&
               target.Options.History.Enabled
            ? DiagnosticsAvailability.Unavailable
            : DiagnosticsAvailability.Disabled;
    }

    private static DiagnosticsSnapshotMetadata CreateMetadata(
        DiagnosticsTarget target,
        DiagnosticsAvailability availability,
        DiagnosticsSource source,
        bool recordsTruncated = false,
        bool fieldsTruncated = false)
    {
        if (target.RuntimeState is { } state)
        {
            return state.CreateMetadata(
                DiagnosticsScope.Instance,
                availability,
                source,
                recordsTruncated: recordsTruncated,
                fieldsTruncated: fieldsTruncated);
        }

        DiagnosticsOwnerIdentity identity =
            s_runtimeDiagnosticsOwnerIdentities.GetValue(
                target.IdentityKey!,
                _ => new DiagnosticsOwnerIdentity(target.Options));
        return identity.CreateMetadata(
            availability,
            source,
            recordsTruncated,
            fieldsTruncated);
    }

    private static RuntimeDiagnosticsSnapshot CreateRuntimeWithoutValues(
        DiagnosticsSnapshotMetadata metadata)
    {
        DiagnosticsAvailability availability = metadata.Availability;
        return new RuntimeDiagnosticsSnapshot(
            metadata,
            DiagnosticsSection<QueryDiagnosticsSummary>.WithoutValue(availability),
            DiagnosticsSection<ConnectionDiagnosticsSnapshot>.WithoutValue(availability),
            DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.WithoutValue(availability),
            DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.WithoutValue(availability),
            DiagnosticsSection<MaintenanceOperationSnapshot>.WithoutValue(availability),
            DiagnosticsSection<HealthDiagnosticsSnapshot>.WithoutValue(availability));
    }

    private static DiagnosticsCollectionSnapshot<T> CreateUnavailableCollection<T>(
        DiagnosticsSnapshotMetadata metadata)
        where T : class, IRuntimeDiagnosticsSnapshot
        => new(
            metadata,
            records: null,
            capacity: null,
            retention: null,
            droppedCount: null,
            isTruncated: null);

    private static ConnectionDiagnosticsSnapshot CreateConnectionSnapshot(
        DiagnosticsSnapshotMetadata metadata,
        DataConnectionDiagnosticsRawSnapshot raw)
        => new(
            metadata,
            NonNegative(raw.PoolCapacity),
            NonNegative(raw.AvailableSlots),
            NonNegative(raw.WaiterCount),
            NonNegative(raw.ActiveLogicalSessions),
            NonNegative(raw.ActiveReaders),
            NonNegative(raw.ActiveTransactions),
            NonNegative(raw.RetiredPoolCount),
            NonNegative(raw.PoisonedPoolCount),
            NonNegative(raw.OldestTransactionAge))
        {
            WarmEngineIdleCount = NonNegative(raw.WarmEngineIdleCount),
            DisabledPoolCount = NonNegative(raw.DisabledPoolCount),
            RetiringPoolCount = NonNegative(raw.RetiringPoolCount),
            TransactionOwnerSessionId = raw.TransactionOwnerSessionId,
            PoolState = raw.PoolState is { } poolState && Enum.IsDefined(poolState)
                ? poolState
                : ConnectionPoolLifecycleState.Unknown,
            ExclusiveMaintenanceActive = null,
        };

    private static SessionDiagnosticsSnapshot CreateSessionSnapshot(
        DiagnosticsSnapshotMetadata metadata,
        DataSessionDiagnosticsRawSnapshot raw)
    {
        DateTimeOffset createdAtUtc = ToUtc(raw.CreatedAtUtc);
        DateTimeOffset lastActiveAtUtc = ToUtc(raw.LastActiveAtUtc);
        if (lastActiveAtUtc < createdAtUtc)
            lastActiveAtUtc = createdAtUtc;

        CSharpDB.Observability.CSharpDbTransport transport =
            Enum.IsDefined(raw.Transport) &&
            raw.Transport != CSharpDB.Observability.CSharpDbTransport.Unknown
                ? raw.Transport
                : CSharpDB.Observability.CSharpDbTransport.Direct;
        DiagnosticsSessionState state = Enum.IsDefined(raw.State)
            ? raw.State
            : DiagnosticsSessionState.Unknown;
        return new SessionDiagnosticsSnapshot(
            metadata,
            raw.SessionId,
            createdAtUtc,
            lastActiveAtUtc,
            raw.CurrentOperationId,
            raw.HasActiveReader,
            raw.HasActiveTransaction,
            transport)
        {
            State = state,
        };
    }

    private static int GetSessionSelectionPriority(
        DataSessionDiagnosticsRawSnapshot session)
    {
        if (session.CurrentOperationId is not null ||
            session.State == DiagnosticsSessionState.Active)
        {
            return 0;
        }
        if (session.HasActiveReader ||
            session.State == DiagnosticsSessionState.SnapshotReader)
        {
            return 1;
        }
        if (session.HasActiveTransaction ||
            session.State == DiagnosticsSessionState.Transaction)
        {
            return 2;
        }
        if (session.State == DiagnosticsSessionState.Abandoned)
            return 3;
        return 4;
    }

    private static DiagnosticsTopologySnapshot<T> CreateInstanceTopology<T>(
        T aggregate)
        where T : class, IRuntimeDiagnosticsSnapshot
        => new(
            aggregate,
            shards: null,
            shardCapacity: null,
            droppedShardCount: null,
            shardsTruncated: null);

    private static async Task<T> DelegateRemoteAsync<T>(Func<Task<T>> operation)
    {
        try
        {
            return await operation();
        }
        catch (CSharpDbObservabilityNotSupportedException)
        {
            throw;
        }
        catch (ObjectDisposedException exception)
        {
            throw new CSharpDbObservabilityNotSupportedException(exception);
        }
    }

    private static void ValidateMaximumRecords(int maximumRecords)
    {
        if (maximumRecords <= 0 ||
            maximumRecords > CSharpDbObservabilityOptions.MaximumHistoryCapacity)
        {
            throw new ArgumentOutOfRangeException(nameof(maximumRecords));
        }
    }

    private static int NonNegative(int value) => Math.Max(0, value);

    private static int? NonNegative(int? value)
        => value is null ? null : Math.Max(0, value.Value);

    private static TimeSpan? NonNegative(TimeSpan? value)
        => value is null
            ? null
            : value.Value < TimeSpan.Zero
                ? TimeSpan.Zero
                : value.Value;

    private static DateTimeOffset ToUtc(DateTimeOffset value)
        => value.Offset == TimeSpan.Zero ? value : value.ToUniversalTime();

    private sealed record DiagnosticsTarget(
        ICSharpDbObservabilityClient? RemoteClient,
        object? IdentityKey,
        IDataRuntimeDiagnosticsContributor? Contributor,
        CSharpDbObservabilityOptions? Options,
        CSharpDbRuntimeDiagnosticsState? RuntimeState);

    private sealed class DiagnosticsOwnerIdentity
    {
        private readonly string _serverInstanceId =
            CSharpDbDiagnostics.CreateServerInstanceId();
        private readonly string _databaseAlias;

        internal DiagnosticsOwnerIdentity(CSharpDbObservabilityOptions? options)
        {
            string? configuredAlias = options?.DatabaseAlias;
            _databaseAlias = CSharpDbObservabilityOptions.IsValidDatabaseAlias(
                    configuredAlias)
                ? configuredAlias!
                : "default";
        }

        internal DiagnosticsOwnerIdentity(CSharpDbRuntimeDiagnosticsState state)
        {
            ArgumentNullException.ThrowIfNull(state);
            _serverInstanceId = state.ServerInstanceId;
            _databaseAlias = state.DatabaseAlias;
            CounterEpoch = state.CounterEpoch;
            TimeProvider = state.TimeProvider;
        }

        private long CounterEpoch { get; }
        private TimeProvider? TimeProvider { get; }

        internal DiagnosticsSnapshotMetadata CreateMetadata(
            DiagnosticsAvailability availability,
            DiagnosticsSource source,
            bool recordsTruncated,
            bool fieldsTruncated)
            => DiagnosticsSnapshotMetadata.Create(
                _serverInstanceId,
                CounterEpoch,
                DiagnosticsScope.Instance,
                availability,
                source,
                _databaseAlias,
                recordsTruncated,
                fieldsTruncated,
                TimeProvider);
    }
}
