using CSharpDB.Observability;
using CSharpDB.Storage.Diagnostics;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Engine;

internal enum StorageRuntimeDiagnosticsProvenance
{
    BuiltIn,
    CustomFactory,
}

internal readonly record struct StorageRuntimeDiagnosticsCapture(
    DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot> Storage,
    DiagnosticsSection<WalRuntimeDiagnosticsSnapshot> Wal,
    bool FieldsTruncated = false);

/// <summary>
/// Runtime-local registry for the storage handles and opening operations that
/// contribute to one exact diagnostics counter family.
/// </summary>
internal sealed class StorageRuntimeDiagnostics : IDisposable
{
    private readonly object _gate = new();
    private readonly HashSet<Registration> _registrations = [];
    private readonly TimeProvider _timeProvider;
    private readonly long _timestampFrequency;
    private long? _retiredLogicalCommitCount = 0;
    private long? _retiredLogicalPageWriteCount = 0;
    private long? _retiredCacheHitCount = 0;
    private long? _retiredCacheMissCount = 0;
    private long? _retiredTerminalConflictCount = 0;
    private long? _retiredCommitFlushBatchCount = 0;
    private long? _retiredCommittedFrameBytesWritten = 0;
    private long? _retiredFlushedCommitCount = 0;
    private long? _retiredDurableFlushCount = 0;
    private long? _retiredGroupCommitBatchCount = 0;
    private long? _retiredGroupCommitCount = 0;
    private StorageRuntimeDetailAvailabilityRaw _retiredPhysicalIoAvailability =
        StorageRuntimeDetailAvailabilityRaw.NotApplicable;
    private StorageDeviceIoRuntimeRawSnapshot _retiredPhysicalIo;
    private RecoveryOperation? _lastCompletedRecovery;
    private CheckpointOperation? _lastCompletedCheckpoint;
    private CheckpointOperation? _lastSuccessfulCheckpoint;
    private CheckpointOperation? _lastFailedCheckpoint;
    private long _checkpointAttemptCount;
    private long _checkpointSuccessCount;
    private long _checkpointFailureCount;
    private long _checkpointCanceledCount;
    private DateTimeOffset? _lastSuccessfulFlushAtUtc;
    private DateTimeOffset? _lastSuccessfulDurableFlushAtUtc;
    private DateTimeOffset? _lastSuccessfulGroupCommitAtUtc;
    private DateTimeOffset? _lastSuccessfulCheckpointAtUtc;
    private DateTimeOffset? _lastFailedCheckpointAtUtc;
    private SafeErrorProjection? _lastCheckpointError;
    private SafeErrorProjection? _lastError;
    private bool _disposed;

    private StorageRuntimeDiagnostics(TimeProvider timeProvider)
    {
        _timeProvider = timeProvider;
        _timestampFrequency = timeProvider.TimestampFrequency;
        if (_timestampFrequency <= 0)
            throw new InvalidOperationException(
                "The diagnostics clock timestamp frequency must be positive.");
    }

    internal static Registration? TryBeginBuiltInOpen(
        CSharpDbRuntimeDiagnosticsState? runtimeState,
        bool recoveryApplicable)
    {
        if (runtimeState?.IsEnabled != true)
            return null;

        try
        {
            StorageRuntimeDiagnostics diagnostics = runtimeState
                .GetOrCreateComponent(
                    () => new StorageRuntimeDiagnostics(runtimeState.TimeProvider));
            return diagnostics.Register(
                provider: null,
                StorageRuntimeDiagnosticsProvenance.BuiltIn,
                observesStorage: true,
                recoveryApplicable);
        }
        catch
        {
            // Diagnostics component creation is best-effort and must not make
            // an otherwise valid database open fail.
            return null;
        }
    }

    internal static Registration? TryRegister(
        CSharpDbRuntimeDiagnosticsState? runtimeState,
        Pager pager,
        StorageRuntimeDiagnosticsProvenance provenance)
    {
        ArgumentNullException.ThrowIfNull(pager);
        if (runtimeState?.IsEnabled != true)
            return null;

        try
        {
            return runtimeState
                .GetOrCreateComponent(
                    () => new StorageRuntimeDiagnostics(runtimeState.TimeProvider))
                .Register(
                    pager,
                    provenance,
                    observesStorage: false,
                    recoveryApplicable: false);
        }
        catch
        {
            return null;
        }
    }

    internal static StorageRuntimeDiagnosticsCapture Capture(
        CSharpDbRuntimeDiagnosticsState runtimeState,
        DiagnosticsSnapshotMetadata metadata)
    {
        ArgumentNullException.ThrowIfNull(runtimeState);
        ArgumentNullException.ThrowIfNull(metadata);

        try
        {
            ClockReading? now = TryCaptureClock(runtimeState.TimeProvider);
            return runtimeState.TryGetComponent<StorageRuntimeDiagnostics>(
                    out StorageRuntimeDiagnostics? diagnostics) &&
                diagnostics is not null
                    ? diagnostics.CaptureCore(metadata, now)
                    : WithoutValue(DiagnosticsAvailability.Unavailable);
        }
        catch
        {
            return WithoutValue(DiagnosticsAvailability.Unavailable);
        }
    }

    public void Dispose()
    {
        Registration[] registrations;
        lock (_gate)
        {
            if (_disposed)
                return;

            _disposed = true;
            registrations = _registrations.ToArray();
            _registrations.Clear();
        }

        foreach (Registration registration in registrations)
            registration.DetachOwner(this);
    }

    private Registration Register(
        Pager? provider,
        StorageRuntimeDiagnosticsProvenance provenance,
        bool observesStorage,
        bool recoveryApplicable)
    {
        var registration = new Registration(
            this,
            provider,
            provenance,
            observesStorage,
            recoveryApplicable);
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _registrations.Add(registration);
        }

        return registration;
    }

    private StorageRuntimeDiagnosticsCapture CaptureCore(
        DiagnosticsSnapshotMetadata metadata,
        ClockReading? now)
    {
        lock (_gate)
        {
            if (_disposed)
                return WithoutValue(DiagnosticsAvailability.Unavailable);

            foreach (Registration registration in _registrations)
            {
                if (registration.Provenance ==
                    StorageRuntimeDiagnosticsProvenance.CustomFactory)
                {
                    return WithoutValue(DiagnosticsAvailability.Unsupported);
                }
            }

            RecoveryOperation? recovery = SelectRecoveryOperation(
                out long activeRecoveryCount);
            CheckpointOperation? currentCheckpoint =
                SelectRepresentativeCheckpoint(out long activeCheckpointCount);
            bool fieldsTruncated =
                activeRecoveryCount > 1 || activeCheckpointCount > 1;
            DiagnosticsSnapshotMetadata projectionMetadata =
                fieldsTruncated && !metadata.FieldsTruncated
                    ? WithFieldsTruncated(metadata)
                    : metadata;

            DiagnosticsSection<WalRecoveryDiagnosticsSnapshot> recoverySection =
                TryCreateRecoverySection(projectionMetadata, recovery, now);

            bool hasLiveProvider = false;
            bool hasObservedLiveProvider = false;
            bool allLiveProvidersRecoveryNotApplicable = true;
            long logicalDatabaseBytes = 0;
            long? allocatedDatabaseBytes = null;
            long pageCount = 0;
            long? dirtyPages = 0;
            int activeReaders = 0;
            int activeWriters = 0;
            long walLogicalBytes = 0;
            long? walAllocatedBytes = null;
            long committedFrameBytes = 0;
            long retainedBytes = 0;
            long frameCount = 0;
            int pendingCommitCount = 0;
            long? logicalCommitCount = _retiredLogicalCommitCount;
            long? logicalPageWriteCount = _retiredLogicalPageWriteCount;
            long? cacheHits = _retiredCacheHitCount;
            long? cacheMisses = _retiredCacheMissCount;
            long? terminalConflictCount = _retiredTerminalConflictCount;
            long? commitFlushBatchCount = _retiredCommitFlushBatchCount;
            long? committedFrameBytesWritten =
                _retiredCommittedFrameBytesWritten;
            long? flushedCommitCount = _retiredFlushedCommitCount;
            long? durableFlushCount = _retiredDurableFlushCount;
            long? groupCommitBatchCount = _retiredGroupCommitBatchCount;
            long? groupCommitCount = _retiredGroupCommitCount;
            long sharedResidentPages = 0;
            long? sharedCapacityPages = 0;
            long walResidentPages = 0;
            long walCapacityPages = 0;
            bool cacheAvailable = false;
            bool cacheUnavailable = false;
            bool cacheUnsupported = false;
            StorageDeviceIoRuntimeRawSnapshot physicalIo = _retiredPhysicalIo;
            bool physicalAvailable = _retiredPhysicalIoAvailability ==
                StorageRuntimeDetailAvailabilityRaw.Available;
            bool physicalNotApplicable = _retiredPhysicalIoAvailability ==
                StorageRuntimeDetailAvailabilityRaw.NotApplicable;
            bool physicalUnavailable = _retiredPhysicalIoAvailability ==
                StorageRuntimeDetailAvailabilityRaw.Unavailable;
            bool physicalUnsupported = _retiredPhysicalIoAvailability ==
                StorageRuntimeDetailAvailabilityRaw.Unsupported;
            StorageCheckpointPhaseRaw rawCheckpointPhase =
                StorageCheckpointPhaseRaw.Idle;

            foreach (Registration registration in _registrations)
            {
                Pager? provider = registration.Provider;
                if (provider is null)
                    continue;

                hasLiveProvider = true;
                hasObservedLiveProvider |= registration.ObservesStorage;
                allLiveProvidersRecoveryNotApplicable &=
                    !registration.RecoveryApplicable;
                if (!provider.TryGetRuntimeDiagnosticsSnapshot(
                        out PagerRuntimeRawSnapshot raw) ||
                    !IsValid(raw))
                {
                    return WithoutValue(DiagnosticsAvailability.Unavailable);
                }

                StorageRuntimeRawSnapshot storage = raw.Storage;
                WalRuntimeRawSnapshot wal = raw.Wal;
                logicalDatabaseBytes = Math.Max(
                    logicalDatabaseBytes,
                    storage.LogicalBytes);
                allocatedDatabaseBytes = Maximum(
                    allocatedDatabaseBytes,
                    storage.AllocatedBytes);
                pageCount = Math.Max(pageCount, storage.PageCount);
                dirtyPages = dirtyPages is null || storage.DirtyPageCount is null
                    ? null
                    : SaturatingAdd(
                        dirtyPages.Value,
                        storage.DirtyPageCount.Value);
                activeReaders = SaturatingAdd(
                    activeReaders,
                    storage.ActiveReaderCount);
                activeWriters = SaturatingAdd(
                    activeWriters,
                    storage.ActiveWriterCount);
                walLogicalBytes = Math.Max(walLogicalBytes, wal.LogicalBytes);
                walAllocatedBytes = Maximum(walAllocatedBytes, wal.AllocatedBytes);
                committedFrameBytes = Math.Max(
                    committedFrameBytes,
                    wal.CommittedFrameBytes);
                retainedBytes = Math.Max(retainedBytes, wal.RetainedBytes);
                frameCount = Math.Max(frameCount, wal.FrameCount);
                pendingCommitCount = SaturatingAdd(
                    pendingCommitCount,
                    wal.PendingCommitCount);
                logicalCommitCount = AddOptionalCumulative(
                    logicalCommitCount,
                    wal.LogicalCommitCount);
                logicalPageWriteCount = AddOptionalCumulative(
                    logicalPageWriteCount,
                    wal.LogicalPageWriteCount);
                terminalConflictCount = AddOptionalCumulative(
                    terminalConflictCount,
                    storage.TerminalConflictCount);
                commitFlushBatchCount = AddOptionalCumulative(
                    commitFlushBatchCount,
                    wal.CommitFlushBatchCount);
                committedFrameBytesWritten = AddOptionalCumulative(
                    committedFrameBytesWritten,
                    wal.CommittedFrameBytesWritten);
                flushedCommitCount = AddOptionalCumulative(
                    flushedCommitCount,
                    wal.FlushedCommitCount);
                durableFlushCount = AddOptionalCumulative(
                    durableFlushCount,
                    wal.DurableFlushCount);
                groupCommitBatchCount = AddOptionalCumulative(
                    groupCommitBatchCount,
                    wal.GroupCommitBatchCount);
                groupCommitCount = AddOptionalCumulative(
                    groupCommitCount,
                    wal.GroupCommitCount);
                if (GetCheckpointPhaseRank(wal.CheckpointPhase) >
                    GetCheckpointPhaseRank(rawCheckpointPhase))
                {
                    rawCheckpointPhase = wal.CheckpointPhase;
                }

                if (!provider.TryGetStorageIoRuntimeDiagnosticsSnapshot(
                        out StorageIoRuntimeRawSnapshot ioRaw))
                {
                    cacheHits = null;
                    cacheMisses = null;
                    cacheUnavailable = true;
                    physicalUnavailable = true;
                    continue;
                }

                if (IsValid(ioRaw.LogicalReads))
                {
                    cacheHits = AddOptionalCumulative(
                        cacheHits,
                        ioRaw.LogicalReads.CacheHits);
                    cacheMisses = AddOptionalCumulative(
                        cacheMisses,
                        ioRaw.LogicalReads.CacheMisses);
                }
                else
                {
                    cacheHits = null;
                    cacheMisses = null;
                }

                switch (ioRaw.CacheAvailability)
                {
                    case StorageRuntimeDetailAvailabilityRaw.Available
                        when IsValid(ioRaw.Cache):
                        cacheAvailable = true;
                        sharedResidentPages = SaturatingAdd(
                            sharedResidentPages,
                            ioRaw.Cache.SharedResidentPages);
                        sharedCapacityPages =
                            sharedCapacityPages is { } aggregateCapacity &&
                            ioRaw.Cache.SharedCapacityPages is { } sourceCapacity
                                ? SaturatingAdd(
                                    aggregateCapacity,
                                    sourceCapacity)
                                : null;
                        walResidentPages = SaturatingAdd(
                            walResidentPages,
                            ioRaw.Cache.WalResidentPages);
                        walCapacityPages = SaturatingAdd(
                            walCapacityPages,
                            ioRaw.Cache.WalCapacityPages);
                        break;
                    case StorageRuntimeDetailAvailabilityRaw.Unsupported:
                        cacheUnsupported = true;
                        break;
                    default:
                        cacheUnavailable = true;
                        break;
                }

                switch (ioRaw.PhysicalIoAvailability)
                {
                    case StorageRuntimeDetailAvailabilityRaw.Available
                        when IsValid(ioRaw.PhysicalIo):
                        physicalAvailable = true;
                        physicalIo = Add(physicalIo, ioRaw.PhysicalIo);
                        break;
                    case StorageRuntimeDetailAvailabilityRaw.NotApplicable:
                        physicalNotApplicable = true;
                        break;
                    case StorageRuntimeDetailAvailabilityRaw.Unsupported:
                        physicalUnsupported = true;
                        break;
                    default:
                        physicalUnavailable = true;
                        break;
                }
            }

            DiagnosticsSection<CheckpointDiagnosticsSnapshot> checkpointSection =
                TryCreateCheckpointSection(
                    projectionMetadata,
                    now,
                    currentCheckpoint,
                    activeCheckpointCount,
                    hasLiveProvider,
                    hasObservedLiveProvider,
                    rawCheckpointPhase);

            if (recoverySection.Availability == DiagnosticsAvailability.Unavailable &&
                hasLiveProvider &&
                hasObservedLiveProvider &&
                allLiveProvidersRecoveryNotApplicable)
            {
                recoverySection = DiagnosticsSection<WalRecoveryDiagnosticsSnapshot>
                    .WithoutValue(DiagnosticsAvailability.NotApplicable);
            }

            DiagnosticsSection<StorageCacheDiagnosticsSnapshot> cacheSection =
                CreateCacheSection(
                    projectionMetadata,
                    cacheAvailable,
                    cacheUnavailable,
                    cacheUnsupported,
                    new StorageCacheRuntimeRawSnapshot(
                        sharedResidentPages,
                        sharedCapacityPages,
                        walResidentPages,
                        walCapacityPages));
            DiagnosticsSection<StorageDeviceIoDiagnosticsSnapshot>
                physicalIoSection = CreatePhysicalIoSection(
                    projectionMetadata,
                    physicalAvailable,
                    physicalNotApplicable,
                    physicalUnavailable,
                    physicalUnsupported,
                    physicalIo);
            long? pageReads = cacheHits is { } hits &&
                cacheMisses is { } misses
                    ? SaturatingAdd(hits, misses)
                    : null;

            DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot> storageSection =
                hasLiveProvider
                    ? DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.Available(
                        new StorageRuntimeDiagnosticsSnapshot(
                            projectionMetadata,
                            logicalDatabaseBytes,
                            allocatedDatabaseBytes,
                            pageCount,
                            PageReads: pageReads,
                            PageWrites: logicalPageWriteCount,
                            BytesRead: pageReads is { } reads
                                ? SaturatingMultiply(
                                    reads,
                                    PageConstants.PageSize)
                                : null,
                            BytesWritten: logicalPageWriteCount is long pageWrites
                                ? SaturatingMultiply(
                                    pageWrites,
                                    PageConstants.PageSize)
                                : null,
                            CacheHits: cacheHits,
                            CacheMisses: cacheMisses,
                            dirtyPages,
                            activeReaders,
                            activeWriters,
                            CommitCount: logicalCommitCount,
                            ConflictCount: terminalConflictCount)
                        {
                            Cache = cacheSection,
                            PhysicalIo = physicalIoSection,
                        })
                    : DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>
                        .WithoutValue(DiagnosticsAvailability.Unavailable);

            bool hasWalDetail =
                recoverySection.Value is not null ||
                checkpointSection.Value is not null;
            if (!hasLiveProvider && !hasWalDetail)
            {
                return new StorageRuntimeDiagnosticsCapture(
                    storageSection,
                    DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.WithoutValue(
                        DiagnosticsAvailability.Unavailable),
                    fieldsTruncated);
            }

            CheckpointPhase checkpointPhase = checkpointSection.Value?.Phase ??
                (hasLiveProvider
                    ? MapCheckpointPhase(rawCheckpointPhase)
                    : CheckpointPhase.Idle);
            var walSnapshot = new WalRuntimeDiagnosticsSnapshot(
                projectionMetadata,
                hasLiveProvider ? walLogicalBytes : null,
                hasLiveProvider ? walAllocatedBytes : null,
                hasLiveProvider ? committedFrameBytes : null,
                hasLiveProvider ? retainedBytes : null,
                hasLiveProvider ? frameCount : null,
                hasLiveProvider ? commitFlushBatchCount : null,
                hasLiveProvider ? committedFrameBytesWritten : null,
                hasLiveProvider ? pendingCommitCount : null,
                checkpointPhase,
                _lastSuccessfulFlushAtUtc,
                _lastSuccessfulCheckpointAtUtc,
                _lastError)
            {
                FlushedCommitCount = hasLiveProvider
                    ? flushedCommitCount
                    : null,
                DurableFlushCount = hasLiveProvider
                    ? durableFlushCount
                    : null,
                LastSuccessfulDurableFlushAtUtc =
                    hasLiveProvider && durableFlushCount is > 0
                        ? _lastSuccessfulDurableFlushAtUtc
                        : null,
                GroupCommitBatchCount = hasLiveProvider
                    ? groupCommitBatchCount
                    : null,
                GroupCommitCount = hasLiveProvider
                    ? groupCommitCount
                    : null,
                LastSuccessfulGroupCommitAtUtc =
                    hasLiveProvider && groupCommitBatchCount is > 0
                        ? _lastSuccessfulGroupCommitAtUtc
                        : null,
                Recovery = recoverySection,
                Checkpoint = checkpointSection,
            };
            return new StorageRuntimeDiagnosticsCapture(
                storageSection,
                DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.Available(
                    walSnapshot),
                fieldsTruncated);
        }
    }

    private DiagnosticsSection<WalRecoveryDiagnosticsSnapshot>
        TryCreateRecoverySection(
            DiagnosticsSnapshotMetadata metadata,
            RecoveryOperation? operation,
            ClockReading? now)
    {
        try
        {
            return CreateRecoverySection(metadata, operation, now);
        }
        catch
        {
            return DiagnosticsSection<WalRecoveryDiagnosticsSnapshot>
                .WithoutValue(DiagnosticsAvailability.Unavailable);
        }
    }

    private static DiagnosticsSection<StorageCacheDiagnosticsSnapshot>
        CreateCacheSection(
            DiagnosticsSnapshotMetadata metadata,
            bool available,
            bool unavailable,
            bool unsupported,
            in StorageCacheRuntimeRawSnapshot raw)
    {
        if (unsupported)
        {
            return DiagnosticsSection<StorageCacheDiagnosticsSnapshot>
                .WithoutValue(DiagnosticsAvailability.Unsupported);
        }

        if (unavailable || !available || !IsValid(raw))
        {
            return DiagnosticsSection<StorageCacheDiagnosticsSnapshot>
                .WithoutValue(DiagnosticsAvailability.Unavailable);
        }

        try
        {
            return DiagnosticsSection<StorageCacheDiagnosticsSnapshot>.Available(
                new StorageCacheDiagnosticsSnapshot(
                    metadata,
                    raw.SharedResidentPages,
                    raw.SharedCapacityPages,
                    raw.WalResidentPages,
                    raw.WalCapacityPages));
        }
        catch
        {
            return DiagnosticsSection<StorageCacheDiagnosticsSnapshot>
                .WithoutValue(DiagnosticsAvailability.Unavailable);
        }
    }

    private static DiagnosticsSection<StorageDeviceIoDiagnosticsSnapshot>
        CreatePhysicalIoSection(
            DiagnosticsSnapshotMetadata metadata,
            bool available,
            bool notApplicable,
            bool unavailable,
            bool unsupported,
            in StorageDeviceIoRuntimeRawSnapshot raw)
    {
        if (unsupported)
        {
            return DiagnosticsSection<StorageDeviceIoDiagnosticsSnapshot>
                .WithoutValue(DiagnosticsAvailability.Unsupported);
        }

        if (unavailable)
        {
            return DiagnosticsSection<StorageDeviceIoDiagnosticsSnapshot>
                .WithoutValue(DiagnosticsAvailability.Unavailable);
        }

        if (!available)
        {
            return DiagnosticsSection<StorageDeviceIoDiagnosticsSnapshot>
                .WithoutValue(
                    notApplicable
                        ? DiagnosticsAvailability.NotApplicable
                        : DiagnosticsAvailability.Unavailable);
        }

        if (!IsValid(raw))
        {
            return DiagnosticsSection<StorageDeviceIoDiagnosticsSnapshot>
                .WithoutValue(DiagnosticsAvailability.Unavailable);
        }

        try
        {
            return DiagnosticsSection<StorageDeviceIoDiagnosticsSnapshot>.Available(
                new StorageDeviceIoDiagnosticsSnapshot(
                    metadata,
                    raw.ReadCount,
                    raw.BytesRead,
                    raw.WriteCount,
                    raw.BytesWritten,
                    raw.FlushCount,
                    raw.ResizeCount,
                    raw.SequentialReadCount,
                    raw.SequentialBytesRead,
                    raw.MemoryMappedPageExposureCount,
                    raw.MemoryMappedBytesExposed));
        }
        catch
        {
            return DiagnosticsSection<StorageDeviceIoDiagnosticsSnapshot>
                .WithoutValue(DiagnosticsAvailability.Unavailable);
        }
    }

    private DiagnosticsSection<WalRecoveryDiagnosticsSnapshot>
        CreateRecoverySection(
            DiagnosticsSnapshotMetadata metadata,
            RecoveryOperation? operation,
            ClockReading? now)
    {
        if (operation is null || !IsValid(operation.Raw))
        {
            return DiagnosticsSection<WalRecoveryDiagnosticsSnapshot>
                .WithoutValue(DiagnosticsAvailability.Unavailable);
        }

        StorageRecoveryRuntimeRawSnapshot raw = operation.Raw;
        bool completed = operation.CompletedAtUtc.HasValue;
        CSharpDbOperationOutcome outcome = MapOutcome(raw.Outcome);
        SafeErrorProjection? error = completed &&
            outcome is CSharpDbOperationOutcome.Failed or
                CSharpDbOperationOutcome.Canceled
                ? ProjectFailure(raw.FailureKind, outcome)
                : null;
        SafeErrorProjection? retryError = raw.RetryCount > 0
            ? ProjectFailure(
                raw.LastRetryFailureKind,
                CSharpDbOperationOutcome.Failed)
            : null;
        var snapshot = new WalRecoveryDiagnosticsSnapshot(
            metadata,
            operation.OperationId,
            MapRecoveryPhase(raw.Phase),
            operation.StartedAtUtc,
            operation.CompletedAtUtc,
            GetElapsed(operation, now),
            outcome,
            raw.ScannedFrameCount,
            raw.ScannedBytes,
            raw.RecoveredFrameCount,
            raw.RecoveredBytes,
            raw.DiscardedFrameCount,
            raw.DiscardedBytes,
            MapTruncationReason(raw.TruncationReason),
            raw.AttemptCount,
            raw.RetryCount,
            retryError,
            error);
        return DiagnosticsSection<WalRecoveryDiagnosticsSnapshot>.Available(
            snapshot);
    }

    private DiagnosticsSection<CheckpointDiagnosticsSnapshot>
        TryCreateCheckpointSection(
            DiagnosticsSnapshotMetadata metadata,
            ClockReading? now,
            CheckpointOperation? current,
            long activeCount,
            bool hasLiveProvider,
            bool hasObservedLiveProvider,
            StorageCheckpointPhaseRaw rawCheckpointPhase)
    {
        try
        {
            return CreateCheckpointSection(
                metadata,
                now,
                current,
                activeCount,
                hasLiveProvider,
                hasObservedLiveProvider,
                rawCheckpointPhase);
        }
        catch
        {
            return DiagnosticsSection<CheckpointDiagnosticsSnapshot>
                .WithoutValue(DiagnosticsAvailability.Unavailable);
        }
    }

    private DiagnosticsSection<CheckpointDiagnosticsSnapshot>
        CreateCheckpointSection(
            DiagnosticsSnapshotMetadata metadata,
            ClockReading? now,
            CheckpointOperation? current,
            long activeCount,
            bool hasLiveProvider,
            bool hasObservedLiveProvider,
            StorageCheckpointPhaseRaw rawCheckpointPhase)
    {
        bool hasHistory = _checkpointAttemptCount > 0;
        if (!hasHistory && !hasObservedLiveProvider)
        {
            return DiagnosticsSection<CheckpointDiagnosticsSnapshot>
                .WithoutValue(DiagnosticsAvailability.Unavailable);
        }

        CheckpointPhase phase;
        CheckpointOrigin origin;
        OpaqueDiagnosticsId? operationId = null;
        DateTimeOffset? startedAtUtc = null;
        TimeSpan? elapsed = null;
        long? completedPageCount = null;
        long? totalPageCount = null;
        CheckpointRetentionReason retentionReason;
        CheckpointOperation? terminal = null;
        if (current is not null)
        {
            if (hasLiveProvider && current.Raw.Phase != rawCheckpointPhase)
            {
                return DiagnosticsSection<CheckpointDiagnosticsSnapshot>
                    .WithoutValue(DiagnosticsAvailability.Unavailable);
            }

            phase = MapCheckpointPhase(current.Raw.Phase);
            origin = MapCheckpointOrigin(current.Raw.Origin);
            operationId = current.OperationId;
            startedAtUtc = current.StartedAtUtc;
            elapsed = GetElapsed(current, now);
            completedPageCount = current.Raw.CompletedPageCount;
            totalPageCount = current.Raw.TotalPageCount;
            retentionReason = MapRetentionReason(current.Raw.RetentionReason);
        }
        else if (hasHistory)
        {
            terminal = hasLiveProvider
                ? SelectCompatibleCheckpointTerminal(rawCheckpointPhase)
                : _lastCompletedCheckpoint;
            if (terminal is null)
            {
                return DiagnosticsSection<CheckpointDiagnosticsSnapshot>
                    .WithoutValue(DiagnosticsAvailability.Unavailable);
            }

            StorageCheckpointRuntimeRawSnapshot terminalRaw = terminal.Raw;
            phase = hasLiveProvider
                ? MapCheckpointPhase(rawCheckpointPhase)
                : MapCheckpointPhase(terminalRaw.Phase);
            origin = MapCheckpointOrigin(terminalRaw.Origin);
            retentionReason = phase == CheckpointPhase.Idle
                ? CheckpointRetentionReason.None
                : MapRetentionReason(terminalRaw.RetentionReason);
        }
        else
        {
            if (hasLiveProvider &&
                rawCheckpointPhase != StorageCheckpointPhaseRaw.Idle)
            {
                return DiagnosticsSection<CheckpointDiagnosticsSnapshot>
                    .WithoutValue(DiagnosticsAvailability.Unavailable);
            }

            phase = MapCheckpointPhase(rawCheckpointPhase);
            origin = CheckpointOrigin.Unknown;
            retentionReason = CheckpointRetentionReason.None;
        }

        CheckpointOperation? lastAttempt = current ?? terminal;
        var snapshot = new CheckpointDiagnosticsSnapshot(
            metadata,
            operationId,
            phase,
            origin,
            startedAtUtc,
            elapsed,
            completedPageCount,
            totalPageCount,
            retentionReason,
            lastAttempt?.StartedAtUtc,
            _lastSuccessfulCheckpointAtUtc,
            _lastFailedCheckpointAtUtc,
            lastAttempt is null
                ? null
                : GetElapsed(lastAttempt, now),
            activeCount,
            _checkpointAttemptCount,
            _checkpointSuccessCount,
            _checkpointFailureCount,
            _checkpointCanceledCount,
            _lastCheckpointError);
        return DiagnosticsSection<CheckpointDiagnosticsSnapshot>.Available(
            snapshot);
    }

    private RecoveryOperation? SelectRecoveryOperation(out long activeCount)
    {
        activeCount = 0;
        RecoveryOperation? selected = null;
        foreach (Registration registration in _registrations)
        {
            RecoveryOperation? candidate = registration.Recovery;
            if (candidate is null || candidate.CompletedAtUtc.HasValue)
                continue;
            activeCount = SaturatingIncrement(activeCount);
            if (selected is null || IsEarlier(candidate, selected))
                selected = candidate;
        }

        return selected ?? _lastCompletedRecovery;
    }

    private CheckpointOperation? SelectRepresentativeCheckpoint(
        out long activeCount)
    {
        activeCount = 0;
        CheckpointOperation? selected = null;
        foreach (Registration registration in _registrations)
        {
            CheckpointOperation? candidate = registration.Checkpoint;
            if (candidate is null || candidate.CompletedAtUtc.HasValue)
                continue;

            activeCount = SaturatingIncrement(activeCount);
            if (selected is null || IsPreferredCheckpoint(candidate, selected))
                selected = candidate;
        }

        return selected;
    }

    private static bool IsEarlier(
        RecoveryOperation candidate,
        RecoveryOperation selected)
        => candidate.StartedAtUtc < selected.StartedAtUtc ||
            candidate.StartedAtUtc == selected.StartedAtUtc &&
            string.CompareOrdinal(candidate.SortKey, selected.SortKey) < 0;

    private static bool IsLaterRecoveryCompletion(
        RecoveryOperation candidate,
        RecoveryOperation selected)
        => candidate.CompletedAtUtc > selected.CompletedAtUtc ||
            candidate.CompletedAtUtc == selected.CompletedAtUtc &&
            (candidate.StartedAtUtc > selected.StartedAtUtc ||
             candidate.StartedAtUtc == selected.StartedAtUtc &&
             string.CompareOrdinal(candidate.SortKey, selected.SortKey) > 0);

    private static bool IsPreferredCheckpoint(
        CheckpointOperation candidate,
        CheckpointOperation selected)
    {
        int candidateRank = GetCheckpointPhaseRank(candidate.Raw.Phase);
        int selectedRank = GetCheckpointPhaseRank(selected.Raw.Phase);
        return candidateRank > selectedRank ||
            candidateRank == selectedRank &&
            (candidate.StartedAtUtc < selected.StartedAtUtc ||
             candidate.StartedAtUtc == selected.StartedAtUtc &&
             string.CompareOrdinal(candidate.SortKey, selected.SortKey) < 0);
    }

    private CheckpointOperation? SelectCompatibleCheckpointTerminal(
        StorageCheckpointPhaseRaw phase)
        => phase switch
        {
            StorageCheckpointPhaseRaw.Idle
                when _lastSuccessfulCheckpoint?.Raw.Phase == phase =>
                    _lastSuccessfulCheckpoint,
            StorageCheckpointPhaseRaw.Faulted
                when _lastFailedCheckpoint?.Raw.Phase == phase =>
                    _lastFailedCheckpoint,
            _ => null,
        };

    private static bool IsLaterCheckpointCompletion(
        CheckpointOperation candidate,
        CheckpointOperation selected)
        => candidate.CompletedAtUtc > selected.CompletedAtUtc ||
            candidate.CompletedAtUtc == selected.CompletedAtUtc &&
            (candidate.StartedAtUtc > selected.StartedAtUtc ||
             candidate.StartedAtUtc == selected.StartedAtUtc &&
             string.CompareOrdinal(candidate.SortKey, selected.SortKey) > 0);

    private void RecoveryStarted(
        Registration registration,
        RecoveryOperation candidate)
    {
        lock (_gate)
        {
            if (!CanObserve(registration) ||
                registration.Recovery is { CompletedAtUtc: null })
            {
                return;
            }

            registration.Recovery = candidate;
        }
    }

    private OpaqueDiagnosticsId? GetActiveRecoveryOperationId(
        Registration registration)
    {
        lock (_gate)
        {
            return CanObserve(registration) &&
                registration.Recovery is { CompletedAtUtc: null } recovery
                    ? recovery.OperationId
                    : null;
        }
    }

    private void RecoveryChanged(
        Registration registration,
        RecoveryOperation? candidate,
        StorageRecoveryRuntimeRawSnapshot raw,
        ClockReading? observedAt)
    {
        lock (_gate)
        {
            if (!CanObserve(registration))
                return;

            RecoveryOperation? current = registration.Recovery is
                { CompletedAtUtc: null } existing
                    ? existing
                    : candidate;
            if (current is null)
                return;
            if (!ReferenceEquals(current, registration.Recovery))
            {
                registration.Recovery = current;
            }

            RecordRecoveryRetry(current, raw);
            current.Raw = raw;
            if (observedAt is { } observation)
            {
                current.Elapsed = CalculateElapsed(
                    current.StartingTimestamp,
                    observation.Timestamp);
            }
        }
    }

    private void RecoveryCompleted(
        Registration registration,
        RecoveryOperation? candidate,
        StorageRecoveryRuntimeRawSnapshot raw,
        ClockReading? completedAt)
    {
        lock (_gate)
        {
            if (!CanObserve(registration))
                return;

            RecoveryOperation? current = registration.Recovery is
                { CompletedAtUtc: null } existing
                    ? existing
                    : candidate;
            if (current is null)
                return;
            if (current.CompletedAtUtc.HasValue)
                return;
            if (!ReferenceEquals(current, registration.Recovery))
            {
                registration.Recovery = current;
            }

            RecordRecoveryRetry(current, raw);
            current.Raw = raw;
            if (completedAt is { } completion)
            {
                current.Elapsed = CalculateElapsed(
                    current.StartingTimestamp,
                    completion.Timestamp);
                current.CompletedAtUtc = completion.UtcNow;
            }
            else
            {
                current.CompletedAtUtc = SafeCompletionUtc(
                    current.StartedAtUtc,
                    current.Elapsed);
            }
            if (_lastCompletedRecovery is null ||
                IsLaterRecoveryCompletion(current, _lastCompletedRecovery))
            {
                _lastCompletedRecovery = current;
            }

            CSharpDbOperationOutcome outcome = MapOutcome(raw.Outcome);
            if (outcome is CSharpDbOperationOutcome.Failed or
                CSharpDbOperationOutcome.Canceled)
            {
                SetLastError(ProjectFailure(raw.FailureKind, outcome));
            }
        }
    }

    private void RecordRecoveryRetry(
        RecoveryOperation operation,
        StorageRecoveryRuntimeRawSnapshot raw)
    {
        if (raw.RetryCount <= operation.LastObservedRetryCount)
            return;

        operation.LastObservedRetryCount = raw.RetryCount;
        SetLastError(ProjectFailure(
            raw.LastRetryFailureKind,
            CSharpDbOperationOutcome.Failed));
    }

    private void CheckpointStarted(
        Registration registration,
        CheckpointOperation candidate)
    {
        lock (_gate)
        {
            if (!CanObserve(registration) ||
                registration.Checkpoint is { CompletedAtUtc: null })
            {
                return;
            }

            registration.Checkpoint = candidate;
            _checkpointAttemptCount = SaturatingIncrement(
                _checkpointAttemptCount);
        }
    }

    private void CheckpointChanged(
        Registration registration,
        StorageCheckpointRuntimeRawSnapshot raw,
        ClockReading? observedAt)
    {
        lock (_gate)
        {
            if (!CanObserve(registration) ||
                registration.Checkpoint is not { CompletedAtUtc: null } current)
            {
                return;
            }

            current.Raw = raw;
            if (observedAt is { } observation)
            {
                current.Elapsed = CalculateElapsed(
                    current.StartingTimestamp,
                    observation.Timestamp);
            }
        }
    }

    private void CheckpointCompleted(
        Registration registration,
        StorageCheckpointRuntimeRawSnapshot raw,
        ClockReading? completedAt)
    {
        lock (_gate)
        {
            if (!CanObserve(registration) ||
                registration.Checkpoint is not { CompletedAtUtc: null } current)
            {
                return;
            }

            current.Raw = raw;
            if (completedAt is { } completion)
            {
                current.Elapsed = CalculateElapsed(
                    current.StartingTimestamp,
                    completion.Timestamp);
                current.CompletedAtUtc = completion.UtcNow;
            }
            else
            {
                current.CompletedAtUtc = SafeCompletionUtc(
                    current.StartedAtUtc,
                    current.Elapsed);
            }
            DateTimeOffset completedAtUtc = current.CompletedAtUtc.Value;
            if (_lastCompletedCheckpoint is null ||
                IsLaterCheckpointCompletion(
                    current,
                    _lastCompletedCheckpoint))
            {
                _lastCompletedCheckpoint = current;
            }
            switch (raw.Outcome)
            {
                case StorageRuntimeOperationOutcomeRaw.Succeeded:
                    _checkpointSuccessCount = SaturatingIncrement(
                        _checkpointSuccessCount);
                    if (_lastSuccessfulCheckpoint is null ||
                        IsLaterCheckpointCompletion(
                            current,
                            _lastSuccessfulCheckpoint))
                    {
                        _lastSuccessfulCheckpoint = current;
                        _lastSuccessfulCheckpointAtUtc = completedAtUtc;
                    }
                    break;
                case StorageRuntimeOperationOutcomeRaw.Canceled:
                    _checkpointCanceledCount = SaturatingIncrement(
                        _checkpointCanceledCount);
                    RecordLatestFailedCheckpoint(
                        current,
                        completedAtUtc,
                        CSharpDbOperationOutcome.Canceled);
                    break;
                default:
                    _checkpointFailureCount = SaturatingIncrement(
                        _checkpointFailureCount);
                    RecordLatestFailedCheckpoint(
                        current,
                        completedAtUtc,
                        CSharpDbOperationOutcome.Failed);
                    break;
            }
        }
    }

    private void RecordLatestFailedCheckpoint(
        CheckpointOperation current,
        DateTimeOffset completedAtUtc,
        CSharpDbOperationOutcome outcome)
    {
        if (_lastFailedCheckpoint is not null &&
            !IsLaterCheckpointCompletion(current, _lastFailedCheckpoint))
        {
            return;
        }

        _lastFailedCheckpoint = current;
        _lastFailedCheckpointAtUtc = completedAtUtc;
        _lastCheckpointError = ProjectFailure(current.Raw.FailureKind, outcome);
        SetLastError(_lastCheckpointError);
    }

    private void WalFlushCompleted(
        int logicalCommitCount,
        DateTimeOffset completedAtUtc)
    {
        lock (_gate)
        {
            if (_disposed)
                return;
            SetLatestUtc(ref _lastSuccessfulFlushAtUtc, completedAtUtc);
            if (logicalCommitCount > 1)
            {
                SetLatestUtc(
                    ref _lastSuccessfulGroupCommitAtUtc,
                    completedAtUtc);
            }
        }
    }

    private void WalDurableFlushCompleted(
        Registration registration,
        long durableFlushCount,
        ClockReading? completedAt)
    {
        lock (_gate)
        {
            if (!CanObserve(registration))
                return;

            if (registration.ProviderDrained &&
                registration.Provider is null)
            {
                long? watermark = registration.RetiredDurableFlushWatermark;
                if (_retiredDurableFlushCount is not null &&
                    watermark is not null)
                {
                    if (durableFlushCount < watermark.Value)
                    {
                        _retiredDurableFlushCount = null;
                        registration.RetiredDurableFlushWatermark = null;
                    }
                    else
                    {
                        _retiredDurableFlushCount = SaturatingAdd(
                            _retiredDurableFlushCount.Value,
                            durableFlushCount - watermark.Value);
                        registration.RetiredDurableFlushWatermark =
                            durableFlushCount;
                    }
                }
            }

            if (completedAt is { } completion)
            {
                SetLatestUtc(
                    ref _lastSuccessfulDurableFlushAtUtc,
                    completion.UtcNow);
            }
        }
    }

    private void StorageDeviceIoSealed(
        Registration registration,
        in StorageDeviceIoRuntimeRawSnapshot finalSnapshot)
    {
        lock (_gate)
        {
            if (!CanObserve(registration))
                return;

            bool unpromoted = registration.Provider is null &&
                !registration.ProviderDrained;
            if (registration.FinalPhysicalIoSample is { } priorFinal)
            {
                if (priorFinal == finalSnapshot)
                    return;

                if (unpromoted)
                {
                    registration.PendingPhysicalIoInvalid = true;
                    return;
                }

                registration.PendingPhysicalIoInvalid = true;
                registration.PendingFinalPhysicalIo = null;
                registration.RetiredPhysicalIoWatermark = null;
                registration.FinalPhysicalIoReconciled = true;
                MarkRetiredPhysicalIoUnknown();
                return;
            }

            if (!IsValid(finalSnapshot))
            {
                registration.PendingPhysicalIoInvalid = true;
                if (!unpromoted)
                    MarkRetiredPhysicalIoUnknown();
                return;
            }

            registration.FinalPhysicalIoSample = finalSnapshot;
            if (!registration.ProviderDrained ||
                registration.Provider is not null)
            {
                registration.PendingFinalPhysicalIo = finalSnapshot;
                return;
            }

            ReconcileFinalPhysicalIo(registration, finalSnapshot);
        }
    }

    private void ReconcileFinalPhysicalIo(
        Registration registration,
        in StorageDeviceIoRuntimeRawSnapshot finalSnapshot)
    {
        if (registration.FinalPhysicalIoReconciled)
            return;

        StorageDeviceIoRuntimeRawSnapshot? watermark =
            registration.RetiredPhysicalIoWatermark;
        if (watermark is null ||
            !IsValid(finalSnapshot) ||
            !IsAtLeast(finalSnapshot, watermark.Value))
        {
            registration.PendingFinalPhysicalIo = null;
            registration.RetiredPhysicalIoWatermark = null;
            registration.FinalPhysicalIoReconciled = true;
            MarkRetiredPhysicalIoUnknown();
            return;
        }

        StorageDeviceIoRuntimeRawSnapshot delta = Subtract(
            finalSnapshot,
            watermark.Value);
        AccumulateRetiredPhysicalIo(delta);
        registration.RetiredPhysicalIoWatermark = finalSnapshot;
        registration.PendingFinalPhysicalIo = null;
        registration.FinalPhysicalIoReconciled = true;
    }

    private static void SetLatestUtc(
        ref DateTimeOffset? current,
        DateTimeOffset candidate)
    {
        if (current is null || candidate > current.Value)
            current = candidate;
    }

    private bool CanObserve(Registration registration)
        => !_disposed && _registrations.Contains(registration);

    private void SetLastError(SafeErrorProjection error)
        => _lastError = error;

    private void Promote(Registration registration, Pager pager)
    {
        lock (_gate)
        {
            if (!CanObserve(registration) || registration.Provider is not null)
                return;
            registration.Provider = pager;
            registration.ProviderDrained = false;
            registration.RetiredDurableFlushWatermark = null;
            registration.RetiredPhysicalIoWatermark = null;
            registration.PendingFinalPhysicalIo = null;
            registration.FinalPhysicalIoSample = null;
            registration.PendingPhysicalIoInvalid = false;
            registration.FinalPhysicalIoReconciled = false;
        }
    }

    private void DrainProvider(Registration registration)
    {
        lock (_gate)
        {
            if (!CanObserve(registration) || registration.Provider is null)
                return;
            RetireCumulativeCounters(registration);
            registration.Provider = null;
            registration.ProviderDrained = true;
        }
    }

    private void Unregister(Registration registration)
    {
        lock (_gate)
        {
            if (!_registrations.Contains(registration))
                return;
            if (registration.Provider is not null &&
                registration.Provenance ==
                    StorageRuntimeDiagnosticsProvenance.BuiltIn)
            {
                RetireCumulativeCounters(registration);
            }
            _registrations.Remove(registration);
            registration.Provider = null;
            registration.ProviderDrained = true;
        }
    }

    private void RetireCumulativeCounters(Registration registration)
    {
        Pager? provider = registration.Provider;
        if (provider is null)
        {
            registration.RetiredDurableFlushWatermark = null;
            registration.RetiredPhysicalIoWatermark = null;
            MarkRetiredCumulativeCountersUnknown();
            MarkRetiredLogicalReadsUnknown();
            MarkRetiredPhysicalIoUnknown();
            return;
        }

        try
        {
            if (!provider.TryGetRuntimeDiagnosticsSnapshot(
                    out PagerRuntimeRawSnapshot raw) ||
                !IsValid(raw))
            {
                registration.RetiredDurableFlushWatermark = null;
                MarkRetiredCumulativeCountersUnknown();
            }
            else
            {
                AccumulateRetiredCounter(
                    ref _retiredLogicalCommitCount,
                    raw.Wal.LogicalCommitCount);
                AccumulateRetiredCounter(
                    ref _retiredLogicalPageWriteCount,
                    raw.Wal.LogicalPageWriteCount);
                AccumulateRetiredCounter(
                    ref _retiredTerminalConflictCount,
                    raw.Storage.TerminalConflictCount);
                AccumulateRetiredCounter(
                    ref _retiredCommitFlushBatchCount,
                    raw.Wal.CommitFlushBatchCount);
                AccumulateRetiredCounter(
                    ref _retiredCommittedFrameBytesWritten,
                    raw.Wal.CommittedFrameBytesWritten);
                AccumulateRetiredCounter(
                    ref _retiredFlushedCommitCount,
                    raw.Wal.FlushedCommitCount);
                AccumulateRetiredCounter(
                    ref _retiredDurableFlushCount,
                    raw.Wal.DurableFlushCount);
                AccumulateRetiredCounter(
                    ref _retiredGroupCommitBatchCount,
                    raw.Wal.GroupCommitBatchCount);
                AccumulateRetiredCounter(
                    ref _retiredGroupCommitCount,
                    raw.Wal.GroupCommitCount);
                registration.RetiredDurableFlushWatermark =
                    raw.Wal.DurableFlushCount;
            }
        }
        catch
        {
            registration.RetiredDurableFlushWatermark = null;
            MarkRetiredCumulativeCountersUnknown();
        }

        try
        {
            if (!provider.TryGetStorageIoRuntimeDiagnosticsSnapshot(
                    out StorageIoRuntimeRawSnapshot raw))
            {
                registration.RetiredPhysicalIoWatermark = null;
                MarkRetiredLogicalReadsUnknown();
                if (!TryRetirePendingFinalPhysicalIoAsFull(registration))
                    MarkRetiredPhysicalIoUnknown();
                return;
            }

            if (IsValid(raw.LogicalReads))
            {
                AccumulateRetiredCounter(
                    ref _retiredCacheHitCount,
                    raw.LogicalReads.CacheHits);
                AccumulateRetiredCounter(
                    ref _retiredCacheMissCount,
                    raw.LogicalReads.CacheMisses);
            }
            else
            {
                MarkRetiredLogicalReadsUnknown();
            }

            RetirePhysicalIo(registration, raw);
        }
        catch
        {
            registration.RetiredPhysicalIoWatermark = null;
            MarkRetiredLogicalReadsUnknown();
            if (!TryRetirePendingFinalPhysicalIoAsFull(registration))
                MarkRetiredPhysicalIoUnknown();
        }
    }

    private void RetirePhysicalIo(
        Registration registration,
        in StorageIoRuntimeRawSnapshot raw)
    {
        switch (raw.PhysicalIoAvailability)
        {
            case StorageRuntimeDetailAvailabilityRaw.Available
                when IsValid(raw.PhysicalIo):
                AccumulateRetiredPhysicalIo(raw.PhysicalIo);
                registration.RetiredPhysicalIoWatermark = raw.PhysicalIo;
                TryReconcilePendingFinalPhysicalIo(registration);
                break;
            case StorageRuntimeDetailAvailabilityRaw.NotApplicable:
                registration.RetiredPhysicalIoWatermark = null;
                break;
            case StorageRuntimeDetailAvailabilityRaw.Unsupported:
                registration.RetiredPhysicalIoWatermark = null;
                if (_retiredPhysicalIoAvailability !=
                    StorageRuntimeDetailAvailabilityRaw.Unavailable)
                {
                    _retiredPhysicalIoAvailability =
                        StorageRuntimeDetailAvailabilityRaw.Unsupported;
                }
                break;
            default:
                registration.RetiredPhysicalIoWatermark = null;
                if (!TryRetirePendingFinalPhysicalIoAsFull(registration))
                    MarkRetiredPhysicalIoUnknown();
                break;
        }
    }

    private bool TryRetirePendingFinalPhysicalIoAsFull(
        Registration registration)
    {
        if (registration.PendingPhysicalIoInvalid ||
            registration.PendingFinalPhysicalIo is not { } finalSnapshot ||
            !IsValid(finalSnapshot))
        {
            return false;
        }

        AccumulateRetiredPhysicalIo(finalSnapshot);
        registration.RetiredPhysicalIoWatermark = finalSnapshot;
        registration.PendingFinalPhysicalIo = null;
        registration.FinalPhysicalIoReconciled = true;
        return true;
    }

    private void TryReconcilePendingFinalPhysicalIo(Registration registration)
    {
        if (registration.PendingPhysicalIoInvalid)
        {
            registration.PendingFinalPhysicalIo = null;
            registration.RetiredPhysicalIoWatermark = null;
            registration.FinalPhysicalIoReconciled = true;
            MarkRetiredPhysicalIoUnknown();
            return;
        }

        if (registration.PendingFinalPhysicalIo is not { } finalSnapshot)
            return;

        ReconcileFinalPhysicalIo(registration, finalSnapshot);
    }

    private static void AccumulateRetiredCounter(
        ref long? retired,
        long? finalValue)
    {
        if (retired is null || finalValue is null)
            return;
        retired = SaturatingAdd(retired.Value, finalValue.Value);
    }

    private void AccumulateRetiredPhysicalIo(
        in StorageDeviceIoRuntimeRawSnapshot value)
    {
        if (_retiredPhysicalIoAvailability is
            StorageRuntimeDetailAvailabilityRaw.Unavailable or
            StorageRuntimeDetailAvailabilityRaw.Unsupported)
        {
            return;
        }

        _retiredPhysicalIo =
            _retiredPhysicalIoAvailability ==
                StorageRuntimeDetailAvailabilityRaw.Available
                ? Add(_retiredPhysicalIo, value)
                : value;
        _retiredPhysicalIoAvailability =
            StorageRuntimeDetailAvailabilityRaw.Available;
    }

    private void MarkRetiredLogicalReadsUnknown()
    {
        _retiredCacheHitCount = null;
        _retiredCacheMissCount = null;
    }

    private void MarkRetiredPhysicalIoUnknown()
    {
        _retiredPhysicalIoAvailability =
            StorageRuntimeDetailAvailabilityRaw.Unavailable;
        _retiredPhysicalIo = default;
    }

    private void MarkRetiredCumulativeCountersUnknown()
    {
        _retiredLogicalCommitCount = null;
        _retiredLogicalPageWriteCount = null;
        _retiredTerminalConflictCount = null;
        _retiredCommitFlushBatchCount = null;
        _retiredCommittedFrameBytesWritten = null;
        _retiredFlushedCommitCount = null;
        _retiredDurableFlushCount = null;
        _retiredGroupCommitBatchCount = null;
        _retiredGroupCommitCount = null;
    }

    internal static bool IsValid(PagerRuntimeRawSnapshot raw)
    {
        StorageRuntimeRawSnapshot storage = raw.Storage;
        WalRuntimeRawSnapshot wal = raw.Wal;
        return storage.PageSize > 0 &&
            storage.PageCount >= 0 &&
            storage.LogicalBytes >= 0 &&
            storage.AllocatedBytes is null or >= 0 &&
            storage.DirtyPageCount is null or >= 0 &&
            storage.ActiveReaderCount >= 0 &&
            storage.ActiveWriterCount >= 0 &&
            storage.TerminalConflictCount >= 0 &&
            wal.LogicalBytes >= 0 &&
            wal.AllocatedBytes is null or >= 0 &&
            wal.FrameCount >= 0 &&
            wal.CommittedFrameBytes >= 0 &&
            wal.RetainedBytes >= 0 &&
            wal.PendingCommitCount >= 0 &&
            wal.LogicalCommitCount is >= 0 &&
            wal.LogicalPageWriteCount >= 0 &&
            wal.CommitFlushBatchCount is null or >= 0 &&
            wal.CommittedFrameBytesWritten is null or >= 0 &&
            wal.FlushedCommitCount is null or >= 0 &&
            wal.DurableFlushCount is null or >= 0 &&
            wal.GroupCommitBatchCount is null or >= 0 &&
            wal.GroupCommitCount is null or >= 0 &&
            HasValidFileLifetimeCounterShape(wal) &&
            Enum.IsDefined(wal.CheckpointPhase);
    }

    private static bool IsValid(LogicalPageReadRuntimeRawSnapshot raw)
        => raw.CacheHits >= 0 && raw.CacheMisses >= 0;

    private static bool IsValid(StorageCacheRuntimeRawSnapshot raw)
        => raw.SharedResidentPages >= 0 &&
            raw.SharedCapacityPages is not < 0 &&
            (raw.SharedCapacityPages is not { } sharedCapacity ||
                raw.SharedResidentPages <= sharedCapacity) &&
            raw.WalResidentPages >= 0 &&
            raw.WalCapacityPages >= 0 &&
            raw.WalResidentPages <= raw.WalCapacityPages;

    private static bool IsValid(StorageDeviceIoRuntimeRawSnapshot raw)
        => raw.ReadCount >= 0 &&
            raw.BytesRead >= 0 &&
            raw.WriteCount >= 0 &&
            raw.BytesWritten >= 0 &&
            raw.FlushCount >= 0 &&
            raw.ResizeCount >= 0 &&
            raw.SequentialReadCount >= 0 &&
            raw.SequentialBytesRead >= 0 &&
            raw.MemoryMappedPageExposureCount >= 0 &&
            raw.MemoryMappedBytesExposed >= 0 &&
            raw.SequentialReadCount <= raw.ReadCount &&
            raw.SequentialBytesRead <= raw.BytesRead;

    private static bool IsAtLeast(
        in StorageDeviceIoRuntimeRawSnapshot value,
        in StorageDeviceIoRuntimeRawSnapshot watermark)
        => value.ReadCount >= watermark.ReadCount &&
            value.BytesRead >= watermark.BytesRead &&
            value.WriteCount >= watermark.WriteCount &&
            value.BytesWritten >= watermark.BytesWritten &&
            value.FlushCount >= watermark.FlushCount &&
            value.ResizeCount >= watermark.ResizeCount &&
            value.SequentialReadCount >= watermark.SequentialReadCount &&
            value.SequentialBytesRead >= watermark.SequentialBytesRead &&
            value.MemoryMappedPageExposureCount >=
                watermark.MemoryMappedPageExposureCount &&
            value.MemoryMappedBytesExposed >=
                watermark.MemoryMappedBytesExposed;

    private static StorageDeviceIoRuntimeRawSnapshot Add(
        in StorageDeviceIoRuntimeRawSnapshot left,
        in StorageDeviceIoRuntimeRawSnapshot right)
        => new(
            SaturatingAdd(left.ReadCount, right.ReadCount),
            SaturatingAdd(left.BytesRead, right.BytesRead),
            SaturatingAdd(left.WriteCount, right.WriteCount),
            SaturatingAdd(left.BytesWritten, right.BytesWritten),
            SaturatingAdd(left.FlushCount, right.FlushCount),
            SaturatingAdd(left.ResizeCount, right.ResizeCount),
            SaturatingAdd(
                left.SequentialReadCount,
                right.SequentialReadCount),
            SaturatingAdd(
                left.SequentialBytesRead,
                right.SequentialBytesRead),
            SaturatingAdd(
                left.MemoryMappedPageExposureCount,
                right.MemoryMappedPageExposureCount),
            SaturatingAdd(
                left.MemoryMappedBytesExposed,
                right.MemoryMappedBytesExposed));

    private static StorageDeviceIoRuntimeRawSnapshot Subtract(
        in StorageDeviceIoRuntimeRawSnapshot value,
        in StorageDeviceIoRuntimeRawSnapshot watermark)
        => new(
            value.ReadCount - watermark.ReadCount,
            value.BytesRead - watermark.BytesRead,
            value.WriteCount - watermark.WriteCount,
            value.BytesWritten - watermark.BytesWritten,
            value.FlushCount - watermark.FlushCount,
            value.ResizeCount - watermark.ResizeCount,
            value.SequentialReadCount - watermark.SequentialReadCount,
            value.SequentialBytesRead - watermark.SequentialBytesRead,
            value.MemoryMappedPageExposureCount -
                watermark.MemoryMappedPageExposureCount,
            value.MemoryMappedBytesExposed -
                watermark.MemoryMappedBytesExposed);

    private static bool IsValid(StorageRecoveryRuntimeRawSnapshot raw)
        => Enum.IsDefined(raw.Phase) &&
            raw.Phase != StorageRecoveryPhaseRaw.Unknown &&
            raw.ScannedFrameCount >= 0 &&
            raw.ScannedBytes >= 0 &&
            raw.RecoveredFrameCount >= 0 &&
            raw.RecoveredBytes >= 0 &&
            raw.DiscardedFrameCount >= 0 &&
            raw.DiscardedBytes >= 0 &&
            raw.RecoveredFrameCount <= raw.ScannedFrameCount &&
            raw.RecoveredBytes <= raw.ScannedBytes &&
            raw.AttemptCount > 0 &&
            raw.RetryCount >= 0 &&
            raw.RetryCount < raw.AttemptCount &&
            Enum.IsDefined(raw.TruncationReason) &&
            Enum.IsDefined(raw.Outcome) &&
            Enum.IsDefined(raw.FailureKind);

    private static bool HasValidFileLifetimeCounterShape(
        WalRuntimeRawSnapshot wal)
    {
        bool hasFileExtent = wal.AllocatedBytes.HasValue;
        if (wal.CommitFlushBatchCount.HasValue != hasFileExtent ||
            wal.CommittedFrameBytesWritten.HasValue != hasFileExtent ||
            wal.FlushedCommitCount.HasValue != hasFileExtent ||
            wal.DurableFlushCount.HasValue != hasFileExtent ||
            wal.GroupCommitBatchCount.HasValue != hasFileExtent ||
            wal.GroupCommitCount.HasValue != hasFileExtent)
        {
            return false;
        }

        if (!hasFileExtent)
            return true;

        long publicationBatchCount = wal.CommitFlushBatchCount!.Value;
        long logicalCommitCount = wal.LogicalCommitCount!.Value;
        long flushedCommitCount = wal.FlushedCommitCount!.Value;
        long groupedBatchCount = wal.GroupCommitBatchCount!.Value;
        long groupedCommitCount = wal.GroupCommitCount!.Value;
        long minimumGroupedCommitCount = groupedBatchCount > long.MaxValue / 2
            ? long.MaxValue
            : groupedBatchCount * 2;
        return publicationBatchCount <= flushedCommitCount &&
            flushedCommitCount <= logicalCommitCount &&
            groupedBatchCount <= publicationBatchCount &&
            groupedCommitCount <= flushedCommitCount &&
            groupedCommitCount >= minimumGroupedCommitCount &&
            (groupedBatchCount == 0) == (groupedCommitCount == 0);
    }

    private static StorageRuntimeDiagnosticsCapture WithoutValue(
        DiagnosticsAvailability availability)
        => new(
            DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.WithoutValue(
                availability),
            DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.WithoutValue(
                availability));

    private static DiagnosticsSnapshotMetadata WithFieldsTruncated(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata.SchemaVersion,
            metadata.CapturedAtUtc,
            metadata.ServerInstanceId,
            metadata.CounterEpoch,
            metadata.Scope,
            metadata.Availability,
            metadata.Source,
            metadata.DatabaseAlias,
            metadata.RecordsTruncated,
            fieldsTruncated: true);

    private static long? Maximum(long? left, long? right)
        => left is null
            ? right
            : right is null
                ? left
                : Math.Max(left.Value, right.Value);

    private static long? AddOptionalCumulative(long? left, long? right)
        => left is null || right is null
            ? null
            : SaturatingAdd(left.Value, right.Value);

    internal static long SaturatingAdd(long left, long right)
        => left >= long.MaxValue - right ? long.MaxValue : left + right;

    internal static long SaturatingMultiply(long value, int multiplier)
    {
        if (value < 0)
            throw new ArgumentOutOfRangeException(nameof(value));
        if (multiplier < 0)
            throw new ArgumentOutOfRangeException(nameof(multiplier));
        if (value == 0 || multiplier == 0)
            return 0;
        return value > long.MaxValue / multiplier
            ? long.MaxValue
            : value * multiplier;
    }

    private static long SaturatingIncrement(long value)
        => value == long.MaxValue ? long.MaxValue : value + 1;

    private static int SaturatingAdd(int left, int right)
        => left >= int.MaxValue - right ? int.MaxValue : left + right;

    internal static int GetCheckpointPhaseRank(StorageCheckpointPhaseRaw phase)
        => phase switch
        {
            StorageCheckpointPhaseRaw.Idle => 0,
            StorageCheckpointPhaseRaw.Requested => 1,
            StorageCheckpointPhaseRaw.Copying => 2,
            StorageCheckpointPhaseRaw.CopyCompleteAwaitingReaders => 3,
            StorageCheckpointPhaseRaw.Finalizing => 4,
            StorageCheckpointPhaseRaw.Faulted => 5,
            _ => -1,
        };

    internal static CheckpointPhase MapCheckpointPhase(
        StorageCheckpointPhaseRaw phase)
        => phase switch
        {
            StorageCheckpointPhaseRaw.Idle => CheckpointPhase.Idle,
            StorageCheckpointPhaseRaw.Requested => CheckpointPhase.Requested,
            StorageCheckpointPhaseRaw.Copying => CheckpointPhase.Copying,
            StorageCheckpointPhaseRaw.CopyCompleteAwaitingReaders =>
                CheckpointPhase.CopyCompleteAwaitingReaders,
            StorageCheckpointPhaseRaw.Finalizing => CheckpointPhase.Finalizing,
            StorageCheckpointPhaseRaw.Faulted => CheckpointPhase.Faulted,
            _ => throw new ArgumentOutOfRangeException(nameof(phase)),
        };

    private static WalRecoveryPhase MapRecoveryPhase(
        StorageRecoveryPhaseRaw phase)
        => phase switch
        {
            StorageRecoveryPhaseRaw.Scanning => WalRecoveryPhase.Scanning,
            StorageRecoveryPhaseRaw.Checkpointing => WalRecoveryPhase.Checkpointing,
            StorageRecoveryPhaseRaw.Completed => WalRecoveryPhase.Completed,
            _ => throw new ArgumentOutOfRangeException(nameof(phase)),
        };

    private static WalRecoveryTruncationReason MapTruncationReason(
        StorageRecoveryTruncationReasonRaw reason)
        => reason switch
        {
            StorageRecoveryTruncationReasonRaw.Unknown =>
                WalRecoveryTruncationReason.Unknown,
            StorageRecoveryTruncationReasonRaw.None =>
                WalRecoveryTruncationReason.None,
            StorageRecoveryTruncationReasonRaw.IncompleteTail =>
                WalRecoveryTruncationReason.IncompleteTail,
            StorageRecoveryTruncationReasonRaw.SaltMismatch =>
                WalRecoveryTruncationReason.SaltMismatch,
            StorageRecoveryTruncationReasonRaw.ChecksumMismatch =>
                WalRecoveryTruncationReason.ChecksumMismatch,
            StorageRecoveryTruncationReasonRaw.UncommittedTail =>
                WalRecoveryTruncationReason.UncommittedTail,
            _ => throw new ArgumentOutOfRangeException(nameof(reason)),
        };

    private static CheckpointOrigin MapCheckpointOrigin(
        StorageCheckpointOriginRaw origin)
        => origin switch
        {
            StorageCheckpointOriginRaw.Unknown => CheckpointOrigin.Unknown,
            StorageCheckpointOriginRaw.Manual => CheckpointOrigin.Manual,
            StorageCheckpointOriginRaw.ForegroundAuto =>
                CheckpointOrigin.ForegroundAuto,
            StorageCheckpointOriginRaw.BackgroundAuto =>
                CheckpointOrigin.BackgroundAuto,
            StorageCheckpointOriginRaw.StartupRecovery =>
                CheckpointOrigin.StartupRecovery,
            StorageCheckpointOriginRaw.Backup => CheckpointOrigin.Backup,
            StorageCheckpointOriginRaw.Shutdown => CheckpointOrigin.Shutdown,
            _ => throw new ArgumentOutOfRangeException(nameof(origin)),
        };

    private static CheckpointRetentionReason MapRetentionReason(
        StorageCheckpointRetentionReasonRaw reason)
        => reason switch
        {
            StorageCheckpointRetentionReasonRaw.None =>
                CheckpointRetentionReason.None,
            StorageCheckpointRetentionReasonRaw.ActiveReaders =>
                CheckpointRetentionReason.ActiveReaders,
            StorageCheckpointRetentionReasonRaw.NewerCommits =>
                CheckpointRetentionReason.NewerCommits,
            StorageCheckpointRetentionReasonRaw.ActiveReadersAndNewerCommits =>
                CheckpointRetentionReason.ActiveReadersAndNewerCommits,
            _ => throw new ArgumentOutOfRangeException(nameof(reason)),
        };

    private static CSharpDbOperationOutcome MapOutcome(
        StorageRuntimeOperationOutcomeRaw outcome)
        => outcome switch
        {
            StorageRuntimeOperationOutcomeRaw.Unknown or
                StorageRuntimeOperationOutcomeRaw.Running =>
                CSharpDbOperationOutcome.Unknown,
            StorageRuntimeOperationOutcomeRaw.Succeeded =>
                CSharpDbOperationOutcome.Succeeded,
            StorageRuntimeOperationOutcomeRaw.Failed =>
                CSharpDbOperationOutcome.Failed,
            StorageRuntimeOperationOutcomeRaw.Canceled =>
                CSharpDbOperationOutcome.Canceled,
            _ => throw new ArgumentOutOfRangeException(nameof(outcome)),
        };

    private static SafeErrorProjection ProjectFailure(
        StorageRuntimeFailureKindRaw failure,
        CSharpDbOperationOutcome outcome)
    {
        SafeErrorKind kind = failure switch
        {
            StorageRuntimeFailureKindRaw.OperationCanceled =>
                SafeErrorKind.OperationCanceled,
            StorageRuntimeFailureKindRaw.TimedOut => SafeErrorKind.TimedOut,
            StorageRuntimeFailureKindRaw.AccessDenied =>
                SafeErrorKind.AccessDenied,
            StorageRuntimeFailureKindRaw.NotFound =>
                SafeErrorKind.DatabaseNotFound,
            StorageRuntimeFailureKindRaw.Busy => SafeErrorKind.DatabaseBusy,
            StorageRuntimeFailureKindRaw.ResourceLimit =>
                SafeErrorKind.DatabaseResourceLimit,
            StorageRuntimeFailureKindRaw.Corrupt =>
                SafeErrorKind.DatabaseCorrupt,
            StorageRuntimeFailureKindRaw.Io => SafeErrorKind.DatabaseIo,
            StorageRuntimeFailureKindRaw.Operation =>
                SafeErrorKind.DatabaseOperation,
            _ when outcome == CSharpDbOperationOutcome.Canceled =>
                SafeErrorKind.OperationCanceled,
            _ => SafeErrorKind.Unexpected,
        };
        return SafeErrorProjector.Project(kind);
    }

    private static ClockReading? TryCaptureClock(TimeProvider timeProvider)
    {
        try
        {
            return new ClockReading(
                timeProvider.GetUtcNow().ToUniversalTime(),
                timeProvider.GetTimestamp());
        }
        catch
        {
            return null;
        }
    }

    private static OperationStart? TryCreateOperationStart(
        TimeProvider timeProvider,
        OpaqueDiagnosticsId? operationId = null)
    {
        try
        {
            ClockReading? clock = TryCaptureClock(timeProvider);
            if (clock is not { } captured)
                return null;
            operationId ??= OpaqueDiagnosticsId.Create();
            return new OperationStart(
                operationId,
                operationId.Value,
                captured.UtcNow,
                captured.Timestamp);
        }
        catch
        {
            return null;
        }
    }

    private TimeSpan GetElapsed(
        RecoveryOperation operation,
        ClockReading? now)
        => operation.CompletedAtUtc.HasValue || now is null
            ? operation.Elapsed
            : CalculateElapsed(operation.StartingTimestamp, now.Value.Timestamp);

    private TimeSpan GetElapsed(
        CheckpointOperation operation,
        ClockReading? now)
        => operation.CompletedAtUtc.HasValue || now is null
            ? operation.Elapsed
            : CalculateElapsed(operation.StartingTimestamp, now.Value.Timestamp);

    private TimeSpan CalculateElapsed(long startingTimestamp, long endingTimestamp)
    {
        try
        {
            long delta = unchecked(endingTimestamp - startingTimestamp);
            if (delta <= 0)
                return TimeSpan.Zero;
            double ticks = delta *
                (double)TimeSpan.TicksPerSecond /
                _timestampFrequency;
            if (ticks >= TimeSpan.MaxValue.Ticks)
                return TimeSpan.MaxValue;
            return TimeSpan.FromTicks((long)ticks);
        }
        catch
        {
            return TimeSpan.Zero;
        }
    }

    private static DateTimeOffset SafeCompletionUtc(
        DateTimeOffset startedAtUtc,
        TimeSpan elapsed)
    {
        try
        {
            return startedAtUtc + elapsed;
        }
        catch
        {
            return startedAtUtc;
        }
    }

    private readonly record struct ClockReading(
        DateTimeOffset UtcNow,
        long Timestamp);

    internal readonly record struct OperationStart(
        OpaqueDiagnosticsId OperationId,
        string SortKey,
        DateTimeOffset StartedAtUtc,
        long StartingTimestamp);

    internal sealed class RecoveryOperation
    {
        internal RecoveryOperation(OperationStart start)
        {
            OperationId = start.OperationId;
            SortKey = start.SortKey;
            StartedAtUtc = start.StartedAtUtc;
            StartingTimestamp = start.StartingTimestamp;
            Raw = new StorageRecoveryRuntimeRawSnapshot(
                StorageRecoveryPhaseRaw.Scanning,
                0, 0, 0, 0, 0, 0,
                StorageRecoveryTruncationReasonRaw.Unknown,
                AttemptCount: 1,
                RetryCount: 0,
                LastRetryFailureKind: StorageRuntimeFailureKindRaw.None,
                Outcome: StorageRuntimeOperationOutcomeRaw.Running,
                FailureKind: StorageRuntimeFailureKindRaw.None);
        }

        internal OpaqueDiagnosticsId OperationId { get; }
        internal string SortKey { get; }
        internal DateTimeOffset StartedAtUtc { get; }
        internal long StartingTimestamp { get; }
        internal DateTimeOffset? CompletedAtUtc { get; set; }
        internal TimeSpan Elapsed { get; set; }
        internal StorageRecoveryRuntimeRawSnapshot Raw { get; set; }
        internal long LastObservedRetryCount { get; set; }
    }

    internal sealed class CheckpointOperation
    {
        internal CheckpointOperation(
            OperationStart start,
            StorageCheckpointRuntimeRawSnapshot raw)
        {
            OperationId = start.OperationId;
            SortKey = start.SortKey;
            StartedAtUtc = start.StartedAtUtc;
            StartingTimestamp = start.StartingTimestamp;
            Raw = raw;
        }

        internal OpaqueDiagnosticsId OperationId { get; }
        internal string SortKey { get; }
        internal DateTimeOffset StartedAtUtc { get; }
        internal long StartingTimestamp { get; }
        internal DateTimeOffset? CompletedAtUtc { get; set; }
        internal TimeSpan Elapsed { get; set; }
        internal StorageCheckpointRuntimeRawSnapshot Raw { get; set; }
    }

    internal sealed class Registration :
        IDisposable,
        IStorageRuntimeDiagnosticsObserver
    {
        private StorageRuntimeDiagnostics? _owner;

        internal Registration(
            StorageRuntimeDiagnostics owner,
            Pager? provider,
            StorageRuntimeDiagnosticsProvenance provenance,
            bool observesStorage,
            bool recoveryApplicable)
        {
            _owner = owner;
            Provider = provider;
            Provenance = provenance;
            ObservesStorage = observesStorage;
            RecoveryApplicable = recoveryApplicable;
        }

        internal Pager? Provider { get; set; }
        internal bool ProviderDrained { get; set; }
        internal long? RetiredDurableFlushWatermark { get; set; }
        internal StorageDeviceIoRuntimeRawSnapshot? RetiredPhysicalIoWatermark
        {
            get;
            set;
        }
        internal StorageDeviceIoRuntimeRawSnapshot? PendingFinalPhysicalIo
        {
            get;
            set;
        }
        internal StorageDeviceIoRuntimeRawSnapshot? FinalPhysicalIoSample
        {
            get;
            set;
        }
        internal bool PendingPhysicalIoInvalid { get; set; }
        internal bool FinalPhysicalIoReconciled { get; set; }
        internal StorageRuntimeDiagnosticsProvenance Provenance { get; }
        internal bool ObservesStorage { get; }
        internal bool RecoveryApplicable { get; }
        internal RecoveryOperation? Recovery { get; set; }
        internal CheckpointOperation? Checkpoint { get; set; }

        internal IStorageRuntimeDiagnosticsObserver Observer => this;

        internal void Promote(Pager pager)
        {
            ArgumentNullException.ThrowIfNull(pager);
            try
            {
                Volatile.Read(ref _owner)?.Promote(this, pager);
            }
            catch
            {
            }
        }

        internal void DrainProvider()
        {
            try
            {
                Volatile.Read(ref _owner)?.DrainProvider(this);
            }
            catch
            {
            }
        }

        public void Dispose()
        {
            StorageRuntimeDiagnostics? owner = Interlocked.Exchange(
                ref _owner,
                null);
            owner?.Unregister(this);
        }

        internal void DetachOwner(StorageRuntimeDiagnostics owner)
            => Interlocked.CompareExchange(ref _owner, null, owner);

        object? IStorageRuntimeDiagnosticsObserver.CaptureCheckpointCorrelation(
            StorageCheckpointOriginRaw origin)
        {
            StorageRuntimeDiagnostics? owner = Volatile.Read(ref _owner);
            if (owner is null)
                return null;

            OpaqueDiagnosticsId? operationId = null;
            if (origin == StorageCheckpointOriginRaw.StartupRecovery)
                operationId = owner.GetActiveRecoveryOperationId(this);
            CSharpDbOperationContext? ambient = CSharpDbOperationScope.Current;
            if (origin == StorageCheckpointOriginRaw.Manual &&
                ambient?.OperationClass == CSharpDbOperationClass.Checkpoint)
            {
                operationId = ambient.OperationId;
            }
            else if (origin == StorageCheckpointOriginRaw.Backup &&
                     ambient?.OperationClass == CSharpDbOperationClass.Backup)
            {
                operationId = ambient.OperationId;
            }

            return TryCreateOperationStart(owner._timeProvider, operationId);
        }

        object? IStorageRuntimeDiagnosticsObserver
            .CaptureCheckpointCompletionCorrelation()
        {
            StorageRuntimeDiagnostics? owner = Volatile.Read(ref _owner);
            return owner is null
                ? null
                : TryCaptureClock(owner._timeProvider);
        }

        void IStorageRuntimeDiagnosticsObserver.OnRecoveryStarted()
        {
            try
            {
                StorageRuntimeDiagnostics? owner = Volatile.Read(ref _owner);
                OperationStart? start = owner is null
                    ? null
                    : TryCreateOperationStart(owner._timeProvider);
                if (start is { } value)
                    owner?.RecoveryStarted(this, new RecoveryOperation(value));
            }
            catch
            {
            }
        }

        void IStorageRuntimeDiagnosticsObserver.OnRecoveryChanged(
            in StorageRecoveryRuntimeRawSnapshot snapshot)
        {
            try
            {
                StorageRuntimeDiagnostics? owner = Volatile.Read(ref _owner);
                if (owner is null)
                    return;
                RecoveryOperation? existing = Recovery;
                if (existing?.CompletedAtUtc is not null)
                    return;
                OperationStart? start = existing is null
                    ? TryCreateOperationStart(owner._timeProvider)
                    : null;
                owner.RecoveryChanged(
                    this,
                    start is { } value
                        ? new RecoveryOperation(value)
                        : null,
                    snapshot,
                    TryCaptureClock(owner._timeProvider));
            }
            catch
            {
            }
        }

        void IStorageRuntimeDiagnosticsObserver.OnRecoveryCompleted(
            in StorageRecoveryRuntimeRawSnapshot snapshot)
        {
            try
            {
                StorageRuntimeDiagnostics? owner = Volatile.Read(ref _owner);
                if (owner is null)
                    return;
                RecoveryOperation? existing = Recovery;
                if (existing?.CompletedAtUtc is not null)
                    return;
                OperationStart? start = existing is null
                    ? TryCreateOperationStart(owner._timeProvider)
                    : null;
                owner.RecoveryCompleted(
                    this,
                    start is { } value
                        ? new RecoveryOperation(value)
                        : null,
                    snapshot,
                    TryCaptureClock(owner._timeProvider));
            }
            catch
            {
            }
        }

        void IStorageRuntimeDiagnosticsObserver.OnCheckpointStarted(
            in StorageCheckpointRuntimeRawSnapshot snapshot,
            object? correlation)
        {
            try
            {
                StorageRuntimeDiagnostics? owner = Volatile.Read(ref _owner);
                if (owner is null)
                    return;
                OperationStart? start = correlation is OperationStart captured
                    ? captured
                    : TryCreateOperationStart(owner._timeProvider);
                if (start is { } value)
                {
                    owner.CheckpointStarted(
                        this,
                        new CheckpointOperation(value, snapshot));
                }
            }
            catch
            {
            }
        }

        void IStorageRuntimeDiagnosticsObserver.OnCheckpointChanged(
            in StorageCheckpointRuntimeRawSnapshot snapshot)
        {
            try
            {
                StorageRuntimeDiagnostics? owner = Volatile.Read(ref _owner);
                if (owner is null)
                    return;
                owner.CheckpointChanged(
                    this,
                    snapshot,
                    TryCaptureClock(owner._timeProvider));
            }
            catch
            {
            }
        }

        void IStorageRuntimeDiagnosticsObserver.OnCheckpointCompleted(
            in StorageCheckpointRuntimeRawSnapshot snapshot,
            object? correlation)
        {
            try
            {
                StorageRuntimeDiagnostics? owner = Volatile.Read(ref _owner);
                if (owner is null)
                    return;
                owner.CheckpointCompleted(
                    this,
                    snapshot,
                    correlation is ClockReading captured
                        ? captured
                        : TryCaptureClock(owner._timeProvider));
            }
            catch
            {
            }
        }

        void IStorageRuntimeDiagnosticsObserver.OnWalFlushCompleted(
            int logicalCommitCount)
        {
            try
            {
                StorageRuntimeDiagnostics? owner = Volatile.Read(ref _owner);
                if (owner is null)
                    return;
                ClockReading? completed = TryCaptureClock(owner._timeProvider);
                if (completed is { } value)
                {
                    owner.WalFlushCompleted(
                        logicalCommitCount,
                        value.UtcNow);
                }
            }
            catch
            {
            }
        }

        void IStorageRuntimeDiagnosticsObserver.OnWalDurableFlushCompleted(
            long durableFlushCount)
        {
            try
            {
                StorageRuntimeDiagnostics? owner = Volatile.Read(ref _owner);
                if (owner is null)
                    return;
                owner.WalDurableFlushCompleted(
                    this,
                    durableFlushCount,
                    TryCaptureClock(owner._timeProvider));
            }
            catch
            {
            }
        }

        void IStorageRuntimeDiagnosticsObserver.OnStorageDeviceIoSealed(
            in StorageDeviceIoRuntimeRawSnapshot snapshot)
        {
            try
            {
                Volatile.Read(ref _owner)?.StorageDeviceIoSealed(this, snapshot);
            }
            catch
            {
            }
        }
    }
}
