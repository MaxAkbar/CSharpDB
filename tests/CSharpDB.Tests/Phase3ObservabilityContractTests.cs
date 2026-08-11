using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using CSharpDB.Client;
using CSharpDB.Observability;

namespace CSharpDB.Tests;

public sealed class Phase3ObservabilityContractTests
{
    private static readonly DateTimeOffset UtcNow =
        new(2026, 8, 11, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Storage_PositionalConstructorAndDeconstructRemainBackwardCompatible()
    {
        DiagnosticsSnapshotMetadata metadata = Metadata();
        StorageRuntimeDiagnosticsSnapshot storage = Storage(metadata);

        (
            DiagnosticsSnapshotMetadata deconstructedMetadata,
            long? logicalDatabaseBytes,
            long? allocatedDatabaseBytes,
            long? pageCount,
            long? pageReads,
            long? pageWrites,
            long? bytesRead,
            long? bytesWritten,
            long? cacheHits,
            long? cacheMisses,
            long? dirtyPages,
            int? activeReaders,
            int? activeWriters,
            long? commitCount,
            long? conflictCount) = storage;

        Assert.Equal(metadata, deconstructedMetadata);
        Assert.Equal(4_096, logicalDatabaseBytes);
        Assert.Equal(8_192, allocatedDatabaseBytes);
        Assert.Equal(2, pageCount);
        Assert.Equal(5, pageReads);
        Assert.Equal(3, pageWrites);
        Assert.Equal(20_480, bytesRead);
        Assert.Equal(12_288, bytesWritten);
        Assert.Equal(3, cacheHits);
        Assert.Equal(2, cacheMisses);
        Assert.Equal(1, dirtyPages);
        Assert.Equal(1, activeReaders);
        Assert.Equal(0, activeWriters);
        Assert.Equal(4, commitCount);
        Assert.Equal(1, conflictCount);
        Assert.Equal(DiagnosticsAvailability.Unavailable, storage.Cache.Availability);
        Assert.Equal(
            DiagnosticsAvailability.Unavailable,
            storage.PhysicalIo.Availability);

        ConstructorInfo constructor = Assert.Single(
            typeof(StorageRuntimeDiagnosticsSnapshot).GetConstructors());
        Assert.Equal(15, constructor.GetParameters().Length);
        MethodInfo deconstruct = Assert.Single(
            typeof(StorageRuntimeDiagnosticsSnapshot)
                .GetMethods(BindingFlags.Public | BindingFlags.Instance),
            static method => method.Name == "Deconstruct");
        Assert.Equal(15, deconstruct.GetParameters().Length);
    }

    [Fact]
    public void Storage_OldJsonPayloadDefaultsDetailSectionsToUnavailable()
    {
        string currentJson = JsonSerializer.Serialize(
            StorageWithDetails(Metadata()),
            CSharpDbObservabilityJsonContext.Default
                .StorageRuntimeDiagnosticsSnapshot);
        JsonObject oldPayload = Assert.IsType<JsonObject>(JsonNode.Parse(currentJson));
        Assert.True(oldPayload.Remove("cache"));
        Assert.True(oldPayload.Remove("physicalIo"));
        oldPayload["metadata"]!["schemaVersion"] = "1.0";

        StorageRuntimeDiagnosticsSnapshot restored = Assert.IsType<
            StorageRuntimeDiagnosticsSnapshot>(JsonSerializer.Deserialize(
                oldPayload.ToJsonString(),
                CSharpDbObservabilityJsonContext.Default
                    .StorageRuntimeDiagnosticsSnapshot));

        Assert.Equal("1.0", restored.Metadata.SchemaVersion);
        Assert.Equal(DiagnosticsAvailability.Unavailable, restored.Cache.Availability);
        Assert.Null(restored.Cache.Value);
        Assert.Equal(
            DiagnosticsAvailability.Unavailable,
            restored.PhysicalIo.Availability);
        Assert.Null(restored.PhysicalIo.Value);
    }

    [Fact]
    public void Storage_DetailSnapshotsValidateLocallyAndRoundTrip()
    {
        DiagnosticsSnapshotMetadata metadata = Metadata();
        StorageRuntimeDiagnosticsSnapshot storage = StorageWithDetails(metadata);

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new StorageCacheDiagnosticsSnapshot(metadata, -1, null, 0, 0));
        Assert.Throws<ArgumentException>(() =>
            new StorageCacheDiagnosticsSnapshot(metadata, 3, 2, 0, 0));
        Assert.Throws<ArgumentException>(() =>
            new StorageCacheDiagnosticsSnapshot(metadata, 0, null, 2, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new StorageDeviceIoDiagnosticsSnapshot(
                metadata, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        Assert.Throws<ArgumentException>(() =>
            new StorageDeviceIoDiagnosticsSnapshot(
                metadata, 1, 10, 0, 0, 0, 0, 2, 0, 0, 0));
        Assert.Throws<ArgumentException>(() =>
            new StorageDeviceIoDiagnosticsSnapshot(
                metadata, 1, 10, 0, 0, 0, 0, 1, 11, 0, 0));

        StorageRuntimeDiagnosticsSnapshot roundTripped = Assert.IsType<
            StorageRuntimeDiagnosticsSnapshot>(JsonSerializer.Deserialize(
                JsonSerializer.Serialize(
                    storage,
                    CSharpDbObservabilityJsonContext.Default
                        .StorageRuntimeDiagnosticsSnapshot),
                CSharpDbObservabilityJsonContext.Default
                    .StorageRuntimeDiagnosticsSnapshot));
        Assert.Equal(storage, roundTripped);
    }

    [Fact]
    public void Storage_DetailAvailabilityAndMetadataAreLocalToEachSection()
    {
        DiagnosticsSnapshotMetadata metadata = Metadata();
        DiagnosticsSnapshotMetadata other = Metadata(databaseAlias: "other");
        StorageRuntimeDiagnosticsSnapshot withoutDetails = Storage(metadata) with
        {
            Cache = DiagnosticsSection<StorageCacheDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.NotApplicable),
            PhysicalIo = DiagnosticsSection<StorageDeviceIoDiagnosticsSnapshot>
                .WithoutValue(DiagnosticsAvailability.Unsupported),
        };

        Assert.Equal(
            DiagnosticsAvailability.NotApplicable,
            withoutDetails.Cache.Availability);
        Assert.Equal(
            DiagnosticsAvailability.Unsupported,
            withoutDetails.PhysicalIo.Availability);
        Assert.Throws<ArgumentException>(() => _ = Storage(other) with
        {
            Cache = DiagnosticsSection<StorageCacheDiagnosticsSnapshot>.Available(
                StorageCache(metadata)),
        });
        Assert.Throws<ArgumentException>(() => _ = Storage(other) with
        {
            PhysicalIo = DiagnosticsSection<StorageDeviceIoDiagnosticsSnapshot>
                .Available(StoragePhysicalIo(metadata)),
        });

        StorageRuntimeDiagnosticsSnapshot withDetails =
            StorageWithDetails(metadata);
        Assert.Throws<ArgumentException>(() => _ = withDetails with
        {
            Metadata = other,
        });
    }

    [Fact]
    public void DirectStorageProjector_ReprojectsNestedMetadataAndPreservesValues()
    {
        StorageRuntimeDiagnosticsSnapshot source = StorageWithDetails(Metadata());
        DiagnosticsSnapshotMetadata projectedMetadata = Metadata(
            databaseAlias: "projected");
        Type? projectorType = typeof(CSharpDbClient).Assembly
            .GetType("CSharpDB.Client.Internal.EngineTransportClient");
        Assert.NotNull(projectorType);
        MethodInfo? projector = projectorType!.GetMethod(
            "ReprojectStorageSnapshot",
            BindingFlags.Static | BindingFlags.NonPublic);
        Assert.NotNull(projector);

        StorageRuntimeDiagnosticsSnapshot projected = Assert.IsType<
            StorageRuntimeDiagnosticsSnapshot>(projector!.Invoke(
                null,
                [source, projectedMetadata]));

        Assert.Equal(projectedMetadata, projected.Metadata);
        Assert.Equal(projectedMetadata, projected.Cache.Value!.Metadata);
        Assert.Equal(projectedMetadata, projected.PhysicalIo.Value!.Metadata);
        Assert.Equal(source.Cache.Value!.SharedResidentPages,
            projected.Cache.Value!.SharedResidentPages);
        Assert.Equal(source.PhysicalIo.Value!.SequentialBytesRead,
            projected.PhysicalIo.Value!.SequentialBytesRead);
        Assert.Equal(source.PageReads, projected.PageReads);
        Assert.Equal(source.CacheHits, projected.CacheHits);
        Assert.Equal(source.CacheMisses, projected.CacheMisses);
    }

    [Fact]
    public void Wal_PositionalConstructorAndDeconstructRemainBackwardCompatible()
    {
        DiagnosticsSnapshotMetadata metadata = Metadata();
        WalRuntimeDiagnosticsSnapshot wal = Wal(metadata);

        (
            DiagnosticsSnapshotMetadata deconstructedMetadata,
            long? logicalBytes,
            long? allocatedBytes,
            long? committedFrameBytes,
            long? retainedBytes,
            long? frameCount,
            long? flushCount,
            long? bytesWritten,
            int? pendingCommitCount,
            CheckpointPhase checkpointPhase,
            DateTimeOffset? lastSuccessfulFlushAtUtc,
            DateTimeOffset? lastSuccessfulCheckpointAtUtc,
            SafeErrorProjection? lastError) = wal;

        Assert.Equal(metadata, deconstructedMetadata);
        Assert.Equal(10, logicalBytes);
        Assert.Equal(12, allocatedBytes);
        Assert.Equal(8, committedFrameBytes);
        Assert.Equal(2, retainedBytes);
        Assert.Equal(2, frameCount);
        Assert.Equal(3, flushCount);
        Assert.Equal(24, bytesWritten);
        Assert.Equal(1, pendingCommitCount);
        Assert.Equal(CheckpointPhase.Idle, checkpointPhase);
        Assert.Equal(UtcNow, lastSuccessfulFlushAtUtc);
        Assert.Equal(UtcNow, lastSuccessfulCheckpointAtUtc);
        Assert.Null(lastError);
        Assert.Equal(5, wal.FlushedCommitCount);
        Assert.Equal(2, wal.DurableFlushCount);
        Assert.Equal(UtcNow, wal.LastSuccessfulDurableFlushAtUtc);
        Assert.Equal(2, wal.GroupCommitBatchCount);
        Assert.Equal(4, wal.GroupCommitCount);
        Assert.Equal(UtcNow, wal.LastSuccessfulGroupCommitAtUtc);
        Assert.Equal(DiagnosticsAvailability.Unavailable, wal.Recovery.Availability);
        Assert.Equal(DiagnosticsAvailability.Unavailable, wal.Checkpoint.Availability);

        ConstructorInfo constructor = Assert.Single(
            typeof(WalRuntimeDiagnosticsSnapshot).GetConstructors());
        Assert.Equal(13, constructor.GetParameters().Length);
        MethodInfo deconstruct = Assert.Single(
            typeof(WalRuntimeDiagnosticsSnapshot)
                .GetMethods(BindingFlags.Public | BindingFlags.Instance),
            static method => method.Name == "Deconstruct");
        Assert.Equal(13, deconstruct.GetParameters().Length);
        Assert.NotNull(typeof(WalRuntimeDiagnosticsSnapshot)
            .GetProperty(nameof(WalRuntimeDiagnosticsSnapshot.FlushedCommitCount))?
            .SetMethod);
    }

    [Fact]
    public void Wal_OldJsonPayloadDefaultsNewMembersSafely()
    {
        string currentJson = JsonSerializer.Serialize(
            Wal(Metadata()),
            CSharpDbObservabilityJsonContext.Default.WalRuntimeDiagnosticsSnapshot);
        JsonObject oldPayload = Assert.IsType<JsonObject>(JsonNode.Parse(currentJson));
        Assert.True(oldPayload.Remove("recovery"));
        Assert.True(oldPayload.Remove("checkpoint"));
        Assert.True(oldPayload.Remove("flushedCommitCount"));
        Assert.True(oldPayload.Remove("durableFlushCount"));
        Assert.True(oldPayload.Remove("lastSuccessfulDurableFlushAtUtc"));
        Assert.True(oldPayload.Remove("groupCommitBatchCount"));
        Assert.True(oldPayload.Remove("groupCommitCount"));
        Assert.True(oldPayload.Remove("lastSuccessfulGroupCommitAtUtc"));
        oldPayload["metadata"]!["schemaVersion"] = "1.0";

        WalRuntimeDiagnosticsSnapshot restored = Assert.IsType<
            WalRuntimeDiagnosticsSnapshot>(JsonSerializer.Deserialize(
                oldPayload.ToJsonString(),
                CSharpDbObservabilityJsonContext.Default.WalRuntimeDiagnosticsSnapshot));

        Assert.Equal("1.0", restored.Metadata.SchemaVersion);
        Assert.Equal(DiagnosticsAvailability.Unavailable, restored.Recovery.Availability);
        Assert.Null(restored.Recovery.Value);
        Assert.Equal(DiagnosticsAvailability.Unavailable, restored.Checkpoint.Availability);
        Assert.Null(restored.Checkpoint.Value);
        Assert.Null(restored.FlushedCommitCount);
        Assert.Null(restored.DurableFlushCount);
        Assert.Null(restored.LastSuccessfulDurableFlushAtUtc);
        Assert.Null(restored.GroupCommitBatchCount);
        Assert.Null(restored.GroupCommitCount);
        Assert.Null(restored.LastSuccessfulGroupCommitAtUtc);
    }

    [Fact]
    public void Wal_DurabilityScalarsValidateLocallyAndJsonOrderIsIndependent()
    {
        DiagnosticsSnapshotMetadata metadata = Metadata();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            _ = Wal(metadata) with { FlushedCommitCount = -1 });
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            _ = Wal(metadata) with { DurableFlushCount = -1 });
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            _ = Wal(metadata) with { GroupCommitBatchCount = -1 });
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            _ = Wal(metadata) with { GroupCommitCount = -1 });
        Assert.Throws<ArgumentException>(() => _ = Wal(metadata) with
        {
            LastSuccessfulDurableFlushAtUtc =
                UtcNow.ToOffset(TimeSpan.FromHours(2)),
        });
        Assert.Throws<ArgumentException>(() => _ = Wal(metadata) with
        {
            LastSuccessfulGroupCommitAtUtc =
                UtcNow.ToOffset(TimeSpan.FromHours(-3)),
        });

        JsonObject serialized = Assert.IsType<JsonObject>(JsonNode.Parse(
            JsonSerializer.Serialize(
                Wal(metadata),
                CSharpDbObservabilityJsonContext.Default
                    .WalRuntimeDiagnosticsSnapshot)));
        string[] durabilityNames =
        [
            "lastSuccessfulGroupCommitAtUtc",
            "lastSuccessfulDurableFlushAtUtc",
            "groupCommitCount",
            "groupCommitBatchCount",
            "durableFlushCount",
            "flushedCommitCount",
        ];
        var reordered = new JsonObject();
        foreach (string name in durabilityNames)
            reordered[name] = serialized[name]!.DeepClone();
        foreach ((string name, JsonNode? value) in serialized)
        {
            if (!durabilityNames.Contains(name, StringComparer.Ordinal))
                reordered[name] = value?.DeepClone();
        }

        WalRuntimeDiagnosticsSnapshot roundTripped = Assert.IsType<
            WalRuntimeDiagnosticsSnapshot>(JsonSerializer.Deserialize(
                reordered.ToJsonString(),
                CSharpDbObservabilityJsonContext.Default
                    .WalRuntimeDiagnosticsSnapshot));
        Assert.Equal(Wal(metadata), roundTripped);
    }

    [Fact]
    public void Wal_FileOnlyDurabilityScalarsRemainOptional()
    {
        WalRuntimeDiagnosticsSnapshot unknown = Wal(Metadata()) with
        {
            FlushedCommitCount = null,
            DurableFlushCount = null,
            LastSuccessfulDurableFlushAtUtc = null,
            GroupCommitBatchCount = null,
            GroupCommitCount = null,
            LastSuccessfulGroupCommitAtUtc = null,
        };

        Assert.Null(unknown.FlushedCommitCount);
        Assert.Null(unknown.DurableFlushCount);
        Assert.Null(unknown.LastSuccessfulDurableFlushAtUtc);
        Assert.Null(unknown.GroupCommitBatchCount);
        Assert.Null(unknown.GroupCommitCount);
        Assert.Null(unknown.LastSuccessfulGroupCommitAtUtc);
        Assert.Equal(
            unknown,
            JsonSerializer.Deserialize(
                JsonSerializer.Serialize(
                    unknown,
                    CSharpDbObservabilityJsonContext.Default
                        .WalRuntimeDiagnosticsSnapshot),
                CSharpDbObservabilityJsonContext.Default
                    .WalRuntimeDiagnosticsSnapshot));
    }

    [Fact]
    public void RecoveryAndCheckpoint_RoundTripWithMatchingParentMetadata()
    {
        DiagnosticsSnapshotMetadata metadata = Metadata();
        WalRecoveryDiagnosticsSnapshot recovery = Recovery(metadata);
        CheckpointDiagnosticsSnapshot checkpoint = Checkpoint(metadata);
        WalRuntimeDiagnosticsSnapshot wal = Wal(metadata, checkpoint.Phase) with
        {
            Recovery = DiagnosticsSection<WalRecoveryDiagnosticsSnapshot>.Available(
                recovery),
            Checkpoint = DiagnosticsSection<CheckpointDiagnosticsSnapshot>.Available(
                checkpoint),
        };

        Assert.Equal("1.1", wal.Metadata.SchemaVersion);
        Assert.Equal(2, recovery.AttemptCount);
        Assert.Equal(1, recovery.RetryCount);
        Assert.NotNull(recovery.LastRetryError);
        Assert.Null(recovery.Error);

        Assert.Equal(
            recovery,
            JsonSerializer.Deserialize(
                JsonSerializer.Serialize(
                    recovery,
                    CSharpDbObservabilityJsonContext.Default.WalRecoveryDiagnosticsSnapshot),
                CSharpDbObservabilityJsonContext.Default.WalRecoveryDiagnosticsSnapshot));
        Assert.Equal(
            checkpoint,
            JsonSerializer.Deserialize(
                JsonSerializer.Serialize(
                    checkpoint,
                    CSharpDbObservabilityJsonContext.Default.CheckpointDiagnosticsSnapshot),
                CSharpDbObservabilityJsonContext.Default.CheckpointDiagnosticsSnapshot));
        Assert.Equal(
            wal,
            JsonSerializer.Deserialize(
                JsonSerializer.Serialize(
                    wal,
                    CSharpDbObservabilityJsonContext.Default.WalRuntimeDiagnosticsSnapshot),
                CSharpDbObservabilityJsonContext.Default.WalRuntimeDiagnosticsSnapshot));

        Assert.Equal(
            typeof(long),
            typeof(WalRecoveryDiagnosticsSnapshot)
                .GetProperty(nameof(WalRecoveryDiagnosticsSnapshot.AttemptCount))!
                .PropertyType);
        Assert.Equal(
            typeof(long),
            typeof(WalRecoveryDiagnosticsSnapshot)
                .GetProperty(nameof(WalRecoveryDiagnosticsSnapshot.RetryCount))!
                .PropertyType);
    }

    [Fact]
    public void RecoveryAndCheckpoint_ConstructorsRejectImpossibleShapes()
    {
        DiagnosticsSnapshotMetadata metadata = Metadata();

        Assert.Throws<ArgumentException>(() => new WalRecoveryDiagnosticsSnapshot(
            metadata,
            Id('a'),
            WalRecoveryPhase.Completed,
            UtcNow,
            UtcNow,
            TimeSpan.Zero,
            CSharpDbOperationOutcome.Succeeded,
            scannedFrameCount: 1,
            scannedBytes: 10,
            recoveredFrameCount: 2,
            recoveredBytes: 8,
            discardedFrameCount: 0,
            discardedBytes: 0,
            WalRecoveryTruncationReason.None,
            attemptCount: 1,
            retryCount: 0,
            lastRetryError: null,
            error: null));
        Assert.Throws<ArgumentException>(() => new WalRecoveryDiagnosticsSnapshot(
            metadata,
            Id('a'),
            WalRecoveryPhase.Scanning,
            UtcNow,
            completedAtUtc: UtcNow,
            TimeSpan.Zero,
            CSharpDbOperationOutcome.Unknown,
            scannedFrameCount: 0,
            scannedBytes: 0,
            recoveredFrameCount: 0,
            recoveredBytes: 0,
            discardedFrameCount: 0,
            discardedBytes: 0,
            WalRecoveryTruncationReason.Unknown,
            attemptCount: 1,
            retryCount: 0,
            lastRetryError: null,
            error: null));
        Assert.Throws<ArgumentException>(() => new WalRecoveryDiagnosticsSnapshot(
            metadata,
            Id('a'),
            WalRecoveryPhase.Completed,
            UtcNow,
            UtcNow,
            TimeSpan.Zero,
            CSharpDbOperationOutcome.Succeeded,
            scannedFrameCount: 0,
            scannedBytes: 0,
            recoveredFrameCount: 0,
            recoveredBytes: 0,
            discardedFrameCount: 0,
            discardedBytes: 0,
            WalRecoveryTruncationReason.None,
            attemptCount: 1,
            retryCount: 1,
            lastRetryError: SafeErrorProjector.Project(SafeErrorKind.DatabaseIo),
            error: null));
        Assert.Throws<ArgumentException>(() => new WalRecoveryDiagnosticsSnapshot(
            metadata,
            Id('a'),
            WalRecoveryPhase.Completed,
            UtcNow,
            UtcNow,
            TimeSpan.Zero,
            CSharpDbOperationOutcome.Succeeded,
            scannedFrameCount: 0,
            scannedBytes: 0,
            recoveredFrameCount: 0,
            recoveredBytes: 0,
            discardedFrameCount: 0,
            discardedBytes: 0,
            WalRecoveryTruncationReason.None,
            attemptCount: 2,
            retryCount: 1,
            lastRetryError: null,
            error: null));
        Assert.Throws<ArgumentException>(() => new CheckpointDiagnosticsSnapshot(
            metadata,
            Id('b'),
            CheckpointPhase.Copying,
            CheckpointOrigin.BackgroundAuto,
            UtcNow,
            TimeSpan.Zero,
            completedPageCount: 2,
            totalPageCount: 1,
            CheckpointRetentionReason.NewerCommits,
            lastStartedAtUtc: UtcNow,
            lastSuccessfulAtUtc: null,
            lastFailedAtUtc: null,
            lastElapsed: TimeSpan.Zero,
            activeCount: 1,
            attemptCount: 1,
            successCount: 0,
            failureCount: 0,
            canceledCount: 0,
            lastError: null));
        Assert.Throws<ArgumentException>(() => new CheckpointDiagnosticsSnapshot(
            metadata,
            operationId: null,
            CheckpointPhase.Idle,
            CheckpointOrigin.Manual,
            startedAtUtc: null,
            elapsed: null,
            completedPageCount: null,
            totalPageCount: null,
            CheckpointRetentionReason.None,
            lastStartedAtUtc: UtcNow,
            lastSuccessfulAtUtc: UtcNow,
            lastFailedAtUtc: UtcNow,
            lastElapsed: TimeSpan.Zero,
            activeCount: 0,
            attemptCount: 2,
            successCount: 1,
            failureCount: 1,
            canceledCount: 1,
            lastError: SafeErrorProjector.Project(SafeErrorKind.OperationCanceled)));
        Assert.Throws<ArgumentException>(() => new CheckpointDiagnosticsSnapshot(
            metadata,
            operationId: null,
            CheckpointPhase.CopyCompleteAwaitingReaders,
            CheckpointOrigin.BackgroundAuto,
            startedAtUtc: null,
            elapsed: null,
            completedPageCount: null,
            totalPageCount: null,
            CheckpointRetentionReason.ActiveReaders,
            lastStartedAtUtc: UtcNow,
            lastSuccessfulAtUtc: null,
            lastFailedAtUtc: null,
            lastElapsed: TimeSpan.Zero,
            activeCount: 1,
            attemptCount: 1,
            successCount: 0,
            failureCount: 0,
            canceledCount: 0,
            lastError: null));
        Assert.Throws<ArgumentException>(() => new CheckpointDiagnosticsSnapshot(
            metadata,
            operationId: null,
            CheckpointPhase.Idle,
            CheckpointOrigin.Manual,
            startedAtUtc: null,
            elapsed: null,
            completedPageCount: null,
            totalPageCount: null,
            CheckpointRetentionReason.None,
            lastStartedAtUtc: UtcNow,
            lastSuccessfulAtUtc: UtcNow,
            lastFailedAtUtc: null,
            lastElapsed: null,
            activeCount: 0,
            attemptCount: 1,
            successCount: 1,
            failureCount: 0,
            canceledCount: 0,
            lastError: null));
    }

    [Fact]
    public void Recovery_IncompleteTailCountsPartialCandidateAsScannedAndDiscarded()
    {
        WalRecoveryDiagnosticsSnapshot recovery = new(
            Metadata(),
            Id('c'),
            WalRecoveryPhase.Completed,
            UtcNow.AddSeconds(-1),
            UtcNow,
            TimeSpan.FromSeconds(1),
            CSharpDbOperationOutcome.Succeeded,
            scannedFrameCount: 1,
            scannedBytes: 17,
            recoveredFrameCount: 0,
            recoveredBytes: 0,
            discardedFrameCount: 1,
            discardedBytes: 17,
            WalRecoveryTruncationReason.IncompleteTail,
            attemptCount: 1,
            retryCount: 0,
            lastRetryError: null,
            error: null);

        Assert.Equal(recovery.ScannedFrameCount, recovery.DiscardedFrameCount);
        Assert.Equal(recovery.ScannedBytes, recovery.DiscardedBytes);
    }

    [Fact]
    public void Recovery_EarlyFailureCanDiscardUnscannedSuffixButCannotRecoverIt()
    {
        WalRecoveryDiagnosticsSnapshot accepted = new(
            Metadata(),
            Id('d'),
            WalRecoveryPhase.Completed,
            UtcNow.AddSeconds(-1),
            UtcNow,
            TimeSpan.FromSeconds(1),
            CSharpDbOperationOutcome.Succeeded,
            scannedFrameCount: 1,
            scannedBytes: 32,
            recoveredFrameCount: 0,
            recoveredBytes: 0,
            discardedFrameCount: 9,
            discardedBytes: 4_096,
            WalRecoveryTruncationReason.ChecksumMismatch,
            attemptCount: 1,
            retryCount: 0,
            lastRetryError: null,
            error: null);

        Assert.Equal(9, accepted.DiscardedFrameCount);
        Assert.Equal(4_096, accepted.DiscardedBytes);
        Assert.Throws<ArgumentException>(() => new WalRecoveryDiagnosticsSnapshot(
            Metadata(),
            Id('e'),
            WalRecoveryPhase.Completed,
            UtcNow.AddSeconds(-1),
            UtcNow,
            TimeSpan.FromSeconds(1),
            CSharpDbOperationOutcome.Succeeded,
            scannedFrameCount: 1,
            scannedBytes: 32,
            recoveredFrameCount: 2,
            recoveredBytes: 64,
            discardedFrameCount: 0,
            discardedBytes: 0,
            WalRecoveryTruncationReason.None,
            attemptCount: 1,
            retryCount: 0,
            lastRetryError: null,
            error: null));
    }

    [Fact]
    public void Checkpoint_ZeroAttemptIdleShapeRetainsNoHistory()
    {
        CheckpointDiagnosticsSnapshot checkpoint = new(
            Metadata(),
            operationId: null,
            CheckpointPhase.Idle,
            CheckpointOrigin.Unknown,
            startedAtUtc: null,
            elapsed: null,
            completedPageCount: null,
            totalPageCount: null,
            CheckpointRetentionReason.None,
            lastStartedAtUtc: null,
            lastSuccessfulAtUtc: null,
            lastFailedAtUtc: null,
            lastElapsed: null,
            activeCount: 0,
            attemptCount: 0,
            successCount: 0,
            failureCount: 0,
            canceledCount: 0,
            lastError: null);

        Assert.Equal(CheckpointPhase.Idle, checkpoint.Phase);
        Assert.Null(checkpoint.LastElapsed);
    }

    [Fact]
    public void Checkpoint_MultipleActiveUsesOneDisclosedRepresentative()
    {
        Assert.Throws<ArgumentException>(() => CreateConcurrentCheckpoint(
            Metadata()));

        DiagnosticsSnapshotMetadata truncated = Metadata(fieldsTruncated: true);
        CheckpointDiagnosticsSnapshot checkpoint = CreateConcurrentCheckpoint(
            truncated);

        Assert.Equal(2, checkpoint.ActiveCount);
        Assert.NotNull(checkpoint.OperationId);
        Assert.True(checkpoint.Metadata.FieldsTruncated);
    }

    [Fact]
    public void Checkpoint_ActivePhaseRetainsAConcurrentTerminalFailure()
    {
        DiagnosticsSnapshotMetadata metadata = Metadata();
        CheckpointDiagnosticsSnapshot checkpoint = CreateMixedCheckpoint(metadata);
        WalRuntimeDiagnosticsSnapshot wal = Wal(metadata, checkpoint.Phase) with
        {
            Checkpoint = DiagnosticsSection<CheckpointDiagnosticsSnapshot>.Available(
                checkpoint),
        };

        Assert.Equal(CheckpointPhase.Copying, checkpoint.Phase);
        Assert.Equal(1, checkpoint.ActiveCount);
        Assert.Equal(1, checkpoint.FailureCount);
        Assert.NotNull(checkpoint.LastError);
        Assert.Equal(checkpoint.Phase, wal.CheckpointPhase);
    }

    [Fact]
    public void Wal_AvailableDetailsRequireExactParentMetadata()
    {
        DiagnosticsSnapshotMetadata metadata = Metadata();
        DiagnosticsSnapshotMetadata other = Metadata(databaseAlias: "other");

        Assert.Throws<ArgumentException>(() => _ = Wal(other) with
        {
            Recovery = DiagnosticsSection<WalRecoveryDiagnosticsSnapshot>.Available(
                Recovery(metadata)),
        });

        WalRuntimeDiagnosticsSnapshot withDetails = Wal(
            metadata,
            CheckpointPhase.CopyCompleteAwaitingReaders) with
        {
            Recovery = DiagnosticsSection<WalRecoveryDiagnosticsSnapshot>.Available(
                Recovery(metadata)),
            Checkpoint = DiagnosticsSection<CheckpointDiagnosticsSnapshot>.Available(
                Checkpoint(metadata)),
        };
        Assert.Throws<ArgumentException>(() => _ = withDetails with
        {
            Metadata = other,
        });
        Assert.Throws<ArgumentException>(() => _ = withDetails with
        {
            CheckpointPhase = CheckpointPhase.Finalizing,
        });
    }

    [Fact]
    public void JsonContext_ContainsEveryNewClosedClientContract()
    {
        Type[] types =
        [
            typeof(StorageCacheDiagnosticsSnapshot),
            typeof(StorageDeviceIoDiagnosticsSnapshot),
            typeof(DiagnosticsSection<StorageCacheDiagnosticsSnapshot>),
            typeof(DiagnosticsSection<StorageDeviceIoDiagnosticsSnapshot>),
            typeof(WalRecoveryDiagnosticsSnapshot),
            typeof(CheckpointDiagnosticsSnapshot),
            typeof(DiagnosticsSection<WalRecoveryDiagnosticsSnapshot>),
            typeof(DiagnosticsSection<CheckpointDiagnosticsSnapshot>),
            typeof(DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot>),
            typeof(DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot>),
            typeof(DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot>>),
            typeof(DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot>>),
            typeof(DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>),
            typeof(RuntimeDiagnosticsFamilySection<DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>),
            typeof(ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>),
        ];

        foreach (Type type in types)
            Assert.NotNull(CSharpDbObservabilityJsonContext.Default.GetTypeInfo(type));
    }

    [Fact]
    public async Task NewOptionalCapabilityMembers_DefaultToOneSafeUnsupportedFailure()
    {
        ICSharpDbObservabilityClient legacy = new LegacyObservabilityCapability();

        Task[] calls =
        [
            AssertUnsupportedAsync(() => legacy.GetStorageDiagnosticsAsync()),
            AssertUnsupportedAsync(() => legacy.GetWalDiagnosticsAsync()),
            AssertUnsupportedAsync(() =>
                legacy.GetActiveMaintenanceOperationsAsync(10)),
            AssertUnsupportedAsync(() =>
                legacy.GetRecentMaintenanceOperationsAsync(10)),
        ];
        await Task.WhenAll(calls);
    }

    private static async Task AssertUnsupportedAsync(Func<Task> action)
    {
        CSharpDbObservabilityNotSupportedException exception =
            await Assert.ThrowsAsync<CSharpDbObservabilityNotSupportedException>(action);
        Assert.Equal(CSharpDbObservabilityNotSupportedException.SafeMessage, exception.Message);
        Assert.Null(exception.InnerException);
    }

    private static DiagnosticsSnapshotMetadata Metadata(
        string databaseAlias = "primary",
        bool fieldsTruncated = false)
        => new(
            CSharpDbDiagnostics.SchemaVersion,
            UtcNow,
            "0123456789abcdef0123456789abcdef",
            counterEpoch: 2,
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine,
            databaseAlias,
            recordsTruncated: false,
            fieldsTruncated);

    private static OpaqueDiagnosticsId Id(char digit)
        => new(new string(digit, 32));

    private static StorageRuntimeDiagnosticsSnapshot Storage(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            LogicalDatabaseBytes: 4_096,
            AllocatedDatabaseBytes: 8_192,
            PageCount: 2,
            PageReads: 5,
            PageWrites: 3,
            BytesRead: 20_480,
            BytesWritten: 12_288,
            CacheHits: 3,
            CacheMisses: 2,
            DirtyPages: 1,
            ActiveReaders: 1,
            ActiveWriters: 0,
            CommitCount: 4,
            ConflictCount: 1);

    private static StorageRuntimeDiagnosticsSnapshot StorageWithDetails(
        DiagnosticsSnapshotMetadata metadata)
        => Storage(metadata) with
        {
            Cache = DiagnosticsSection<StorageCacheDiagnosticsSnapshot>.Available(
                StorageCache(metadata)),
            PhysicalIo = DiagnosticsSection<StorageDeviceIoDiagnosticsSnapshot>
                .Available(StoragePhysicalIo(metadata)),
        };

    private static StorageCacheDiagnosticsSnapshot StorageCache(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            sharedResidentPages: 7,
            sharedCapacityPages: 16,
            walResidentPages: 2,
            walCapacityPages: 8);

    private static StorageDeviceIoDiagnosticsSnapshot StoragePhysicalIo(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            readCount: 9,
            bytesRead: 36_864,
            writeCount: 3,
            bytesWritten: 12_288,
            flushCount: 2,
            resizeCount: 1,
            sequentialReadCount: 4,
            sequentialBytesRead: 16_384,
            memoryMappedPageExposureCount: 2,
            memoryMappedBytesExposed: 8_192);

    private static WalRuntimeDiagnosticsSnapshot Wal(
        DiagnosticsSnapshotMetadata metadata,
        CheckpointPhase checkpointPhase = CheckpointPhase.Idle)
        => new(
            metadata,
            LogicalBytes: 10,
            AllocatedBytes: 12,
            CommittedFrameBytes: 8,
            RetainedBytes: 2,
            FrameCount: 2,
            FlushCount: 3,
            BytesWritten: 24,
            PendingCommitCount: 1,
            checkpointPhase,
            LastSuccessfulFlushAtUtc: UtcNow,
            LastSuccessfulCheckpointAtUtc: UtcNow,
            LastError: null)
        {
            FlushedCommitCount = 5,
            DurableFlushCount = 2,
            LastSuccessfulDurableFlushAtUtc = UtcNow,
            GroupCommitBatchCount = 2,
            GroupCommitCount = 4,
            LastSuccessfulGroupCommitAtUtc = UtcNow,
        };

    private static WalRecoveryDiagnosticsSnapshot Recovery(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            Id('a'),
            WalRecoveryPhase.Completed,
            UtcNow.AddSeconds(-2),
            UtcNow,
            TimeSpan.FromSeconds(2),
            CSharpDbOperationOutcome.Succeeded,
            scannedFrameCount: 4,
            scannedBytes: 400,
            recoveredFrameCount: 3,
            recoveredBytes: 300,
            discardedFrameCount: 1,
            discardedBytes: 100,
            WalRecoveryTruncationReason.UncommittedTail,
            attemptCount: 2,
            retryCount: 1,
            lastRetryError: SafeErrorProjector.Project(SafeErrorKind.DatabaseIo),
            error: null);

    private static CheckpointDiagnosticsSnapshot Checkpoint(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            Id('b'),
            CheckpointPhase.CopyCompleteAwaitingReaders,
            CheckpointOrigin.BackgroundAuto,
            UtcNow.AddSeconds(-1),
            TimeSpan.FromSeconds(1),
            completedPageCount: 3,
            totalPageCount: 4,
            CheckpointRetentionReason.ActiveReadersAndNewerCommits,
            lastStartedAtUtc: UtcNow.AddSeconds(-1),
            lastSuccessfulAtUtc: UtcNow.AddMinutes(-1),
            lastFailedAtUtc: null,
            lastElapsed: TimeSpan.FromSeconds(1),
            activeCount: 1,
            attemptCount: 2,
            successCount: 1,
            failureCount: 0,
            canceledCount: 0,
            lastError: null);

    private static CheckpointDiagnosticsSnapshot CreateConcurrentCheckpoint(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            Id('d'),
            CheckpointPhase.Copying,
            CheckpointOrigin.BackgroundAuto,
            UtcNow.AddSeconds(-1),
            TimeSpan.FromSeconds(1),
            completedPageCount: 1,
            totalPageCount: 2,
            CheckpointRetentionReason.None,
            lastStartedAtUtc: UtcNow.AddSeconds(-1),
            lastSuccessfulAtUtc: null,
            lastFailedAtUtc: null,
            lastElapsed: TimeSpan.FromSeconds(1),
            activeCount: 2,
            attemptCount: 2,
            successCount: 0,
            failureCount: 0,
            canceledCount: 0,
            lastError: null);

    private static CheckpointDiagnosticsSnapshot CreateMixedCheckpoint(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            Id('e'),
            CheckpointPhase.Copying,
            CheckpointOrigin.BackgroundAuto,
            UtcNow.AddSeconds(-1),
            TimeSpan.FromSeconds(1),
            completedPageCount: 1,
            totalPageCount: 2,
            CheckpointRetentionReason.None,
            lastStartedAtUtc: UtcNow.AddSeconds(-1),
            lastSuccessfulAtUtc: null,
            lastFailedAtUtc: UtcNow.AddSeconds(-2),
            lastElapsed: TimeSpan.FromSeconds(1),
            activeCount: 1,
            attemptCount: 2,
            successCount: 0,
            failureCount: 1,
            canceledCount: 0,
            lastError: SafeErrorProjector.Project(SafeErrorKind.DatabaseIo));

    private sealed class LegacyObservabilityCapability : ICSharpDbObservabilityClient
    {
        public Task<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>
            GetRuntimeDiagnosticsAsync(CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>
            GetActiveQueriesAsync(
                int maximumRecords,
                CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>>
            GetRecentQueriesAsync(
                int maximumRecords,
                CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>>
            GetQueryPlanDiagnosticsAsync(
                OpaqueDiagnosticsId operationId,
                CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>>
            GetSessionsAsync(
                int maximumRecords,
                CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>>
            GetQueryDetailAsync(
                OpaqueDiagnosticsId operationId,
                CancellationToken ct = default)
            => throw new NotSupportedException();
    }
}
