using System.Text.Json;
using CSharpDB.Observability;

namespace CSharpDB.Observability.Tests;

public sealed class RuntimeDiagnosticsContractTests
{
    [Fact]
    public void PhaseTwoEnums_HaveStableUnknownZeroAndPublishedValues()
    {
        Assert.Equal(0, (int)ConnectionPoolLifecycleState.Unknown);
        Assert.Equal(5, (int)ConnectionPoolLifecycleState.Retired);
        Assert.Equal(0, (int)DiagnosticsSessionState.Unknown);
        Assert.Equal(6, (int)DiagnosticsSessionState.Disposed);
    }

    [Fact]
    public void CollectionEnvelope_RepresentsEmptyAvailableAndUnavailableResultsTruthfully()
    {
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero));
        var state = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions { Enabled = true, DatabaseAlias = "primary" },
            clock);
        var disabledState = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions { Enabled = false, DatabaseAlias = "primary" },
            clock);

        DiagnosticsSnapshotMetadata availableMetadata = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine);
        var mutableRecords = new List<ActiveQuerySnapshot>();
        var available = new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            availableMetadata,
            mutableRecords,
            capacity: 1_000,
            retention: null,
            droppedCount: 0,
            isTruncated: false);
        mutableRecords.Add(new ActiveQuerySnapshot(
            availableMetadata,
            OpaqueDiagnosticsId.Create(),
            ParentOperationId: null,
            CSharpDbOperationClass.Query,
            CSharpDbOperationRole.Root,
            QueryExecutionPhase.Executing,
            clock.GetUtcNow(),
            TimeSpan.Zero,
            Fingerprint: null,
            CSharpDbTransport.Direct,
            TraceId: null,
            SessionId: null));

        DiagnosticsSnapshotMetadata disabledMetadata = disabledState.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Disabled,
            DiagnosticsSource.Engine);
        var disabled = new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            disabledMetadata,
            records: null,
            capacity: null,
            retention: null,
            droppedCount: null,
            isTruncated: null);

        Assert.Empty(Assert.IsAssignableFrom<IReadOnlyList<ActiveQuerySnapshot>>(available.Records));
        Assert.Equal(1_000, available.Capacity);
        Assert.Equal(DiagnosticsAvailability.Available, available.Metadata.Availability);
        Assert.Null(disabled.Records);
        Assert.Null(disabled.Capacity);
        Assert.Null(disabled.DroppedCount);
        Assert.Null(disabled.IsTruncated);
        Assert.Equal(DiagnosticsAvailability.Disabled, disabled.Metadata.Availability);
        Assert.Equal(disabledState.ServerInstanceId, disabled.Metadata.ServerInstanceId);
    }

    [Fact]
    public void ValueEnvelope_RepresentsAvailableAndUnavailableResultsWithoutFabrication()
    {
        var state = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions { Enabled = true, DatabaseAlias = "primary" });
        DiagnosticsSnapshotMetadata availableMetadata = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine);
        var plan = new QueryPlanDiagnosticsSnapshot(
            availableMetadata,
            OpaqueDiagnosticsId.Create(),
            Fingerprint: null,
            QueryAccessPathCategory.PrimaryKeyLookup,
            PlanCacheHit: true,
            Reoptimized: false,
            EstimatedRows: 1,
            ActualRows: 1,
            PlanNodeCount: null,
            PlanTruncated: false);
        var available = new DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>(
            availableMetadata,
            plan);

        DiagnosticsSnapshotMetadata unavailableMetadata = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Unavailable,
            DiagnosticsSource.Engine);
        var unavailable = new DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>(
            unavailableMetadata,
            value: null);

        Assert.Same(plan, available.Value);
        Assert.Null(unavailable.Value);

        var typeInfo = CSharpDbObservabilityJsonContext.Default
            .DiagnosticsValueSnapshotQueryPlanDiagnosticsSnapshot;
        string json = JsonSerializer.Serialize(unavailable, typeInfo);
        Assert.DoesNotContain("\"value\":", json, StringComparison.Ordinal);
        var roundTrip = JsonSerializer.Deserialize(json, typeInfo);
        Assert.NotNull(roundTrip);
        Assert.Equal(DiagnosticsAvailability.Unavailable, roundTrip.Metadata.Availability);
        Assert.Null(roundTrip.Value);
    }

    [Fact]
    public void ValueEnvelope_RejectsAvailabilityIdentityAndTruncationContradictions()
    {
        var state = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions { Enabled = true, DatabaseAlias = "primary" });
        var otherState = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions { Enabled = true, DatabaseAlias = "other" });
        DiagnosticsSnapshotMetadata availableMetadata = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine);
        DiagnosticsSnapshotMetadata otherMetadata = otherState.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine);
        var otherPlan = new QueryPlanDiagnosticsSnapshot(
            otherMetadata,
            OpaqueDiagnosticsId.Create(),
            Fingerprint: null,
            QueryAccessPathCategory.Unknown,
            PlanCacheHit: null,
            Reoptimized: null,
            EstimatedRows: null,
            ActualRows: null,
            PlanNodeCount: null,
            PlanTruncated: false);

        Assert.Throws<ArgumentException>(() =>
            new DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>(
                availableMetadata,
                value: null));
        Assert.Throws<ArgumentException>(() =>
            new DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>(
                availableMetadata,
                otherPlan));

        DiagnosticsSnapshotMetadata truncated = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Unavailable,
            DiagnosticsSource.Engine,
            recordsTruncated: true);
        Assert.Throws<ArgumentException>(() =>
            new DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>(
                truncated,
                value: null));
    }

    [Fact]
    public void CollectionEnvelope_ValidatesCapacityRetentionAvailabilityAndTruncation()
    {
        var state = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions { Enabled = true, DatabaseAlias = "primary" });
        DiagnosticsSnapshotMetadata available = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine);
        DiagnosticsSnapshotMetadata truncated = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine,
            recordsTruncated: true);
        DiagnosticsSnapshotMetadata disabled = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Disabled,
            DiagnosticsSource.Engine);

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new DiagnosticsCollectionSnapshot<RecentQuerySnapshot>(
                available, [], 0, TimeSpan.FromMinutes(1), 0, false));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new DiagnosticsCollectionSnapshot<RecentQuerySnapshot>(
                available, [], 1, TimeSpan.Zero, 0, false));
        Assert.Throws<ArgumentException>(() =>
            new DiagnosticsCollectionSnapshot<RecentQuerySnapshot>(
                available, [], 1, TimeSpan.FromMinutes(1), 1, true));
        Assert.Throws<ArgumentException>(() =>
            new DiagnosticsCollectionSnapshot<RecentQuerySnapshot>(
                truncated, [], 1, TimeSpan.FromMinutes(1), 1, false));
        Assert.Throws<ArgumentException>(() =>
            new DiagnosticsCollectionSnapshot<RecentQuerySnapshot>(
                disabled, [], 1, TimeSpan.FromMinutes(1), 0, false));

        var otherState = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions { Enabled = true, DatabaseAlias = "other" });
        DiagnosticsSnapshotMetadata otherMetadata = otherState.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine);
        var mixedRecord = new RecentQuerySnapshot(
            otherMetadata,
            OpaqueDiagnosticsId.Create(),
            ParentOperationId: null,
            CSharpDbOperationClass.Query,
            CSharpDbOperationRole.Root,
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow,
            TimeSpan.Zero,
            TimeToFirstResult: null,
            ResultConsumptionDuration: null,
            CSharpDbOperationOutcome.Succeeded,
            Fingerprint: null,
            CSharpDbTransport.Direct,
            RowsProduced: 0,
            RowsAffected: 0,
            TraceId: null,
            SessionId: null,
            Error: null);
        Assert.Throws<ArgumentException>(() =>
            new DiagnosticsCollectionSnapshot<RecentQuerySnapshot>(
                available, [mixedRecord], 1, TimeSpan.FromMinutes(1), 0, false));

        DiagnosticsSnapshotMetadata disabledTruncated = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Disabled,
            DiagnosticsSource.Engine,
            recordsTruncated: true);
        Assert.Throws<ArgumentException>(() =>
            new DiagnosticsCollectionSnapshot<RecentQuerySnapshot>(
                disabledTruncated, null, null, null, null, null));
    }

    [Fact]
    public void PerShardEnvelope_PreservesRemoteIdentityAndRepresentsUnsupportedShard()
    {
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero));
        var shardState = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions { Enabled = true, DatabaseAlias = "shard-1" },
            clock);
        DiagnosticsSnapshotMetadata metadata = shardState.CreateMetadata(
            DiagnosticsScope.Shard,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine);
        var collection = new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            metadata,
            [],
            capacity: 10,
            retention: null,
            droppedCount: 0,
            isTruncated: false);

        var available = new ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
            "shard-1",
            DiagnosticsAvailability.Available,
            collection);
        var unsupported = new ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
            "shard-2",
            DiagnosticsAvailability.Unsupported,
            value: null);

        Assert.Equal(shardState.ServerInstanceId, available.Value?.Metadata.ServerInstanceId);
        Assert.Null(unsupported.Value);
        var shardTypeInfo = CSharpDbObservabilityJsonContext.Default.GetTypeInfo(unsupported.GetType())!;
        string unsupportedJson = JsonSerializer.Serialize(unsupported, shardTypeInfo);
        Assert.DoesNotContain("\"value\":", unsupportedJson, StringComparison.Ordinal);
        var unsupportedRoundTrip = Assert.IsType<
            ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>(
            JsonSerializer.Deserialize(unsupportedJson, shardTypeInfo));
        Assert.Equal(DiagnosticsAvailability.Unsupported, unsupportedRoundTrip.Availability);
        Assert.Null(unsupportedRoundTrip.Value);
        Assert.Throws<ArgumentException>(() =>
            new ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
                "shard-2",
                DiagnosticsAvailability.Available,
                collection));
    }

    [Fact]
    public void RuntimeState_OwnsStableIdentityEpochClockAndImmutableOptions()
    {
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero));
        var options = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "primary",
            LongRunningQueryThreshold = TimeSpan.FromSeconds(7),
            History = new CSharpDbHistoryOptions
            {
                ActiveQueryCapacity = 17,
                RecentQueryCapacity = 19,
                RecentOperationCapacity = 23,
                Retention = TimeSpan.FromMinutes(3),
            },
        };
        var state = new CSharpDbRuntimeDiagnosticsState(options, clock);
        var independent = new CSharpDbRuntimeDiagnosticsState(options, clock);

        options.DatabaseAlias = "mutated";
        options.History.ActiveQueryCapacity = 1;
        CSharpDbObservabilityOptions copy = state.CreateOptionsSnapshot();
        copy.DatabaseAlias = "copy-mutated";

        DiagnosticsSnapshotMetadata first = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine);
        clock.Advance(TimeSpan.FromSeconds(1));
        state.AdvanceCounterEpoch();
        DiagnosticsSnapshotMetadata second = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine);

        Assert.Equal("primary", state.DatabaseAlias);
        Assert.Equal(17, state.ActiveQueryCapacity);
        Assert.Equal(19, state.RecentQueryCapacity);
        Assert.Equal(TimeSpan.FromMinutes(3), state.RecentQueryRetention);
        Assert.Equal(TimeSpan.FromSeconds(7), state.LongRunningQueryThreshold);
        Assert.Equal("primary", state.CreateOptionsSnapshot().DatabaseAlias);
        Assert.Equal(first.ServerInstanceId, second.ServerInstanceId);
        Assert.NotEqual(first.ServerInstanceId, independent.ServerInstanceId);
        Assert.Equal(0, first.CounterEpoch);
        Assert.Equal(1, second.CounterEpoch);
        Assert.Equal(clock.GetUtcNow(), second.CapturedAtUtc);
    }

    [Fact]
    public void DisabledRuntimeState_RetainsIdentityAcrossRepeatedCaptures()
    {
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero));
        var state = new CSharpDbRuntimeDiagnosticsState(options: null, timeProvider: clock);

        DiagnosticsSnapshotMetadata first = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Disabled,
            DiagnosticsSource.Client);
        clock.Advance(TimeSpan.FromMinutes(1));
        DiagnosticsSnapshotMetadata second = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Disabled,
            DiagnosticsSource.Client);
        var restarted = new CSharpDbRuntimeDiagnosticsState(options: null, timeProvider: clock);

        Assert.False(state.IsEnabled);
        Assert.Equal("default", state.DatabaseAlias);
        Assert.Equal(first.ServerInstanceId, second.ServerInstanceId);
        Assert.Equal(first.CounterEpoch, second.CounterEpoch);
        Assert.NotEqual(first.CapturedAtUtc, second.CapturedAtUtc);
        Assert.NotEqual(first.ServerInstanceId, restarted.ServerInstanceId);
    }

    [Fact]
    public void ReplacementDatabaseState_RetainsHostIdentityButReplacesAliasAndOptions()
    {
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero));
        var primary = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions { Enabled = true, DatabaseAlias = "primary" },
            clock);
        var secondary = primary.CreateForOptions(
            new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = "secondary",
                LongRunningQueryThreshold = TimeSpan.FromSeconds(11),
            });

        primary.CompleteCounterFamilyOpen(replacesExistingFamily: false);
        primary.AdvanceCounterEpoch();
        Assert.Equal(0, secondary.CounterEpoch);
        secondary.CompleteCounterFamilyOpen(replacesExistingFamily: true);
        DiagnosticsSnapshotMetadata primaryMetadata = primary.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine);
        DiagnosticsSnapshotMetadata secondaryMetadata = secondary.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine);

        Assert.Equal(primary.ServerInstanceId, secondary.ServerInstanceId);
        Assert.Equal(1, primaryMetadata.CounterEpoch);
        Assert.Equal(2, secondaryMetadata.CounterEpoch);
        Assert.Equal("primary", primary.DatabaseAlias);
        Assert.Equal("secondary", secondary.DatabaseAlias);
        Assert.Equal(TimeSpan.FromSeconds(11), secondary.LongRunningQueryThreshold);
    }

    [Fact]
    public void NewCollectionAndShardContracts_RoundTripWithSourceGeneration()
    {
        var state = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions { Enabled = true, DatabaseAlias = "shard-1" });
        DiagnosticsSnapshotMetadata metadata = state.CreateMetadata(
            DiagnosticsScope.Shard,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine);
        var collection = new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            metadata,
            [],
            capacity: 3,
            retention: null,
            droppedCount: 0,
            isTruncated: false);
        var shard = new ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
            "shard-1",
            DiagnosticsAvailability.Available,
            collection);
        var typeInfo = CSharpDbObservabilityJsonContext.Default.GetTypeInfo(shard.GetType())!;

        string json = JsonSerializer.Serialize(shard, typeInfo);
        object? roundTrip = JsonSerializer.Deserialize(json, typeInfo);

        var typed = Assert.IsType<ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>(roundTrip);
        Assert.Equal("shard-1", typed.ShardAlias);
        Assert.Equal(state.ServerInstanceId, typed.Value?.Metadata.ServerInstanceId);
        Assert.Equal(3, typed.Value?.Capacity);

        DiagnosticsSnapshotMetadata unsupportedMetadata = state.CreateMetadata(
            DiagnosticsScope.Shard,
            DiagnosticsAvailability.Unsupported,
            DiagnosticsSource.Engine);
        var unavailableCollection = new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            unsupportedMetadata,
            records: null,
            capacity: null,
            retention: null,
            droppedCount: null,
            isTruncated: null);
        var unavailableTypeInfo = CSharpDbObservabilityJsonContext.Default.GetTypeInfo(
            typeof(DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>))!;
        string unavailableJson = JsonSerializer.Serialize(unavailableCollection, unavailableTypeInfo);
        Assert.DoesNotContain("\"records\":", unavailableJson, StringComparison.Ordinal);
        Assert.DoesNotContain("\"capacity\":", unavailableJson, StringComparison.Ordinal);
        Assert.DoesNotContain("\"droppedCount\":", unavailableJson, StringComparison.Ordinal);
        Assert.DoesNotContain("\"isTruncated\":", unavailableJson, StringComparison.Ordinal);
        var unavailableRoundTrip = Assert.IsType<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
            JsonSerializer.Deserialize(unavailableJson, unavailableTypeInfo));
        Assert.Equal(DiagnosticsAvailability.Unsupported, unavailableRoundTrip.Metadata.Availability);
        Assert.Null(unavailableRoundTrip.Records);
    }

    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private DateTimeOffset _utcNow = utcNow;
        private long _timestamp;

        public override DateTimeOffset GetUtcNow() => _utcNow;
        public override long GetTimestamp() => _timestamp;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public void Advance(TimeSpan elapsed)
        {
            _utcNow += elapsed;
            _timestamp += elapsed.Ticks;
        }
    }
}
