using System.Text.Json;
using CSharpDB.Observability;

namespace CSharpDB.Observability.Tests;

public sealed class DiagnosticsTopologyContractTests
{
    [Fact]
    public void InstanceTopology_OmitsEveryShardFieldAndRoundTripsWithSourceGeneration()
    {
        var state = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = "primary",
            });
        DiagnosticsSnapshotMetadata metadata = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine);
        var collection = new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            metadata,
            records: [],
            capacity: 10,
            retention: null,
            droppedCount: 0,
            isTruncated: false);
        var topology = new DiagnosticsTopologySnapshot<
            DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
            collection,
            shards: null,
            shardCapacity: null,
            droppedShardCount: null,
            shardsTruncated: null);
        var typeInfo = CSharpDbObservabilityJsonContext.Default.GetTypeInfo(
            topology.GetType())!;

        string json = JsonSerializer.Serialize(topology, typeInfo);
        object? deserialized = JsonSerializer.Deserialize(json, typeInfo);

        Assert.DoesNotContain("\"shards\":", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"shardCapacity\":", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"droppedShardCount\":", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"shardsTruncated\":", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"runtimeFamilies\":", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"runtimeFamilyCapacity\":", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"droppedRuntimeFamilyCount\":", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"runtimeFamiliesTruncated\":", json, StringComparison.Ordinal);
        var roundTrip = Assert.IsType<DiagnosticsTopologySnapshot<
            DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>(deserialized);
        Assert.Equal(metadata, roundTrip.Metadata);
        Assert.Same(roundTrip.Aggregate.Metadata, roundTrip.Metadata);
        Assert.Null(roundTrip.Shards);
    }

    [Fact]
    public void AggregateTopology_BoundsAliasesAndPreservesEachShardIdentity()
    {
        var aggregateState = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = "cluster",
            });
        var shardState = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = "shard-1",
            });
        DiagnosticsSnapshotMetadata aggregateMetadata = aggregateState.CreateMetadata(
            DiagnosticsScope.Aggregate,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Client);
        DiagnosticsSnapshotMetadata shardMetadata = shardState.CreateMetadata(
            DiagnosticsScope.Shard,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine);
        var aggregateCollection = new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            aggregateMetadata,
            records: [],
            capacity: 20,
            retention: null,
            droppedCount: 0,
            isTruncated: false);
        var shardCollection = new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            shardMetadata,
            records: [],
            capacity: 10,
            retention: null,
            droppedCount: 0,
            isTruncated: false);
        var availableShard = new ShardDiagnosticsSection<
            DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
            "shard-1",
            DiagnosticsAvailability.Available,
            shardCollection);
        var unsupportedShard = new ShardDiagnosticsSection<
            DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
            "shard-2",
            DiagnosticsAvailability.Unsupported,
            value: null);

        var topology = new DiagnosticsTopologySnapshot<
            DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
            aggregateCollection,
            [availableShard, unsupportedShard],
            shardCapacity: 2,
            droppedShardCount: 0,
            shardsTruncated: false);

        Assert.Equal(aggregateState.ServerInstanceId, topology.Metadata.ServerInstanceId);
        Assert.Equal(
            shardState.ServerInstanceId,
            Assert.Single(topology.Shards!, static shard => shard.Value is not null)
                .Value!.Metadata.ServerInstanceId);
        Assert.NotEqual(
            topology.Metadata.ServerInstanceId,
            topology.Shards![0].Value!.Metadata.ServerInstanceId);

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
                aggregateCollection,
                [],
                shardCapacity: CSharpDbDiagnostics.MaximumConfiguredDatabaseAliases + 1,
                droppedShardCount: 0,
                shardsTruncated: false));
        Assert.Throws<ArgumentException>(() =>
            new DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
                aggregateCollection,
                [availableShard, availableShard],
                shardCapacity: 2,
                droppedShardCount: 0,
                shardsTruncated: false));
        Assert.Throws<ArgumentException>(() =>
            new DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
                aggregateCollection,
                [],
                shardCapacity: 1,
                droppedShardCount: 1,
                shardsTruncated: false));
        Assert.Throws<ArgumentException>(() =>
            new DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
                aggregateCollection,
                [availableShard, unsupportedShard],
                shardCapacity: 1,
                droppedShardCount: 0,
                shardsTruncated: false));
    }

    [Fact]
    public void Topology_RejectsShardScopeAsAPrimaryResponse()
    {
        var state = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = "shard-1",
            });
        DiagnosticsSnapshotMetadata metadata = state.CreateMetadata(
            DiagnosticsScope.Shard,
            DiagnosticsAvailability.Unavailable,
            DiagnosticsSource.Engine);
        var value = new DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>(
            metadata,
            value: null);

        Assert.Throws<ArgumentException>(() =>
            new DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>(
                value,
                shards: null,
                shardCapacity: null,
                droppedShardCount: null,
                shardsTruncated: null));
    }

    [Fact]
    public void ConnectionSnapshot_ExclusiveMaintenanceStateRoundTripsWithSourceGeneration()
    {
        var state = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = "primary",
            });
        DiagnosticsSnapshotMetadata metadata = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Client);
        var snapshot = new ConnectionDiagnosticsSnapshot(
            metadata,
            PoolCapacity: null,
            AvailableSlots: null,
            WaiterCount: null,
            ActiveLogicalSessions: 2,
            ActiveReaders: 1,
            ActiveTransactions: 1,
            RetiredPoolCount: null,
            PoisonedPoolCount: null,
            OldestTransactionAge: TimeSpan.FromSeconds(3))
        {
            PoolState = ConnectionPoolLifecycleState.Unknown,
            ExclusiveMaintenanceActive = true,
        };
        var typeInfo = CSharpDbObservabilityJsonContext.Default.ConnectionDiagnosticsSnapshot;

        string json = JsonSerializer.Serialize(snapshot, typeInfo);
        ConnectionDiagnosticsSnapshot roundTrip = Assert.IsType<ConnectionDiagnosticsSnapshot>(
            JsonSerializer.Deserialize(json, typeInfo));

        Assert.True(roundTrip.ExclusiveMaintenanceActive);
        Assert.Equal(metadata, roundTrip.Metadata);
    }

    [Fact]
    public void AggregateTopology_PreservesSameAliasRuntimeFamiliesWithDifferentEpochs()
    {
        var state = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = "primary",
            });
        DiagnosticsSnapshotMetadata firstMetadata = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine);
        state.AdvanceCounterEpoch();
        DiagnosticsSnapshotMetadata secondMetadata = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine);
        DiagnosticsSnapshotMetadata aggregateMetadata = state.CreateMetadata(
            DiagnosticsScope.Aggregate,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Client);
        var first = new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            firstMetadata, [], 10, retention: null, droppedCount: 0, isTruncated: false);
        var second = new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            secondMetadata, [], 10, retention: null, droppedCount: 0, isTruncated: false);
        var aggregate = new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            aggregateMetadata, [], 10, retention: null, droppedCount: 0, isTruncated: false);
        var firstFamily = new RuntimeDiagnosticsFamilySection<
            DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>("primary", first);
        var secondFamily = new RuntimeDiagnosticsFamilySection<
            DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>("primary", second);

        var topology = new DiagnosticsTopologySnapshot<
            DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
            aggregate,
            shards: null,
            shardCapacity: null,
            droppedShardCount: null,
            shardsTruncated: null,
            runtimeFamilies: [firstFamily, secondFamily],
            runtimeFamilyCapacity: 2,
            droppedRuntimeFamilyCount: 0,
            runtimeFamiliesTruncated: false);

        Assert.Equal([0L, 1L], topology.RuntimeFamilies!
            .Select(static family => family.Value.Metadata.CounterEpoch)
            .ToArray());
        Assert.All(topology.RuntimeFamilies!, static family =>
            Assert.Equal("primary", family.DatabaseAlias));
        Assert.Throws<ArgumentException>(() =>
            new DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
                aggregate,
                shards: null,
                shardCapacity: null,
                droppedShardCount: null,
                shardsTruncated: null,
                runtimeFamilies: [firstFamily, firstFamily],
                runtimeFamilyCapacity: 2,
                droppedRuntimeFamilyCount: 0,
                runtimeFamiliesTruncated: false));
    }

    [Fact]
    public void RuntimeFamily_PreservesUnavailableValueEnvelopeIdentityAndRejectsDrift()
    {
        var state = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = "primary",
            });
        DiagnosticsSnapshotMetadata unavailableMetadata = state.CreateMetadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Unavailable,
            DiagnosticsSource.Engine);
        var unavailable = new DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>(
            unavailableMetadata,
            value: null);
        var family = new RuntimeDiagnosticsFamilySection<
            DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>(
            "primary",
            unavailable);
        var typeInfo = CSharpDbObservabilityJsonContext.Default.GetTypeInfo(
            family.GetType())!;

        string json = JsonSerializer.Serialize(family, typeInfo);
        var roundTrip = Assert.IsType<RuntimeDiagnosticsFamilySection<
            DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>>(
            JsonSerializer.Deserialize(json, typeInfo));

        Assert.Equal(DiagnosticsAvailability.Unavailable, roundTrip.Value.Metadata.Availability);
        Assert.Equal(state.ServerInstanceId, roundTrip.Value.Metadata.ServerInstanceId);
        Assert.Null(roundTrip.Value.Value);
        Assert.Throws<ArgumentException>(() =>
            new RuntimeDiagnosticsFamilySection<
                DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>(
                "other",
                unavailable));

        DiagnosticsSnapshotMetadata shardMetadata = state.CreateMetadata(
            DiagnosticsScope.Shard,
            DiagnosticsAvailability.Unavailable,
            DiagnosticsSource.Engine);
        var shardValue = new DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>(
            shardMetadata,
            value: null);
        Assert.Throws<ArgumentException>(() =>
            new RuntimeDiagnosticsFamilySection<
                DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>(
                "primary",
                shardValue));
    }

    [Fact]
    public void ReachableShard_PreservesDisabledAndUnavailableTypedResponses()
    {
        var state = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = "shard-1",
            });
        DiagnosticsSnapshotMetadata disabledMetadata = state.CreateMetadata(
            DiagnosticsScope.Shard,
            DiagnosticsAvailability.Disabled,
            DiagnosticsSource.Engine);
        var disabledCollection = new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            disabledMetadata,
            records: null,
            capacity: null,
            retention: null,
            droppedCount: null,
            isTruncated: null);
        var reachableDisabled = new ShardDiagnosticsSection<
            DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
            "shard-1",
            DiagnosticsAvailability.Available,
            disabledCollection);

        DiagnosticsSnapshotMetadata unavailableMetadata = state.CreateMetadata(
            DiagnosticsScope.Shard,
            DiagnosticsAvailability.Unavailable,
            DiagnosticsSource.Engine);
        var unavailablePlan = new DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>(
            unavailableMetadata,
            value: null);
        var reachableUnavailable = new ShardDiagnosticsSection<
            DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>(
            "shard-1",
            DiagnosticsAvailability.Available,
            unavailablePlan);
        var typeInfo = CSharpDbObservabilityJsonContext.Default.GetTypeInfo(
            reachableUnavailable.GetType())!;

        string json = JsonSerializer.Serialize(reachableUnavailable, typeInfo);
        var roundTrip = Assert.IsType<ShardDiagnosticsSection<
            DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>>(
            JsonSerializer.Deserialize(json, typeInfo));

        Assert.Equal(DiagnosticsAvailability.Disabled, reachableDisabled.Value!.Metadata.Availability);
        Assert.Equal(DiagnosticsAvailability.Available, roundTrip.Availability);
        Assert.Equal(DiagnosticsAvailability.Unavailable, roundTrip.Value!.Metadata.Availability);
        Assert.Equal(state.ServerInstanceId, roundTrip.Value.Metadata.ServerInstanceId);
        Assert.Throws<ArgumentException>(() =>
            new ShardDiagnosticsSection<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>(
                "shard-1",
                DiagnosticsAvailability.Disabled,
                unavailablePlan));
    }
}
