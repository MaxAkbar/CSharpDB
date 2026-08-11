using System.Text.Json;
using System.Text.Json.Nodes;
using CSharpDB.Observability;

namespace CSharpDB.Observability.Tests;

public sealed class DiagnosticsTopologyAdversarialTests
{
    [Fact]
    public void UnavailableEnvelopes_OmitEveryBoundedAndTruncationField()
    {
        DiagnosticsSnapshotMetadata unavailable = Metadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Unavailable,
            "primary");
        DiagnosticsSnapshotMetadata fieldsTruncated = Metadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Unavailable,
            "primary",
            fieldsTruncated: true);

        _ = new DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>(
            unavailable,
            value: null);
        _ = new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            unavailable,
            records: null,
            capacity: null,
            retention: null,
            droppedCount: null,
            isTruncated: null);

        Assert.Throws<ArgumentException>(() =>
            new DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>(
                fieldsTruncated,
                value: null));
        Assert.Throws<ArgumentException>(() =>
            new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
                fieldsTruncated,
                records: null,
                capacity: null,
                retention: null,
                droppedCount: null,
                isTruncated: null));
    }

    [Fact]
    public void ViewTruncation_RemainsValidWithoutCumulativeRegistryDrops()
    {
        DiagnosticsSnapshotMetadata truncated = Metadata(
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            "primary",
            recordsTruncated: true);
        var snapshot = new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            truncated,
            records: [],
            capacity: 1,
            retention: null,
            droppedCount: 0,
            isTruncated: true);

        Assert.True(snapshot.IsTruncated);
        Assert.Equal(0, snapshot.DroppedCount);
    }

    [Fact]
    public void Topology_MaterializesPartitionInputsAndPreservesMixedIdentities()
    {
        DiagnosticsCollectionSnapshot<ActiveQuerySnapshot> aggregate = Collection(
            Metadata(DiagnosticsScope.Aggregate, DiagnosticsAvailability.Available, "cluster"));
        DiagnosticsCollectionSnapshot<ActiveQuerySnapshot> shardValue = Collection(
            Metadata(DiagnosticsScope.Shard, DiagnosticsAvailability.Available, "shard-1"));
        DiagnosticsCollectionSnapshot<ActiveQuerySnapshot> familyValue = Collection(
            Metadata(DiagnosticsScope.Instance, DiagnosticsAvailability.Available, "primary"));
        var shard = new ShardDiagnosticsSection<
            DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
            "shard-1",
            DiagnosticsAvailability.Available,
            shardValue);
        var family = new RuntimeDiagnosticsFamilySection<
            DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
            "primary",
            familyValue);
        var shards = new List<ShardDiagnosticsSection<
            DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>> { shard };
        var families = new List<RuntimeDiagnosticsFamilySection<
            DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>> { family };

        var topology = new DiagnosticsTopologySnapshot<
            DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
            aggregate,
            shards,
            shardCapacity: 2,
            droppedShardCount: 0,
            shardsTruncated: false,
            families,
            runtimeFamilyCapacity: 2,
            droppedRuntimeFamilyCount: 0,
            runtimeFamiliesTruncated: false);

        shards.Clear();
        families.Clear();

        Assert.Single(topology.Shards!);
        Assert.Single(topology.RuntimeFamilies!);
        Assert.NotEqual(topology.Metadata.ServerInstanceId, topology.Shards![0].Value!.Metadata.ServerInstanceId);
        Assert.NotEqual(topology.Metadata.ServerInstanceId, topology.RuntimeFamilies![0].Value.Metadata.ServerInstanceId);
    }

    [Fact]
    public void SourceGeneratedTopology_RejectsDuplicateAndPartialPartitions()
    {
        DiagnosticsCollectionSnapshot<ActiveQuerySnapshot> aggregate = Collection(
            Metadata(DiagnosticsScope.Aggregate, DiagnosticsAvailability.Available, "cluster"));
        DiagnosticsCollectionSnapshot<ActiveQuerySnapshot> shardValue = Collection(
            Metadata(DiagnosticsScope.Shard, DiagnosticsAvailability.Available, "shard-1"));
        var shard = new ShardDiagnosticsSection<
            DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
            "shard-1",
            DiagnosticsAvailability.Available,
            shardValue);
        var topology = new DiagnosticsTopologySnapshot<
            DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>(
            aggregate,
            [shard],
            shardCapacity: 2,
            droppedShardCount: 0,
            shardsTruncated: false);
        var typeInfo = CSharpDbObservabilityJsonContext.Default
            .DiagnosticsTopologySnapshotDiagnosticsCollectionSnapshotActiveQuerySnapshot;
        string json = JsonSerializer.Serialize(topology, typeInfo);

        JsonObject duplicate = Assert.IsType<JsonObject>(JsonNode.Parse(json));
        JsonArray duplicateShards = Assert.IsType<JsonArray>(duplicate["shards"]);
        duplicateShards.Add(duplicateShards[0]!.DeepClone());
        Assert.Throws<ArgumentException>(() => JsonSerializer.Deserialize(
            duplicate.ToJsonString(),
            typeInfo));

        JsonObject partial = Assert.IsType<JsonObject>(JsonNode.Parse(json));
        Assert.True(partial.Remove("shardCapacity"));
        Assert.Throws<ArgumentOutOfRangeException>(() => JsonSerializer.Deserialize(
            partial.ToJsonString(),
            typeInfo));
    }

    [Fact]
    public void SourceGeneratedContext_CoversEveryClosedPhaseTwoTopologyPartition()
    {
        Type[] requiredTypes =
        [
            typeof(DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>),
            typeof(RuntimeDiagnosticsFamilySection<RuntimeDiagnosticsSnapshot>),
            typeof(ShardDiagnosticsSection<RuntimeDiagnosticsSnapshot>),
            typeof(DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>),
            typeof(RuntimeDiagnosticsFamilySection<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>),
            typeof(ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>),
            typeof(DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>),
            typeof(RuntimeDiagnosticsFamilySection<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>),
            typeof(ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>),
            typeof(DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>),
            typeof(RuntimeDiagnosticsFamilySection<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>),
            typeof(ShardDiagnosticsSection<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>),
            typeof(DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>),
            typeof(RuntimeDiagnosticsFamilySection<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>),
            typeof(ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>),
            typeof(DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>),
            typeof(RuntimeDiagnosticsFamilySection<DiagnosticsValueSnapshot<QueryDetailSnapshot>>),
            typeof(ShardDiagnosticsSection<DiagnosticsValueSnapshot<QueryDetailSnapshot>>),
        ];

        Assert.All(requiredTypes, type =>
            Assert.NotNull(CSharpDbObservabilityJsonContext.Default.GetTypeInfo(type)));
    }

    private static DiagnosticsCollectionSnapshot<ActiveQuerySnapshot> Collection(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            records: [],
            capacity: 1,
            retention: null,
            droppedCount: 0,
            isTruncated: false);

    private static DiagnosticsSnapshotMetadata Metadata(
        DiagnosticsScope scope,
        DiagnosticsAvailability availability,
        string alias,
        bool recordsTruncated = false,
        bool fieldsTruncated = false)
        => new(
            CSharpDbDiagnostics.SchemaVersion,
            new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero),
            alias switch
            {
                "cluster" => "11111111111111111111111111111111",
                "shard-1" => "22222222222222222222222222222222",
                _ => "33333333333333333333333333333333",
            },
            counterEpoch: 0,
            scope,
            availability,
            DiagnosticsSource.Engine,
            alias,
            recordsTruncated,
            fieldsTruncated);
}
