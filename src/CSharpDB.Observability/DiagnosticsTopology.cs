using System.Collections.ObjectModel;
using System.Text.Json.Serialization;

namespace CSharpDB.Observability;

/// <summary>
/// One exact runtime counter family retained by a client while work from an
/// older database generation is still active.
/// </summary>
public sealed class RuntimeDiagnosticsFamilySection<T>
    where T : class, IRuntimeDiagnosticsSnapshot
{
    [JsonConstructor]
    public RuntimeDiagnosticsFamilySection(string databaseAlias, T value)
    {
        if (!CSharpDbObservabilityOptions.IsValidDatabaseAlias(databaseAlias))
            throw new ArgumentException("A safe database alias is required.", nameof(databaseAlias));
        ArgumentNullException.ThrowIfNull(value);
        if (value.Metadata.Scope != DiagnosticsScope.Instance ||
            value.Metadata.Availability == DiagnosticsAvailability.Unknown ||
            !Enum.IsDefined(value.Metadata.Availability) ||
            !string.Equals(value.Metadata.DatabaseAlias, databaseAlias, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "A runtime-family payload must be an Instance-scope snapshot with defined availability and the matching safe alias.",
                nameof(value));
        }

        DatabaseAlias = databaseAlias;
        Value = value;
    }

    public string DatabaseAlias { get; }
    public T Value { get; }
}

/// <summary>
/// A transport-neutral diagnostics response that preserves one primary
/// instance or aggregate payload and, for a sharded aggregate, a separately
/// bounded set of per-shard payloads.
/// </summary>
/// <remarks>
/// The primary payload owns the response metadata. Its records therefore
/// share one exact capture identity and counter epoch. Per-shard payloads keep
/// their own shard identity and epoch and must never be folded into that
/// primary record set without being reprojected as aggregate records.
/// </remarks>
public sealed class DiagnosticsTopologySnapshot<T> : IRuntimeDiagnosticsSnapshot
    where T : class, IRuntimeDiagnosticsSnapshot
{
    [JsonConstructor]
    public DiagnosticsTopologySnapshot(
        T aggregate,
        IReadOnlyList<ShardDiagnosticsSection<T>>? shards,
        int? shardCapacity,
        long? droppedShardCount,
        bool? shardsTruncated,
        IReadOnlyList<RuntimeDiagnosticsFamilySection<T>>? runtimeFamilies = null,
        int? runtimeFamilyCapacity = null,
        long? droppedRuntimeFamilyCount = null,
        bool? runtimeFamiliesTruncated = null)
    {
        ArgumentNullException.ThrowIfNull(aggregate);

        ShardDiagnosticsSection<T>[]? shardArray = shards?.ToArray();
        DiagnosticsScope scope = aggregate.Metadata.Scope;
        RuntimeDiagnosticsFamilySection<T>[]? runtimeFamilyArray =
            runtimeFamilies?.ToArray();
        if (scope == DiagnosticsScope.Instance)
        {
            if (shardArray is not null || shardCapacity is not null ||
                droppedShardCount is not null || shardsTruncated is not null ||
                runtimeFamilyArray is not null || runtimeFamilyCapacity is not null ||
                droppedRuntimeFamilyCount is not null || runtimeFamiliesTruncated is not null)
            {
                throw new ArgumentException(
                    "An instance diagnostics response must omit topology partitions.",
                    nameof(shards));
            }
        }
        else if (scope == DiagnosticsScope.Aggregate)
        {
            bool hasAnyShardField = shardArray is not null || shardCapacity is not null ||
                droppedShardCount is not null || shardsTruncated is not null;
            if (hasAnyShardField)
            {
                ArgumentNullException.ThrowIfNull(shardArray);
                if (shardCapacity is null or <= 0 ||
                    shardCapacity > CSharpDbDiagnostics.MaximumConfiguredDatabaseAliases)
                {
                    throw new ArgumentOutOfRangeException(nameof(shardCapacity));
                }
                if (droppedShardCount is null or < 0)
                    throw new ArgumentOutOfRangeException(nameof(droppedShardCount));
                if (shardsTruncated is null)
                    throw new ArgumentNullException(nameof(shardsTruncated));
                if (droppedShardCount > 0 && !shardsTruncated.Value)
                {
                    throw new ArgumentException(
                        "A topology response with dropped shards must mark its shard list truncated.",
                        nameof(shardsTruncated));
                }
                if (shardArray.Length > shardCapacity.Value)
                {
                    throw new ArgumentException(
                        "The topology response cannot contain more shards than its shard capacity.",
                        nameof(shards));
                }

                var aliases = new HashSet<string>(StringComparer.Ordinal);
                foreach (ShardDiagnosticsSection<T> shard in shardArray)
                {
                    ArgumentNullException.ThrowIfNull(shard);
                    if (!aliases.Add(shard.ShardAlias))
                    {
                        throw new ArgumentException(
                            "A topology response cannot contain a shard alias more than once.",
                            nameof(shards));
                    }
                }
            }

            bool hasAnyRuntimeFamilyField = runtimeFamilyArray is not null ||
                runtimeFamilyCapacity is not null ||
                droppedRuntimeFamilyCount is not null || runtimeFamiliesTruncated is not null;
            if (hasAnyRuntimeFamilyField)
            {
                ArgumentNullException.ThrowIfNull(runtimeFamilyArray);
                if (runtimeFamilyCapacity is null or <= 0 ||
                    runtimeFamilyCapacity > CSharpDbDiagnostics.MaximumRuntimeDiagnosticsFamilies)
                {
                    throw new ArgumentOutOfRangeException(nameof(runtimeFamilyCapacity));
                }
                if (droppedRuntimeFamilyCount is null or < 0)
                    throw new ArgumentOutOfRangeException(nameof(droppedRuntimeFamilyCount));
                if (runtimeFamiliesTruncated is null)
                    throw new ArgumentNullException(nameof(runtimeFamiliesTruncated));
                if (droppedRuntimeFamilyCount > 0 && !runtimeFamiliesTruncated.Value)
                {
                    throw new ArgumentException(
                        "A topology response with dropped runtime families must mark its family list truncated.",
                        nameof(runtimeFamiliesTruncated));
                }
                if (runtimeFamilyArray.Length > runtimeFamilyCapacity.Value)
                {
                    throw new ArgumentException(
                        "The topology response cannot contain more runtime families than its family capacity.",
                        nameof(runtimeFamilies));
                }

                var identities = new HashSet<(string ServerInstanceId, long CounterEpoch)>();
                foreach (RuntimeDiagnosticsFamilySection<T> family in runtimeFamilyArray)
                {
                    ArgumentNullException.ThrowIfNull(family);
                    DiagnosticsSnapshotMetadata metadata = family.Value.Metadata;
                    if (!identities.Add((metadata.ServerInstanceId, metadata.CounterEpoch)))
                    {
                        throw new ArgumentException(
                            "A topology response cannot contain the same runtime-family identity more than once.",
                            nameof(runtimeFamilies));
                    }
                }
            }
        }
        else
        {
            throw new ArgumentException(
                "A topology response must have Instance or Aggregate scope.",
                nameof(aggregate));
        }

        Aggregate = aggregate;
        Shards = shardArray is null
            ? null
            : new ReadOnlyCollection<ShardDiagnosticsSection<T>>(shardArray);
        ShardCapacity = shardCapacity;
        DroppedShardCount = droppedShardCount;
        ShardsTruncated = shardsTruncated;
        RuntimeFamilies = runtimeFamilyArray is null
            ? null
            : new ReadOnlyCollection<RuntimeDiagnosticsFamilySection<T>>(runtimeFamilyArray);
        RuntimeFamilyCapacity = runtimeFamilyCapacity;
        DroppedRuntimeFamilyCount = droppedRuntimeFamilyCount;
        RuntimeFamiliesTruncated = runtimeFamiliesTruncated;
    }

    /// <summary>
    /// The primary instance or aggregate payload. For a direct client this is
    /// the instance payload; for a sharded client it is the coordinator-owned
    /// aggregate payload.
    /// </summary>
    public T Aggregate { get; }

    public DiagnosticsSnapshotMetadata Metadata => Aggregate.Metadata;

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IReadOnlyList<ShardDiagnosticsSection<T>>? Shards { get; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? ShardCapacity { get; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public long? DroppedShardCount { get; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public bool? ShardsTruncated { get; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IReadOnlyList<RuntimeDiagnosticsFamilySection<T>>? RuntimeFamilies { get; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? RuntimeFamilyCapacity { get; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public long? DroppedRuntimeFamilyCount { get; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public bool? RuntimeFamiliesTruncated { get; }
}
