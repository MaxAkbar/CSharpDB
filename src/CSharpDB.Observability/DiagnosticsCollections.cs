using System.Collections.ObjectModel;
using System.Text.Json.Serialization;

namespace CSharpDB.Observability;

/// <summary>
/// A transport-neutral diagnostics result that carries one optional value.
/// Availability is explicit so disabled, unsupported, denied, and unavailable
/// results never need a fabricated payload or lose their runtime identity.
/// </summary>
public sealed class DiagnosticsValueSnapshot<T> : IRuntimeDiagnosticsSnapshot
    where T : class, IRuntimeDiagnosticsSnapshot
{
    [JsonConstructor]
    public DiagnosticsValueSnapshot(
        DiagnosticsSnapshotMetadata metadata,
        T? value)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        if (metadata.RecordsTruncated)
        {
            throw new ArgumentException(
                "A single-value diagnostics result cannot report truncated records.",
                nameof(metadata));
        }
        if (metadata.Availability != DiagnosticsAvailability.Available &&
            metadata.FieldsTruncated)
        {
            throw new ArgumentException(
                "A diagnostics result without a value cannot report truncated fields.",
                nameof(metadata));
        }
        if ((metadata.Availability == DiagnosticsAvailability.Available) !=
            (value is not null))
        {
            throw new ArgumentException(
                "An available diagnostics result requires a value, and an unavailable result must omit it.",
                nameof(value));
        }
        if (value is not null && value.Metadata != metadata)
        {
            throw new ArgumentException(
                "The diagnostics value must share the complete envelope capture metadata.",
                nameof(value));
        }

        Metadata = metadata;
        Value = value;
    }

    public DiagnosticsSnapshotMetadata Metadata { get; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public T? Value { get; }
}

/// <summary>
/// A transport-neutral, bounded diagnostics collection. Unlike the lower-level
/// <see cref="BoundedDiagnosticsSnapshot{T}"/>, this public envelope carries the
/// identity, availability, capacity, and retention semantics needed even when
/// no records are returned.
/// </summary>
public sealed class DiagnosticsCollectionSnapshot<T> : IRuntimeDiagnosticsSnapshot
    where T : class, IRuntimeDiagnosticsSnapshot
{
    [JsonConstructor]
    public DiagnosticsCollectionSnapshot(
        DiagnosticsSnapshotMetadata metadata,
        IReadOnlyList<T>? records,
        int? capacity,
        TimeSpan? retention,
        long? droppedCount,
        bool? isTruncated)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        T[]? recordArray = records?.ToArray();

        if (metadata.Availability == DiagnosticsAvailability.Available)
        {
            ArgumentNullException.ThrowIfNull(recordArray);
            if (capacity is null or <= 0 || capacity > CSharpDbObservabilityOptions.MaximumHistoryCapacity)
                throw new ArgumentOutOfRangeException(nameof(capacity));
            if (droppedCount is null or < 0)
                throw new ArgumentOutOfRangeException(nameof(droppedCount));
            if (isTruncated is null)
                throw new ArgumentNullException(nameof(isTruncated));
            if (droppedCount > 0 && !isTruncated.Value)
            {
                throw new ArgumentException(
                    "A collection with dropped records must be marked truncated.",
                    nameof(isTruncated));
            }
            if (metadata.RecordsTruncated != isTruncated.Value)
            {
                throw new ArgumentException(
                    "Collection truncation must match the snapshot metadata.",
                    nameof(isTruncated));
            }
            if (recordArray.Length > capacity.Value)
                throw new ArgumentException("The collection cannot contain more records than its capacity.", nameof(records));
            if (retention is { } configuredRetention &&
                (configuredRetention <= TimeSpan.Zero ||
                 configuredRetention > CSharpDbObservabilityOptions.MaximumRetention))
            {
                throw new ArgumentOutOfRangeException(nameof(retention));
            }

            foreach (T record in recordArray)
            {
                ArgumentNullException.ThrowIfNull(record);
                DiagnosticsSnapshotMetadata recordMetadata = record.Metadata;
                if (recordMetadata != metadata)
                {
                    throw new ArgumentException(
                        "Every collection record must share the complete envelope capture metadata.",
                        nameof(records));
                }
            }
        }
        else if (metadata.RecordsTruncated || metadata.FieldsTruncated ||
                 recordArray is not null || capacity is not null ||
                 retention is not null || droppedCount is not null || isTruncated is not null)
        {
            throw new ArgumentException(
                "An unavailable diagnostics collection must omit records and bounded-storage values.");
        }

        Metadata = metadata;
        Records = recordArray is null
            ? null
            : new ReadOnlyCollection<T>(recordArray);
        Capacity = capacity;
        Retention = retention;
        DroppedCount = droppedCount;
        IsTruncated = isTruncated;
    }

    public DiagnosticsSnapshotMetadata Metadata { get; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IReadOnlyList<T>? Records { get; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? Capacity { get; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public TimeSpan? Retention { get; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public long? DroppedCount { get; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public bool? IsTruncated { get; }
}

/// <summary>
/// One explicitly identified shard section. A reachable shard carries its
/// exact typed response, whose nested metadata can itself report disabled,
/// denied, unavailable, or not-applicable data. An unreachable or unsupported
/// shard omits the child because no trustworthy remote identity was obtained.
/// </summary>
public sealed class ShardDiagnosticsSection<T>
    where T : class, IRuntimeDiagnosticsSnapshot
{
    [JsonConstructor]
    public ShardDiagnosticsSection(
        string shardAlias,
        DiagnosticsAvailability availability,
        T? value)
    {
        if (!CSharpDbObservabilityOptions.IsValidDatabaseAlias(shardAlias))
            throw new ArgumentException("A safe shard alias is required.", nameof(shardAlias));
        if (availability == DiagnosticsAvailability.Unknown || !Enum.IsDefined(availability))
            throw new ArgumentOutOfRangeException(nameof(availability));
        if ((availability == DiagnosticsAvailability.Available) != (value is not null))
        {
            throw new ArgumentException(
                "An available shard requires a value, and an unavailable shard must omit it.");
        }
        if (value is not null &&
            (value.Metadata.Scope != DiagnosticsScope.Shard ||
             !string.Equals(value.Metadata.DatabaseAlias, shardAlias, StringComparison.Ordinal)))
        {
            throw new ArgumentException(
                "A reachable shard payload must carry matching Shard-scope metadata.",
                nameof(value));
        }

        ShardAlias = shardAlias;
        Availability = availability;
        Value = value;
    }

    public string ShardAlias { get; }
    public DiagnosticsAvailability Availability { get; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public T? Value { get; }
}
