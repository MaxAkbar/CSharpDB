using CSharpDB.Observability;

namespace CSharpDB.Api.Diagnostics;

internal static class CSharpDbHostRequestDiagnosticsProjection
{
    internal static DiagnosticsTopologySnapshot<
        DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>> MergeSessions(
            DiagnosticsTopologySnapshot<
                DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>> topology,
            ICSharpDbHostRequestDiagnosticsContributor? contributor,
            int maximumRecords)
    {
        ArgumentNullException.ThrowIfNull(topology);
        if (contributor is null ||
            topology.Aggregate.Metadata.Availability ==
                DiagnosticsAvailability.Disabled)
        {
            return topology;
        }

        HostRequestDiagnosticsRawCollection host;
        try
        {
            host = contributor.Capture();
        }
        catch
        {
            return topology;
        }

        DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot> aggregate =
            topology.Aggregate;
        SessionDiagnosticsSnapshot[] existing = aggregate.Records?.ToArray() ?? [];
        if (host.Records.Count == 0 && host.DroppedCount == 0)
            return topology;

        int combinedCount = SaturatingAdd(existing.Length, host.Records.Count);
        int selectedCount = Math.Min(maximumRecords, combinedCount);
        int remainingShardRecords = Math.Max(0, maximumRecords - selectedCount);
        long droppedCount = SaturatingAdd(
            aggregate.DroppedCount ?? 0,
            host.DroppedCount);
        bool truncated =
            aggregate.Metadata.RecordsTruncated ||
            host.IsTruncated ||
            combinedCount > maximumRecords ||
            droppedCount > 0;
        DiagnosticsSnapshotMetadata metadata = CopyMetadata(
            aggregate.Metadata,
            DiagnosticsAvailability.Available,
            truncated);

        var records = new List<SessionDiagnosticsSnapshot>(selectedCount);
        foreach (HostRequestDiagnosticsRawSnapshot request in host.Records)
        {
            if (records.Count == maximumRecords)
                break;
            records.Add(new SessionDiagnosticsSnapshot(
                metadata,
                request.SessionId,
                request.CreatedAtUtc,
                request.LastActiveAtUtc,
                request.CurrentOperationId,
                HasActiveReader: false,
                HasActiveTransaction: false,
                request.Transport)
            {
                State = DiagnosticsSessionState.Active,
            });
        }

        foreach (SessionDiagnosticsSnapshot session in existing)
        {
            if (records.Count == maximumRecords)
                break;
            records.Add(session with { Metadata = metadata });
        }

        var merged = new DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>(
            metadata,
            records,
            maximumRecords,
            aggregate.Retention,
            droppedCount,
            truncated);
        IReadOnlyList<ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<
            SessionDiagnosticsSnapshot>>>? shards = RebudgetShards(
                topology.Shards,
                remainingShardRecords);
        return new DiagnosticsTopologySnapshot<
            DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>(
                merged,
                shards,
                topology.ShardCapacity,
                topology.DroppedShardCount,
                topology.ShardsTruncated,
                topology.RuntimeFamilies,
                topology.RuntimeFamilyCapacity,
                topology.DroppedRuntimeFamilyCount,
                topology.RuntimeFamiliesTruncated);
    }

    private static IReadOnlyList<ShardDiagnosticsSection<
        DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>>? RebudgetShards(
            IReadOnlyList<ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<
                SessionDiagnosticsSnapshot>>>? shards,
            int remainingRecords)
    {
        if (shards is null || shards.Count == 0)
            return shards;

        int quotient = remainingRecords / shards.Count;
        int remainder = remainingRecords % shards.Count;
        var budgets = new Dictionary<string, int>(shards.Count, StringComparer.Ordinal);
        int index = 0;
        foreach (ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<
                     SessionDiagnosticsSnapshot>> shard in shards
                     .OrderBy(static shard => shard.ShardAlias, StringComparer.Ordinal))
        {
            budgets.Add(
                shard.ShardAlias,
                quotient + (index < remainder ? 1 : 0));
            index++;
        }

        var projected = new ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<
            SessionDiagnosticsSnapshot>>[shards.Count];
        for (index = 0; index < shards.Count; index++)
        {
            ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<
                SessionDiagnosticsSnapshot>> shard = shards[index];
            DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>? value =
                shard.Value;
            if (value?.Records is not { } sourceRecords ||
                sourceRecords.Count <= budgets[shard.ShardAlias])
            {
                projected[index] = shard;
                continue;
            }

            int budget = budgets[shard.ShardAlias];
            DiagnosticsSnapshotMetadata childMetadata = CopyMetadata(
                value.Metadata,
                value.Metadata.Availability,
                recordsTruncated: true);
            SessionDiagnosticsSnapshot[] records = sourceRecords
                .Take(budget)
                .Select(record => record with { Metadata = childMetadata })
                .ToArray();
            var child = new DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>(
                childMetadata,
                records,
                value.Capacity,
                value.Retention,
                value.DroppedCount,
                isTruncated: true);
            projected[index] = new ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<
                SessionDiagnosticsSnapshot>>(
                    shard.ShardAlias,
                    shard.Availability,
                    child);
        }

        return projected;
    }

    private static DiagnosticsSnapshotMetadata CopyMetadata(
        DiagnosticsSnapshotMetadata source,
        DiagnosticsAvailability availability,
        bool recordsTruncated)
        => new(
            source.SchemaVersion,
            source.CapturedAtUtc,
            source.ServerInstanceId,
            source.CounterEpoch,
            source.Scope,
            availability,
            source.Source,
            source.DatabaseAlias,
            recordsTruncated,
            source.FieldsTruncated);

    private static int SaturatingAdd(int left, int right)
        => left > int.MaxValue - right ? int.MaxValue : left + right;

    private static long SaturatingAdd(long left, long right)
    {
        left = Math.Max(0, left);
        right = Math.Max(0, right);
        return left > long.MaxValue - right
            ? long.MaxValue
            : left + right;
    }
}
