using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Client.Internal;
using CSharpDB.Engine;
using CSharpDB.Observability;
using CSharpDB.Sql;

namespace CSharpDB.Client;

public sealed partial class CSharpDbShardedClient
{
    private const int MaximumConcurrentDiagnosticsShardCaptures = 8;

    private readonly string _diagnosticsDatabaseAlias;
    private readonly TimeProvider? _coordinatorTimeProvider;
    private CSharpDbRuntimeDiagnosticsState? _coordinatorRuntimeState;
    private int _coordinatorDiagnosticsDisposed;

    /// <summary>
    /// Internal composition seam used by in-process hosts and focused contract
    /// tests that already own the physical shard clients.
    /// </summary>
    internal CSharpDbShardedClient(
        CSharpDbShardingOptions effectiveOptions,
        IReadOnlyDictionary<string, ICSharpDbClient> clients,
        TimeProvider? coordinatorTimeProvider = null)
        : this(
            CSharpDbShardMap.Create(
                effectiveOptions ?? throw new ArgumentNullException(nameof(effectiveOptions))),
            CopyDiagnosticsClients(clients),
            routeContextAccessor: null,
            catalogStore: null,
            effectiveOptions,
            coordinatorTimeProvider)
    {
    }

    public async Task<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>
        GetRuntimeDiagnosticsAsync(CancellationToken ct = default)
    {
        ShardCaptureSet<RuntimeDiagnosticsSnapshot> capture = await CaptureShardsAsync(
            static (client, _, token) => client.GetRuntimeDiagnosticsAsync(token),
            ProjectRuntimeSnapshot,
            ct).ConfigureAwait(false);

        RuntimeDiagnosticsSnapshot aggregate = CreateCoordinatorRuntimeAggregate();

        return CreateTopology(aggregate, capture);
    }

    public async Task<DiagnosticsTopologySnapshot<
        DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>
        GetActiveQueriesAsync(
            int maximumRecords,
            CancellationToken ct = default)
    {
        ValidateDiagnosticsMaximumRecords(maximumRecords);
        DiagnosticsCollectionSnapshot<ActiveQuerySnapshot> aggregate =
            CreateCoordinatorActiveCollection(maximumRecords);
        IReadOnlyDictionary<string, int> perShardRecordBudgets =
            CreatePerShardRecordBudgets(
                maximumRecords,
                aggregate.Records?.Count ?? 0);
        ShardCaptureSet<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>> capture =
            await CaptureShardsAsync(
                (client, shardAlias, token) => client.GetActiveQueriesAsync(
                    ToWireRecordBudget(perShardRecordBudgets[shardAlias]),
                    token),
                (value, shardAlias) => ProjectActiveQueryCollection(
                    value,
                    shardAlias,
                    perShardRecordBudgets[shardAlias]),
                ct).ConfigureAwait(false);

        return CreateTopology(aggregate, capture);
    }

    public async Task<DiagnosticsTopologySnapshot<
        DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>>
        GetRecentQueriesAsync(
            int maximumRecords,
            CancellationToken ct = default)
    {
        ValidateDiagnosticsMaximumRecords(maximumRecords);
        DiagnosticsCollectionSnapshot<RecentQuerySnapshot> aggregate =
            CreateCoordinatorRecentCollection(maximumRecords);
        IReadOnlyDictionary<string, int> perShardRecordBudgets =
            CreatePerShardRecordBudgets(
                maximumRecords,
                aggregate.Records?.Count ?? 0);
        ShardCaptureSet<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>> capture =
            await CaptureShardsAsync(
                (client, shardAlias, token) => client.GetRecentQueriesAsync(
                    ToWireRecordBudget(perShardRecordBudgets[shardAlias]),
                    token),
                (value, shardAlias) => ProjectRecentQueryCollection(
                    value,
                    shardAlias,
                    perShardRecordBudgets[shardAlias]),
                ct).ConfigureAwait(false);

        return CreateTopology(aggregate, capture);
    }

    public async Task<DiagnosticsTopologySnapshot<
        DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>>
        GetQueryPlanDiagnosticsAsync(
            OpaqueDiagnosticsId operationId,
            CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operationId);
        ShardCaptureSet<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>> capture =
            await CaptureShardsAsync(
                (client, _, token) => client.GetQueryPlanDiagnosticsAsync(operationId, token),
                ProjectQueryPlanValue,
                ct).ConfigureAwait(false);

        DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot> aggregate =
            CreateCoordinatorQueryPlanValue(operationId);

        return CreateTopology(aggregate, capture);
    }

    public async Task<DiagnosticsTopologySnapshot<
        DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>>
        GetSessionsAsync(
            int maximumRecords,
            CancellationToken ct = default)
    {
        ValidateDiagnosticsMaximumRecords(maximumRecords);
        DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot> aggregate =
            CreateCoordinatorSessionCollection(maximumRecords);
        IReadOnlyDictionary<string, int> perShardRecordBudgets =
            CreatePerShardRecordBudgets(
                maximumRecords,
                aggregate.Records?.Count ?? 0);
        ShardCaptureSet<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>> capture =
            await CaptureShardsAsync(
                (client, shardAlias, token) => client.GetSessionsAsync(
                    ToWireRecordBudget(perShardRecordBudgets[shardAlias]),
                    token),
                (value, shardAlias) => ProjectSessionCollection(
                    value,
                    shardAlias,
                    perShardRecordBudgets[shardAlias]),
                ct).ConfigureAwait(false);

        return CreateTopology(aggregate, capture);
    }

    public async Task<DiagnosticsTopologySnapshot<
        DiagnosticsValueSnapshot<QueryDetailSnapshot>>>
        GetQueryDetailAsync(
            OpaqueDiagnosticsId operationId,
            CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operationId);

        // Detail is deliberately captured through its own child call. No
        // ordinary runtime/query collection capture can materialize SQL text.
        ShardCaptureSet<DiagnosticsValueSnapshot<QueryDetailSnapshot>> capture =
            await CaptureShardsAsync(
                (client, _, token) => client.GetQueryDetailAsync(operationId, token),
                ProjectQueryDetailValue,
                ct).ConfigureAwait(false);

        DiagnosticsValueSnapshot<QueryDetailSnapshot> aggregate =
            CreateCoordinatorQueryDetailValue(operationId);

        return CreateTopology(aggregate, capture);
    }

    private async Task<ShardCaptureSet<T>> CaptureShardsAsync<T>(
        Func<ICSharpDbObservabilityClient, string, CancellationToken, Task<DiagnosticsTopologySnapshot<T>>> capture,
        Func<T, string, T> project,
        CancellationToken ct)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        ArgumentNullException.ThrowIfNull(capture);
        ArgumentNullException.ThrowIfNull(project);
        ct.ThrowIfCancellationRequested();

        CSharpDbShardDefinition[] configured = _map.Shards
            .OrderBy(static shard => shard.ShardId, StringComparer.Ordinal)
            .ToArray();
        int shardCapacity = Math.Min(
            configured.Length,
            CSharpDbDiagnostics.MaximumConfiguredDatabaseAliases);
        if (shardCapacity <= 0)
            throw new InvalidOperationException("A sharded client requires at least one diagnostics partition.");

        CSharpDbShardDefinition[] selected = configured[..shardCapacity];
        IReadOnlyDictionary<string, string> diagnosticsAliases =
            CreateDiagnosticsShardAliases(selected);
        using var gate = new SemaphoreSlim(
            Math.Min(MaximumConcurrentDiagnosticsShardCaptures, shardCapacity));
        Task<ShardCapture<T>>[] tasks = selected
            .Select(shard => CaptureShardAsync(
                shard,
                diagnosticsAliases[shard.ShardId],
                capture,
                project,
                gate,
                ct))
            .ToArray();
        ShardCapture<T>[] captures = await Task.WhenAll(tasks).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();

        long droppedShardCount = Math.Max(0L, (long)configured.Length - captures.Length);
        return new ShardCaptureSet<T>(captures, shardCapacity, droppedShardCount);
    }

    private async Task<ShardCapture<T>> CaptureShardAsync<T>(
        CSharpDbShardDefinition shard,
        string diagnosticsAlias,
        Func<ICSharpDbObservabilityClient, string, CancellationToken, Task<DiagnosticsTopologySnapshot<T>>> capture,
        Func<T, string, T> project,
        SemaphoreSlim gate,
        CancellationToken ct)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        string shardId = shard.ShardId;
        if (!shard.Enabled)
        {
            return ShardCapture<T>.WithoutValue(
                diagnosticsAlias,
                DiagnosticsAvailability.Disabled);
        }

        if (!_clients.TryGetValue(shardId, out ICSharpDbClient? child))
        {
            return ShardCapture<T>.WithoutValue(
                diagnosticsAlias,
                DiagnosticsAvailability.Unavailable);
        }

        if (child is not ICSharpDbObservabilityClient observabilityClient)
        {
            return ShardCapture<T>.WithoutValue(
                diagnosticsAlias,
                DiagnosticsAvailability.Unsupported);
        }

        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            DiagnosticsTopologySnapshot<T> childTopology =
                await capture(observabilityClient, shardId, ct).ConfigureAwait(false);
            ArgumentNullException.ThrowIfNull(childTopology);
            T projected = project(childTopology.Aggregate, diagnosticsAlias);
            return ShardCapture<T>.Available(diagnosticsAlias, projected);
        }
        catch (NotSupportedException)
        {
            return ShardCapture<T>.WithoutValue(
                diagnosticsAlias,
                DiagnosticsAvailability.Unsupported);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            // Child failures are deliberately projected without exception
            // text, endpoint, database path, or captured SQL.
            return ShardCapture<T>.WithoutValue(
                diagnosticsAlias,
                DiagnosticsAvailability.Unavailable);
        }
        finally
        {
            gate.Release();
        }
    }

    private DiagnosticsTopologySnapshot<T> CreateTopology<T>(
        T aggregate,
        ShardCaptureSet<T> capture)
        where T : class, IRuntimeDiagnosticsSnapshot
        => new(
            aggregate,
            capture.Captures.Select(static item => item.Section).ToArray(),
            capture.ShardCapacity,
            capture.DroppedShardCount,
            capture.DroppedShardCount > 0);

    private DiagnosticsSnapshotMetadata CreateAggregateMetadata(
        DiagnosticsAvailability availability,
        bool recordsTruncated = false,
        bool fieldsTruncated = false)
        => GetOrCreateCoordinatorRuntimeState().CreateMetadata(
            DiagnosticsScope.Aggregate,
            availability,
            DiagnosticsSource.Client,
            _diagnosticsDatabaseAlias,
            recordsTruncated: recordsTruncated,
            fieldsTruncated: fieldsTruncated);

    private RuntimeDiagnosticsSnapshot CreateCoordinatorRuntimeAggregate()
    {
        CSharpDbRuntimeDiagnosticsState runtimeState =
            GetOrCreateCoordinatorRuntimeState();
        if (!runtimeState.IsEnabled)
        {
            DiagnosticsSnapshotMetadata disabled = CreateAggregateMetadata(
                DiagnosticsAvailability.Disabled);
            return new RuntimeDiagnosticsSnapshot(
                disabled,
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

        QueryRuntimeDiagnostics? diagnostics = TryGetCoordinatorQueryDiagnostics();
        if (diagnostics is null)
        {
            DiagnosticsSnapshotMetadata unavailable = CreateAggregateMetadata(
                DiagnosticsAvailability.Unavailable);
            return new RuntimeDiagnosticsSnapshot(
                unavailable,
                DiagnosticsSection<QueryDiagnosticsSummary>.WithoutValue(
                    DiagnosticsAvailability.Unavailable),
                DiagnosticsSection<ConnectionDiagnosticsSnapshot>.WithoutValue(
                    DiagnosticsAvailability.Unavailable),
                DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.WithoutValue(
                    DiagnosticsAvailability.Unavailable),
                DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.WithoutValue(
                    DiagnosticsAvailability.Unavailable),
                DiagnosticsSection<MaintenanceOperationSnapshot>.WithoutValue(
                    DiagnosticsAvailability.Unavailable),
                DiagnosticsSection<HealthDiagnosticsSnapshot>.WithoutValue(
                    DiagnosticsAvailability.Unavailable));
        }

        QueryDiagnosticsSummary raw = diagnostics.GetSummary();
        DiagnosticsSnapshotMetadata metadata = ProjectAggregateMetadata(raw.Metadata);
        return new RuntimeDiagnosticsSnapshot(
            metadata,
            DiagnosticsSection<QueryDiagnosticsSummary>.Available(
                raw with { Metadata = metadata }),
            DiagnosticsSection<ConnectionDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<MaintenanceOperationSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<HealthDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable));
    }

    private QueryRuntimeDiagnostics? TryGetCoordinatorQueryDiagnostics()
    {
        CSharpDbRuntimeDiagnosticsState runtimeState =
            GetOrCreateCoordinatorRuntimeState();
        if (!runtimeState.IsEnabled)
            return null;
        try
        {
            return QueryRuntimeDiagnostics.GetOrCreate(runtimeState);
        }
        catch
        {
            return null;
        }
    }

    private DiagnosticsSnapshotMetadata ProjectAggregateMetadata(
        DiagnosticsSnapshotMetadata metadata,
        bool? recordsTruncated = null,
        bool? fieldsTruncated = null)
        => new(
            metadata.SchemaVersion,
            metadata.CapturedAtUtc,
            metadata.ServerInstanceId,
            metadata.CounterEpoch,
            DiagnosticsScope.Aggregate,
            metadata.Availability,
            DiagnosticsSource.Client,
            _diagnosticsDatabaseAlias,
            recordsTruncated ?? metadata.RecordsTruncated,
            fieldsTruncated ?? metadata.FieldsTruncated);

    private DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>
        CreateCoordinatorActiveCollection(int maximumRecords)
    {
        QueryRuntimeDiagnostics? diagnostics = TryGetCoordinatorQueryDiagnostics();
        if (diagnostics is null)
        {
            return CreateCoordinatorCollectionWithoutValue<ActiveQuerySnapshot>(
                GetOrCreateCoordinatorRuntimeState().IsEnabled
                    ? DiagnosticsAvailability.Unavailable
                    : DiagnosticsAvailability.Disabled);
        }

        DiagnosticsCollectionSnapshot<ActiveQuerySnapshot> raw =
            diagnostics.GetActiveCollectionSnapshot(maximumRecords);
        DiagnosticsSnapshotMetadata metadata = ProjectAggregateMetadata(raw.Metadata);
        return new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            metadata,
            raw.Records!.Select(item => item with { Metadata = metadata }).ToArray(),
            raw.Capacity,
            raw.Retention,
            raw.DroppedCount,
            raw.IsTruncated);
    }

    private DiagnosticsCollectionSnapshot<RecentQuerySnapshot>
        CreateCoordinatorRecentCollection(int maximumRecords)
    {
        QueryRuntimeDiagnostics? diagnostics = TryGetCoordinatorQueryDiagnostics();
        if (diagnostics is null)
        {
            return CreateCoordinatorCollectionWithoutValue<RecentQuerySnapshot>(
                GetOrCreateCoordinatorRuntimeState().IsEnabled
                    ? DiagnosticsAvailability.Unavailable
                    : DiagnosticsAvailability.Disabled);
        }

        DiagnosticsCollectionSnapshot<RecentQuerySnapshot> raw =
            diagnostics.GetRecentCollectionSnapshot(maximumRecords);
        DiagnosticsSnapshotMetadata metadata = ProjectAggregateMetadata(raw.Metadata);
        return new DiagnosticsCollectionSnapshot<RecentQuerySnapshot>(
            metadata,
            raw.Records!.Select(item => item with { Metadata = metadata }).ToArray(),
            raw.Capacity,
            raw.Retention,
            raw.DroppedCount,
            raw.IsTruncated);
    }

    private DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>
        CreateCoordinatorSessionCollection(int maximumRecords)
    {
        if (!GetOrCreateCoordinatorRuntimeState().IsEnabled)
        {
            return CreateCoordinatorCollectionWithoutValue<SessionDiagnosticsSnapshot>(
                DiagnosticsAvailability.Disabled);
        }

        DiagnosticsSnapshotMetadata metadata = CreateAggregateMetadata(
            DiagnosticsAvailability.Available);
        return new DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>(
            metadata,
            records: [],
            capacity: maximumRecords,
            retention: null,
            droppedCount: 0,
            isTruncated: false);
    }

    private DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>
        CreateCoordinatorQueryPlanValue(OpaqueDiagnosticsId operationId)
    {
        QueryRuntimeDiagnostics? diagnostics = TryGetCoordinatorQueryDiagnostics();
        if (diagnostics is null)
        {
            return CreateCoordinatorValueWithoutValue<QueryPlanDiagnosticsSnapshot>(
                GetOrCreateCoordinatorRuntimeState().IsEnabled
                    ? DiagnosticsAvailability.Unavailable
                    : DiagnosticsAvailability.Disabled);
        }

        QueryPlanDiagnosticsSnapshot? plan = diagnostics.GetPlanSnapshot(operationId);
        if (plan is null || !HasMeaningfulCoordinatorPlan(plan))
        {
            return CreateCoordinatorValueWithoutValue<QueryPlanDiagnosticsSnapshot>(
                DiagnosticsAvailability.Unavailable);
        }

        DiagnosticsSnapshotMetadata metadata = ProjectAggregateMetadata(plan.Metadata);
        return new DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>(
            metadata,
            plan with { Metadata = metadata });
    }

    private DiagnosticsValueSnapshot<QueryDetailSnapshot>
        CreateCoordinatorQueryDetailValue(OpaqueDiagnosticsId operationId)
    {
        QueryRuntimeDiagnostics? diagnostics = TryGetCoordinatorQueryDiagnostics();
        if (diagnostics is null)
        {
            return CreateCoordinatorValueWithoutValue<QueryDetailSnapshot>(
                GetOrCreateCoordinatorRuntimeState().IsEnabled
                    ? DiagnosticsAvailability.Unavailable
                    : DiagnosticsAvailability.Disabled);
        }

        QueryDetailSnapshot? detail = diagnostics.GetQueryDetailSnapshot(operationId);
        if (detail is null)
        {
            return CreateCoordinatorValueWithoutValue<QueryDetailSnapshot>(
                DiagnosticsAvailability.Unavailable);
        }

        DiagnosticsSnapshotMetadata metadata = ProjectAggregateMetadata(detail.Metadata);
        return new DiagnosticsValueSnapshot<QueryDetailSnapshot>(
            metadata,
            detail with { Metadata = metadata });
    }

    private DiagnosticsCollectionSnapshot<T>
        CreateCoordinatorCollectionWithoutValue<T>(DiagnosticsAvailability availability)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        DiagnosticsSnapshotMetadata metadata = CreateAggregateMetadata(availability);
        return new DiagnosticsCollectionSnapshot<T>(
            metadata,
            records: null,
            capacity: null,
            retention: null,
            droppedCount: null,
            isTruncated: null);
    }

    private DiagnosticsValueSnapshot<T> CreateCoordinatorValueWithoutValue<T>(
        DiagnosticsAvailability availability)
        where T : class, IRuntimeDiagnosticsSnapshot
        => new(CreateAggregateMetadata(availability), value: null);

    private static bool HasMeaningfulCoordinatorPlan(
        QueryPlanDiagnosticsSnapshot plan)
        => plan.AccessPath != QueryAccessPathCategory.Unknown ||
           plan.PlanCacheHit is not null ||
           plan.Reoptimized is not null ||
           plan.EstimatedRows is not null ||
           plan.ActualRows is not null ||
           plan.PlanNodeCount is not null ||
           plan.PlanTruncated;

    private static IReadOnlyDictionary<string, string> CreateDiagnosticsShardAliases(
        IReadOnlyList<CSharpDbShardDefinition> shards)
    {
        var aliases = new Dictionary<string, string>(shards.Count, StringComparer.Ordinal);
        var used = new HashSet<string>(StringComparer.Ordinal);

        // Preserve every already-valid public shard id exactly. Reserve these
        // before hashing long ids so a digest can never displace a short id.
        foreach (CSharpDbShardDefinition shard in shards)
        {
            if (CSharpDbObservabilityOptions.IsValidDatabaseAlias(shard.ShardId))
            {
                aliases.Add(shard.ShardId, shard.ShardId);
                used.Add(shard.ShardId);
            }
        }

        foreach (CSharpDbShardDefinition shard in shards)
        {
            if (aliases.ContainsKey(shard.ShardId))
                continue;

            int discriminator = 0;
            string diagnosticsAlias;
            do
            {
                string hashInput = discriminator == 0
                    ? shard.ShardId
                    : string.Concat(
                        discriminator.ToString(CultureInfo.InvariantCulture),
                        ":",
                        shard.ShardId);
                diagnosticsAlias = Convert.ToHexString(
                        SHA256.HashData(Encoding.UTF8.GetBytes(hashInput)))
                    .ToLowerInvariant();
                discriminator++;
            }
            while (!used.Add(diagnosticsAlias));

            aliases.Add(shard.ShardId, diagnosticsAlias);
        }

        return aliases;
    }

    private IReadOnlyDictionary<string, int> CreatePerShardRecordBudgets(
        int maximumRecords,
        int aggregateRecordCount)
    {
        CSharpDbShardDefinition[] selected = _map.Shards
            .OrderBy(static shard => shard.ShardId, StringComparer.Ordinal)
            .Take(CSharpDbDiagnostics.MaximumConfiguredDatabaseAliases)
            .ToArray();
        int remaining = Math.Max(0, maximumRecords - Math.Max(0, aggregateRecordCount));
        int quotient = remaining / selected.Length;
        int remainder = remaining % selected.Length;
        var budgets = new Dictionary<string, int>(selected.Length, StringComparer.Ordinal);
        for (int index = 0; index < selected.Length; index++)
        {
            budgets.Add(
                selected[index].ShardId,
                quotient + (index < remainder ? 1 : 0));
        }

        return budgets;
    }

    // Child capability methods validate a positive request limit. A shard
    // assigned zero response records is queried with the wire minimum solely
    // to retain truthful identity and availability, then projected to zero.
    private static int ToWireRecordBudget(int projectedRecordBudget)
        => Math.Max(1, projectedRecordBudget);

    private static RuntimeDiagnosticsSnapshot ProjectRuntimeSnapshot(
        RuntimeDiagnosticsSnapshot value,
        string shardAlias)
    {
        DiagnosticsSnapshotMetadata metadata = ProjectShardMetadata(
            value.Metadata,
            shardAlias);
        return new RuntimeDiagnosticsSnapshot(
            metadata,
            ProjectSection(value.Queries, metadata, static (item, projected) =>
                item with { Metadata = projected }),
            ProjectSection(value.Connections, metadata, static (item, projected) =>
                item with { Metadata = projected }),
            ProjectSection(value.Storage, metadata, static (item, projected) =>
                item with { Metadata = projected }),
            ProjectSection(value.Wal, metadata, static (item, projected) =>
                item with { Metadata = projected }),
            ProjectSection(value.ActiveMaintenance, metadata, static (item, projected) =>
                item with { Metadata = projected }),
            ProjectSection(value.Health, metadata, static (item, projected) =>
                item with { Metadata = projected }));
    }

    private static DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>
        ProjectActiveQueryCollection(
            DiagnosticsCollectionSnapshot<ActiveQuerySnapshot> value,
            string shardAlias,
            int maximumRecords)
        => ProjectCollection(
            value,
            shardAlias,
            maximumRecords,
            static (record, metadata) => record with { Metadata = metadata });

    private static DiagnosticsCollectionSnapshot<RecentQuerySnapshot>
        ProjectRecentQueryCollection(
            DiagnosticsCollectionSnapshot<RecentQuerySnapshot> value,
            string shardAlias,
            int maximumRecords)
        => ProjectCollection(
            value,
            shardAlias,
            maximumRecords,
            static (record, metadata) => record with { Metadata = metadata });

    private static DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>
        ProjectSessionCollection(
            DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot> value,
            string shardAlias,
            int maximumRecords)
        => ProjectCollection(
            value,
            shardAlias,
            maximumRecords,
            static (record, metadata) => record with { Metadata = metadata });

    private static DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>
        ProjectQueryPlanValue(
            DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot> value,
            string shardAlias)
        => ProjectValue(
            value,
            shardAlias,
            static (item, metadata) => item with { Metadata = metadata });

    private static DiagnosticsValueSnapshot<QueryDetailSnapshot>
        ProjectQueryDetailValue(
            DiagnosticsValueSnapshot<QueryDetailSnapshot> value,
            string shardAlias)
        => ProjectValue(
            value,
            shardAlias,
            static (item, metadata) => item with { Metadata = metadata });

    private static DiagnosticsCollectionSnapshot<T> ProjectCollection<T>(
        DiagnosticsCollectionSnapshot<T> value,
        string shardAlias,
        int maximumRecords,
        Func<T, DiagnosticsSnapshotMetadata, T> projectRecord)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        ArgumentNullException.ThrowIfNull(value);
        if (value.Metadata.Availability != DiagnosticsAvailability.Available)
        {
            DiagnosticsSnapshotMetadata metadata = ProjectShardMetadata(
                value.Metadata,
                shardAlias);
            return new DiagnosticsCollectionSnapshot<T>(
                metadata,
                records: null,
                capacity: null,
                retention: null,
                droppedCount: null,
                isTruncated: null);
        }

        IReadOnlyList<T> sourceRecords = value.Records!;
        bool truncated = value.IsTruncated == true ||
            sourceRecords.Count > maximumRecords;
        DiagnosticsSnapshotMetadata availableMetadata = ProjectShardMetadata(
            value.Metadata,
            shardAlias,
            recordsTruncated: truncated);
        T[] records = sourceRecords
            .Take(maximumRecords)
            .Select(record => projectRecord(record, availableMetadata))
            .ToArray();
        return new DiagnosticsCollectionSnapshot<T>(
            availableMetadata,
            records,
            value.Capacity,
            value.Retention,
            value.DroppedCount,
            truncated);
    }

    private static DiagnosticsValueSnapshot<T> ProjectValue<T>(
        DiagnosticsValueSnapshot<T> value,
        string shardAlias,
        Func<T, DiagnosticsSnapshotMetadata, T> projectValue)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        ArgumentNullException.ThrowIfNull(value);
        DiagnosticsSnapshotMetadata metadata = ProjectShardMetadata(
            value.Metadata,
            shardAlias);
        return new DiagnosticsValueSnapshot<T>(
            metadata,
            value.Value is null ? null : projectValue(value.Value, metadata));
    }

    private static DiagnosticsSection<T> ProjectSection<T>(
        DiagnosticsSection<T> section,
        DiagnosticsSnapshotMetadata metadata,
        Func<T, DiagnosticsSnapshotMetadata, T> projectValue)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        ArgumentNullException.ThrowIfNull(section);
        return section.Value is null
            ? DiagnosticsSection<T>.WithoutValue(section.Availability)
            : DiagnosticsSection<T>.Available(projectValue(section.Value, metadata));
    }

    private static DiagnosticsSnapshotMetadata ProjectShardMetadata(
        DiagnosticsSnapshotMetadata metadata,
        string shardAlias,
        bool? recordsTruncated = null)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        return new DiagnosticsSnapshotMetadata(
            metadata.SchemaVersion,
            metadata.CapturedAtUtc,
            metadata.ServerInstanceId,
            metadata.CounterEpoch,
            DiagnosticsScope.Shard,
            metadata.Availability,
            metadata.Source,
            shardAlias,
            recordsTruncated ?? metadata.RecordsTruncated,
            metadata.FieldsTruncated);
    }

    private static void ValidateDiagnosticsMaximumRecords(int maximumRecords)
    {
        if (maximumRecords <= 0 ||
            maximumRecords > CSharpDbObservabilityOptions.MaximumHistoryCapacity)
        {
            throw new ArgumentOutOfRangeException(nameof(maximumRecords));
        }
    }

    private static string ResolveDiagnosticsDatabaseAlias(
        CSharpDbShardingOptions effectiveOptions)
    {
        string? configured = effectiveOptions.DirectDatabaseOptions?
            .ObservabilityOptions?.DatabaseAlias;
        return CSharpDbObservabilityOptions.IsValidDatabaseAlias(configured)
            ? configured!
            : "sharded";
    }

    private static CSharpDbRuntimeDiagnosticsState? CreateCoordinatorRuntimeStateIfEnabled(
        CSharpDbShardingOptions effectiveOptions,
        string databaseAlias,
        TimeProvider? timeProvider)
    {
        CSharpDbObservabilityOptions? configured = effectiveOptions
            .DirectDatabaseOptions?.ObservabilityOptions;
        return configured?.Enabled == true
            ? CreateCoordinatorRuntimeState(
                effectiveOptions,
                databaseAlias,
                timeProvider)
            : null;
    }

    private static CSharpDbRuntimeDiagnosticsState CreateCoordinatorRuntimeState(
        CSharpDbShardingOptions effectiveOptions,
        string databaseAlias,
        TimeProvider? timeProvider)
    {
        CSharpDbObservabilityOptions? configured = effectiveOptions
            .DirectDatabaseOptions?.ObservabilityOptions;
        return new CSharpDbRuntimeDiagnosticsState(
            configured ?? new CSharpDbObservabilityOptions
            {
                Enabled = false,
                DatabaseAlias = databaseAlias,
            },
            timeProvider);
    }

    private CSharpDbRuntimeDiagnosticsState GetOrCreateCoordinatorRuntimeState()
    {
        CSharpDbRuntimeDiagnosticsState? existing =
            Volatile.Read(ref _coordinatorRuntimeState);
        if (existing is not null)
            return existing;
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref _coordinatorDiagnosticsDisposed) != 0,
            this);

        CSharpDbRuntimeDiagnosticsState candidate = CreateCoordinatorRuntimeState(
            _effectiveOptions,
            _diagnosticsDatabaseAlias,
            _coordinatorTimeProvider);
        CSharpDbRuntimeDiagnosticsState? raced = Interlocked.CompareExchange(
            ref _coordinatorRuntimeState,
            candidate,
            comparand: null);
        if (raced is not null)
        {
            candidate.Dispose();
            return raced;
        }

        if (Volatile.Read(ref _coordinatorDiagnosticsDisposed) == 0)
            return candidate;

        Interlocked.CompareExchange(
            ref _coordinatorRuntimeState,
            null,
            candidate);
        candidate.Dispose();
        throw new ObjectDisposedException(GetType().FullName);
    }

    private CoordinatorRuntimeObservation? StartCoordinatorRuntimeObservation(
        string sql,
        out ClientOperationObservation? loggingObservation)
    {
        loggingObservation = null;
        CSharpDbRuntimeDiagnosticsState? runtimeState =
            Volatile.Read(ref _coordinatorRuntimeState);
        if (runtimeState?.IsEnabled != true ||
            CSharpDbOperationScope.IsDiagnosticsSuppressed)
        {
            return null;
        }

        try
        {
            CSharpDbObservabilityOptions options =
                runtimeState.CreateOptionsSnapshot();
            SqlTextCaptureMode captureMode = options.Logging.SqlText;
            QueryFingerprint? fingerprint = null;
            string? capturedSqlText = null;
            try
            {
                if (captureMode == SqlTextCaptureMode.Normalized)
                {
                    QueryFingerprintResult normalized = SqlQueryFingerprintProvider
                        .Instance.NormalizeAndFingerprint(sql);
                    fingerprint = normalized.Fingerprint;
                    capturedSqlText = normalized.NormalizedText;
                }
                else
                {
                    fingerprint = SqlQueryFingerprintProvider.Instance
                        .CreateFingerprint(sql);
                    if (captureMode == SqlTextCaptureMode.Raw)
                        capturedSqlText = sql;
                }
            }
            catch
            {
                capturedSqlText = null;
            }

            CSharpDbOperationContext context = CreateCoordinatorOperationContext(
                fingerprint,
                runtimeState.TimeProvider);
            loggingObservation = ClientOperationObservation.StartQueryCoordinator(
                options,
                sql,
                context);
            QueryRuntimeDiagnostics diagnostics = QueryRuntimeDiagnostics.GetOrCreate(
                runtimeState);
            QueryRuntimeDiagnostics.QueryRuntimeOperation? operation = diagnostics.TryStart(
                context,
                QueryExecutionPhase.Planning,
                captureMode,
                capturedSqlText,
                out _);
            if (operation is null)
                return null;

            return new CoordinatorRuntimeObservation(
                context,
                operation,
                options.Logging.GetSlowQueryThreshold(CSharpDbOperationClass.Query));
        }
        catch
        {
            return null;
        }
    }

    private CSharpDbOperationContext CreateCoordinatorOperationContext(
        QueryFingerprint? fingerprint,
        TimeProvider timeProvider)
    {
        CSharpDbOperationContext? parent = CSharpDbOperationScope.Current;
        if (parent is not null)
        {
            return CSharpDbOperationContext.CreateStatement(
                parent,
                fingerprint,
                timeProvider);
        }

        CSharpDB.Observability.CSharpDbTransport boundary =
            CSharpDbOperationScope.CurrentTransport;
        CSharpDB.Observability.CSharpDbTransport transport =
            boundary == CSharpDB.Observability.CSharpDbTransport.Embedded
                ? CSharpDB.Observability.CSharpDbTransport.Sharded
                : boundary;
        return CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            transport,
            _diagnosticsDatabaseAlias,
            CSharpDbOperationScope.CurrentSessionId,
            fingerprint,
            timeProvider);
    }

    private static long SaturatingAddNonNegative(long left, long right)
    {
        left = Math.Max(0, left);
        right = Math.Max(0, right);
        return right > long.MaxValue - left ? long.MaxValue : left + right;
    }

    private static Dictionary<string, ICSharpDbClient> CopyDiagnosticsClients(
        IReadOnlyDictionary<string, ICSharpDbClient> clients)
    {
        ArgumentNullException.ThrowIfNull(clients);
        var copy = new Dictionary<string, ICSharpDbClient>(
            clients.Count,
            StringComparer.OrdinalIgnoreCase);
        foreach ((string shardId, ICSharpDbClient client) in clients)
        {
            ArgumentNullException.ThrowIfNull(client);
            copy.Add(CSharpDbShardMap.NormalizeShardId(shardId), client);
        }
        return copy;
    }

    private sealed class CoordinatorRuntimeObservation
    {
        private readonly CSharpDbOperationContext _context;
        private readonly QueryRuntimeDiagnostics.QueryRuntimeOperation _operation;
        private readonly TimeSpan _slowQueryThreshold;
        private int _completed;

        internal CoordinatorRuntimeObservation(
            CSharpDbOperationContext context,
            QueryRuntimeDiagnostics.QueryRuntimeOperation operation,
            TimeSpan slowQueryThreshold)
        {
            _context = context;
            _operation = operation;
            _slowQueryThreshold = slowQueryThreshold;
        }

        internal IDisposable EnterScope()
            => CSharpDbOperationScope.Enter(_context);

        internal void MarkExecuting()
            => _operation.SetPhase(QueryExecutionPhase.Executing);

        internal void Complete(
            CSharpDbOperationOutcome outcome,
            long rowsProduced,
            long rowsAffected,
            SafeErrorProjection? error)
        {
            if (Interlocked.Exchange(ref _completed, 1) != 0)
                return;

            TimeSpan duration;
            DateTimeOffset completedAtUtc;
            try
            {
                duration = _context.GetElapsedTime();
                completedAtUtc = _context.GetUtcNow();
            }
            catch
            {
                duration = TimeSpan.Zero;
                completedAtUtc = _context.StartedAtUtc;
            }

            try
            {
                _operation.Complete(
                    outcome,
                    completedAtUtc,
                    duration,
                    timeToFirstResult: null,
                    Math.Max(0, rowsProduced),
                    Math.Max(0, rowsAffected),
                    error,
                    duration >= _slowQueryThreshold);
            }
            catch
            {
                // Coordinator runtime diagnostics cannot affect fan-out work.
            }
        }
    }

    private sealed record ShardCapture<T>(
        string ShardAlias,
        ShardDiagnosticsSection<T> Section)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        public static ShardCapture<T> Available(string shardAlias, T value)
            => new(
                shardAlias,
                new ShardDiagnosticsSection<T>(
                    shardAlias,
                    DiagnosticsAvailability.Available,
                    value));

        public static ShardCapture<T> WithoutValue(
            string shardAlias,
            DiagnosticsAvailability availability)
            => new(
                shardAlias,
                new ShardDiagnosticsSection<T>(shardAlias, availability, value: null));
    }

    private sealed record ShardCaptureSet<T>(
        IReadOnlyList<ShardCapture<T>> Captures,
        int ShardCapacity,
        long DroppedShardCount)
        where T : class, IRuntimeDiagnosticsSnapshot;

    private sealed partial class RoutedClient
    {
        public Task<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>
            GetRuntimeDiagnosticsAsync(CancellationToken ct = default)
            => ResolveObservabilityClient().GetRuntimeDiagnosticsAsync(ct);

        public Task<DiagnosticsTopologySnapshot<
            DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>
            GetActiveQueriesAsync(
                int maximumRecords,
                CancellationToken ct = default)
            => ResolveObservabilityClient().GetActiveQueriesAsync(maximumRecords, ct);

        public Task<DiagnosticsTopologySnapshot<
            DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>>
            GetRecentQueriesAsync(
                int maximumRecords,
                CancellationToken ct = default)
            => ResolveObservabilityClient().GetRecentQueriesAsync(maximumRecords, ct);

        public Task<DiagnosticsTopologySnapshot<
            DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>>
            GetQueryPlanDiagnosticsAsync(
                OpaqueDiagnosticsId operationId,
                CancellationToken ct = default)
            => ResolveObservabilityClient().GetQueryPlanDiagnosticsAsync(operationId, ct);

        public Task<DiagnosticsTopologySnapshot<
            DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>>
            GetSessionsAsync(
                int maximumRecords,
                CancellationToken ct = default)
            => ResolveObservabilityClient().GetSessionsAsync(maximumRecords, ct);

        public Task<DiagnosticsTopologySnapshot<
            DiagnosticsValueSnapshot<QueryDetailSnapshot>>>
            GetQueryDetailAsync(
                OpaqueDiagnosticsId operationId,
                CancellationToken ct = default)
            => ResolveObservabilityClient().GetQueryDetailAsync(operationId, ct);

        private ICSharpDbObservabilityClient ResolveObservabilityClient()
            => ResolveClient() as ICSharpDbObservabilityClient
               ?? throw new CSharpDbObservabilityNotSupportedException();
    }
}
