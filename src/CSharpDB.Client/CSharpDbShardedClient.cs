using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Client.Models;
using CSharpDB.Storage.Diagnostics;

namespace CSharpDB.Client;

public sealed class CSharpDbShardedClient : ICSharpDbClient, ICSharpDbShardAdminClient
{
    private const string TransactionPrefix = "csdbshard";

    private readonly CSharpDbShardMap _map;
    private readonly Dictionary<string, ICSharpDbClient> _clients;
    private readonly ICSharpDbRouteContextAccessor? _routeContextAccessor;
    private readonly CSharpDbShardCatalogStore? _catalogStore;
    private readonly RoutedClient _requestRoutedClient;

    private CSharpDbShardedClient(
        CSharpDbShardMap map,
        Dictionary<string, ICSharpDbClient> clients,
        ICSharpDbRouteContextAccessor? routeContextAccessor,
        CSharpDbShardCatalogStore? catalogStore)
    {
        _map = map;
        _clients = clients;
        _routeContextAccessor = routeContextAccessor;
        _catalogStore = catalogStore;
        _requestRoutedClient = new RoutedClient(this, fixedRoute: null);
    }

    public string DataSource => $"sharded://{_map.Keyspace}?version={_map.MapVersion}";

    public static CSharpDbShardedClient Create(
        CSharpDbShardingOptions options,
        ICSharpDbRouteContextAccessor? routeContextAccessor = null)
    {
        var client = CreateCore(options, routeContextAccessor);
        client.WarmAsync(CancellationToken.None).GetAwaiter().GetResult();
        return client;
    }

    public static async Task<CSharpDbShardedClient> CreateAsync(
        CSharpDbShardingOptions options,
        ICSharpDbRouteContextAccessor? routeContextAccessor = null,
        CancellationToken ct = default)
    {
        var client = CreateCore(options, routeContextAccessor);
        await client.WarmAsync(ct).ConfigureAwait(false);
        return client;
    }

    public static string GetCanonicalRouteText(CSharpDbRouteContext routeContext)
    {
        var (keyspace, key) = CSharpDbShardMap.NormalizeRoute(routeContext);
        return $"{keyspace.Length}:{keyspace}|{key.Length}:{key}";
    }

    public static ulong ComputeRouteToken(CSharpDbRouteContext routeContext)
    {
        byte[] canonicalBytes = Encoding.UTF8.GetBytes(GetCanonicalRouteText(routeContext));
        byte[] hash = SHA256.HashData(canonicalBytes);
        return BinaryPrimitives.ReadUInt64BigEndian(hash.AsSpan(0, sizeof(ulong)));
    }

    public static CSharpDbShardMapSnapshot CreateShardMapSnapshot(CSharpDbShardingOptions options)
        => CSharpDbShardMap.Create(options).ToSnapshot();

    public static CSharpDbShardCatalogValidationResult ValidateCatalogUpdate(
        CSharpDbShardMapSnapshot currentMap,
        CSharpDbShardCatalogUpdateRequest request)
    {
        ArgumentNullException.ThrowIfNull(currentMap);
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Options);

        var issues = new List<CSharpDbShardCatalogIssue>();
        CSharpDbShardMapSnapshot? preview = null;
        bool requiresDataMigration = false;

        if (request.ExpectedCurrentMapVersion is int expectedVersion &&
            expectedVersion != currentMap.MapVersion)
        {
            issues.Add(new CSharpDbShardCatalogIssue
            {
                Severity = CSharpDbShardCatalogIssueSeverity.Error,
                Code = "map-version-mismatch",
                Message = $"Expected current map version {expectedVersion}, but the live map version is {currentMap.MapVersion}.",
            });
        }

        try
        {
            preview = CreateShardMapSnapshot(request.Options);
        }
        catch (Exception ex) when (ex is CSharpDbClientConfigurationException or CSharpDbClientException or ArgumentException)
        {
            issues.Add(new CSharpDbShardCatalogIssue
            {
                Severity = CSharpDbShardCatalogIssueSeverity.Error,
                Code = "invalid-map",
                Message = ex.Message,
            });
        }

        if (preview is not null)
        {
            if (preview.MapVersion <= currentMap.MapVersion)
            {
                issues.Add(new CSharpDbShardCatalogIssue
                {
                    Severity = CSharpDbShardCatalogIssueSeverity.Error,
                    Code = "map-version-not-incremented",
                    Message = $"Proposed map version {preview.MapVersion} must be greater than the live map version {currentMap.MapVersion}.",
                });
            }

            requiresDataMigration = HasOwnershipChange(currentMap, preview);
            if (requiresDataMigration && !request.AllowMetadataOnlyOwnershipChange)
            {
                issues.Add(new CSharpDbShardCatalogIssue
                {
                    Severity = CSharpDbShardCatalogIssueSeverity.Error,
                    Code = "migration-required",
                    Message = "Bucket ranges or exact-key pins changed. Move or verify affected data first, or explicitly acknowledge a metadata-only ownership change.",
                });
            }

            if (!requiresDataMigration)
            {
                issues.Add(new CSharpDbShardCatalogIssue
                {
                    Severity = CSharpDbShardCatalogIssueSeverity.Info,
                    Code = "metadata-compatible",
                    Message = "The proposed map does not change bucket ownership or exact-key pins.",
                });
            }
        }

        return CreateValidationResult(issues, preview, requiresDataMigration);
    }

    public ICSharpDbClient ForRoute(CSharpDbRouteContext routeContext)
    {
        ArgumentNullException.ThrowIfNull(routeContext);
        return new RoutedClient(this, routeContext);
    }

    public ICSharpDbClient ForShardId(string shardId)
        => GetShardClient(CSharpDbShardMap.NormalizeShardId(shardId));

    public CSharpDbShardResolution ResolveRoute(CSharpDbRouteContext routeContext)
        => _map.Resolve(routeContext);

    public Task<CSharpDbShardMapSnapshot> GetShardMapAsync(CancellationToken ct = default)
        => Task.FromResult(_map.ToSnapshot());

    public Task<CSharpDbShardCatalogState> GetShardCatalogAsync(CancellationToken ct = default)
        => _catalogStore is null
            ? Task.FromResult(new CSharpDbShardCatalogState
            {
                Source = "runtime-config",
                IsCatalogEnabled = false,
                IsWritable = false,
                ActiveMap = _map.ToSnapshot(),
                PendingMap = null,
                History = [],
            })
            : _catalogStore.GetStateAsync(_map.ToSnapshot(), ct);

    public Task<CSharpDbShardCatalogValidationResult> ValidateShardCatalogUpdateAsync(
        CSharpDbShardCatalogUpdateRequest request,
        CancellationToken ct = default)
        => Task.FromResult(ValidateCatalogUpdate(_map.ToSnapshot(), request));

    public Task<CSharpDbShardCatalogApplyResult> ApplyShardCatalogUpdateAsync(
        CSharpDbShardCatalogUpdateRequest request,
        CancellationToken ct = default)
    {
        if (_catalogStore is null)
        {
            var validation = ValidateCatalogUpdate(_map.ToSnapshot(), request);
            validation.Issues.Add(new CSharpDbShardCatalogIssue
            {
                Severity = CSharpDbShardCatalogIssueSeverity.Error,
                Code = "catalog-not-enabled",
                Message = "Shard catalog writes require CSharpDB:Sharding:Catalog:Enabled=true.",
            });

            return Task.FromResult(new CSharpDbShardCatalogApplyResult
            {
                Applied = false,
                RequiresRestart = false,
                Message = "Shard catalog writes are not enabled.",
                Validation = CreateValidationResult(validation.Issues, validation.Preview, validation.RequiresDataMigration),
                PendingMap = null,
            });
        }

        return _catalogStore.ApplyAsync(_map.ToSnapshot(), request, ct);
    }

    public Task<CSharpDbShardResolution> ResolveRouteAsync(
        CSharpDbRouteContext routeContext,
        CancellationToken ct = default)
        => Task.FromResult(ResolveRoute(routeContext));

    public async Task<IReadOnlyList<CSharpDbShardStatus>> GetShardStatusAsync(CancellationToken ct = default)
    {
        var statuses = new List<CSharpDbShardStatus>(_map.Shards.Count);
        foreach (CSharpDbShardDefinition shard in _map.Shards)
        {
            if (!shard.Enabled)
            {
                statuses.Add(new CSharpDbShardStatus
                {
                    ShardId = shard.ShardId,
                    DataSource = GetConfiguredDataSource(shard),
                    Enabled = false,
                    Healthy = false,
                    Error = "Shard is disabled.",
                });
                continue;
            }

            try
            {
                ICSharpDbClient client = GetShardClient(shard.ShardId);
                DatabaseInfo info = await client.GetInfoAsync(ct).ConfigureAwait(false);
                statuses.Add(new CSharpDbShardStatus
                {
                    ShardId = shard.ShardId,
                    DataSource = client.DataSource,
                    Enabled = true,
                    Healthy = true,
                    Info = info,
                });
            }
            catch (Exception ex)
            {
                statuses.Add(new CSharpDbShardStatus
                {
                    ShardId = shard.ShardId,
                    DataSource = GetConfiguredDataSource(shard),
                    Enabled = true,
                    Healthy = false,
                    Error = ex.Message,
                });
            }
        }

        return statuses;
    }

    public async Task<IReadOnlyList<CSharpDbShardSqlExecutionResult>> ExecuteSqlOnAllShardsAsync(
        string sql,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);

        var results = new List<CSharpDbShardSqlExecutionResult>(_clients.Count);
        foreach ((string shardId, ICSharpDbClient client) in _clients)
        {
            try
            {
                SqlExecutionResult result = await client.ExecuteSqlAsync(sql, ct).ConfigureAwait(false);
                results.Add(new CSharpDbShardSqlExecutionResult
                {
                    ShardId = shardId,
                    Result = result,
                    Error = result.Error,
                });
            }
            catch (Exception ex)
            {
                results.Add(new CSharpDbShardSqlExecutionResult
                {
                    ShardId = shardId,
                    Error = ex.Message,
                });
            }
        }

        return results;
    }

    public async ValueTask DisposeAsync()
    {
        List<Exception>? exceptions = null;
        foreach (ICSharpDbClient client in _clients.Values)
        {
            try
            {
                await client.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                (exceptions ??= []).Add(ex);
            }
        }

        if (exceptions is { Count: > 0 })
            throw new AggregateException(exceptions);
    }

    public Task<DatabaseInfo> GetInfoAsync(CancellationToken ct = default)
        => _requestRoutedClient.GetInfoAsync(ct);

    public Task<IReadOnlyList<string>> GetTableNamesAsync(CancellationToken ct = default)
        => _requestRoutedClient.GetTableNamesAsync(ct);

    public Task<TableSchema?> GetTableSchemaAsync(string tableName, CancellationToken ct = default)
        => _requestRoutedClient.GetTableSchemaAsync(tableName, ct);

    public Task<int> GetRowCountAsync(string tableName, CancellationToken ct = default)
        => _requestRoutedClient.GetRowCountAsync(tableName, ct);

    public Task<TableBrowseResult> BrowseTableAsync(string tableName, int page = 1, int pageSize = 50, CancellationToken ct = default)
        => _requestRoutedClient.BrowseTableAsync(tableName, page, pageSize, ct);

    public Task<Dictionary<string, object?>?> GetRowByPkAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default)
        => _requestRoutedClient.GetRowByPkAsync(tableName, pkColumn, pkValue, ct);

    public Task<int> InsertRowAsync(string tableName, Dictionary<string, object?> values, CancellationToken ct = default)
        => _requestRoutedClient.InsertRowAsync(tableName, values, ct);

    public Task<int> UpdateRowAsync(string tableName, string pkColumn, object pkValue, Dictionary<string, object?> values, CancellationToken ct = default)
        => _requestRoutedClient.UpdateRowAsync(tableName, pkColumn, pkValue, values, ct);

    public Task<int> DeleteRowAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default)
        => _requestRoutedClient.DeleteRowAsync(tableName, pkColumn, pkValue, ct);

    public Task DropTableAsync(string tableName, CancellationToken ct = default)
        => _requestRoutedClient.DropTableAsync(tableName, ct);

    public Task RenameTableAsync(string tableName, string newTableName, CancellationToken ct = default)
        => _requestRoutedClient.RenameTableAsync(tableName, newTableName, ct);

    public Task AddColumnAsync(string tableName, string columnName, DbType type, bool notNull, CancellationToken ct = default)
        => _requestRoutedClient.AddColumnAsync(tableName, columnName, type, notNull, ct);

    public Task AddColumnAsync(string tableName, string columnName, DbType type, bool notNull, string? collation, CancellationToken ct = default)
        => _requestRoutedClient.AddColumnAsync(tableName, columnName, type, notNull, collation, ct);

    public Task DropColumnAsync(string tableName, string columnName, CancellationToken ct = default)
        => _requestRoutedClient.DropColumnAsync(tableName, columnName, ct);

    public Task RenameColumnAsync(string tableName, string oldColumnName, string newColumnName, CancellationToken ct = default)
        => _requestRoutedClient.RenameColumnAsync(tableName, oldColumnName, newColumnName, ct);

    public Task<IReadOnlyList<IndexSchema>> GetIndexesAsync(CancellationToken ct = default)
        => _requestRoutedClient.GetIndexesAsync(ct);

    public Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default)
        => _requestRoutedClient.CreateIndexAsync(indexName, tableName, columnName, isUnique, ct);

    public Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, string? collation, CancellationToken ct = default)
        => _requestRoutedClient.CreateIndexAsync(indexName, tableName, columnName, isUnique, collation, ct);

    public Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default)
        => _requestRoutedClient.UpdateIndexAsync(existingIndexName, newIndexName, tableName, columnName, isUnique, ct);

    public Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique, string? collation, CancellationToken ct = default)
        => _requestRoutedClient.UpdateIndexAsync(existingIndexName, newIndexName, tableName, columnName, isUnique, collation, ct);

    public Task DropIndexAsync(string indexName, CancellationToken ct = default)
        => _requestRoutedClient.DropIndexAsync(indexName, ct);

    public Task<IReadOnlyList<string>> GetViewNamesAsync(CancellationToken ct = default)
        => _requestRoutedClient.GetViewNamesAsync(ct);

    public Task<IReadOnlyList<ViewDefinition>> GetViewsAsync(CancellationToken ct = default)
        => _requestRoutedClient.GetViewsAsync(ct);

    public Task<ViewDefinition?> GetViewAsync(string viewName, CancellationToken ct = default)
        => _requestRoutedClient.GetViewAsync(viewName, ct);

    public Task<string?> GetViewSqlAsync(string viewName, CancellationToken ct = default)
        => _requestRoutedClient.GetViewSqlAsync(viewName, ct);

    public Task<ViewBrowseResult> BrowseViewAsync(string viewName, int page = 1, int pageSize = 50, CancellationToken ct = default)
        => _requestRoutedClient.BrowseViewAsync(viewName, page, pageSize, ct);

    public Task CreateViewAsync(string viewName, string selectSql, CancellationToken ct = default)
        => _requestRoutedClient.CreateViewAsync(viewName, selectSql, ct);

    public Task UpdateViewAsync(string existingViewName, string newViewName, string selectSql, CancellationToken ct = default)
        => _requestRoutedClient.UpdateViewAsync(existingViewName, newViewName, selectSql, ct);

    public Task DropViewAsync(string viewName, CancellationToken ct = default)
        => _requestRoutedClient.DropViewAsync(viewName, ct);

    public Task<IReadOnlyList<TriggerSchema>> GetTriggersAsync(CancellationToken ct = default)
        => _requestRoutedClient.GetTriggersAsync(ct);

    public Task CreateTriggerAsync(string triggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default)
        => _requestRoutedClient.CreateTriggerAsync(triggerName, tableName, timing, triggerEvent, bodySql, ct);

    public Task UpdateTriggerAsync(string existingTriggerName, string newTriggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default)
        => _requestRoutedClient.UpdateTriggerAsync(existingTriggerName, newTriggerName, tableName, timing, triggerEvent, bodySql, ct);

    public Task DropTriggerAsync(string triggerName, CancellationToken ct = default)
        => _requestRoutedClient.DropTriggerAsync(triggerName, ct);

    public Task<IReadOnlyList<SavedQueryDefinition>> GetSavedQueriesAsync(CancellationToken ct = default)
        => _requestRoutedClient.GetSavedQueriesAsync(ct);

    public Task<SavedQueryDefinition?> GetSavedQueryAsync(string name, CancellationToken ct = default)
        => _requestRoutedClient.GetSavedQueryAsync(name, ct);

    public Task<SavedQueryDefinition> UpsertSavedQueryAsync(string name, string sqlText, CancellationToken ct = default)
        => _requestRoutedClient.UpsertSavedQueryAsync(name, sqlText, ct);

    public Task DeleteSavedQueryAsync(string name, CancellationToken ct = default)
        => _requestRoutedClient.DeleteSavedQueryAsync(name, ct);

    public Task<IReadOnlyList<ProcedureDefinition>> GetProceduresAsync(bool includeDisabled = true, CancellationToken ct = default)
        => _requestRoutedClient.GetProceduresAsync(includeDisabled, ct);

    public Task<ProcedureDefinition?> GetProcedureAsync(string name, CancellationToken ct = default)
        => _requestRoutedClient.GetProcedureAsync(name, ct);

    public Task CreateProcedureAsync(ProcedureDefinition definition, CancellationToken ct = default)
        => _requestRoutedClient.CreateProcedureAsync(definition, ct);

    public Task UpdateProcedureAsync(string existingName, ProcedureDefinition definition, CancellationToken ct = default)
        => _requestRoutedClient.UpdateProcedureAsync(existingName, definition, ct);

    public Task DeleteProcedureAsync(string name, CancellationToken ct = default)
        => _requestRoutedClient.DeleteProcedureAsync(name, ct);

    public Task<ProcedureExecutionResult> ExecuteProcedureAsync(string name, IReadOnlyDictionary<string, object?> args, CancellationToken ct = default)
        => _requestRoutedClient.ExecuteProcedureAsync(name, args, ct);

    public Task<SqlExecutionResult> ExecuteSqlAsync(string sql, CancellationToken ct = default)
        => _requestRoutedClient.ExecuteSqlAsync(sql, ct);

    public Task<TransactionSessionInfo> BeginTransactionAsync(CancellationToken ct = default)
        => _requestRoutedClient.BeginTransactionAsync(ct);

    public Task<SqlExecutionResult> ExecuteInTransactionAsync(string transactionId, string sql, CancellationToken ct = default)
        => _requestRoutedClient.ExecuteInTransactionAsync(transactionId, sql, ct);

    public Task CommitTransactionAsync(string transactionId, CancellationToken ct = default)
        => _requestRoutedClient.CommitTransactionAsync(transactionId, ct);

    public Task RollbackTransactionAsync(string transactionId, CancellationToken ct = default)
        => _requestRoutedClient.RollbackTransactionAsync(transactionId, ct);

    public Task<IReadOnlyList<string>> GetCollectionNamesAsync(CancellationToken ct = default)
        => _requestRoutedClient.GetCollectionNamesAsync(ct);

    public Task<int> GetCollectionCountAsync(string collectionName, CancellationToken ct = default)
        => _requestRoutedClient.GetCollectionCountAsync(collectionName, ct);

    public Task<CollectionBrowseResult> BrowseCollectionAsync(string collectionName, int page = 1, int pageSize = 50, CancellationToken ct = default)
        => _requestRoutedClient.BrowseCollectionAsync(collectionName, page, pageSize, ct);

    public Task<System.Text.Json.JsonElement?> GetDocumentAsync(string collectionName, string key, CancellationToken ct = default)
        => _requestRoutedClient.GetDocumentAsync(collectionName, key, ct);

    public Task PutDocumentAsync(string collectionName, string key, System.Text.Json.JsonElement document, CancellationToken ct = default)
        => _requestRoutedClient.PutDocumentAsync(collectionName, key, document, ct);

    public Task<bool> DeleteDocumentAsync(string collectionName, string key, CancellationToken ct = default)
        => _requestRoutedClient.DeleteDocumentAsync(collectionName, key, ct);

    public Task DropCollectionAsync(string collectionName, CancellationToken ct = default)
        => _requestRoutedClient.DropCollectionAsync(collectionName, ct);

    public Task CheckpointAsync(CancellationToken ct = default)
        => _requestRoutedClient.CheckpointAsync(ct);

    public Task<BackupResult> BackupAsync(BackupRequest request, CancellationToken ct = default)
        => _requestRoutedClient.BackupAsync(request, ct);

    public Task<RestoreResult> RestoreAsync(RestoreRequest request, CancellationToken ct = default)
        => _requestRoutedClient.RestoreAsync(request, ct);

    public Task<ForeignKeyMigrationResult> MigrateForeignKeysAsync(ForeignKeyMigrationRequest request, CancellationToken ct = default)
        => _requestRoutedClient.MigrateForeignKeysAsync(request, ct);

    public Task<DatabaseMaintenanceReport> GetMaintenanceReportAsync(CancellationToken ct = default)
        => _requestRoutedClient.GetMaintenanceReportAsync(ct);

    public Task<ReindexResult> ReindexAsync(ReindexRequest request, CancellationToken ct = default)
        => _requestRoutedClient.ReindexAsync(request, ct);

    public Task<VacuumResult> VacuumAsync(CancellationToken ct = default)
        => _requestRoutedClient.VacuumAsync(ct);

    public Task<DatabaseInspectReport> InspectStorageAsync(string? databasePath = null, bool includePages = false, CancellationToken ct = default)
        => _requestRoutedClient.InspectStorageAsync(databasePath, includePages, ct);

    public Task<WalInspectReport> CheckWalAsync(string? databasePath = null, CancellationToken ct = default)
        => _requestRoutedClient.CheckWalAsync(databasePath, ct);

    public Task<PageInspectReport> InspectPageAsync(uint pageId, bool includeHex = false, string? databasePath = null, CancellationToken ct = default)
        => _requestRoutedClient.InspectPageAsync(pageId, includeHex, databasePath, ct);

    public Task<IndexInspectReport> CheckIndexesAsync(string? databasePath = null, string? indexName = null, int? sampleSize = null, CancellationToken ct = default)
        => _requestRoutedClient.CheckIndexesAsync(databasePath, indexName, sampleSize, ct);

    private static CSharpDbShardCatalogValidationResult CreateValidationResult(
        List<CSharpDbShardCatalogIssue> issues,
        CSharpDbShardMapSnapshot? preview,
        bool requiresDataMigration)
        => new()
        {
            IsValid = issues.All(issue => issue.Severity != CSharpDbShardCatalogIssueSeverity.Error),
            RequiresDataMigration = requiresDataMigration,
            Preview = preview,
            Issues = issues,
        };

    private static bool HasOwnershipChange(CSharpDbShardMapSnapshot currentMap, CSharpDbShardMapSnapshot proposedMap)
    {
        if (!string.Equals(currentMap.Keyspace, proposedMap.Keyspace, StringComparison.OrdinalIgnoreCase))
            return true;
        if (currentMap.VirtualBucketCount != proposedMap.VirtualBucketCount)
            return true;
        if (!BucketRangesEqual(currentMap.BucketRanges, proposedMap.BucketRanges))
            return true;
        if (!DictionaryEqual(currentMap.ExactKeyPins, proposedMap.ExactKeyPins, StringComparer.Ordinal))
            return true;

        return false;
    }

    private static bool BucketRangesEqual(
        IReadOnlyList<CSharpDbShardBucketRange> left,
        IReadOnlyList<CSharpDbShardBucketRange> right)
    {
        if (left.Count != right.Count)
            return false;

        for (int i = 0; i < left.Count; i++)
        {
            if (left[i].StartBucketInclusive != right[i].StartBucketInclusive ||
                left[i].EndBucketExclusive != right[i].EndBucketExclusive ||
                !string.Equals(left[i].ShardId, right[i].ShardId, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        return true;
    }

    private static bool DictionaryEqual(
        IReadOnlyDictionary<string, string> left,
        IReadOnlyDictionary<string, string> right,
        StringComparer keyComparer)
    {
        if (left.Count != right.Count)
            return false;

        foreach ((string key, string value) in left)
        {
            if (!right.TryGetValue(key, out string? otherValue) ||
                !keyComparer.Equals(value, otherValue))
            {
                return false;
            }
        }

        return true;
    }

    private static CSharpDbShardedClient CreateCore(
        CSharpDbShardingOptions options,
        ICSharpDbRouteContextAccessor? routeContextAccessor)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!options.Enabled)
            throw new CSharpDbClientConfigurationException("CSharpDB sharding options are disabled.");

        CSharpDbShardingOptions effectiveOptions = CSharpDbShardCatalogStore.ResolveEffectiveOptions(options);
        CSharpDbShardCatalogStore? catalogStore = CSharpDbShardCatalogStore.Create(options, effectiveOptions);
        CSharpDbShardMap map = CSharpDbShardMap.Create(effectiveOptions);
        var clients = new Dictionary<string, ICSharpDbClient>(StringComparer.OrdinalIgnoreCase);
        foreach (CSharpDbShardDefinition shard in map.Shards.Where(shard => shard.Enabled))
            clients.Add(shard.ShardId, CSharpDbClient.Create(BuildShardClientOptions(shard, effectiveOptions)));

        return new CSharpDbShardedClient(map, clients, routeContextAccessor, catalogStore);
    }

    private async Task WarmAsync(CancellationToken ct)
    {
        foreach (ICSharpDbClient client in _clients.Values)
            _ = await client.GetInfoAsync(ct).ConfigureAwait(false);
    }

    private async Task<DatabaseInfo> GetAggregateInfoAsync(CancellationToken ct)
    {
        int tableCount = 0;
        int indexCount = 0;
        int viewCount = 0;
        int triggerCount = 0;
        int procedureCount = 0;
        int collectionCount = 0;
        int savedQueryCount = 0;

        foreach (ICSharpDbClient client in _clients.Values)
        {
            DatabaseInfo info = await client.GetInfoAsync(ct).ConfigureAwait(false);
            tableCount += info.TableCount;
            indexCount += info.IndexCount;
            viewCount += info.ViewCount;
            triggerCount += info.TriggerCount;
            procedureCount += info.ProcedureCount;
            collectionCount += info.CollectionCount;
            savedQueryCount += info.SavedQueryCount;
        }

        return new DatabaseInfo
        {
            DataSource = DataSource,
            TableCount = tableCount,
            IndexCount = indexCount,
            ViewCount = viewCount,
            TriggerCount = triggerCount,
            ProcedureCount = procedureCount,
            CollectionCount = collectionCount,
            SavedQueryCount = savedQueryCount,
        };
    }

    private static CSharpDbClientOptions BuildShardClientOptions(
        CSharpDbShardDefinition shard,
        CSharpDbShardingOptions options)
    {
        bool applyDirectOptions = ShouldApplyDirectOptions(shard);
        return new CSharpDbClientOptions
        {
            Transport = shard.Transport,
            Endpoint = shard.Endpoint,
            ConnectionString = shard.ConnectionString,
            DataSource = shard.DataSource,
            ApiKey = shard.ApiKey,
            ApiKeyHeaderName = string.IsNullOrWhiteSpace(shard.ApiKeyHeaderName)
                ? "X-CSharpDB-Api-Key"
                : shard.ApiKeyHeaderName,
            DirectDatabaseOptions = applyDirectOptions ? options.DirectDatabaseOptions : null,
            HybridDatabaseOptions = applyDirectOptions ? options.HybridDatabaseOptions : null,
        };
    }

    private static bool ShouldApplyDirectOptions(CSharpDbShardDefinition shard)
    {
        if (shard.Transport is CSharpDbTransport.Http or CSharpDbTransport.Grpc or CSharpDbTransport.NamedPipes)
            return false;

        if (string.IsNullOrWhiteSpace(shard.Endpoint))
            return true;

        return !Uri.TryCreate(shard.Endpoint.Trim(), UriKind.Absolute, out Uri? endpointUri) ||
               endpointUri.Scheme.Equals(Uri.UriSchemeFile, StringComparison.OrdinalIgnoreCase);
    }

    private static string GetConfiguredDataSource(CSharpDbShardDefinition shard)
        => shard.DataSource
           ?? shard.ConnectionString
           ?? shard.Endpoint
           ?? shard.ShardId;

    private ICSharpDbClient GetShardClient(string shardId)
    {
        CSharpDbShardDefinition shard = _map.GetShard(shardId);
        if (!shard.Enabled)
            throw new CSharpDbClientException($"Shard '{shardId}' is disabled.");

        return _clients.TryGetValue(shardId, out ICSharpDbClient? client)
            ? client
            : throw new CSharpDbClientException($"Shard '{shardId}' is not available.");
    }

    private ICSharpDbClient ResolveClient(CSharpDbRouteContext routeContext)
    {
        CSharpDbShardResolution resolution = _map.Resolve(routeContext);
        return GetShardClient(resolution.ShardId);
    }

    private (ICSharpDbClient Client, string InnerTransactionId, string ShardId) ResolveTransactionClient(
        string transactionId,
        CSharpDbRouteContext? currentRoute)
    {
        if (!TryParseTransactionId(transactionId, out int mapVersion, out string? shardId, out string? innerTransactionId))
        {
            throw new CSharpDbClientException(
                "Sharded transaction IDs must use the 'csdbshard:{mapVersion}:{shardId}:{innerTransactionId}' format.");
        }

        if (mapVersion != _map.MapVersion)
        {
            throw new CSharpDbClientException(
                $"Transaction '{transactionId}' was created for shard map version {mapVersion}, but the active map version is {_map.MapVersion}.");
        }

        if (currentRoute is not null)
        {
            CSharpDbShardResolution routeResolution = _map.Resolve(currentRoute);
            if (!string.Equals(routeResolution.ShardId, shardId, StringComparison.OrdinalIgnoreCase))
            {
                throw new CSharpDbClientException(
                    $"Route context resolves to shard '{routeResolution.ShardId}', but transaction '{transactionId}' belongs to shard '{shardId}'.");
            }
        }

        return (GetShardClient(shardId), innerTransactionId, shardId);
    }

    private static string CreateTransactionId(int mapVersion, string shardId, string innerTransactionId)
        => $"{TransactionPrefix}:{mapVersion}:{shardId}:{innerTransactionId}";

    private static bool TryParseTransactionId(
        string transactionId,
        out int mapVersion,
        out string shardId,
        out string innerTransactionId)
    {
        mapVersion = 0;
        shardId = string.Empty;
        innerTransactionId = string.Empty;

        if (string.IsNullOrWhiteSpace(transactionId))
            return false;

        string[] parts = transactionId.Split(':', 4);
        if (parts.Length != 4 ||
            !string.Equals(parts[0], TransactionPrefix, StringComparison.Ordinal) ||
            !int.TryParse(parts[1], System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out mapVersion))
        {
            return false;
        }

        shardId = parts[2];
        innerTransactionId = parts[3];
        return !string.IsNullOrWhiteSpace(shardId) && !string.IsNullOrWhiteSpace(innerTransactionId);
    }

    private CSharpDbRouteContext? GetCurrentRoute()
        => _routeContextAccessor?.Current;

    private sealed class RoutedClient : ICSharpDbClient
    {
        private readonly CSharpDbShardedClient _owner;
        private readonly CSharpDbRouteContext? _fixedRoute;

        public RoutedClient(CSharpDbShardedClient owner, CSharpDbRouteContext? fixedRoute)
        {
            _owner = owner;
            _fixedRoute = fixedRoute;
        }

        public string DataSource
            => _fixedRoute is null
                ? _owner.DataSource
                : _owner.ResolveClient(_fixedRoute).DataSource;

        public Task<DatabaseInfo> GetInfoAsync(CancellationToken ct = default)
            => _fixedRoute is null && _owner.GetCurrentRoute() is null
                ? _owner.GetAggregateInfoAsync(ct)
                : ResolveClient().GetInfoAsync(ct);

        public Task<IReadOnlyList<string>> GetTableNamesAsync(CancellationToken ct = default)
            => ResolveClient().GetTableNamesAsync(ct);

        public Task<TableSchema?> GetTableSchemaAsync(string tableName, CancellationToken ct = default)
            => ResolveClient().GetTableSchemaAsync(tableName, ct);

        public Task<int> GetRowCountAsync(string tableName, CancellationToken ct = default)
            => ResolveClient().GetRowCountAsync(tableName, ct);

        public Task<TableBrowseResult> BrowseTableAsync(string tableName, int page = 1, int pageSize = 50, CancellationToken ct = default)
            => ResolveClient().BrowseTableAsync(tableName, page, pageSize, ct);

        public Task<Dictionary<string, object?>?> GetRowByPkAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default)
            => ResolveClient().GetRowByPkAsync(tableName, pkColumn, pkValue, ct);

        public Task<int> InsertRowAsync(string tableName, Dictionary<string, object?> values, CancellationToken ct = default)
            => ResolveClient().InsertRowAsync(tableName, values, ct);

        public Task<int> UpdateRowAsync(string tableName, string pkColumn, object pkValue, Dictionary<string, object?> values, CancellationToken ct = default)
            => ResolveClient().UpdateRowAsync(tableName, pkColumn, pkValue, values, ct);

        public Task<int> DeleteRowAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default)
            => ResolveClient().DeleteRowAsync(tableName, pkColumn, pkValue, ct);

        public Task DropTableAsync(string tableName, CancellationToken ct = default)
            => ResolveClient().DropTableAsync(tableName, ct);

        public Task RenameTableAsync(string tableName, string newTableName, CancellationToken ct = default)
            => ResolveClient().RenameTableAsync(tableName, newTableName, ct);

        public Task AddColumnAsync(string tableName, string columnName, DbType type, bool notNull, CancellationToken ct = default)
            => ResolveClient().AddColumnAsync(tableName, columnName, type, notNull, ct);

        public Task AddColumnAsync(string tableName, string columnName, DbType type, bool notNull, string? collation, CancellationToken ct = default)
            => ResolveClient().AddColumnAsync(tableName, columnName, type, notNull, collation, ct);

        public Task DropColumnAsync(string tableName, string columnName, CancellationToken ct = default)
            => ResolveClient().DropColumnAsync(tableName, columnName, ct);

        public Task RenameColumnAsync(string tableName, string oldColumnName, string newColumnName, CancellationToken ct = default)
            => ResolveClient().RenameColumnAsync(tableName, oldColumnName, newColumnName, ct);

        public Task<IReadOnlyList<IndexSchema>> GetIndexesAsync(CancellationToken ct = default)
            => ResolveClient().GetIndexesAsync(ct);

        public Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default)
            => ResolveClient().CreateIndexAsync(indexName, tableName, columnName, isUnique, ct);

        public Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, string? collation, CancellationToken ct = default)
            => ResolveClient().CreateIndexAsync(indexName, tableName, columnName, isUnique, collation, ct);

        public Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default)
            => ResolveClient().UpdateIndexAsync(existingIndexName, newIndexName, tableName, columnName, isUnique, ct);

        public Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique, string? collation, CancellationToken ct = default)
            => ResolveClient().UpdateIndexAsync(existingIndexName, newIndexName, tableName, columnName, isUnique, collation, ct);

        public Task DropIndexAsync(string indexName, CancellationToken ct = default)
            => ResolveClient().DropIndexAsync(indexName, ct);

        public Task<IReadOnlyList<string>> GetViewNamesAsync(CancellationToken ct = default)
            => ResolveClient().GetViewNamesAsync(ct);

        public Task<IReadOnlyList<ViewDefinition>> GetViewsAsync(CancellationToken ct = default)
            => ResolveClient().GetViewsAsync(ct);

        public Task<ViewDefinition?> GetViewAsync(string viewName, CancellationToken ct = default)
            => ResolveClient().GetViewAsync(viewName, ct);

        public Task<string?> GetViewSqlAsync(string viewName, CancellationToken ct = default)
            => ResolveClient().GetViewSqlAsync(viewName, ct);

        public Task<ViewBrowseResult> BrowseViewAsync(string viewName, int page = 1, int pageSize = 50, CancellationToken ct = default)
            => ResolveClient().BrowseViewAsync(viewName, page, pageSize, ct);

        public Task CreateViewAsync(string viewName, string selectSql, CancellationToken ct = default)
            => ResolveClient().CreateViewAsync(viewName, selectSql, ct);

        public Task UpdateViewAsync(string existingViewName, string newViewName, string selectSql, CancellationToken ct = default)
            => ResolveClient().UpdateViewAsync(existingViewName, newViewName, selectSql, ct);

        public Task DropViewAsync(string viewName, CancellationToken ct = default)
            => ResolveClient().DropViewAsync(viewName, ct);

        public Task<IReadOnlyList<TriggerSchema>> GetTriggersAsync(CancellationToken ct = default)
            => ResolveClient().GetTriggersAsync(ct);

        public Task CreateTriggerAsync(string triggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default)
            => ResolveClient().CreateTriggerAsync(triggerName, tableName, timing, triggerEvent, bodySql, ct);

        public Task UpdateTriggerAsync(string existingTriggerName, string newTriggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default)
            => ResolveClient().UpdateTriggerAsync(existingTriggerName, newTriggerName, tableName, timing, triggerEvent, bodySql, ct);

        public Task DropTriggerAsync(string triggerName, CancellationToken ct = default)
            => ResolveClient().DropTriggerAsync(triggerName, ct);

        public Task<IReadOnlyList<SavedQueryDefinition>> GetSavedQueriesAsync(CancellationToken ct = default)
            => ResolveClient().GetSavedQueriesAsync(ct);

        public Task<SavedQueryDefinition?> GetSavedQueryAsync(string name, CancellationToken ct = default)
            => ResolveClient().GetSavedQueryAsync(name, ct);

        public Task<SavedQueryDefinition> UpsertSavedQueryAsync(string name, string sqlText, CancellationToken ct = default)
            => ResolveClient().UpsertSavedQueryAsync(name, sqlText, ct);

        public Task DeleteSavedQueryAsync(string name, CancellationToken ct = default)
            => ResolveClient().DeleteSavedQueryAsync(name, ct);

        public Task<IReadOnlyList<ProcedureDefinition>> GetProceduresAsync(bool includeDisabled = true, CancellationToken ct = default)
            => ResolveClient().GetProceduresAsync(includeDisabled, ct);

        public Task<ProcedureDefinition?> GetProcedureAsync(string name, CancellationToken ct = default)
            => ResolveClient().GetProcedureAsync(name, ct);

        public Task CreateProcedureAsync(ProcedureDefinition definition, CancellationToken ct = default)
            => ResolveClient().CreateProcedureAsync(definition, ct);

        public Task UpdateProcedureAsync(string existingName, ProcedureDefinition definition, CancellationToken ct = default)
            => ResolveClient().UpdateProcedureAsync(existingName, definition, ct);

        public Task DeleteProcedureAsync(string name, CancellationToken ct = default)
            => ResolveClient().DeleteProcedureAsync(name, ct);

        public Task<ProcedureExecutionResult> ExecuteProcedureAsync(string name, IReadOnlyDictionary<string, object?> args, CancellationToken ct = default)
            => ResolveClient().ExecuteProcedureAsync(name, args, ct);

        public Task<SqlExecutionResult> ExecuteSqlAsync(string sql, CancellationToken ct = default)
            => ResolveClient().ExecuteSqlAsync(sql, ct);

        public async Task<TransactionSessionInfo> BeginTransactionAsync(CancellationToken ct = default)
        {
            CSharpDbRouteContext routeContext = GetRequiredRoute();
            CSharpDbShardResolution resolution = _owner.ResolveRoute(routeContext);
            TransactionSessionInfo inner = await _owner.GetShardClient(resolution.ShardId)
                .BeginTransactionAsync(ct)
                .ConfigureAwait(false);

            return new TransactionSessionInfo
            {
                TransactionId = CreateTransactionId(resolution.MapVersion, resolution.ShardId, inner.TransactionId),
                ExpiresAtUtc = inner.ExpiresAtUtc,
            };
        }

        public Task<SqlExecutionResult> ExecuteInTransactionAsync(string transactionId, string sql, CancellationToken ct = default)
        {
            CSharpDbRouteContext? route = GetOptionalRoute();
            var (client, innerTransactionId, _) = _owner.ResolveTransactionClient(transactionId, route);
            return client.ExecuteInTransactionAsync(innerTransactionId, sql, ct);
        }

        public Task CommitTransactionAsync(string transactionId, CancellationToken ct = default)
        {
            CSharpDbRouteContext? route = GetOptionalRoute();
            var (client, innerTransactionId, _) = _owner.ResolveTransactionClient(transactionId, route);
            return client.CommitTransactionAsync(innerTransactionId, ct);
        }

        public Task RollbackTransactionAsync(string transactionId, CancellationToken ct = default)
        {
            CSharpDbRouteContext? route = GetOptionalRoute();
            var (client, innerTransactionId, _) = _owner.ResolveTransactionClient(transactionId, route);
            return client.RollbackTransactionAsync(innerTransactionId, ct);
        }

        public Task<IReadOnlyList<string>> GetCollectionNamesAsync(CancellationToken ct = default)
            => ResolveClient().GetCollectionNamesAsync(ct);

        public Task<int> GetCollectionCountAsync(string collectionName, CancellationToken ct = default)
            => ResolveClient().GetCollectionCountAsync(collectionName, ct);

        public Task<CollectionBrowseResult> BrowseCollectionAsync(string collectionName, int page = 1, int pageSize = 50, CancellationToken ct = default)
            => ResolveClient().BrowseCollectionAsync(collectionName, page, pageSize, ct);

        public Task<System.Text.Json.JsonElement?> GetDocumentAsync(string collectionName, string key, CancellationToken ct = default)
            => ResolveClient().GetDocumentAsync(collectionName, key, ct);

        public Task PutDocumentAsync(string collectionName, string key, System.Text.Json.JsonElement document, CancellationToken ct = default)
            => ResolveClient().PutDocumentAsync(collectionName, key, document, ct);

        public Task<bool> DeleteDocumentAsync(string collectionName, string key, CancellationToken ct = default)
            => ResolveClient().DeleteDocumentAsync(collectionName, key, ct);

        public Task DropCollectionAsync(string collectionName, CancellationToken ct = default)
            => ResolveClient().DropCollectionAsync(collectionName, ct);

        public Task CheckpointAsync(CancellationToken ct = default)
            => ResolveClient().CheckpointAsync(ct);

        public Task<BackupResult> BackupAsync(BackupRequest request, CancellationToken ct = default)
            => ResolveClient().BackupAsync(request, ct);

        public Task<RestoreResult> RestoreAsync(RestoreRequest request, CancellationToken ct = default)
            => ResolveClient().RestoreAsync(request, ct);

        public Task<ForeignKeyMigrationResult> MigrateForeignKeysAsync(ForeignKeyMigrationRequest request, CancellationToken ct = default)
            => ResolveClient().MigrateForeignKeysAsync(request, ct);

        public Task<DatabaseMaintenanceReport> GetMaintenanceReportAsync(CancellationToken ct = default)
            => ResolveClient().GetMaintenanceReportAsync(ct);

        public Task<ReindexResult> ReindexAsync(ReindexRequest request, CancellationToken ct = default)
            => ResolveClient().ReindexAsync(request, ct);

        public Task<VacuumResult> VacuumAsync(CancellationToken ct = default)
            => ResolveClient().VacuumAsync(ct);

        public Task<DatabaseInspectReport> InspectStorageAsync(string? databasePath = null, bool includePages = false, CancellationToken ct = default)
            => ResolveClient().InspectStorageAsync(databasePath, includePages, ct);

        public Task<WalInspectReport> CheckWalAsync(string? databasePath = null, CancellationToken ct = default)
            => ResolveClient().CheckWalAsync(databasePath, ct);

        public Task<PageInspectReport> InspectPageAsync(uint pageId, bool includeHex = false, string? databasePath = null, CancellationToken ct = default)
            => ResolveClient().InspectPageAsync(pageId, includeHex, databasePath, ct);

        public Task<IndexInspectReport> CheckIndexesAsync(string? databasePath = null, string? indexName = null, int? sampleSize = null, CancellationToken ct = default)
            => ResolveClient().CheckIndexesAsync(databasePath, indexName, sampleSize, ct);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private ICSharpDbClient ResolveClient()
            => _owner.ResolveClient(GetRequiredRoute());

        private CSharpDbRouteContext GetRequiredRoute()
            => GetOptionalRoute()
               ?? throw new CSharpDbClientException(
                   "A CSharpDB route context is required for sharded operations. Supply X-CSharpDB-Keyspace and X-CSharpDB-Shard-Key for remote calls or use CSharpDbShardedClient.ForRoute locally.");

        private CSharpDbRouteContext? GetOptionalRoute()
            => _fixedRoute ?? _owner.GetCurrentRoute();
    }

    private sealed class CSharpDbShardCatalogStore
    {
        private static readonly JsonSerializerOptions s_jsonOptions = CreateJsonOptions();

        private readonly CSharpDbShardCatalogOptions _options;
        private readonly string _path;

        private CSharpDbShardCatalogStore(CSharpDbShardCatalogOptions options, string path)
        {
            _options = options;
            _path = path;
        }

        public static CSharpDbShardingOptions ResolveEffectiveOptions(CSharpDbShardingOptions configuredOptions)
        {
            ArgumentNullException.ThrowIfNull(configuredOptions);

            if (configuredOptions.Catalog?.Enabled != true)
                return CloneOptions(configuredOptions, includeRuntimeOptions: true);

            string path = NormalizeCatalogPath(configuredOptions.Catalog);
            if (!File.Exists(path))
                return CloneOptions(configuredOptions, includeRuntimeOptions: true);

            CSharpDbShardCatalogDocument document = ReadDocument(path);
            CSharpDbShardingOptions effective = CloneOptions(document.ActiveMap, includeRuntimeOptions: false);
            effective.Enabled = configuredOptions.Enabled;
            effective.Catalog = CloneCatalogOptions(configuredOptions.Catalog);
            effective.DirectDatabaseOptions = configuredOptions.DirectDatabaseOptions;
            effective.HybridDatabaseOptions = configuredOptions.HybridDatabaseOptions;
            return effective;
        }

        public static CSharpDbShardCatalogStore? Create(
            CSharpDbShardingOptions configuredOptions,
            CSharpDbShardingOptions effectiveOptions)
        {
            ArgumentNullException.ThrowIfNull(configuredOptions);
            ArgumentNullException.ThrowIfNull(effectiveOptions);

            if (configuredOptions.Catalog?.Enabled != true)
                return null;

            string path = NormalizeCatalogPath(configuredOptions.Catalog);
            return new CSharpDbShardCatalogStore(CloneCatalogOptions(configuredOptions.Catalog), path);
        }

        public async Task<CSharpDbShardCatalogState> GetStateAsync(
            CSharpDbShardMapSnapshot activeMap,
            CancellationToken ct)
        {
            CSharpDbShardCatalogDocument? document = File.Exists(_path)
                ? await ReadDocumentAsync(_path, ct).ConfigureAwait(false)
                : null;

            CSharpDbShardMapSnapshot? pendingMap = null;
            if (document?.ActiveMap is not null)
            {
                CSharpDbShardMapSnapshot catalogMap = CreateShardMapSnapshot(document.ActiveMap);
                if (catalogMap.MapVersion != activeMap.MapVersion)
                    pendingMap = catalogMap;
            }

            return new CSharpDbShardCatalogState
            {
                Source = _path,
                IsCatalogEnabled = true,
                IsWritable = _options.AllowWrites,
                ActiveMap = activeMap,
                PendingMap = pendingMap,
                History = document?.History ?? [],
            };
        }

        public async Task<CSharpDbShardCatalogApplyResult> ApplyAsync(
            CSharpDbShardMapSnapshot currentMap,
            CSharpDbShardCatalogUpdateRequest request,
            CancellationToken ct)
        {
            CSharpDbShardCatalogValidationResult validation = ValidateCatalogUpdate(currentMap, request);
            if (!_options.AllowWrites)
            {
                validation.Issues.Add(new CSharpDbShardCatalogIssue
                {
                    Severity = CSharpDbShardCatalogIssueSeverity.Error,
                    Code = "catalog-read-only",
                    Message = "Shard catalog writes are disabled by configuration.",
                });
                validation = CreateValidationResult(validation.Issues, validation.Preview, validation.RequiresDataMigration);
            }

            if (!validation.IsValid || validation.Preview is null)
            {
                return new CSharpDbShardCatalogApplyResult
                {
                    Applied = false,
                    RequiresRestart = false,
                    Message = "Shard catalog update was rejected by validation.",
                    Validation = validation,
                    PendingMap = validation.Preview,
                };
            }

            CSharpDbShardCatalogDocument document = File.Exists(_path)
                ? await ReadDocumentAsync(_path, ct).ConfigureAwait(false)
                : new CSharpDbShardCatalogDocument();

            document.ActiveMap = CloneOptions(request.Options, includeRuntimeOptions: false);
            document.History.Add(new CSharpDbShardCatalogHistoryEntry
            {
                AppliedUtc = DateTimeOffset.UtcNow,
                MapVersion = validation.Preview.MapVersion,
                Operator = request.Operator,
                Comment = request.Comment,
                MetadataOnlyOwnershipChange = validation.RequiresDataMigration && request.AllowMetadataOnlyOwnershipChange,
            });

            string? directory = Path.GetDirectoryName(_path);
            if (!string.IsNullOrWhiteSpace(directory))
                Directory.CreateDirectory(directory);

            await using (FileStream stream = File.Create(_path))
            {
                await JsonSerializer.SerializeAsync(stream, document, s_jsonOptions, ct).ConfigureAwait(false);
            }

            return new CSharpDbShardCatalogApplyResult
            {
                Applied = true,
                RequiresRestart = true,
                Message = "Shard catalog update was written. Restart or recreate the sharded client to activate the new map.",
                Validation = validation,
                PendingMap = validation.Preview,
            };
        }

        private static string NormalizeCatalogPath(CSharpDbShardCatalogOptions options)
        {
            if (string.IsNullOrWhiteSpace(options.Path))
                throw new CSharpDbClientConfigurationException("CSharpDB shard catalog mode requires Catalog:Path.");

            return Path.GetFullPath(options.Path.Trim());
        }

        private static CSharpDbShardCatalogDocument ReadDocument(string path)
        {
            using FileStream stream = File.OpenRead(path);
            return JsonSerializer.Deserialize<CSharpDbShardCatalogDocument>(stream, s_jsonOptions)
                   ?? throw new CSharpDbClientConfigurationException($"Shard catalog file '{path}' is empty.");
        }

        private static async Task<CSharpDbShardCatalogDocument> ReadDocumentAsync(string path, CancellationToken ct)
        {
            await using FileStream stream = File.OpenRead(path);
            return await JsonSerializer.DeserializeAsync<CSharpDbShardCatalogDocument>(stream, s_jsonOptions, ct).ConfigureAwait(false)
                   ?? throw new CSharpDbClientConfigurationException($"Shard catalog file '{path}' is empty.");
        }

        private static CSharpDbShardingOptions CloneOptions(
            CSharpDbShardingOptions options,
            bool includeRuntimeOptions)
            => new()
            {
                Enabled = options.Enabled,
                Keyspace = options.Keyspace,
                MapVersion = options.MapVersion,
                VirtualBucketCount = options.VirtualBucketCount,
                Shards = options.Shards.Select(CloneShard).ToArray(),
                BucketRanges = options.BucketRanges.Select(CloneBucketRange).ToArray(),
                ExactKeyPins = new Dictionary<string, string>(options.ExactKeyPins ?? [], StringComparer.Ordinal),
                Directories = options.Directories.Select(CloneDirectory).ToArray(),
                DirectoryEntries = options.DirectoryEntries.Select(CloneDirectoryEntry).ToArray(),
                Catalog = includeRuntimeOptions ? CloneCatalogOptions(options.Catalog) : new CSharpDbShardCatalogOptions(),
                DirectDatabaseOptions = includeRuntimeOptions ? options.DirectDatabaseOptions : null,
                HybridDatabaseOptions = includeRuntimeOptions ? options.HybridDatabaseOptions : null,
            };

        private static CSharpDbShardDefinition CloneShard(CSharpDbShardDefinition shard)
            => new()
            {
                ShardId = shard.ShardId,
                Enabled = shard.Enabled,
                Transport = shard.Transport,
                Endpoint = shard.Endpoint,
                ConnectionString = shard.ConnectionString,
                DataSource = shard.DataSource,
                ApiKey = shard.ApiKey,
                ApiKeyHeaderName = shard.ApiKeyHeaderName,
            };

        private static CSharpDbShardBucketRange CloneBucketRange(CSharpDbShardBucketRange range)
            => new()
            {
                StartBucketInclusive = range.StartBucketInclusive,
                EndBucketExclusive = range.EndBucketExclusive,
                ShardId = range.ShardId,
            };

        private static CSharpDbShardDirectoryDefinition CloneDirectory(CSharpDbShardDirectoryDefinition directory)
            => new()
            {
                DirectoryName = directory.DirectoryName,
                TargetKeyspace = directory.TargetKeyspace,
                Description = directory.Description,
                ReadOnly = directory.ReadOnly,
                EntryCount = directory.EntryCount,
            };

        private static CSharpDbShardDirectoryEntry CloneDirectoryEntry(CSharpDbShardDirectoryEntry entry)
            => new()
            {
                DirectoryName = entry.DirectoryName,
                LookupKey = entry.LookupKey,
                TargetKeyspace = entry.TargetKeyspace,
                RouteKey = entry.RouteKey,
                ShardId = entry.ShardId,
                MapVersion = entry.MapVersion,
                State = entry.State,
            };

        private static CSharpDbShardCatalogOptions CloneCatalogOptions(CSharpDbShardCatalogOptions? options)
            => new()
            {
                Enabled = options?.Enabled ?? false,
                Path = options?.Path,
                AllowWrites = options?.AllowWrites ?? true,
            };

        private static JsonSerializerOptions CreateJsonOptions()
        {
            var options = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = true,
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            };
            options.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase));
            return options;
        }

        private sealed class CSharpDbShardCatalogDocument
        {
            public int FormatVersion { get; set; } = 1;
            public CSharpDbShardingOptions ActiveMap { get; set; } = new()
            {
                Enabled = true,
                Keyspace = "default",
                Shards = [],
            };
            public List<CSharpDbShardCatalogHistoryEntry> History { get; set; } = [];
        }
    }

    private sealed class CSharpDbShardMap
    {
        private readonly Dictionary<string, CSharpDbShardDefinition> _shards;
        private readonly string[] _bucketOwners;
        private readonly Dictionary<string, string> _exactKeyPins;
        private readonly IReadOnlyList<CSharpDbShardDirectoryDefinition> _directories;
        private readonly IReadOnlyList<CSharpDbShardDirectoryEntry> _directoryEntries;

        private CSharpDbShardMap(
            string keyspace,
            int mapVersion,
            int virtualBucketCount,
            IReadOnlyList<CSharpDbShardDefinition> shards,
            Dictionary<string, CSharpDbShardDefinition> shardMap,
            string[] bucketOwners,
            Dictionary<string, string> exactKeyPins,
            IReadOnlyList<CSharpDbShardDirectoryDefinition> directories,
            IReadOnlyList<CSharpDbShardDirectoryEntry> directoryEntries)
        {
            Keyspace = keyspace;
            MapVersion = mapVersion;
            VirtualBucketCount = virtualBucketCount;
            Shards = shards;
            _shards = shardMap;
            _bucketOwners = bucketOwners;
            _exactKeyPins = exactKeyPins;
            _directories = directories;
            _directoryEntries = directoryEntries;
        }

        public string Keyspace { get; }
        public int MapVersion { get; }
        public int VirtualBucketCount { get; }
        public IReadOnlyList<CSharpDbShardDefinition> Shards { get; }

        public static CSharpDbShardMap Create(CSharpDbShardingOptions options)
        {
            string keyspace = NormalizeNonEmpty(options.Keyspace, nameof(options.Keyspace));
            if (options.MapVersion <= 0)
                throw new CSharpDbClientConfigurationException("CSharpDB sharding MapVersion must be greater than 0.");
            if (options.VirtualBucketCount <= 0)
                throw new CSharpDbClientConfigurationException("CSharpDB sharding VirtualBucketCount must be greater than 0.");
            if (options.Shards.Length == 0)
                throw new CSharpDbClientConfigurationException("CSharpDB sharding requires at least one shard.");

            var shardMap = new Dictionary<string, CSharpDbShardDefinition>(StringComparer.OrdinalIgnoreCase);
            var normalizedShards = new List<CSharpDbShardDefinition>(options.Shards.Length);
            foreach (CSharpDbShardDefinition shard in options.Shards)
            {
                string shardId = NormalizeShardId(shard.ShardId);
                if (shardMap.ContainsKey(shardId))
                    throw new CSharpDbClientConfigurationException($"Duplicate CSharpDB shard id '{shardId}'.");

                var normalized = new CSharpDbShardDefinition
                {
                    ShardId = shardId,
                    Enabled = shard.Enabled,
                    Transport = shard.Transport,
                    Endpoint = NormalizeOptional(shard.Endpoint),
                    ConnectionString = NormalizeOptional(shard.ConnectionString),
                    DataSource = NormalizeOptional(shard.DataSource),
                    ApiKey = NormalizeOptional(shard.ApiKey),
                    ApiKeyHeaderName = NormalizeOptional(shard.ApiKeyHeaderName),
                };
                ValidateShardTarget(normalized);
                shardMap.Add(shardId, normalized);
                normalizedShards.Add(normalized);
            }

            if (!normalizedShards.Any(shard => shard.Enabled))
                throw new CSharpDbClientConfigurationException("CSharpDB sharding requires at least one enabled shard.");

            string[] bucketOwners = BuildBucketOwners(options, normalizedShards, shardMap);
            var exactKeyPins = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (KeyValuePair<string, string> pin in options.ExactKeyPins ?? [])
            {
                string key = NormalizeNonEmpty(pin.Key, "ExactKeyPins key");
                string shardId = NormalizeShardId(pin.Value);
                if (!shardMap.ContainsKey(shardId))
                    throw new CSharpDbClientConfigurationException($"Exact route-key pin '{key}' references unknown shard '{shardId}'.");
                exactKeyPins[key] = shardId;
            }

            List<CSharpDbShardDirectoryDefinition> directories = NormalizeDirectories(options.Directories);

            var map = new CSharpDbShardMap(
                keyspace,
                options.MapVersion,
                options.VirtualBucketCount,
                normalizedShards,
                shardMap,
                bucketOwners,
                exactKeyPins,
                directories,
                []);

            List<CSharpDbShardDirectoryEntry> directoryEntries = map.NormalizeDirectoryEntries(options.DirectoryEntries, directories);
            return new CSharpDbShardMap(
                keyspace,
                options.MapVersion,
                options.VirtualBucketCount,
                normalizedShards,
                shardMap,
                bucketOwners,
                exactKeyPins,
                directories,
                directoryEntries);
        }

        public CSharpDbShardResolution Resolve(CSharpDbRouteContext routeContext)
        {
            var (routeKeyspace, routeKey) = NormalizeRoute(routeContext);
            if (!string.Equals(routeKeyspace, Keyspace, StringComparison.OrdinalIgnoreCase))
            {
                throw new CSharpDbClientException(
                    $"Route keyspace '{routeKeyspace}' does not match configured keyspace '{Keyspace}'.");
            }

            ulong token = ComputeRouteToken(new CSharpDbRouteContext { Keyspace = Keyspace, Key = routeKey });
            int bucket = (int)(token % (ulong)VirtualBucketCount);
            string shardId = _exactKeyPins.TryGetValue(routeKey, out string? pinnedShardId)
                ? pinnedShardId
                : _bucketOwners[bucket];

            CSharpDbShardDefinition shard = GetShard(shardId);
            if (!shard.Enabled)
                throw new CSharpDbClientException($"Route key '{routeKey}' resolves to disabled shard '{shardId}'.");

            return new CSharpDbShardResolution
            {
                Keyspace = Keyspace,
                Key = routeKey,
                Token = token,
                Bucket = bucket,
                ShardId = shardId,
                MapVersion = MapVersion,
            };
        }

        public CSharpDbShardDefinition GetShard(string shardId)
            => _shards.TryGetValue(NormalizeShardId(shardId), out CSharpDbShardDefinition? shard)
                ? shard
                : throw new CSharpDbClientException($"Shard '{shardId}' is not configured.");

        public CSharpDbShardMapSnapshot ToSnapshot()
            => new()
            {
                Keyspace = Keyspace,
                MapVersion = MapVersion,
                VirtualBucketCount = VirtualBucketCount,
                Shards = Shards.Select(ToShardSnapshot).ToList(),
                BucketRanges = BuildBucketRangeSnapshot(),
                ExactKeyPins = new Dictionary<string, string>(_exactKeyPins, StringComparer.Ordinal),
                Directories = _directories.Select(directory => new CSharpDbShardDirectoryDefinition
                {
                    DirectoryName = directory.DirectoryName,
                    TargetKeyspace = directory.TargetKeyspace,
                    Description = directory.Description,
                    ReadOnly = directory.ReadOnly,
                    EntryCount = _directoryEntries.Count(entry =>
                        string.Equals(entry.DirectoryName, directory.DirectoryName, StringComparison.OrdinalIgnoreCase)),
                }).ToList(),
            };

        public static (string Keyspace, string Key) NormalizeRoute(CSharpDbRouteContext routeContext)
        {
            ArgumentNullException.ThrowIfNull(routeContext);
            return (
                NormalizeNonEmpty(routeContext.Keyspace, nameof(routeContext.Keyspace)),
                NormalizeNonEmpty(routeContext.Key, nameof(routeContext.Key)));
        }

        public static string NormalizeShardId(string shardId)
        {
            string normalized = NormalizeNonEmpty(shardId, nameof(shardId));
            if (normalized.Contains(':', StringComparison.Ordinal))
                throw new CSharpDbClientConfigurationException("Shard ids cannot contain ':'.");

            foreach (char ch in normalized)
            {
                if (char.IsAsciiLetterOrDigit(ch) || ch is '_' or '-' or '.')
                    continue;

                throw new CSharpDbClientConfigurationException(
                    "Shard ids can contain only ASCII letters, digits, '_', '-', and '.'.");
            }

            return normalized;
        }

        private static CSharpDbShardDefinitionSnapshot ToShardSnapshot(CSharpDbShardDefinition shard)
            => new()
            {
                ShardId = shard.ShardId,
                Enabled = shard.Enabled,
                Transport = shard.Transport,
                Endpoint = shard.Endpoint,
                DataSource = shard.DataSource,
                HasConnectionString = !string.IsNullOrWhiteSpace(shard.ConnectionString),
                HasApiKey = !string.IsNullOrWhiteSpace(shard.ApiKey),
                ApiKeyHeaderName = shard.ApiKeyHeaderName,
            };

        private static List<CSharpDbShardDirectoryDefinition> NormalizeDirectories(
            IReadOnlyList<CSharpDbShardDirectoryDefinition>? directories)
        {
            var normalized = new List<CSharpDbShardDirectoryDefinition>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (CSharpDbShardDirectoryDefinition directory in directories ?? [])
            {
                string directoryName = NormalizeNonEmpty(directory.DirectoryName, nameof(directory.DirectoryName));
                string targetKeyspace = NormalizeNonEmpty(directory.TargetKeyspace, nameof(directory.TargetKeyspace));
                if (!seen.Add(directoryName))
                    throw new CSharpDbClientConfigurationException($"Duplicate shard-directory name '{directoryName}'.");

                normalized.Add(new CSharpDbShardDirectoryDefinition
                {
                    DirectoryName = directoryName,
                    TargetKeyspace = targetKeyspace,
                    Description = NormalizeOptional(directory.Description),
                    ReadOnly = directory.ReadOnly,
                    EntryCount = 0,
                });
            }

            return normalized;
        }

        private List<CSharpDbShardDirectoryEntry> NormalizeDirectoryEntries(
            IReadOnlyList<CSharpDbShardDirectoryEntry>? entries,
            IReadOnlyList<CSharpDbShardDirectoryDefinition> directories)
        {
            var normalized = new List<CSharpDbShardDirectoryEntry>();
            var directoryMap = directories.ToDictionary(
                directory => directory.DirectoryName,
                StringComparer.OrdinalIgnoreCase);
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (CSharpDbShardDirectoryEntry entry in entries ?? [])
            {
                string directoryName = NormalizeNonEmpty(entry.DirectoryName, nameof(entry.DirectoryName));
                string lookupKey = NormalizeNonEmpty(entry.LookupKey, nameof(entry.LookupKey));
                string targetKeyspace = NormalizeNonEmpty(entry.TargetKeyspace, nameof(entry.TargetKeyspace));
                string routeKey = NormalizeNonEmpty(entry.RouteKey, nameof(entry.RouteKey));
                string shardId = NormalizeShardId(entry.ShardId);
                string state = NormalizeNonEmpty(entry.State, nameof(entry.State));

                if (!directoryMap.TryGetValue(directoryName, out CSharpDbShardDirectoryDefinition? directory))
                    throw new CSharpDbClientConfigurationException($"Shard-directory entry '{directoryName}:{lookupKey}' references unknown directory '{directoryName}'.");
                if (!string.Equals(directory.TargetKeyspace, targetKeyspace, StringComparison.OrdinalIgnoreCase))
                    throw new CSharpDbClientConfigurationException($"Shard-directory entry '{directoryName}:{lookupKey}' target keyspace '{targetKeyspace}' does not match directory keyspace '{directory.TargetKeyspace}'.");
                if (!IsValidDirectoryEntryState(state))
                    throw new CSharpDbClientConfigurationException($"Shard-directory entry '{directoryName}:{lookupKey}' has invalid state '{state}'.");
                if (entry.MapVersion <= 0)
                    throw new CSharpDbClientConfigurationException($"Shard-directory entry '{directoryName}:{lookupKey}' requires MapVersion greater than 0.");
                if (entry.MapVersion > MapVersion)
                    throw new CSharpDbClientConfigurationException($"Shard-directory entry '{directoryName}:{lookupKey}' references future map version {entry.MapVersion}.");
                if (!seen.Add($"{directoryName}\0{lookupKey}"))
                    throw new CSharpDbClientConfigurationException($"Duplicate shard-directory entry '{directoryName}:{lookupKey}'.");

                CSharpDbShardResolution resolution = Resolve(new CSharpDbRouteContext
                {
                    Keyspace = targetKeyspace,
                    Key = routeKey,
                });
                if (!string.Equals(resolution.ShardId, shardId, StringComparison.OrdinalIgnoreCase))
                {
                    throw new CSharpDbClientConfigurationException(
                        $"Shard-directory entry '{directoryName}:{lookupKey}' points to shard '{shardId}', but route key '{routeKey}' resolves to shard '{resolution.ShardId}'.");
                }

                normalized.Add(new CSharpDbShardDirectoryEntry
                {
                    DirectoryName = directoryName,
                    LookupKey = lookupKey,
                    TargetKeyspace = targetKeyspace,
                    RouteKey = routeKey,
                    ShardId = shardId,
                    MapVersion = entry.MapVersion,
                    State = state,
                });
            }

            return normalized;
        }

        private static bool IsValidDirectoryEntryState(string state)
            => state is "Reserved" or "Active" or "Moving" or "Disabled" or "Deleted";

        private List<CSharpDbShardBucketRange> BuildBucketRangeSnapshot()
        {
            var ranges = new List<CSharpDbShardBucketRange>();
            int start = 0;
            string current = _bucketOwners[0];
            for (int bucket = 1; bucket < _bucketOwners.Length; bucket++)
            {
                if (string.Equals(_bucketOwners[bucket], current, StringComparison.OrdinalIgnoreCase))
                    continue;

                ranges.Add(new CSharpDbShardBucketRange
                {
                    StartBucketInclusive = start,
                    EndBucketExclusive = bucket,
                    ShardId = current,
                });
                start = bucket;
                current = _bucketOwners[bucket];
            }

            ranges.Add(new CSharpDbShardBucketRange
            {
                StartBucketInclusive = start,
                EndBucketExclusive = _bucketOwners.Length,
                ShardId = current,
            });

            return ranges;
        }

        private static string[] BuildBucketOwners(
            CSharpDbShardingOptions options,
            IReadOnlyList<CSharpDbShardDefinition> normalizedShards,
            IReadOnlyDictionary<string, CSharpDbShardDefinition> shardMap)
        {
            if (options.BucketRanges.Length == 0)
            {
                if (normalizedShards.Count != 1)
                {
                    throw new CSharpDbClientConfigurationException(
                        "CSharpDB sharding requires explicit BucketRanges when more than one shard is configured.");
                }

                return Enumerable.Repeat(normalizedShards[0].ShardId, options.VirtualBucketCount).ToArray();
            }

            var bucketOwners = new string?[options.VirtualBucketCount];
            foreach (CSharpDbShardBucketRange range in options.BucketRanges)
            {
                string shardId = NormalizeShardId(range.ShardId);
                if (!shardMap.ContainsKey(shardId))
                    throw new CSharpDbClientConfigurationException($"Bucket range references unknown shard '{shardId}'.");
                if (range.StartBucketInclusive < 0 ||
                    range.EndBucketExclusive > options.VirtualBucketCount ||
                    range.StartBucketInclusive >= range.EndBucketExclusive)
                {
                    throw new CSharpDbClientConfigurationException(
                        $"Invalid bucket range [{range.StartBucketInclusive}, {range.EndBucketExclusive}) for shard '{shardId}'.");
                }

                for (int bucket = range.StartBucketInclusive; bucket < range.EndBucketExclusive; bucket++)
                {
                    if (bucketOwners[bucket] is not null)
                        throw new CSharpDbClientConfigurationException($"Bucket {bucket} is assigned to more than one shard.");

                    bucketOwners[bucket] = shardId;
                }
            }

            for (int bucket = 0; bucket < bucketOwners.Length; bucket++)
            {
                if (bucketOwners[bucket] is null)
                    throw new CSharpDbClientConfigurationException($"Bucket {bucket} is not assigned to any shard.");
            }

            return bucketOwners!;
        }

        private static void ValidateShardTarget(CSharpDbShardDefinition shard)
        {
            int targetCount = 0;
            if (!string.IsNullOrWhiteSpace(shard.Endpoint)) targetCount++;
            if (!string.IsNullOrWhiteSpace(shard.ConnectionString)) targetCount++;
            if (!string.IsNullOrWhiteSpace(shard.DataSource)) targetCount++;

            if (targetCount == 0)
                throw new CSharpDbClientConfigurationException($"Shard '{shard.ShardId}' requires Endpoint, ConnectionString, or DataSource.");
            if (targetCount > 1)
                throw new CSharpDbClientConfigurationException($"Shard '{shard.ShardId}' can use only one of Endpoint, ConnectionString, or DataSource.");
        }

        private static string NormalizeNonEmpty(string? value, string name)
        {
            if (string.IsNullOrWhiteSpace(value))
                throw new CSharpDbClientConfigurationException($"{name} is required.");

            return value.Trim();
        }

        private static string? NormalizeOptional(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }
}
