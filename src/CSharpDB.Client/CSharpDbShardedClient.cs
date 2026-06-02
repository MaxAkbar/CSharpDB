using System.Collections.Concurrent;
using System.Buffers.Binary;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Client.Models;
using CSharpDB.Sql;
using CSharpDB.Storage.Diagnostics;

namespace CSharpDB.Client;

public sealed class CSharpDbShardedClient : ICSharpDbClient, ICSharpDbShardAdminClient
{
    private const string TransactionPrefix = "csdbshard";

    private readonly CSharpDbShardMap _map;
    private readonly Dictionary<string, ICSharpDbClient> _clients;
    private readonly ICSharpDbRouteContextAccessor? _routeContextAccessor;
    private readonly CSharpDbShardCatalogStore? _catalogStore;
    private readonly CSharpDbShardingOptions _effectiveOptions;
    private readonly ConcurrentDictionary<string, byte> _writeFences = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, BucketRangeFence> _bucketRangeFences = new(StringComparer.Ordinal);
    private readonly RoutedClient _requestRoutedClient;

    private CSharpDbShardedClient(
        CSharpDbShardMap map,
        Dictionary<string, ICSharpDbClient> clients,
        ICSharpDbRouteContextAccessor? routeContextAccessor,
        CSharpDbShardCatalogStore? catalogStore,
        CSharpDbShardingOptions effectiveOptions)
    {
        _map = map;
        _clients = clients;
        _routeContextAccessor = routeContextAccessor;
        _catalogStore = catalogStore;
        _effectiveOptions = effectiveOptions;
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

    public Task<IReadOnlyList<CSharpDbShardMigrationHistoryEntry>> GetShardMigrationHistoryAsync(CancellationToken ct = default)
        => _catalogStore is null
            ? Task.FromResult((IReadOnlyList<CSharpDbShardMigrationHistoryEntry>)Array.Empty<CSharpDbShardMigrationHistoryEntry>())
            : _catalogStore.GetMigrationHistoryAsync(ct);

    public async Task<CSharpDbShardMigrationResult> MigrateExactRouteKeyAsync(
        CSharpDbShardExactKeyMigrationRequest request,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Manifest);

        DateTimeOffset startedUtc = DateTimeOffset.UtcNow;
        string migrationId = CreateMigrationId(startedUtc);
        string keyspace = string.Empty;
        string routeKey = string.Empty;
        string sourceShardId = string.Empty;
        string destinationShardId = string.Empty;
        var issues = new List<CSharpDbShardCatalogIssue>();

        try
        {
            (keyspace, routeKey) = CSharpDbShardMap.NormalizeRoute(new CSharpDbRouteContext
            {
                Keyspace = request.Keyspace,
                Key = request.RouteKey,
            });
            destinationShardId = CSharpDbShardMap.NormalizeShardId(request.DestinationShardId);

            if (_catalogStore is null)
            {
                issues.Add(CreateMigrationIssue(
                    "catalog-not-enabled",
                    "Exact route-key migration requires catalog mode so the verified ownership change can be recorded."));
            }
            else if (!_catalogStore.CanWrite)
            {
                issues.Add(CreateMigrationIssue(
                    "catalog-read-only",
                    "Exact route-key migration requires writable catalog mode."));
            }

            if (request.ExpectedCurrentMapVersion is int expectedVersion && expectedVersion != _map.MapVersion)
            {
                issues.Add(CreateMigrationIssue(
                    "map-version-mismatch",
                    $"Expected current map version {expectedVersion}, but the live map version is {_map.MapVersion}."));
            }

            CSharpDbShardResolution sourceResolution = _map.Resolve(new CSharpDbRouteContext
            {
                Keyspace = keyspace,
                Key = routeKey,
            });
            sourceShardId = sourceResolution.ShardId;

            CSharpDbShardDefinition destinationShard = _map.GetShard(destinationShardId);
            if (!destinationShard.Enabled)
            {
                issues.Add(CreateMigrationIssue(
                    "destination-shard-disabled",
                    $"Destination shard '{destinationShardId}' is disabled."));
            }
            if (!string.Equals(destinationShard.Role, CSharpDbShardRoles.Primary, StringComparison.OrdinalIgnoreCase))
            {
                issues.Add(CreateMigrationIssue(
                    "destination-shard-not-primary",
                    $"Destination shard '{destinationShardId}' is a {destinationShard.Role} shard. Exact route-key ownership can move only to primary shards."));
            }

            if (string.Equals(sourceShardId, destinationShardId, StringComparison.OrdinalIgnoreCase))
            {
                issues.Add(CreateMigrationIssue(
                    "destination-is-source",
                    $"Route key '{routeKey}' already resolves to shard '{sourceShardId}'."));
            }

            ValidateMigrationManifest(request.Manifest, issues);
            if (HasErrors(issues))
            {
                CSharpDbShardMigrationResult rejectedResult = CreateMigrationResult(
                    migrationId,
                    startedUtc,
                    succeeded: false,
                    status: "Rejected",
                    message: "Exact route-key migration request was rejected by validation.",
                    keyspace,
                    routeKey,
                    sourceShardId,
                    destinationShardId,
                    pendingMapVersion: null,
                    requiresRestart: false,
                    [],
                    [],
                    issues,
                    catalogApplyResult: null);
                await TryRecordMigrationHistoryAsync(rejectedResult, request, ct).ConfigureAwait(false);
                return rejectedResult;
            }

            string fenceKey = BuildFenceKey(keyspace, routeKey);
            if (!_writeFences.TryAdd(fenceKey, 0))
            {
                issues.Add(CreateMigrationIssue(
                    "route-key-already-fenced",
                    $"Route key '{routeKey}' already has an active migration fence."));
                CSharpDbShardMigrationResult fencedResult = CreateMigrationResult(
                    migrationId,
                    startedUtc,
                    succeeded: false,
                    status: "Rejected",
                    message: "Exact route-key migration request was rejected because the route key is already fenced.",
                    keyspace,
                    routeKey,
                    sourceShardId,
                    destinationShardId,
                    pendingMapVersion: null,
                    requiresRestart: false,
                    [],
                    [],
                    issues,
                    catalogApplyResult: null);
                await TryRecordMigrationHistoryAsync(fencedResult, request, ct).ConfigureAwait(false);
                return fencedResult;
            }

            try
            {
                int pageSize = NormalizeMigrationPageSize(request.Manifest.PageSize);
                ICSharpDbClient sourceClient = GetShardClient(sourceShardId);
                ICSharpDbClient destinationClient = GetShardClient(destinationShardId);

                var tableResults = new List<CSharpDbShardMigrationTableResult>();
                foreach (CSharpDbShardMigrationTableManifest table in request.Manifest.Tables)
                {
                    CSharpDbShardMigrationTableResult tableResult = await MigrateTableAsync(
                        table,
                        routeKey,
                        sourceClient,
                        destinationClient,
                        request.OverwriteDestinationRows,
                        request.DeleteSourceAfterVerification,
                        pageSize,
                        ct).ConfigureAwait(false);
                    tableResults.Add(tableResult);
                    if (!tableResult.Verified)
                    {
                        issues.Add(CreateMigrationIssue(
                            "table-verification-failed",
                            $"Table '{tableResult.TableName}' did not verify: {tableResult.Error ?? "checksum or count mismatch"}"));
                    }
                }

                var collectionResults = new List<CSharpDbShardMigrationCollectionResult>();
                foreach (CSharpDbShardMigrationCollectionManifest collection in request.Manifest.Collections)
                {
                    CSharpDbShardMigrationCollectionResult collectionResult = await MigrateCollectionAsync(
                        collection,
                        routeKey,
                        sourceClient,
                        destinationClient,
                        request.DeleteSourceAfterVerification,
                        pageSize,
                        ct).ConfigureAwait(false);
                    collectionResults.Add(collectionResult);
                    if (!collectionResult.Verified)
                    {
                        issues.Add(CreateMigrationIssue(
                            "collection-verification-failed",
                            $"Collection '{collectionResult.CollectionName}' did not verify: {collectionResult.Error ?? "checksum or count mismatch"}"));
                    }
                }

                if (HasErrors(issues))
                {
                    CSharpDbShardMigrationResult verificationFailedResult = CreateMigrationResult(
                        migrationId,
                        startedUtc,
                        succeeded: false,
                        status: "VerificationFailed",
                        message: "Exact route-key migration copied data, but verification failed. The active shard map was left unchanged.",
                        keyspace,
                        routeKey,
                        sourceShardId,
                        destinationShardId,
                        pendingMapVersion: null,
                        requiresRestart: false,
                        tableResults,
                        collectionResults,
                        issues,
                        catalogApplyResult: null);
                    await TryRecordMigrationHistoryAsync(verificationFailedResult, request, ct).ConfigureAwait(false);
                    return verificationFailedResult;
                }

                CSharpDbShardingOptions proposedOptions = BuildExactRouteMigrationOptions(routeKey, destinationShardId);
                CSharpDbShardCatalogApplyResult applyResult = await ApplyShardCatalogUpdateAsync(new CSharpDbShardCatalogUpdateRequest
                {
                    Options = proposedOptions,
                    ExpectedCurrentMapVersion = _map.MapVersion,
                    AllowMetadataOnlyOwnershipChange = true,
                    Operator = request.Operator,
                    Comment = string.IsNullOrWhiteSpace(request.Comment)
                        ? $"Exact route-key migration {migrationId}: {routeKey} -> {destinationShardId}"
                        : $"{request.Comment} (migration {migrationId})",
                }, ct).ConfigureAwait(false);

                issues.AddRange(applyResult.Validation.Issues);
                CSharpDbShardMigrationResult appliedResult = CreateMigrationResult(
                    migrationId,
                    startedUtc,
                    succeeded: applyResult.Applied,
                    status: applyResult.Applied ? "PendingActivation" : "CatalogApplyFailed",
                    message: applyResult.Applied
                        ? "Exact route-key migration verified and wrote a pending shard map. Restart or recreate the sharded client to activate the new route."
                        : "Exact route-key migration verified data movement, but the catalog update failed. The active shard map was left unchanged.",
                    keyspace,
                    routeKey,
                    sourceShardId,
                    destinationShardId,
                    pendingMapVersion: applyResult.PendingMap?.MapVersion,
                    requiresRestart: applyResult.RequiresRestart,
                    tableResults,
                    collectionResults,
                    issues,
                    applyResult);
                await TryRecordMigrationHistoryAsync(appliedResult, request, ct).ConfigureAwait(false);
                return appliedResult;
            }
            finally
            {
                _writeFences.TryRemove(fenceKey, out _);
            }
        }
        catch (Exception ex) when (ex is CSharpDbClientException or CSharpDbClientConfigurationException or ArgumentException or InvalidOperationException)
        {
            issues.Add(CreateMigrationIssue("migration-failed", ex.Message));
            CSharpDbShardMigrationResult failedResult = CreateMigrationResult(
                migrationId,
                startedUtc,
                succeeded: false,
                status: "Failed",
                message: "Exact route-key migration failed. The active shard map was left unchanged.",
                keyspace,
                routeKey,
                sourceShardId,
                destinationShardId,
                pendingMapVersion: null,
                requiresRestart: false,
                [],
                [],
                issues,
                catalogApplyResult: null);
            await TryRecordMigrationHistoryAsync(failedResult, request, ct).ConfigureAwait(false);
            return failedResult;
        }
    }

    public async Task<CSharpDbShardMigrationResult> MigrateBucketRangeAsync(
        CSharpDbShardBucketRangeMigrationRequest request,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Manifest);

        DateTimeOffset startedUtc = DateTimeOffset.UtcNow;
        string migrationId = CreateMigrationId(startedUtc);
        string keyspace = string.Empty;
        string routeKey = string.Empty;
        string sourceShardId = string.Empty;
        string destinationShardId = string.Empty;
        var issues = new List<CSharpDbShardCatalogIssue>();

        try
        {
            keyspace = NormalizeRequired(request.Keyspace, nameof(request.Keyspace));
            routeKey = FormatBucketRangeRouteKey(request.StartBucketInclusive, request.EndBucketExclusive);
            sourceShardId = CSharpDbShardMap.NormalizeShardId(request.SourceShardId);
            destinationShardId = CSharpDbShardMap.NormalizeShardId(request.DestinationShardId);

            if (!string.Equals(keyspace, _map.Keyspace, StringComparison.OrdinalIgnoreCase))
            {
                issues.Add(CreateMigrationIssue(
                    "keyspace-mismatch",
                    $"Bucket-range migration keyspace '{keyspace}' does not match configured keyspace '{_map.Keyspace}'."));
            }

            if (_catalogStore is null)
            {
                issues.Add(CreateMigrationIssue(
                    "catalog-not-enabled",
                    "Bucket-range migration requires catalog mode so the verified ownership change can be recorded."));
            }
            else if (!_catalogStore.CanWrite)
            {
                issues.Add(CreateMigrationIssue(
                    "catalog-read-only",
                    "Bucket-range migration requires writable catalog mode."));
            }

            if (request.ExpectedCurrentMapVersion is int expectedVersion && expectedVersion != _map.MapVersion)
            {
                issues.Add(CreateMigrationIssue(
                    "map-version-mismatch",
                    $"Expected current map version {expectedVersion}, but the live map version is {_map.MapVersion}."));
            }

            if (!_map.IsValidBucketRange(request.StartBucketInclusive, request.EndBucketExclusive))
            {
                issues.Add(CreateMigrationIssue(
                    "invalid-bucket-range",
                    $"Bucket range [{request.StartBucketInclusive}, {request.EndBucketExclusive}) is outside virtual bucket count {_map.VirtualBucketCount}."));
            }
            else if (!_map.BucketRangeOwnedBy(request.StartBucketInclusive, request.EndBucketExclusive, sourceShardId))
            {
                issues.Add(CreateMigrationIssue(
                    "source-does-not-own-bucket-range",
                    $"Source shard '{sourceShardId}' does not own every bucket in range [{request.StartBucketInclusive}, {request.EndBucketExclusive})."));
            }

            CSharpDbShardDefinition sourceShard = _map.GetShard(sourceShardId);
            if (!sourceShard.Enabled)
            {
                issues.Add(CreateMigrationIssue(
                    "source-shard-disabled",
                    $"Source shard '{sourceShardId}' is disabled."));
            }

            CSharpDbShardDefinition destinationShard = _map.GetShard(destinationShardId);
            if (!destinationShard.Enabled)
            {
                issues.Add(CreateMigrationIssue(
                    "destination-shard-disabled",
                    $"Destination shard '{destinationShardId}' is disabled."));
            }
            if (!string.Equals(destinationShard.Role, CSharpDbShardRoles.Primary, StringComparison.OrdinalIgnoreCase))
            {
                issues.Add(CreateMigrationIssue(
                    "destination-shard-not-primary",
                    $"Destination shard '{destinationShardId}' is a {destinationShard.Role} shard. Bucket ownership can move only to primary shards."));
            }

            if (string.Equals(sourceShardId, destinationShardId, StringComparison.OrdinalIgnoreCase))
            {
                issues.Add(CreateMigrationIssue(
                    "destination-is-source",
                    $"Bucket range [{request.StartBucketInclusive}, {request.EndBucketExclusive}) already belongs to shard '{sourceShardId}'."));
            }

            ValidateMigrationManifest(request.Manifest, issues);
            if (HasErrors(issues))
            {
                CSharpDbShardMigrationResult rejectedResult = CreateMigrationResult(
                    migrationId,
                    startedUtc,
                    succeeded: false,
                    status: "Rejected",
                    message: "Bucket-range migration request was rejected by validation.",
                    keyspace,
                    routeKey,
                    sourceShardId,
                    destinationShardId,
                    pendingMapVersion: null,
                    requiresRestart: false,
                    [],
                    [],
                    issues,
                    catalogApplyResult: null);
                await TryRecordMigrationHistoryAsync(rejectedResult, "BucketRange", request.Operator, request.Comment, ct).ConfigureAwait(false);
                return rejectedResult;
            }

            string fenceKey = BuildBucketRangeFenceKey(keyspace, request.StartBucketInclusive, request.EndBucketExclusive);
            var fence = new BucketRangeFence(keyspace, request.StartBucketInclusive, request.EndBucketExclusive);
            if (!TryAddBucketRangeFence(fenceKey, fence))
            {
                issues.Add(CreateMigrationIssue(
                    "bucket-range-already-fenced",
                    $"Bucket range [{request.StartBucketInclusive}, {request.EndBucketExclusive}) overlaps an active shard migration fence."));
                CSharpDbShardMigrationResult fencedResult = CreateMigrationResult(
                    migrationId,
                    startedUtc,
                    succeeded: false,
                    status: "Rejected",
                    message: "Bucket-range migration request was rejected because the bucket range is already fenced.",
                    keyspace,
                    routeKey,
                    sourceShardId,
                    destinationShardId,
                    pendingMapVersion: null,
                    requiresRestart: false,
                    [],
                    [],
                    issues,
                    catalogApplyResult: null);
                await TryRecordMigrationHistoryAsync(fencedResult, "BucketRange", request.Operator, request.Comment, ct).ConfigureAwait(false);
                return fencedResult;
            }

            try
            {
                int pageSize = NormalizeMigrationPageSize(request.Manifest.PageSize);
                ICSharpDbClient sourceClient = GetShardClient(sourceShardId);
                ICSharpDbClient destinationClient = GetShardClient(destinationShardId);

                var tableResults = new List<CSharpDbShardMigrationTableResult>();
                foreach (CSharpDbShardMigrationTableManifest table in request.Manifest.Tables)
                {
                    CSharpDbShardMigrationTableResult tableResult = await MigrateBucketRangeTableAsync(
                        table,
                        request.StartBucketInclusive,
                        request.EndBucketExclusive,
                        sourceClient,
                        destinationClient,
                        request.OverwriteDestinationRows,
                        request.DeleteSourceAfterVerification,
                        pageSize,
                        ct).ConfigureAwait(false);
                    tableResults.Add(tableResult);
                    if (!tableResult.Verified)
                    {
                        issues.Add(CreateMigrationIssue(
                            "table-verification-failed",
                            $"Table '{tableResult.TableName}' did not verify: {tableResult.Error ?? "checksum or count mismatch"}"));
                    }
                }

                var collectionResults = new List<CSharpDbShardMigrationCollectionResult>();
                foreach (CSharpDbShardMigrationCollectionManifest collection in request.Manifest.Collections)
                {
                    CSharpDbShardMigrationCollectionResult collectionResult = await MigrateBucketRangeCollectionAsync(
                        collection,
                        request.StartBucketInclusive,
                        request.EndBucketExclusive,
                        sourceClient,
                        destinationClient,
                        request.DeleteSourceAfterVerification,
                        pageSize,
                        ct).ConfigureAwait(false);
                    collectionResults.Add(collectionResult);
                    if (!collectionResult.Verified)
                    {
                        issues.Add(CreateMigrationIssue(
                            "collection-verification-failed",
                            $"Collection '{collectionResult.CollectionName}' did not verify: {collectionResult.Error ?? "checksum or count mismatch"}"));
                    }
                }

                if (HasErrors(issues))
                {
                    CSharpDbShardMigrationResult verificationFailedResult = CreateMigrationResult(
                        migrationId,
                        startedUtc,
                        succeeded: false,
                        status: "VerificationFailed",
                        message: "Bucket-range migration copied data, but verification failed. The active shard map was left unchanged.",
                        keyspace,
                        routeKey,
                        sourceShardId,
                        destinationShardId,
                        pendingMapVersion: null,
                        requiresRestart: false,
                        tableResults,
                        collectionResults,
                        issues,
                        catalogApplyResult: null);
                    await TryRecordMigrationHistoryAsync(verificationFailedResult, "BucketRange", request.Operator, request.Comment, ct).ConfigureAwait(false);
                    return verificationFailedResult;
                }

                CSharpDbShardingOptions proposedOptions = BuildBucketRangeMigrationOptions(
                    request.StartBucketInclusive,
                    request.EndBucketExclusive,
                    sourceShardId,
                    destinationShardId);
                CSharpDbShardCatalogApplyResult applyResult = await ApplyShardCatalogUpdateAsync(new CSharpDbShardCatalogUpdateRequest
                {
                    Options = proposedOptions,
                    ExpectedCurrentMapVersion = _map.MapVersion,
                    AllowMetadataOnlyOwnershipChange = true,
                    Operator = request.Operator,
                    Comment = string.IsNullOrWhiteSpace(request.Comment)
                        ? $"Bucket-range migration {migrationId}: [{request.StartBucketInclusive}, {request.EndBucketExclusive}) {sourceShardId} -> {destinationShardId}"
                        : $"{request.Comment} (migration {migrationId})",
                }, ct).ConfigureAwait(false);

                issues.AddRange(applyResult.Validation.Issues);
                CSharpDbShardMigrationResult appliedResult = CreateMigrationResult(
                    migrationId,
                    startedUtc,
                    succeeded: applyResult.Applied,
                    status: applyResult.Applied ? "PendingActivation" : "CatalogApplyFailed",
                    message: applyResult.Applied
                        ? "Bucket-range migration verified and wrote a pending shard map. Restart or recreate the sharded client to activate the new bucket ownership."
                        : "Bucket-range migration verified data movement, but the catalog update failed. The active shard map was left unchanged.",
                    keyspace,
                    routeKey,
                    sourceShardId,
                    destinationShardId,
                    pendingMapVersion: applyResult.PendingMap?.MapVersion,
                    requiresRestart: applyResult.RequiresRestart,
                    tableResults,
                    collectionResults,
                    issues,
                    applyResult);
                await TryRecordMigrationHistoryAsync(appliedResult, "BucketRange", request.Operator, request.Comment, ct).ConfigureAwait(false);
                return appliedResult;
            }
            finally
            {
                _bucketRangeFences.TryRemove(fenceKey, out _);
            }
        }
        catch (Exception ex) when (ex is CSharpDbClientException or CSharpDbClientConfigurationException or ArgumentException or InvalidOperationException)
        {
            issues.Add(CreateMigrationIssue("migration-failed", ex.Message));
            CSharpDbShardMigrationResult failedResult = CreateMigrationResult(
                migrationId,
                startedUtc,
                succeeded: false,
                status: "Failed",
                message: "Bucket-range migration failed. The active shard map was left unchanged.",
                keyspace,
                routeKey,
                sourceShardId,
                destinationShardId,
                pendingMapVersion: null,
                requiresRestart: false,
                [],
                [],
                issues,
                catalogApplyResult: null);
            await TryRecordMigrationHistoryAsync(failedResult, "BucketRange", request.Operator, request.Comment, ct).ConfigureAwait(false);
            return failedResult;
        }
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
                    Role = shard.Role,
                    PrimaryShardId = shard.PrimaryShardId,
                    PromotionEligible = shard.PromotionEligible,
                    CanPromote = false,
                    ReplicationLagBytes = shard.ReplicationLagBytes,
                    LastReplicatedUtc = shard.LastReplicatedUtc,
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
                    Role = shard.Role,
                    PrimaryShardId = shard.PrimaryShardId,
                    PromotionEligible = shard.PromotionEligible,
                    CanPromote = IsReplicaRole(shard.Role) && shard.PromotionEligible,
                    ReplicationLagBytes = shard.ReplicationLagBytes,
                    LastReplicatedUtc = shard.LastReplicatedUtc,
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
                    Role = shard.Role,
                    PrimaryShardId = shard.PrimaryShardId,
                    PromotionEligible = shard.PromotionEligible,
                    CanPromote = false,
                    ReplicationLagBytes = shard.ReplicationLagBytes,
                    LastReplicatedUtc = shard.LastReplicatedUtc,
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

    public async Task<IReadOnlyList<CSharpDbShardSqlExecutionResult>> ExecuteReadOnlySqlOnAllShardsAsync(
        string sql,
        CancellationToken ct = default)
    {
        ValidateReadOnlyFanOutSql(sql);
        return await ExecuteSqlOnAllShardsAsync(sql, ct).ConfigureAwait(false);
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

        return new CSharpDbShardedClient(
            map,
            clients,
            routeContextAccessor,
            catalogStore,
            CSharpDbShardCatalogStore.CloneOptionsForRuntime(effectiveOptions));
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

    private async Task<CSharpDbShardMigrationTableResult> MigrateTableAsync(
        CSharpDbShardMigrationTableManifest manifest,
        string routeKey,
        ICSharpDbClient sourceClient,
        ICSharpDbClient destinationClient,
        bool overwriteDestinationRows,
        bool deleteSourceAfterVerification,
        int pageSize,
        CancellationToken ct)
    {
        string tableName = NormalizeRequired(manifest.TableName, nameof(manifest.TableName));
        try
        {
            var sourceRows = await ReadRouteTableRowsAsync(sourceClient, manifest, routeKey, pageSize, ct).ConfigureAwait(false);
            int copied = 0;
            foreach (Dictionary<string, object?> row in sourceRows.Rows)
            {
                object pkValue = GetRequiredRowValue(row, sourceRows.PrimaryKeyColumn, tableName);
                Dictionary<string, object?>? existing = overwriteDestinationRows
                    ? await destinationClient.GetRowByPkAsync(tableName, sourceRows.PrimaryKeyColumn, pkValue, ct).ConfigureAwait(false)
                    : null;

                if (existing is null)
                    copied += await destinationClient.InsertRowAsync(tableName, row, ct).ConfigureAwait(false);
                else
                    copied += await destinationClient.UpdateRowAsync(tableName, sourceRows.PrimaryKeyColumn, pkValue, row, ct).ConfigureAwait(false);
            }

            var destinationRows = await ReadRouteTableRowsAsync(destinationClient, manifest, routeKey, pageSize, ct).ConfigureAwait(false);
            string sourceChecksum = ComputeTableChecksum(sourceRows.Rows, sourceRows.Schema.Columns, sourceRows.PrimaryKeyColumn);
            string destinationChecksum = ComputeTableChecksum(destinationRows.Rows, destinationRows.Schema.Columns, destinationRows.PrimaryKeyColumn);
            bool verified = sourceRows.Rows.Count == destinationRows.Rows.Count &&
                            string.Equals(sourceChecksum, destinationChecksum, StringComparison.Ordinal);

            int deleted = 0;
            if (verified && deleteSourceAfterVerification)
            {
                foreach (Dictionary<string, object?> row in sourceRows.Rows)
                {
                    object pkValue = GetRequiredRowValue(row, sourceRows.PrimaryKeyColumn, tableName);
                    deleted += await sourceClient.DeleteRowAsync(tableName, sourceRows.PrimaryKeyColumn, pkValue, ct).ConfigureAwait(false);
                }
            }

            return new CSharpDbShardMigrationTableResult
            {
                TableName = tableName,
                SourceRows = sourceRows.Rows.Count,
                DestinationRows = destinationRows.Rows.Count,
                RowsCopied = copied,
                SourceRowsDeleted = deleted,
                Verified = verified,
                SourceChecksum = sourceChecksum,
                DestinationChecksum = destinationChecksum,
                Error = verified ? null : "Destination route-key row set does not match source row set.",
            };
        }
        catch (Exception ex) when (ex is CSharpDbClientException or ArgumentException or InvalidOperationException)
        {
            return new CSharpDbShardMigrationTableResult
            {
                TableName = tableName,
                SourceRows = 0,
                DestinationRows = 0,
                RowsCopied = 0,
                SourceRowsDeleted = 0,
                Verified = false,
                Error = ex.Message,
            };
        }
    }

    private async Task<CSharpDbShardMigrationCollectionResult> MigrateCollectionAsync(
        CSharpDbShardMigrationCollectionManifest manifest,
        string routeKey,
        ICSharpDbClient sourceClient,
        ICSharpDbClient destinationClient,
        bool deleteSourceAfterVerification,
        int pageSize,
        CancellationToken ct)
    {
        string collectionName = NormalizeRequired(manifest.CollectionName, nameof(manifest.CollectionName));
        try
        {
            IReadOnlyList<CollectionDocument> sourceDocuments =
                await ReadRouteCollectionDocumentsAsync(sourceClient, manifest, routeKey, pageSize, ct).ConfigureAwait(false);
            int copied = 0;
            foreach (CollectionDocument document in sourceDocuments)
            {
                await destinationClient.PutDocumentAsync(collectionName, document.Key, document.Document, ct).ConfigureAwait(false);
                copied++;
            }

            IReadOnlyList<CollectionDocument> destinationDocuments =
                await ReadRouteCollectionDocumentsAsync(destinationClient, manifest, routeKey, pageSize, ct).ConfigureAwait(false);
            string sourceChecksum = ComputeCollectionChecksum(sourceDocuments);
            string destinationChecksum = ComputeCollectionChecksum(destinationDocuments);
            bool verified = sourceDocuments.Count == destinationDocuments.Count &&
                            string.Equals(sourceChecksum, destinationChecksum, StringComparison.Ordinal);

            int deleted = 0;
            if (verified && deleteSourceAfterVerification)
            {
                foreach (CollectionDocument document in sourceDocuments)
                {
                    if (await sourceClient.DeleteDocumentAsync(collectionName, document.Key, ct).ConfigureAwait(false))
                        deleted++;
                }
            }

            return new CSharpDbShardMigrationCollectionResult
            {
                CollectionName = collectionName,
                SourceDocuments = sourceDocuments.Count,
                DestinationDocuments = destinationDocuments.Count,
                DocumentsCopied = copied,
                SourceDocumentsDeleted = deleted,
                Verified = verified,
                SourceChecksum = sourceChecksum,
                DestinationChecksum = destinationChecksum,
                Error = verified ? null : "Destination route-key document set does not match source document set.",
            };
        }
        catch (Exception ex) when (ex is CSharpDbClientException or ArgumentException or InvalidOperationException)
        {
            return new CSharpDbShardMigrationCollectionResult
            {
                CollectionName = collectionName,
                SourceDocuments = 0,
                DestinationDocuments = 0,
                DocumentsCopied = 0,
                SourceDocumentsDeleted = 0,
                Verified = false,
                Error = ex.Message,
            };
        }
    }

    private async Task<CSharpDbShardMigrationTableResult> MigrateBucketRangeTableAsync(
        CSharpDbShardMigrationTableManifest manifest,
        int startBucketInclusive,
        int endBucketExclusive,
        ICSharpDbClient sourceClient,
        ICSharpDbClient destinationClient,
        bool overwriteDestinationRows,
        bool deleteSourceAfterVerification,
        int pageSize,
        CancellationToken ct)
    {
        string tableName = NormalizeRequired(manifest.TableName, nameof(manifest.TableName));
        try
        {
            var sourceRows = await ReadBucketRangeTableRowsAsync(
                sourceClient,
                manifest,
                startBucketInclusive,
                endBucketExclusive,
                pageSize,
                ct).ConfigureAwait(false);
            int copied = 0;
            foreach (Dictionary<string, object?> row in sourceRows.Rows)
            {
                object pkValue = GetRequiredRowValue(row, sourceRows.PrimaryKeyColumn, tableName);
                Dictionary<string, object?>? existing = overwriteDestinationRows
                    ? await destinationClient.GetRowByPkAsync(tableName, sourceRows.PrimaryKeyColumn, pkValue, ct).ConfigureAwait(false)
                    : null;

                if (existing is null)
                    copied += await destinationClient.InsertRowAsync(tableName, row, ct).ConfigureAwait(false);
                else
                    copied += await destinationClient.UpdateRowAsync(tableName, sourceRows.PrimaryKeyColumn, pkValue, row, ct).ConfigureAwait(false);
            }

            var destinationRows = await ReadBucketRangeTableRowsAsync(
                destinationClient,
                manifest,
                startBucketInclusive,
                endBucketExclusive,
                pageSize,
                ct).ConfigureAwait(false);
            string sourceChecksum = ComputeTableChecksum(sourceRows.Rows, sourceRows.Schema.Columns, sourceRows.PrimaryKeyColumn);
            string destinationChecksum = ComputeTableChecksum(destinationRows.Rows, destinationRows.Schema.Columns, destinationRows.PrimaryKeyColumn);
            bool verified = sourceRows.Rows.Count == destinationRows.Rows.Count &&
                            string.Equals(sourceChecksum, destinationChecksum, StringComparison.Ordinal);

            int deleted = 0;
            if (verified && deleteSourceAfterVerification)
            {
                foreach (Dictionary<string, object?> row in sourceRows.Rows)
                {
                    object pkValue = GetRequiredRowValue(row, sourceRows.PrimaryKeyColumn, tableName);
                    deleted += await sourceClient.DeleteRowAsync(tableName, sourceRows.PrimaryKeyColumn, pkValue, ct).ConfigureAwait(false);
                }
            }

            return new CSharpDbShardMigrationTableResult
            {
                TableName = tableName,
                SourceRows = sourceRows.Rows.Count,
                DestinationRows = destinationRows.Rows.Count,
                RowsCopied = copied,
                SourceRowsDeleted = deleted,
                Verified = verified,
                SourceChecksum = sourceChecksum,
                DestinationChecksum = destinationChecksum,
                Error = verified ? null : "Destination bucket-range row set does not match source row set.",
            };
        }
        catch (Exception ex) when (ex is CSharpDbClientException or ArgumentException or InvalidOperationException)
        {
            return new CSharpDbShardMigrationTableResult
            {
                TableName = tableName,
                SourceRows = 0,
                DestinationRows = 0,
                RowsCopied = 0,
                SourceRowsDeleted = 0,
                Verified = false,
                Error = ex.Message,
            };
        }
    }

    private async Task<CSharpDbShardMigrationCollectionResult> MigrateBucketRangeCollectionAsync(
        CSharpDbShardMigrationCollectionManifest manifest,
        int startBucketInclusive,
        int endBucketExclusive,
        ICSharpDbClient sourceClient,
        ICSharpDbClient destinationClient,
        bool deleteSourceAfterVerification,
        int pageSize,
        CancellationToken ct)
    {
        string collectionName = NormalizeRequired(manifest.CollectionName, nameof(manifest.CollectionName));
        try
        {
            IReadOnlyList<CollectionDocument> sourceDocuments =
                await ReadBucketRangeCollectionDocumentsAsync(
                    sourceClient,
                    manifest,
                    startBucketInclusive,
                    endBucketExclusive,
                    pageSize,
                    ct).ConfigureAwait(false);
            int copied = 0;
            foreach (CollectionDocument document in sourceDocuments)
            {
                await destinationClient.PutDocumentAsync(collectionName, document.Key, document.Document, ct).ConfigureAwait(false);
                copied++;
            }

            IReadOnlyList<CollectionDocument> destinationDocuments =
                await ReadBucketRangeCollectionDocumentsAsync(
                    destinationClient,
                    manifest,
                    startBucketInclusive,
                    endBucketExclusive,
                    pageSize,
                    ct).ConfigureAwait(false);
            string sourceChecksum = ComputeCollectionChecksum(sourceDocuments);
            string destinationChecksum = ComputeCollectionChecksum(destinationDocuments);
            bool verified = sourceDocuments.Count == destinationDocuments.Count &&
                            string.Equals(sourceChecksum, destinationChecksum, StringComparison.Ordinal);

            int deleted = 0;
            if (verified && deleteSourceAfterVerification)
            {
                foreach (CollectionDocument document in sourceDocuments)
                {
                    if (await sourceClient.DeleteDocumentAsync(collectionName, document.Key, ct).ConfigureAwait(false))
                        deleted++;
                }
            }

            return new CSharpDbShardMigrationCollectionResult
            {
                CollectionName = collectionName,
                SourceDocuments = sourceDocuments.Count,
                DestinationDocuments = destinationDocuments.Count,
                DocumentsCopied = copied,
                SourceDocumentsDeleted = deleted,
                Verified = verified,
                SourceChecksum = sourceChecksum,
                DestinationChecksum = destinationChecksum,
                Error = verified ? null : "Destination bucket-range document set does not match source document set.",
            };
        }
        catch (Exception ex) when (ex is CSharpDbClientException or ArgumentException or InvalidOperationException)
        {
            return new CSharpDbShardMigrationCollectionResult
            {
                CollectionName = collectionName,
                SourceDocuments = 0,
                DestinationDocuments = 0,
                DocumentsCopied = 0,
                SourceDocumentsDeleted = 0,
                Verified = false,
                Error = ex.Message,
            };
        }
    }

    private static async Task<RouteTableRows> ReadRouteTableRowsAsync(
        ICSharpDbClient client,
        CSharpDbShardMigrationTableManifest manifest,
        string routeKey,
        int pageSize,
        CancellationToken ct)
    {
        string tableName = NormalizeRequired(manifest.TableName, nameof(manifest.TableName));
        string routeKeyColumn = NormalizeRequired(manifest.RouteKeyColumn, nameof(manifest.RouteKeyColumn));
        string primaryKeyColumn = NormalizeRequired(manifest.PrimaryKeyColumn, nameof(manifest.PrimaryKeyColumn));
        TableSchema schema = await client.GetTableSchemaAsync(tableName, ct).ConfigureAwait(false)
                             ?? throw new CSharpDbClientException($"Table '{tableName}' was not found.");

        ColumnDefinition routeColumn = FindColumn(schema, routeKeyColumn);
        ColumnDefinition primaryKey = FindColumn(schema, primaryKeyColumn);
        var rows = new List<Dictionary<string, object?>>();
        int page = 1;
        while (true)
        {
            TableBrowseResult result = await client.BrowseTableAsync(tableName, page, pageSize, ct).ConfigureAwait(false);
            foreach (object?[] row in result.Rows)
            {
                Dictionary<string, object?> values = ToRowDictionary(schema, row);
                if (RouteValueMatches(values[routeColumn.Name], routeKey))
                    rows.Add(values);
            }

            if (page >= result.TotalPages || result.Rows.Count == 0)
                break;

            page++;
        }

        return new RouteTableRows(schema, routeColumn.Name, primaryKey.Name, rows);
    }

    private async Task<RouteTableRows> ReadBucketRangeTableRowsAsync(
        ICSharpDbClient client,
        CSharpDbShardMigrationTableManifest manifest,
        int startBucketInclusive,
        int endBucketExclusive,
        int pageSize,
        CancellationToken ct)
    {
        string tableName = NormalizeRequired(manifest.TableName, nameof(manifest.TableName));
        string routeKeyColumn = NormalizeRequired(manifest.RouteKeyColumn, nameof(manifest.RouteKeyColumn));
        string primaryKeyColumn = NormalizeRequired(manifest.PrimaryKeyColumn, nameof(manifest.PrimaryKeyColumn));
        TableSchema schema = await client.GetTableSchemaAsync(tableName, ct).ConfigureAwait(false)
                             ?? throw new CSharpDbClientException($"Table '{tableName}' was not found.");

        ColumnDefinition routeColumn = FindColumn(schema, routeKeyColumn);
        ColumnDefinition primaryKey = FindColumn(schema, primaryKeyColumn);
        var rows = new List<Dictionary<string, object?>>();
        int page = 1;
        while (true)
        {
            TableBrowseResult result = await client.BrowseTableAsync(tableName, page, pageSize, ct).ConfigureAwait(false);
            foreach (object?[] row in result.Rows)
            {
                Dictionary<string, object?> values = ToRowDictionary(schema, row);
                if (ShouldMoveBucketRangeRouteValue(values[routeColumn.Name], startBucketInclusive, endBucketExclusive))
                    rows.Add(values);
            }

            if (page >= result.TotalPages || result.Rows.Count == 0)
                break;

            page++;
        }

        return new RouteTableRows(schema, routeColumn.Name, primaryKey.Name, rows);
    }

    private static async Task<IReadOnlyList<CollectionDocument>> ReadRouteCollectionDocumentsAsync(
        ICSharpDbClient client,
        CSharpDbShardMigrationCollectionManifest manifest,
        string routeKey,
        int pageSize,
        CancellationToken ct)
    {
        string collectionName = NormalizeRequired(manifest.CollectionName, nameof(manifest.CollectionName));
        string routePropertyName = NormalizeRequired(manifest.RouteKeyPropertyName, nameof(manifest.RouteKeyPropertyName));
        var documents = new List<CollectionDocument>();
        int page = 1;
        while (true)
        {
            CollectionBrowseResult result = await client.BrowseCollectionAsync(collectionName, page, pageSize, ct).ConfigureAwait(false);
            foreach (CollectionDocument document in result.Documents)
            {
                if (DocumentRouteValueMatches(document.Document, routePropertyName, routeKey))
                    documents.Add(document);
            }

            if (page >= result.TotalPages || result.Documents.Count == 0)
                break;

            page++;
        }

        return documents;
    }

    private async Task<IReadOnlyList<CollectionDocument>> ReadBucketRangeCollectionDocumentsAsync(
        ICSharpDbClient client,
        CSharpDbShardMigrationCollectionManifest manifest,
        int startBucketInclusive,
        int endBucketExclusive,
        int pageSize,
        CancellationToken ct)
    {
        string collectionName = NormalizeRequired(manifest.CollectionName, nameof(manifest.CollectionName));
        string routePropertyName = NormalizeRequired(manifest.RouteKeyPropertyName, nameof(manifest.RouteKeyPropertyName));
        var documents = new List<CollectionDocument>();
        int page = 1;
        while (true)
        {
            CollectionBrowseResult result = await client.BrowseCollectionAsync(collectionName, page, pageSize, ct).ConfigureAwait(false);
            foreach (CollectionDocument document in result.Documents)
            {
                if (TryGetDocumentRouteValue(document.Document, routePropertyName, out string? routeKey) &&
                    routeKey is not null &&
                    ShouldMoveBucketRangeRouteKey(routeKey, startBucketInclusive, endBucketExclusive))
                {
                    documents.Add(document);
                }
            }

            if (page >= result.TotalPages || result.Documents.Count == 0)
                break;

            page++;
        }

        return documents;
    }

    private CSharpDbShardingOptions BuildExactRouteMigrationOptions(string routeKey, string destinationShardId)
    {
        CSharpDbShardingOptions proposed = CSharpDbShardCatalogStore.CloneOptionsForRuntime(_effectiveOptions);
        proposed.MapVersion = _map.MapVersion + 1;
        proposed.Keyspace = _map.Keyspace;
        proposed.ExactKeyPins[routeKey] = destinationShardId;
        proposed.DirectoryEntries = proposed.DirectoryEntries
            .Select(entry => ShouldMoveDirectoryEntry(entry, proposed.Keyspace, routeKey)
                ? new CSharpDbShardDirectoryEntry
                {
                    DirectoryName = entry.DirectoryName,
                    LookupKey = entry.LookupKey,
                    TargetKeyspace = entry.TargetKeyspace,
                    RouteKey = entry.RouteKey,
                    ShardId = destinationShardId,
                    MapVersion = proposed.MapVersion,
                    State = entry.State,
                }
                : entry)
            .ToArray();
        return proposed;
    }

    private CSharpDbShardingOptions BuildBucketRangeMigrationOptions(
        int startBucketInclusive,
        int endBucketExclusive,
        string sourceShardId,
        string destinationShardId)
    {
        CSharpDbShardingOptions proposed = CSharpDbShardCatalogStore.CloneOptionsForRuntime(_effectiveOptions);
        proposed.MapVersion = _map.MapVersion + 1;
        proposed.Keyspace = _map.Keyspace;

        string[] bucketOwners = _map.CreateBucketOwnerSnapshot();
        for (int bucket = startBucketInclusive; bucket < endBucketExclusive; bucket++)
            bucketOwners[bucket] = destinationShardId;

        proposed.BucketRanges = BuildBucketRanges(bucketOwners);
        proposed.DirectoryEntries = proposed.DirectoryEntries
            .Select(entry => ShouldMoveDirectoryEntry(entry, proposed.Keyspace, sourceShardId, startBucketInclusive, endBucketExclusive)
                ? new CSharpDbShardDirectoryEntry
                {
                    DirectoryName = entry.DirectoryName,
                    LookupKey = entry.LookupKey,
                    TargetKeyspace = entry.TargetKeyspace,
                    RouteKey = entry.RouteKey,
                    ShardId = destinationShardId,
                    MapVersion = proposed.MapVersion,
                    State = entry.State,
                }
                : entry)
            .ToArray();
        return proposed;
    }

    private static bool ShouldMoveDirectoryEntry(
        CSharpDbShardDirectoryEntry entry,
        string keyspace,
        string routeKey)
        => string.Equals(entry.TargetKeyspace, keyspace, StringComparison.OrdinalIgnoreCase) &&
           string.Equals(entry.RouteKey, routeKey, StringComparison.Ordinal) &&
           !string.Equals(entry.State, "Deleted", StringComparison.Ordinal);

    private bool ShouldMoveDirectoryEntry(
        CSharpDbShardDirectoryEntry entry,
        string keyspace,
        string sourceShardId,
        int startBucketInclusive,
        int endBucketExclusive)
        => string.Equals(entry.TargetKeyspace, keyspace, StringComparison.OrdinalIgnoreCase) &&
           string.Equals(entry.ShardId, sourceShardId, StringComparison.OrdinalIgnoreCase) &&
           !string.Equals(entry.State, "Deleted", StringComparison.Ordinal) &&
           ShouldMoveBucketRangeRouteKey(entry.RouteKey, startBucketInclusive, endBucketExclusive);

    private static CSharpDbShardBucketRange[] BuildBucketRanges(IReadOnlyList<string> bucketOwners)
    {
        if (bucketOwners.Count == 0)
            return [];

        var ranges = new List<CSharpDbShardBucketRange>();
        int start = 0;
        string current = bucketOwners[0];
        for (int bucket = 1; bucket < bucketOwners.Count; bucket++)
        {
            if (string.Equals(bucketOwners[bucket], current, StringComparison.OrdinalIgnoreCase))
                continue;

            ranges.Add(new CSharpDbShardBucketRange
            {
                StartBucketInclusive = start,
                EndBucketExclusive = bucket,
                ShardId = current,
            });
            start = bucket;
            current = bucketOwners[bucket];
        }

        ranges.Add(new CSharpDbShardBucketRange
        {
            StartBucketInclusive = start,
            EndBucketExclusive = bucketOwners.Count,
            ShardId = current,
        });

        return ranges.ToArray();
    }

    private CSharpDbShardMigrationResult CreateMigrationResult(
        string migrationId,
        DateTimeOffset startedUtc,
        bool succeeded,
        string status,
        string message,
        string keyspace,
        string routeKey,
        string sourceShardId,
        string destinationShardId,
        int? pendingMapVersion,
        bool requiresRestart,
        List<CSharpDbShardMigrationTableResult> tableResults,
        List<CSharpDbShardMigrationCollectionResult> collectionResults,
        List<CSharpDbShardCatalogIssue> issues,
        CSharpDbShardCatalogApplyResult? catalogApplyResult)
        => new()
        {
            MigrationId = migrationId,
            StartedUtc = startedUtc,
            CompletedUtc = DateTimeOffset.UtcNow,
            Succeeded = succeeded,
            Status = status,
            Message = message,
            Keyspace = keyspace,
            RouteKey = routeKey,
            SourceShardId = sourceShardId,
            DestinationShardId = destinationShardId,
            MapVersion = _map.MapVersion,
            PendingMapVersion = pendingMapVersion,
            RequiresRestart = requiresRestart,
            Tables = tableResults,
            Collections = collectionResults,
            Issues = issues,
            CatalogApplyResult = catalogApplyResult,
        };

    private async Task TryRecordMigrationHistoryAsync(
        CSharpDbShardMigrationResult result,
        CSharpDbShardExactKeyMigrationRequest request,
        CancellationToken ct)
        => await TryRecordMigrationHistoryAsync(result, "ExactRouteKey", request.Operator, request.Comment, ct).ConfigureAwait(false);

    private async Task TryRecordMigrationHistoryAsync(
        CSharpDbShardMigrationResult result,
        string migrationType,
        string? operatorName,
        string? comment,
        CancellationToken ct)
    {
        if (_catalogStore is null || !_catalogStore.CanWrite)
            return;

        try
        {
            await _catalogStore.AppendMigrationHistoryAsync(
                _effectiveOptions,
                CreateMigrationHistoryEntry(result, migrationType, operatorName, comment),
                ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or JsonException or CSharpDbClientException or CSharpDbClientConfigurationException)
        {
            result.Issues.Add(new CSharpDbShardCatalogIssue
            {
                Severity = CSharpDbShardCatalogIssueSeverity.Warning,
                Code = "migration-history-write-failed",
                Message = $"Migration completed with status '{result.Status}', but writing migration history failed: {ex.Message}",
            });
        }
    }

    private static CSharpDbShardMigrationHistoryEntry CreateMigrationHistoryEntry(
        CSharpDbShardMigrationResult result,
        string migrationType,
        string? operatorName,
        string? comment)
        => new()
        {
            MigrationId = result.MigrationId,
            MigrationType = migrationType,
            StartedUtc = result.StartedUtc,
            CompletedUtc = result.CompletedUtc,
            RecordedUtc = DateTimeOffset.UtcNow,
            Succeeded = result.Succeeded,
            Status = result.Status,
            Message = result.Message,
            Keyspace = result.Keyspace,
            RouteKey = result.RouteKey,
            SourceShardId = result.SourceShardId,
            DestinationShardId = result.DestinationShardId,
            MapVersion = result.MapVersion,
            PendingMapVersion = result.PendingMapVersion,
            RequiresRestart = result.RequiresRestart,
            Operator = operatorName,
            Comment = comment,
            Tables = result.Tables.Select(CloneMigrationTableResult).ToList(),
            Collections = result.Collections.Select(CloneMigrationCollectionResult).ToList(),
            Issues = result.Issues.Select(CloneIssue).ToList(),
        };

    private static CSharpDbShardMigrationTableResult CloneMigrationTableResult(CSharpDbShardMigrationTableResult value)
        => new()
        {
            TableName = value.TableName,
            SourceRows = value.SourceRows,
            DestinationRows = value.DestinationRows,
            RowsCopied = value.RowsCopied,
            SourceRowsDeleted = value.SourceRowsDeleted,
            Verified = value.Verified,
            SourceChecksum = value.SourceChecksum,
            DestinationChecksum = value.DestinationChecksum,
            Error = value.Error,
        };

    private static CSharpDbShardMigrationCollectionResult CloneMigrationCollectionResult(CSharpDbShardMigrationCollectionResult value)
        => new()
        {
            CollectionName = value.CollectionName,
            SourceDocuments = value.SourceDocuments,
            DestinationDocuments = value.DestinationDocuments,
            DocumentsCopied = value.DocumentsCopied,
            SourceDocumentsDeleted = value.SourceDocumentsDeleted,
            Verified = value.Verified,
            SourceChecksum = value.SourceChecksum,
            DestinationChecksum = value.DestinationChecksum,
            Error = value.Error,
        };

    private static CSharpDbShardCatalogIssue CloneIssue(CSharpDbShardCatalogIssue value)
        => new()
        {
            Severity = value.Severity,
            Code = value.Code,
            Message = value.Message,
        };

    private static void ValidateMigrationManifest(
        CSharpDbShardMigrationManifest manifest,
        List<CSharpDbShardCatalogIssue> issues)
    {
        if (manifest.PageSize <= 0)
        {
            issues.Add(CreateMigrationIssue(
                "invalid-page-size",
                "Migration manifest PageSize must be greater than 0."));
        }

        if (manifest.Tables.Count == 0 && manifest.Collections.Count == 0)
        {
            issues.Add(CreateMigrationIssue(
                "missing-manifest-items",
                "Exact route-key migration requires at least one table or collection manifest item."));
        }

        foreach (CSharpDbShardMigrationTableManifest table in manifest.Tables)
        {
            ValidateRequired(table.TableName, "table name", issues);
            ValidateRequired(table.RouteKeyColumn, $"table '{table.TableName}' route-key column", issues);
            ValidateRequired(table.PrimaryKeyColumn, $"table '{table.TableName}' primary-key column", issues);
        }

        foreach (CSharpDbShardMigrationCollectionManifest collection in manifest.Collections)
        {
            ValidateRequired(collection.CollectionName, "collection name", issues);
            ValidateRequired(collection.RouteKeyPropertyName, $"collection '{collection.CollectionName}' route-key property", issues);
        }
    }

    private static void ValidateRequired(
        string? value,
        string name,
        List<CSharpDbShardCatalogIssue> issues)
    {
        if (string.IsNullOrWhiteSpace(value))
            issues.Add(CreateMigrationIssue("missing-manifest-value", $"Migration manifest requires {name}."));
    }

    private static CSharpDbShardCatalogIssue CreateMigrationIssue(string code, string message)
        => new()
        {
            Severity = CSharpDbShardCatalogIssueSeverity.Error,
            Code = code,
            Message = message,
        };

    private static bool HasErrors(IEnumerable<CSharpDbShardCatalogIssue> issues)
        => issues.Any(issue => issue.Severity == CSharpDbShardCatalogIssueSeverity.Error);

    private static bool IsReplicaRole(string role)
        => string.Equals(role, CSharpDbShardRoles.Replica, StringComparison.OrdinalIgnoreCase);

    private static void ValidateReadOnlyFanOutSql(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            throw new CSharpDbClientException("Read-only cross-shard SQL requires at least one statement.");

        try
        {
            IReadOnlyList<string> statements = SqlScriptSplitter.SplitExecutableStatements(sql);
            if (statements.Count == 0)
                throw new CSharpDbClientException("Read-only cross-shard SQL requires at least one statement.");

            foreach (string statementSql in statements)
            {
                SqlStatementClassification classification = SqlStatementClassifier.Classify(statementSql);
                if (classification.IsReadOnly)
                    continue;

                throw new CSharpDbClientException(
                    "Read-only cross-shard SQL accepts only SELECT, WITH, and EXPLAIN ESTIMATE statements. Use ExecuteSqlOnAllShardsAsync only for explicit admin schema setup.");
            }
        }
        catch (CSharpDB.Primitives.CSharpDbException ex)
        {
            throw new CSharpDbClientException($"Read-only cross-shard SQL was invalid: {ex.Message}", ex);
        }
    }

    private static int NormalizeMigrationPageSize(int pageSize)
        => Math.Clamp(pageSize, 1, 5000);

    private static string NormalizeRequired(string value, string name)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new ArgumentException($"{name} is required.", name);
        return value.Trim();
    }

    private static ColumnDefinition FindColumn(TableSchema schema, string columnName)
        => schema.Columns.FirstOrDefault(column => string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase))
           ?? throw new CSharpDbClientException($"Column '{columnName}' was not found in table '{schema.TableName}'.");

    private static Dictionary<string, object?> ToRowDictionary(TableSchema schema, object?[] row)
    {
        var values = new Dictionary<string, object?>(schema.Columns.Count, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < schema.Columns.Count; i++)
            values[schema.Columns[i].Name] = i < row.Length ? row[i] : null;
        return values;
    }

    private static object GetRequiredRowValue(
        IReadOnlyDictionary<string, object?> row,
        string columnName,
        string tableName)
        => row.TryGetValue(columnName, out object? value) && value is not null
            ? value
            : throw new CSharpDbClientException($"Migrated row in table '{tableName}' has a null primary-key value in column '{columnName}'.");

    private static bool RouteValueMatches(object? value, string routeKey)
        => TryGetRouteValue(value, out string? normalized) &&
           string.Equals(normalized, routeKey, StringComparison.Ordinal);

    private static bool DocumentRouteValueMatches(JsonElement document, string routePropertyName, string routeKey)
        => TryGetDocumentRouteValue(document, routePropertyName, out string? normalized) &&
           string.Equals(normalized, routeKey, StringComparison.Ordinal);

    private bool ShouldMoveBucketRangeRouteValue(
        object? value,
        int startBucketInclusive,
        int endBucketExclusive)
        => TryGetRouteValue(value, out string? routeKey) &&
           routeKey is not null &&
           ShouldMoveBucketRangeRouteKey(routeKey, startBucketInclusive, endBucketExclusive);

    private bool ShouldMoveBucketRangeRouteKey(
        string routeKey,
        int startBucketInclusive,
        int endBucketExclusive)
    {
        if (_map.HasExactKeyPin(routeKey))
            return false;

        int bucket = _map.GetBucket(routeKey);
        return bucket >= startBucketInclusive && bucket < endBucketExclusive;
    }

    private static bool TryGetRouteValue(object? value, out string? routeKey)
    {
        routeKey = value switch
        {
            null => null,
            string text => text,
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString(),
        };

        if (string.IsNullOrWhiteSpace(routeKey))
        {
            routeKey = null;
            return false;
        }

        routeKey = routeKey.Trim();
        return true;
    }

    private static bool TryGetDocumentRouteValue(JsonElement document, string routePropertyName, out string? routeKey)
    {
        routeKey = null;
        if (document.ValueKind != JsonValueKind.Object ||
            !document.TryGetProperty(routePropertyName, out JsonElement routeValue))
        {
            return false;
        }

        routeKey = routeValue.ValueKind == JsonValueKind.String
            ? routeValue.GetString()
            : routeValue.ToString();
        if (string.IsNullOrWhiteSpace(routeKey))
        {
            routeKey = null;
            return false;
        }

        routeKey = routeKey.Trim();
        return true;
    }

    private static string ComputeTableChecksum(
        IReadOnlyList<Dictionary<string, object?>> rows,
        IReadOnlyList<ColumnDefinition> columns,
        string primaryKeyColumn)
    {
        var projected = rows
            .OrderBy(row => CanonicalValue(row.TryGetValue(primaryKeyColumn, out object? value) ? value : null), StringComparer.Ordinal)
            .Select(row => columns.Select(column => row.TryGetValue(column.Name, out object? value) ? NormalizeChecksumValue(value) : null).ToArray())
            .ToArray();

        return ComputeJsonChecksum(projected);
    }

    private static string ComputeCollectionChecksum(IReadOnlyList<CollectionDocument> documents)
    {
        var projected = documents
            .OrderBy(document => document.Key, StringComparer.Ordinal)
            .Select(document => new object?[] { document.Key, document.Document.GetRawText() })
            .ToArray();

        return ComputeJsonChecksum(projected);
    }

    private static object? NormalizeChecksumValue(object? value)
        => value switch
        {
            null => null,
            byte[] bytes => Convert.ToBase64String(bytes),
            JsonElement json => json.GetRawText(),
            _ => value,
        };

    private static string CanonicalValue(object? value)
        => value switch
        {
            null => string.Empty,
            byte[] bytes => Convert.ToBase64String(bytes),
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty,
        };

    private static string ComputeJsonChecksum<T>(T value)
    {
        byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(value);
        return Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
    }

    private static string CreateMigrationId(DateTimeOffset startedUtc)
        => $"csdbmig-{startedUtc:yyyyMMddHHmmss}-{Guid.NewGuid():N}";

    private static string FormatBucketRangeRouteKey(int startBucketInclusive, int endBucketExclusive)
        => $"bucket-range:[{startBucketInclusive},{endBucketExclusive})";

    private static string BuildFenceKey(string keyspace, string routeKey)
        => $"{keyspace.Length}:{keyspace}|{routeKey.Length}:{routeKey}";

    private static string BuildBucketRangeFenceKey(string keyspace, int startBucketInclusive, int endBucketExclusive)
        => $"{keyspace.Length}:{keyspace}|{startBucketInclusive}:{endBucketExclusive}";

    private bool TryAddBucketRangeFence(string fenceKey, BucketRangeFence fence)
    {
        if (_bucketRangeFences.Values.Any(existing => existing.Overlaps(fence)))
            return false;

        return _bucketRangeFences.TryAdd(fenceKey, fence);
    }

    private void ThrowIfRouteFenced(CSharpDbRouteContext routeContext)
    {
        var (keyspace, routeKey) = CSharpDbShardMap.NormalizeRoute(routeContext);
        if (_writeFences.ContainsKey(BuildFenceKey(keyspace, routeKey)))
        {
            throw new CSharpDbClientException(
                $"Route key '{routeKey}' in keyspace '{keyspace}' is fenced by an active shard migration.");
        }

        if (_map.HasExactKeyPin(routeKey))
            return;

        int bucket = _map.GetBucket(routeKey);
        foreach (BucketRangeFence fence in _bucketRangeFences.Values)
        {
            if (fence.Contains(keyspace, bucket))
            {
                throw new CSharpDbClientException(
                    $"Route key '{routeKey}' in keyspace '{keyspace}' is fenced by an active shard bucket-range migration.");
            }
        }
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
            => ResolveWritableClient().InsertRowAsync(tableName, values, ct);

        public Task<int> UpdateRowAsync(string tableName, string pkColumn, object pkValue, Dictionary<string, object?> values, CancellationToken ct = default)
            => ResolveWritableClient().UpdateRowAsync(tableName, pkColumn, pkValue, values, ct);

        public Task<int> DeleteRowAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default)
            => ResolveWritableClient().DeleteRowAsync(tableName, pkColumn, pkValue, ct);

        public Task DropTableAsync(string tableName, CancellationToken ct = default)
            => ResolveWritableClient().DropTableAsync(tableName, ct);

        public Task RenameTableAsync(string tableName, string newTableName, CancellationToken ct = default)
            => ResolveWritableClient().RenameTableAsync(tableName, newTableName, ct);

        public Task AddColumnAsync(string tableName, string columnName, DbType type, bool notNull, CancellationToken ct = default)
            => ResolveWritableClient().AddColumnAsync(tableName, columnName, type, notNull, ct);

        public Task AddColumnAsync(string tableName, string columnName, DbType type, bool notNull, string? collation, CancellationToken ct = default)
            => ResolveWritableClient().AddColumnAsync(tableName, columnName, type, notNull, collation, ct);

        public Task DropColumnAsync(string tableName, string columnName, CancellationToken ct = default)
            => ResolveWritableClient().DropColumnAsync(tableName, columnName, ct);

        public Task RenameColumnAsync(string tableName, string oldColumnName, string newColumnName, CancellationToken ct = default)
            => ResolveWritableClient().RenameColumnAsync(tableName, oldColumnName, newColumnName, ct);

        public Task<IReadOnlyList<IndexSchema>> GetIndexesAsync(CancellationToken ct = default)
            => ResolveClient().GetIndexesAsync(ct);

        public Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default)
            => ResolveWritableClient().CreateIndexAsync(indexName, tableName, columnName, isUnique, ct);

        public Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, string? collation, CancellationToken ct = default)
            => ResolveWritableClient().CreateIndexAsync(indexName, tableName, columnName, isUnique, collation, ct);

        public Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default)
            => ResolveWritableClient().UpdateIndexAsync(existingIndexName, newIndexName, tableName, columnName, isUnique, ct);

        public Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique, string? collation, CancellationToken ct = default)
            => ResolveWritableClient().UpdateIndexAsync(existingIndexName, newIndexName, tableName, columnName, isUnique, collation, ct);

        public Task DropIndexAsync(string indexName, CancellationToken ct = default)
            => ResolveWritableClient().DropIndexAsync(indexName, ct);

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
            => ResolveWritableClient().CreateViewAsync(viewName, selectSql, ct);

        public Task UpdateViewAsync(string existingViewName, string newViewName, string selectSql, CancellationToken ct = default)
            => ResolveWritableClient().UpdateViewAsync(existingViewName, newViewName, selectSql, ct);

        public Task DropViewAsync(string viewName, CancellationToken ct = default)
            => ResolveWritableClient().DropViewAsync(viewName, ct);

        public Task<IReadOnlyList<TriggerSchema>> GetTriggersAsync(CancellationToken ct = default)
            => ResolveClient().GetTriggersAsync(ct);

        public Task CreateTriggerAsync(string triggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default)
            => ResolveWritableClient().CreateTriggerAsync(triggerName, tableName, timing, triggerEvent, bodySql, ct);

        public Task UpdateTriggerAsync(string existingTriggerName, string newTriggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default)
            => ResolveWritableClient().UpdateTriggerAsync(existingTriggerName, newTriggerName, tableName, timing, triggerEvent, bodySql, ct);

        public Task DropTriggerAsync(string triggerName, CancellationToken ct = default)
            => ResolveWritableClient().DropTriggerAsync(triggerName, ct);

        public Task<IReadOnlyList<SavedQueryDefinition>> GetSavedQueriesAsync(CancellationToken ct = default)
            => ResolveClient().GetSavedQueriesAsync(ct);

        public Task<SavedQueryDefinition?> GetSavedQueryAsync(string name, CancellationToken ct = default)
            => ResolveClient().GetSavedQueryAsync(name, ct);

        public Task<SavedQueryDefinition> UpsertSavedQueryAsync(string name, string sqlText, CancellationToken ct = default)
            => ResolveWritableClient().UpsertSavedQueryAsync(name, sqlText, ct);

        public Task DeleteSavedQueryAsync(string name, CancellationToken ct = default)
            => ResolveWritableClient().DeleteSavedQueryAsync(name, ct);

        public Task<IReadOnlyList<ProcedureDefinition>> GetProceduresAsync(bool includeDisabled = true, CancellationToken ct = default)
            => ResolveClient().GetProceduresAsync(includeDisabled, ct);

        public Task<ProcedureDefinition?> GetProcedureAsync(string name, CancellationToken ct = default)
            => ResolveClient().GetProcedureAsync(name, ct);

        public Task CreateProcedureAsync(ProcedureDefinition definition, CancellationToken ct = default)
            => ResolveWritableClient().CreateProcedureAsync(definition, ct);

        public Task UpdateProcedureAsync(string existingName, ProcedureDefinition definition, CancellationToken ct = default)
            => ResolveWritableClient().UpdateProcedureAsync(existingName, definition, ct);

        public Task DeleteProcedureAsync(string name, CancellationToken ct = default)
            => ResolveWritableClient().DeleteProcedureAsync(name, ct);

        public Task<ProcedureExecutionResult> ExecuteProcedureAsync(string name, IReadOnlyDictionary<string, object?> args, CancellationToken ct = default)
            => ResolveWritableClient().ExecuteProcedureAsync(name, args, ct);

        public Task<SqlExecutionResult> ExecuteSqlAsync(string sql, CancellationToken ct = default)
            => ResolveWritableClient().ExecuteSqlAsync(sql, ct);

        public async Task<TransactionSessionInfo> BeginTransactionAsync(CancellationToken ct = default)
        {
            CSharpDbRouteContext routeContext = GetRequiredRoute();
            _owner.ThrowIfRouteFenced(routeContext);
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
            if (route is not null)
                _owner.ThrowIfRouteFenced(route);
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
            => ResolveWritableClient().PutDocumentAsync(collectionName, key, document, ct);

        public Task<bool> DeleteDocumentAsync(string collectionName, string key, CancellationToken ct = default)
            => ResolveWritableClient().DeleteDocumentAsync(collectionName, key, ct);

        public Task DropCollectionAsync(string collectionName, CancellationToken ct = default)
            => ResolveWritableClient().DropCollectionAsync(collectionName, ct);

        public Task CheckpointAsync(CancellationToken ct = default)
            => ResolveWritableClient().CheckpointAsync(ct);

        public Task<BackupResult> BackupAsync(BackupRequest request, CancellationToken ct = default)
            => ResolveClient().BackupAsync(request, ct);

        public Task<RestoreResult> RestoreAsync(RestoreRequest request, CancellationToken ct = default)
            => ResolveWritableClient().RestoreAsync(request, ct);

        public Task<ForeignKeyMigrationResult> MigrateForeignKeysAsync(ForeignKeyMigrationRequest request, CancellationToken ct = default)
            => ResolveWritableClient().MigrateForeignKeysAsync(request, ct);

        public Task<DatabaseMaintenanceReport> GetMaintenanceReportAsync(CancellationToken ct = default)
            => ResolveClient().GetMaintenanceReportAsync(ct);

        public Task<ReindexResult> ReindexAsync(ReindexRequest request, CancellationToken ct = default)
            => ResolveWritableClient().ReindexAsync(request, ct);

        public Task<VacuumResult> VacuumAsync(CancellationToken ct = default)
            => ResolveWritableClient().VacuumAsync(ct);

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

        private ICSharpDbClient ResolveWritableClient()
        {
            CSharpDbRouteContext route = GetRequiredRoute();
            _owner.ThrowIfRouteFenced(route);
            return _owner.ResolveClient(route);
        }

        private CSharpDbRouteContext GetRequiredRoute()
            => GetOptionalRoute()
               ?? throw new CSharpDbClientException(
                   "A CSharpDB route context is required for sharded operations. Supply X-CSharpDB-Keyspace and X-CSharpDB-Shard-Key for remote calls or use CSharpDbShardedClient.ForRoute locally.");

        private CSharpDbRouteContext? GetOptionalRoute()
            => _fixedRoute ?? _owner.GetCurrentRoute();
    }

    private sealed record RouteTableRows(
        TableSchema Schema,
        string RouteKeyColumn,
        string PrimaryKeyColumn,
        List<Dictionary<string, object?>> Rows);

    private sealed record BucketRangeFence(string Keyspace, int StartBucketInclusive, int EndBucketExclusive)
    {
        public bool Contains(string keyspace, int bucket)
            => string.Equals(Keyspace, keyspace, StringComparison.OrdinalIgnoreCase) &&
               bucket >= StartBucketInclusive &&
               bucket < EndBucketExclusive;

        public bool Overlaps(BucketRangeFence other)
            => string.Equals(Keyspace, other.Keyspace, StringComparison.OrdinalIgnoreCase) &&
               StartBucketInclusive < other.EndBucketExclusive &&
               other.StartBucketInclusive < EndBucketExclusive;
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

        public bool CanWrite => _options.AllowWrites;

        public static CSharpDbShardingOptions CloneOptionsForRuntime(CSharpDbShardingOptions options)
            => CloneOptions(options, includeRuntimeOptions: true);

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

        public async Task<IReadOnlyList<CSharpDbShardMigrationHistoryEntry>> GetMigrationHistoryAsync(CancellationToken ct)
        {
            if (!File.Exists(_path))
                return [];

            CSharpDbShardCatalogDocument document = await ReadDocumentAsync(_path, ct).ConfigureAwait(false);
            return document.MigrationHistory
                .OrderByDescending(entry => entry.CompletedUtc)
                .Select(CloneMigrationHistoryEntry)
                .ToList();
        }

        public async Task AppendMigrationHistoryAsync(
            CSharpDbShardingOptions activeOptions,
            CSharpDbShardMigrationHistoryEntry entry,
            CancellationToken ct)
        {
            if (!_options.AllowWrites)
                throw new CSharpDbClientConfigurationException("Shard catalog writes are disabled by configuration.");

            CSharpDbShardCatalogDocument document = File.Exists(_path)
                ? await ReadDocumentAsync(_path, ct).ConfigureAwait(false)
                : new CSharpDbShardCatalogDocument
                {
                    ActiveMap = CloneOptions(activeOptions, includeRuntimeOptions: false),
                };

            document.MigrationHistory.Add(CloneMigrationHistoryEntry(entry));
            await WriteDocumentAsync(document, ct).ConfigureAwait(false);
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

            await WriteDocumentAsync(document, ct).ConfigureAwait(false);

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

        private async Task WriteDocumentAsync(CSharpDbShardCatalogDocument document, CancellationToken ct)
        {
            string? directory = Path.GetDirectoryName(_path);
            if (!string.IsNullOrWhiteSpace(directory))
                Directory.CreateDirectory(directory);

            await using FileStream stream = File.Create(_path);
            await JsonSerializer.SerializeAsync(stream, document, s_jsonOptions, ct).ConfigureAwait(false);
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
                Role = shard.Role,
                PrimaryShardId = shard.PrimaryShardId,
                PromotionEligible = shard.PromotionEligible,
                ReplicationLagBytes = shard.ReplicationLagBytes,
                LastReplicatedUtc = shard.LastReplicatedUtc,
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

        private static CSharpDbShardMigrationHistoryEntry CloneMigrationHistoryEntry(CSharpDbShardMigrationHistoryEntry entry)
            => new()
            {
                MigrationId = entry.MigrationId,
                MigrationType = entry.MigrationType,
                StartedUtc = entry.StartedUtc,
                CompletedUtc = entry.CompletedUtc,
                RecordedUtc = entry.RecordedUtc,
                Succeeded = entry.Succeeded,
                Status = entry.Status,
                Message = entry.Message,
                Keyspace = entry.Keyspace,
                RouteKey = entry.RouteKey,
                SourceShardId = entry.SourceShardId,
                DestinationShardId = entry.DestinationShardId,
                MapVersion = entry.MapVersion,
                PendingMapVersion = entry.PendingMapVersion,
                RequiresRestart = entry.RequiresRestart,
                Operator = entry.Operator,
                Comment = entry.Comment,
                Tables = entry.Tables.Select(CloneMigrationTableResult).ToList(),
                Collections = entry.Collections.Select(CloneMigrationCollectionResult).ToList(),
                Issues = entry.Issues.Select(CloneIssue).ToList(),
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
            public List<CSharpDbShardMigrationHistoryEntry> MigrationHistory { get; set; } = [];
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
                    Role = NormalizeShardRole(shard.Role),
                    PrimaryShardId = string.IsNullOrWhiteSpace(shard.PrimaryShardId)
                        ? null
                        : NormalizeShardId(shard.PrimaryShardId),
                    PromotionEligible = shard.PromotionEligible,
                    ReplicationLagBytes = shard.ReplicationLagBytes,
                    LastReplicatedUtc = shard.LastReplicatedUtc,
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
            ValidateShardReplicationMetadata(normalizedShards, shardMap);

            string[] bucketOwners = BuildBucketOwners(options, normalizedShards, shardMap);
            var exactKeyPins = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (KeyValuePair<string, string> pin in options.ExactKeyPins ?? [])
            {
                string key = NormalizeNonEmpty(pin.Key, "ExactKeyPins key");
                string shardId = NormalizeShardId(pin.Value);
                if (!shardMap.TryGetValue(shardId, out CSharpDbShardDefinition? shard))
                    throw new CSharpDbClientConfigurationException($"Exact route-key pin '{key}' references unknown shard '{shardId}'.");
                ValidateRouteOwnerShard(shard, $"Exact route-key pin '{key}'");
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

        public bool HasExactKeyPin(string routeKey)
            => _exactKeyPins.ContainsKey(routeKey);

        public int GetBucket(string routeKey)
        {
            ulong token = ComputeRouteToken(new CSharpDbRouteContext { Keyspace = Keyspace, Key = routeKey });
            return (int)(token % (ulong)VirtualBucketCount);
        }

        public bool IsValidBucketRange(int startBucketInclusive, int endBucketExclusive)
            => startBucketInclusive >= 0 &&
               endBucketExclusive <= VirtualBucketCount &&
               startBucketInclusive < endBucketExclusive;

        public bool BucketRangeOwnedBy(int startBucketInclusive, int endBucketExclusive, string shardId)
        {
            if (!IsValidBucketRange(startBucketInclusive, endBucketExclusive))
                return false;

            for (int bucket = startBucketInclusive; bucket < endBucketExclusive; bucket++)
            {
                if (!string.Equals(_bucketOwners[bucket], shardId, StringComparison.OrdinalIgnoreCase))
                    return false;
            }

            return true;
        }

        public string[] CreateBucketOwnerSnapshot()
            => _bucketOwners.ToArray();

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
                Role = shard.Role,
                PrimaryShardId = shard.PrimaryShardId,
                PromotionEligible = shard.PromotionEligible,
                ReplicationLagBytes = shard.ReplicationLagBytes,
                LastReplicatedUtc = shard.LastReplicatedUtc,
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
                if (!shardMap.TryGetValue(shardId, out CSharpDbShardDefinition? shard))
                    throw new CSharpDbClientConfigurationException($"Bucket range references unknown shard '{shardId}'.");
                ValidateRouteOwnerShard(shard, $"Bucket range [{range.StartBucketInclusive}, {range.EndBucketExclusive})");
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

        private static string NormalizeShardRole(string? role)
        {
            string normalized = NormalizeNonEmpty(role ?? CSharpDbShardRoles.Primary, nameof(CSharpDbShardDefinition.Role));
            if (string.Equals(normalized, CSharpDbShardRoles.Primary, StringComparison.OrdinalIgnoreCase))
                return CSharpDbShardRoles.Primary;
            if (string.Equals(normalized, CSharpDbShardRoles.Replica, StringComparison.OrdinalIgnoreCase))
                return CSharpDbShardRoles.Replica;

            throw new CSharpDbClientConfigurationException(
                $"Shard role '{normalized}' is invalid. Supported roles are '{CSharpDbShardRoles.Primary}' and '{CSharpDbShardRoles.Replica}'.");
        }

        private static bool IsReplicaRole(string role)
            => string.Equals(role, CSharpDbShardRoles.Replica, StringComparison.OrdinalIgnoreCase);

        private static void ValidateShardReplicationMetadata(
            IReadOnlyList<CSharpDbShardDefinition> shards,
            IReadOnlyDictionary<string, CSharpDbShardDefinition> shardMap)
        {
            foreach (CSharpDbShardDefinition shard in shards)
            {
                if (shard.ReplicationLagBytes is < 0)
                    throw new CSharpDbClientConfigurationException($"Shard '{shard.ShardId}' ReplicationLagBytes cannot be negative.");

                if (IsReplicaRole(shard.Role))
                {
                    string? primaryShardId = shard.PrimaryShardId is null ? null : NormalizeShardId(shard.PrimaryShardId);
                    if (string.IsNullOrWhiteSpace(primaryShardId))
                        throw new CSharpDbClientConfigurationException($"Replica shard '{shard.ShardId}' requires PrimaryShardId.");
                    if (string.Equals(primaryShardId, shard.ShardId, StringComparison.OrdinalIgnoreCase))
                        throw new CSharpDbClientConfigurationException($"Replica shard '{shard.ShardId}' cannot reference itself as PrimaryShardId.");
                    if (!shardMap.TryGetValue(primaryShardId, out CSharpDbShardDefinition? primary))
                        throw new CSharpDbClientConfigurationException($"Replica shard '{shard.ShardId}' references unknown primary shard '{primaryShardId}'.");
                    if (!string.Equals(primary.Role, CSharpDbShardRoles.Primary, StringComparison.OrdinalIgnoreCase))
                        throw new CSharpDbClientConfigurationException($"Replica shard '{shard.ShardId}' PrimaryShardId '{primaryShardId}' must reference a primary shard.");
                    continue;
                }

                if (!string.IsNullOrWhiteSpace(shard.PrimaryShardId))
                    throw new CSharpDbClientConfigurationException($"Primary shard '{shard.ShardId}' cannot set PrimaryShardId.");
                if (shard.PromotionEligible)
                    throw new CSharpDbClientConfigurationException($"Primary shard '{shard.ShardId}' cannot be marked PromotionEligible.");
                if (shard.ReplicationLagBytes is not null)
                    throw new CSharpDbClientConfigurationException($"Primary shard '{shard.ShardId}' cannot set ReplicationLagBytes.");
                if (shard.LastReplicatedUtc is not null)
                    throw new CSharpDbClientConfigurationException($"Primary shard '{shard.ShardId}' cannot set LastReplicatedUtc.");
            }
        }

        private static void ValidateRouteOwnerShard(CSharpDbShardDefinition shard, string source)
        {
            if (string.Equals(shard.Role, CSharpDbShardRoles.Primary, StringComparison.OrdinalIgnoreCase))
                return;

            throw new CSharpDbClientConfigurationException(
                $"{source} references shard '{shard.ShardId}', but only primary shards can own route buckets or exact route-key pins.");
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
