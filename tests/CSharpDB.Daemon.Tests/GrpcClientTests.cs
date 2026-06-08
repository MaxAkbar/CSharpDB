using System.Net;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Client;
using CSharpDB.Client.Grpc;
using CSharpDB.Client.Models;
using CSharpDB.Daemon.Configuration;
using CSharpDB.Engine;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Daemon.Tests;

public sealed class GrpcClientTests : IAsyncLifetime
{
    private string _dbPath = null!;
    private TestDaemonFactory _factory = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public ValueTask InitializeAsync()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_daemon_grpc_{Guid.NewGuid():N}.db");
        _factory = new TestDaemonFactory(_dbPath);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await _factory.DisposeAsync();
        TryDelete(_dbPath);
        TryDelete(_dbPath + ".wal");
    }

    [Fact]
    public void Daemon_DefaultHostDatabaseOptions_EnableHybridConcurrentInsertAndWritePreset()
    {
        DaemonHostDatabaseOptions hostOptions = GetResolvedHostDatabaseOptions(_factory);
        CSharpDbClientOptions clientOptions = GetResolvedClientOptions(_factory);

        Assert.Equal(DaemonHostOpenMode.HybridIncrementalDurable, hostOptions.OpenMode);
        Assert.Equal(ImplicitInsertExecutionMode.ConcurrentWriteTransactions, hostOptions.ImplicitInsertExecutionMode);
        Assert.True(hostOptions.UseWriteOptimizedPreset);
        Assert.Empty(hostOptions.HotTableNames);
        Assert.Empty(hostOptions.HotCollectionNames);

        Assert.Equal(CSharpDbTransport.Direct, clientOptions.Transport);
        Assert.NotNull(clientOptions.DirectDatabaseOptions);
        Assert.NotNull(clientOptions.HybridDatabaseOptions);

        DatabaseOptions directOptions = clientOptions.DirectDatabaseOptions!;
        Assert.Equal(ImplicitInsertExecutionMode.ConcurrentWriteTransactions, directOptions.ImplicitInsertExecutionMode);

        PagerOptions pagerOptions = directOptions.StorageEngineOptions.PagerOptions;
        var checkpointPolicy = Assert.IsType<FrameCountCheckpointPolicy>(pagerOptions.CheckpointPolicy);
        Assert.Equal(4096, checkpointPolicy.Threshold);
        Assert.Equal(AutoCheckpointExecutionMode.Background, pagerOptions.AutoCheckpointExecutionMode);

        HybridDatabaseOptions hybridOptions = clientOptions.HybridDatabaseOptions!;
        Assert.Equal(HybridPersistenceMode.IncrementalDurable, hybridOptions.PersistenceMode);
        Assert.Empty(hybridOptions.HotTableNames);
        Assert.Empty(hybridOptions.HotCollectionNames);
    }

    [Fact]
    public void Daemon_HostDatabaseOpenModeDirect_DisablesHybridOpen()
    {
        using var factory = new TestDaemonFactory(
            _dbPath,
            new Dictionary<string, string?>
            {
                ["CSharpDB:HostDatabase:OpenMode"] = "Direct",
            });

        DaemonHostDatabaseOptions hostOptions = GetResolvedHostDatabaseOptions(factory);
        CSharpDbClientOptions clientOptions = GetResolvedClientOptions(factory);

        Assert.Equal(DaemonHostOpenMode.Direct, hostOptions.OpenMode);
        Assert.Equal(CSharpDbTransport.Direct, clientOptions.Transport);
        Assert.NotNull(clientOptions.DirectDatabaseOptions);
        Assert.Null(clientOptions.HybridDatabaseOptions);
    }

    [Fact]
    public void Daemon_ImplicitInsertExecutionModeOverride_IsRespected()
    {
        using var factory = new TestDaemonFactory(
            _dbPath,
            new Dictionary<string, string?>
            {
                ["CSharpDB:HostDatabase:ImplicitInsertExecutionMode"] = "Serialized",
            });

        DaemonHostDatabaseOptions hostOptions = GetResolvedHostDatabaseOptions(factory);
        CSharpDbClientOptions clientOptions = GetResolvedClientOptions(factory);

        Assert.Equal(ImplicitInsertExecutionMode.Serialized, hostOptions.ImplicitInsertExecutionMode);
        Assert.NotNull(clientOptions.DirectDatabaseOptions);
        Assert.Equal(
            ImplicitInsertExecutionMode.Serialized,
            clientOptions.DirectDatabaseOptions!.ImplicitInsertExecutionMode);
    }

    [Fact]
    public async Task Daemon_HybridHotSetOverrides_FlowIntoClientOptions()
    {
        await SeedHybridHotSetDatabaseAsync(_dbPath);

        using var factory = new TestDaemonFactory(
            _dbPath,
            new Dictionary<string, string?>
            {
                ["CSharpDB:HostDatabase:HotTableNames:0"] = "users",
                ["CSharpDB:HostDatabase:HotTableNames:1"] = "sessions",
                ["CSharpDB:HostDatabase:HotCollectionNames:0"] = "session_cache",
            });

        DaemonHostDatabaseOptions hostOptions = GetResolvedHostDatabaseOptions(factory);
        CSharpDbClientOptions clientOptions = GetResolvedClientOptions(factory);

        Assert.Equal(["users", "sessions"], hostOptions.HotTableNames);
        Assert.Equal(["session_cache"], hostOptions.HotCollectionNames);

        HybridDatabaseOptions hybridOptions = Assert.IsType<HybridDatabaseOptions>(clientOptions.HybridDatabaseOptions);
        Assert.Equal(HybridPersistenceMode.IncrementalDurable, hybridOptions.PersistenceMode);
        Assert.Equal(["users", "sessions"], hybridOptions.HotTableNames);
        Assert.Equal(["session_cache"], hybridOptions.HotCollectionNames);
    }

    [Fact]
    public async Task GrpcClient_RowCrud_RoundTripsPrimitiveValues()
    {
        using var transportClient = CreateGrpcHttpClient();
        await using var client = CreateGrpcClient(transportClient);

        DatabaseInfo info = await client.GetInfoAsync(Ct);
        Assert.Equal(Path.GetFullPath(_dbPath), info.DataSource);

        SqlExecutionResult createResult = await client.ExecuteSqlAsync(
            "CREATE TABLE grpc_users (id INTEGER PRIMARY KEY, name TEXT, score REAL)",
            Ct);
        Assert.Null(createResult.Error);

        int inserted = await client.InsertRowAsync(
            "grpc_users",
            new Dictionary<string, object?>
            {
                ["id"] = 7,
                ["name"] = "seven",
                ["score"] = 12.5,
            },
            Ct);
        Assert.Equal(1, inserted);

        Dictionary<string, object?>? row = await client.GetRowByPkAsync("grpc_users", "id", 7, Ct);
        Assert.NotNull(row);
        Assert.Equal(7L, Assert.IsType<long>(row["id"]));
        Assert.Equal("seven", Assert.IsType<string>(row["name"]));
        Assert.Equal(12.5, Assert.IsType<double>(row["score"]));

        TableBrowseResult browse = await client.BrowseTableAsync("grpc_users", ct: Ct);
        Assert.Single(browse.Rows);
        Assert.Equal(7L, Assert.IsType<long>(browse.Rows[0][0]));
        Assert.Equal("seven", Assert.IsType<string>(browse.Rows[0][1]));
        Assert.Equal(12.5, Assert.IsType<double>(browse.Rows[0][2]));
    }

    [Fact]
    public async Task Daemon_RestApi_IsServedByDefault()
    {
        using var transportClient = CreateHttpTransportClient();
        await using var client = CreateHttpClient(transportClient);

        DatabaseInfo info = await client.GetInfoAsync(Ct);

        Assert.Equal(Path.GetFullPath(_dbPath), info.DataSource);
    }

    [Fact]
    public async Task Daemon_ApiKeyModeRejectsMissingAndWrongKeysForRestAndGrpc()
    {
        const string secret = "daemon-secret-value";
        using var factory = new TestDaemonFactory(
            _dbPath,
            new Dictionary<string, string?>
            {
                ["CSharpDB:Daemon:Security:Mode"] = "ApiKey",
                ["CSharpDB:Daemon:Security:ApiKey"] = secret,
            });

        using var restTransportClient = factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            BaseAddress = new Uri("http://localhost"),
        });

        using HttpResponseMessage restMissing = await restTransportClient.GetAsync("/api/info", Ct);
        Assert.Equal(HttpStatusCode.Unauthorized, restMissing.StatusCode);
        string restMissingPayload = await restMissing.Content.ReadAsStringAsync(Ct);
        Assert.DoesNotContain(secret, restMissingPayload, StringComparison.Ordinal);

        using var restWrongRequest = new HttpRequestMessage(HttpMethod.Get, "/api/info");
        restWrongRequest.Headers.TryAddWithoutValidation("X-CSharpDB-Api-Key", "wrong-secret");
        using HttpResponseMessage restWrong = await restTransportClient.SendAsync(restWrongRequest, Ct);
        Assert.Equal(HttpStatusCode.Unauthorized, restWrong.StatusCode);

        using var grpcTransportClient = CreateGrpcHttpClient(factory);
        using var channel = GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions
        {
            HttpClient = grpcTransportClient,
            DisposeHttpClient = false,
        });

        var rpcClient = new CSharpDbRpc.CSharpDbRpcClient(channel);

        var missingGrpc = await Assert.ThrowsAsync<RpcException>(
            () => rpcClient.GetInfoAsync(new Empty(), cancellationToken: Ct).ResponseAsync);
        Assert.Equal(StatusCode.Unauthenticated, missingGrpc.StatusCode);

        var wrongGrpc = await Assert.ThrowsAsync<RpcException>(
            () => rpcClient.GetInfoAsync(
                new Empty(),
                headers: new Metadata { { "x-csharpdb-api-key", "wrong-secret" } },
                cancellationToken: Ct).ResponseAsync);
        Assert.Equal(StatusCode.Unauthenticated, wrongGrpc.StatusCode);
    }

    [Fact]
    public async Task Daemon_ApiKeyModeAcceptsCorrectKeyForRestAndGrpcClients()
    {
        const string secret = "daemon-client-secret";
        using var factory = new TestDaemonFactory(
            _dbPath,
            new Dictionary<string, string?>
            {
                ["CSharpDB:Daemon:Security:Mode"] = "ApiKey",
                ["CSharpDB:Daemon:Security:ApiKey"] = secret,
            });

        using var grpcTransportClient = CreateGrpcHttpClient(factory);
        await using var grpcClient = CreateGrpcClient(grpcTransportClient, secret);

        DatabaseInfo grpcInfo = await grpcClient.GetInfoAsync(Ct);
        Assert.Equal(Path.GetFullPath(_dbPath), grpcInfo.DataSource);

        SqlExecutionResult create = await grpcClient.ExecuteSqlAsync(
            "CREATE TABLE daemon_auth (id INTEGER PRIMARY KEY);",
            Ct);
        Assert.Null(create.Error);

        using var restTransportClient = factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            BaseAddress = new Uri("http://localhost"),
        });
        await using var restClient = CreateHttpClient(restTransportClient, secret);

        DatabaseInfo restInfo = await restClient.GetInfoAsync(Ct);
        Assert.Equal(Path.GetFullPath(_dbPath), restInfo.DataSource);
        Assert.Contains("daemon_auth", await restClient.GetTableNamesAsync(Ct));
    }

    [Fact]
    public async Task Daemon_RestAndGrpcClients_ShareHostedDatabaseState()
    {
        using var grpcTransportClient = CreateGrpcHttpClient();
        await using var grpcClient = CreateGrpcClient(grpcTransportClient);

        using var httpTransportClient = CreateHttpTransportClient();
        await using var httpClient = CreateHttpClient(httpTransportClient);

        SqlExecutionResult createResult = await grpcClient.ExecuteSqlAsync(
            "CREATE TABLE daemon_shared_users (id INTEGER PRIMARY KEY, name TEXT)",
            Ct);
        Assert.Null(createResult.Error);

        await grpcClient.InsertRowAsync(
            "daemon_shared_users",
            new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "grpc" },
            Ct);

        Dictionary<string, object?>? grpcRowFromRest = await httpClient.GetRowByPkAsync(
            "daemon_shared_users",
            "id",
            1L,
            Ct);
        Assert.NotNull(grpcRowFromRest);
        Assert.Equal("grpc", Assert.IsType<string>(grpcRowFromRest!["name"]));

        await httpClient.InsertRowAsync(
            "daemon_shared_users",
            new Dictionary<string, object?> { ["id"] = 2L, ["name"] = "rest" },
            Ct);

        Dictionary<string, object?>? restRowFromGrpc = await grpcClient.GetRowByPkAsync(
            "daemon_shared_users",
            "id",
            2L,
            Ct);
        Assert.NotNull(restRowFromGrpc);
        Assert.Equal("rest", Assert.IsType<string>(restRowFromGrpc!["name"]));

        Assert.Equal(2, await grpcClient.GetRowCountAsync("daemon_shared_users", Ct));
    }

    [Fact]
    public async Task Daemon_ShardedRestAndGrpcClients_RouteByHeaderMetadata()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"csharpdb_daemon_shards_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            string masterDbPath = Path.Combine(directory, "master.db");
            await SeedMasterCatalogAsync(masterDbPath, CreateSeedShardingOptions(directory));
            using var factory = new TestDaemonFactory(masterDbPath);
            using var grpcTransportClient = CreateGrpcHttpClient(factory);
            using var httpTransportClient = factory.CreateClient(new WebApplicationFactoryClientOptions
            {
                BaseAddress = new Uri("http://localhost"),
            });

            await using var restTenantA = CreateHttpClient(
                httpTransportClient,
                routeContext: new CSharpDbRouteContext { Keyspace = "tenants", Key = "tenant-a" });
            await using var grpcTenantB = CreateGrpcClient(
                grpcTransportClient,
                routeContext: new CSharpDbRouteContext { Keyspace = "tenants", Key = "tenant-b" });

            Assert.Null((await restTenantA.ExecuteSqlAsync("CREATE TABLE routed_items (id INTEGER PRIMARY KEY, name TEXT);", Ct)).Error);
            Assert.Equal(1, await restTenantA.InsertRowAsync("routed_items", new Dictionary<string, object?>
            {
                ["id"] = 1L,
                ["name"] = "rest-a",
            }, Ct));

            Assert.Null((await grpcTenantB.ExecuteSqlAsync("CREATE TABLE routed_items (id INTEGER PRIMARY KEY, name TEXT);", Ct)).Error);
            Assert.Equal(1, await grpcTenantB.InsertRowAsync("routed_items", new Dictionary<string, object?>
            {
                ["id"] = 1L,
                ["name"] = "grpc-b",
            }, Ct));

            Dictionary<string, object?>? rowA = await restTenantA.GetRowByPkAsync("routed_items", "id", 1L, Ct);
            Dictionary<string, object?>? rowB = await grpcTenantB.GetRowByPkAsync("routed_items", "id", 1L, Ct);
            Assert.Equal("rest-a", Assert.IsType<string>(rowA!["name"]));
            Assert.Equal("grpc-b", Assert.IsType<string>(rowB!["name"]));

            await using var missingRoute = CreateGrpcClient(grpcTransportClient);
            await Assert.ThrowsAsync<CSharpDbClientException>(
                () => missingRoute.ExecuteSqlAsync("SELECT 1;", Ct));
        }
        finally
        {
            TryDelete(Path.Combine(directory, "s0.db"));
            TryDelete(Path.Combine(directory, "s0.db.wal"));
            TryDelete(Path.Combine(directory, "s1.db"));
            TryDelete(Path.Combine(directory, "s1.db.wal"));
            TryDelete(Path.Combine(directory, "unused.db"));
            TryDelete(Path.Combine(directory, "unused.db.wal"));
            try
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, recursive: true);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                // Ignore transient test cleanup file locks.
            }
        }
    }

    [Fact]
    public async Task Daemon_ShardAdminRestAndGrpcExposeMapStatusAndExecuteAll()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"csharpdb_daemon_shard_admin_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            string masterDbPath = Path.Combine(directory, "master.db");
            await SeedMasterCatalogAsync(masterDbPath, CreateSeedShardingOptions(directory));
            using var factory = new TestDaemonFactory(masterDbPath);
            using var grpcTransportClient = CreateGrpcHttpClient(factory);
            using var httpTransportClient = factory.CreateClient(new WebApplicationFactoryClientOptions
            {
                BaseAddress = new Uri("http://localhost"),
            });

            await using var restAdmin = CreateHttpShardAdmin(httpTransportClient);
            await using var grpcAdmin = CreateGrpcShardAdmin(grpcTransportClient);

            CSharpDbShardMapSnapshot map = await restAdmin.GetShardMapAsync(Ct);
            Assert.Equal("tenants", map.Keyspace);
            Assert.Equal(4, map.VirtualBucketCount);
            Assert.Equal(["s0", "s1"], map.Shards.Select(shard => shard.ShardId).ToArray());
            Assert.Equal("s0", map.ExactKeyPins["tenant-a"]);
            Assert.Empty(map.Directories);

            CSharpDbShardResolution resolution = await grpcAdmin.ResolveRouteAsync(new CSharpDbRouteContext
            {
                Keyspace = "tenants",
                Key = "tenant-b",
            }, Ct);
            Assert.Equal("s1", resolution.ShardId);

            IReadOnlyList<CSharpDbShardSqlExecutionResult> schemaResults =
                await grpcAdmin.ExecuteSqlOnAllShardsAsync(
                    "CREATE TABLE shard_admin_schema (id INTEGER PRIMARY KEY, name TEXT);",
                    Ct);
            Assert.Equal(2, schemaResults.Count);
            Assert.All(schemaResults, result =>
            {
                Assert.Null(result.Error);
                Assert.NotNull(result.Result);
                Assert.Null(result.Result!.Error);
            });

            IReadOnlyList<CSharpDbShardStatus> statuses = await restAdmin.GetShardStatusAsync(Ct);
            Assert.Equal(2, statuses.Count);
            Assert.All(statuses, status =>
            {
                Assert.True(status.Enabled);
                Assert.True(status.Healthy);
                Assert.NotNull(status.Info);
            });

            await using var tenantA = CreateHttpClient(
                httpTransportClient,
                routeContext: new CSharpDbRouteContext { Keyspace = "tenants", Key = "tenant-a" });
            await using var tenantB = CreateGrpcClient(
                grpcTransportClient,
                routeContext: new CSharpDbRouteContext { Keyspace = "tenants", Key = "tenant-b" });

            Assert.Contains("shard_admin_schema", await tenantA.GetTableNamesAsync(Ct));
            Assert.Contains("shard_admin_schema", await tenantB.GetTableNamesAsync(Ct));
            Assert.Equal(1, await tenantA.InsertRowAsync("shard_admin_schema", new Dictionary<string, object?>
            {
                ["id"] = 1L,
                ["name"] = "tenant-a",
            }, Ct));
            Assert.Equal(1, await tenantB.InsertRowAsync("shard_admin_schema", new Dictionary<string, object?>
            {
                ["id"] = 2L,
                ["name"] = "tenant-b",
            }, Ct));

            IReadOnlyList<CSharpDbShardSqlExecutionResult> readResults =
                await restAdmin.ExecuteReadOnlySqlOnAllShardsAsync(
                    "SELECT COUNT(*) FROM shard_admin_schema;",
                    Ct);
            Assert.Equal(2, readResults.Count);
            Assert.All(readResults, result =>
            {
                Assert.Null(result.Error);
                Assert.True(result.Result!.IsQuery);
                Assert.Equal(1L, Assert.IsType<long>(Assert.Single(result.Result.Rows!)[0]));
            });

            await Assert.ThrowsAsync<CSharpDbClientException>(
                () => grpcAdmin.ExecuteReadOnlySqlOnAllShardsAsync(
                    "CREATE TABLE read_all_rejected (id INTEGER PRIMARY KEY);",
                    Ct));
        }
        finally
        {
            TryDelete(Path.Combine(directory, "s0.db"));
            TryDelete(Path.Combine(directory, "s0.db.wal"));
            TryDelete(Path.Combine(directory, "s1.db"));
            TryDelete(Path.Combine(directory, "s1.db.wal"));
            TryDelete(Path.Combine(directory, "unused.db"));
            TryDelete(Path.Combine(directory, "unused.db.wal"));
            try
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, recursive: true);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                // Ignore transient test cleanup file locks.
            }
        }
    }

    [Fact]
    public async Task Daemon_ShardReplicaMetadataFlowsThroughRestAndGrpc()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"csharpdb_daemon_shard_replica_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            string masterDbPath = Path.Combine(directory, "master.db");
            await SeedMasterCatalogAsync(masterDbPath, CreateShardingOptionsWithReplica(directory));
            using var factory = new TestDaemonFactory(masterDbPath);
            using var grpcTransportClient = CreateGrpcHttpClient(factory);
            using var httpTransportClient = factory.CreateClient(new WebApplicationFactoryClientOptions
            {
                BaseAddress = new Uri("http://localhost"),
            });

            await using var restAdmin = CreateHttpShardAdmin(httpTransportClient);
            await using var grpcAdmin = CreateGrpcShardAdmin(grpcTransportClient);

            CSharpDbShardDefinitionSnapshot restReplica = Assert.Single(
                (await restAdmin.GetShardMapAsync(Ct)).Shards,
                shard => shard.ShardId == "s1-replica");
            Assert.Equal(CSharpDbShardRoles.Replica, restReplica.Role);
            Assert.Equal("s1", restReplica.PrimaryShardId);
            Assert.True(restReplica.PromotionEligible);
            Assert.Equal(256, restReplica.ReplicationLagBytes);
            Assert.NotNull(restReplica.LastReplicatedUtc);

            CSharpDbShardDefinitionSnapshot grpcReplica = Assert.Single(
                (await grpcAdmin.GetShardMapAsync(Ct)).Shards,
                shard => shard.ShardId == "s1-replica");
            Assert.Equal(restReplica.Role, grpcReplica.Role);
            Assert.Equal(restReplica.PrimaryShardId, grpcReplica.PrimaryShardId);
            Assert.Equal(restReplica.ReplicationLagBytes, grpcReplica.ReplicationLagBytes);

            CSharpDbShardStatus grpcReplicaStatus = Assert.Single(
                await grpcAdmin.GetShardStatusAsync(Ct),
                status => status.ShardId == "s1-replica");
            Assert.True(grpcReplicaStatus.Healthy);
            Assert.Equal(CSharpDbShardRoles.Replica, grpcReplicaStatus.Role);
            Assert.Equal("s1", grpcReplicaStatus.PrimaryShardId);
            Assert.True(grpcReplicaStatus.PromotionEligible);
            Assert.True(grpcReplicaStatus.CanPromote);
            Assert.Equal(256, grpcReplicaStatus.ReplicationLagBytes);
            Assert.NotNull(grpcReplicaStatus.LastReplicatedUtc);
        }
        finally
        {
            TryDelete(Path.Combine(directory, "s0.db"));
            TryDelete(Path.Combine(directory, "s0.db.wal"));
            TryDelete(Path.Combine(directory, "s1.db"));
            TryDelete(Path.Combine(directory, "s1.db.wal"));
            TryDelete(Path.Combine(directory, "s1-replica.db"));
            TryDelete(Path.Combine(directory, "s1-replica.db.wal"));
            TryDelete(Path.Combine(directory, "unused.db"));
            TryDelete(Path.Combine(directory, "unused.db.wal"));
            try
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, recursive: true);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                // Ignore transient test cleanup file locks.
            }
        }
    }

    [Fact]
    public async Task Daemon_ShardCatalogRestAndGrpcValidateApplyAndReload()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"csharpdb_daemon_shard_catalog_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            string masterDbPath = Path.Combine(directory, "master.db");
            await SeedMasterCatalogAsync(masterDbPath, CreateSeedShardingOptions(directory));

            using (var factory = new TestDaemonFactory(masterDbPath))
            {
                using var grpcTransportClient = CreateGrpcHttpClient(factory);
                using var httpTransportClient = factory.CreateClient(new WebApplicationFactoryClientOptions
                {
                    BaseAddress = new Uri("http://localhost"),
                });

                await using var restAdmin = CreateHttpShardAdmin(httpTransportClient);
                await using var grpcAdmin = CreateGrpcShardAdmin(grpcTransportClient);

                CSharpDbShardCatalogState initial = await restAdmin.GetShardCatalogAsync(Ct);
                Assert.True(initial.IsCatalogEnabled);
                Assert.True(initial.IsWritable);
                Assert.Equal(1, initial.ActiveMap.MapVersion);
                Assert.Null(initial.PendingMap);

                CSharpDbShardingOptions proposed = CreateShardingOptions(directory, mapVersion: 2);
                proposed.Directories =
                [
                    new CSharpDbShardDirectoryDefinition
                    {
                        DirectoryName = "orders_by_id",
                        TargetKeyspace = "tenants",
                        Description = "remote order lookup",
                    },
                ];
                proposed.DirectoryEntries =
                [
                    new CSharpDbShardDirectoryEntry
                    {
                        DirectoryName = "orders_by_id",
                        LookupKey = "SO-REMOTE-1",
                        TargetKeyspace = "tenants",
                        RouteKey = "tenant-b",
                        ShardId = "s1",
                        MapVersion = 2,
                        State = "Active",
                    },
                ];

                CSharpDbShardCatalogValidationResult validation =
                    await grpcAdmin.ValidateShardCatalogUpdateAsync(new CSharpDbShardCatalogUpdateRequest
                    {
                        Options = proposed,
                        ExpectedCurrentMapVersion = 1,
                        Operator = "daemon-test",
                        Comment = "add directory metadata",
                    }, Ct);
                Assert.True(validation.IsValid);
                Assert.Equal(2, validation.Preview!.MapVersion);

                CSharpDbShardCatalogApplyResult applied =
                    await restAdmin.ApplyShardCatalogUpdateAsync(new CSharpDbShardCatalogUpdateRequest
                    {
                        Options = proposed,
                        ExpectedCurrentMapVersion = 1,
                        Operator = "daemon-test",
                        Comment = "add directory metadata",
                    }, Ct);
                Assert.True(applied.Applied);
                Assert.True(applied.RequiresRestart);
                Assert.True(File.Exists(masterDbPath));

                CSharpDbShardCatalogState pending = await grpcAdmin.GetShardCatalogAsync(Ct);
                Assert.Equal(1, pending.ActiveMap.MapVersion);
                Assert.Equal(2, pending.PendingMap!.MapVersion);
                Assert.Single(pending.History);
            }

            using (var reloadedFactory = new TestDaemonFactory(masterDbPath))
            {
                using var grpcTransportClient = CreateGrpcHttpClient(reloadedFactory);
                await using var grpcAdmin = CreateGrpcShardAdmin(grpcTransportClient);

                CSharpDbShardCatalogState reloaded = await grpcAdmin.GetShardCatalogAsync(Ct);
                Assert.Equal(2, reloaded.ActiveMap.MapVersion);
                Assert.Null(reloaded.PendingMap);
                Assert.Equal(1, Assert.Single(reloaded.ActiveMap.Directories).EntryCount);
            }
        }
        finally
        {
            TryDelete(Path.Combine(directory, "s0.db"));
            TryDelete(Path.Combine(directory, "s0.db.wal"));
            TryDelete(Path.Combine(directory, "s1.db"));
            TryDelete(Path.Combine(directory, "s1.db.wal"));
            TryDelete(Path.Combine(directory, "unused.db"));
            TryDelete(Path.Combine(directory, "unused.db.wal"));
            try
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, recursive: true);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                // Ignore transient test cleanup file locks.
            }
        }
    }

    [Fact]
    public async Task Daemon_ShardDirectoryRestAndGrpcReserveActivateAndResolve()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"csharpdb_daemon_shard_directory_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            string masterDbPath = Path.Combine(directory, "master.db");
            CSharpDbShardingOptions seedOptions = CreateSeedShardingOptions(directory);
            seedOptions.Directories =
            [
                new CSharpDbShardDirectoryDefinition
                {
                    DirectoryName = "orders_by_id",
                    TargetKeyspace = "tenants",
                    Description = "remote order lookup",
                    ReadOnly = false,
                },
            ];
            await SeedMasterCatalogAsync(masterDbPath, seedOptions);

            using (var factory = new TestDaemonFactory(masterDbPath))
            {
                using var grpcTransportClient = CreateGrpcHttpClient(factory);
                using var httpTransportClient = factory.CreateClient(new WebApplicationFactoryClientOptions
                {
                    BaseAddress = new Uri("http://localhost"),
                });

                await using var restDirectory = CreateHttpShardDirectory(httpTransportClient);
                await using var grpcDirectory = CreateGrpcShardDirectory(grpcTransportClient);

                CSharpDbShardDirectoryMutationResult reserved = await restDirectory.ReserveDirectoryEntryAsync(
                    new CSharpDbShardDirectoryReserveRequest
                    {
                        DirectoryName = "orders_by_id",
                        LookupKey = "SO-REMOTE-2",
                        TargetKeyspace = "tenants",
                        RouteKey = "tenant-a",
                        ExpectedCurrentMapVersion = 1,
                        Operator = "daemon-test",
                    },
                    Ct);

                Assert.True(reserved.Succeeded, reserved.Message);
                Assert.Equal("Reserved", reserved.Status);
                Assert.Equal(2, reserved.PendingMapVersion);

                CSharpDbShardDirectoryMutationResult activated = await grpcDirectory.ActivateDirectoryEntryAsync(
                    new CSharpDbShardDirectoryActivateRequest
                    {
                        DirectoryName = "orders_by_id",
                        LookupKey = "SO-REMOTE-2",
                        ExpectedCurrentMapVersion = 1,
                        Operator = "daemon-test",
                    },
                    Ct);

                Assert.True(activated.Succeeded, activated.Message);
                Assert.Equal("Activated", activated.Status);
                Assert.Equal(3, activated.PendingMapVersion);
            }

            using (var reloadedFactory = new TestDaemonFactory(masterDbPath))
            {
                using var grpcTransportClient = CreateGrpcHttpClient(reloadedFactory);
                await using var grpcDirectory = CreateGrpcShardDirectory(grpcTransportClient);

                CSharpDbShardDirectoryResolution resolution = await grpcDirectory.ResolveDirectoryEntryAsync(
                    new CSharpDbShardDirectoryResolveRequest
                    {
                        DirectoryName = "orders_by_id",
                        LookupKey = "SO-REMOTE-2",
                    },
                    Ct);

                Assert.Equal(CSharpDbShardDirectoryEntryStates.Active, resolution.Entry.State);
                Assert.Equal("tenant-a", resolution.Entry.RouteKey);
                Assert.Equal("s0", resolution.RouteResolution.ShardId);
            }
        }
        finally
        {
            TryDelete(Path.Combine(directory, "s0.db"));
            TryDelete(Path.Combine(directory, "s0.db.wal"));
            TryDelete(Path.Combine(directory, "s1.db"));
            TryDelete(Path.Combine(directory, "s1.db.wal"));
            TryDelete(Path.Combine(directory, "unused.db"));
            TryDelete(Path.Combine(directory, "unused.db.wal"));
            try
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, recursive: true);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                // Ignore transient test cleanup file locks.
            }
        }
    }

    [Fact]
    public async Task Daemon_ShardExactKeyMigrationRestAndGrpcWritesPendingMap()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"csharpdb_daemon_shard_migration_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            string masterDbPath = Path.Combine(directory, "master.db");
            await SeedMasterCatalogAsync(masterDbPath, CreateSeedShardingOptions(directory));

            using (var factory = new TestDaemonFactory(masterDbPath))
            {
                using var grpcTransportClient = CreateGrpcHttpClient(factory);
                using var httpTransportClient = factory.CreateClient(new WebApplicationFactoryClientOptions
                {
                    BaseAddress = new Uri("http://localhost"),
                });

                await using var restAdmin = CreateHttpShardAdmin(httpTransportClient);
                await using var grpcAdmin = CreateGrpcShardAdmin(grpcTransportClient);

                IReadOnlyList<CSharpDbShardSqlExecutionResult> schemaResults =
                    await restAdmin.ExecuteSqlOnAllShardsAsync(
                        "CREATE TABLE remote_orders (id INTEGER PRIMARY KEY, tenant_id TEXT, total INTEGER);",
                        Ct);
                Assert.All(schemaResults, result => Assert.Null(result.Error));

                await using var tenantA = CreateHttpClient(
                    httpTransportClient,
                    routeContext: new CSharpDbRouteContext { Keyspace = "tenants", Key = "tenant-a" });
                Assert.Equal(1, await tenantA.InsertRowAsync("remote_orders", new Dictionary<string, object?>
                {
                    ["id"] = 10L,
                    ["tenant_id"] = "tenant-a",
                    ["total"] = 123L,
                }, Ct));

                CSharpDbShardMigrationResult migration = await grpcAdmin.MigrateExactRouteKeyAsync(
                    new CSharpDbShardExactKeyMigrationRequest
                    {
                        Keyspace = "tenants",
                        RouteKey = "tenant-a",
                        DestinationShardId = "s1",
                        ExpectedCurrentMapVersion = 1,
                        Operator = "daemon-test",
                        Manifest = new CSharpDbShardMigrationManifest
                        {
                            Tables =
                            [
                                new CSharpDbShardMigrationTableManifest
                                {
                                    TableName = "remote_orders",
                                    RouteKeyColumn = "tenant_id",
                                    PrimaryKeyColumn = "id",
                                },
                            ],
                        },
                    },
                    Ct);

                Assert.True(migration.Succeeded, string.Join(Environment.NewLine, migration.Issues.Select(issue => issue.Message)));
                Assert.Equal("PendingActivation", migration.Status);
                Assert.Equal("s0", migration.SourceShardId);
                Assert.Equal("s1", migration.DestinationShardId);
                Assert.True(migration.RequiresRestart);
                Assert.Equal(1, Assert.Single(migration.Tables).SourceRows);

                CSharpDbShardCatalogState pending = await restAdmin.GetShardCatalogAsync(Ct);
                Assert.Equal(1, pending.ActiveMap.MapVersion);
                Assert.Equal(2, pending.PendingMap!.MapVersion);
                Assert.Equal("s1", pending.PendingMap.ExactKeyPins["tenant-a"]);

                CSharpDbShardMigrationProgress restProgress =
                    Assert.Single(await restAdmin.GetShardMigrationProgressAsync(Ct));
                Assert.Equal(migration.MigrationId, restProgress.MigrationId);
                Assert.Equal("ExactRouteKey", restProgress.MigrationType);
                Assert.Equal("PendingActivation", restProgress.Status);
                Assert.Equal("Completed", restProgress.Phase);
                Assert.Equal(100d, restProgress.PercentComplete);
                Assert.Equal(2, restProgress.PendingMapVersion);

                CSharpDbShardMigrationProgress? grpcProgress =
                    await grpcAdmin.GetShardMigrationProgressAsync(migration.MigrationId, Ct);
                Assert.NotNull(grpcProgress);
                Assert.Equal(restProgress.MigrationId, grpcProgress.MigrationId);
                Assert.Equal(restProgress.Status, grpcProgress.Status);

                CSharpDbShardMigrationResult resumed =
                    await restAdmin.ResumeShardMigrationAsync(migration.MigrationId, Ct);
                Assert.True(resumed.Succeeded);
                Assert.Equal("PendingActivation", resumed.Status);
                Assert.Equal(migration.MigrationId, resumed.MigrationId);

                CSharpDbShardMigrationHistoryEntry restHistory = Assert.Single(await restAdmin.GetShardMigrationHistoryAsync(Ct));
                Assert.Equal(migration.MigrationId, restHistory.MigrationId);
                Assert.Equal("ExactRouteKey", restHistory.MigrationType);
                Assert.True(restHistory.Succeeded);
                Assert.Equal("PendingActivation", restHistory.Status);
                Assert.Equal("tenant-a", restHistory.RouteKey);

                CSharpDbShardMigrationHistoryEntry grpcHistory = Assert.Single(await grpcAdmin.GetShardMigrationHistoryAsync(Ct));
                Assert.Equal(restHistory.MigrationId, grpcHistory.MigrationId);
                Assert.Equal(restHistory.Status, grpcHistory.Status);
                Assert.Equal("s1", grpcHistory.DestinationShardId);
            }

            using (var reloadedFactory = new TestDaemonFactory(masterDbPath))
            {
                using var httpTransportClient = reloadedFactory.CreateClient(new WebApplicationFactoryClientOptions
                {
                    BaseAddress = new Uri("http://localhost"),
                });

                await using var tenantA = CreateHttpClient(
                    httpTransportClient,
                    routeContext: new CSharpDbRouteContext { Keyspace = "tenants", Key = "tenant-a" });

                Dictionary<string, object?>? row = await tenantA.GetRowByPkAsync("remote_orders", "id", 10L, Ct);
                Assert.NotNull(row);
                Assert.Equal(123L, Assert.IsType<long>(row!["total"]));
            }
        }
        finally
        {
            TryDelete(Path.Combine(directory, "s0.db"));
            TryDelete(Path.Combine(directory, "s0.db.wal"));
            TryDelete(Path.Combine(directory, "s1.db"));
            TryDelete(Path.Combine(directory, "s1.db.wal"));
            TryDelete(Path.Combine(directory, "unused.db"));
            TryDelete(Path.Combine(directory, "unused.db.wal"));
            try
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, recursive: true);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                // Ignore transient test cleanup file locks.
            }
        }
    }

    [Fact]
    public async Task Daemon_ShardBucketRangeMigrationRestAndGrpcWritesPendingMap()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"csharpdb_daemon_shard_bucket_migration_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            string routeKey = FindRouteKeyForBucket("tenants", bucket: 0, virtualBucketCount: 4);
            string masterDbPath = Path.Combine(directory, "master.db");
            await SeedMasterCatalogAsync(masterDbPath, CreateSeedShardingOptions(directory));

            using (var factory = new TestDaemonFactory(masterDbPath))
            {
                using var grpcTransportClient = CreateGrpcHttpClient(factory);
                using var httpTransportClient = factory.CreateClient(new WebApplicationFactoryClientOptions
                {
                    BaseAddress = new Uri("http://localhost"),
                });

                await using var restAdmin = CreateHttpShardAdmin(httpTransportClient);
                await using var grpcAdmin = CreateGrpcShardAdmin(grpcTransportClient);

                IReadOnlyList<CSharpDbShardSqlExecutionResult> schemaResults =
                    await restAdmin.ExecuteSqlOnAllShardsAsync(
                        "CREATE TABLE remote_bucket_orders (id INTEGER PRIMARY KEY, tenant_id TEXT, total INTEGER);",
                        Ct);
                Assert.All(schemaResults, result => Assert.Null(result.Error));

                await using var routed = CreateHttpClient(
                    httpTransportClient,
                    routeContext: new CSharpDbRouteContext { Keyspace = "tenants", Key = routeKey });
                Assert.Equal(1, await routed.InsertRowAsync("remote_bucket_orders", new Dictionary<string, object?>
                {
                    ["id"] = 30L,
                    ["tenant_id"] = routeKey,
                    ["total"] = 321L,
                }, Ct));

                CSharpDbShardMigrationResult migration = await grpcAdmin.MigrateBucketRangeAsync(
                    new CSharpDbShardBucketRangeMigrationRequest
                    {
                        Keyspace = "tenants",
                        SourceShardId = "s0",
                        DestinationShardId = "s1",
                        StartBucketInclusive = 0,
                        EndBucketExclusive = 1,
                        ExpectedCurrentMapVersion = 1,
                        Operator = "daemon-test",
                        Manifest = new CSharpDbShardMigrationManifest
                        {
                            Tables =
                            [
                                new CSharpDbShardMigrationTableManifest
                                {
                                    TableName = "remote_bucket_orders",
                                    RouteKeyColumn = "tenant_id",
                                    PrimaryKeyColumn = "id",
                                },
                            ],
                        },
                    },
                    Ct);

                Assert.True(migration.Succeeded, string.Join(Environment.NewLine, migration.Issues.Select(issue => issue.Message)));
                Assert.Equal("PendingActivation", migration.Status);
                Assert.Equal("bucket-range:[0,1)", migration.RouteKey);
                Assert.Equal("s0", migration.SourceShardId);
                Assert.Equal("s1", migration.DestinationShardId);
                Assert.Equal(1, Assert.Single(migration.Tables).SourceRows);

                CSharpDbShardCatalogState pending = await restAdmin.GetShardCatalogAsync(Ct);
                Assert.Equal(1, pending.ActiveMap.MapVersion);
                Assert.Equal(2, pending.PendingMap!.MapVersion);
                Assert.Equal("s1", GetOwnerForBucket(pending.PendingMap, 0));
                Assert.Equal("s0", GetOwnerForBucket(pending.PendingMap, 1));

                CSharpDbShardMigrationProgress restProgress =
                    Assert.Single(await restAdmin.GetShardMigrationProgressAsync(Ct));
                Assert.Equal(migration.MigrationId, restProgress.MigrationId);
                Assert.Equal("BucketRange", restProgress.MigrationType);
                Assert.Equal("PendingActivation", restProgress.Status);
                Assert.Equal("Completed", restProgress.Phase);
                Assert.Equal(100d, restProgress.PercentComplete);
                Assert.Equal(2, restProgress.PendingMapVersion);

                CSharpDbShardMigrationProgress? grpcProgress =
                    await grpcAdmin.GetShardMigrationProgressAsync(migration.MigrationId, Ct);
                Assert.NotNull(grpcProgress);
                Assert.Equal(restProgress.MigrationId, grpcProgress.MigrationId);
                Assert.Equal(restProgress.Status, grpcProgress.Status);

                CSharpDbShardMigrationHistoryEntry history = Assert.Single(await grpcAdmin.GetShardMigrationHistoryAsync(Ct));
                Assert.Equal(migration.MigrationId, history.MigrationId);
                Assert.Equal("BucketRange", history.MigrationType);
                Assert.Equal("bucket-range:[0,1)", history.RouteKey);
            }

            using (var reloadedFactory = new TestDaemonFactory(masterDbPath))
            {
                using var httpTransportClient = reloadedFactory.CreateClient(new WebApplicationFactoryClientOptions
                {
                    BaseAddress = new Uri("http://localhost"),
                });

                await using var routed = CreateHttpClient(
                    httpTransportClient,
                    routeContext: new CSharpDbRouteContext { Keyspace = "tenants", Key = routeKey });

                Dictionary<string, object?>? row = await routed.GetRowByPkAsync("remote_bucket_orders", "id", 30L, Ct);
                Assert.NotNull(row);
                Assert.Equal(321L, Assert.IsType<long>(row!["total"]));
            }
        }
        finally
        {
            TryDelete(Path.Combine(directory, "s0.db"));
            TryDelete(Path.Combine(directory, "s0.db.wal"));
            TryDelete(Path.Combine(directory, "s1.db"));
            TryDelete(Path.Combine(directory, "s1.db.wal"));
            TryDelete(Path.Combine(directory, "unused.db"));
            TryDelete(Path.Combine(directory, "unused.db.wal"));
            try
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, recursive: true);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                // Ignore transient test cleanup file locks.
            }
        }
    }

    [Fact]
    public async Task Daemon_RestApiCanBeDisabledWithoutDisablingGrpc()
    {
        using var factory = new TestDaemonFactory(
            _dbPath,
            new Dictionary<string, string?>
            {
                ["CSharpDB:Daemon:EnableRestApi"] = "false",
            });

        using var httpClient = factory.CreateClient();
        using HttpResponseMessage restResponse = await httpClient.GetAsync("/api/info", Ct);

        Assert.Equal(HttpStatusCode.NotFound, restResponse.StatusCode);

        using var grpcTransportClient = CreateGrpcHttpClient(factory);
        await using var grpcClient = CreateGrpcClient(grpcTransportClient);

        DatabaseInfo info = await grpcClient.GetInfoAsync(Ct);
        Assert.Equal(Path.GetFullPath(_dbPath), info.DataSource);
    }

    [Fact]
    public async Task GrpcClient_Collections_RoundTripNestedDocuments()
    {
        using var transportClient = CreateGrpcHttpClient();
        await using var client = CreateGrpcClient(transportClient);

        using JsonDocument document = JsonDocument.Parse("""
            {
              "name": "typed",
              "tags": ["grpc", "proto"],
              "meta": {
                "score": 9,
                "active": true
              }
            }
            """);

        await client.PutDocumentAsync("grpc_docs", "doc-1", document.RootElement, Ct);

        JsonElement? fetched = await client.GetDocumentAsync("grpc_docs", "doc-1", Ct);
        Assert.True(fetched.HasValue);
        Assert.Equal("typed", fetched.Value.GetProperty("name").GetString());
        Assert.Equal(9, fetched.Value.GetProperty("meta").GetProperty("score").GetInt32());

        CollectionBrowseResult browse = await client.BrowseCollectionAsync("grpc_docs", ct: Ct);
        Assert.Single(browse.Documents);
        Assert.Equal("doc-1", browse.Documents[0].Key);
        Assert.Equal("proto", browse.Documents[0].Document.GetProperty("tags")[1].GetString());

        await client.DropCollectionAsync("grpc_docs", Ct);
        Assert.DoesNotContain("grpc_docs", await client.GetCollectionNamesAsync(Ct));
    }

    [Fact]
    public async Task GrpcClient_ProcedureCrudAndValidation_WorkThroughTransport()
    {
        using var transportClient = CreateGrpcHttpClient();
        await using var client = CreateGrpcClient(transportClient);

        await client.CreateProcedureAsync(
            new ProcedureDefinition
            {
                Name = "GrpcProc",
                BodySql = """
                    CREATE TABLE IF NOT EXISTS grpc_proc_data (id INTEGER PRIMARY KEY, name TEXT);
                    INSERT INTO grpc_proc_data VALUES (@id, @name);
                    SELECT id, name FROM grpc_proc_data WHERE id = @id;
                    """,
                Parameters =
                [
                    new ProcedureParameterDefinition { Name = "id", Type = DbType.Integer, Required = true },
                    new ProcedureParameterDefinition { Name = "name", Type = DbType.Text, Required = false, Default = "fallback" },
                ],
                Description = "gRPC test",
                IsEnabled = true,
            },
            Ct);

        IReadOnlyList<ProcedureDefinition> procedures = await client.GetProceduresAsync(ct: Ct);
        Assert.Contains(procedures, p => p.Name == "GrpcProc");

        ProcedureExecutionResult execution = await client.ExecuteProcedureAsync(
            "GrpcProc",
            new Dictionary<string, object?> { ["id"] = 10L },
            Ct);

        Assert.True(execution.Succeeded);
        Assert.NotEmpty(execution.Statements);
        Assert.Equal(10L, Assert.IsType<long>(execution.Statements[^1].Rows![0][0]));
        Assert.Equal("fallback", Assert.IsType<string>(execution.Statements[^1].Rows![0][1]));

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => client.CreateProcedureAsync(
            new ProcedureDefinition
            {
                Name = "BrokenProc",
                BodySql = "SELECT @missing;",
                Parameters = [],
            },
            Ct));

        Assert.Contains("missing from params metadata", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task GrpcClient_BackupAndRestore_WorkThroughTransport()
    {
        using var transportClient = CreateGrpcHttpClient();
        await using var client = CreateGrpcClient(transportClient);

        string backupPath = Path.Combine(Path.GetTempPath(), $"csharpdb_daemon_backup_{Guid.NewGuid():N}.db");
        string manifestPath = backupPath + ".manifest.json";

        try
        {
            SqlExecutionResult createResult = await client.ExecuteSqlAsync(
                "CREATE TABLE grpc_restore (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO grpc_restore VALUES (1, 'before');",
                Ct);
            Assert.Null(createResult.Error);

            BackupResult backup = await client.BackupAsync(new BackupRequest
            {
                DestinationPath = backupPath,
                WithManifest = true,
            }, Ct);

            Assert.Equal(Path.GetFullPath(backupPath), backup.DestinationPath);
            Assert.True(File.Exists(backupPath));
            Assert.True(File.Exists(manifestPath));

            SqlExecutionResult mutateResult = await client.ExecuteSqlAsync(
                "INSERT INTO grpc_restore VALUES (2, 'after');",
                Ct);
            Assert.Null(mutateResult.Error);

            RestoreResult validate = await client.RestoreAsync(new RestoreRequest
            {
                SourcePath = backupPath,
                ValidateOnly = true,
            }, Ct);
            Assert.True(validate.ValidateOnly);

            RestoreResult restore = await client.RestoreAsync(new RestoreRequest
            {
                SourcePath = backupPath,
            }, Ct);
            Assert.False(restore.ValidateOnly);

            SqlExecutionResult rows = await client.ExecuteSqlAsync("SELECT id, value FROM grpc_restore ORDER BY id;", Ct);
            Assert.Null(rows.Error);
            Assert.NotNull(rows.Rows);
            var row = Assert.Single(rows.Rows);
            Assert.Equal(1L, row[0]);
            Assert.Equal("before", row[1]);
        }
        finally
        {
            TryDelete(backupPath);
            TryDelete(backupPath + ".wal");
            TryDelete(manifestPath);
        }
    }

    [Fact]
    public async Task GrpcClient_MutatingSchemaEndpoints_AcceptCollationMetadata()
    {
        using var transportClient = CreateGrpcHttpClient();
        await using var client = CreateGrpcClient(transportClient);

        SqlExecutionResult createResult = await client.ExecuteSqlAsync(
            "CREATE TABLE grpc_mutation_collation (id INTEGER PRIMARY KEY);",
            Ct);
        Assert.Null(createResult.Error);

        await client.AddColumnAsync("grpc_mutation_collation", "name", DbType.Text, notNull: false, collation: "NOCASE", ct: Ct);
        await client.CreateIndexAsync("idx_grpc_mutation_collation_name_binary", "grpc_mutation_collation", "name", isUnique: false, collation: "BINARY", ct: Ct);
        await client.UpdateIndexAsync("idx_grpc_mutation_collation_name_binary", "idx_grpc_mutation_collation_name_nocase", "grpc_mutation_collation", "name", isUnique: false, collation: "NOCASE", ct: Ct);

        TableSchema? schema = await client.GetTableSchemaAsync("grpc_mutation_collation", Ct);
        Assert.NotNull(schema);
        Assert.Equal("NOCASE", Assert.Single(schema!.Columns, column => column.Name == "name").Collation);

        IReadOnlyList<IndexSchema> indexes = await client.GetIndexesAsync(Ct);
        IndexSchema index = Assert.Single(indexes, item => item.IndexName == "idx_grpc_mutation_collation_name_nocase");
        Assert.Equal(["name"], index.Columns);
        Assert.Equal(["NOCASE"], index.ColumnCollations);
    }

    [Fact]
    public async Task GrpcClient_MapsForeignKeyMetadata()
    {
        using var transportClient = CreateGrpcHttpClient();
        await using var client = CreateGrpcClient(transportClient);

        SqlExecutionResult createResult = await client.ExecuteSqlAsync(
            """
            CREATE TABLE grpc_parents (id INTEGER PRIMARY KEY);
            CREATE TABLE grpc_children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES grpc_parents(id) ON DELETE CASCADE
            );
            """,
            Ct);
        Assert.Null(createResult.Error);

        TableSchema? schema = await client.GetTableSchemaAsync("grpc_children", Ct);
        Assert.NotNull(schema);
        var foreignKey = Assert.Single(schema!.ForeignKeys);
        Assert.Equal("parent_id", foreignKey.ColumnName);
        Assert.Equal("grpc_parents", foreignKey.ReferencedTableName);
        Assert.Equal("id", foreignKey.ReferencedColumnName);
        Assert.Equal(ForeignKeyOnDeleteAction.Cascade, foreignKey.OnDelete);
        Assert.StartsWith("__fk_grpc_children_parent_id_", foreignKey.SupportingIndexName, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GrpcClient_MigrateForeignKeys_RoundTripsValidationAndApply()
    {
        using var transportClient = CreateGrpcHttpClient();
        await using var client = CreateGrpcClient(transportClient);

        SqlExecutionResult createResult = await client.ExecuteSqlAsync(
            """
            CREATE TABLE grpc_migrate_parents (id INTEGER PRIMARY KEY);
            CREATE TABLE grpc_migrate_children (id INTEGER PRIMARY KEY, parent_id INTEGER);
            INSERT INTO grpc_migrate_parents VALUES (1);
            INSERT INTO grpc_migrate_children VALUES (10, 1);
            """,
            Ct);
        Assert.Null(createResult.Error);

        ForeignKeyMigrationResult validate = await client.MigrateForeignKeysAsync(
            new ForeignKeyMigrationRequest
            {
                ValidateOnly = true,
                Constraints =
                [
                    new ForeignKeyMigrationConstraintSpec
                    {
                        TableName = "grpc_migrate_children",
                        ColumnName = "parent_id",
                        ReferencedTableName = "grpc_migrate_parents",
                        ReferencedColumnName = "id",
                    },
                ],
            },
            Ct);

        Assert.True(validate.ValidateOnly);
        Assert.True(validate.Succeeded);
        Assert.Equal(1, validate.AppliedForeignKeys);
        Assert.Empty(validate.Violations);

        ForeignKeyMigrationResult apply = await client.MigrateForeignKeysAsync(
            new ForeignKeyMigrationRequest
            {
                Constraints =
                [
                    new ForeignKeyMigrationConstraintSpec
                    {
                        TableName = "grpc_migrate_children",
                        ColumnName = "parent_id",
                        ReferencedTableName = "grpc_migrate_parents",
                        ReferencedColumnName = "id",
                        OnDelete = ForeignKeyOnDeleteAction.Cascade,
                    },
                ],
            },
            Ct);

        Assert.False(apply.ValidateOnly);
        Assert.True(apply.Succeeded);
        Assert.Equal(1, apply.CopiedRows);

        TableSchema? schema = await client.GetTableSchemaAsync("grpc_migrate_children", Ct);
        Assert.NotNull(schema);
        var foreignKey = Assert.Single(schema!.ForeignKeys);
        Assert.Equal(ForeignKeyOnDeleteAction.Cascade, foreignKey.OnDelete);
    }

    [Fact]
    public async Task GrpcContract_ExposesExplicitRpcMethods()
    {
        using var transportClient = CreateGrpcHttpClient();
        using var channel = GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions
        {
            HttpClient = transportClient,
            DisposeHttpClient = false,
        });

        var rpcClient = new CSharpDbRpc.CSharpDbRpcClient(channel);

        SqlExecutionResultMessage createResponse = await rpcClient.ExecuteSqlAsync(
            new SqlRequest
            {
                Sql = "CREATE TABLE grpc_contract (id INTEGER PRIMARY KEY, name TEXT)",
            },
            cancellationToken: Ct).ResponseAsync;

        Assert.Null(createResponse.Error);

        await rpcClient.InsertRowAsync(
            new InsertRowRequest
            {
                TableName = "grpc_contract",
                Values = GrpcValueMapper.ToObject(new Dictionary<string, object?>
                {
                    ["id"] = 11L,
                    ["name"] = "typed",
                }),
            },
            cancellationToken: Ct).ResponseAsync;

        DatabaseInfoMessage infoResponse = await rpcClient.GetInfoAsync(new Empty(), cancellationToken: Ct).ResponseAsync;
        DatabaseInfo info = GrpcModelMapper.ToModel(infoResponse);
        Assert.Equal(Path.GetFullPath(_dbPath), info.DataSource);

        StringList namesResponse = await rpcClient.GetTableNamesAsync(new Empty(), cancellationToken: Ct).ResponseAsync;
        IReadOnlyList<string> tableNames = GrpcModelMapper.ToStringList(namesResponse);
        Assert.Contains("grpc_contract", tableNames);

        OptionalVariantObjectResponse rowResponse = await rpcClient.GetRowByPkAsync(
            new GetRowByPkRequest
            {
                TableName = "grpc_contract",
                PkColumn = "id",
                PkValue = GrpcValueMapper.ToMessage(11),
            },
            cancellationToken: Ct).ResponseAsync;

        Assert.NotNull(rowResponse.Value);
        Assert.Equal("typed", rowResponse.Value.Fields["name"].StringValue);

        await rpcClient.PutDocumentAsync(
            new PutDocumentRequest
            {
                CollectionName = "grpc_contract_docs",
                Key = "doc-1",
                Document = GrpcValueMapper.ToMessage(JsonDocument.Parse("""
                    {
                      "nested": {
                        "count": 3
                      },
                      "tags": ["typed", "contract"]
                    }
                    """).RootElement),
            },
            cancellationToken: Ct).ResponseAsync;

        OptionalVariantValueResponse documentResponse = await rpcClient.GetDocumentAsync(
            new GetDocumentRequest
            {
                CollectionName = "grpc_contract_docs",
                Key = "doc-1",
            },
            cancellationToken: Ct).ResponseAsync;

        Assert.NotNull(documentResponse.Value);
        Assert.Equal(3L, documentResponse.Value.ObjectValue.Fields["nested"].ObjectValue.Fields["count"].Int64Value);
        Assert.Equal("contract", documentResponse.Value.ObjectValue.Fields["tags"].ArrayValue.Items[1].StringValue);
    }

    private ICSharpDbClient CreateGrpcClient(
        HttpClient transportClient,
        string? apiKey = null,
        CSharpDbRouteContext? routeContext = null)
        => CSharpDbClient.Create(new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Grpc,
            Endpoint = "http://localhost",
            HttpClient = transportClient,
            ApiKey = apiKey,
            RouteContext = routeContext,
        });

    private HttpClient CreateGrpcHttpClient()
        => CreateGrpcHttpClient(_factory);

    private HttpClient CreateHttpTransportClient()
        => _factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            BaseAddress = new Uri("http://localhost"),
        });

    private static HttpClient CreateGrpcHttpClient(TestDaemonFactory factory)
    {
        return new HttpClient(factory.Server.CreateHandler())
        {
            BaseAddress = new Uri("http://localhost"),
            DefaultRequestVersion = HttpVersion.Version20,
            DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact,
        };
    }

    private static ICSharpDbClient CreateHttpClient(
        HttpClient transportClient,
        string? apiKey = null,
        CSharpDbRouteContext? routeContext = null)
        => CSharpDbClient.Create(new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Http,
            Endpoint = "http://localhost",
            HttpClient = transportClient,
            ApiKey = apiKey,
            RouteContext = routeContext,
        });

    private static ICSharpDbShardAdminClient CreateHttpShardAdmin(HttpClient transportClient)
        => CSharpDbClient.CreateShardAdmin(new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Http,
            Endpoint = "http://localhost",
            HttpClient = transportClient,
        });

    private static ICSharpDbShardAdminClient CreateGrpcShardAdmin(HttpClient transportClient)
        => CSharpDbClient.CreateShardAdmin(new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Grpc,
            Endpoint = "http://localhost",
            HttpClient = transportClient,
        });

    private static ICSharpDbShardDirectoryClient CreateHttpShardDirectory(HttpClient transportClient)
        => CSharpDbClient.CreateShardDirectoryClient(new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Http,
            Endpoint = "http://localhost",
            HttpClient = transportClient,
        });

    private static ICSharpDbShardDirectoryClient CreateGrpcShardDirectory(HttpClient transportClient)
        => CSharpDbClient.CreateShardDirectoryClient(new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Grpc,
            Endpoint = "http://localhost",
            HttpClient = transportClient,
        });

    private static async Task SeedMasterCatalogAsync(string masterDbPath, CSharpDbShardingOptions options)
    {
        string? directory = Path.GetDirectoryName(masterDbPath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        await CSharpDbShardedClient.SeedMasterCatalogAsync(
            new CSharpDbClientOptions
            {
                DataSource = masterDbPath,
                DirectDatabaseOptions = CreateSeedDirectDatabaseOptions(),
                HybridDatabaseOptions = new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                },
            },
            options,
            Ct);
    }

    private static DatabaseOptions CreateSeedDirectDatabaseOptions()
        => new DatabaseOptions
        {
            ImplicitInsertExecutionMode = ImplicitInsertExecutionMode.ConcurrentWriteTransactions,
        }.ConfigureStorageEngine(builder => builder.UseWriteOptimizedPreset());

    private static CSharpDbShardingOptions CreateSeedShardingOptions(string directory)
        => new()
        {
            Keyspace = "tenants",
            MapVersion = 1,
            VirtualBucketCount = 4,
            Shards =
            [
                new CSharpDbShardDefinition { ShardId = "s0", DataSource = Path.Combine(directory, "s0.db") },
                new CSharpDbShardDefinition { ShardId = "s1", DataSource = Path.Combine(directory, "s1.db") },
            ],
            BucketRanges =
            [
                new CSharpDbShardBucketRange { StartBucketInclusive = 0, EndBucketExclusive = 2, ShardId = "s0" },
                new CSharpDbShardBucketRange { StartBucketInclusive = 2, EndBucketExclusive = 4, ShardId = "s1" },
            ],
            ExactKeyPins = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["tenant-a"] = "s0",
                ["tenant-b"] = "s1",
            },
        };

    private static CSharpDbShardingOptions CreateShardingOptionsWithReplica(string directory)
    {
        CSharpDbShardingOptions options = CreateSeedShardingOptions(directory);
        options.Shards =
        [
            .. options.Shards,
            new CSharpDbShardDefinition
            {
                ShardId = "s1-replica",
                DataSource = Path.Combine(directory, "s1-replica.db"),
                Role = CSharpDbShardRoles.Replica,
                PrimaryShardId = "s1",
                PromotionEligible = true,
                ReplicationLagBytes = 256,
                LastReplicatedUtc = DateTimeOffset.Parse("2026-06-01T12:30:00+00:00", CultureInfo.InvariantCulture),
            },
        ];
        return options;
    }

    private static string FindRouteKeyForBucket(string keyspace, int bucket, int virtualBucketCount)
    {
        for (int i = 0; i < 10_000; i++)
        {
            string routeKey = $"bucket-{bucket}-{i}";
            ulong token = CSharpDbShardedClient.ComputeRouteToken(new CSharpDbRouteContext
            {
                Keyspace = keyspace,
                Key = routeKey,
            });
            if ((int)(token % (ulong)virtualBucketCount) == bucket)
                return routeKey;
        }

        throw new InvalidOperationException($"Could not find a route key for bucket {bucket}.");
    }

    private static string GetOwnerForBucket(CSharpDbShardMapSnapshot map, int bucket)
        => map.BucketRanges.Single(range =>
            bucket >= range.StartBucketInclusive &&
            bucket < range.EndBucketExclusive).ShardId;

    private static CSharpDbShardingOptions CreateShardingOptions(
        string directory,
        int mapVersion)
        => new()
        {
            Keyspace = "tenants",
            MapVersion = mapVersion,
            VirtualBucketCount = 4,
            Shards =
            [
                new CSharpDbShardDefinition { ShardId = "s0", DataSource = Path.Combine(directory, "s0.db") },
                new CSharpDbShardDefinition { ShardId = "s1", DataSource = Path.Combine(directory, "s1.db") },
            ],
            BucketRanges =
            [
                new CSharpDbShardBucketRange { StartBucketInclusive = 0, EndBucketExclusive = 2, ShardId = "s0" },
                new CSharpDbShardBucketRange { StartBucketInclusive = 2, EndBucketExclusive = 4, ShardId = "s1" },
            ],
            ExactKeyPins = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["tenant-a"] = "s0",
                ["tenant-b"] = "s1",
            },
        };

    private static DaemonHostDatabaseOptions GetResolvedHostDatabaseOptions(TestDaemonFactory factory)
        => factory.Services.GetRequiredService<DaemonHostDatabaseOptions>();

    private static CSharpDbClientOptions GetResolvedClientOptions(TestDaemonFactory factory)
        => factory.Services.GetRequiredService<CSharpDbClientOptions>();

    private static async Task SeedHybridHotSetDatabaseAsync(string dbPath)
    {
        await using var db = await Database.OpenAsync(dbPath, TestContext.Current.CancellationToken);
        await db.ExecuteAsync(
            "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);",
            TestContext.Current.CancellationToken);
        await db.ExecuteAsync(
            "CREATE TABLE IF NOT EXISTS sessions (id INTEGER PRIMARY KEY, token TEXT);",
            TestContext.Current.CancellationToken);

        var collection = await db.GetCollectionAsync<JsonElement>(
            "session_cache",
            TestContext.Current.CancellationToken);

        using JsonDocument document = JsonDocument.Parse("""{"seed": true}""");
        await collection.PutAsync("seed", document.RootElement.Clone(), TestContext.Current.CancellationToken);
    }

    private sealed class TestDaemonFactory(
        string dbPath,
        IReadOnlyDictionary<string, string?>? extraConfig = null) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Development");
            builder.ConfigureAppConfiguration((_, config) =>
            {
                var values = new Dictionary<string, string?>
                {
                    ["ConnectionStrings:CSharpDB"] = $"Data Source={dbPath}",
                };

                if (extraConfig is not null)
                {
                    foreach (var pair in extraConfig)
                        values[pair.Key] = pair.Value;
                }

                config.AddInMemoryCollection(values);
            });
        }
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Ignore transient file locks in test cleanup.
        }
    }
}
