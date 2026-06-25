using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.Tests;

public sealed class CSharpDbShardedClientTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ResolveRoute_UsesCanonicalSha256TokenAndExactPins()
    {
        string directory = CreateTempDirectory();
        try
        {
            await using var client = await CSharpDbShardedClient.CreateAsync(CreateOptions(directory), ct: Ct);

            var route = new CSharpDbRouteContext { Keyspace = " tenants ", Key = " alpha " };
            string canonical = CSharpDbShardedClient.GetCanonicalRouteText(route);
            Assert.Equal("7:tenants|5:alpha", canonical);

            ulong expectedToken = BinaryPrimitives.ReadUInt64BigEndian(
                SHA256.HashData(Encoding.UTF8.GetBytes(canonical)).AsSpan(0, sizeof(ulong)));
            Assert.Equal(expectedToken, CSharpDbShardedClient.ComputeRouteToken(route));

            CSharpDbShardResolution pinned = client.ResolveRoute(new CSharpDbRouteContext
            {
                Keyspace = "tenants",
                Key = "tenant-b",
            });
            Assert.Equal("s1", pinned.ShardId);

            Assert.Throws<CSharpDbClientException>(() => client.ResolveRoute(new CSharpDbRouteContext
            {
                Keyspace = "other",
                Key = "tenant-a",
            }));
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task ShardAdmin_ReturnsReadOnlyMapSnapshotAndRoutePreview()
    {
        string directory = CreateTempDirectory();
        try
        {
            await using var client = await CSharpDbShardedClient.CreateAsync(CreateOptions(directory), ct: Ct);

            CSharpDbShardMapSnapshot snapshot = await client.GetShardMapAsync(Ct);

            Assert.Equal("tenants", snapshot.Keyspace);
            Assert.Equal(1, snapshot.MapVersion);
            Assert.Equal(4, snapshot.VirtualBucketCount);
            Assert.Equal(["s0", "s1"], snapshot.Shards.Select(shard => shard.ShardId).ToArray());
            Assert.All(snapshot.Shards, shard => Assert.False(shard.HasApiKey));
            Assert.All(snapshot.Shards, shard => Assert.Equal(CSharpDbShardRoles.Primary, shard.Role));
            Assert.Equal("s0", snapshot.ExactKeyPins["tenant-a"]);
            Assert.Equal("s1", snapshot.ExactKeyPins["tenant-b"]);
            Assert.Empty(snapshot.Directories);

            Assert.Collection(
                snapshot.BucketRanges,
                range =>
                {
                    Assert.Equal(0, range.StartBucketInclusive);
                    Assert.Equal(2, range.EndBucketExclusive);
                    Assert.Equal("s0", range.ShardId);
                },
                range =>
                {
                    Assert.Equal(2, range.StartBucketInclusive);
                    Assert.Equal(4, range.EndBucketExclusive);
                    Assert.Equal("s1", range.ShardId);
                });

            CSharpDbShardResolution resolution = await client.ResolveRouteAsync(new CSharpDbRouteContext
            {
                Keyspace = "tenants",
                Key = "tenant-b",
            }, Ct);

            Assert.Equal("s1", resolution.ShardId);
            Assert.Equal(1, resolution.MapVersion);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task CreateAsync_LoadsShardMapFromCSharpDbCatalogWithoutStaticMap()
    {
        string directory = CreateTempDirectory();
        try
        {
            string catalogPath = Path.Combine(directory, "master-catalog.db");
            CSharpDbShardingOptions seedOptions = CreateOptions(directory);
            seedOptions.Catalog = new CSharpDbShardCatalogOptions
            {
                DataSource = catalogPath,
            };

            await using (var seeded = await CSharpDbShardedClient.CreateAsync(seedOptions, ct: Ct))
            {
                CSharpDbShardingOptions catalogMap = CreateOptions(directory);
                catalogMap.MapVersion = 2;
                CSharpDbShardCatalogApplyResult applied = await seeded.ApplyShardCatalogUpdateAsync(
                    new CSharpDbShardCatalogUpdateRequest
                    {
                        Options = catalogMap,
                        ExpectedCurrentMapVersion = 1,
                        Operator = "test",
                        Comment = "seed master catalog",
                    },
                    Ct);

                Assert.True(applied.Applied, applied.Message);
            }

            CSharpDbShardedClient? discovered = await CSharpDbShardedClient.TryCreateFromMasterCatalogAsync(
                new CSharpDbClientOptions
                {
                    DataSource = catalogPath,
                },
                ct: Ct);
            Assert.NotNull(discovered);
            await using CSharpDbShardedClient client = discovered;

            CSharpDbShardMapSnapshot snapshot = await client.GetShardMapAsync(Ct);
            Assert.Equal("tenants", snapshot.Keyspace);
            Assert.Equal(2, snapshot.MapVersion);
            Assert.Equal(["s0", "s1"], snapshot.Shards.Select(shard => shard.ShardId).ToArray());
            Assert.Equal("s1", snapshot.ExactKeyPins["tenant-b"]);

            CSharpDbShardCatalogState catalog = await client.GetShardCatalogAsync(Ct);
            Assert.StartsWith("csharpdb:", catalog.Source, StringComparison.OrdinalIgnoreCase);
            Assert.True(catalog.IsCatalogEnabled);
            Assert.True(catalog.IsWritable);
            Assert.Null(catalog.PendingMap);
            Assert.Single(catalog.History);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task TryCreateFromMasterCatalog_ReturnsNullWhenMasterHasNoActiveShardMap()
    {
        string directory = CreateTempDirectory();
        try
        {
            string masterPath = Path.Combine(directory, "master.db");
            await using ICSharpDbClient seed = CSharpDbClient.Create(new CSharpDbClientOptions
            {
                DataSource = masterPath,
            });
            _ = await seed.GetInfoAsync(Ct);

            CSharpDbShardedClient? discovered = await CSharpDbShardedClient.TryCreateFromMasterCatalogAsync(
                new CSharpDbClientOptions
                {
                    DataSource = masterPath,
                },
                ct: Ct);

            Assert.Null(discovered);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task ShardDirectory_ResolvesActiveEntriesAndRejectsStaleEntries()
    {
        string directory = CreateTempDirectory();
        try
        {
            CSharpDbShardingOptions options = CreateOptions(directory);
            options.Directories =
            [
                new CSharpDbShardDirectoryDefinition
                {
                    DirectoryName = "orders_by_id",
                    TargetKeyspace = "tenants",
                    Description = "order id lookup",
                },
            ];
            options.DirectoryEntries =
            [
                new CSharpDbShardDirectoryEntry
                {
                    DirectoryName = "orders_by_id",
                    LookupKey = "SO-1",
                    TargetKeyspace = "tenants",
                    RouteKey = "tenant-a",
                    ShardId = "s0",
                    MapVersion = 1,
                    State = CSharpDbShardDirectoryEntryStates.Active,
                },
                new CSharpDbShardDirectoryEntry
                {
                    DirectoryName = "orders_by_id",
                    LookupKey = "SO-stale",
                    TargetKeyspace = "tenants",
                    RouteKey = "tenant-b",
                    ShardId = "s1",
                    MapVersion = 1,
                    State = CSharpDbShardDirectoryEntryStates.Stale,
                },
            ];

            await using var client = await CSharpDbShardedClient.CreateAsync(options, ct: Ct);

            CSharpDbShardDirectoryResolution resolution = await client.ResolveDirectoryEntryAsync(
                new CSharpDbShardDirectoryResolveRequest
                {
                    DirectoryName = "orders_by_id",
                    LookupKey = "SO-1",
                },
                Ct);

            Assert.Equal("tenant-a", resolution.Entry.RouteKey);
            Assert.Equal("s0", resolution.RouteResolution.ShardId);

            CSharpDbClientException staleError = await Assert.ThrowsAsync<CSharpDbClientException>(
                () => client.ResolveDirectoryEntryAsync(
                    new CSharpDbShardDirectoryResolveRequest
                    {
                        DirectoryName = "orders_by_id",
                        LookupKey = "SO-stale",
                    },
                    Ct));
            Assert.Contains("not Active", staleError.Message);

            CSharpDbShardDirectoryResolution inactiveResolution = await client.ResolveDirectoryEntryAsync(
                new CSharpDbShardDirectoryResolveRequest
                {
                    DirectoryName = "orders_by_id",
                    LookupKey = "SO-stale",
                    IncludeInactive = true,
                },
                Ct);
            Assert.Equal(CSharpDbShardDirectoryEntryStates.Stale, inactiveResolution.Entry.State);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task ShardDirectory_ReserveActivateAndRepairBuildOnPendingCatalog()
    {
        string directory = CreateTempDirectory();
        try
        {
            string catalogPath = Path.Combine(directory, "master-catalog.db");
            CSharpDbShardingOptions options = CreateOptions(directory);
            options.Catalog = new CSharpDbShardCatalogOptions
            {
                DataSource = catalogPath,
            };
            options.Directories =
            [
                new CSharpDbShardDirectoryDefinition
                {
                    DirectoryName = "orders_by_id",
                    TargetKeyspace = "tenants",
                    Description = "order id lookup",
                    ReadOnly = false,
                },
            ];

            await using (var client = await CSharpDbShardedClient.CreateAsync(options, ct: Ct))
            {
                CSharpDbShardDirectoryMutationResult reserved = await client.ReserveDirectoryEntryAsync(
                    new CSharpDbShardDirectoryReserveRequest
                    {
                        DirectoryName = "orders_by_id",
                        LookupKey = "SO-2",
                        TargetKeyspace = "tenants",
                        RouteKey = "tenant-a",
                        ExpectedCurrentMapVersion = 1,
                        Operator = "test",
                    },
                    Ct);

                Assert.True(reserved.Succeeded, reserved.Message);
                Assert.Equal("Reserved", reserved.Status);
                Assert.Equal(2, reserved.PendingMapVersion);
                Assert.Equal(CSharpDbShardDirectoryEntryStates.Reserved, reserved.Entry!.State);
                Assert.True(reserved.RequiresRestart);

                CSharpDbShardDirectoryMutationResult duplicateReserve = await client.ReserveDirectoryEntryAsync(
                    new CSharpDbShardDirectoryReserveRequest
                    {
                        DirectoryName = "orders_by_id",
                        LookupKey = "SO-2",
                        TargetKeyspace = "tenants",
                        RouteKey = "tenant-a",
                        ExpectedCurrentMapVersion = 1,
                    },
                    Ct);
                Assert.True(duplicateReserve.Succeeded, duplicateReserve.Message);
                Assert.Equal("AlreadyReserved", duplicateReserve.Status);

                CSharpDbShardDirectoryMutationResult activated = await client.ActivateDirectoryEntryAsync(
                    new CSharpDbShardDirectoryActivateRequest
                    {
                        DirectoryName = "orders_by_id",
                        LookupKey = "SO-2",
                        ExpectedCurrentMapVersion = 1,
                        Operator = "test",
                    },
                    Ct);

                Assert.True(activated.Succeeded, activated.Message);
                Assert.Equal("Activated", activated.Status);
                Assert.Equal(3, activated.PendingMapVersion);
                Assert.Equal(CSharpDbShardDirectoryEntryStates.Active, activated.Entry!.State);

                CSharpDbShardCatalogState pending = await client.GetShardCatalogAsync(Ct);
                Assert.Equal(1, pending.ActiveMap.MapVersion);
                Assert.Equal(3, pending.PendingMap!.MapVersion);
                Assert.Equal(1, Assert.Single(pending.PendingMap.Directories).EntryCount);
            }

            await using (var reloaded = await CSharpDbShardedClient.CreateAsync(options, ct: Ct))
            {
                CSharpDbShardDirectoryResolution resolution = await reloaded.ResolveDirectoryEntryAsync(
                    new CSharpDbShardDirectoryResolveRequest
                    {
                        DirectoryName = "orders_by_id",
                        LookupKey = "SO-2",
                    },
                    Ct);
                Assert.Equal("tenant-a", resolution.Entry.RouteKey);
                Assert.Equal("s0", resolution.RouteResolution.ShardId);

                CSharpDbShardDirectoryMutationResult alreadyActive = await reloaded.ActivateDirectoryEntryAsync(
                    new CSharpDbShardDirectoryActivateRequest
                    {
                        DirectoryName = "orders_by_id",
                        LookupKey = "SO-2",
                        ExpectedCurrentMapVersion = 3,
                    },
                    Ct);
                Assert.True(alreadyActive.Succeeded, alreadyActive.Message);
                Assert.Equal("AlreadyActive", alreadyActive.Status);

                CSharpDbShardDirectoryMutationResult repaired = await reloaded.UpsertDirectoryEntryAsync(
                    new CSharpDbShardDirectoryUpsertRequest
                    {
                        DirectoryName = "orders_by_id",
                        LookupKey = "SO-2",
                        TargetKeyspace = "tenants",
                        RouteKey = "tenant-b",
                        State = CSharpDbShardDirectoryEntryStates.Active,
                        ExpectedCurrentMapVersion = 3,
                        Operator = "test",
                    },
                    Ct);

                Assert.True(repaired.Succeeded, repaired.Message);
                Assert.Equal("Repaired", repaired.Status);
                Assert.Equal(4, repaired.PendingMapVersion);
                Assert.Equal("s1", repaired.Entry!.ShardId);
            }

            await using (var repairedReload = await CSharpDbShardedClient.CreateAsync(options, ct: Ct))
            {
                CSharpDbShardDirectoryResolution repairedResolution = await repairedReload.ResolveDirectoryEntryAsync(
                    new CSharpDbShardDirectoryResolveRequest
                    {
                        DirectoryName = "orders_by_id",
                        LookupKey = "SO-2",
                    },
                    Ct);

                Assert.Equal("tenant-b", repairedResolution.Entry.RouteKey);
                Assert.Equal("s1", repairedResolution.RouteResolution.ShardId);

                CSharpDbShardDirectoryMutationResult stale = await repairedReload.MarkDirectoryEntryStaleAsync(
                    new CSharpDbShardDirectoryMarkStaleRequest
                    {
                        DirectoryName = "orders_by_id",
                        LookupKey = "SO-2",
                        ExpectedCurrentMapVersion = 4,
                        Operator = "test",
                    },
                    Ct);

                Assert.True(stale.Succeeded, stale.Message);
                Assert.Equal("MarkedStale", stale.Status);
                Assert.Equal(CSharpDbShardDirectoryEntryStates.Stale, stale.Entry!.State);
            }

            await using (var staleReload = await CSharpDbShardedClient.CreateAsync(options, ct: Ct))
            {
                await Assert.ThrowsAsync<CSharpDbClientException>(
                    () => staleReload.ResolveDirectoryEntryAsync(
                        new CSharpDbShardDirectoryResolveRequest
                        {
                            DirectoryName = "orders_by_id",
                            LookupKey = "SO-2",
                        },
                        Ct));
            }
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task ShardDirectory_DisableAndDeleteUpdatePendingCatalog()
    {
        string directory = CreateTempDirectory();
        try
        {
            string catalogPath = Path.Combine(directory, "master-catalog.db");
            CSharpDbShardingOptions options = CreateOptions(directory);
            options.Catalog = new CSharpDbShardCatalogOptions
            {
                DataSource = catalogPath,
            };
            options.Directories =
            [
                new CSharpDbShardDirectoryDefinition
                {
                    DirectoryName = "orders_by_id",
                    TargetKeyspace = "tenants",
                    ReadOnly = false,
                },
            ];
            options.DirectoryEntries =
            [
                new CSharpDbShardDirectoryEntry
                {
                    DirectoryName = "orders_by_id",
                    LookupKey = "SO-3",
                    TargetKeyspace = "tenants",
                    RouteKey = "tenant-a",
                    ShardId = "s0",
                    MapVersion = 1,
                    State = CSharpDbShardDirectoryEntryStates.Active,
                },
            ];

            await using (var client = await CSharpDbShardedClient.CreateAsync(options, ct: Ct))
            {
                CSharpDbShardDirectoryMutationResult disabled = await client.DisableDirectoryEntryAsync(
                    new CSharpDbShardDirectoryDisableRequest
                    {
                        DirectoryName = "orders_by_id",
                        LookupKey = "SO-3",
                        ExpectedCurrentMapVersion = 1,
                    },
                    Ct);

                Assert.True(disabled.Succeeded, disabled.Message);
                Assert.Equal("Disabled", disabled.Status);
                Assert.Equal(2, disabled.PendingMapVersion);
                Assert.Equal(CSharpDbShardDirectoryEntryStates.Disabled, disabled.Entry!.State);
            }

            await using (var disabledReload = await CSharpDbShardedClient.CreateAsync(options, ct: Ct))
            {
                CSharpDbShardDirectoryResolution inactive = await disabledReload.ResolveDirectoryEntryAsync(
                    new CSharpDbShardDirectoryResolveRequest
                    {
                        DirectoryName = "orders_by_id",
                        LookupKey = "SO-3",
                        IncludeInactive = true,
                    },
                    Ct);
                Assert.Equal(CSharpDbShardDirectoryEntryStates.Disabled, inactive.Entry.State);

                CSharpDbShardDirectoryMutationResult deleted = await disabledReload.DeleteDirectoryEntryAsync(
                    new CSharpDbShardDirectoryDeleteRequest
                    {
                        DirectoryName = "orders_by_id",
                        LookupKey = "SO-3",
                        ExpectedCurrentMapVersion = 2,
                    },
                    Ct);

                Assert.True(deleted.Succeeded, deleted.Message);
                Assert.Equal("Deleted", deleted.Status);
                Assert.Equal(3, deleted.PendingMapVersion);
                Assert.Equal(CSharpDbShardDirectoryEntryStates.Deleted, deleted.Entry!.State);
            }

            await using (var deletedReload = await CSharpDbShardedClient.CreateAsync(options, ct: Ct))
            {
                CSharpDbShardDirectoryMutationResult removed = await deletedReload.DeleteDirectoryEntryAsync(
                    new CSharpDbShardDirectoryDeleteRequest
                    {
                        DirectoryName = "orders_by_id",
                        LookupKey = "SO-3",
                        RemoveEntry = true,
                        ExpectedCurrentMapVersion = 3,
                    },
                    Ct);

                Assert.True(removed.Succeeded, removed.Message);
                Assert.Equal("Removed", removed.Status);
                Assert.Equal(4, removed.PendingMapVersion);
                Assert.Null(removed.Entry);
            }

            await using (var removedReload = await CSharpDbShardedClient.CreateAsync(options, ct: Ct))
            {
                CSharpDbClientException missing = await Assert.ThrowsAsync<CSharpDbClientException>(
                    () => removedReload.ResolveDirectoryEntryAsync(
                        new CSharpDbShardDirectoryResolveRequest
                        {
                            DirectoryName = "orders_by_id",
                            LookupKey = "SO-3",
                            IncludeInactive = true,
                        },
                        Ct));
                Assert.Contains("was not found", missing.Message);
            }
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task ReplicaMetadata_IsExposedAndCannotOwnRoutes()
    {
        string directory = CreateTempDirectory();
        try
        {
            DateTimeOffset lastReplicatedUtc = new(2026, 6, 1, 12, 30, 0, TimeSpan.Zero);
            CSharpDbShardingOptions options = CreateOptions(directory);
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
                    ReplicationLagBytes = 128,
                    LastReplicatedUtc = lastReplicatedUtc,
                },
            ];

            await using (var client = await CSharpDbShardedClient.CreateAsync(options, ct: Ct))
            {
                CSharpDbShardDefinitionSnapshot replica = Assert.Single(
                    (await client.GetShardMapAsync(Ct)).Shards,
                    shard => shard.ShardId == "s1-replica");
                Assert.Equal(CSharpDbShardRoles.Replica, replica.Role);
                Assert.Equal("s1", replica.PrimaryShardId);
                Assert.True(replica.PromotionEligible);
                Assert.Equal(128, replica.ReplicationLagBytes);
                Assert.Equal(lastReplicatedUtc, replica.LastReplicatedUtc);

                CSharpDbShardStatus replicaStatus = Assert.Single(
                    await client.GetShardStatusAsync(Ct),
                    status => status.ShardId == "s1-replica");
                Assert.True(replicaStatus.Healthy);
                Assert.Equal(CSharpDbShardRoles.Replica, replicaStatus.Role);
                Assert.Equal("s1", replicaStatus.PrimaryShardId);
                Assert.True(replicaStatus.PromotionEligible);
                Assert.True(replicaStatus.CanPromote);
                Assert.Equal(128, replicaStatus.ReplicationLagBytes);
                Assert.Equal(lastReplicatedUtc, replicaStatus.LastReplicatedUtc);
            }

            CSharpDbShardingOptions replicaBucketOwner = CreateOptions(directory);
            replicaBucketOwner.Shards =
            [
                .. replicaBucketOwner.Shards,
                new CSharpDbShardDefinition
                {
                    ShardId = "s1-replica",
                    DataSource = Path.Combine(directory, "s1-replica.db"),
                    Role = CSharpDbShardRoles.Replica,
                    PrimaryShardId = "s1",
                },
            ];
            replicaBucketOwner.BucketRanges =
            [
                new CSharpDbShardBucketRange { StartBucketInclusive = 0, EndBucketExclusive = 2, ShardId = "s0" },
                new CSharpDbShardBucketRange { StartBucketInclusive = 2, EndBucketExclusive = 4, ShardId = "s1-replica" },
            ];

            CSharpDbClientConfigurationException bucketError = await Assert.ThrowsAsync<CSharpDbClientConfigurationException>(
                () => CSharpDbShardedClient.CreateAsync(replicaBucketOwner, ct: Ct));
            Assert.Contains("only primary shards", bucketError.Message);

            CSharpDbShardingOptions replicaPinnedOwner = CreateOptions(directory);
            replicaPinnedOwner.Shards =
            [
                .. replicaPinnedOwner.Shards,
                new CSharpDbShardDefinition
                {
                    ShardId = "s1-replica",
                    DataSource = Path.Combine(directory, "s1-replica.db"),
                    Role = CSharpDbShardRoles.Replica,
                    PrimaryShardId = "s1",
                },
            ];
            replicaPinnedOwner.ExactKeyPins["tenant-b"] = "s1-replica";

            CSharpDbClientConfigurationException pinError = await Assert.ThrowsAsync<CSharpDbClientConfigurationException>(
                () => CSharpDbShardedClient.CreateAsync(replicaPinnedOwner, ct: Ct));
            Assert.Contains("only primary shards", pinError.Message);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task CatalogApply_WritesPendingMapAndReloadUsesCatalogVersion()
    {
        string directory = CreateTempDirectory();
        try
        {
            string catalogPath = Path.Combine(directory, "master-catalog.db");
            CSharpDbShardingOptions options = CreateOptions(directory);
            options.Catalog = new CSharpDbShardCatalogOptions
            {
                DataSource = catalogPath,
            };
            options.Directories =
            [
                new CSharpDbShardDirectoryDefinition
                {
                    DirectoryName = "orders_by_id",
                    TargetKeyspace = "tenants",
                    Description = "order id lookup",
                },
            ];
            options.DirectoryEntries =
            [
                new CSharpDbShardDirectoryEntry
                {
                    DirectoryName = "orders_by_id",
                    LookupKey = "SO-1",
                    TargetKeyspace = "tenants",
                    RouteKey = "tenant-a",
                    ShardId = "s0",
                    MapVersion = 1,
                    State = "Active",
                },
            ];

            await using (var client = await CSharpDbShardedClient.CreateAsync(options, ct: Ct))
            {
                CSharpDbShardCatalogState initial = await client.GetShardCatalogAsync(Ct);
                Assert.True(initial.IsCatalogEnabled);
                Assert.True(initial.IsWritable);
                Assert.Equal(1, initial.ActiveMap.MapVersion);
                Assert.Null(initial.PendingMap);

                CSharpDbShardingOptions proposed = CreateOptions(directory);
                proposed.MapVersion = 2;
                proposed.Catalog = options.Catalog;
                proposed.Directories =
                [
                    new CSharpDbShardDirectoryDefinition
                    {
                        DirectoryName = "orders_by_id",
                        TargetKeyspace = "tenants",
                        Description = "order id lookup",
                    },
                ];
                proposed.DirectoryEntries =
                [
                    new CSharpDbShardDirectoryEntry
                    {
                        DirectoryName = "orders_by_id",
                        LookupKey = "SO-1",
                        TargetKeyspace = "tenants",
                        RouteKey = "tenant-a",
                        ShardId = "s0",
                        MapVersion = 2,
                        State = "Active",
                    },
                ];

                CSharpDbShardCatalogValidationResult validation =
                    await client.ValidateShardCatalogUpdateAsync(new CSharpDbShardCatalogUpdateRequest
                    {
                        Options = proposed,
                        ExpectedCurrentMapVersion = 1,
                        Operator = "test",
                        Comment = "add directory metadata",
                    }, Ct);

                Assert.True(validation.IsValid);
                Assert.False(validation.RequiresDataMigration);
                Assert.NotNull(validation.Preview);
                Assert.Equal(2, validation.Preview!.MapVersion);
                Assert.Equal(1, Assert.Single(validation.Preview.Directories).EntryCount);

                CSharpDbShardCatalogApplyResult applied =
                    await client.ApplyShardCatalogUpdateAsync(new CSharpDbShardCatalogUpdateRequest
                    {
                        Options = proposed,
                        ExpectedCurrentMapVersion = 1,
                        Operator = "test",
                        Comment = "add directory metadata",
                    }, Ct);

                Assert.True(applied.Applied);
                Assert.True(applied.RequiresRestart);
                Assert.True(File.Exists(catalogPath));

                CSharpDbShardCatalogState pending = await client.GetShardCatalogAsync(Ct);
                Assert.Equal(1, pending.ActiveMap.MapVersion);
                Assert.Equal(2, pending.PendingMap!.MapVersion);
                Assert.Single(pending.History);
            }

            await using (var reloaded = await CSharpDbShardedClient.CreateAsync(options, ct: Ct))
            {
                CSharpDbShardMapSnapshot reloadedMap = await reloaded.GetShardMapAsync(Ct);
                Assert.Equal(2, reloadedMap.MapVersion);
                Assert.Equal(1, Assert.Single(reloadedMap.Directories).EntryCount);
            }
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task CatalogValidation_RejectsOwnershipChangeWithoutAcknowledgement()
    {
        string directory = CreateTempDirectory();
        try
        {
            CSharpDbShardingOptions options = CreateOptions(directory);
            options.Catalog = new CSharpDbShardCatalogOptions
            {
                DataSource = Path.Combine(directory, "master-catalog.db"),
            };

            await using var client = await CSharpDbShardedClient.CreateAsync(options, ct: Ct);
            CSharpDbShardingOptions proposed = CreateOptions(directory);
            proposed.MapVersion = 2;
            proposed.Catalog = options.Catalog;
            proposed.ExactKeyPins["tenant-a"] = "s1";

            CSharpDbShardCatalogValidationResult validation =
                await client.ValidateShardCatalogUpdateAsync(new CSharpDbShardCatalogUpdateRequest
                {
                    Options = proposed,
                    ExpectedCurrentMapVersion = 1,
                }, Ct);

            Assert.False(validation.IsValid);
            Assert.True(validation.RequiresDataMigration);
            Assert.Contains(validation.Issues, issue => issue.Code == "migration-required");

            CSharpDbShardCatalogValidationResult acknowledged =
                await client.ValidateShardCatalogUpdateAsync(new CSharpDbShardCatalogUpdateRequest
                {
                    Options = proposed,
                    ExpectedCurrentMapVersion = 1,
                    AllowMetadataOnlyOwnershipChange = true,
                }, Ct);

            Assert.True(acknowledged.IsValid);
            Assert.True(acknowledged.RequiresDataMigration);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task ExactKeyMigration_CopiesManifestDataAndWritesPendingPin()
    {
        string directory = CreateTempDirectory();
        try
        {
            string catalogPath = Path.Combine(directory, "master-catalog.db");
            CSharpDbShardingOptions options = CreateOptions(directory);
            options.Catalog = new CSharpDbShardCatalogOptions
            {
                DataSource = catalogPath,
            };
            options.Directories =
            [
                new CSharpDbShardDirectoryDefinition
                {
                    DirectoryName = "orders_by_id",
                    TargetKeyspace = "tenants",
                    Description = "order id lookup",
                },
            ];
            options.DirectoryEntries =
            [
                new CSharpDbShardDirectoryEntry
                {
                    DirectoryName = "orders_by_id",
                    LookupKey = "SO-1",
                    TargetKeyspace = "tenants",
                    RouteKey = "tenant-a",
                    ShardId = "s0",
                    MapVersion = 1,
                    State = "Active",
                },
            ];

            await using (var client = await CSharpDbShardedClient.CreateAsync(options, ct: Ct))
            {
                IReadOnlyList<CSharpDbShardSqlExecutionResult> schemaResults =
                    await client.ExecuteSqlOnAllShardsAsync(
                        "CREATE TABLE orders (id INTEGER PRIMARY KEY, tenant_id TEXT, total INTEGER);",
                        Ct);
                Assert.All(schemaResults, result => Assert.Null(result.Error));

                ICSharpDbClient tenantA = client.ForRoute(new CSharpDbRouteContext
                {
                    Keyspace = "tenants",
                    Key = "tenant-a",
                });
                ICSharpDbClient tenantB = client.ForRoute(new CSharpDbRouteContext
                {
                    Keyspace = "tenants",
                    Key = "tenant-b",
                });

                Assert.Equal(1, await tenantA.InsertRowAsync("orders", new Dictionary<string, object?>
                {
                    ["id"] = 1L,
                    ["tenant_id"] = "tenant-a",
                    ["total"] = 42L,
                }, Ct));
                Assert.Equal(1, await tenantA.InsertRowAsync("orders", new Dictionary<string, object?>
                {
                    ["id"] = 2L,
                    ["tenant_id"] = "tenant-a",
                    ["total"] = 99L,
                }, Ct));
                Assert.Equal(1, await tenantB.InsertRowAsync("orders", new Dictionary<string, object?>
                {
                    ["id"] = 3L,
                    ["tenant_id"] = "tenant-b",
                    ["total"] = 7L,
                }, Ct));

                using JsonDocument sourceDocument = JsonDocument.Parse("""{"tenantId":"tenant-a","status":"paid"}""");
                await tenantA.PutDocumentAsync("order_documents", "doc-1", sourceDocument.RootElement, Ct);

                CSharpDbShardMigrationResult migration = await client.MigrateExactRouteKeyAsync(
                    new CSharpDbShardExactKeyMigrationRequest
                    {
                        Keyspace = "tenants",
                        RouteKey = "tenant-a",
                        DestinationShardId = "s1",
                        ExpectedCurrentMapVersion = 1,
                        Operator = "test",
                        Manifest = new CSharpDbShardMigrationManifest
                        {
                            PageSize = 2,
                            Tables =
                            [
                                new CSharpDbShardMigrationTableManifest
                                {
                                    TableName = "orders",
                                    RouteKeyColumn = "tenant_id",
                                    PrimaryKeyColumn = "id",
                                },
                            ],
                            Collections =
                            [
                                new CSharpDbShardMigrationCollectionManifest
                                {
                                    CollectionName = "order_documents",
                                    RouteKeyPropertyName = "tenantId",
                                },
                            ],
                        },
                    },
                    Ct);

                Assert.True(migration.Succeeded, string.Join(Environment.NewLine, migration.Issues.Select(issue => issue.Message)));
                Assert.Equal("PendingActivation", migration.Status);
                Assert.Equal("s0", migration.SourceShardId);
                Assert.Equal("s1", migration.DestinationShardId);
                Assert.Equal(2, migration.PendingMapVersion);
                Assert.True(migration.RequiresRestart);

                CSharpDbShardMigrationTableResult table = Assert.Single(migration.Tables);
                Assert.True(table.Verified, table.Error);
                Assert.Equal(2, table.SourceRows);
                Assert.Equal(2, table.DestinationRows);
                Assert.Equal(2, table.RowsCopied);

                CSharpDbShardMigrationCollectionResult collection = Assert.Single(migration.Collections);
                Assert.True(collection.Verified, collection.Error);
                Assert.Equal(1, collection.SourceDocuments);
                Assert.Equal(1, collection.DestinationDocuments);

                Dictionary<string, object?>? copied = await client.ForShardId("s1").GetRowByPkAsync("orders", "id", 1L, Ct);
                Assert.NotNull(copied);
                Assert.Equal("tenant-a", Assert.IsType<string>(copied!["tenant_id"]));

                CSharpDbShardCatalogState pending = await client.GetShardCatalogAsync(Ct);
                Assert.Equal(1, pending.ActiveMap.MapVersion);
                Assert.Equal("s1", pending.PendingMap!.ExactKeyPins["tenant-a"]);
                Assert.Equal(1, Assert.Single(pending.PendingMap.Directories).EntryCount);

                CSharpDbShardMigrationProgress progress = Assert.Single(await client.GetShardMigrationProgressAsync(Ct));
                Assert.Equal(migration.MigrationId, progress.MigrationId);
                Assert.Equal("ExactRouteKey", progress.MigrationType);
                Assert.Equal("PendingActivation", progress.Status);
                Assert.Equal("Completed", progress.Phase);
                Assert.Equal(100d, progress.PercentComplete);
                Assert.Equal(1, progress.Attempt);
                Assert.Equal(2, progress.PendingMapVersion);
                Assert.True(progress.RequiresRestart);

                CSharpDbShardMigrationProgress? progressById =
                    await client.GetShardMigrationProgressAsync(migration.MigrationId, Ct);
                Assert.NotNull(progressById);
                Assert.Equal(progress.Status, progressById.Status);

                CSharpDbShardMigrationHistoryEntry history = Assert.Single(await client.GetShardMigrationHistoryAsync(Ct));
                Assert.Equal(migration.MigrationId, history.MigrationId);
                Assert.Equal("ExactRouteKey", history.MigrationType);
                Assert.True(history.Succeeded);
                Assert.Equal("PendingActivation", history.Status);
                Assert.Equal("tenant-a", history.RouteKey);
                Assert.Equal("s0", history.SourceShardId);
                Assert.Equal("s1", history.DestinationShardId);
                Assert.Equal(2, history.PendingMapVersion);
                Assert.Equal(2, Assert.Single(history.Tables).SourceRows);
                Assert.Equal(1, Assert.Single(history.Collections).SourceDocuments);

                CSharpDbShardMigrationResult resume = await client.ResumeShardMigrationAsync(migration.MigrationId, Ct);
                Assert.True(resume.Succeeded);
                Assert.Equal("PendingActivation", resume.Status);
                Assert.Equal(migration.MigrationId, resume.MigrationId);
                Assert.Single(await client.GetShardMigrationHistoryAsync(Ct));
            }

            await using (var reloaded = await CSharpDbShardedClient.CreateAsync(options, ct: Ct))
            {
                CSharpDbShardResolution resolution = await reloaded.ResolveRouteAsync(new CSharpDbRouteContext
                {
                    Keyspace = "tenants",
                    Key = "tenant-a",
                }, Ct);
                Assert.Equal("s1", resolution.ShardId);
                Assert.Equal(2, resolution.MapVersion);
                CSharpDbShardMapSnapshot reloadedMap = await reloaded.GetShardMapAsync(Ct);
                Assert.Equal(1, Assert.Single(reloadedMap.Directories).EntryCount);

                CSharpDbShardDirectoryResolution directoryResolution =
                    await reloaded.ResolveDirectoryEntryAsync(new CSharpDbShardDirectoryResolveRequest
                    {
                        DirectoryName = "orders_by_id",
                        LookupKey = "SO-1",
                    }, Ct);
                Assert.Equal("s1", directoryResolution.Entry.ShardId);
                Assert.Equal(2, directoryResolution.Entry.MapVersion);
                Assert.Equal("s1", directoryResolution.RouteResolution.ShardId);
                Assert.Equal(2, directoryResolution.RouteResolution.MapVersion);

                ICSharpDbClient tenantA = reloaded.ForRoute(new CSharpDbRouteContext
                {
                    Keyspace = "tenants",
                    Key = "tenant-a",
                });
                Dictionary<string, object?>? row = await tenantA.GetRowByPkAsync("orders", "id", 2L, Ct);
                Assert.NotNull(row);
                Assert.Equal(99L, Assert.IsType<long>(row!["total"]));

                JsonElement? document = await tenantA.GetDocumentAsync("order_documents", "doc-1", Ct);
                Assert.NotNull(document);
                Assert.Equal("paid", document.Value.GetProperty("status").GetString());

                CSharpDbShardMigrationHistoryEntry history = Assert.Single(await reloaded.GetShardMigrationHistoryAsync(Ct));
                Assert.Equal("PendingActivation", history.Status);
            }
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task BucketRangeMigration_CopiesManifestDataAndWritesPendingBucketMap()
    {
        string directory = CreateTempDirectory();
        try
        {
            string routeKey = FindRouteKeyForBucket("tenants", bucket: 0, virtualBucketCount: 4);
            string catalogPath = Path.Combine(directory, "master-catalog.db");
            CSharpDbShardingOptions options = CreateOptions(directory);
            options.Catalog = new CSharpDbShardCatalogOptions
            {
                DataSource = catalogPath,
            };

            await using (var client = await CSharpDbShardedClient.CreateAsync(options, ct: Ct))
            {
                IReadOnlyList<CSharpDbShardSqlExecutionResult> schemaResults =
                    await client.ExecuteSqlOnAllShardsAsync(
                        "CREATE TABLE bucket_orders (id INTEGER PRIMARY KEY, tenant_id TEXT, total INTEGER);",
                        Ct);
                Assert.All(schemaResults, result => Assert.Null(result.Error));

                ICSharpDbClient routed = client.ForRoute(new CSharpDbRouteContext
                {
                    Keyspace = "tenants",
                    Key = routeKey,
                });
                Assert.Equal(1, await routed.InsertRowAsync("bucket_orders", new Dictionary<string, object?>
                {
                    ["id"] = 20L,
                    ["tenant_id"] = routeKey,
                    ["total"] = 500L,
                }, Ct));

                CSharpDbShardMigrationResult migration = await client.MigrateBucketRangeAsync(
                    new CSharpDbShardBucketRangeMigrationRequest
                    {
                        Keyspace = "tenants",
                        SourceShardId = "s0",
                        DestinationShardId = "s1",
                        StartBucketInclusive = 0,
                        EndBucketExclusive = 1,
                        ExpectedCurrentMapVersion = 1,
                        Operator = "test",
                        Manifest = new CSharpDbShardMigrationManifest
                        {
                            Tables =
                            [
                                new CSharpDbShardMigrationTableManifest
                                {
                                    TableName = "bucket_orders",
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
                Assert.Equal(2, migration.PendingMapVersion);

                CSharpDbShardMigrationTableResult table = Assert.Single(migration.Tables);
                Assert.True(table.Verified, table.Error);
                Assert.Equal(1, table.SourceRows);
                Assert.Equal(1, table.DestinationRows);
                Assert.Equal(1, table.RowsCopied);

                Dictionary<string, object?>? copied = await client.ForShardId("s1").GetRowByPkAsync("bucket_orders", "id", 20L, Ct);
                Assert.NotNull(copied);
                Assert.Equal(routeKey, Assert.IsType<string>(copied!["tenant_id"]));

                CSharpDbShardCatalogState pending = await client.GetShardCatalogAsync(Ct);
                Assert.Equal(1, pending.ActiveMap.MapVersion);
                Assert.Equal(2, pending.PendingMap!.MapVersion);
                Assert.Equal("s1", GetOwnerForBucket(pending.PendingMap, 0));
                Assert.Equal("s0", GetOwnerForBucket(pending.PendingMap, 1));

                CSharpDbShardMigrationProgress progress = Assert.Single(await client.GetShardMigrationProgressAsync(Ct));
                Assert.Equal(migration.MigrationId, progress.MigrationId);
                Assert.Equal("BucketRange", progress.MigrationType);
                Assert.Equal("PendingActivation", progress.Status);
                Assert.Equal("Completed", progress.Phase);
                Assert.Equal(100d, progress.PercentComplete);
                Assert.Equal(2, progress.PendingMapVersion);

                CSharpDbShardMigrationHistoryEntry history = Assert.Single(await client.GetShardMigrationHistoryAsync(Ct));
                Assert.Equal(migration.MigrationId, history.MigrationId);
                Assert.Equal("BucketRange", history.MigrationType);
                Assert.Equal("bucket-range:[0,1)", history.RouteKey);
                Assert.Equal("PendingActivation", history.Status);
            }

            await using (var reloaded = await CSharpDbShardedClient.CreateAsync(options, ct: Ct))
            {
                CSharpDbShardResolution resolution = await reloaded.ResolveRouteAsync(new CSharpDbRouteContext
                {
                    Keyspace = "tenants",
                    Key = routeKey,
                }, Ct);
                Assert.Equal("s1", resolution.ShardId);
                Assert.Equal(2, resolution.MapVersion);

                ICSharpDbClient routed = reloaded.ForRoute(new CSharpDbRouteContext
                {
                    Keyspace = "tenants",
                    Key = routeKey,
                });
                Dictionary<string, object?>? row = await routed.GetRowByPkAsync("bucket_orders", "id", 20L, Ct);
                Assert.NotNull(row);
                Assert.Equal(500L, Assert.IsType<long>(row!["total"]));
            }
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task ExactKeyMigration_RejectsMissingManifestItems()
    {
        string directory = CreateTempDirectory();
        try
        {
            CSharpDbShardingOptions options = CreateOptions(directory);
            options.Catalog = new CSharpDbShardCatalogOptions
            {
                DataSource = Path.Combine(directory, "master-catalog.db"),
            };

            await using var client = await CSharpDbShardedClient.CreateAsync(options, ct: Ct);
            CSharpDbShardMigrationResult migration = await client.MigrateExactRouteKeyAsync(
                new CSharpDbShardExactKeyMigrationRequest
                {
                    Keyspace = "tenants",
                    RouteKey = "tenant-a",
                    DestinationShardId = "s1",
                    ExpectedCurrentMapVersion = 1,
                    Manifest = new CSharpDbShardMigrationManifest(),
                },
                Ct);

            Assert.False(migration.Succeeded);
            Assert.Equal("Rejected", migration.Status);
            Assert.Contains(migration.Issues, issue => issue.Code == "missing-manifest-items");

            CSharpDbShardMigrationHistoryEntry history = Assert.Single(await client.GetShardMigrationHistoryAsync(Ct));
            Assert.Equal(migration.MigrationId, history.MigrationId);
            Assert.Equal("Rejected", history.Status);
            Assert.False(history.Succeeded);
            Assert.Contains(history.Issues, issue => issue.Code == "missing-manifest-items");

            CSharpDbShardMigrationProgress progress = Assert.Single(await client.GetShardMigrationProgressAsync(Ct));
            Assert.Equal(migration.MigrationId, progress.MigrationId);
            Assert.Equal("Rejected", progress.Status);
            Assert.Equal("Rejected", progress.Phase);
            Assert.Equal(0, progress.CompletedSteps);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task ExactKeyMigration_RecordsProgressInCSharpDbCatalog()
    {
        string directory = CreateTempDirectory();
        try
        {
            CSharpDbShardingOptions options = CreateOptions(directory);
            options.Catalog = new CSharpDbShardCatalogOptions
            {
                DataSource = Path.Combine(directory, "master-catalog.db"),
            };

            await using var client = await CSharpDbShardedClient.CreateAsync(options, ct: Ct);
            CSharpDbShardMigrationResult migration = await client.MigrateExactRouteKeyAsync(
                new CSharpDbShardExactKeyMigrationRequest
                {
                    Keyspace = "tenants",
                    RouteKey = "tenant-a",
                    DestinationShardId = "s1",
                    ExpectedCurrentMapVersion = 1,
                    Manifest = new CSharpDbShardMigrationManifest(),
                },
                Ct);

            Assert.False(migration.Succeeded);
            Assert.Equal("Rejected", migration.Status);

            CSharpDbShardMigrationHistoryEntry history = Assert.Single(await client.GetShardMigrationHistoryAsync(Ct));
            Assert.Equal(migration.MigrationId, history.MigrationId);
            Assert.Equal("Rejected", history.Status);

            CSharpDbShardMigrationProgress progress = Assert.Single(await client.GetShardMigrationProgressAsync(Ct));
            Assert.Equal(migration.MigrationId, progress.MigrationId);
            Assert.Equal("Rejected", progress.Status);
            Assert.Equal(0, progress.CompletedSteps);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task ExactKeyMigration_VerificationFailureRequiresOperatorRecovery()
    {
        string directory = CreateTempDirectory();
        try
        {
            CSharpDbShardingOptions options = CreateOptions(directory);
            options.Catalog = new CSharpDbShardCatalogOptions
            {
                DataSource = Path.Combine(directory, "master-catalog.db"),
            };

            await using var client = await CSharpDbShardedClient.CreateAsync(options, ct: Ct);
            CSharpDbShardMigrationResult migration = await client.MigrateExactRouteKeyAsync(
                new CSharpDbShardExactKeyMigrationRequest
                {
                    Keyspace = "tenants",
                    RouteKey = "tenant-a",
                    DestinationShardId = "s1",
                    ExpectedCurrentMapVersion = 1,
                    Manifest = new CSharpDbShardMigrationManifest
                    {
                        Tables =
                        [
                            new CSharpDbShardMigrationTableManifest
                            {
                                TableName = "missing_orders",
                                RouteKeyColumn = "tenant_id",
                                PrimaryKeyColumn = "id",
                            },
                        ],
                    },
                },
                Ct);

            Assert.False(migration.Succeeded);
            Assert.Equal("VerificationFailed", migration.Status);
            Assert.True(migration.RequiresOperatorRecovery);
            Assert.Contains("rerun the migration", migration.RecoveryAction);
            Assert.Null(migration.PendingMapVersion);
            Assert.Contains(migration.Issues, issue => issue.Code == "table-verification-failed");

            CSharpDbShardMigrationHistoryEntry history = Assert.Single(await client.GetShardMigrationHistoryAsync(Ct));
            Assert.Equal(migration.MigrationId, history.MigrationId);
            Assert.True(history.RequiresOperatorRecovery);
            Assert.Equal(migration.RecoveryAction, history.RecoveryAction);

            CSharpDbShardMigrationProgress progress = Assert.Single(await client.GetShardMigrationProgressAsync(Ct));
            Assert.Equal(migration.MigrationId, progress.MigrationId);
            Assert.Equal("VerificationFailed", progress.Status);
            Assert.Equal("VerificationFailed", progress.Phase);
            Assert.True(progress.RequiresOperatorRecovery);
            Assert.Equal(migration.RecoveryAction, progress.RecoveryAction);
            Assert.Contains(progress.Issues, issue => issue.Code == "table-verification-failed");
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task ExactKeyMigration_RetryUsesStoredPlanWithOverwrite()
    {
        string directory = CreateTempDirectory();
        try
        {
            CSharpDbShardingOptions options = CreateOptions(directory);
            options.Catalog = new CSharpDbShardCatalogOptions
            {
                DataSource = Path.Combine(directory, "master-catalog.db"),
            };

            await using var client = await CSharpDbShardedClient.CreateAsync(options, ct: Ct);
            IReadOnlyList<CSharpDbShardSqlExecutionResult> schemaResults =
                await client.ExecuteSqlOnAllShardsAsync(
                    "CREATE TABLE retry_orders (id INTEGER PRIMARY KEY, tenant_id TEXT, total INTEGER);",
                    Ct);
            Assert.All(schemaResults, result => Assert.Null(result.Error));

            ICSharpDbClient tenantA = client.ForRoute(new CSharpDbRouteContext
            {
                Keyspace = "tenants",
                Key = "tenant-a",
            });
            Assert.Equal(1, await tenantA.InsertRowAsync("retry_orders", new Dictionary<string, object?>
            {
                ["id"] = 40L,
                ["tenant_id"] = "tenant-a",
                ["total"] = 100L,
            }, Ct));
            Assert.Equal(1, await client.ForShardId("s1").InsertRowAsync("retry_orders", new Dictionary<string, object?>
            {
                ["id"] = 40L,
                ["tenant_id"] = "tenant-a",
                ["total"] = 1L,
            }, Ct));

            CSharpDbShardMigrationResult firstAttempt = await client.MigrateExactRouteKeyAsync(
                new CSharpDbShardExactKeyMigrationRequest
                {
                    Keyspace = "tenants",
                    RouteKey = "tenant-a",
                    DestinationShardId = "s1",
                    ExpectedCurrentMapVersion = 1,
                    OverwriteDestinationRows = false,
                    Manifest = new CSharpDbShardMigrationManifest
                    {
                        Tables =
                        [
                            new CSharpDbShardMigrationTableManifest
                            {
                                TableName = "retry_orders",
                                RouteKeyColumn = "tenant_id",
                                PrimaryKeyColumn = "id",
                            },
                        ],
                    },
                },
                Ct);

            Assert.False(firstAttempt.Succeeded);
            Assert.Equal("VerificationFailed", firstAttempt.Status);
            Assert.True(firstAttempt.RequiresOperatorRecovery);
            Assert.Null(firstAttempt.PendingMapVersion);

            CSharpDbShardMigrationProgress failedProgress =
                Assert.Single(await client.GetShardMigrationProgressAsync(Ct));
            Assert.Equal(firstAttempt.MigrationId, failedProgress.MigrationId);
            Assert.Equal(1, failedProgress.Attempt);
            Assert.Equal("VerificationFailed", failedProgress.Status);
            Assert.True(failedProgress.RequiresOperatorRecovery);

            CSharpDbShardMigrationResult retry = await client.RetryShardMigrationAsync(firstAttempt.MigrationId, Ct);
            Assert.True(retry.Succeeded, string.Join(Environment.NewLine, retry.Issues.Select(issue => issue.Message)));
            Assert.Equal("PendingActivation", retry.Status);
            Assert.Equal(firstAttempt.MigrationId, retry.MigrationId);
            Assert.Equal(2, retry.PendingMapVersion);

            CSharpDbShardMigrationProgress retryProgress =
                Assert.Single(await client.GetShardMigrationProgressAsync(Ct));
            Assert.Equal(firstAttempt.MigrationId, retryProgress.MigrationId);
            Assert.Equal(2, retryProgress.Attempt);
            Assert.Equal("PendingActivation", retryProgress.Status);
            Assert.Equal("Completed", retryProgress.Phase);
            Assert.Equal(100d, retryProgress.PercentComplete);

            Dictionary<string, object?>? destination =
                await client.ForShardId("s1").GetRowByPkAsync("retry_orders", "id", 40L, Ct);
            Assert.NotNull(destination);
            Assert.Equal(100L, Assert.IsType<long>(destination!["total"]));

            IReadOnlyList<CSharpDbShardMigrationHistoryEntry> history =
                await client.GetShardMigrationHistoryAsync(Ct);
            Assert.Equal(2, history.Count);
            Assert.All(history, entry => Assert.Equal(firstAttempt.MigrationId, entry.MigrationId));
            Assert.Contains(history, entry => entry.Status == "VerificationFailed");
            Assert.Contains(history, entry => entry.Status == "PendingActivation");
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task RouteBoundClient_RoutesOperationsToStableShard()
    {
        string directory = CreateTempDirectory();
        try
        {
            await using var client = await CSharpDbShardedClient.CreateAsync(CreateOptions(directory), ct: Ct);
            ICSharpDbClient tenantA = client.ForRoute(new CSharpDbRouteContext
            {
                Keyspace = "tenants",
                Key = "tenant-a",
            });

            Assert.Null((await tenantA.ExecuteSqlAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);", Ct)).Error);
            Assert.Equal(1, await tenantA.InsertRowAsync("items", new Dictionary<string, object?>
            {
                ["id"] = 1L,
                ["name"] = "alpha",
            }, Ct));

            ICSharpDbClient tenantAAgain = client.ForRoute(new CSharpDbRouteContext
            {
                Keyspace = "tenants",
                Key = "tenant-a",
            });
            Dictionary<string, object?>? row = await tenantAAgain.GetRowByPkAsync("items", "id", 1L, Ct);
            Assert.NotNull(row);
            Assert.Equal("alpha", Assert.IsType<string>(row!["name"]));

            await Assert.ThrowsAsync<CSharpDbClientException>(
                () => ((ICSharpDbClient)client).ExecuteSqlAsync("SELECT 1;", Ct));
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task ReadOnlyFanOut_ReturnsPerShardResultsAndRejectsWrites()
    {
        string directory = CreateTempDirectory();
        try
        {
            await using var client = await CSharpDbShardedClient.CreateAsync(CreateOptions(directory), ct: Ct);
            IReadOnlyList<CSharpDbShardSqlExecutionResult> schemaResults =
                await client.ExecuteSqlOnAllShardsAsync(
                    "CREATE TABLE fanout_items (id INTEGER PRIMARY KEY, tenant_id TEXT, name TEXT);",
                    Ct);
            Assert.All(schemaResults, result => Assert.Null(result.Error));

            ICSharpDbClient tenantA = client.ForRoute(new CSharpDbRouteContext
            {
                Keyspace = "tenants",
                Key = "tenant-a",
            });
            ICSharpDbClient tenantB = client.ForRoute(new CSharpDbRouteContext
            {
                Keyspace = "tenants",
                Key = "tenant-b",
            });

            Assert.Equal(1, await tenantA.InsertRowAsync("fanout_items", new Dictionary<string, object?>
            {
                ["id"] = 1L,
                ["tenant_id"] = "tenant-a",
                ["name"] = "alpha",
            }, Ct));
            Assert.Equal(1, await tenantB.InsertRowAsync("fanout_items", new Dictionary<string, object?>
            {
                ["id"] = 2L,
                ["tenant_id"] = "tenant-b",
                ["name"] = "bravo",
            }, Ct));

            IReadOnlyList<CSharpDbShardSqlExecutionResult> readResults =
                await client.ExecuteReadOnlySqlOnAllShardsAsync(
                    "SELECT COUNT(*) FROM fanout_items;",
                    Ct);

            Assert.Equal(2, readResults.Count);
            Assert.All(readResults, result =>
            {
                Assert.Null(result.Error);
                Assert.True(result.Result!.IsQuery);
                Assert.Equal(1L, Assert.IsType<long>(Assert.Single(result.Result.Rows!)[0]));
            });

            await Assert.ThrowsAsync<CSharpDbClientException>(
                () => client.ExecuteReadOnlySqlOnAllShardsAsync(
                    "INSERT INTO fanout_items (id, tenant_id, name) VALUES (3, 'tenant-a', 'blocked');",
                    Ct));
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task Transactions_CanRouteFromPrefixedTransactionIdWithoutRouteContext()
    {
        string directory = CreateTempDirectory();
        try
        {
            await using var client = await CSharpDbShardedClient.CreateAsync(CreateOptions(directory), ct: Ct);
            ICSharpDbClient tenantA = client.ForRoute(new CSharpDbRouteContext
            {
                Keyspace = "tenants",
                Key = "tenant-a",
            });
            ICSharpDbClient tenantB = client.ForRoute(new CSharpDbRouteContext
            {
                Keyspace = "tenants",
                Key = "tenant-b",
            });

            Assert.Null((await tenantA.ExecuteSqlAsync("CREATE TABLE tx_items (id INTEGER PRIMARY KEY, name TEXT);", Ct)).Error);
            TransactionSessionInfo tx = await tenantA.BeginTransactionAsync(Ct);
            Assert.StartsWith("csdbshard:1:s0:", tx.TransactionId, StringComparison.Ordinal);

            await Assert.ThrowsAsync<CSharpDbClientException>(
                () => tenantB.ExecuteInTransactionAsync(tx.TransactionId, "INSERT INTO tx_items VALUES (2, 'wrong');", Ct));

            SqlExecutionResult insert = await client.ExecuteInTransactionAsync(
                tx.TransactionId,
                "INSERT INTO tx_items VALUES (1, 'committed');",
                Ct);
            Assert.Null(insert.Error);
            await client.CommitTransactionAsync(tx.TransactionId, Ct);

            SqlExecutionResult verify = await tenantA.ExecuteSqlAsync("SELECT name FROM tx_items WHERE id = 1;", Ct);
            Assert.Null(verify.Error);
            Assert.Equal("committed", Assert.IsType<string>(Assert.Single(verify.Rows!)[0]));
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task ResolveRoute_RejectsDisabledShard()
    {
        string directory = CreateTempDirectory();
        try
        {
            var options = new CSharpDbShardingOptions
            {
                Keyspace = "tenants",
                VirtualBucketCount = 1,
                Shards =
                [
                    new CSharpDbShardDefinition
                    {
                        ShardId = "disabled",
                        Enabled = false,
                        DataSource = Path.Combine(directory, "disabled.db"),
                    },
                    new CSharpDbShardDefinition
                    {
                        ShardId = "active",
                        DataSource = Path.Combine(directory, "active.db"),
                    },
                ],
                BucketRanges =
                [
                    new CSharpDbShardBucketRange
                    {
                        StartBucketInclusive = 0,
                        EndBucketExclusive = 1,
                        ShardId = "disabled",
                    },
                ],
            };

            await using var client = await CSharpDbShardedClient.CreateAsync(options, ct: Ct);
            Assert.Throws<CSharpDbClientException>(() => client.ResolveRoute(new CSharpDbRouteContext
            {
                Keyspace = "tenants",
                Key = "any",
            }));
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task CreateAsync_RejectsOverlappingBucketRanges()
    {
        string directory = CreateTempDirectory();
        try
        {
            var options = new CSharpDbShardingOptions
            {
                Keyspace = "tenants",
                VirtualBucketCount = 4,
                Shards =
                [
                    new CSharpDbShardDefinition { ShardId = "s0", DataSource = Path.Combine(directory, "s0.db") },
                    new CSharpDbShardDefinition { ShardId = "s1", DataSource = Path.Combine(directory, "s1.db") },
                ],
                BucketRanges =
                [
                    new CSharpDbShardBucketRange { StartBucketInclusive = 0, EndBucketExclusive = 3, ShardId = "s0" },
                    new CSharpDbShardBucketRange { StartBucketInclusive = 2, EndBucketExclusive = 4, ShardId = "s1" },
                ],
            };

            await Assert.ThrowsAsync<CSharpDbClientConfigurationException>(
                () => CSharpDbShardedClient.CreateAsync(options, ct: Ct));
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    private static CSharpDbShardingOptions CreateOptions(string directory)
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

    private static string CreateTempDirectory()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"csharpdb_shards_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        return directory;
    }

    private static void DeleteDirectory(string directory)
    {
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
