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
    public async Task CatalogApply_WritesPendingMapAndReloadUsesCatalogVersion()
    {
        string directory = CreateTempDirectory();
        try
        {
            string catalogPath = Path.Combine(directory, "shard-catalog.json");
            CSharpDbShardingOptions options = CreateOptions(directory);
            options.Catalog = new CSharpDbShardCatalogOptions
            {
                Enabled = true,
                Path = catalogPath,
            };

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
                Enabled = true,
                Path = Path.Combine(directory, "catalog.json"),
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
            string catalogPath = Path.Combine(directory, "catalog.json");
            CSharpDbShardingOptions options = CreateOptions(directory);
            options.Catalog = new CSharpDbShardCatalogOptions
            {
                Enabled = true,
                Path = catalogPath,
            };

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
                Enabled = true,
                Path = Path.Combine(directory, "catalog.json"),
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
                Enabled = true,
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
                Enabled = true,
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
            Enabled = true,
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
