using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
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
