using CSharpDB.Admin.Configuration;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Tests.Admin;

public sealed class DatabaseClientHolderShardingTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task CreateShardCatalogAndReloadAsync_SeedsMasterDbAndEnablesShardAdmin()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"csharpdb-admin-sharding-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);

        DatabaseClientHolder? holder = null;
        try
        {
            string masterPath = Path.Combine(directory, "master.db");
            var options = new CSharpDbClientOptions
            {
                Transport = CSharpDbTransport.Direct,
                DataSource = masterPath,
            };

            ICSharpDbClient client = CSharpDbClient.Create(options);
            holder = new DatabaseClientHolder(
                client,
                shardAdmin: null,
                baseClientOptions: options,
                hostDatabaseOptions: new AdminHostDatabaseOptions { OpenMode = AdminHostOpenMode.Direct },
                functions: DbFunctionRegistry.Create(_ => { }));

            CSharpDbShardingOptions activeMap = CreateShardingOptions(directory);

            Assert.True(holder.SupportsMasterCatalogBootstrap);
            Assert.False(holder.SupportsShardAdmin);

            await holder.CreateShardCatalogAndReloadAsync(activeMap, Ct);

            Assert.True(holder.SupportsShardAdmin);
            Assert.False(holder.SupportsMasterCatalogBootstrap);

            CSharpDbShardMapSnapshot snapshot = await holder.GetShardMapAsync(Ct);
            Assert.Equal("tenants", snapshot.Keyspace);
            Assert.Equal(["s0", "s1"], snapshot.Shards.Select(item => item.ShardId).ToArray());
        }
        finally
        {
            if (holder is not null)
                await holder.DisposeAsync();

            try
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, recursive: true);
            }
            catch (IOException)
            {
                // Ignore transient cleanup locks from the direct engine.
            }
        }
    }

    private static CSharpDbShardingOptions CreateShardingOptions(string directory)
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
}
