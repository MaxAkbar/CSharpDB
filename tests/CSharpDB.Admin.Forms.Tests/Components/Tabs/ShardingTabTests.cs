using System.Reflection;
using CSharpDB.Admin.Components.Tabs;
using CSharpDB.Client;

namespace CSharpDB.Admin.Forms.Tests.Components.Tabs;

public sealed class ShardingTabTests
{
    [Theory]
    [InlineData("SELECT * FROM sys.tables;", true)]
    [InlineData("  -- inspect\nWITH recent AS (SELECT 1 AS id) SELECT id FROM recent;", true)]
    [InlineData("EXPLAIN ESTIMATE SELECT * FROM orders;", true)]
    [InlineData("UPDATE orders SET total = 0;", false)]
    [InlineData("DROP TABLE orders;", false)]
    [InlineData("", false)]
    public void IsLikelyReadOnlyFanOutSql_ClassifiesObviousStatements(string sql, bool expected)
    {
        bool actual = InvokeStatic<bool>("IsLikelyReadOnlyFanOutSql", sql);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void FormatCellValue_UsesStableOperatorText()
    {
        Assert.Equal("NULL", InvokeStatic<string>("FormatCellValue", [null]));
        Assert.Equal("[3 bytes]", InvokeStatic<string>("FormatCellValue", new byte[] { 1, 2, 3 }));
        Assert.Equal("42", InvokeStatic<string>("FormatCellValue", 42));
    }

    [Fact]
    public void CreateBootstrapDraftOptions_CreatesValidMapNextToMasterDb()
    {
        string directory = Path.Combine(Path.GetTempPath(), "csharpdb-admin-sharding-test");
        string masterPath = Path.Combine(directory, "master.db");

        CSharpDbShardingOptions options = InvokeStatic<CSharpDbShardingOptions>(
            "CreateBootstrapDraftOptions",
            masterPath,
            " tenants ",
            8,
            3);

        CSharpDbShardMapSnapshot snapshot = CSharpDbShardedClient.CreateShardMapSnapshot(options);

        Assert.Equal("tenants", snapshot.Keyspace);
        Assert.Equal(8, snapshot.VirtualBucketCount);
        Assert.Equal(["shard-0", "shard-1", "shard-2"], snapshot.Shards.Select(item => item.ShardId).ToArray());
        Assert.Equal(Path.Combine(directory, "shard-0.db"), options.Shards[0].DataSource);
        Assert.Collection(
            snapshot.BucketRanges,
            range =>
            {
                Assert.Equal(0, range.StartBucketInclusive);
                Assert.Equal(2, range.EndBucketExclusive);
                Assert.Equal("shard-0", range.ShardId);
            },
            range =>
            {
                Assert.Equal(2, range.StartBucketInclusive);
                Assert.Equal(5, range.EndBucketExclusive);
                Assert.Equal("shard-1", range.ShardId);
            },
            range =>
            {
                Assert.Equal(5, range.StartBucketInclusive);
                Assert.Equal(8, range.EndBucketExclusive);
                Assert.Equal("shard-2", range.ShardId);
            });
    }

    private static T InvokeStatic<T>(string methodName, params object?[] args)
    {
        MethodInfo method = typeof(ShardingTab).GetMethod(methodName, BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"{methodName} was not found.");
        return (T)method.Invoke(null, args)!;
    }
}
