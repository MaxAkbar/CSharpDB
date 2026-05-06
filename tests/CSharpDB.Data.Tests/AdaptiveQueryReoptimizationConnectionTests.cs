using CSharpDB.Engine;

namespace CSharpDB.Data.Tests;

public sealed class AdaptiveQueryReoptimizationConnectionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void ConnectionStringBuilder_ParsesAdaptiveQueryReoptimization()
    {
        var builder = new CSharpDbConnectionStringBuilder(
            "Data Source=bench.db;Adaptive Query Reoptimization=true");

        Assert.Equal("bench.db", builder.DataSource);
        Assert.True(builder.AdaptiveQueryReoptimization);
    }

    [Fact]
    public void EmbeddedResolver_AppliesAdaptiveQueryReoptimizationWhenNoDirectOptionsAreSupplied()
    {
        var builder = new CSharpDbConnectionStringBuilder(
            "Data Source=bench.db;Adaptive Query Reoptimization=true");

        ResolvedEmbeddedConfiguration configuration =
            CSharpDbEmbeddedConfigurationResolver.Resolve(builder, null, null);

        Assert.True(configuration.HasRequestedTuning);
        Assert.True(configuration.EffectiveAdaptiveQueryReoptimization);
        Assert.True(configuration.EffectiveDirectDatabaseOptions.AdaptiveQueryReoptimization.Enabled);
    }

    [Fact]
    public void EmbeddedResolver_ExplicitDirectOptionsTakePrecedence()
    {
        var builder = new CSharpDbConnectionStringBuilder(
            "Data Source=bench.db;Adaptive Query Reoptimization=true");
        var explicitOptions = new DatabaseOptions();

        ResolvedEmbeddedConfiguration configuration =
            CSharpDbEmbeddedConfigurationResolver.Resolve(builder, explicitOptions, null);

        Assert.Same(explicitOptions, configuration.EffectiveDirectDatabaseOptions);
        Assert.False(configuration.EffectiveAdaptiveQueryReoptimization);
        Assert.False(configuration.EffectiveDirectDatabaseOptions.AdaptiveQueryReoptimization.Enabled);
    }

    [Fact]
    public async Task OpenAsync_RejectsAdaptiveQueryReoptimizationForRemoteConnections()
    {
        await using var connection = new CSharpDbConnection(
            "Endpoint=http://localhost:5820;Transport=Grpc;Adaptive Query Reoptimization=true");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => connection.OpenAsync(Ct));

        Assert.Contains("remote host", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
