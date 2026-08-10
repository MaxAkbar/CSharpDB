using CSharpDB.Observability;

namespace CSharpDB.Observability.Tests;

public sealed class ObservabilityOptionsTests
{
    [Fact]
    public void Defaults_AreSafeAndValid()
    {
        var options = new CSharpDbObservabilityOptions();

        options.Validate();

        Assert.False(options.Enabled);
        Assert.False(options.Logging.Queries);
        Assert.Equal(SqlTextCaptureMode.None, options.Logging.SqlText);
        Assert.False(options.OpenTelemetry.Enabled);
        Assert.False(options.OpenTelemetry.Otlp.Enabled);
        Assert.False(options.Prometheus.Enabled);
        Assert.False(options.Prometheus.AllowInsecureRemoteAccess);
        Assert.True(options.Health.Enabled);
    }

    [Fact]
    public void InvalidUnsafeOrUnboundedConfiguration_ReportsEveryProblem()
    {
        var options = new CSharpDbObservabilityOptions
        {
            DatabaseAlias = "C:\\secret\\database.db",
            LongRunningQueryThreshold = TimeSpan.Zero,
            Logging = new CSharpDbLoggingOptions
            {
                SlowQueryThreshold = TimeSpan.FromDays(2),
                SqlText = (SqlTextCaptureMode)999,
            },
            History = new CSharpDbHistoryOptions
            {
                ActiveQueryCapacity = 0,
                RecentQueryCapacity = int.MaxValue,
                RecentOperationCapacity = -1,
                Retention = TimeSpan.FromDays(30),
            },
            OpenTelemetry = new CSharpDbOpenTelemetryOptions
            {
                Enabled = false,
                SamplingRatio = double.NaN,
                Otlp = new CSharpDbOtlpOptions { Enabled = true },
            },
            Prometheus = new CSharpDbPrometheusOptions
            {
                Enabled = true,
                Path = "/same",
            },
            Health = new CSharpDbHealthOptions
            {
                LivenessPath = "/same",
                ReadinessPath = "relative?detail=secret",
                ReadinessTimeout = TimeSpan.Zero,
            },
        };

        CSharpDbObservabilityOptionsValidationException exception =
            Assert.Throws<CSharpDbObservabilityOptionsValidationException>(options.Validate);

        Assert.True(exception.Errors.Count >= 10);
        Assert.Contains(exception.Errors, error => error.Contains("DatabaseAlias", StringComparison.Ordinal));
        Assert.Contains(exception.Errors, error => error.Contains("ActiveQueryCapacity", StringComparison.Ordinal));
        Assert.Contains(exception.Errors, error => error.Contains("SamplingRatio", StringComparison.Ordinal));
        Assert.Contains(exception.Errors, error => error.Contains("Otlp", StringComparison.Ordinal));
        Assert.Contains(exception.Errors, error => error.Contains("Logging.SqlText", StringComparison.Ordinal));
        Assert.Contains(exception.Errors, error => error.Contains("endpoint paths", StringComparison.Ordinal));
        Assert.DoesNotContain("secret\\database.db", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("primary")]
    [InlineData("tenant-01")]
    [InlineData("shard.us_west.2")]
    public void SafeDatabaseAliases_AreAccepted(string alias)
        => Assert.True(CSharpDbObservabilityOptions.IsValidDatabaseAlias(alias));

    [Theory]
    [InlineData("")]
    [InlineData("with space")]
    [InlineData("../database")]
    [InlineData("C:\\database.db")]
    [InlineData("tenant:secret")]
    public void PathLikeOrHighRiskDatabaseAliases_AreRejected(string alias)
        => Assert.False(CSharpDbObservabilityOptions.IsValidDatabaseAlias(alias));

    [Fact]
    public void ConfiguredAliasSet_IsBoundedAndUnique()
    {
        CSharpDbObservabilityOptions.ValidateDatabaseAliases(["primary", "shard-1"]);

        Assert.Throws<ArgumentException>(
            () => CSharpDbObservabilityOptions.ValidateDatabaseAliases(["primary", "primary"]));
        Assert.Throws<ArgumentException>(
            () => CSharpDbObservabilityOptions.ValidateDatabaseAliases(
                Enumerable.Range(0, CSharpDbDiagnostics.MaximumConfiguredDatabaseAliases + 1)
                    .Select(index => $"shard-{index}")));
    }
}
