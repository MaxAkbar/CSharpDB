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
        Assert.False(options.OpenTelemetry.Console.Enabled);
        Assert.Equal("CSharpDB", options.OpenTelemetry.Resource.ServiceNamespace);
        Assert.Null(options.OpenTelemetry.Resource.ServiceName);
        Assert.False(options.Prometheus.Enabled);
        Assert.False(options.Prometheus.AllowInsecureRemoteAccess);
        Assert.True(options.Health.Enabled);
        Assert.True(options.History.Enabled);
    }

    [Fact]
    public void Exporters_RequireTheOwningTelemetrySwitches()
    {
        var options = new CSharpDbObservabilityOptions
        {
            Enabled = false,
            OpenTelemetry = new CSharpDbOpenTelemetryOptions
            {
                Enabled = false,
                Otlp = new CSharpDbOtlpOptions { Enabled = true },
                Console = new CSharpDbConsoleExporterOptions { Enabled = true },
            },
            Prometheus = new CSharpDbPrometheusOptions
            {
                Enabled = false,
                AllowInsecureRemoteAccess = true,
            },
        };

        IReadOnlyList<string> errors = options.GetValidationErrors();

        Assert.Contains(errors, error => error.Contains("Otlp", StringComparison.Ordinal));
        Assert.Contains(errors, error => error.Contains("Console", StringComparison.Ordinal));
        Assert.Contains(
            errors,
            error => error.Contains("AllowInsecureRemoteAccess", StringComparison.Ordinal));

        options.OpenTelemetry.Enabled = true;
        options.Prometheus.Enabled = true;
        errors = options.GetValidationErrors();
        Assert.Contains(
            errors,
            error => error.Contains(
                "OpenTelemetry cannot be enabled",
                StringComparison.Ordinal));
        Assert.Contains(
            errors,
            error => error.Contains(
                "Prometheus cannot be enabled",
                StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("/metrics/")]
    [InlineData("/metrics/{name}")]
    [InlineData("/metrics/../private")]
    [InlineData("/metrics%2Fprivate")]
    public void PrometheusPath_MustBeCanonicalAndLiteral(string path)
    {
        var options = new CSharpDbObservabilityOptions();
        options.Prometheus.Path = path;

        Assert.Contains(
            options.GetValidationErrors(),
            error => error.Contains("Prometheus.Path", StringComparison.Ordinal));
    }

    [Fact]
    public void ResourceOptions_AreBoundedPathFreeAndSourceGeneratedSerializable()
    {
        var options = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            OpenTelemetry = new CSharpDbOpenTelemetryOptions
            {
                Enabled = true,
                SamplingRatio = 0.25,
                Resource = new CSharpDbOpenTelemetryResourceOptions
                {
                    ServiceName = "orders-db",
                    ServiceNamespace = "sample",
                    ServiceVersion = "1.2.3+build",
                    ServiceInstanceId = "instance-1",
                    DeploymentEnvironment = "staging",
                },
                Console = new CSharpDbConsoleExporterOptions { Enabled = true },
            },
        };

        options.Validate();
        string json = System.Text.Json.JsonSerializer.Serialize(
            options,
            CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions);
        CSharpDbObservabilityOptions roundTrip = Assert.IsType<
            CSharpDbObservabilityOptions>(
            System.Text.Json.JsonSerializer.Deserialize(
                json,
                CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions));

        Assert.Equal("orders-db", roundTrip.OpenTelemetry.Resource.ServiceName);
        Assert.Equal(0.25, roundTrip.OpenTelemetry.SamplingRatio);
        Assert.True(roundTrip.OpenTelemetry.Console.Enabled);
        Assert.True(roundTrip.History.Enabled);

        options.OpenTelemetry.Resource.ServiceInstanceId = @"C:\private\instance";
        Assert.Contains(
            options.GetValidationErrors(),
            error => error.Contains("ServiceInstanceId", StringComparison.Ordinal));
    }

    [Fact]
    public void HistoryEnabled_RoundTripsIndependentlyFromTelemetrySignals()
    {
        var options = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            History = new CSharpDbHistoryOptions { Enabled = false },
            OpenTelemetry = new CSharpDbOpenTelemetryOptions { Enabled = true },
        };

        options.Validate();
        string json = System.Text.Json.JsonSerializer.Serialize(
            options,
            CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions);
        CSharpDbObservabilityOptions roundTrip = Assert.IsType<
            CSharpDbObservabilityOptions>(
            System.Text.Json.JsonSerializer.Deserialize(
                json,
                CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions));

        Assert.True(roundTrip.Enabled);
        Assert.False(roundTrip.History.Enabled);
        Assert.True(roundTrip.OpenTelemetry.Enabled);
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

    [Fact]
    public void SlowQueryThresholdOverrides_AreBoundedValidatedAndSerializable()
    {
        var options = new CSharpDbObservabilityOptions();
        options.Logging.SlowQueryThreshold = TimeSpan.FromMilliseconds(500);
        options.Logging.SlowQueryThresholdOverrides[CSharpDbOperationClass.Query] =
            TimeSpan.FromMilliseconds(125);
        options.Logging.SlowQueryThresholdOverrides[CSharpDbOperationClass.Procedure] =
            TimeSpan.FromSeconds(2);
        options.Logging.SlowQueryThresholdOverrides[CSharpDbOperationClass.Pipeline] =
            TimeSpan.FromSeconds(3);

        options.Validate();

        Assert.Equal(
            TimeSpan.FromMilliseconds(125),
            options.Logging.GetSlowQueryThreshold(CSharpDbOperationClass.Query));
        Assert.Equal(
            TimeSpan.FromSeconds(2),
            options.Logging.GetSlowQueryThreshold(CSharpDbOperationClass.Procedure));
        Assert.Equal(
            TimeSpan.FromMilliseconds(500),
            options.Logging.GetSlowQueryThreshold(CSharpDbOperationClass.Script));
        Assert.Equal(
            TimeSpan.FromSeconds(3),
            options.Logging.GetSlowQueryThreshold(CSharpDbOperationClass.Pipeline));

        string json = System.Text.Json.JsonSerializer.Serialize(
            options,
            CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions);
        CSharpDbObservabilityOptions roundTrip = Assert.IsType<CSharpDbObservabilityOptions>(
            System.Text.Json.JsonSerializer.Deserialize(
                json,
                CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions));
        Assert.Equal(
            TimeSpan.FromMilliseconds(125),
            roundTrip.Logging.GetSlowQueryThreshold(CSharpDbOperationClass.Query));
        Assert.Equal(
            TimeSpan.FromSeconds(3),
            roundTrip.Logging.GetSlowQueryThreshold(CSharpDbOperationClass.Pipeline));
    }

    [Fact]
    public void InvalidSlowQueryThresholdOverrides_ReportConfigurationErrors()
    {
        var options = new CSharpDbObservabilityOptions();
        options.Logging.SlowQueryThresholdOverrides[CSharpDbOperationClass.Unknown] =
            TimeSpan.FromMilliseconds(1);
        options.Logging.SlowQueryThresholdOverrides[(CSharpDbOperationClass)999] =
            TimeSpan.FromMilliseconds(1);
        options.Logging.SlowQueryThresholdOverrides[CSharpDbOperationClass.Query] =
            TimeSpan.Zero;

        IReadOnlyList<string> errors = options.GetValidationErrors();

        Assert.Contains(errors, error => error.Contains("keys", StringComparison.Ordinal));
        Assert.Contains(errors, error => error.Contains("[Query]", StringComparison.Ordinal));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            options.Logging.GetSlowQueryThreshold(CSharpDbOperationClass.Unknown));
    }
}
