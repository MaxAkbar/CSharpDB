using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Micro;
using CSharpDB.Observability;

namespace CSharpDB.Benchmarks.Tests;

public sealed class ObservabilityBenchmarkContractTests
{
    private static readonly ObservabilityBenchmarkMode[] ExpectedModes =
    [
        ObservabilityBenchmarkMode.Disabled,
        ObservabilityBenchmarkMode.HistoryCapture,
        ObservabilityBenchmarkMode.StructuredLogging,
        ObservabilityBenchmarkMode.MetricsOnly,
        ObservabilityBenchmarkMode.SampledTracing,
    ];

    [Fact]
    public void ExistingModeValuesRemainStableAndTelemetryModesAppend()
    {
        Assert.Equal(0, (int)ObservabilityBenchmarkMode.Disabled);
        Assert.Equal(1, (int)ObservabilityBenchmarkMode.StructuredLogging);
        Assert.Equal(2, (int)ObservabilityBenchmarkMode.HistoryCapture);
        Assert.Equal(3, (int)ObservabilityBenchmarkMode.MetricsOnly);
        Assert.Equal(4, (int)ObservabilityBenchmarkMode.SampledTracing);
    }

    [Fact]
    public void EngineAndPoolRowsCoverEveryModeInTheSameOrder()
    {
        AssertModeParameters<ObservabilityNoListenerEngineBenchmarks>();
        AssertModeParameters<ObservabilityNoListenerConnectionPoolBenchmarks>();
    }

    [Fact]
    public void ExistingModesPreserveTheirConfigurationContract()
    {
        Assert.Null(ObservabilityBenchmarkConfiguration.CreateOptions(
            ObservabilityBenchmarkMode.Disabled));

        CSharpDbObservabilityOptions history = Assert.IsType<CSharpDbObservabilityOptions>(
            ObservabilityBenchmarkConfiguration.CreateOptions(
                ObservabilityBenchmarkMode.HistoryCapture));
        Assert.True(history.Enabled);
        Assert.False(history.Logging.Enabled);
        Assert.False(history.OpenTelemetry.Enabled);

        CSharpDbObservabilityOptions logging = Assert.IsType<CSharpDbObservabilityOptions>(
            ObservabilityBenchmarkConfiguration.CreateOptions(
                ObservabilityBenchmarkMode.StructuredLogging));
        Assert.True(logging.Enabled);
        Assert.True(logging.Logging.Enabled);
        Assert.True(logging.Logging.Queries);
        Assert.False(logging.OpenTelemetry.Enabled);
    }

    [Theory]
    [InlineData(ObservabilityBenchmarkMode.MetricsOnly)]
    [InlineData(ObservabilityBenchmarkMode.SampledTracing)]
    public void TelemetryModesUseValidatedFullSamplingOptionsWithoutHistory(
        ObservabilityBenchmarkMode mode)
    {
        CSharpDbObservabilityOptions options = Assert.IsType<CSharpDbObservabilityOptions>(
            ObservabilityBenchmarkConfiguration.CreateOptions(mode));

        Assert.True(options.Enabled);
        Assert.Equal("benchmark", options.DatabaseAlias);
        Assert.False(options.Logging.Enabled);
        Assert.True(options.OpenTelemetry.Enabled);
        Assert.Equal(1d, options.OpenTelemetry.SamplingRatio);
        Assert.False(options.History.Enabled);
        Assert.Empty(options.GetValidationErrors());
    }

    [Fact]
    public void MetricsModeOwnsOnlyAMeterListenerAndDisposesExactlyOnce()
    {
        bool activityListenerWasPresent = CSharpDbDiagnostics.ActivitySource.HasListeners();
        ObservabilityBenchmarkListenerSet listeners = Assert.IsType<
            ObservabilityBenchmarkListenerSet>(ObservabilityBenchmarkListenerSet.Start(
                ObservabilityBenchmarkMode.MetricsOnly));
        try
        {
            Assert.True(listeners.HasMetricsListener);
            Assert.False(listeners.HasTracingListener);
            Assert.Equal(
                activityListenerWasPresent,
                CSharpDbDiagnostics.ActivitySource.HasListeners());
        }
        finally
        {
            listeners.Dispose();
        }

        listeners.Dispose();

        Assert.True(listeners.IsDisposed);
        Assert.False(listeners.HasMetricsListener);
        Assert.False(listeners.HasTracingListener);
    }

    [Fact]
    public void SampledTracingModeRecordsCSharpDbActivitiesAndRestoresListenerState()
    {
        bool activityListenerWasPresent = CSharpDbDiagnostics.ActivitySource.HasListeners();
        ObservabilityBenchmarkListenerSet listeners =
            Assert.IsType<ObservabilityBenchmarkListenerSet>(
                ObservabilityBenchmarkListenerSet.Start(
                    ObservabilityBenchmarkMode.SampledTracing));
        try
        {
            Assert.False(listeners.HasMetricsListener);
            Assert.True(listeners.HasTracingListener);

            using Activity? activity = CSharpDbDiagnostics.ActivitySource.StartActivity(
                "csharpdb.query",
                ActivityKind.Internal);

            Assert.NotNull(activity);
            Assert.True(activity.Recorded);
        }
        finally
        {
            listeners.Dispose();
        }

        Assert.True(listeners.IsDisposed);
        Assert.Equal(
            activityListenerWasPresent,
            CSharpDbDiagnostics.ActivitySource.HasListeners());
    }

    [Fact]
    public void FormalPerformancePolicyMatchesTheBenchmarkAndDocumentedCeilings()
    {
        string repoRoot = FindRepositoryRoot();
        string policyPath = Path.Combine(
            repoRoot,
            "tests",
            "CSharpDB.Benchmarks",
            "observability-perf-thresholds.json");
        using JsonDocument document = JsonDocument.Parse(File.ReadAllText(policyPath));
        JsonElement root = document.RootElement;

        Assert.Equal(1, root.GetProperty("schemaVersion").GetInt32());
        JsonElement reference = root.GetProperty("reference");
        Assert.Equal("approved", reference.GetProperty("status").GetString());
        Assert.Equal(
            "4f9457fb829d746b6cf6c767b1a32513262ab222",
            reference.GetProperty("commit").GetString());
        JsonElement qualification = root.GetProperty("qualification");
        Assert.Equal(3, qualification.GetProperty("requiredPairCount").GetInt32());
        Assert.Equal(
            5d,
            qualification.GetProperty("maximumLaunchSpreadPercent").GetDouble());
        Assert.Equal(3, qualification.GetProperty("warmupCount").GetInt32());
        Assert.Equal(10, qualification.GetProperty("iterationCount").GetInt32());
        Assert.Equal(1, qualification.GetProperty("benchmarkLaunchCount").GetInt32());
        Assert.Equal(
            ExpectedModes.Select(static mode => mode.ToString()),
            qualification
                .GetProperty("candidateModeOrder")
                .EnumerateArray()
                .Select(static value => value.GetString()));

        JsonElement modes = root.GetProperty("modes");
        JsonElement disabled = modes.GetProperty("Disabled");
        Assert.Equal("approved", disabled.GetProperty("status").GetString());
        Assert.Equal(
            "detachedReference",
            disabled.GetProperty("comparison").GetString());
        Assert.Equal(3d, disabled.GetProperty("maxElapsedPercent").GetDouble());
        Assert.Equal(
            0d,
            disabled.GetProperty("maxAdditionalAllocatedBytes").GetDouble());

        JsonElement history = modes.GetProperty("HistoryCapture");
        Assert.Equal("approved", history.GetProperty("status").GetString());
        Assert.Equal(
            "maximumOfRelativeAndFixed",
            history.GetProperty("elapsedAllowance").GetString());
        Assert.Equal(20d, history.GetProperty("maxElapsedPercent").GetDouble());
        Assert.Equal(
            1500d,
            history.GetProperty("maxElapsedNanoseconds").GetDouble());
        Assert.Equal(
            1024d,
            history.GetProperty("maxAdditionalAllocatedBytes").GetDouble());

        JsonElement metrics = modes.GetProperty("MetricsOnly");
        Assert.Equal("approved", metrics.GetProperty("status").GetString());
        Assert.Equal(
            "resolved",
            metrics.GetProperty("configurationStatus").GetString());
        Assert.Equal(
            new[] { "metricsRuntime", "metricsListener" },
            metrics
                .GetProperty("measurementComposition")
                .EnumerateArray()
                .Select(static value => value.GetString()));
        Assert.Equal(10d, metrics.GetProperty("maxElapsedPercent").GetDouble());
        Assert.Equal(
            64d,
            metrics.GetProperty("maxAdditionalAllocatedBytes").GetDouble());

        JsonElement tracing = modes.GetProperty("SampledTracing");
        Assert.Equal(
            "decisionRequired",
            tracing.GetProperty("status").GetString());
        Assert.Equal(
            "resolved",
            tracing.GetProperty("configurationStatus").GetString());
        Assert.Equal(
            new[] { "metricsRuntime", "sampledTracingListener" },
            tracing
                .GetProperty("measurementComposition")
                .EnumerateArray()
                .Select(static value => value.GetString()));
        Assert.False(tracing.TryGetProperty("maxElapsedPercent", out _));
        Assert.False(tracing.TryGetProperty("maxAdditionalAllocatedBytes", out _));

        JsonElement logging = modes.GetProperty("StructuredLogging");
        Assert.Equal(
            "characterization",
            logging.GetProperty("status").GetString());

        JsonElement[] paths = root.GetProperty("paths").EnumerateArray().ToArray();
        Assert.Equal(7, paths.Length);
        Assert.Equal(
            6,
            paths.Count(static path =>
                path.GetProperty("suite").GetString() == "engine"));
        JsonElement pool = Assert.Single(
            paths,
            static path => path.GetProperty("suite").GetString() == "pool");
        Assert.Equal(0, pool.GetProperty("logicalQueriesPerOperation").GetInt32());

        string[] benchmarkDescriptions =
        [
            .. GetBenchmarkDescriptions<ObservabilityNoListenerEngineBenchmarks>(),
            .. GetBenchmarkDescriptions<ObservabilityNoListenerConnectionPoolBenchmarks>(),
        ];
        Assert.Equal(
            benchmarkDescriptions.Order(StringComparer.Ordinal),
            paths
                .Select(static path => path.GetProperty("method").GetString()!)
                .Order(StringComparer.Ordinal));
        Assert.All(paths, static path => Assert.StartsWith(
            "No listeners: ",
            path.GetProperty("referenceMethod").GetString(),
            StringComparison.Ordinal));
    }

    private static void AssertModeParameters<TBenchmark>()
    {
        PropertyInfo? modeProperty = typeof(TBenchmark).GetProperty(
            nameof(ObservabilityNoListenerEngineBenchmarks.Mode));
        Assert.NotNull(modeProperty);
        ParamsAttribute attribute = Assert.Single(
            modeProperty.GetCustomAttributes<ParamsAttribute>());

        Assert.Equal(
            ExpectedModes,
            attribute.Values.Cast<ObservabilityBenchmarkMode>());
    }

    private static IEnumerable<string> GetBenchmarkDescriptions<TBenchmark>()
        => typeof(TBenchmark)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Select(static method => method.GetCustomAttribute<BenchmarkAttribute>())
            .Where(static attribute => attribute is not null)
            .Select(static attribute => attribute!.Description ??
                throw new InvalidOperationException(
                    "Every observability benchmark must have a description."));

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "CSharpDB.slnx")))
                return directory.FullName;
            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate the repository root.");
    }
}
