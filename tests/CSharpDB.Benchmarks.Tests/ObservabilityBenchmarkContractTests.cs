using System.Diagnostics;
using System.Reflection;
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
    public void TelemetryModesUseValidatedFullSamplingOptions(
        ObservabilityBenchmarkMode mode)
    {
        CSharpDbObservabilityOptions options = Assert.IsType<CSharpDbObservabilityOptions>(
            ObservabilityBenchmarkConfiguration.CreateOptions(mode));

        Assert.True(options.Enabled);
        Assert.Equal("benchmark", options.DatabaseAlias);
        Assert.False(options.Logging.Enabled);
        Assert.True(options.OpenTelemetry.Enabled);
        Assert.Equal(1d, options.OpenTelemetry.SamplingRatio);
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
}
