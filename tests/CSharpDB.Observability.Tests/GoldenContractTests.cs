using System.Diagnostics.Metrics;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using CSharpDB.Observability;

namespace CSharpDB.Observability.Tests;

[Collection(MetricsContractCollection.Name)]
public sealed class GoldenContractTests
{
    [Fact]
    public void MetricSchema_MatchesFrozenNamesKindsUnitsSemanticsAndLabels()
    {
        var instruments = new Dictionary<string, Instrument>(StringComparer.Ordinal);
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, _) =>
        {
            if (instrument.Meter.Name == CSharpDbDiagnostics.MeterName)
                instruments[instrument.Name] = instrument;
        };
        listener.Start();

        var options = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "schema-golden",
            Logging = new CSharpDbLoggingOptions { Enabled = false },
        };
        options.OpenTelemetry.Enabled = true;
        using var state = new CSharpDbRuntimeDiagnosticsState(options);

        string[] publishedNames = typeof(CSharpDbMetricInstrumentNames)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(static field => field.IsLiteral && field.FieldType == typeof(string))
            .Select(static field => Assert.IsType<string>(field.GetRawConstantValue()))
            .OrderBy(static name => name, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(publishedNames, instruments.Keys.OrderBy(
            static name => name,
            StringComparer.Ordinal));

        var lines = new List<string>
        {
            $"diagnostics-schema|{CSharpDbDiagnostics.SchemaVersion}",
            $"metric-schema|{CSharpDbDiagnostics.MetricSchemaVersion}",
            $"instrumentation|{CSharpDbDiagnostics.MeterName}|{CSharpDbDiagnostics.InstrumentationVersion}",
        };
        lines.AddRange(CSharpDbMetricTagNames.Allowed
            .OrderBy(static name => name, StringComparer.Ordinal)
            .Select(static name => $"tag|{name}"));
        lines.AddRange(publishedNames.Select(name =>
        {
            Instrument instrument = instruments[name];
            string semantics = CSharpDbCounterSemantics.Instruments.TryGetValue(
                name,
                out CounterSemantics counterSemantics)
                    ? counterSemantics.ToString().ToLowerInvariant()
                    : "histogram";
            return $"metric|{name}|{InstrumentKind(instrument)}|{instrument.Unit}|{semantics}";
        }));

        AssertGolden("metric-schema.golden.txt", string.Join('\n', lines));
    }

    [Fact]
    public void DefaultRedaction_MatchesFrozenSafeQueryFailurePayload()
    {
        const string secret = "GoldenBearerSecret";
        const string path = "C:\\private\\GoldenCustomer.db";
        var exception = new InvalidOperationException(
            $"SELECT '{secret}' FROM {path} WHERE password = 'do-not-emit'");
        var timeProvider = new GoldenTimeProvider(
            new DateTimeOffset(2026, 8, 12, 12, 0, 0, TimeSpan.Zero),
            timestamp: 123);
        CSharpDbOperationContext context = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Direct,
            "golden-primary",
            timeProvider: timeProvider);
        var failed = new CSharpDbQueryFailedEvent(
            context,
            new DateTimeOffset(2026, 8, 12, 12, 0, 2, TimeSpan.Zero),
            totalDuration: TimeSpan.FromSeconds(2),
            timeToFirstResult: null,
            queueDuration: TimeSpan.FromMilliseconds(5),
            executionAndConsumptionDuration: TimeSpan.FromMilliseconds(10),
            rowsProduced: 0,
            rowsAffected: 0,
            SafeErrorProjector.Project(exception));

        string serialized = JsonSerializer.Serialize(
            failed,
            CSharpDbObservabilityJsonContext.Default.CSharpDbQueryFailedEvent);
        Assert.DoesNotContain(secret, serialized, StringComparison.Ordinal);
        Assert.DoesNotContain(path, serialized, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("password", serialized, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            nameof(InvalidOperationException),
            serialized,
            StringComparison.Ordinal);

        JsonObject root = Assert.IsType<JsonObject>(JsonNode.Parse(serialized));
        JsonObject serializedContext = Assert.IsType<JsonObject>(root["context"]);
        serializedContext["operationId"] = "<opaque-operation-id>";
        string normalized = root.ToJsonString(new JsonSerializerOptions
        {
            WriteIndented = true,
        });

        AssertGolden("default-redaction.golden.json", normalized);
    }

    private static string InstrumentKind(Instrument instrument)
        => instrument switch
        {
            Counter<long> => "counter",
            Histogram<double> or Histogram<long> => "histogram",
            ObservableCounter<long> => "observable_counter",
            ObservableGauge<long> or ObservableGauge<double> => "observable_gauge",
            ObservableUpDownCounter<long> => "observable_up_down_counter",
            _ => throw new InvalidOperationException(
                $"Unrecognized metric instrument type '{instrument.GetType()}'."),
        };

    private static void AssertGolden(string fileName, string actual)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "Golden", fileName);
        string expected = File.ReadAllText(path);
        Assert.Equal(Normalize(expected), Normalize(actual));
    }

    private static string Normalize(string value)
        => value.Replace("\r\n", "\n", StringComparison.Ordinal).TrimEnd();

    private sealed class GoldenTimeProvider(
        DateTimeOffset utcNow,
        long timestamp) : TimeProvider
    {
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override DateTimeOffset GetUtcNow() => utcNow;
        public override long GetTimestamp() => timestamp;
    }
}
