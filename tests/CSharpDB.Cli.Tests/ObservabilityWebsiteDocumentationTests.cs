using System.Reflection;
using CSharpDB.Observability;

namespace CSharpDB.Cli.Tests;

public sealed class ObservabilityWebsiteDocumentationTests
{
    [Fact]
    public void PublicGuide_CoversCanonicalSchemaAndSafetyBoundaries()
    {
        string repoRoot = FindRepoRoot();
        string guide = File.ReadAllText(
            Path.Combine(repoRoot, "www", "docs", "observability.html"));

        Assert.Contains(
            $"Runtime snapshot schema: <code>{CSharpDbDiagnostics.SchemaVersion}</code>",
            guide,
            StringComparison.Ordinal);
        Assert.Contains(
            $"Metric schema: <code>{CSharpDbDiagnostics.MetricSchemaVersion}</code>",
            guide,
            StringComparison.Ordinal);
        Assert.Contains(
            $"instrumentation version <code>{CSharpDbDiagnostics.InstrumentationVersion}</code>",
            guide,
            StringComparison.Ordinal);

        foreach (string metricName in PublicStringConstants(
                     typeof(CSharpDbMetricInstrumentNames)))
        {
            Assert.Contains(
                $"<code>{metricName}</code>",
                guide,
                StringComparison.Ordinal);
        }

        foreach (string unit in PublicStringConstants(typeof(CSharpDbMetricUnits)))
        {
            Assert.Contains(
                $"<code>{unit}</code>",
                guide,
                StringComparison.Ordinal);
        }

        foreach (string tagName in CSharpDbMetricTagNames.Allowed)
        {
            Assert.Contains(
                $"<code>{tagName}</code>",
                guide,
                StringComparison.Ordinal);
        }

        foreach (CSharpDbLogEventDefinition logEvent in CSharpDbLogEvents.All)
        {
            Assert.Contains(
                $"<td>{logEvent.EventId}</td>",
                guide,
                StringComparison.Ordinal);
            Assert.Contains(
                $"<code>{logEvent.Name}</code>",
                guide,
                StringComparison.Ordinal);
        }

        foreach (string spanName in new[]
                 {
                     "csharpdb.query",
                     "csharpdb.script",
                     "csharpdb.procedure",
                     "csharpdb.transaction",
                     "csharpdb.database",
                     "csharpdb.recovery",
                     "csharpdb.checkpoint",
                     "csharpdb.backup",
                     "csharpdb.restore",
                     "csharpdb.reindex",
                     "csharpdb.vacuum",
                     "csharpdb.maintenance",
                     "csharpdb.pipeline",
                     "csharpdb.operation",
                 })
        {
            Assert.Contains(spanName, guide, StringComparison.Ordinal);
        }

        foreach (string requiredBoundary in new[]
                 {
                     "CSharpDB.Observability</code> is deliberately BCL-only",
                     "Health is independent",
                     "Startup WAL recovery and automatic foreground, background, and shutdown checkpoints create explicit-root physical spans",
                     "checkpoint sub-step inside startup recovery reuse their logical parent and suppress a second physical checkpoint span",
                     "diagnostics and Prometheus accept only the actual loopback peer",
                     "Query text requires a separate authorized reveal",
                     "Never calculate a counter delta across a changed server instance id or counter epoch",
                     "OTEL_EXPORTER_OTLP_HEADERS",
                     "AllowSensitiveQueryDetailAccess=true",
                 })
        {
            Assert.Contains(requiredBoundary, guide, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void PublicGuide_IsCrossLinkedAndInTheSitemap()
    {
        string repoRoot = FindRepoRoot();
        foreach (string relativePath in new[]
                 {
                     Path.Combine("www", "docs", "configuration.html"),
                     Path.Combine("www", "docs", "rest-api.html"),
                     Path.Combine("www", "docs", "admin-ui.html"),
                     Path.Combine("www", "docs", "api-reference.html"),
                     Path.Combine("www", "docs", "ecosystem.html"),
                     Path.Combine("www", "docs", "index.html"),
                     Path.Combine(
                         "www",
                         "docs",
                         "tutorials",
                         "multi-writer-daemon-grpc.html"),
                     Path.Combine("www", "architecture.html"),
                     Path.Combine("www", "architecture-reference.html"),
                     Path.Combine("www", "docs", "samples.html"),
                     Path.Combine("www", "sitemap.xml"),
                     Path.Combine("src", "CSharpDB.Observability", "README.md"),
                     "README.md",
                 })
        {
            string content = File.ReadAllText(Path.Combine(repoRoot, relativePath));
            Assert.Contains(
                "observability.html",
                content,
                StringComparison.OrdinalIgnoreCase);
        }

        string sitemap = File.ReadAllText(
            Path.Combine(repoRoot, "www", "sitemap.xml"));
        Assert.Contains(
            "https://csharpdb.com/docs/observability.html",
            sitemap,
            StringComparison.Ordinal);
    }

    [Fact]
    public void SupportedSample_UsesSafeRunnableHostWiring()
    {
        string repoRoot = FindRepoRoot();
        string sampleRoot = Path.Combine(
            repoRoot,
            "samples",
            "observability-host");
        string program = File.ReadAllText(Path.Combine(sampleRoot, "Program.cs"));
        string configuration = File.ReadAllText(
            Path.Combine(sampleRoot, "appsettings.json"));
        string readme = File.ReadAllText(Path.Combine(sampleRoot, "README.md"));

        foreach (string hostCall in new[]
                 {
                     "AddCSharpDbObservability",
                     "AddCSharpDbHealth",
                     "UseCSharpDbObservability",
                     "MapCSharpDbHealthEndpoints",
                     "MapCSharpDbPrometheusEndpoint",
                     "ILoggerFactory",
                 })
        {
            Assert.Contains(hostCall, program, StringComparison.Ordinal);
        }

        foreach (string safeDefault in new[]
                 {
                     "Data Source=:memory:",
                     "\"SqlText\": \"None\"",
                     "\"Otlp\": {",
                     "\"Enabled\": false",
                     "\"AllowInsecureRemoteAccess\": false",
                     "\"LivenessPath\": \"/health/live\"",
                     "\"ReadinessPath\": \"/health/ready\"",
                 })
        {
            Assert.Contains(safeDefault, configuration, StringComparison.Ordinal);
        }

        Assert.Contains("OTEL_EXPORTER_OTLP_ENDPOINT", readme, StringComparison.Ordinal);
        Assert.Contains("OTEL_EXPORTER_OTLP_HEADERS", readme, StringComparison.Ordinal);
        Assert.Contains("actual loopback peer", readme, StringComparison.Ordinal);

        string solution = File.ReadAllText(Path.Combine(repoRoot, "CSharpDB.slnx"));
        Assert.Contains(
            "samples/observability-host/ObservabilityHostSample.csproj",
            solution,
            StringComparison.Ordinal);
        string samplesReadme = File.ReadAllText(
            Path.Combine(repoRoot, "samples", "README.md"));
        Assert.Contains("observability-host/", samplesReadme, StringComparison.Ordinal);
    }

    private static IEnumerable<string> PublicStringConstants(Type type)
        => type.GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(static field =>
                field.IsLiteral &&
                !field.IsInitOnly &&
                field.FieldType == typeof(string))
            .Select(static field => (string)field.GetRawConstantValue()!)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static value => value, StringComparer.Ordinal);

    private static string FindRepoRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "CSharpDB.slnx")))
                return directory.FullName;

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException(
            "Could not locate repository root from test base directory.");
    }
}
