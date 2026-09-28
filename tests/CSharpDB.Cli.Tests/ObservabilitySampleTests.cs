namespace CSharpDB.Cli.Tests;

public sealed class ObservabilitySampleTests
{
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

        string solution = File.ReadAllText(Path.Combine(repoRoot, "CSharpDB.slnx"));
        Assert.Contains(
            "samples/observability-host/ObservabilityHostSample.csproj",
            solution,
            StringComparison.Ordinal);
    }

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
