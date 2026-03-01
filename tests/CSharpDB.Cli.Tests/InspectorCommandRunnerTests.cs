using CSharpDB.Engine;

namespace CSharpDB.Cli.Tests;

[Collection("CliConsole")]
public sealed class InspectorCommandRunnerTests
{
    [Fact]
    public async Task RunAsync_InspectPageMissingArgs_ReturnsUsage()
    {
        var ct = TestContext.Current.CancellationToken;
        var stdout = new StringWriter();
        var stderr = new StringWriter();

        int code = await InspectorCommandRunner.RunAsync(
            ["inspect-page", "only-db-path.db"],
            stdout,
            stderr,
            ct);

        Assert.Equal(InspectorCommandRunner.ExitUsage, code);
        Assert.Contains("Usage: csharpdb inspect-page", stderr.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task RunAsync_InspectJson_WritesReport()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();
        var stdout = new StringWriter();
        var stderr = new StringWriter();

        try
        {
            await using (var db = await Database.OpenAsync(dbPath, ct))
            {
                await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", ct);
                await db.ExecuteAsync("INSERT INTO t VALUES (1, 'alice')", ct);
            }

            int code = await InspectorCommandRunner.RunAsync(
                ["inspect", dbPath, "--json"],
                stdout,
                stderr,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitOk, code);
            Assert.Contains("\"schemaVersion\": \"1.0\"", stdout.ToString(), StringComparison.Ordinal);
            Assert.True(string.IsNullOrWhiteSpace(stderr.ToString()));
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task RunAsync_CheckWalMissing_ReturnsOk()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();
        var stdout = new StringWriter();
        var stderr = new StringWriter();

        try
        {
            int code = await InspectorCommandRunner.RunAsync(
                ["check-wal", dbPath],
                stdout,
                stderr,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitOk, code);
            Assert.Contains("No WAL file present.", stdout.ToString(), StringComparison.Ordinal);
            Assert.True(string.IsNullOrWhiteSpace(stderr.ToString()));
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    private static string NewTempDbPath()
    {
        return Path.Combine(Path.GetTempPath(), $"csharpdb_cli_inspect_test_{Guid.NewGuid():N}.db");
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
