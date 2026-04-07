using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;
using CSharpDB.Engine;

namespace CSharpDB.Cli.Tests;

[Collection("CliConsole")]
public sealed class CliIntegrationTests
{
    [Fact]
    public async Task CliProcess_InfoCommand_WorksOnFreshDatabase()
    {
        var ct = TestContext.Current.CancellationToken;
        string workDir = NewTempDirectory();

        try
        {
            var result = await RunCliAsync(
                [],
                ".info" + Environment.NewLine + ".quit" + Environment.NewLine,
                workDir,
                ct);

            Assert.Equal(0, result.ExitCode);
            Assert.Contains("Database: csharpdb.db", result.StdOut, StringComparison.Ordinal);
            Assert.Contains("Objects:", result.StdOut, StringComparison.Ordinal);
            Assert.DoesNotContain("Error:", result.StdOut, StringComparison.Ordinal);
            Assert.True(string.IsNullOrWhiteSpace(result.StdErr));
            Assert.True(File.Exists(Path.Combine(workDir, "csharpdb.db")));
        }
        finally
        {
            DeleteDirectoryIfExists(workDir);
        }
    }

    [Fact]
    public async Task CliProcess_PositionalDatabasePath_ExecutesSqlAndPersistsRows()
    {
        var ct = TestContext.Current.CancellationToken;
        string workDir = NewTempDirectory();
        string dbPath = Path.Combine(workDir, "orders.db");

        try
        {
            string input = string.Join(Environment.NewLine, new[]
            {
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, qty INTEGER NOT NULL);",
                "INSERT INTO orders VALUES (1, 5);",
                "SELECT id, qty FROM orders;",
                ".quit",
                "",
            });

            var result = await RunCliAsync([dbPath], input, workDir, ct);

            Assert.Equal(0, result.ExitCode);
            Assert.Contains("1 row affected", result.StdOut, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("1 row", result.StdOut, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("qty", result.StdOut, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("5", result.StdOut, StringComparison.Ordinal);
            Assert.True(string.IsNullOrWhiteSpace(result.StdErr));

            await using var db = await Database.OpenAsync(dbPath, ct);
            await using var query = await db.ExecuteAsync("SELECT COUNT(*) FROM orders;", ct);
            var rows = await query.ToListAsync(ct);
            Assert.Equal(1L, rows[0][0].AsInteger);
        }
        finally
        {
            DeleteDirectoryIfExists(workDir);
        }
    }

    [Fact]
    public async Task CliProcess_ReadCommand_ExecutesScriptFile()
    {
        var ct = TestContext.Current.CancellationToken;
        string workDir = NewTempDirectory();
        string dbPath = Path.Combine(workDir, "script.db");
        string scriptPath = Path.Combine(workDir, "seed.sql");

        try
        {
            await File.WriteAllTextAsync(scriptPath, """
                CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
                INSERT INTO items VALUES (1, 'alpha');
                INSERT INTO items VALUES (2, 'beta');
                """, ct);

            string input = string.Join(Environment.NewLine, new[]
            {
                $".read {scriptPath}",
                ".quit",
                "",
            });

            var result = await RunCliAsync([dbPath], input, workDir, ct);

            Assert.Equal(0, result.ExitCode);
            Assert.Contains("Script complete: 3 passed, 0 failed.", result.StdOut, StringComparison.Ordinal);
            Assert.True(string.IsNullOrWhiteSpace(result.StdErr));

            await using var db = await Database.OpenAsync(dbPath, ct);
            await using var query = await db.ExecuteAsync("SELECT COUNT(*) FROM items;", ct);
            var rows = await query.ToListAsync(ct);
            Assert.Equal(2L, rows[0][0].AsInteger);
        }
        finally
        {
            DeleteDirectoryIfExists(workDir);
        }
    }

    [Fact]
    public async Task CliProcess_DotCommand_ShowsHelpWhenInputIsRedirected()
    {
        var ct = TestContext.Current.CancellationToken;
        string workDir = NewTempDirectory();

        try
        {
            var result = await RunCliAsync(
                [],
                "." + Environment.NewLine + ".quit" + Environment.NewLine,
                workDir,
                ct);

            Assert.Equal(0, result.ExitCode);
            Assert.Contains("Available Commands", result.StdOut, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(".tables", result.StdOut, StringComparison.OrdinalIgnoreCase);
            Assert.True(string.IsNullOrWhiteSpace(result.StdErr));
        }
        finally
        {
            DeleteDirectoryIfExists(workDir);
        }
    }

    private static async Task<CliProcessResult> RunCliAsync(
        IReadOnlyList<string> args,
        string input,
        string workingDirectory,
        CancellationToken ct)
    {
        string cliAssemblyPath = typeof(CliShellOptions).Assembly.Location;
        var startInfo = new ProcessStartInfo("dotnet")
        {
            WorkingDirectory = workingDirectory,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };

        startInfo.ArgumentList.Add(cliAssemblyPath);
        foreach (string arg in args)
            startInfo.ArgumentList.Add(arg);

        using var process = new Process { StartInfo = startInfo };
        if (!process.Start())
            throw new InvalidOperationException("Failed to start the CLI process.");

        Task<string> stdoutTask = process.StandardOutput.ReadToEndAsync(ct);
        Task<string> stderrTask = process.StandardError.ReadToEndAsync(ct);

        if (!string.IsNullOrEmpty(input))
            await process.StandardInput.WriteAsync(input.AsMemory(), ct);

        process.StandardInput.Close();
        await process.WaitForExitAsync(ct);

        return new CliProcessResult(
            process.ExitCode,
            StripAnsi(await stdoutTask),
            StripAnsi(await stderrTask));
    }

    private static string StripAnsi(string value)
        => Regex.Replace(value, @"\x1B\[[0-9;]*m", string.Empty);

    private static string NewTempDirectory()
    {
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_cli_integration_{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteDirectoryIfExists(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }

    private sealed record CliProcessResult(int ExitCode, string StdOut, string StdErr);
}
