using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using CSharpDB.Engine;

namespace CSharpDB.Cli.Tests;

[Collection("CliConsole")]
public sealed class CliIntegrationTests
{
    [Fact]
    public async Task CliProcess_MigrateInspect_DispatchesWithoutOpeningADatabaseNamedMigrate()
    {
        var ct = TestContext.Current.CancellationToken;
        string workDir = NewTempDirectory();
        string catalogPath = Path.Combine(workDir, "catalog.json");

        try
        {
            var result = await RunCliAsync(
                ["migrate", "inspect", "--source", "synthetic", "--out", catalogPath],
                string.Empty,
                workDir,
                ct);

            Assert.Equal(1, result.ExitCode);
            Assert.Contains("catalog=", result.StdOut, StringComparison.Ordinal);
            Assert.True(File.Exists(catalogPath));
            Assert.False(File.Exists(Path.Combine(workDir, "migrate")));
            Assert.False(File.Exists(Path.Combine(workDir, "migrate.db")));
            Assert.True(string.IsNullOrWhiteSpace(result.StdErr));
        }
        finally
        {
            DeleteDirectoryIfExists(workDir);
        }
    }

    [Fact]
    public async Task CliProcess_MigrateApply_DispatchesAndEmitsPureJsonRunReport()
    {
        var ct = TestContext.Current.CancellationToken;
        string workDir = NewTempDirectory();
        string catalogPath = Path.Combine(workDir, "catalog.json");
        string planPath = Path.Combine(workDir, "plan.json");
        string targetPath = Path.Combine(workDir, "staged.csdb");
        string runPath = Path.Combine(workDir, "run.json");

        try
        {
            Assert.Equal(
                InspectorCommandRunner.ExitWarn,
                await MigrationCommandRunner.RunAsync(
                    ["migrate", "inspect", "--source", "synthetic", "--out", catalogPath],
                    TextWriter.Null,
                    TextWriter.Null,
                    ct));
            Assert.Equal(
                InspectorCommandRunner.ExitWarn,
                await MigrationCommandRunner.RunAsync(
                    [
                        "migrate", "plan", catalogPath,
                        "--out", planPath,
                        "--accept-exclusions", "all",
                    ],
                    TextWriter.Null,
                    TextWriter.Null,
                    ct));

            var result = await RunCliAsync(
                [
                    "migrate", "apply", planPath,
                    "--catalog", catalogPath,
                    "--target", targetPath,
                    "--out", runPath,
                    "--format", "json",
                ],
                string.Empty,
                workDir,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitWarn, result.ExitCode);
            Assert.True(string.IsNullOrWhiteSpace(result.StdErr));
            using JsonDocument stdout = JsonDocument.Parse(result.StdOut);
            using JsonDocument report = JsonDocument.Parse(await File.ReadAllTextAsync(runPath, ct));
            Assert.Equal("csharpdb-migration-run/v1", stdout.RootElement.GetProperty("format").GetString());
            Assert.Equal("awaitingValidation", stdout.RootElement.GetProperty("status").GetString());
            Assert.Equal(stdout.RootElement.GetRawText(), report.RootElement.GetRawText());
            Assert.True(stdout.RootElement.GetProperty("rowsWritten").GetInt64() > 0);
            Assert.True(File.Exists(targetPath));
            Assert.False(File.Exists(Path.Combine(workDir, "migrate")));
            Assert.False(File.Exists(Path.Combine(workDir, "migrate.db")));
            Assert.False(File.Exists(targetPath + ".migration.lock"));
        }
        finally
        {
            DeleteDirectoryIfExists(workDir);
        }
    }

    [Fact]
    public async Task CliProcess_MigrateCsv_UsesRetainedPackageAfterRawInputIsDeleted()
    {
        var ct = TestContext.Current.CancellationToken;
        string workDir = NewTempDirectory();
        string sourcePath = Path.Combine(workDir, "orders.csv");
        string packagePath = Path.Combine(workDir, "orders.csdbcsv");
        string catalogPath = Path.Combine(workDir, "catalog.json");
        string planPath = Path.Combine(workDir, "plan.json");
        string targetPath = Path.Combine(workDir, "staged.csdb");
        string runPath = Path.Combine(workDir, "run.json");

        try
        {
            await File.WriteAllTextAsync(
                sourcePath,
                """
                id,name
                1,alpha
                2,"bravo
                incorporated"
                3,charlie
                """,
                new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
                ct);

            CliProcessResult inspect = await RunCliAsync(
                [
                    "migrate", "inspect",
                    "--source", "csv",
                    "--input", sourcePath,
                    "--package", packagePath,
                    "--out", catalogPath,
                ],
                string.Empty,
                workDir,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitOk, inspect.ExitCode);
            Assert.True(string.IsNullOrWhiteSpace(inspect.StdErr));
            Match digestMatch = Regex.Match(
                inspect.StdOut,
                @"(?:^|\|\s*)manifestDigest=(sha256:[0-9a-f]{64})(?:\s*\||\s*$)",
                RegexOptions.CultureInvariant);
            Assert.True(digestMatch.Success, $"No manifest digest was emitted: {inspect.StdOut}");
            string manifestDigest = digestMatch.Groups[1].Value;
            Assert.True(File.Exists(packagePath));
            Assert.True(File.Exists(catalogPath));
            byte[] originalPackage = await File.ReadAllBytesAsync(packagePath, ct);

            File.Delete(sourcePath);

            CliProcessResult plan = await RunCliAsync(
                [
                    "migrate", "plan", catalogPath,
                    "--out", planPath,
                    "--accept-exclusions", "all",
                ],
                string.Empty,
                workDir,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitWarn, plan.ExitCode);
            Assert.True(string.IsNullOrWhiteSpace(plan.StdErr));
            Assert.True(File.Exists(planPath));

            CliProcessResult apply = await RunCliAsync(
                [
                    "migrate", "apply", planPath,
                    "--catalog", catalogPath,
                    "--source-package", packagePath,
                    "--expected-manifest-digest", manifestDigest,
                    "--target", targetPath,
                    "--out", runPath,
                    "--format", "json",
                ],
                string.Empty,
                workDir,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitWarn, apply.ExitCode);
            Assert.True(string.IsNullOrWhiteSpace(apply.StdErr));
            Assert.False(File.Exists(sourcePath));
            Assert.True(File.Exists(packagePath));
            Assert.Equal(originalPackage, await File.ReadAllBytesAsync(packagePath, ct));
            Assert.True(File.Exists(targetPath));
            Assert.True(File.Exists(runPath));
            Assert.False(File.Exists(targetPath + ".migration.lock"));

            using JsonDocument stdout = JsonDocument.Parse(apply.StdOut);
            using JsonDocument report = JsonDocument.Parse(await File.ReadAllTextAsync(runPath, ct));
            Assert.Equal(3, stdout.RootElement.GetProperty("rowsWritten").GetInt64());
            Assert.Equal(stdout.RootElement.GetRawText(), report.RootElement.GetRawText());
        }
        finally
        {
            DeleteDirectoryIfExists(workDir);
        }
    }

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
            Assert.Contains("csharpdb.db", result.StdOut, StringComparison.Ordinal);
            Assert.Contains("Tables:", result.StdOut, StringComparison.Ordinal);
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
            Assert.Contains("Inspection", result.StdOut, StringComparison.OrdinalIgnoreCase);
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
