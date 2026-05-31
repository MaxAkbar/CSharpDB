using System.Text.Json;
using CSharpDB.Engine;

namespace CSharpDB.Cli.Tests;

public sealed class DevOpsCommandRunnerTests
{
    [Fact]
    public async Task CompareSchema_JsonAndScriptOut_Works()
    {
        var ct = TestContext.Current.CancellationToken;
        string sourcePath = NewTempFilePath(".source.db");
        string targetPath = NewTempFilePath(".target.db");
        string scriptPath = NewTempFilePath(".sql");

        try
        {
            await using (var source = await Database.OpenAsync(sourcePath, ct))
            {
                await source.ExecuteAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL COLLATE NOCASE);", ct);
                await source.ExecuteAsync("CREATE INDEX idx_customers_name ON customers (name COLLATE NOCASE);", ct);
            }

            await using (var target = await Database.OpenAsync(targetPath, ct))
            {
                await target.ExecuteAsync("CREATE TABLE legacy (id INTEGER PRIMARY KEY);", ct);
            }

            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await DevOpsCommandRunner.RunAsync(
                ["compare", "schema", sourcePath, targetPath, "--json", "--script-out", scriptPath],
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitOk, exitCode);
            Assert.Equal(string.Empty, error.ToString());

            using JsonDocument document = JsonDocument.Parse(output.ToString());
            int totalChanges = document.RootElement.GetProperty("summary").GetProperty("totalChanges").GetInt32();
            Assert.Equal(3, totalChanges);

            string script = await File.ReadAllTextAsync(scriptPath, ct);
            Assert.Contains("CREATE TABLE customers", script, StringComparison.Ordinal);
            Assert.Contains("CREATE INDEX idx_customers_name", script, StringComparison.Ordinal);
            Assert.Contains("-- DROP TABLE legacy;", script, StringComparison.Ordinal);
        }
        finally
        {
            DeleteIfExists(sourcePath);
            DeleteIfExists(sourcePath + ".wal");
            DeleteIfExists(targetPath);
            DeleteIfExists(targetPath + ".wal");
            DeleteIfExists(scriptPath);
        }
    }

    [Fact]
    public async Task CompareData_JsonAndScriptOut_Works()
    {
        var ct = TestContext.Current.CancellationToken;
        string sourcePath = NewTempFilePath(".source.db");
        string targetPath = NewTempFilePath(".target.db");
        string scriptPath = NewTempFilePath(".sql");

        try
        {
            await using (var source = await Database.OpenAsync(sourcePath, ct))
            {
                await source.ExecuteAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL);", ct);
                await source.ExecuteAsync("INSERT INTO customers VALUES (1, 'same'), (2, 'source'), (3, 'insert');", ct);
            }

            await using (var target = await Database.OpenAsync(targetPath, ct))
            {
                await target.ExecuteAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL);", ct);
                await target.ExecuteAsync("INSERT INTO customers VALUES (1, 'same'), (2, 'target'), (4, 'delete');", ct);
            }

            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await DevOpsCommandRunner.RunAsync(
                ["compare", "data", sourcePath, targetPath, "--table", "customers", "--json", "--script-out", scriptPath],
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitOk, exitCode);
            Assert.Equal(string.Empty, error.ToString());

            using JsonDocument document = JsonDocument.Parse(output.ToString());
            JsonElement summary = document.RootElement.GetProperty("summary");
            Assert.Equal(1, summary.GetProperty("sourceOnlyRows").GetInt32());
            Assert.Equal(1, summary.GetProperty("targetOnlyRows").GetInt32());
            Assert.Equal(1, summary.GetProperty("changedRows").GetInt32());

            string script = await File.ReadAllTextAsync(scriptPath, ct);
            Assert.Contains("INSERT INTO customers", script, StringComparison.Ordinal);
            Assert.Contains("UPDATE customers SET name = 'source' WHERE id = 2;", script, StringComparison.Ordinal);
            Assert.Contains("DELETE FROM customers WHERE id = 4;", script, StringComparison.Ordinal);
        }
        finally
        {
            DeleteIfExists(sourcePath);
            DeleteIfExists(sourcePath + ".wal");
            DeleteIfExists(targetPath);
            DeleteIfExists(targetPath + ".wal");
            DeleteIfExists(scriptPath);
        }
    }

    [Fact]
    public async Task Drift_ReturnsWarn_WhenCurrentDiffersFromBaseline()
    {
        var ct = TestContext.Current.CancellationToken;
        string baselinePath = NewTempFilePath(".baseline.db");
        string currentPath = NewTempFilePath(".current.db");

        try
        {
            await using (var baseline = await Database.OpenAsync(baselinePath, ct))
            {
                await baseline.ExecuteAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL);", ct);
                await baseline.ExecuteAsync("INSERT INTO customers VALUES (1, 'baseline');", ct);
            }

            await using (var current = await Database.OpenAsync(currentPath, ct))
            {
                await current.ExecuteAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT);", ct);
                await current.ExecuteAsync("INSERT INTO customers (id, name, email) VALUES (1, 'current', 'a@example.com');", ct);
            }

            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await DevOpsCommandRunner.RunAsync(
                ["drift", currentPath, "--baseline", baselinePath, "--json"],
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitWarn, exitCode);
            Assert.Equal(string.Empty, error.ToString());

            using JsonDocument document = JsonDocument.Parse(output.ToString());
            Assert.True(document.RootElement.GetProperty("summary").GetProperty("hasDrift").GetBoolean());
            Assert.Equal(1, document.RootElement.GetProperty("summary").GetProperty("schemaChanges").GetInt32());
        }
        finally
        {
            DeleteIfExists(baselinePath);
            DeleteIfExists(baselinePath + ".wal");
            DeleteIfExists(currentPath);
            DeleteIfExists(currentPath + ".wal");
        }
    }

    private static string NewTempFilePath(string extension)
        => Path.Combine(Path.GetTempPath(), $"csharpdb_cli_devops_test_{Guid.NewGuid():N}{extension}");

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
