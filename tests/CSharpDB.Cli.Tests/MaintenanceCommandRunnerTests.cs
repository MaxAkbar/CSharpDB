using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Client.Models;
using CSharpDB.Engine;

namespace CSharpDB.Cli.Tests;

public sealed class MaintenanceCommandRunnerTests
{
    [Fact]
    public async Task MaintenanceCommandRunner_MigrateForeignKeys_ValidateOnlyJson_Works()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempFilePath(".db");
        string specPath = NewTempFilePath(".json");

        try
        {
            await using (var db = await Database.OpenAsync(dbPath, ct))
            {
                await db.ExecuteAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY);", ct);
                await db.ExecuteAsync("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER);", ct);
                await db.ExecuteAsync("INSERT INTO customers VALUES (1);", ct);
                await db.ExecuteAsync("INSERT INTO orders VALUES (10, 1);", ct);
            }

            await WriteForeignKeyMigrationSpecAsync(
                specPath,
                new ForeignKeyMigrationRequest
                {
                    Constraints =
                    [
                        new ForeignKeyMigrationConstraintSpec
                        {
                            TableName = "orders",
                            ColumnName = "customer_id",
                            ReferencedTableName = "customers",
                            ReferencedColumnName = "id",
                        },
                    ],
                },
                ct);

            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await MaintenanceCommandRunner.RunAsync(
                ["migrate-foreign-keys", dbPath, "--spec", specPath, "--validate-only", "--json"],
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitOk, exitCode);
            Assert.Equal(string.Empty, error.ToString());

            var result = JsonSerializer.Deserialize<ForeignKeyMigrationResult>(output.ToString(), JsonOptions);
            Assert.NotNull(result);
            Assert.True(result!.ValidateOnly);
            Assert.True(result.Succeeded);
            Assert.Equal(1, result.AppliedForeignKeys);
            Assert.Empty(result.Violations);
        }
        finally
        {
            DeleteIfExists(specPath);
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    private static readonly JsonSerializerOptions JsonOptions = CreateJsonOptions();

    private static JsonSerializerOptions CreateJsonOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true,
        };
        options.Converters.Add(new JsonStringEnumConverter());
        return options;
    }

    private static async Task WriteForeignKeyMigrationSpecAsync(string path, ForeignKeyMigrationRequest request, CancellationToken ct)
    {
        await File.WriteAllTextAsync(path, JsonSerializer.Serialize(request, JsonOptions), ct);
    }

    private static string NewTempFilePath(string extension)
        => Path.Combine(Path.GetTempPath(), $"csharpdb_cli_maintenance_test_{Guid.NewGuid():N}{extension}");

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
