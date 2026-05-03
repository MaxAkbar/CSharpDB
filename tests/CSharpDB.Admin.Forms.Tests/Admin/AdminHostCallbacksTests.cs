using CSharpDB.Admin.Configuration;
using CSharpDB.Admin.Services;
using CSharpDB.Client;

namespace CSharpDB.Admin.Forms.Tests.Admin;

public sealed class AdminHostCallbacksTests
{
    [Fact]
    public async Task DefaultFunctionRegistry_IsAvailableToDirectClientSql()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_admin_callbacks_{Guid.NewGuid():N}.db");

        try
        {
            await using ICSharpDbClient client = CSharpDbClient.Create(
                AdminClientOptionsBuilder.BuildDirectDataSource(
                    dbPath,
                    new AdminHostDatabaseOptions { OpenMode = AdminHostOpenMode.Direct },
                    AdminHostCallbacks.CreateFunctionRegistry()));

            Assert.Null((await client.ExecuteSqlAsync("CREATE TABLE inputs (value TEXT);", ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync("INSERT INTO inputs VALUES ('Hello From Admin');", ct)).Error);

            var result = await client.ExecuteSqlAsync("SELECT Slugify(value) FROM inputs;", ct);

            Assert.Null(result.Error);
            Assert.NotNull(result.Rows);
            Assert.Equal("hello-from-admin", result.Rows![0][0]);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task DefaultCommandRegistry_ProvidesExecutableEchoCommand()
    {
        var ct = TestContext.Current.CancellationToken;
        var registry = AdminHostCallbacks.CreateCommandRegistry();

        Assert.True(registry.TryGetCommand("EchoAutomationEvent", out var command));

        var result = await command.InvokeAsync(
            metadata: new Dictionary<string, string>
            {
                ["surface"] = "AdminForms",
                ["event"] = "BeforeInsert",
            },
            ct: ct);

        Assert.True(result.Succeeded);
        Assert.Equal("Received AdminForms.BeforeInsert.", result.Message);
        Assert.Equal("Received AdminForms.BeforeInsert.", result.Value.AsText);
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
