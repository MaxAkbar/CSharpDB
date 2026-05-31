using System.Net;
using System.Net.Http.Json;
using CSharpDB.Admin.Services;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.Admin.Forms.Tests.Admin;

public sealed class AdminDesktopShellEndpointTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Healthz_ReturnsOk()
    {
        string dbPath = NewTempDbPath();

        try
        {
            await using var factory = new TestAdminFactory(dbPath);
            using HttpClient client = factory.CreateClient();

            using HttpResponseMessage response = await client.GetAsync("/healthz", Ct);

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }
        finally
        {
            await DeleteDatabaseFilesAsync(dbPath);
        }
    }

    [Fact]
    public async Task OpenDatabase_IsUnavailable_WhenDesktopShellDisabled()
    {
        string dbPath = NewTempDbPath();
        string targetPath = NewTempDbPath();

        try
        {
            await using var factory = new TestAdminFactory(dbPath);
            using HttpClient client = factory.CreateClient();

            using HttpResponseMessage response = await client.PostAsJsonAsync(
                "/_desktop/open-database",
                new DesktopShellEndpoints.OpenDatabaseRequest(targetPath),
                Ct);

            Assert.Contains(response.StatusCode, new[] { HttpStatusCode.NotFound, HttpStatusCode.MethodNotAllowed });
        }
        finally
        {
            await DeleteDatabaseFilesAsync(dbPath);
            await DeleteDatabaseFilesAsync(targetPath);
        }
    }

    [Fact]
    public async Task OpenDatabase_RejectsMissingAndWrongDesktopShellToken()
    {
        const string shellToken = "desktop-test-token";
        string dbPath = NewTempDbPath();
        string targetPath = NewTempDbPath();

        try
        {
            await using var factory = new TestAdminFactory(
                dbPath,
                new Dictionary<string, string?>
                {
                    ["CSharpDB:DesktopShell"] = "true",
                    ["CSharpDB:DesktopShellToken"] = shellToken,
                });

            using HttpClient client = factory.CreateClient();

            using HttpResponseMessage missing = await client.PostAsJsonAsync(
                "/_desktop/open-database",
                new DesktopShellEndpoints.OpenDatabaseRequest(targetPath),
                Ct);
            Assert.Equal(HttpStatusCode.Unauthorized, missing.StatusCode);

            using var wrongRequest = new HttpRequestMessage(HttpMethod.Post, "/_desktop/open-database")
            {
                Content = JsonContent.Create(new DesktopShellEndpoints.OpenDatabaseRequest(targetPath)),
            };
            wrongRequest.Headers.TryAddWithoutValidation(DesktopShellEndpoints.TokenHeaderName, "wrong-token");

            using HttpResponseMessage wrong = await client.SendAsync(wrongRequest, Ct);
            Assert.Equal(HttpStatusCode.Unauthorized, wrong.StatusCode);
        }
        finally
        {
            await DeleteDatabaseFilesAsync(dbPath);
            await DeleteDatabaseFilesAsync(targetPath);
        }
    }

    [Fact]
    public async Task OpenDatabase_WithValidDesktopShellToken_SwitchesDatabase()
    {
        const string shellToken = "desktop-test-token";
        string dbPath = NewTempDbPath();
        string targetPath = NewTempDbPath();

        try
        {
            await using var factory = new TestAdminFactory(
                dbPath,
                new Dictionary<string, string?>
                {
                    ["CSharpDB:DesktopShell"] = "true",
                    ["CSharpDB:DesktopShellToken"] = shellToken,
                });

            using HttpClient client = factory.CreateClient();
            using var request = new HttpRequestMessage(HttpMethod.Post, "/_desktop/open-database")
            {
                Content = JsonContent.Create(new DesktopShellEndpoints.OpenDatabaseRequest(targetPath)),
            };
            request.Headers.TryAddWithoutValidation(DesktopShellEndpoints.TokenHeaderName, shellToken);

            using HttpResponseMessage response = await client.SendAsync(request, Ct);

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);

            DatabaseClientHolder holder = factory.Services.GetRequiredService<DatabaseClientHolder>();
            Assert.Equal(Path.GetFullPath(targetPath), holder.DataSource);
        }
        finally
        {
            await DeleteDatabaseFilesAsync(dbPath);
            await DeleteDatabaseFilesAsync(targetPath);
        }
    }

    private static string NewTempDbPath()
        => Path.Combine(Path.GetTempPath(), $"csharpdb_admin_desktop_{Guid.NewGuid():N}.db");

    private sealed class TestAdminFactory(
        string dbPath,
        IReadOnlyDictionary<string, string?>? extraConfig = null) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Development");
            builder.ConfigureAppConfiguration((_, config) =>
            {
                var values = new Dictionary<string, string?>
                {
                    ["ConnectionStrings:CSharpDB"] = $"Data Source={dbPath}",
                };

                if (extraConfig is not null)
                {
                    foreach (var pair in extraConfig)
                        values[pair.Key] = pair.Value;
                }

                config.AddInMemoryCollection(values);
            });
        }
    }

    private static async Task DeleteDatabaseFilesAsync(string dbPath)
    {
        await DeleteIfExistsAsync(dbPath);
        await DeleteIfExistsAsync(dbPath + ".wal");
    }

    private static async Task DeleteIfExistsAsync(string path)
    {
        if (!File.Exists(path))
            return;

        var sw = System.Diagnostics.Stopwatch.StartNew();
        Exception? lastException = null;
        while (true)
        {
            try
            {
                File.Delete(path);
                return;
            }
            catch (IOException ex) when (sw.Elapsed < TimeSpan.FromSeconds(2))
            {
                lastException = ex;
            }
            catch (UnauthorizedAccessException ex) when (sw.Elapsed < TimeSpan.FromSeconds(2))
            {
                lastException = ex;
            }

            if (!File.Exists(path))
                return;

            if (sw.Elapsed >= TimeSpan.FromSeconds(2))
                break;

            await Task.Delay(25);
        }

        throw new IOException($"Failed to delete temporary database file '{path}' within the cleanup timeout.", lastException);
    }
}
