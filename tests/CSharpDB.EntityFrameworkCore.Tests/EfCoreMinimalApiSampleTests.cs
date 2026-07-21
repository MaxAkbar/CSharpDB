using System.Net;
using System.Net.Http.Json;
using CSharpDB.Data;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using SampleProgram = EfCoreMinimalApiSample.Program;

namespace CSharpDB.EntityFrameworkCore.Tests;

public sealed class EfCoreMinimalApiSampleTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task AspNetCoreMinimalApi_CrudPersistsAcrossHostRestart()
    {
        string outputDirectory = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_efcore_minimal_api_{Guid.NewGuid():N}");
        string databasePath = Path.Combine(outputDirectory, "todos.db");
        Directory.CreateDirectory(outputDirectory);

        try
        {
            int todoId;

            await using (var firstFactory = new SampleApiFactory(databasePath))
            {
                using HttpClient client = firstFactory.CreateClient();

                List<TodoResponse>? initial = await client.GetFromJsonAsync<List<TodoResponse>>(
                    "/todos",
                    Ct);
                Assert.NotNull(initial);
                Assert.Empty(initial);

                using HttpResponseMessage createResponse = await client.PostAsJsonAsync(
                    "/todos",
                    new { title = "Ship the provider" },
                    Ct);
                Assert.Equal(HttpStatusCode.Created, createResponse.StatusCode);
                Assert.NotNull(createResponse.Headers.Location);

                TodoResponse? created = await createResponse.Content.ReadFromJsonAsync<TodoResponse>(Ct);
                Assert.NotNull(created);
                todoId = created.Id;
                Assert.True(todoId > 0);
                Assert.Equal("Ship the provider", created.Title);
                Assert.False(created.IsComplete);

                TodoResponse? loaded = await client.GetFromJsonAsync<TodoResponse>(
                    createResponse.Headers.Location,
                    Ct);
                Assert.Equal(created, loaded);

                using HttpResponseMessage updateResponse = await client.PutAsJsonAsync(
                    $"/todos/{todoId}",
                    new
                    {
                        title = "Ship the provider sample",
                        isComplete = true,
                    },
                    Ct);
                Assert.Equal(HttpStatusCode.OK, updateResponse.StatusCode);
            }

            await CSharpDbConnection.ClearPoolAsync($"Data Source={databasePath}");

            Assert.True(
                File.Exists(databasePath),
                "The minimal API did not create the configured database file.");

            await using (var reopenedFactory = new SampleApiFactory(databasePath))
            {
                using HttpClient client = reopenedFactory.CreateClient();

                TodoResponse? persisted = await client.GetFromJsonAsync<TodoResponse>(
                    $"/todos/{todoId}",
                    Ct);
                Assert.NotNull(persisted);
                Assert.Equal("Ship the provider sample", persisted.Title);
                Assert.True(persisted.IsComplete);

                using HttpResponseMessage deleteResponse = await client.DeleteAsync(
                    $"/todos/{todoId}",
                    Ct);
                Assert.Equal(HttpStatusCode.NoContent, deleteResponse.StatusCode);

                using HttpResponseMessage missingResponse = await client.GetAsync(
                    $"/todos/{todoId}",
                    Ct);
                Assert.Equal(HttpStatusCode.NotFound, missingResponse.StatusCode);
            }
        }
        finally
        {
            await CSharpDbConnection.ClearPoolAsync($"Data Source={databasePath}");
            await DeleteIfExistsAsync(databasePath);
            await DeleteIfExistsAsync(databasePath + ".wal");

            if (Directory.Exists(outputDirectory))
                Directory.Delete(outputDirectory, recursive: true);
        }
    }

    private sealed class SampleApiFactory(string databasePath)
        : WebApplicationFactory<SampleProgram>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Development");
            builder.ConfigureAppConfiguration((_, configuration) =>
            {
                configuration.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ConnectionStrings:CSharpDB"] = $"Data Source={databasePath}",
                });
            });
        }
    }

    private static async ValueTask DeleteIfExistsAsync(string path)
    {
        if (!File.Exists(path))
            return;

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        Exception? lastException = null;

        while (true)
        {
            try
            {
                File.Delete(path);
                return;
            }
            catch (IOException ex) when (stopwatch.Elapsed < TimeSpan.FromSeconds(2))
            {
                lastException = ex;
            }
            catch (UnauthorizedAccessException ex) when (stopwatch.Elapsed < TimeSpan.FromSeconds(2))
            {
                lastException = ex;
            }

            if (!File.Exists(path))
                return;

            if (stopwatch.Elapsed >= TimeSpan.FromSeconds(2))
                break;

            await Task.Delay(25);
        }

        throw new IOException(
            $"Failed to delete temporary database file '{path}' within the cleanup timeout.",
            lastException);
    }

    private sealed record TodoResponse(int Id, string Title, bool IsComplete);
}
