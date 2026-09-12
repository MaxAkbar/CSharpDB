using System.Net;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace CSharpDB.Admin.Forms.Tests.Admin;

public sealed class AdminStaticAssetsTests
{
    [Theory]
    [InlineData("Development")]
    [InlineData("Production")]
    public async Task Startup_ServesScriptsAndStylesRequiredForInteraction(string environment)
    {
        // An unpublished build must remain interactive even without a launch profile.
        await using var factory = new StaticAssetsFactory(environment);
        using HttpClient client = factory.CreateClient();
        foreach (var (path, expectedContent) in new[]
        {
            ("/_framework/blazor.web.js", "Blazor"),
            ("/js/interop.js", "keyboardInterop"),
            ("/js/modeler.js", "window."),
            ("/CSharpDB.Admin.styles.css", ".definition-explorer"),
            ("/_content/CSharpDB.Admin.Forms/css/designer.css", ".designer"),
            ("/_content/CSharpDB.Admin.Reports/js/reports.js", "window.")
        })
        {
            using var response = await client.GetAsync(path, TestContext.Current.CancellationToken);
            string content = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
            Assert.True(response.StatusCode == HttpStatusCode.OK,
                $"{environment}: {path} returned {(int)response.StatusCode}.");
            Assert.Contains(expectedContent, content);
        }
    }

    private sealed class StaticAssetsFactory(string environment) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment(environment);
            builder.ConfigureAppConfiguration((_, configuration) => configuration.AddInMemoryCollection(
                new Dictionary<string, string?>
                {
                    ["ConnectionStrings:CSharpDB"] = "Data Source=:memory:",
                    ["CSharpDB:HostDatabase:OpenMode"] = "Direct",
                    ["Logging:LogLevel:Default"] = "None"
                }));
        }
    }
}
