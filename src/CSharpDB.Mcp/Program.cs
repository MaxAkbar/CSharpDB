using CSharpDB.Service;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using ModelContextProtocol;

var builder = Host.CreateApplicationBuilder(args);

// ─── Database path configuration ────────────────────────────────────
// Priority: --database CLI arg > CSHARPDB_DATABASE env var > appsettings.json > default
string? dbPath = null;

for (int i = 0; i < args.Length - 1; i++)
{
    if (args[i] is "--database" or "-d")
    {
        dbPath = args[i + 1];
        break;
    }
}

dbPath ??= Environment.GetEnvironmentVariable("CSHARPDB_DATABASE");

if (dbPath is not null)
{
    builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
    {
        ["ConnectionStrings:CSharpDB"] = $"Data Source={dbPath}",
    });
}

// ─── Services ───────────────────────────────────────────────────────
builder.Services.AddSingleton<CSharpDbService>();

builder.Services
    .AddMcpServer(options =>
    {
        options.ServerInfo = new()
        {
            Name = "CSharpDB",
            Version = "1.0.0",
        };
    })
    .WithStdioServerTransport()
    .WithToolsFromAssembly();

var host = builder.Build();

// Initialize the database connection before accepting requests
var dbService = host.Services.GetRequiredService<CSharpDbService>();
await dbService.InitializeAsync();

await host.RunAsync();
