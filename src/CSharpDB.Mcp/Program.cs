using CSharpDB.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using ModelContextProtocol;

var builder = Host.CreateApplicationBuilder(args);

// ─── Client target configuration ────────────────────────────────────
// Priority:
//   endpoint: --endpoint CLI arg > CSHARPDB_ENDPOINT env var
//   database: --database CLI arg > CSHARPDB_DATABASE env var > appsettings.json > default
//   transport: --transport CLI arg > CSHARPDB_TRANSPORT env var
string? endpoint = null;
string? dbPath = null;
CSharpDbTransport? transport = null;

for (int i = 0; i < args.Length - 1; i++)
{
    if (args[i] is "--database" or "-d")
    {
        dbPath = args[i + 1];
        continue;
    }

    if (args[i] is "--endpoint" or "-e")
    {
        endpoint = args[i + 1];
        continue;
    }

    if (args[i] is "--transport" or "-t")
    {
        transport = ParseTransport(args[i + 1]);
    }
}

endpoint ??= Environment.GetEnvironmentVariable("CSHARPDB_ENDPOINT");
dbPath ??= Environment.GetEnvironmentVariable("CSHARPDB_DATABASE");
transport ??= ParseTransport(Environment.GetEnvironmentVariable("CSHARPDB_TRANSPORT"));

// ─── Services ───────────────────────────────────────────────────────
builder.Services.AddCSharpDbClient(sp =>
{
    if (!string.IsNullOrWhiteSpace(endpoint))
    {
        return new CSharpDbClientOptions
        {
            Transport = transport,
            Endpoint = endpoint,
        };
    }

    if (!string.IsNullOrWhiteSpace(dbPath))
    {
        return new CSharpDbClientOptions
        {
            Transport = transport,
            DataSource = dbPath,
        };
    }

    return new CSharpDbClientOptions
    {
        Transport = transport,
        ConnectionString = sp.GetRequiredService<IConfiguration>().GetConnectionString("CSharpDB")
            ?? "Data Source=csharpdb.db",
    };
});

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
await using (var scope = host.Services.CreateAsyncScope())
{
    var dbClient = scope.ServiceProvider.GetRequiredService<ICSharpDbClient>();
    _ = await dbClient.GetInfoAsync();
}

await host.RunAsync();

static CSharpDbTransport? ParseTransport(string? value)
{
    if (string.IsNullOrWhiteSpace(value))
        return null;

    return value.Trim().ToLowerInvariant() switch
    {
        "direct" => CSharpDbTransport.Direct,
        "http" => CSharpDbTransport.Http,
        "grpc" => CSharpDbTransport.Grpc,
        "tcp" => CSharpDbTransport.Tcp,
        "namedpipes" => CSharpDbTransport.NamedPipes,
        "named-pipes" => CSharpDbTransport.NamedPipes,
        "npipe" => CSharpDbTransport.NamedPipes,
        "pipe" => CSharpDbTransport.NamedPipes,
        _ => throw new InvalidOperationException($"Unsupported transport '{value}'."),
    };
}
