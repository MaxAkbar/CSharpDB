using CSharpDB.Admin.Components;
using CSharpDB.Admin.Services;
using CSharpDB.Client;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.AddCSharpDbClient(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    string? endpoint = configuration["CSharpDB:Endpoint"];
    CSharpDbTransport? transport = ParseTransport(configuration["CSharpDB:Transport"]);

    if (!string.IsNullOrWhiteSpace(endpoint))
    {
        return new CSharpDbClientOptions
        {
            Transport = transport,
            Endpoint = endpoint,
        };
    }

    return new CSharpDbClientOptions
    {
        Transport = transport,
        ConnectionString = configuration.GetConnectionString("CSharpDB")
            ?? "Data Source=csharpdb.db",
    };
});
builder.Services.AddScoped<TabManagerService>();
builder.Services.AddScoped<ThemeService>();
builder.Services.AddScoped<ToastService>();
builder.Services.AddScoped<ModalService>();
builder.Services.AddScoped<DatabaseChangeService>();

var app = builder.Build();

// Open the database connection at startup (before any requests arrive)
await using (var scope = app.Services.CreateAsyncScope())
{
    var dbClient = scope.ServiceProvider.GetRequiredService<ICSharpDbClient>();
    _ = await dbClient.GetInfoAsync();
}

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/error");
}

app.MapStaticAssets();
app.UseStaticFiles();
app.UseAntiforgery();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();

static CSharpDbTransport? ParseTransport(string? value)
{
    if (string.IsNullOrWhiteSpace(value))
        return null;

    return value.Trim().ToLowerInvariant() switch
    {
        "direct" => CSharpDbTransport.Direct,
        "http" => CSharpDbTransport.Http,
        "grpc" => CSharpDbTransport.Grpc,
        "namedpipes" => CSharpDbTransport.NamedPipes,
        "named-pipes" => CSharpDbTransport.NamedPipes,
        "npipe" => CSharpDbTransport.NamedPipes,
        "pipe" => CSharpDbTransport.NamedPipes,
        _ => throw new InvalidOperationException($"Unsupported transport '{value}'."),
    };
}
