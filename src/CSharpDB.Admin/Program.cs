using CSharpDB.Admin.Configuration;
using CSharpDB.Admin.Components;
using CSharpDB.Admin.Components.Samples.FormControls;
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Admin.Reports.Services;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.Primitives;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.AddSingleton(sp =>
    AdminClientOptionsBuilder.BindHostDatabaseOptions(sp.GetRequiredService<IConfiguration>()));
builder.Services.AddSingleton(AdminHostCallbacks.CreateFunctionRegistry());
builder.Services.AddSingleton(AdminHostCallbacks.CreateCommandRegistry());
builder.Services.AddSingleton(AdminHostCallbacks.CreatePolicy());
builder.Services.AddSingleton<DatabaseClientHolder>(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    var hostDatabaseOptions = sp.GetRequiredService<AdminHostDatabaseOptions>();
    var functions = sp.GetRequiredService<DbFunctionRegistry>();
    string? endpoint = configuration["CSharpDB:Endpoint"];
    CSharpDbTransport? transport = ParseTransport(configuration["CSharpDB:Transport"]);

    CSharpDbClientOptions options = AdminClientOptionsBuilder.Build(
        configuration,
        hostDatabaseOptions,
        transport,
        endpoint,
        functions);

    return new DatabaseClientHolder(CSharpDbClient.Create(options), hostDatabaseOptions, functions);
});
builder.Services.AddSingleton<ICSharpDbClient>(sp => sp.GetRequiredService<DatabaseClientHolder>());
builder.Services.AddScoped<TabManagerService>();
builder.Services.AddScoped<ThemeService>();
builder.Services.AddScoped<ToastService>();
builder.Services.AddScoped<ModalService>();
builder.Services.AddScoped<DatabaseChangeService>();
builder.Services.AddScoped<HostCallbackCatalogService>();
builder.Services.AddScoped<HostCallbackPolicyService>();
builder.Services.AddScoped<HostCallbackReadinessService>();
builder.Services.AddCSharpDbAdminForms();
if (builder.Configuration.GetValue<bool>("AdminForms:EnableSampleControls"))
    builder.Services.AddSampleFormControls();
builder.Services.AddCSharpDbAdminReports();

var app = builder.Build();

// Warm the in-process database instance before any requests arrive.
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
