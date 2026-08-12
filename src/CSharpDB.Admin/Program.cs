using CSharpDB.Admin.Configuration;
using CSharpDB.Admin.Components;
using CSharpDB.Admin.Components.Samples.FormControls;
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Admin.ImportExport.Services;
using CSharpDB.Admin.Reports.Services;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.CodeModules;
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
    var readiness = sp.GetRequiredService<AdminHostReadinessService>();
    string? endpoint = configuration["CSharpDB:Endpoint"];
    CSharpDbTransport? transport = ParseTransport(configuration["CSharpDB:Transport"]);

    CSharpDbClientOptions options = AdminClientOptionsBuilder.Build(
        configuration,
        hostDatabaseOptions,
        transport,
        endpoint,
        functions);

    if (CSharpDbShardedClient.TryCreateFromMasterCatalog(options) is { } shardedClient)
        return new DatabaseClientHolder(shardedClient, shardedClient, null, hostDatabaseOptions, functions, readiness);

    ICSharpDbClient client = CSharpDbClient.Create(options);
    ICSharpDbShardAdminClient? shardAdmin = TryCreateShardAdmin(options);
    return new DatabaseClientHolder(client, shardAdmin, options, hostDatabaseOptions, functions, readiness);
});
builder.Services.AddSingleton<ICSharpDbClient>(sp => sp.GetRequiredService<DatabaseClientHolder>());
builder.Services.AddSingleton<ICSharpDbShardAdminClient>(sp => sp.GetRequiredService<DatabaseClientHolder>());
builder.Services.AddSingleton<ICSharpDbShardDirectoryClient>(sp => sp.GetRequiredService<DatabaseClientHolder>());
builder.Services.AddSingleton(sp =>
{
    CSharpDB.Observability.CSharpDbHealthOptions options = sp
        .GetRequiredService<IConfiguration>()
        .GetSection(
            CSharpDB.Observability.CSharpDbObservabilityOptions.ConfigurationSectionName +
            ":Health")
        .Get<CSharpDB.Observability.CSharpDbHealthOptions>() ?? new();
    new CSharpDB.Observability.CSharpDbObservabilityOptions
    {
        Health = options,
    }.Validate();
    return options;
});
builder.Services.AddSingleton<CSharpDB.Observability.CSharpDbHostState>();
builder.Services.AddSingleton<AdminHostReadinessService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<AdminHostReadinessService>());
builder.Services.AddScoped<TabManagerService>();
builder.Services.AddScoped<ThemeService>();
builder.Services.AddScoped<ToastService>();
builder.Services.AddScoped<ModalService>();
builder.Services.AddScoped<DatabaseChangeService>();
builder.Services.AddScoped<DataHygieneAdminService>();
builder.Services.AddScoped<CompareDeployAdminService>();
builder.Services.AddScoped<HostCallbackCatalogService>();
builder.Services.AddScoped<HostCallbackPolicyService>();
builder.Services.AddScoped<HostCallbackReadinessService>();
builder.Services.AddScoped<DataModelService>();
builder.Services.AddScoped<IDataModelService>(sp => sp.GetRequiredService<DataModelService>());
builder.Services.AddScoped<IDataModelDiagramService>(sp => sp.GetRequiredService<DataModelService>());
builder.Services.AddSingleton<HostCallbackDiagnosticsHistoryService>();
builder.Services.AddCSharpDbCodeModules(options => options.EnableInProcessExecution = true);
builder.Services.AddCSharpDbAdminForms();
builder.Services.AddCSharpDbAdminFormCodeModules();
if (builder.Configuration.GetValue<bool>("AdminForms:EnableSampleControls"))
    builder.Services.AddSampleFormControls();
builder.Services.AddCSharpDbAdminImportExport();
builder.Services.AddCSharpDbAdminReports();

var app = builder.Build();
_ = app.Services.GetRequiredService<HostCallbackDiagnosticsHistoryService>();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/error");
}

app.MapStaticAssets();
app.UseStaticFiles();
app.UseAntiforgery();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();
app.MapCSharpDbAdminImportExport();
app.MapCSharpDbDesktopShellEndpoints();

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

static ICSharpDbShardAdminClient? TryCreateShardAdmin(CSharpDbClientOptions options)
{
    try
    {
        return CSharpDbClient.CreateShardAdmin(options);
    }
    catch (CSharpDbClientConfigurationException)
    {
        return null;
    }
}

public partial class Program;
