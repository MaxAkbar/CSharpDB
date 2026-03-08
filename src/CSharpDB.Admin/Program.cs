using CSharpDB.Admin.Components;
using CSharpDB.Admin.Services;
using CSharpDB.Client;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.AddCSharpDbClient(sp => new CSharpDbClientOptions
{
    ConnectionString = sp.GetRequiredService<IConfiguration>().GetConnectionString("CSharpDB")
        ?? "Data Source=csharpdb.db",
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

app.UseStaticFiles();
app.UseAntiforgery();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();
