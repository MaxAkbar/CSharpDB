using CSharpDB.Admin.Forms.Services;
using CSharpDB.Admin.Forms.Web.Components;
using CSharpDB.Admin.Forms.Web.Configuration;
using CSharpDB.Client;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.UseStaticWebAssets();

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.AddSingleton(sp =>
    FormsHostClientOptionsBuilder.BindHostDatabaseOptions(sp.GetRequiredService<IConfiguration>()));
builder.Services.AddSingleton<ICSharpDbClient>(sp =>
{
    IConfiguration configuration = sp.GetRequiredService<IConfiguration>();
    FormsHostDatabaseOptions hostDatabaseOptions = sp.GetRequiredService<FormsHostDatabaseOptions>();
    return CSharpDbClient.Create(FormsHostClientOptionsBuilder.Build(configuration, hostDatabaseOptions));
});
builder.Services.AddCSharpDbAdminForms();

var app = builder.Build();

await using (var scope = app.Services.CreateAsyncScope())
{
    var dbClient = scope.ServiceProvider.GetRequiredService<ICSharpDbClient>();
    _ = await dbClient.GetInfoAsync();
}

if (!app.Environment.IsDevelopment())
    app.UseExceptionHandler("/error");

app.MapStaticAssets();
app.UseStaticFiles();
app.UseAntiforgery();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();
