using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Reports.Contracts;
using CSharpDB.Admin.Reports.Models;
using CSharpDB.Admin.Services;
using CSharpDB.Primitives;
using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.Admin.Forms.Tests.Admin;

public class HostCallbackCatalogServiceTests
{
    [Fact]
    public void GetCallbacks_ReturnsRegisteredFunctionsAndCommandsInStableOrder()
    {
        DbFunctionRegistry functions = DbFunctionRegistry.Create(builder =>
            builder.AddScalar(
                "normalize_name",
                1,
                new DbScalarFunctionOptions(
                    ReturnType: DbType.Text,
                    IsDeterministic: true,
                    Description: "Normalize a display name."),
                static (_, _) => DbValue.Null));

        DbCommandRegistry commands = DbCommandRegistry.Create(builder =>
            builder.AddCommand(
                "refresh_cache",
                new DbCommandOptions(
                    Description: "Refresh cached projections.",
                    Timeout: TimeSpan.FromSeconds(2),
                    AdditionalCapabilities:
                    [
                        new DbExtensionCapabilityRequest(DbExtensionCapability.ReadDatabase, Reason: "Read source rows.")
                    ]),
                static _ => DbCommandResult.Success()));

        using ServiceProvider provider = new ServiceCollection()
            .AddSingleton(functions)
            .AddSingleton(commands)
            .AddScoped<HostCallbackCatalogService>()
            .BuildServiceProvider();

        HostCallbackCatalogService catalog = provider.GetRequiredService<HostCallbackCatalogService>();

        IReadOnlyList<DbHostCallbackDescriptor> callbacks = catalog.GetCallbacks();

        Assert.Collection(
            callbacks,
            scalar =>
            {
                Assert.Equal(AutomationCallbackKind.ScalarFunction, scalar.Kind);
                Assert.Equal("normalize_name", scalar.Name);
                Assert.Equal(1, scalar.Arity);
                Assert.Equal(DbType.Text, scalar.ReturnType);
                Assert.True(scalar.IsDeterministic);
                Assert.Contains(scalar.Capabilities, capability =>
                    capability.Name == DbExtensionCapability.ScalarFunctions
                    && capability.Exports is not null
                    && capability.Exports.Contains("normalize_name"));
            },
            command =>
            {
                Assert.Equal(AutomationCallbackKind.Command, command.Kind);
                Assert.Equal("refresh_cache", command.Name);
                Assert.Equal(TimeSpan.FromSeconds(2), command.Timeout);
                Assert.Contains(command.Capabilities, capability => capability.Name == DbExtensionCapability.Commands);
                Assert.Contains(command.Capabilities, capability => capability.Name == DbExtensionCapability.ReadDatabase);
            });
    }

    [Fact]
    public void GetCallbacks_AdminHostDefaultsExposeScalarAndCommandCallbacks()
    {
        using ServiceProvider provider = new ServiceCollection()
            .AddSingleton(AdminHostCallbacks.CreateFunctionRegistry())
            .AddSingleton(AdminHostCallbacks.CreateCommandRegistry())
            .AddScoped<HostCallbackCatalogService>()
            .BuildServiceProvider();

        HostCallbackCatalogService catalog = provider.GetRequiredService<HostCallbackCatalogService>();

        IReadOnlyList<DbHostCallbackDescriptor> callbacks = catalog.GetCallbacks();

        DbHostCallbackDescriptor slugify = Assert.Single(
            callbacks,
            callback => callback.Kind == AutomationCallbackKind.ScalarFunction
                && callback.Name == "Slugify");
        Assert.Equal(DbExtensionRuntimeKind.HostCallback, slugify.Runtime);
        Assert.Equal(DbType.Text, slugify.ReturnType);
        Assert.True(slugify.IsDeterministic);
        Assert.True(slugify.NullPropagating);

        DbHostCallbackDescriptor echo = Assert.Single(
            callbacks,
            callback => callback.Kind == AutomationCallbackKind.Command
                && callback.Name == "EchoAutomationEvent");
        Assert.Equal(DbExtensionRuntimeKind.HostCallback, echo.Runtime);
        Assert.Contains(echo.Capabilities, capability => capability.Name == DbExtensionCapability.Commands);
    }

    [Fact]
    public void GetCallbacks_WhenRegistriesAreMissing_ReturnsEmptyList()
    {
        using ServiceProvider provider = new ServiceCollection().BuildServiceProvider();
        var catalog = new HostCallbackCatalogService(provider);

        Assert.Empty(catalog.GetCallbacks());
    }

    [Fact]
    public async Task GetEntriesAsync_ReturnsRegisteredAndReferencedCallbacks()
    {
        DbFunctionRegistry functions = DbFunctionRegistry.Create(builder =>
            builder.AddScalar("Slugify", 1, (_, _) => DbValue.Null));
        DbCommandRegistry commands = DbCommandRegistry.Create(builder =>
            builder.AddCommand("EchoAutomationEvent", _ => DbCommandResult.Success()));

        using ServiceProvider provider = new ServiceCollection()
            .AddSingleton(functions)
            .AddSingleton(commands)
            .AddSingleton<IFormRepository>(new StubFormRepository(
            [
                CreateForm("orders-form", "Orders") with
                {
                    Automation = new DbAutomationMetadata(
                        Commands:
                        [
                            new DbAutomationCommandReference("EchoAutomationEvent", "admin.forms", "form.events.OnLoad"),
                            new DbAutomationCommandReference("MissingFormCommand", "admin.forms", "controls.submit.commandButton.click"),
                        ],
                        ScalarFunctions:
                        [
                            new DbAutomationScalarFunctionReference("Slugify", 1, "admin.forms", "controls.slug.formula"),
                        ]),
                },
            ]))
            .AddSingleton<IReportRepository>(new StubReportRepository(
            [
                CreateReport("orders-report", "Orders") with
                {
                    Automation = new DbAutomationMetadata(
                        ScalarFunctions:
                        [
                            new DbAutomationScalarFunctionReference("MissingReportFunction", 2, "admin.reports", "bands.detail.controls.total.expression"),
                        ]),
                },
            ]))
            .AddScoped<HostCallbackCatalogService>()
            .BuildServiceProvider();

        HostCallbackCatalogService catalog = provider.GetRequiredService<HostCallbackCatalogService>();

        IReadOnlyList<HostCallbackCatalogEntry> entries = await catalog.GetEntriesAsync();

        HostCallbackCatalogEntry missingCommand = Assert.Single(entries, entry => entry.Name == "MissingFormCommand");
        Assert.True(missingCommand.IsMissingRegistration);
        Assert.Equal(AutomationCallbackKind.Command, missingCommand.Kind);
        HostCallbackReference commandReference = Assert.Single(missingCommand.References);
        Assert.Equal("Form", commandReference.OwnerKind);
        Assert.Equal("orders-form", commandReference.OwnerId);
        Assert.Equal("controls.submit.commandButton.click", commandReference.Location);

        HostCallbackCatalogEntry missingFunction = Assert.Single(entries, entry => entry.Name == "MissingReportFunction");
        Assert.True(missingFunction.IsMissingRegistration);
        Assert.Equal(2, missingFunction.Arity);
        HostCallbackReference functionReference = Assert.Single(missingFunction.References);
        Assert.Equal("Report", functionReference.OwnerKind);
        Assert.Equal("bands.detail.controls.total.expression", functionReference.Location);

        HostCallbackCatalogEntry registeredFunction = Assert.Single(entries, entry => entry.Name == "Slugify");
        Assert.True(registeredFunction.IsRegistered);
        Assert.False(registeredFunction.IsMissingRegistration);
        Assert.Single(registeredFunction.References);

        HostCallbackCatalogEntry registeredCommand = Assert.Single(entries, entry => entry.Name == "EchoAutomationEvent");
        Assert.True(registeredCommand.IsRegistered);
        Assert.False(registeredCommand.IsMissingRegistration);
        Assert.Single(registeredCommand.References);
    }

    [Fact]
    public async Task GetEntriesAsync_DeduplicatesRepeatedReferences()
    {
        using ServiceProvider provider = new ServiceCollection()
            .AddSingleton(DbFunctionRegistry.Empty)
            .AddSingleton(DbCommandRegistry.Empty)
            .AddSingleton<IFormRepository>(new StubFormRepository(
            [
                CreateForm("orders-form", "Orders") with
                {
                    Automation = new DbAutomationMetadata(
                        Commands:
                        [
                            new DbAutomationCommandReference("AuditOrder", "admin.forms", "form.events.OnLoad"),
                            new DbAutomationCommandReference("AuditOrder", "admin.forms", "form.events.OnLoad"),
                        ]),
                },
            ]))
            .AddScoped<HostCallbackCatalogService>()
            .BuildServiceProvider();

        HostCallbackCatalogService catalog = provider.GetRequiredService<HostCallbackCatalogService>();

        HostCallbackCatalogEntry entry = Assert.Single(await catalog.GetEntriesAsync());

        Assert.Equal("AuditOrder", entry.Name);
        Assert.True(entry.IsMissingRegistration);
        Assert.Single(entry.References);
    }

    private static FormDefinition CreateForm(string formId, string tableName)
        => new(
            formId,
            $"{tableName} Form",
            tableName,
            1,
            $"sig:{tableName}:v1",
            new LayoutDefinition("absolute", 8, true, [new Breakpoint("md", 0, null)]),
            []);

    private static ReportDefinition CreateReport(string reportId, string sourceName)
        => new(
            reportId,
            $"{sourceName} Report",
            new ReportSourceReference(ReportSourceKind.Table, sourceName),
            1,
            $"sig:{sourceName}:v1",
            ReportPageSettings.DefaultLetterPortrait,
            [],
            [],
            [new ReportBandDefinition("detail", ReportBandKind.Detail, 28, null, [])]);

    private sealed class StubFormRepository : IFormRepository
    {
        private readonly IReadOnlyList<FormDefinition> _forms;

        public StubFormRepository(IReadOnlyList<FormDefinition> forms)
        {
            _forms = forms;
        }

        public Task<FormDefinition?> GetAsync(string formId) => throw new NotSupportedException();
        public Task<FormDefinition> CreateAsync(FormDefinition form) => throw new NotSupportedException();
        public Task<UpdateResult> TryUpdateAsync(string formId, int expectedVersion, FormDefinition updated) => throw new NotSupportedException();
        public Task<IReadOnlyList<FormDefinition>> ListAsync(string? tableName = null) => Task.FromResult(_forms);
        public Task<bool> DeleteAsync(string formId) => throw new NotSupportedException();
    }

    private sealed class StubReportRepository : IReportRepository
    {
        private readonly IReadOnlyList<ReportDefinition> _reports;

        public StubReportRepository(IReadOnlyList<ReportDefinition> reports)
        {
            _reports = reports;
        }

        public Task<ReportDefinition?> GetAsync(string reportId) => throw new NotSupportedException();
        public Task<ReportDefinition> CreateAsync(ReportDefinition report) => throw new NotSupportedException();
        public Task<ReportUpdateResult> TryUpdateAsync(string reportId, int expectedVersion, ReportDefinition updated) => throw new NotSupportedException();
        public Task<IReadOnlyList<ReportDefinition>> ListAsync(ReportSourceKind? sourceKind = null, string? sourceName = null) => Task.FromResult(_reports);
        public Task<bool> DeleteAsync(string reportId) => throw new NotSupportedException();
    }
}
