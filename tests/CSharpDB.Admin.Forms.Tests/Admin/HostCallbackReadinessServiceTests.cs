using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Reports.Contracts;
using CSharpDB.Admin.Reports.Models;
using CSharpDB.Admin.Services;
using CSharpDB.Primitives;
using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.Admin.Forms.Tests.Admin;

public sealed class HostCallbackReadinessServiceTests
{
    [Fact]
    public async Task GetReadinessAsync_ReturnsMissingReferencedCallbacks()
    {
        using ServiceProvider provider = CreateProvider(
            functions: DbFunctionRegistry.Empty,
            commands: DbCommandRegistry.Empty,
            forms:
            [
                CreateForm("orders-form", "Orders") with
                {
                    Automation = new DbAutomationMetadata(
                        Commands:
                        [
                            new DbAutomationCommandReference("AuditOrder", "admin.forms", "form.events.OnLoad"),
                        ]),
                },
            ],
            reports:
            [
                CreateReport("orders-report", "Orders") with
                {
                    Automation = new DbAutomationMetadata(
                        ScalarFunctions:
                        [
                            new DbAutomationScalarFunctionReference("FormatTotal", 1, "admin.reports", "bands.detail.controls.total.expression"),
                        ]),
                },
            ]);

        HostCallbackReadinessService readiness = provider.GetRequiredService<HostCallbackReadinessService>();

        HostCallbackReadinessReport report = await readiness.GetReadinessAsync();

        Assert.False(report.Ready);
        Assert.Equal(0, report.RegisteredCount);
        Assert.Equal(2, report.ReferencedCount);
        Assert.Equal(2, report.MissingCount);
        Assert.Contains(report.MissingEntries, entry => entry.Name == "AuditOrder" && entry.Kind == AutomationCallbackKind.Command);
        Assert.Contains(report.MissingEntries, entry => entry.Name == "FormatTotal" && entry.Kind == AutomationCallbackKind.ScalarFunction);
    }

    [Fact]
    public async Task GetReadinessAsync_IsReadyWhenReferencedCallbacksAreRegistered()
    {
        DbFunctionRegistry functions = DbFunctionRegistry.Create(builder =>
            builder.AddScalar("FormatTotal", 1, (_, _) => DbValue.Null));
        DbCommandRegistry commands = DbCommandRegistry.Create(builder =>
            builder.AddCommand("AuditOrder", _ => DbCommandResult.Success()));

        using ServiceProvider provider = CreateProvider(
            functions,
            commands,
            forms:
            [
                CreateForm("orders-form", "Orders") with
                {
                    Automation = new DbAutomationMetadata(
                        Commands:
                        [
                            new DbAutomationCommandReference("AuditOrder", "admin.forms", "form.events.OnLoad"),
                        ],
                        ScalarFunctions:
                        [
                            new DbAutomationScalarFunctionReference("FormatTotal", 1, "admin.forms", "controls.total.formula"),
                        ]),
                },
            ],
            reports: []);

        HostCallbackReadinessService readiness = provider.GetRequiredService<HostCallbackReadinessService>();

        HostCallbackReadinessReport report = await readiness.GetReadinessAsync();

        Assert.True(report.Ready);
        Assert.Equal(2, report.RegisteredCount);
        Assert.Equal(2, report.ReferencedCount);
        Assert.Empty(report.MissingEntries);
    }

    [Fact]
    public async Task GenerateMissingStubSourceAsync_ProducesCSharpForMissingReferencesOnly()
    {
        DbFunctionRegistry functions = DbFunctionRegistry.Create(builder =>
            builder.AddScalar("RegisteredFunction", 1, (_, _) => DbValue.Null));

        using ServiceProvider provider = CreateProvider(
            functions,
            DbCommandRegistry.Empty,
            forms:
            [
                CreateForm("orders-form", "Orders") with
                {
                    Automation = new DbAutomationMetadata(
                        Commands:
                        [
                            new DbAutomationCommandReference("AuditOrder", "admin.forms", "form.events.OnLoad"),
                        ],
                        ScalarFunctions:
                        [
                            new DbAutomationScalarFunctionReference("RegisteredFunction", 1, "admin.forms", "controls.slug.formula"),
                            new DbAutomationScalarFunctionReference("MissingFunction", 2, "admin.forms", "controls.total.formula"),
                        ]),
                },
            ],
            reports: []);

        HostCallbackReadinessService readiness = provider.GetRequiredService<HostCallbackReadinessService>();

        string source = await readiness.GenerateMissingStubSourceAsync();

        Assert.Contains("class MissingHostCallbackRegistration", source);
        Assert.Contains("commands.AddAsyncCommand", source);
        Assert.Contains("\"AuditOrder\"", source);
        Assert.Contains("functions.AddScalar", source);
        Assert.Contains("\"MissingFunction\"", source);
        Assert.Contains("Form Orders Form (orders-form): form.events.OnLoad", source);
        Assert.DoesNotContain("\"RegisteredFunction\"", source);
    }

    [Fact]
    public void GenerateStubSource_ProducesSourceForSelectedEntry()
    {
        var entry = new HostCallbackCatalogEntry(
            AutomationCallbackKind.Command,
            "Send_Invoice",
            Arity: null,
            Descriptor: null,
            References:
            [
                new HostCallbackReference(
                    AutomationCallbackKind.Command,
                    "Send_Invoice",
                    Arity: null,
                    "admin.forms",
                    "controls.send.commandButton.click",
                    "Form",
                    "invoice-form",
                    "Invoice Form"),
            ]);
        var readiness = new HostCallbackReadinessService(new HostCallbackCatalogService(new ServiceCollection().BuildServiceProvider()));

        string source = readiness.GenerateStubSource(entry);

        Assert.Contains("class RegisterSendInvoiceCallback", source);
        Assert.Contains("\"Send_Invoice\"", source);
        Assert.Contains("Form Invoice Form (invoice-form): controls.send.commandButton.click", source);
    }

    private static ServiceProvider CreateProvider(
        DbFunctionRegistry functions,
        DbCommandRegistry commands,
        IReadOnlyList<FormDefinition> forms,
        IReadOnlyList<ReportDefinition> reports)
        => new ServiceCollection()
            .AddSingleton(functions)
            .AddSingleton(commands)
            .AddSingleton<IFormRepository>(new StubFormRepository(forms))
            .AddSingleton<IReportRepository>(new StubReportRepository(reports))
            .AddScoped<HostCallbackCatalogService>()
            .AddScoped<HostCallbackReadinessService>()
            .BuildServiceProvider();

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
