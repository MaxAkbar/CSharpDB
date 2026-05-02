using System.Text.Json;
using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Reports.Contracts;
using CSharpDB.Admin.Reports.Models;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.Primitives;
using Microsoft.Extensions.DependencyInjection;
using FormPropertyBag = CSharpDB.Admin.Forms.Models.PropertyBag;
using FormRect = CSharpDB.Admin.Forms.Models.Rect;
using ReportPropertyBag = CSharpDB.Admin.Reports.Models.PropertyBag;
using ReportRect = CSharpDB.Admin.Reports.Models.Rect;

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
    public void GetCallbacks_IncludesRegisteredValidationRules()
    {
        DbValidationRuleRegistry validationRules = DbValidationRuleRegistry.Create(builder =>
            builder.AddRule(
                "CreditLimit",
                new DbValidationRuleOptions(Description: "Checks customer credit limit."),
                static (_, _) => ValueTask.FromResult(DbValidationRuleResult.Success())));

        using ServiceProvider provider = new ServiceCollection()
            .AddSingleton(validationRules)
            .AddScoped<HostCallbackCatalogService>()
            .BuildServiceProvider();

        HostCallbackCatalogService catalog = provider.GetRequiredService<HostCallbackCatalogService>();

        DbHostCallbackDescriptor callback = Assert.Single(catalog.GetCallbacks());

        Assert.Equal(AutomationCallbackKind.ValidationRule, callback.Kind);
        Assert.Equal("CreditLimit", callback.Name);
        Assert.Contains(callback.Capabilities, capability =>
            capability.Name == DbExtensionCapability.ValidationRules
            && capability.Exports is not null
            && capability.Exports.Contains("CreditLimit"));
    }

    [Fact]
    public async Task GetEntriesAsync_ReturnsRegisteredAndReferencedCallbacks()
    {
        DbFunctionRegistry functions = DbFunctionRegistry.Create(builder =>
            builder.AddScalar("Slugify", 1, (_, _) => DbValue.Null));
        DbCommandRegistry commands = DbCommandRegistry.Create(builder =>
            builder.AddCommand("EchoAutomationEvent", _ => DbCommandResult.Success()));
        DbValidationRuleRegistry validationRules = DbValidationRuleRegistry.Create(builder =>
            builder.AddRule("CreditLimit", static (_, _) => ValueTask.FromResult(DbValidationRuleResult.Success())));

        using ServiceProvider provider = new ServiceCollection()
            .AddSingleton(functions)
            .AddSingleton(commands)
            .AddSingleton(validationRules)
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
                        ],
                        ValidationRules:
                        [
                            new DbAutomationValidationRuleReference("CreditLimit", "admin.forms", "controls.credit.validationRules.CreditLimit"),
                            new DbAutomationValidationRuleReference("MissingValidationRule", "admin.forms", "form.validationRules.MissingValidationRule"),
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

        HostCallbackCatalogEntry registeredValidationRule = Assert.Single(entries, entry => entry.Name == "CreditLimit");
        Assert.Equal(AutomationCallbackKind.ValidationRule, registeredValidationRule.Kind);
        Assert.True(registeredValidationRule.IsRegistered);
        Assert.False(registeredValidationRule.IsMissingRegistration);
        Assert.Equal("controls.credit.validationRules.CreditLimit", Assert.Single(registeredValidationRule.References).Location);

        HostCallbackCatalogEntry missingValidationRule = Assert.Single(entries, entry => entry.Name == "MissingValidationRule");
        Assert.Equal(AutomationCallbackKind.ValidationRule, missingValidationRule.Kind);
        Assert.True(missingValidationRule.IsMissingRegistration);
        Assert.Equal("form.validationRules.MissingValidationRule", Assert.Single(missingValidationRule.References).Location);
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

    [Fact]
    public async Task GetEntriesAsync_RebuildsFormAndReportReferencesWhenAutomationMetadataIsMissing()
    {
        FormDefinition form = CreateForm("orders-form", "Orders") with
        {
            Controls =
            [
                new ControlDefinition(
                    "slug",
                    "computed",
                    new FormRect(0, 0, 120, 24),
                    null,
                    new FormPropertyBag(new Dictionary<string, object?>
                    {
                        ["formula"] = "Slugify(Name)",
                    }),
                    null),
                new ControlDefinition(
                    "send",
                    "commandButton",
                    new FormRect(0, 32, 120, 24),
                    null,
                    new FormPropertyBag(new Dictionary<string, object?>
                    {
                        ["commandName"] = "SendReceipt",
                    }),
                    null),
            ],
        };

        ReportDefinition report = CreateReport("orders-report", "Orders") with
        {
            Bands =
            [
                new ReportBandDefinition(
                    "detail",
                    ReportBandKind.Detail,
                    28,
                    GroupId: null,
                    Controls:
                    [
                        new ReportControlDefinition(
                            "risk",
                            ReportControlType.CalculatedText,
                            "detail",
                            new ReportRect(0, 0, 120, 20),
                            null,
                            "RiskScore(Total, Region)",
                            null,
                            ReportPropertyBag.Empty),
                    ]),
            ],
        };

        using ServiceProvider provider = new ServiceCollection()
            .AddSingleton(DbFunctionRegistry.Empty)
            .AddSingleton(DbCommandRegistry.Empty)
            .AddSingleton<IFormRepository>(new StubFormRepository([form]))
            .AddSingleton<IReportRepository>(new StubReportRepository([report]))
            .AddScoped<HostCallbackCatalogService>()
            .BuildServiceProvider();

        HostCallbackCatalogService catalog = provider.GetRequiredService<HostCallbackCatalogService>();

        IReadOnlyList<HostCallbackCatalogEntry> entries = await catalog.GetEntriesAsync();

        HostCallbackCatalogEntry slugify = Assert.Single(entries, entry => entry.Name == "Slugify");
        Assert.Equal(AutomationCallbackKind.ScalarFunction, slugify.Kind);
        Assert.Equal("Form", Assert.Single(slugify.References).OwnerKind);

        HostCallbackCatalogEntry sendReceipt = Assert.Single(entries, entry => entry.Name == "SendReceipt");
        Assert.Equal(AutomationCallbackKind.Command, sendReceipt.Kind);
        Assert.Equal("controls.send.commandButton.click", Assert.Single(sendReceipt.References).Location);

        HostCallbackCatalogEntry riskScore = Assert.Single(entries, entry => entry.Name == "RiskScore");
        Assert.Equal(2, riskScore.Arity);
        Assert.Equal("Report", Assert.Single(riskScore.References).OwnerKind);
    }

    [Fact]
    public async Task GetEntriesAsync_DiscoversSavedQueryProcedureAndTriggerScalarReferences()
    {
        using ServiceProvider provider = new ServiceCollection()
            .AddSingleton(DbFunctionRegistry.Empty)
            .AddSingleton(DbCommandRegistry.Empty)
            .AddSingleton<ICSharpDbClient>(new StubDbClient(
                savedQueries:
                [
                    new CSharpDB.Client.Models.SavedQueryDefinition
                    {
                        Id = 12,
                        Name = "Customer Score Query",
                        SqlText = """
                            SELECT NormalizeName(Name), COUNT(*)
                            FROM Customers
                            WHERE Status IN ('Open', 'Ready')
                              AND EXISTS (SELECT 1 FROM Regions WHERE Regions.Id = Customers.RegionId)
                            GROUP BY NormalizeName(Name);
                            """,
                    },
                ],
                procedures:
                [
                    new CSharpDB.Client.Models.ProcedureDefinition
                    {
                        Name = "RefreshCustomerRisk",
                        BodySql = """
                            CREATE INDEX idx_ops_events_entity_date ON ops_events (entity_type, event_date);
                            INSERT INTO ops_events (entity_type, entity_id, event_type)
                            VALUES ('customer', @customerId, 'risk-refreshed');
                            UPDATE Customers SET Risk = RiskScore(Total, Region);
                            """,
                    },
                ],
                triggers:
                [
                    new CSharpDB.Client.Models.TriggerSchema
                    {
                        TriggerName = "Customers_Audit",
                        TableName = "Customers",
                        Timing = CSharpDB.Client.Models.TriggerTiming.After,
                        Event = CSharpDB.Client.Models.TriggerEvent.Update,
                        BodySql = """
                            INSERT INTO Audit(Value) VALUES(AuditScore(new.Risk));
                            INSERT INTO ops_events (entity_type, entity_id, event_type)
                            VALUES ('customer', new.Id, 'updated');
                            """,
                    },
                ]))
            .AddScoped<HostCallbackCatalogService>()
            .BuildServiceProvider();

        HostCallbackCatalogService catalog = provider.GetRequiredService<HostCallbackCatalogService>();

        IReadOnlyList<HostCallbackCatalogEntry> entries = await catalog.GetEntriesAsync();

        HostCallbackCatalogEntry normalizeName = Assert.Single(entries, entry => entry.Name == "NormalizeName");
        Assert.Equal(1, normalizeName.Arity);
        HostCallbackReference savedQueryReference = Assert.Single(normalizeName.References);
        Assert.Equal("SavedQuery", savedQueryReference.OwnerKind);
        Assert.Equal("12", savedQueryReference.OwnerId);
        Assert.Equal("savedQueries", savedQueryReference.Surface);

        HostCallbackCatalogEntry riskScore = Assert.Single(entries, entry => entry.Name == "RiskScore");
        Assert.Equal(2, riskScore.Arity);
        Assert.Equal("Procedure", Assert.Single(riskScore.References).OwnerKind);

        HostCallbackCatalogEntry auditScore = Assert.Single(entries, entry => entry.Name == "AuditScore");
        Assert.Equal(1, auditScore.Arity);
        Assert.Equal("Trigger", Assert.Single(auditScore.References).OwnerKind);

        Assert.DoesNotContain(entries, entry => entry.Name == "COUNT");
        Assert.DoesNotContain(entries, entry => entry.Name == "EXISTS");
        Assert.DoesNotContain(entries, entry => entry.Name == "IN");
        Assert.DoesNotContain(entries, entry => entry.Name == "INTO");
        Assert.DoesNotContain(entries, entry => entry.Name == "Audit");
        Assert.DoesNotContain(entries, entry => entry.Name == "ops_events");
        Assert.DoesNotContain(entries, entry => entry.Name == "VALUES");
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

    private sealed class StubDbClient : ICSharpDbClient
    {
        private readonly IReadOnlyList<CSharpDB.Client.Models.SavedQueryDefinition> _savedQueries;
        private readonly IReadOnlyList<CSharpDB.Client.Models.ProcedureDefinition> _procedures;
        private readonly IReadOnlyList<CSharpDB.Client.Models.TriggerSchema> _triggers;
        private readonly IReadOnlyList<string> _tableNames;

        public StubDbClient(
            IReadOnlyList<CSharpDB.Client.Models.SavedQueryDefinition>? savedQueries = null,
            IReadOnlyList<CSharpDB.Client.Models.ProcedureDefinition>? procedures = null,
            IReadOnlyList<CSharpDB.Client.Models.TriggerSchema>? triggers = null,
            IReadOnlyList<string>? tableNames = null)
        {
            _savedQueries = savedQueries ?? [];
            _procedures = procedures ?? [];
            _triggers = triggers ?? [];
            _tableNames = tableNames ?? [];
        }

        public string DataSource => "stub";

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public Task<CSharpDB.Client.Models.DatabaseInfo> GetInfoAsync(CancellationToken ct = default) => throw new NotSupportedException();
        public Task<IReadOnlyList<string>> GetTableNamesAsync(CancellationToken ct = default) => Task.FromResult(_tableNames);
        public Task<CSharpDB.Client.Models.TableSchema?> GetTableSchemaAsync(string tableName, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<int> GetRowCountAsync(string tableName, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<CSharpDB.Client.Models.TableBrowseResult> BrowseTableAsync(string tableName, int page = 1, int pageSize = 50, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Dictionary<string, object?>?> GetRowByPkAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<int> InsertRowAsync(string tableName, Dictionary<string, object?> values, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<int> UpdateRowAsync(string tableName, string pkColumn, object pkValue, Dictionary<string, object?> values, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<int> DeleteRowAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default) => throw new NotSupportedException();
        public Task DropTableAsync(string tableName, CancellationToken ct = default) => throw new NotSupportedException();
        public Task RenameTableAsync(string tableName, string newTableName, CancellationToken ct = default) => throw new NotSupportedException();
        public Task AddColumnAsync(string tableName, string columnName, CSharpDB.Client.Models.DbType type, bool notNull, CancellationToken ct = default) => throw new NotSupportedException();
        public Task AddColumnAsync(string tableName, string columnName, CSharpDB.Client.Models.DbType type, bool notNull, string? collation, CancellationToken ct = default) => throw new NotSupportedException();
        public Task DropColumnAsync(string tableName, string columnName, CancellationToken ct = default) => throw new NotSupportedException();
        public Task RenameColumnAsync(string tableName, string oldColumnName, string newColumnName, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<IReadOnlyList<CSharpDB.Client.Models.IndexSchema>> GetIndexesAsync(CancellationToken ct = default) => throw new NotSupportedException();
        public Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default) => throw new NotSupportedException();
        public Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, string? collation, CancellationToken ct = default) => throw new NotSupportedException();
        public Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default) => throw new NotSupportedException();
        public Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique, string? collation, CancellationToken ct = default) => throw new NotSupportedException();
        public Task DropIndexAsync(string indexName, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<IReadOnlyList<string>> GetViewNamesAsync(CancellationToken ct = default) => throw new NotSupportedException();
        public Task<IReadOnlyList<CSharpDB.Client.Models.ViewDefinition>> GetViewsAsync(CancellationToken ct = default) => throw new NotSupportedException();
        public Task<CSharpDB.Client.Models.ViewDefinition?> GetViewAsync(string viewName, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<string?> GetViewSqlAsync(string viewName, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<CSharpDB.Client.Models.ViewBrowseResult> BrowseViewAsync(string viewName, int page = 1, int pageSize = 50, CancellationToken ct = default) => throw new NotSupportedException();
        public Task CreateViewAsync(string viewName, string selectSql, CancellationToken ct = default) => throw new NotSupportedException();
        public Task UpdateViewAsync(string existingViewName, string newViewName, string selectSql, CancellationToken ct = default) => throw new NotSupportedException();
        public Task DropViewAsync(string viewName, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<IReadOnlyList<CSharpDB.Client.Models.TriggerSchema>> GetTriggersAsync(CancellationToken ct = default) => Task.FromResult(_triggers);
        public Task CreateTriggerAsync(string triggerName, string tableName, CSharpDB.Client.Models.TriggerTiming timing, CSharpDB.Client.Models.TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default) => throw new NotSupportedException();
        public Task UpdateTriggerAsync(string existingTriggerName, string newTriggerName, string tableName, CSharpDB.Client.Models.TriggerTiming timing, CSharpDB.Client.Models.TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default) => throw new NotSupportedException();
        public Task DropTriggerAsync(string triggerName, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<IReadOnlyList<CSharpDB.Client.Models.SavedQueryDefinition>> GetSavedQueriesAsync(CancellationToken ct = default) => Task.FromResult(_savedQueries);
        public Task<CSharpDB.Client.Models.SavedQueryDefinition?> GetSavedQueryAsync(string name, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<CSharpDB.Client.Models.SavedQueryDefinition> UpsertSavedQueryAsync(string name, string sqlText, CancellationToken ct = default) => throw new NotSupportedException();
        public Task DeleteSavedQueryAsync(string name, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<IReadOnlyList<CSharpDB.Client.Models.ProcedureDefinition>> GetProceduresAsync(bool includeDisabled = true, CancellationToken ct = default) => Task.FromResult(_procedures);
        public Task<CSharpDB.Client.Models.ProcedureDefinition?> GetProcedureAsync(string name, CancellationToken ct = default) => throw new NotSupportedException();
        public Task CreateProcedureAsync(CSharpDB.Client.Models.ProcedureDefinition definition, CancellationToken ct = default) => throw new NotSupportedException();
        public Task UpdateProcedureAsync(string existingName, CSharpDB.Client.Models.ProcedureDefinition definition, CancellationToken ct = default) => throw new NotSupportedException();
        public Task DeleteProcedureAsync(string name, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<CSharpDB.Client.Models.ProcedureExecutionResult> ExecuteProcedureAsync(string name, IReadOnlyDictionary<string, object?> args, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<CSharpDB.Client.Models.SqlExecutionResult> ExecuteSqlAsync(string sql, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<CSharpDB.Client.Models.TransactionSessionInfo> BeginTransactionAsync(CancellationToken ct = default) => throw new NotSupportedException();
        public Task<CSharpDB.Client.Models.SqlExecutionResult> ExecuteInTransactionAsync(string transactionId, string sql, CancellationToken ct = default) => throw new NotSupportedException();
        public Task CommitTransactionAsync(string transactionId, CancellationToken ct = default) => throw new NotSupportedException();
        public Task RollbackTransactionAsync(string transactionId, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<IReadOnlyList<string>> GetCollectionNamesAsync(CancellationToken ct = default) => throw new NotSupportedException();
        public Task<int> GetCollectionCountAsync(string collectionName, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<CSharpDB.Client.Models.CollectionBrowseResult> BrowseCollectionAsync(string collectionName, int page = 1, int pageSize = 50, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<JsonElement?> GetDocumentAsync(string collectionName, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task PutDocumentAsync(string collectionName, string key, JsonElement document, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<bool> DeleteDocumentAsync(string collectionName, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task DropCollectionAsync(string collectionName, CancellationToken ct = default) => throw new NotSupportedException();
        public Task CheckpointAsync(CancellationToken ct = default) => throw new NotSupportedException();
        public Task<CSharpDB.Client.Models.BackupResult> BackupAsync(CSharpDB.Client.Models.BackupRequest request, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<CSharpDB.Client.Models.RestoreResult> RestoreAsync(CSharpDB.Client.Models.RestoreRequest request, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<CSharpDB.Client.Models.ForeignKeyMigrationResult> MigrateForeignKeysAsync(CSharpDB.Client.Models.ForeignKeyMigrationRequest request, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<CSharpDB.Client.Models.DatabaseMaintenanceReport> GetMaintenanceReportAsync(CancellationToken ct = default) => throw new NotSupportedException();
        public Task<CSharpDB.Client.Models.ReindexResult> ReindexAsync(CSharpDB.Client.Models.ReindexRequest request, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<CSharpDB.Client.Models.VacuumResult> VacuumAsync(CancellationToken ct = default) => throw new NotSupportedException();
        public Task<CSharpDB.Storage.Diagnostics.DatabaseInspectReport> InspectStorageAsync(string? databasePath = null, bool includePages = false, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<CSharpDB.Storage.Diagnostics.WalInspectReport> CheckWalAsync(string? databasePath = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<CSharpDB.Storage.Diagnostics.PageInspectReport> InspectPageAsync(uint pageId, bool includeHex = false, string? databasePath = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<CSharpDB.Storage.Diagnostics.IndexInspectReport> CheckIndexesAsync(string? databasePath = null, string? indexName = null, int? sampleSize = null, CancellationToken ct = default) => throw new NotSupportedException();
    }
}
