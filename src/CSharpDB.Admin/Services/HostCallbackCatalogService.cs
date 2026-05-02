using System.Globalization;
using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Admin.Reports.Contracts;
using CSharpDB.Admin.Reports.Models;
using CSharpDB.Admin.Reports.Services;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Client.Pipelines;
using CSharpDB.Pipelines.Models;
using CSharpDB.Primitives;
using Microsoft.Extensions.DependencyInjection;
using ClientTriggerSchema = CSharpDB.Client.Models.TriggerSchema;

namespace CSharpDB.Admin.Services;

public sealed record HostCallbackReference(
    AutomationCallbackKind Kind,
    string Name,
    int? Arity,
    string Surface,
    string Location,
    string OwnerKind,
    string OwnerId,
    string OwnerName);

public sealed record HostCallbackCatalogEntry(
    AutomationCallbackKind Kind,
    string Name,
    int? Arity,
    DbHostCallbackDescriptor? Descriptor,
    IReadOnlyList<HostCallbackReference> References)
{
    public bool IsRegistered => Descriptor is not null;
    public bool IsReferenced => References.Count > 0;
    public bool IsMissingRegistration => IsReferenced && !IsRegistered;
}

public sealed class HostCallbackCatalogService
{
    private static readonly string[] SqlFunctionIgnoreList =
    [
        "ABS",
        "AND",
        "AS",
        "AVG",
        "BETWEEN",
        "CAST",
        "CHECK",
        "COALESCE",
        "COUNT",
        "DATE",
        "DATETIME",
        "DEFAULT",
        "EXISTS",
        "FILTER",
        "FOREIGN",
        "FROM",
        "GROUP",
        "IFNULL",
        "IN",
        "JULIANDAY",
        "KEY",
        "LENGTH",
        "LOWER",
        "LTRIM",
        "MAX",
        "MIN",
        "NOT",
        "NULLIF",
        "ON",
        "OR",
        "OVER",
        "PRIMARY",
        "PRINTF",
        "RAISE",
        "RANDOM",
        "REFERENCES",
        "ROUND",
        "RTRIM",
        "SELECT",
        "STRFTIME",
        "SUBSTR",
        "SUBSTRING",
        "SUM",
        "TIME",
        "TRIM",
        "TYPEOF",
        "UNIQUE",
        "UPPER",
        "VALUES",
        "WHERE",
    ];

    private readonly IServiceProvider _services;

    public HostCallbackCatalogService(IServiceProvider services)
    {
        _services = services;
    }

    public IReadOnlyList<DbHostCallbackDescriptor> GetCallbacks()
    {
        DbFunctionRegistry functions = _services.GetService<DbFunctionRegistry>() ?? DbFunctionRegistry.Empty;
        DbCommandRegistry commands = _services.GetService<DbCommandRegistry>() ?? DbCommandRegistry.Empty;
        DbValidationRuleRegistry validationRules = _services.GetService<DbValidationRuleRegistry>() ?? DbValidationRuleRegistry.Empty;

        return functions.Callbacks
            .Concat(commands.Callbacks)
            .Concat(validationRules.Callbacks)
            .OrderBy(static callback => callback.Kind)
            .ThenBy(static callback => callback.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static callback => callback.Arity ?? -1)
            .ToArray();
    }

    public async Task<IReadOnlyList<HostCallbackCatalogEntry>> GetEntriesAsync()
    {
        IReadOnlyList<DbHostCallbackDescriptor> registered = GetCallbacks();
        IReadOnlyList<HostCallbackReference> references = await GetReferencesAsync();

        var entries = new Dictionary<string, HostCallbackCatalogEntry>(StringComparer.OrdinalIgnoreCase);
        foreach (DbHostCallbackDescriptor descriptor in registered)
        {
            string key = GetEntryKey(descriptor.Kind, descriptor.Name, descriptor.Arity);
            entries[key] = new HostCallbackCatalogEntry(
                descriptor.Kind,
                descriptor.Name,
                descriptor.Arity,
                descriptor,
                []);
        }

        foreach (HostCallbackReference reference in references)
        {
            string key = GetEntryKey(reference.Kind, reference.Name, reference.Arity);
            if (entries.TryGetValue(key, out HostCallbackCatalogEntry? existing))
            {
                entries[key] = existing with
                {
                    References = existing.References.Concat([reference]).ToArray()
                };
                continue;
            }

            entries[key] = new HostCallbackCatalogEntry(
                reference.Kind,
                reference.Name,
                reference.Arity,
                Descriptor: null,
                References: [reference]);
        }

        return entries.Values
            .Select(static entry => entry with
            {
                References = entry.References
                    .OrderBy(static reference => reference.OwnerKind, StringComparer.OrdinalIgnoreCase)
                    .ThenBy(static reference => reference.OwnerName, StringComparer.OrdinalIgnoreCase)
                    .ThenBy(static reference => reference.Surface, StringComparer.OrdinalIgnoreCase)
                    .ThenBy(static reference => reference.Location, StringComparer.OrdinalIgnoreCase)
                    .ToArray()
            })
            .OrderByDescending(static entry => entry.IsMissingRegistration)
            .ThenBy(static entry => entry.Kind)
            .ThenBy(static entry => entry.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static entry => entry.Arity ?? -1)
            .ToArray();
    }

    private async Task<IReadOnlyList<HostCallbackReference>> GetReferencesAsync()
    {
        var references = new List<HostCallbackReference>();

        if (_services.GetService<IFormRepository>() is { } formRepository)
        {
            try
            {
                IReadOnlyList<FormDefinition> forms = await formRepository.ListAsync();
                foreach (FormDefinition form in forms)
                    AddReferences(
                        references,
                        form.Automation ?? FormAutomationMetadata.Build(form),
                        "Form",
                        form.FormId,
                        form.Name);
            }
            catch
            {
                // Keep the host callback catalog usable even if saved form metadata is unavailable.
            }
        }

        if (_services.GetService<IReportRepository>() is { } reportRepository)
        {
            try
            {
                IReadOnlyList<ReportDefinition> reports = await reportRepository.ListAsync();
                foreach (ReportDefinition report in reports)
                    AddReferences(
                        references,
                        report.Automation ?? ReportAutomationMetadata.Build(report),
                        "Report",
                        report.ReportId,
                        report.Name);
            }
            catch
            {
                // Keep the host callback catalog usable even if saved report metadata is unavailable.
            }
        }

        if (_services.GetService<ICSharpDbClient>() is { } dbClient)
            await AddDatabaseReferencesAsync(references, dbClient);

        return references
            .GroupBy(
                static reference => GetReferenceKey(reference),
                StringComparer.OrdinalIgnoreCase)
            .Select(static group => group.First())
            .ToArray();
    }

    private static async Task AddDatabaseReferencesAsync(List<HostCallbackReference> references, ICSharpDbClient dbClient)
    {
        try
        {
            IReadOnlyList<SavedQueryDefinition> savedQueries = await dbClient.GetSavedQueriesAsync();
            foreach (SavedQueryDefinition query in savedQueries)
            {
                AddSqlScalarFunctionReferences(
                    references,
                    query.SqlText,
                    surface: "savedQueries",
                    locationPrefix: "sqlText",
                    ownerKind: "SavedQuery",
                    ownerId: query.Id.ToString(CultureInfo.InvariantCulture),
                    ownerName: query.Name);
            }
        }
        catch
        {
            // Keep the host callback catalog usable even if saved query metadata is unavailable.
        }

        try
        {
            IReadOnlyList<ProcedureDefinition> procedures = await dbClient.GetProceduresAsync(includeDisabled: true);
            foreach (ProcedureDefinition procedure in procedures)
            {
                AddSqlScalarFunctionReferences(
                    references,
                    procedure.BodySql,
                    surface: "procedures",
                    locationPrefix: "bodySql",
                    ownerKind: "Procedure",
                    ownerId: procedure.Name,
                    ownerName: procedure.Name);
            }
        }
        catch
        {
            // Keep the host callback catalog usable even if procedure metadata is unavailable.
        }

        try
        {
            IReadOnlyList<ClientTriggerSchema> triggers = await dbClient.GetTriggersAsync();
            foreach (ClientTriggerSchema trigger in triggers)
            {
                AddSqlScalarFunctionReferences(
                    references,
                    trigger.BodySql,
                    surface: "triggers",
                    locationPrefix: "bodySql",
                    ownerKind: "Trigger",
                    ownerId: trigger.TriggerName,
                    ownerName: trigger.TriggerName);
            }
        }
        catch
        {
            // Keep the host callback catalog usable even if trigger metadata is unavailable.
        }

        await AddPipelineReferencesAsync(references, dbClient);
    }

    private static async Task AddPipelineReferencesAsync(List<HostCallbackReference> references, ICSharpDbClient dbClient)
    {
        try
        {
            IReadOnlyList<string> tableNames = await dbClient.GetTableNamesAsync();
            if (!tableNames.Contains("_etl_pipelines", StringComparer.OrdinalIgnoreCase)
                || !tableNames.Contains("_etl_pipeline_versions", StringComparer.OrdinalIgnoreCase))
            {
                return;
            }

            var pipelines = new CSharpDbPipelineCatalogClient(dbClient);
            IReadOnlyList<PipelineDefinitionSummary> summaries = await pipelines.ListPipelinesAsync(limit: 500);
            foreach (PipelineDefinitionSummary summary in summaries)
            {
                PipelinePackageDefinition? package = await pipelines.GetPipelineAsync(summary.Name);
                if (package is null)
                    continue;

                DbAutomationMetadata automation = package.Automation ?? PipelineAutomationMetadata.Build(package);
                AddReferences(
                    references,
                    automation,
                    ownerKind: "Pipeline",
                    ownerId: summary.Name,
                    ownerName: summary.Name);
            }
        }
        catch
        {
            // Keep the host callback catalog usable even if pipeline metadata is unavailable.
        }
    }

    private static void AddSqlScalarFunctionReferences(
        List<HostCallbackReference> references,
        string? sql,
        string surface,
        string locationPrefix,
        string ownerKind,
        string ownerId,
        string ownerName)
    {
        int index = 0;
        foreach (DbAutomationScalarFunctionCall call in DbAutomationExpressionInspector.FindScalarFunctionCalls(sql, SqlFunctionIgnoreList))
        {
            references.Add(new HostCallbackReference(
                AutomationCallbackKind.ScalarFunction,
                call.Name,
                call.Arity,
                surface,
                $"{locationPrefix}.functions[{index}]",
                ownerKind,
                ownerId,
                ownerName));
            index++;
        }
    }

    private static void AddReferences(
        List<HostCallbackReference> references,
        DbAutomationMetadata? metadata,
        string ownerKind,
        string ownerId,
        string ownerName)
    {
        if (metadata is null)
            return;

        foreach (DbAutomationCommandReference command in metadata.Commands ?? [])
        {
            references.Add(new HostCallbackReference(
                AutomationCallbackKind.Command,
                command.Name,
                Arity: null,
                command.Surface,
                command.Location,
                ownerKind,
                ownerId,
                ownerName));
        }

        foreach (DbAutomationScalarFunctionReference function in metadata.ScalarFunctions ?? [])
        {
            references.Add(new HostCallbackReference(
                AutomationCallbackKind.ScalarFunction,
                function.Name,
                function.Arity,
                function.Surface,
                function.Location,
                ownerKind,
                ownerId,
                ownerName));
        }

        foreach (DbAutomationValidationRuleReference validationRule in metadata.ValidationRules ?? [])
        {
            references.Add(new HostCallbackReference(
                AutomationCallbackKind.ValidationRule,
                validationRule.Name,
                Arity: null,
                validationRule.Surface,
                validationRule.Location,
                ownerKind,
                ownerId,
                ownerName));
        }
    }

    private static string GetEntryKey(AutomationCallbackKind kind, string name, int? arity)
        => $"{kind}\u001f{name.Trim()}\u001f{arity?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "-"}";

    private static string GetReferenceKey(HostCallbackReference reference)
        => $"{GetEntryKey(reference.Kind, reference.Name, reference.Arity)}\u001f{reference.Surface}\u001f{reference.Location}\u001f{reference.OwnerKind}\u001f{reference.OwnerId}";
}
