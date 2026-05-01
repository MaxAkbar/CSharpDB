using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Reports.Contracts;
using CSharpDB.Admin.Reports.Models;
using CSharpDB.Primitives;
using Microsoft.Extensions.DependencyInjection;

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
    private readonly IServiceProvider _services;

    public HostCallbackCatalogService(IServiceProvider services)
    {
        _services = services;
    }

    public IReadOnlyList<DbHostCallbackDescriptor> GetCallbacks()
    {
        DbFunctionRegistry functions = _services.GetService<DbFunctionRegistry>() ?? DbFunctionRegistry.Empty;
        DbCommandRegistry commands = _services.GetService<DbCommandRegistry>() ?? DbCommandRegistry.Empty;

        return functions.Callbacks
            .Concat(commands.Callbacks)
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
                    AddReferences(references, form.Automation, "Form", form.FormId, form.Name);
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
                    AddReferences(references, report.Automation, "Report", report.ReportId, report.Name);
            }
            catch
            {
                // Keep the host callback catalog usable even if saved report metadata is unavailable.
            }
        }

        return references
            .GroupBy(
                static reference => GetReferenceKey(reference),
                StringComparer.OrdinalIgnoreCase)
            .Select(static group => group.First())
            .ToArray();
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
    }

    private static string GetEntryKey(AutomationCallbackKind kind, string name, int? arity)
        => $"{kind}\u001f{name.Trim()}\u001f{arity?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "-"}";

    private static string GetReferenceKey(HostCallbackReference reference)
        => $"{GetEntryKey(reference.Kind, reference.Name, reference.Arity)}\u001f{reference.Surface}\u001f{reference.Location}\u001f{reference.OwnerKind}\u001f{reference.OwnerId}";
}
