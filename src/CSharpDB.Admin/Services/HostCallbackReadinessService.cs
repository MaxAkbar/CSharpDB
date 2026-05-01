using System.Globalization;
using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Services;

public sealed record HostCallbackReadinessReport(
    int RegisteredCount,
    int ReferencedCount,
    IReadOnlyList<HostCallbackCatalogEntry> MissingEntries)
{
    public int MissingCount => MissingEntries.Count;
    public bool Ready => MissingCount == 0;
}

public sealed class HostCallbackReadinessService
{
    private static readonly AutomationStubGenerationOptions s_missingStubOptions = new(
        Namespace: "CSharpDbAutomation",
        ClassName: "MissingHostCallbackRegistration");

    private readonly HostCallbackCatalogService _catalog;

    public HostCallbackReadinessService(HostCallbackCatalogService catalog)
    {
        _catalog = catalog;
    }

    public async Task<HostCallbackReadinessReport> GetReadinessAsync()
    {
        IReadOnlyList<HostCallbackCatalogEntry> entries = await _catalog.GetEntriesAsync();
        HostCallbackCatalogEntry[] missingEntries = entries
            .Where(static entry => entry.IsMissingRegistration)
            .ToArray();

        return new HostCallbackReadinessReport(
            RegisteredCount: entries.Count(static entry => entry.IsRegistered),
            ReferencedCount: entries.Count(static entry => entry.IsReferenced),
            MissingEntries: missingEntries);
    }

    public async Task<string> GenerateMissingStubSourceAsync()
    {
        HostCallbackReadinessReport report = await GetReadinessAsync();
        return GenerateStubSource(report.MissingEntries, s_missingStubOptions);
    }

    public string GenerateStubSource(HostCallbackCatalogEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        return GenerateStubSource(
            [entry],
            new AutomationStubGenerationOptions(
                Namespace: "CSharpDbAutomation",
                ClassName: CreateStubClassName(entry),
                MethodName: "Register"));
    }

    private static string GenerateStubSource(
        IReadOnlyList<HostCallbackCatalogEntry> entries,
        AutomationStubGenerationOptions options)
    {
        DbAutomationMetadata metadata = BuildMetadata(entries);
        return AutomationStubGenerator.GenerateCSharp(metadata, options);
    }

    private static DbAutomationMetadata BuildMetadata(IReadOnlyList<HostCallbackCatalogEntry> entries)
    {
        var commands = new List<DbAutomationCommandReference>();
        var scalarFunctions = new List<DbAutomationScalarFunctionReference>();

        foreach (HostCallbackCatalogEntry entry in entries)
        {
            foreach (HostCallbackReference reference in entry.References)
            {
                string location = FormatReferenceLocation(reference);
                if (entry.Kind == AutomationCallbackKind.Command)
                {
                    commands.Add(new DbAutomationCommandReference(
                        entry.Name,
                        reference.Surface,
                        location));
                }
                else if (entry.Kind == AutomationCallbackKind.ScalarFunction && entry.Arity.HasValue)
                {
                    scalarFunctions.Add(new DbAutomationScalarFunctionReference(
                        entry.Name,
                        entry.Arity.Value,
                        reference.Surface,
                        location));
                }
            }
        }

        return new DbAutomationMetadata(
            DbAutomationMetadata.CurrentMetadataVersion,
            commands,
            scalarFunctions);
    }

    private static string FormatReferenceLocation(HostCallbackReference reference)
    {
        string owner = string.IsNullOrWhiteSpace(reference.OwnerName)
            ? reference.OwnerId
            : reference.OwnerName;

        return $"{reference.OwnerKind} {owner} ({reference.OwnerId}): {reference.Location}";
    }

    private static string CreateStubClassName(HostCallbackCatalogEntry entry)
    {
        StringBuilder builder = new("Register");
        foreach (string part in entry.Name.Split(['_', '-', '.', ' '], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            builder.Append(char.ToUpperInvariant(part[0]));
            if (part.Length > 1)
                builder.Append(part[1..]);
        }

        if (builder.Length == "Register".Length)
            builder.Append(entry.Kind.ToString());

        if (entry.Arity.HasValue)
            builder.Append("Arity").Append(entry.Arity.Value.ToString(CultureInfo.InvariantCulture));

        builder.Append("Callback");
        return SanitizeIdentifier(builder.ToString());
    }

    private static string SanitizeIdentifier(string candidate)
    {
        var builder = new StringBuilder(candidate.Length);
        for (int i = 0; i < candidate.Length; i++)
        {
            char ch = candidate[i];
            bool valid = i == 0
                ? char.IsLetter(ch) || ch == '_'
                : char.IsLetterOrDigit(ch) || ch == '_';
            builder.Append(valid ? ch : '_');
        }

        return builder.Length == 0 || char.IsDigit(builder[0])
            ? "_" + builder
            : builder.ToString();
    }
}
