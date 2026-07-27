using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Primitives;

namespace CSharpDB.Migration;

public enum CSharpDbCapabilityFeature
{
    Object,
    Identifier,
    ColumnType,
    Nullable,
    DefaultValue,
    Identity,
    RowVersion,
    PrimaryKey,
    UniqueConstraint,
    ForeignKey,
    CheckConstraint,
    Index,
    ViewBody,
    TriggerBody,
    TriggerWhen,
    CollectionIndex,
    TargetTypeEnforcement,
}

public sealed record CSharpDbValueCapability
{
    public required DbType Type { get; init; }

    public required bool IsRuntimeValue { get; init; }

    public required bool IsColumnType { get; init; }

    public required string Representation { get; init; }
}

public sealed record CSharpDbCapabilityRule
{
    public required string RuleId { get; init; }

    public required MigrationObjectKind ObjectKind { get; init; }

    public required CSharpDbCapabilityFeature Feature { get; init; }

    public required MigrationCompatibilityStatus Status { get; init; }

    public IReadOnlyList<DbType> AllowedTypes { get; init; } = [];

    public IReadOnlyList<string> AllowedValues { get; init; } = [];

    public int? MaxCount { get; init; }

    public required string EvidenceId { get; init; }

    public required string Notes { get; init; }
}

public sealed record CSharpDbCapabilityEvidence
{
    public required string EvidenceId { get; init; }

    public required string Source { get; init; }

    public required string Assertion { get; init; }
}

public sealed record CSharpDbCapabilityCatalog
{
    public required string Format { get; init; }

    public required string TargetCSharpDbVersion { get; init; }

    public required string Surface { get; init; }

    public required int MaxIdentifierLength { get; init; }

    public required bool IdentifiersCaseInsensitive { get; init; }

    public required bool QuotedIdentifiers { get; init; }

    public required bool EngineEnforcesMappedColumnType { get; init; }

    public IReadOnlyList<CSharpDbValueCapability> ValueTypes { get; init; } = [];

    public IReadOnlyList<CSharpDbCapabilityRule> Rules { get; init; } = [];

    public IReadOnlyList<CSharpDbCapabilityEvidence> Evidence { get; init; } = [];

    [JsonIgnore]
    public string Digest { get; init; } = string.Empty;

    public MigrationCompatibilityStatus GetObjectStatus(MigrationObjectKind kind) =>
        Rules.Single(rule => rule.ObjectKind == kind && rule.Feature == CSharpDbCapabilityFeature.Object).Status;

    public bool IsColumnType(DbType type) =>
        ValueTypes.Single(item => item.Type == type).IsColumnType;
}

public static class CSharpDbCapabilityCatalogLoader
{
    public const string CurrentTargetVersion = "4.3.0";
    public const string Format = "csharpdb-target-capabilities/v1";

    private const string ResourceName = "CSharpDB.Migration.Capabilities.csharpdb-4.3.0.json";

    private static readonly JsonSerializerOptions s_options = CreateOptions();
    private static readonly Lazy<CSharpDbCapabilityCatalog> s_current = new(LoadCurrent);

    public static CSharpDbCapabilityCatalog LoadEmbedded(string targetCSharpDbVersion = CurrentTargetVersion)
    {
        if (!string.Equals(targetCSharpDbVersion, CurrentTargetVersion, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"No embedded CSharpDB capability catalog is available for target version '{targetCSharpDbVersion}'.");
        }

        return s_current.Value;
    }

    private static CSharpDbCapabilityCatalog LoadCurrent()
    {
        RequireAssemblyVersion(typeof(CSharpDbCapabilityCatalogLoader).Assembly, "CSharpDB.Migration");
        RequireAssemblyVersion(typeof(DbType).Assembly, "CSharpDB.Primitives");

        using Stream stream = typeof(CSharpDbCapabilityCatalogLoader).Assembly
            .GetManifestResourceStream(ResourceName)
            ?? throw new InvalidOperationException($"Embedded capability resource '{ResourceName}' is missing.");
        using var document = JsonDocument.Parse(stream, new JsonDocumentOptions
        {
            AllowTrailingCommas = false,
            CommentHandling = JsonCommentHandling.Disallow,
            MaxDepth = 32,
        });

        RejectDuplicateProperties(document.RootElement, "$", new HashSet<string>(StringComparer.Ordinal));

        CSharpDbCapabilityCatalog catalog;
        try
        {
            catalog = document.RootElement.Deserialize<CSharpDbCapabilityCatalog>(s_options)
                ?? throw new InvalidDataException("Embedded capability catalog is empty.");
        }
        catch (JsonException ex)
        {
            throw new InvalidDataException("Embedded capability catalog is invalid.", ex);
        }

        CSharpDbCapabilityCatalog normalized = Normalize(catalog);
        Validate(normalized);
        byte[] canonicalBytes = JsonSerializer.SerializeToUtf8Bytes(normalized, s_options);
        return normalized with
        {
            Digest = Convert.ToHexString(SHA256.HashData(canonicalBytes)).ToLowerInvariant(),
        };
    }

    private static CSharpDbCapabilityCatalog Normalize(CSharpDbCapabilityCatalog catalog) => catalog with
    {
        ValueTypes = catalog.ValueTypes.OrderBy(item => item.Type).ToArray(),
        Rules = catalog.Rules
            .Select(rule => rule with
            {
                AllowedTypes = rule.AllowedTypes.OrderBy(item => item).ToArray(),
                AllowedValues = rule.AllowedValues.OrderBy(item => item, StringComparer.Ordinal).ToArray(),
            })
            .OrderBy(rule => rule.RuleId, StringComparer.Ordinal)
            .ToArray(),
        Evidence = catalog.Evidence.OrderBy(item => item.EvidenceId, StringComparer.Ordinal).ToArray(),
    };

    private static void Validate(CSharpDbCapabilityCatalog catalog)
    {
        if (!string.Equals(catalog.Format, Format, StringComparison.Ordinal))
            throw new InvalidDataException($"Capability catalog format must be '{Format}'.");
        if (!string.Equals(catalog.TargetCSharpDbVersion, CurrentTargetVersion, StringComparison.Ordinal))
            throw new InvalidDataException("Capability catalog target version does not match this binary.");
        if (!string.Equals(catalog.Surface, "local-typed-engine", StringComparison.Ordinal))
            throw new InvalidDataException("Capability catalog surface is not supported.");
        if (catalog.MaxIdentifierLength != SqlIdentifierRules.MaxLength)
            throw new InvalidDataException("Capability catalog identifier limit does not match CSharpDB.Primitives.");

        DbType[] runtimeTypes = Enum.GetValues<DbType>();
        if (!runtimeTypes.SequenceEqual(catalog.ValueTypes.Select(item => item.Type)))
            throw new InvalidDataException("Capability catalog must describe every DbType exactly once.");
        if (catalog.ValueTypes.Single(item => item.Type == DbType.Null).IsColumnType)
            throw new InvalidDataException("DbType.Null cannot be declared as a persistent column type.");

        var evidenceIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (CSharpDbCapabilityEvidence evidence in catalog.Evidence)
        {
            RequireText(evidence.EvidenceId, "Capability evidence id");
            RequireText(evidence.Source, $"Source for capability evidence '{evidence.EvidenceId}'");
            RequireText(evidence.Assertion, $"Assertion for capability evidence '{evidence.EvidenceId}'");
            if (!evidenceIds.Add(evidence.EvidenceId))
                throw new InvalidDataException($"Duplicate capability evidence id '{evidence.EvidenceId}'.");
        }

        var ruleIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (CSharpDbCapabilityRule rule in catalog.Rules)
        {
            RequireText(rule.RuleId, "Capability rule id");
            RequireText(rule.Notes, $"Notes for capability rule '{rule.RuleId}'");
            if (!ruleIds.Add(rule.RuleId))
                throw new InvalidDataException($"Duplicate capability rule id '{rule.RuleId}'.");
            if (!evidenceIds.Contains(rule.EvidenceId))
                throw new InvalidDataException($"Capability rule '{rule.RuleId}' has unknown evidence '{rule.EvidenceId}'.");
        }

        MigrationObjectKind[] objectKinds = Enum.GetValues<MigrationObjectKind>();
        MigrationObjectKind[] coveredKinds = catalog.Rules
            .Where(rule => rule.Feature == CSharpDbCapabilityFeature.Object)
            .Select(rule => rule.ObjectKind)
            .OrderBy(kind => kind)
            .ToArray();
        if (!objectKinds.SequenceEqual(coveredKinds))
            throw new InvalidDataException("Capability catalog must contain one object rule for every migration object kind.");
    }

    private static void RequireAssemblyVersion(Assembly assembly, string description)
    {
        string? informational = assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
            .InformationalVersion;
        string version = (informational ?? assembly.GetName().Version?.ToString() ?? string.Empty)
            .Split('+', 2)[0];
        if (!string.Equals(version, CurrentTargetVersion, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"{description} binary version '{version}' does not match capability catalog version '{CurrentTargetVersion}'.");
        }
    }

    private static void RejectDuplicateProperties(
        JsonElement element,
        string path,
        HashSet<string> names)
    {
        if (element.ValueKind == JsonValueKind.Object)
        {
            names.Clear();
            foreach (JsonProperty property in element.EnumerateObject())
            {
                if (!names.Add(property.Name))
                    throw new InvalidDataException($"Capability catalog contains duplicate property '{path}.{property.Name}'.");

                RejectDuplicateProperties(property.Value, $"{path}.{property.Name}", new HashSet<string>(StringComparer.Ordinal));
            }
        }
        else if (element.ValueKind == JsonValueKind.Array)
        {
            int index = 0;
            foreach (JsonElement item in element.EnumerateArray())
                RejectDuplicateProperties(item, $"{path}[{index++}]", new HashSet<string>(StringComparer.Ordinal));
        }
    }

    private static void RequireText(string? value, string description)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidDataException($"{description} is required.");
    }

    private static JsonSerializerOptions CreateOptions()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            PropertyNameCaseInsensitive = false,
            UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
            AllowTrailingCommas = false,
            ReadCommentHandling = JsonCommentHandling.Disallow,
            MaxDepth = 32,
        };
        options.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase, allowIntegerValues: false));
        return options;
    }
}
