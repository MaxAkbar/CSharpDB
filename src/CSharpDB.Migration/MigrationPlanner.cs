using System.Globalization;
using CSharpDB.Primitives;

namespace CSharpDB.Migration;

public sealed record MigrationPlanningOptions
{
    public MigrationMappingProfile MappingProfile { get; init; } = MigrationMappingProfile.Preserve;

    public IReadOnlyDictionary<string, DbType> CustomTargetTypes { get; init; } =
        new Dictionary<string, DbType>(StringComparer.Ordinal);

    public IReadOnlyList<string> AcceptedDiagnosticIds { get; init; } = [];

    public IReadOnlyList<string> AcceptedExclusionObjectIds { get; init; } = [];

    public bool AcceptAllExclusions { get; init; }
}

public sealed class MigrationPlanner
{
    private readonly CSharpDbCapabilityCatalog _capabilities;
    private readonly IDataTypeMappingProvider _typeMapper;

    public MigrationPlanner(
        CSharpDbCapabilityCatalog? capabilities = null,
        IDataTypeMappingProvider? typeMapper = null)
    {
        _capabilities = capabilities ?? CSharpDbCapabilityCatalogLoader.LoadEmbedded();
        _typeMapper = typeMapper ?? new StandardDataTypeMappingProvider();
    }

    public MigrationPlan CreatePlan(
        MigrationCatalog catalog,
        MigrationPlanningOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        options ??= new MigrationPlanningOptions();
        MigrationContractValidator.ValidateCatalog(catalog);
        if (!string.Equals(
                catalog.TargetCSharpDbVersion,
                _capabilities.TargetCSharpDbVersion,
                StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "The source catalog target version does not match the loaded capability catalog.");
        }
        if (options.MappingProfile != MigrationMappingProfile.Custom &&
            options.CustomTargetTypes.Count != 0)
        {
            throw new ArgumentException(
                "Custom target types require the custom mapping profile.",
                nameof(options));
        }

        IReadOnlyDictionary<string, MigrationCatalogObject> catalogObjectsById = catalog.Objects
            .ToDictionary(item => item.ObjectId, StringComparer.Ordinal);
        foreach (string objectId in options.CustomTargetTypes.Keys)
        {
            if (!catalogObjectsById.TryGetValue(objectId, out MigrationCatalogObject? item) ||
                item.NativeType is null)
            {
                throw new ArgumentException(
                    $"Custom target type references unknown or non-scalar object '{objectId}'.",
                    nameof(options));
            }
        }

        IReadOnlyDictionary<string, string> targetNames =
            DeterministicMigrationNameMapper.Map(catalog);
        var decisions = new Dictionary<string, MigrationTypeMappingDecision>(StringComparer.Ordinal);
        var diagnostics = catalog.Diagnostics.ToDictionary(
            item => item.DiagnosticId,
            item => item,
            StringComparer.Ordinal);

        foreach (MigrationCatalogObject item in catalog.Objects.Where(item => item.NativeType is not null))
        {
            options.CustomTargetTypes.TryGetValue(item.ObjectId, out DbType customTargetType);
            var request = new MigrationTypeMappingRequest
            {
                SourceObject = item,
                Profile = options.MappingProfile,
                Coverage = ReadCoverage(item),
                CustomTargetType = options.MappingProfile == MigrationMappingProfile.Custom &&
                                   options.CustomTargetTypes.ContainsKey(item.ObjectId)
                    ? customTargetType
                    : null,
            };
            MigrationTypeMappingDecision decision = _typeMapper.Map(request);
            decisions.Add(item.ObjectId, decision);
            if (decision.Diagnostic is not null)
            {
                if (diagnostics.TryGetValue(
                        decision.Diagnostic.DiagnosticId,
                        out MigrationDiagnostic? existing) &&
                    existing != decision.Diagnostic)
                {
                    throw new InvalidOperationException(
                        $"Diagnostic id collision '{decision.Diagnostic.DiagnosticId}'.");
                }

                diagnostics[decision.Diagnostic.DiagnosticId] = decision.Diagnostic;
            }
        }

        IReadOnlyDictionary<string, MigrationTypeMapping> mappingsByObjectId = decisions
            .ToDictionary(
                item => item.Key,
                item => item.Value.Mapping,
                StringComparer.Ordinal);
        var targetCapabilityEvaluator = new CSharpDbTargetCapabilityEvaluator(_capabilities);

        var exclusions = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (MigrationCatalogObject item in catalog.Objects.OrderBy(item => item.ObjectId, StringComparer.Ordinal))
        {
            MigrationCompatibilityStatus status = _capabilities.GetObjectStatus(item.Kind);
            if (item.Kind == MigrationObjectKind.Namespace)
            {
                Exclude(exclusions, item.ObjectId, "Source namespace is flattened into deterministic target names.");
            }
            else if (status is MigrationCompatibilityStatus.Conditional or
                     MigrationCompatibilityStatus.Unsupported or
                     MigrationCompatibilityStatus.Unknown)
            {
                Exclude(
                    exclusions,
                    item.ObjectId,
                    $"Target capability for {item.Kind} is {status} at planning evidence level.");
            }

            if (decisions.TryGetValue(item.ObjectId, out MigrationTypeMappingDecision? decision) &&
                decision.Mapping.Classification == MigrationMappingClassification.Unsupported)
            {
                Exclude(exclusions, item.ObjectId, "No supported target type mapping is available.");
                if (item.ParentObjectId is not null &&
                    catalogObjectsById[item.ParentObjectId].Kind is MigrationObjectKind.Table or
                        MigrationObjectKind.Collection)
                {
                    Exclude(
                        exclusions,
                        item.ParentObjectId,
                        $"Contained object '{item.ObjectId}' has no supported target type mapping.");
                }
            }

            if (!exclusions.ContainsKey(item.ObjectId))
            {
                string? capabilityReason = targetCapabilityEvaluator.GetExclusionReason(
                    item,
                    catalogObjectsById,
                    mappingsByObjectId);
                if (capabilityReason is not null)
                {
                    Exclude(exclusions, item.ObjectId, capabilityReason);
                    if (item.Kind == MigrationObjectKind.Column &&
                        item.ParentObjectId is not null &&
                        catalogObjectsById[item.ParentObjectId].Kind is MigrationObjectKind.Table or
                            MigrationObjectKind.Collection)
                    {
                        Exclude(
                            exclusions,
                            item.ParentObjectId,
                            $"Contained object '{item.ObjectId}' is rejected by the target capability catalog.");
                    }
                }
            }
        }

        bool changed;
        do
        {
            changed = false;
            foreach (MigrationCatalogObject item in catalog.Objects.OrderBy(item => item.ObjectId, StringComparer.Ordinal))
            {
                if (exclusions.ContainsKey(item.ObjectId))
                    continue;

                string? targetParent = DeterministicMigrationNameMapper.GetTargetParentObjectId(
                    item,
                    catalogObjectsById);
                if (targetParent is not null && exclusions.ContainsKey(targetParent))
                {
                    Exclude(exclusions, item.ObjectId, $"Target parent '{targetParent}' is excluded.");
                    changed = true;
                    continue;
                }

                string? excludedDependency = item.DependsOn
                    .OrderBy(id => id, StringComparer.Ordinal)
                    .FirstOrDefault(exclusions.ContainsKey);
                if (excludedDependency is not null)
                {
                    Exclude(exclusions, item.ObjectId, $"Dependency '{excludedDependency}' is excluded.");
                    changed = true;
                }
            }
        }
        while (changed);

        MigrationPlanObject[] planObjects = catalog.Objects
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .Select(item =>
            {
                bool included = !exclusions.TryGetValue(item.ObjectId, out string? reason);
                return new MigrationPlanObject
                {
                    SourceObjectId = item.ObjectId,
                    TargetParentObjectId = DeterministicMigrationNameMapper.GetTargetParentObjectId(
                        item,
                        catalogObjectsById),
                    Included = included,
                    ExclusionReason = reason,
                    TargetName = included ? targetNames[item.ObjectId] : null,
                    TypeMappings = decisions.TryGetValue(item.ObjectId, out MigrationTypeMappingDecision? decision)
                        ? [decision.Mapping]
                        : [],
                    DependsOn = item.DependsOn,
                };
            })
            .ToArray();

        IReadOnlyList<string> acceptedExclusionObjectIds = options.AcceptAllExclusions
            ? exclusions.Keys.OrderBy(item => item, StringComparer.Ordinal).ToArray()
            : options.AcceptedExclusionObjectIds;

        var plan = new MigrationPlan
        {
            TargetCSharpDbVersion = catalog.TargetCSharpDbVersion,
            Source = catalog.Source,
            CatalogDigest = MigrationArtifactSerializer.ComputeCatalogDigest(catalog),
            CapabilityDigest = _capabilities.Digest,
            NamingAlgorithmVersion = DeterministicMigrationNameMapper.AlgorithmVersion,
            MappingPolicyId = _typeMapper.PolicyId,
            MappingPolicyVersion = _typeMapper.PolicyVersion,
            MappingProfile = options.MappingProfile,
            Objects = planObjects,
            Diagnostics = diagnostics.Values.OrderBy(item => item.DiagnosticId, StringComparer.Ordinal).ToArray(),
            AcceptedDiagnosticIds = options.AcceptedDiagnosticIds,
            AcceptedExclusionObjectIds = acceptedExclusionObjectIds,
        };

        MigrationContractValidator.ValidatePlan(
            plan,
            catalog,
            plan.CatalogDigest,
            _capabilities.Digest,
            _typeMapper);
        return plan;
    }

    private static MigrationProfileCoverage ReadCoverage(MigrationCatalogObject item)
    {
        string? kindValue = Facet(item, "profileKind");
        if (kindValue is null)
        {
            return new MigrationProfileCoverage
            {
                Kind = MigrationCoverageKind.None,
                ValuesExamined = 0,
                RequiresFullStreamValidation = true,
            };
        }

        if (!Enum.TryParse(kindValue, ignoreCase: true, out MigrationCoverageKind kind) ||
            kind == MigrationCoverageKind.None)
        {
            throw new InvalidDataException(
                $"Object '{item.ObjectId}' contains invalid profile coverage kind '{kindValue}'.");
        }

        long examined = ParseLongFacet(item, "profileValuesExamined");
        long total = ParseLongFacet(item, "profileTotalValues");
        return new MigrationProfileCoverage
        {
            Kind = kind,
            ValuesExamined = examined,
            TotalValues = total,
            RequiresFullStreamValidation = kind != MigrationCoverageKind.Full,
        };
    }

    private static long ParseLongFacet(MigrationCatalogObject item, string name)
    {
        string? value = Facet(item, name);
        if (!long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out long parsed) || parsed < 0)
            throw new InvalidDataException($"Object '{item.ObjectId}' has invalid facet '{name}'.");
        return parsed;
    }

    private static string? Facet(MigrationCatalogObject item, string name) =>
        item.Facets.FirstOrDefault(facet => string.Equals(facet.Name, name, StringComparison.Ordinal))?.Value;

    private static void Exclude(
        IDictionary<string, string> exclusions,
        string objectId,
        string reason)
    {
        if (!exclusions.ContainsKey(objectId))
            exclusions.Add(objectId, reason);
    }
}
