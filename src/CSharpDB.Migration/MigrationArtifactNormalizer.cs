namespace CSharpDB.Migration;

internal static class MigrationArtifactNormalizer
{
    public static MigrationCatalog Normalize(MigrationCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(catalog);

        IReadOnlyList<MigrationCatalogObject> objects = RequireList(
                catalog.Objects,
                "Catalog objects")
            .Select(Normalize)
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .ToArray();

        IReadOnlyList<MigrationDiagnostic> diagnostics = RequireList(
                catalog.Diagnostics,
                "Catalog diagnostics")
            .OrderBy(item => item?.DiagnosticId, StringComparer.Ordinal)
            .ToArray()!;

        return catalog with
        {
            Objects = objects,
            Diagnostics = diagnostics,
        };
    }

    public static MigrationPlan Normalize(MigrationPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);

        IReadOnlyList<MigrationPlanObject> objects = RequireList(
                plan.Objects,
                "Plan objects")
            .Select(Normalize)
            .OrderBy(item => item.SourceObjectId, StringComparer.Ordinal)
            .ToArray();

        IReadOnlyList<MigrationDiagnostic> diagnostics = RequireList(
                plan.Diagnostics,
                "Plan diagnostics")
            .OrderBy(item => item?.DiagnosticId, StringComparer.Ordinal)
            .ToArray()!;

        IReadOnlyList<string> acceptedDiagnosticIds = RequireList(
                plan.AcceptedDiagnosticIds,
                "Accepted diagnostic ids")
            .OrderBy(item => item, StringComparer.Ordinal)
            .ToArray();

        IReadOnlyList<string> acceptedExclusionObjectIds = RequireList(
                plan.AcceptedExclusionObjectIds,
                "Accepted exclusion object ids")
            .OrderBy(item => item, StringComparer.Ordinal)
            .ToArray();

        return plan with
        {
            Objects = objects,
            Diagnostics = diagnostics,
            AcceptedDiagnosticIds = acceptedDiagnosticIds,
            AcceptedExclusionObjectIds = acceptedExclusionObjectIds,
        };
    }

    private static MigrationCatalogObject Normalize(MigrationCatalogObject item)
    {
        if (item is null)
            throw new InvalidDataException("Catalog objects cannot contain null values.");

        IReadOnlyList<MigrationCatalogFacet> facets = RequireList(
                item.Facets,
                $"Facets for catalog object '{item.ObjectId}'")
            .OrderBy(facet => facet?.Name, StringComparer.Ordinal)
            .ThenBy(facet => facet?.Value, StringComparer.Ordinal)
            .ToArray()!;

        IReadOnlyList<string> dependencies = RequireList(
                item.DependsOn,
                $"Dependencies for catalog object '{item.ObjectId}'")
            .OrderBy(dependency => dependency, StringComparer.Ordinal)
            .ToArray();

        IReadOnlyList<MigrationObjectReference> members = RequireList(
                item.Members,
                $"Members for catalog object '{item.ObjectId}'")
            .OrderBy(member => member?.Role, StringComparer.Ordinal)
            .ThenBy(member => member?.Ordinal)
            .ThenBy(member => member?.ObjectId, StringComparer.Ordinal)
            .ToArray()!;

        return item with
        {
            Facets = facets,
            Members = members,
            DependsOn = dependencies,
        };
    }

    private static MigrationPlanObject Normalize(MigrationPlanObject item)
    {
        if (item is null)
            throw new InvalidDataException("Plan objects cannot contain null values.");

        IReadOnlyList<MigrationTypeMapping> mappings = RequireList(
                item.TypeMappings,
                $"Type mappings for plan object '{item.SourceObjectId}'")
            .Select(Normalize)
            .OrderBy(mapping => mapping?.SourceObjectId, StringComparer.Ordinal)
            .ToArray()!;

        IReadOnlyList<string> dependencies = RequireList(
                item.DependsOn,
                $"Dependencies for plan object '{item.SourceObjectId}'")
            .OrderBy(dependency => dependency, StringComparer.Ordinal)
            .ToArray();

        return item with
        {
            TypeMappings = mappings,
            DependsOn = dependencies,
        };
    }

    private static MigrationTypeMapping Normalize(MigrationTypeMapping mapping)
    {
        if (mapping is null)
            return null!;

        if (mapping.Conversion is null)
            return mapping;

        IReadOnlyList<MigrationCatalogFacet> parameters = RequireList(
                mapping.Conversion.Parameters,
                $"Conversion parameters for mapping '{mapping.SourceObjectId}'")
            .OrderBy(parameter => parameter?.Name, StringComparer.Ordinal)
            .ThenBy(parameter => parameter?.Value, StringComparer.Ordinal)
            .ToArray()!;

        return mapping with
        {
            Conversion = mapping.Conversion with { Parameters = parameters },
        };
    }

    private static IReadOnlyList<T> RequireList<T>(IReadOnlyList<T>? values, string description) =>
        values ?? throw new InvalidDataException($"{description} cannot be null.");
}
