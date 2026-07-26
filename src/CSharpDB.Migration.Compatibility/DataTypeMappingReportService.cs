using System.Globalization;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Compatibility;

/// <summary>
/// Produces a standalone, deterministic view of the exact mapping policy used
/// by migration planning. This service never reads source rows or mutates a
/// target database.
/// </summary>
public sealed class DataTypeMappingReportService
{
    private readonly IDataTypeMappingProvider _mappingProvider;

    public DataTypeMappingReportService(
        IDataTypeMappingProvider? mappingProvider = null) =>
        _mappingProvider = mappingProvider ?? new StandardDataTypeMappingProvider();

    public DataTypeMappingReport Create(
        MigrationCatalog catalog,
        DataTypeMappingReportOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        options ??= new DataTypeMappingReportOptions();
        ArgumentNullException.ThrowIfNull(options.CustomTargetTypes);

        MigrationContractValidator.ValidateCatalog(catalog);
        ValidateOptions(catalog, options);

        DataTypeMappingReportEntry[] entries = catalog.Objects
            .Where(static item => item.NativeType is not null)
            .OrderBy(static item => item.ObjectId, StringComparer.Ordinal)
            .Select(item => CreateEntry(item, options))
            .ToArray();

        return new DataTypeMappingReport
        {
            TargetCSharpDbVersion = catalog.TargetCSharpDbVersion,
            SourceKind = catalog.Source.Kind,
            CatalogDigest = MigrationArtifactSerializer.ComputeCatalogDigest(catalog),
            MappingPolicyId = _mappingProvider.PolicyId,
            MappingPolicyVersion = _mappingProvider.PolicyVersion,
            Profile = options.Profile,
            Summary = Summarize(entries),
            Entries = entries,
        };
    }

    private DataTypeMappingReportEntry CreateEntry(
        MigrationCatalogObject sourceObject,
        DataTypeMappingReportOptions options)
    {
        bool hasCustomTarget = options.CustomTargetTypes.TryGetValue(
            sourceObject.ObjectId,
            out DbType customTargetType);
        MigrationTypeMappingDecision decision = _mappingProvider.Map(
            new MigrationTypeMappingRequest
            {
                SourceObject = sourceObject,
                Profile = options.Profile,
                Coverage = ReadCoverage(sourceObject),
                CustomTargetType = hasCustomTarget ? customTargetType : null,
            });

        MigrationTypeMapping mapping = decision.Mapping;
        return new DataTypeMappingReportEntry
        {
            SourceObjectId = sourceObject.ObjectId,
            SourceObjectKind = sourceObject.Kind,
            ParentObjectId = sourceObject.ParentObjectId,
            SourceNamespace = sourceObject.SourceNamespace,
            SourceName = sourceObject.SourceName,
            SourceNativeType = mapping.SourceNativeType,
            SourceLogicalType = GetFacet(sourceObject, "logicalType") ??
                mapping.SourceNativeType,
            TargetType = mapping.TargetType,
            RequestedTargetType = mapping.RequestedTargetType,
            Classification = mapping.Classification,
            Profile = mapping.Profile,
            Coverage = mapping.Coverage,
            Conversion = Normalize(mapping.Conversion),
            Diagnostic = decision.Diagnostic is null
                ? null
                : new DataTypeMappingReportDiagnostic
                {
                    DiagnosticId = decision.Diagnostic.DiagnosticId,
                    RuleId = decision.Diagnostic.RuleId,
                    Severity = decision.Diagnostic.Severity,
                    Status = decision.Diagnostic.Status,
                    Summary = decision.Diagnostic.Summary,
                    Explanation = decision.Diagnostic.Explanation,
                    Remediation = decision.Diagnostic.Remediation,
                    CanOverride = decision.Diagnostic.CanOverride,
                },
        };
    }

    private static void ValidateOptions(
        MigrationCatalog catalog,
        DataTypeMappingReportOptions options)
    {
        if (!Enum.IsDefined(options.Profile))
            throw new ArgumentOutOfRangeException(nameof(options), "Unknown mapping profile.");

        if (options.Profile != MigrationMappingProfile.Custom &&
            options.CustomTargetTypes.Count != 0)
        {
            throw new ArgumentException(
                "Custom target types require the custom mapping profile.",
                nameof(options));
        }

        IReadOnlyDictionary<string, MigrationCatalogObject> scalarObjects =
            catalog.Objects
                .Where(static item => item.NativeType is not null)
                .ToDictionary(static item => item.ObjectId, StringComparer.Ordinal);
        foreach ((string objectId, DbType targetType) in options.CustomTargetTypes)
        {
            if (string.IsNullOrWhiteSpace(objectId) ||
                !scalarObjects.ContainsKey(objectId))
            {
                throw new ArgumentException(
                    $"Custom target type references unknown or non-scalar object '{objectId}'.",
                    nameof(options));
            }

            if (!Enum.IsDefined(targetType))
            {
                throw new ArgumentOutOfRangeException(
                    nameof(options),
                    $"Custom target type for '{objectId}' is not defined.");
            }
        }
    }

    private static DataTypeMappingReportSummary Summarize(
        IReadOnlyList<DataTypeMappingReportEntry> entries) =>
        new()
        {
            Total = entries.Count,
            Exact = entries.Count(static item =>
                item.Classification == MigrationMappingClassification.Exact),
            LosslessReencoded = entries.Count(static item =>
                item.Classification ==
                MigrationMappingClassification.LosslessReencoded),
            Lossy = entries.Count(static item =>
                item.Classification == MigrationMappingClassification.Lossy),
            Unsupported = entries.Count(static item =>
                item.Classification == MigrationMappingClassification.Unsupported),
            RequiresFullStreamValidation = entries.Count(static item =>
                item.Coverage.RequiresFullStreamValidation),
        };

    private static MigrationConversionDescriptor? Normalize(
        MigrationConversionDescriptor? conversion) =>
        conversion is null
            ? null
            : conversion with
            {
                Parameters = conversion.Parameters
                    .OrderBy(static item => item.Name, StringComparer.Ordinal)
                    .ThenBy(static item => item.Value, StringComparer.Ordinal)
                    .ToArray(),
            };

    private static MigrationProfileCoverage ReadCoverage(
        MigrationCatalogObject item)
    {
        string? kindValue = GetFacet(item, "profileKind");
        if (kindValue is null)
        {
            return new MigrationProfileCoverage
            {
                Kind = MigrationCoverageKind.None,
                ValuesExamined = 0,
                RequiresFullStreamValidation = true,
            };
        }

        if (!Enum.TryParse(
                kindValue,
                ignoreCase: true,
                out MigrationCoverageKind kind) ||
            kind == MigrationCoverageKind.None)
        {
            throw new InvalidDataException(
                $"Object '{item.ObjectId}' contains invalid profile coverage kind '{kindValue}'.");
        }

        long examined = ParseLongFacet(item, "profileValuesExamined");
        string? totalValue = GetFacet(item, "profileTotalValues");
        long? total = totalValue is null
            ? null
            : ParseLongFacet(item, "profileTotalValues");
        if (kind == MigrationCoverageKind.Full && total is null)
        {
            throw new InvalidDataException(
                $"Object '{item.ObjectId}' must report 'profileTotalValues' for full profile coverage.");
        }

        if (total is not null && examined > total)
        {
            throw new InvalidDataException(
                $"Object '{item.ObjectId}' profile coverage exceeds its total value count.");
        }

        return new MigrationProfileCoverage
        {
            Kind = kind,
            ValuesExamined = examined,
            TotalValues = total,
            RequiresFullStreamValidation =
                kind != MigrationCoverageKind.Full ||
                total is null ||
                examined != total,
        };
    }

    private static long ParseLongFacet(
        MigrationCatalogObject item,
        string name)
    {
        string? value = GetFacet(item, name);
        if (value is null ||
            !long.TryParse(
                value,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out long parsed) ||
            parsed < 0)
        {
            throw new InvalidDataException(
                $"Object '{item.ObjectId}' contains invalid '{name}' profile coverage.");
        }

        return parsed;
    }

    private static string? GetFacet(
        MigrationCatalogObject item,
        string name) =>
        item.Facets.FirstOrDefault(
            facet => string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal))?.Value;
}
