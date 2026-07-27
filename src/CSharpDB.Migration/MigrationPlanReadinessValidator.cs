namespace CSharpDB.Migration;

public enum MigrationPlanReadinessStatus
{
    Ready,
    RequiresApproval,
    Blocked,
}

public sealed record MigrationPlanReadiness
{
    public required MigrationPlanReadinessStatus Status { get; init; }

    public IReadOnlyList<string> PendingDiagnosticIds { get; init; } = [];

    public IReadOnlyList<string> PendingExclusionObjectIds { get; init; } = [];

    public IReadOnlyList<string> BlockingDiagnosticIds { get; init; } = [];

    public IReadOnlyList<string> ExcludedObjectIds { get; init; } = [];
}

/// <summary>
/// Applies execution-only gates to a structurally valid migration plan.
/// Planning and preview intentionally permit unresolved choices so a user can
/// inspect them before authorizing execution.
/// </summary>
public static class MigrationPlanReadinessValidator
{
    public static MigrationPlanReadiness Evaluate(
        MigrationPlan plan,
        MigrationCatalog catalog,
        IDataTypeMappingProvider? mappingPolicy = null)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(catalog);

        CSharpDbCapabilityCatalog capabilities = CSharpDbCapabilityCatalogLoader.LoadEmbedded(
            plan.TargetCSharpDbVersion);
        MigrationContractValidator.ValidatePlan(
            plan,
            catalog,
            MigrationArtifactSerializer.ComputeCatalogDigest(catalog),
            capabilities.Digest,
            mappingPolicy);

        IReadOnlyDictionary<string, MigrationDiagnostic> diagnosticsById = plan.Diagnostics
            .ToDictionary(item => item.DiagnosticId, StringComparer.Ordinal);
        IReadOnlySet<string> acceptedDiagnosticIds = plan.AcceptedDiagnosticIds
            .ToHashSet(StringComparer.Ordinal);
        IReadOnlySet<string> acceptedExclusionObjectIds = plan.AcceptedExclusionObjectIds
            .ToHashSet(StringComparer.Ordinal);
        IReadOnlySet<string> excludedObjectIds = plan.Objects
            .Where(item => !item.Included)
            .Select(item => item.SourceObjectId)
            .ToHashSet(StringComparer.Ordinal);
        var pending = new SortedSet<string>(StringComparer.Ordinal);
        var pendingExclusions = new SortedSet<string>(
            excludedObjectIds.Where(objectId => !acceptedExclusionObjectIds.Contains(objectId)),
            StringComparer.Ordinal);
        var blocking = new SortedSet<string>(StringComparer.Ordinal);

        foreach (MigrationDiagnostic diagnostic in plan.Diagnostics)
        {
            if (diagnostic.ObjectId is not null && excludedObjectIds.Contains(diagnostic.ObjectId))
                continue;

            if (!diagnostic.CanOverride &&
                (diagnostic.Severity == MigrationDiagnosticSeverity.Error ||
                 diagnostic.Status is MigrationCompatibilityStatus.Unsupported or
                     MigrationCompatibilityStatus.Unknown))
            {
                blocking.Add(diagnostic.DiagnosticId);
            }
        }

        foreach (MigrationPlanObject planObject in plan.Objects)
        {
            if (!planObject.Included)
                continue;

            foreach (MigrationTypeMapping mapping in planObject.TypeMappings)
            {
                if (mapping.Classification == MigrationMappingClassification.Unsupported)
                {
                    blocking.Add(mapping.DiagnosticId!);
                    continue;
                }

                if (mapping.Classification != MigrationMappingClassification.Lossy)
                    continue;

                MigrationDiagnostic diagnostic = diagnosticsById[mapping.DiagnosticId!];
                if (!StandardDataTypeMappingProvider.IsTrustedLossyDiagnostic(mapping, diagnostic))
                {
                    blocking.Add(mapping.DiagnosticId!);
                }
                else if (!acceptedDiagnosticIds.Contains(mapping.DiagnosticId!))
                {
                    pending.Add(mapping.DiagnosticId!);
                }
            }
        }

        MigrationPlanReadinessStatus status = blocking.Count > 0
            ? MigrationPlanReadinessStatus.Blocked
            : pending.Count > 0 || pendingExclusions.Count > 0
                ? MigrationPlanReadinessStatus.RequiresApproval
                : MigrationPlanReadinessStatus.Ready;
        return new MigrationPlanReadiness
        {
            Status = status,
            PendingDiagnosticIds = pending.ToArray(),
            PendingExclusionObjectIds = pendingExclusions.ToArray(),
            BlockingDiagnosticIds = blocking.ToArray(),
            ExcludedObjectIds = excludedObjectIds.OrderBy(item => item, StringComparer.Ordinal).ToArray(),
        };
    }

    public static void ValidateForApply(
        MigrationPlan plan,
        MigrationCatalog catalog,
        IDataTypeMappingProvider? mappingPolicy = null)
    {
        MigrationPlanReadiness readiness = Evaluate(plan, catalog, mappingPolicy);
        if (readiness.Status == MigrationPlanReadinessStatus.Ready)
            return;

        if (readiness.Status == MigrationPlanReadinessStatus.RequiresApproval)
        {
            var approvals = new List<string>();
            if (readiness.PendingDiagnosticIds.Count > 0)
            {
                approvals.Add(
                    $"diagnostic(s): {string.Join(", ", readiness.PendingDiagnosticIds)}");
            }
            if (readiness.PendingExclusionObjectIds.Count > 0)
            {
                approvals.Add(
                    $"exclusion(s): {string.Join(", ", readiness.PendingExclusionObjectIds)}");
            }

            throw new InvalidDataException(
                $"Migration plan requires accepted {string.Join("; ", approvals)} before apply.");
        }

        throw new InvalidDataException(
            $"Migration plan is blocked by diagnostic(s): {string.Join(", ", readiness.BlockingDiagnosticIds)}.");
    }
}
