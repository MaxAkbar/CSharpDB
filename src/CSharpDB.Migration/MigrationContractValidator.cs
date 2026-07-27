using System.Security.Cryptography;
using CSharpDB.Primitives;

namespace CSharpDB.Migration;

public static class MigrationContractValidator
{
    public static void ValidateCatalog(MigrationCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        RequireText(catalog.TargetCSharpDbVersion, "Catalog target CSharpDB version");
        ValidateSource(catalog.Source);

        IReadOnlyList<MigrationCatalogObject> objects = RequireList(catalog.Objects, "Catalog objects");
        var objectIds = new HashSet<string>(StringComparer.Ordinal);
        var objectsById =
            new Dictionary<string, MigrationCatalogObject>(StringComparer.Ordinal);
        foreach (MigrationCatalogObject item in objects)
        {
            if (item is null)
                throw Invalid("Catalog objects cannot contain null values.");

            RequireText(item.ObjectId, "Catalog object id");
            RequireText(item.SourceName, $"Source name for catalog object '{item.ObjectId}'");
            if (!objectIds.Add(item.ObjectId) ||
                !objectsById.TryAdd(item.ObjectId, item))
                throw Invalid($"Catalog contains duplicate object id '{item.ObjectId}'.");

            var facetNames = new HashSet<string>(StringComparer.Ordinal);
            foreach (MigrationCatalogFacet facet in RequireList(
                         item.Facets,
                         $"Facets for catalog object '{item.ObjectId}'"))
            {
                if (facet is null)
                    throw Invalid($"Catalog object '{item.ObjectId}' contains a null facet.");

                RequireText(facet.Name, $"Facet name for catalog object '{item.ObjectId}'");
                if (!facetNames.Add(facet.Name))
                {
                    throw Invalid(
                        $"Catalog object '{item.ObjectId}' contains duplicate facet '{facet.Name}'.");
                }
            }
        }

        foreach (MigrationCatalogObject item in objects)
        {
            if (item.ParentObjectId is not null)
            {
                RequireText(item.ParentObjectId, $"Parent object id for catalog object '{item.ObjectId}'");
                if (string.Equals(item.ParentObjectId, item.ObjectId, StringComparison.Ordinal))
                    throw Invalid($"Catalog object '{item.ObjectId}' cannot contain itself.");
                if (!objectIds.Contains(item.ParentObjectId))
                {
                    throw Invalid(
                        $"Catalog object '{item.ObjectId}' has unknown parent '{item.ParentObjectId}'.");
                }
            }

            var dependencies = new HashSet<string>(StringComparer.Ordinal);
            foreach (string dependency in RequireList(
                         item.DependsOn,
                         $"Dependencies for catalog object '{item.ObjectId}'"))
            {
                RequireText(dependency, $"Dependency for catalog object '{item.ObjectId}'");
                if (!dependencies.Add(dependency))
                {
                    throw Invalid(
                        $"Catalog object '{item.ObjectId}' repeats dependency '{dependency}'.");
                }

                if (string.Equals(dependency, item.ObjectId, StringComparison.Ordinal))
                    throw Invalid($"Catalog object '{item.ObjectId}' cannot depend on itself.");

                if (!objectIds.Contains(dependency))
                {
                    throw Invalid(
                        $"Catalog object '{item.ObjectId}' depends on unknown object '{dependency}'.");
                }
            }

            IReadOnlyList<MigrationObjectReference> members = RequireList(
                item.Members,
                $"Members for catalog object '{item.ObjectId}'");
            var memberOrdinals = new HashSet<(string Role, int Ordinal)>();
            foreach (MigrationObjectReference member in members)
            {
                if (member is null)
                    throw Invalid($"Catalog object '{item.ObjectId}' contains a null member reference.");

                RequireText(member.ObjectId, $"Member object id for catalog object '{item.ObjectId}'");
                RequireText(member.Role, $"Member role for catalog object '{item.ObjectId}'");
                if (member.Ordinal < 0)
                    throw Invalid($"Catalog object '{item.ObjectId}' contains a negative member ordinal.");
                if (!memberOrdinals.Add((member.Role, member.Ordinal)))
                {
                    throw Invalid(
                        $"Catalog object '{item.ObjectId}' repeats member ordinal {member.Ordinal} for role '{member.Role}'.");
                }
                if (string.Equals(member.ObjectId, item.ObjectId, StringComparison.Ordinal))
                    throw Invalid($"Catalog object '{item.ObjectId}' cannot contain itself as a member.");
                if (!objectIds.Contains(member.ObjectId))
                {
                    throw Invalid(
                        $"Catalog object '{item.ObjectId}' references unknown member '{member.ObjectId}'.");
                }
                if (!dependencies.Contains(member.ObjectId))
                {
                    throw Invalid(
                        $"Catalog object '{item.ObjectId}' member '{member.ObjectId}' must also be an execution dependency.");
                }
            }

            foreach (IGrouping<string, MigrationObjectReference> role in members.GroupBy(
                         member => member.Role,
                         StringComparer.Ordinal))
            {
                int[] ordinals = role.Select(member => member.Ordinal).OrderBy(value => value).ToArray();
                if (!ordinals.SequenceEqual(Enumerable.Range(0, ordinals.Length)))
                {
                    throw Invalid(
                        $"Catalog object '{item.ObjectId}' member ordinals for role '{role.Key}' must be contiguous from zero.");
                }
            }

            ValidateMemberRoles(item, members);
        }

        ValidateAcyclic(
            objects.Select(item => item.ObjectId),
            id =>
            {
                MigrationCatalogObject item = objectsById[id];
                IEnumerable<string> structuralReferences = item.DependsOn.Concat(
                    item.Members.Select(member => member.ObjectId));
                return item.ParentObjectId is null
                    ? structuralReferences
                    : structuralReferences.Append(item.ParentObjectId);
            },
            "Catalog object graph");

        IReadOnlyDictionary<string, MigrationDiagnostic> diagnostics =
            ValidateDiagnostics(RequireList(catalog.Diagnostics, "Catalog diagnostics"));
        foreach (MigrationDiagnostic diagnostic in diagnostics.Values)
        {
            if (diagnostic.ObjectId is not null && !objectIds.Contains(diagnostic.ObjectId))
            {
                throw Invalid(
                    $"Migration diagnostic '{diagnostic.DiagnosticId}' references unknown object '{diagnostic.ObjectId}'.");
            }
        }
    }

    public static void ValidatePlan(
        MigrationPlan plan,
        MigrationCatalog catalog,
        string catalogDigest,
        string capabilityDigest,
        IDataTypeMappingProvider? mappingPolicy = null)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(catalog);
        ValidateCatalog(catalog);

        RequireText(plan.TargetCSharpDbVersion, "Plan target CSharpDB version");
        RequireSha256(plan.CatalogDigest, "Plan catalog digest");
        RequireSha256(catalogDigest, "Computed catalog digest");
        if (!FixedTimeEquals(plan.CatalogDigest, catalogDigest))
            throw Invalid("Plan catalog digest does not match the supplied catalog.");

        RequireSha256(plan.CapabilityDigest, "Plan capability digest");
        RequireSha256(capabilityDigest, "Computed capability digest");
        if (!FixedTimeEquals(plan.CapabilityDigest, capabilityDigest))
            throw Invalid("Plan capability digest does not match the embedded target capability catalog.");

        RequireText(plan.NamingAlgorithmVersion, "Plan naming algorithm version");
        if (!string.Equals(
                plan.NamingAlgorithmVersion,
                DeterministicMigrationNameMapper.AlgorithmVersion,
                StringComparison.Ordinal))
        {
            throw Invalid(
                $"Plan naming algorithm '{plan.NamingAlgorithmVersion}' is not supported.");
        }

        mappingPolicy ??= ResolveBuiltInMappingPolicy(plan);
        RequireText(plan.MappingPolicyId, "Plan mapping policy id");
        RequireText(mappingPolicy.PolicyId, "Mapping policy id");
        if (!string.Equals(plan.MappingPolicyId, mappingPolicy.PolicyId, StringComparison.Ordinal) ||
            plan.MappingPolicyVersion != mappingPolicy.PolicyVersion)
        {
            throw Invalid(
                $"Plan mapping policy '{plan.MappingPolicyId}' version {plan.MappingPolicyVersion} does not match the supplied policy '{mappingPolicy.PolicyId}' version {mappingPolicy.PolicyVersion}.");
        }

        if (!string.Equals(
                plan.TargetCSharpDbVersion,
                catalog.TargetCSharpDbVersion,
                StringComparison.Ordinal))
        {
            throw Invalid("Plan target CSharpDB version does not match the supplied catalog.");
        }

        ValidateSource(plan.Source);
        if (plan.Source != catalog.Source)
            throw Invalid("Plan source identity does not match the supplied catalog.");

        if (plan.Load is null)
            throw Invalid("Migration load policy is required.");
        if (plan.Load.BatchSize <= 0)
            throw Invalid("Migration load batch size must be greater than zero.");
        if (plan.Load.MaxBatchBytes <= 0)
            throw Invalid("Migration load maximum batch bytes must be greater than zero.");
        if (plan.Load.MaxValueBytes <= 0)
            throw Invalid("Migration load maximum value bytes must be greater than zero.");
        if (plan.Load.MaxValueBytes > plan.Load.MaxBatchBytes)
            throw Invalid("Migration load maximum value bytes cannot exceed maximum batch bytes.");
        MigrationDeterministicRejectPolicyValidator.Validate(plan.Load);

        if (plan.Validation is null)
            throw Invalid("Migration validation policy is required.");
        RequireText(plan.Validation.CanonicalizationVersion, "Canonicalization version");
        if (plan.GeneratedDdlDigest is not null)
        {
            RequireSha256(plan.GeneratedDdlDigest, "Generated DDL digest");
            if (plan.GeneratedDdlDigest.Any(static character =>
                    character is >= 'A' and <= 'F'))
            {
                throw Invalid(
                    "Generated DDL digest must use lowercase hexadecimal.");
            }
        }

        IReadOnlyList<MigrationDiagnostic> diagnostics = RequireList(plan.Diagnostics, "Plan diagnostics");
        IReadOnlyDictionary<string, MigrationDiagnostic> diagnosticsById = ValidateDiagnostics(diagnostics);
        IReadOnlyDictionary<string, MigrationDiagnostic> catalogDiagnosticsById = catalog.Diagnostics
            .ToDictionary(item => item.DiagnosticId, StringComparer.Ordinal);
        foreach (MigrationDiagnostic diagnostic in diagnostics)
        {
            if (catalogDiagnosticsById.TryGetValue(
                    diagnostic.DiagnosticId,
                    out MigrationDiagnostic? catalogDiagnostic) &&
                diagnostic != catalogDiagnostic)
            {
                throw Invalid(
                    $"Plan diagnostic '{diagnostic.DiagnosticId}' does not match the supplied catalog diagnostic.");
            }

            if (diagnostic.ObjectId is not null &&
                !catalog.Objects.Any(item => item.ObjectId == diagnostic.ObjectId))
            {
                throw Invalid(
                    $"Plan diagnostic '{diagnostic.DiagnosticId}' references unknown catalog object '{diagnostic.ObjectId}'.");
            }
        }

        foreach (MigrationDiagnostic catalogDiagnostic in catalog.Diagnostics)
        {
            if (!diagnosticsById.TryGetValue(
                    catalogDiagnostic.DiagnosticId,
                    out MigrationDiagnostic? planDiagnostic) ||
                planDiagnostic != catalogDiagnostic)
            {
                throw Invalid(
                    $"Plan must retain catalog diagnostic '{catalogDiagnostic.DiagnosticId}' unchanged.");
            }
        }

        var acceptedDiagnosticIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (string diagnosticId in RequireList(
                     plan.AcceptedDiagnosticIds,
                     "Accepted diagnostic ids"))
        {
            RequireText(diagnosticId, "Accepted diagnostic id");
            if (!acceptedDiagnosticIds.Add(diagnosticId))
                throw Invalid($"Plan accepts diagnostic '{diagnosticId}' more than once.");

            if (!diagnosticsById.TryGetValue(diagnosticId, out MigrationDiagnostic? diagnostic))
                throw Invalid($"Plan accepts unknown diagnostic '{diagnosticId}'.");
            if (!diagnostic.CanOverride)
                throw Invalid($"Plan accepts non-overrideable diagnostic '{diagnosticId}'.");
        }

        IReadOnlyDictionary<string, MigrationCatalogObject> catalogObjectsById = catalog.Objects
            .ToDictionary(item => item.ObjectId, StringComparer.Ordinal);
        IReadOnlySet<string> catalogObjectIds = catalogObjectsById.Keys.ToHashSet(StringComparer.Ordinal);
        IReadOnlyDictionary<string, string> expectedTargetNames =
            DeterministicMigrationNameMapper.Map(catalog);

        IReadOnlyList<MigrationPlanObject> planObjects = RequireList(plan.Objects, "Plan objects");
        var planObjectsById = new Dictionary<string, MigrationPlanObject>(StringComparer.Ordinal);
        foreach (MigrationPlanObject item in planObjects)
        {
            if (item is null)
                throw Invalid("Plan objects cannot contain null values.");

            RequireText(item.SourceObjectId, "Plan source object id");
            if (!catalogObjectIds.Contains(item.SourceObjectId))
                throw Invalid($"Plan references unknown catalog object '{item.SourceObjectId}'.");
            if (!planObjectsById.TryAdd(item.SourceObjectId, item))
                throw Invalid($"Plan contains duplicate source object id '{item.SourceObjectId}'.");

            MigrationCatalogObject catalogObject = catalogObjectsById[item.SourceObjectId];
            string? expectedTargetParent = DeterministicMigrationNameMapper.GetTargetParentObjectId(
                catalogObject,
                catalogObjectsById);
            if (!string.Equals(
                    item.TargetParentObjectId,
                    expectedTargetParent,
                    StringComparison.Ordinal))
            {
                throw Invalid(
                    $"Plan object '{item.SourceObjectId}' target parent does not match catalog containment.");
            }

            if (item.Included)
            {
                RequireText(item.TargetName, $"Target name for included object '{item.SourceObjectId}'");
                ValidateTargetName(item.SourceObjectId, item.TargetName!);
                if (!string.Equals(
                        item.TargetName,
                        expectedTargetNames[item.SourceObjectId],
                        StringComparison.Ordinal))
                {
                    throw Invalid(
                        $"Plan object '{item.SourceObjectId}' target name does not match the bound naming algorithm.");
                }
            }
            else if (string.IsNullOrWhiteSpace(item.ExclusionReason))
            {
                throw Invalid(
                    $"Excluded plan object '{item.SourceObjectId}' must include an exclusion reason.");
            }
            else if (item.TargetName is not null)
            {
                throw Invalid(
                    $"Excluded plan object '{item.SourceObjectId}' cannot declare a target name.");
            }

            IReadOnlyList<MigrationTypeMapping> mappings = RequireList(
                item.TypeMappings,
                $"Type mappings for plan object '{item.SourceObjectId}'");
            foreach (MigrationTypeMapping mapping in mappings)
            {
                ValidateMapping(
                    item,
                    mapping,
                    plan.MappingProfile,
                    catalogObject,
                    catalogObjectsById,
                    diagnosticsById,
                    mappingPolicy);
            }

            int expectedMappings = catalogObject.NativeType is null ? 0 : 1;
            if (mappings.Count != expectedMappings)
            {
                throw Invalid(
                    $"Plan object '{item.SourceObjectId}' must contain exactly {expectedMappings} type mapping(s).");
            }
        }


        if (planObjectsById.Count != catalogObjectsById.Count)
        {
            string missing = string.Join(", ", catalogObjectIds
                .Where(id => !planObjectsById.ContainsKey(id))
                .OrderBy(id => id, StringComparer.Ordinal));
            throw Invalid($"Plan must explicitly include or exclude every catalog object. Missing: {missing}.");
        }

        var acceptedExclusionObjectIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (string objectId in RequireList(
                     plan.AcceptedExclusionObjectIds,
                     "Accepted exclusion object ids"))
        {
            RequireText(objectId, "Accepted exclusion object id");
            if (!acceptedExclusionObjectIds.Add(objectId))
                throw Invalid($"Plan accepts exclusion '{objectId}' more than once.");
            if (!planObjectsById.TryGetValue(objectId, out MigrationPlanObject? acceptedObject))
                throw Invalid($"Plan accepts exclusion for unknown object '{objectId}'.");
            if (acceptedObject.Included)
                throw Invalid($"Plan accepts exclusion for included object '{objectId}'.");
        }

        foreach (MigrationPlanObject item in planObjects)
        {
            if (item.TargetParentObjectId is not null)
            {
                if (!planObjectsById.TryGetValue(
                        item.TargetParentObjectId,
                        out MigrationPlanObject? parentObject))
                {
                    throw Invalid(
                        $"Plan object '{item.SourceObjectId}' has unknown target parent '{item.TargetParentObjectId}'.");
                }

                if (item.Included && !parentObject.Included)
                {
                    throw Invalid(
                        $"Included plan object '{item.SourceObjectId}' has excluded parent '{item.TargetParentObjectId}'.");
                }
            }

            var dependencies = new HashSet<string>(StringComparer.Ordinal);
            foreach (string dependency in RequireList(
                         item.DependsOn,
                         $"Dependencies for plan object '{item.SourceObjectId}'"))
            {
                RequireText(dependency, $"Dependency for plan object '{item.SourceObjectId}'");
                if (!dependencies.Add(dependency))
                {
                    throw Invalid(
                        $"Plan object '{item.SourceObjectId}' repeats dependency '{dependency}'.");
                }

                if (string.Equals(dependency, item.SourceObjectId, StringComparison.Ordinal))
                    throw Invalid($"Plan object '{item.SourceObjectId}' cannot depend on itself.");

                if (!planObjectsById.TryGetValue(dependency, out MigrationPlanObject? dependencyObject))
                {
                    throw Invalid(
                        $"Plan object '{item.SourceObjectId}' depends on unplanned object '{dependency}'.");
                }

                if (item.Included && !dependencyObject.Included)
                {
                    throw Invalid(
                        $"Included plan object '{item.SourceObjectId}' depends on excluded object '{dependency}'.");
                }
            }

            MigrationCatalogObject catalogObject = catalogObjectsById[item.SourceObjectId];
            if (!item.DependsOn.SequenceEqual(catalogObject.DependsOn, StringComparer.Ordinal))
            {
                throw Invalid(
                    $"Plan object '{item.SourceObjectId}' dependencies do not match the supplied catalog.");
            }
        }

        ValidateAcyclic(
            planObjects.Select(item => item.SourceObjectId),
            id => planObjectsById[id].DependsOn,
            "Plan dependency graph");

        CSharpDbCapabilityCatalog capabilities = CSharpDbCapabilityCatalogLoader.LoadEmbedded(
            plan.TargetCSharpDbVersion);
        var capabilityEvaluator = new CSharpDbTargetCapabilityEvaluator(capabilities);
        IReadOnlyDictionary<string, MigrationTypeMapping> mappingsByObjectId = planObjects
            .SelectMany(item => item.TypeMappings)
            .ToDictionary(item => item.SourceObjectId, StringComparer.Ordinal);
        foreach (MigrationPlanObject item in planObjects.Where(item => item.Included))
        {
            MigrationCatalogObject catalogObject = catalogObjectsById[item.SourceObjectId];
            MigrationCompatibilityStatus objectStatus = capabilities.GetObjectStatus(catalogObject.Kind);
            if (catalogObject.Kind == MigrationObjectKind.Namespace ||
                (objectStatus != MigrationCompatibilityStatus.Compatible &&
                 !(objectStatus == MigrationCompatibilityStatus.Conditional &&
                   capabilityEvaluator.CanEvaluateConditionalObject(catalogObject))))
            {
                throw Invalid(
                    $"Included plan object '{item.SourceObjectId}' is not supported by its bound target object capability ({objectStatus}).");
            }
            if (catalogObject.Kind == MigrationObjectKind.Collection &&
                plan.Load.RejectMode != MigrationRejectMode.FailFast)
            {
                throw Invalid(
                    $"Included document collection '{item.SourceObjectId}' requires fail-fast row handling.");
            }

            string? capabilityReason = capabilityEvaluator.GetExclusionReason(
                catalogObject,
                catalogObjectsById,
                mappingsByObjectId);
            if (capabilityReason is not null)
            {
                throw Invalid(
                    $"Included plan object '{item.SourceObjectId}' violates its bound target capability: {capabilityReason}");
            }
            if (catalogObject.Kind == MigrationObjectKind.Collection)
            {
                bool bound = MigrationDocumentCollectionContract.TryBindSupportedV1Collection(
                    catalogObject,
                    catalogObjectsById,
                    out MigrationDocumentCollectionBinding? binding,
                    out _);
                if (!bound ||
                    !planObjectsById[binding!.KeyColumn.ObjectId].Included ||
                    !planObjectsById[binding.DocumentColumn.ObjectId].Included)
                {
                    throw Invalid(
                        $"Included document collection '{item.SourceObjectId}' requires included key and document bridge columns.");
                }
            }
        }

        ValidateTargetNameCollisions(planObjects, catalogObjectsById);
        ValidateCollectionPhysicalTargetNames(planObjects, catalogObjectsById);
    }

    private static void ValidateTargetName(string sourceObjectId, string targetName)
    {
        try
        {
            SqlIdentifierRules.Validate(targetName, $"Target name for plan object '{sourceObjectId}'");
        }
        catch (Exception ex) when (ex is ArgumentException or CSharpDbException)
        {
            throw Invalid(ex.Message);
        }

        if (IsReservedTargetName(targetName))
        {
            throw Invalid(
                $"Target name '{targetName}' for plan object '{sourceObjectId}' uses a reserved CSharpDB prefix.");
        }
    }

    private static void ValidateMemberRoles(
        MigrationCatalogObject item,
        IReadOnlyList<MigrationObjectReference> members)
    {
        if (members.Count == 0)
            return;

        if (item.Kind is MigrationObjectKind.Key or MigrationObjectKind.Index)
        {
            if (members.Any(member => !string.Equals(
                    member.Role,
                    MigrationObjectReferenceRoles.Column,
                    StringComparison.Ordinal)))
            {
                throw Invalid(
                    $"Catalog {item.Kind} '{item.ObjectId}' may contain only '{MigrationObjectReferenceRoles.Column}' members.");
            }

            return;
        }

        if (item.Kind == MigrationObjectKind.ForeignKey)
        {
            if (members.Any(member => member.Role is not (
                    MigrationObjectReferenceRoles.SourceColumn or
                    MigrationObjectReferenceRoles.ReferencedKey)))
            {
                throw Invalid(
                    $"Catalog foreign key '{item.ObjectId}' contains an unsupported member role.");
            }

            int sourceColumnCount = members.Count(member => member.Role == MigrationObjectReferenceRoles.SourceColumn);
            int referencedKeyCount = members.Count(member => member.Role == MigrationObjectReferenceRoles.ReferencedKey);
            if (sourceColumnCount == 0 || referencedKeyCount != 1)
            {
                throw Invalid(
                    $"Catalog foreign key '{item.ObjectId}' must contain source columns and exactly one referenced key member.");
            }

            return;
        }

        throw Invalid($"Catalog object '{item.ObjectId}' cannot declare ordered members for kind '{item.Kind}'.");
    }

    internal static bool IsReservedTargetName(string targetName) =>
        targetName.StartsWith("sys.", StringComparison.OrdinalIgnoreCase) ||
        targetName.StartsWith("sys_", StringComparison.OrdinalIgnoreCase) ||
        targetName.StartsWith("_col_", StringComparison.OrdinalIgnoreCase) ||
        targetName.StartsWith("__", StringComparison.OrdinalIgnoreCase);

    private static void ValidateTargetNameCollisions(
        IReadOnlyList<MigrationPlanObject> planObjects,
        IReadOnlyDictionary<string, MigrationCatalogObject> catalogObjectsById)
    {
        var namesByScope = new Dictionary<DeterministicMigrationNameMapper.TargetNameScope, HashSet<string>>();
        foreach (MigrationPlanObject item in planObjects.Where(item => item.Included))
        {
            DeterministicMigrationNameMapper.TargetNameScope scope =
                DeterministicMigrationNameMapper.GetTargetNameScope(
                    catalogObjectsById[item.SourceObjectId],
                    catalogObjectsById);
            if (!namesByScope.TryGetValue(scope, out HashSet<string>? names))
            {
                names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                namesByScope.Add(scope, names);
            }

            if (!names.Add(item.TargetName!))
            {
                throw Invalid(
                    $"Included plan objects have colliding target name '{item.TargetName}' in scope '{scope}'.");
            }
        }
    }

    private static void ValidateCollectionPhysicalTargetNames(
        IReadOnlyList<MigrationPlanObject> planObjects,
        IReadOnlyDictionary<string, MigrationCatalogObject> catalogObjectsById)
    {
        var includedTableNames = planObjects
            .Where(item =>
                item.Included &&
                catalogObjectsById[item.SourceObjectId].Kind == MigrationObjectKind.Table)
            .Select(item => item.TargetName!)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (MigrationPlanObject collection in planObjects.Where(item =>
                     item.Included &&
                     catalogObjectsById[item.SourceObjectId].Kind ==
                     MigrationObjectKind.Collection))
        {
            string logicalName = collection.TargetName!;
            if (logicalName.Length >
                MigrationDocumentCollectionContract.MaximumLogicalCollectionNameLength)
            {
                throw Invalid(
                    $"Included collection '{collection.SourceObjectId}' target name cannot fit the CSharpDB physical '{MigrationDocumentCollectionContract.CollectionPhysicalNamePrefix}' prefix within the {SqlIdentifierRules.MaxLength}-character identifier limit.");
            }

            string physicalName =
                MigrationDocumentCollectionContract.CollectionPhysicalNamePrefix + logicalName;
            if (includedTableNames.Contains(physicalName))
            {
                throw Invalid(
                    $"Included collection '{collection.SourceObjectId}' physical table name '{physicalName}' collides case-insensitively with an included target table.");
            }
        }
    }

    private static void ValidateSource(MigrationSourceIdentity source)
    {
        if (source is null)
            throw Invalid("Migration source identity is required.");

        RequireText(source.Identity, "Migration source identity");
        RequireText(source.Fingerprint, "Migration source fingerprint");

        if (source.Consistency is null)
            throw Invalid("Migration source consistency strategy is required.");

        RequireText(source.Consistency.Description, "Migration source consistency description");
    }

    private static IReadOnlyDictionary<string, MigrationDiagnostic> ValidateDiagnostics(
        IReadOnlyList<MigrationDiagnostic> diagnostics)
    {
        var byId = new Dictionary<string, MigrationDiagnostic>(StringComparer.Ordinal);
        foreach (MigrationDiagnostic diagnostic in diagnostics)
        {
            if (diagnostic is null)
                throw Invalid("Migration diagnostics cannot contain null values.");

            RequireText(diagnostic.DiagnosticId, "Migration diagnostic id");
            RequireText(diagnostic.RuleId, $"Rule id for diagnostic '{diagnostic.DiagnosticId}'");
            RequireText(diagnostic.Summary, $"Summary for migration diagnostic '{diagnostic.DiagnosticId}'");
            RequireText(
                diagnostic.Explanation,
                $"Explanation for migration diagnostic '{diagnostic.DiagnosticId}'");

            if (!byId.TryAdd(diagnostic.DiagnosticId, diagnostic))
                throw Invalid($"Migration diagnostic id '{diagnostic.DiagnosticId}' is duplicated.");
        }

        return byId;
    }

    private static void ValidateMapping(
        MigrationPlanObject planObject,
        MigrationTypeMapping mapping,
        MigrationMappingProfile planProfile,
        MigrationCatalogObject catalogObject,
        IReadOnlyDictionary<string, MigrationCatalogObject> catalogObjectsById,
        IReadOnlyDictionary<string, MigrationDiagnostic> diagnosticsById,
        IDataTypeMappingProvider mappingPolicy)
    {
        if (mapping is null)
            throw Invalid($"Plan object '{planObject.SourceObjectId}' contains a null type mapping.");

        RequireText(
            mapping.SourceObjectId,
            $"Mapping source object id in '{planObject.SourceObjectId}'");
        RequireText(
            mapping.SourceNativeType,
            $"Source native type for mapping '{mapping.SourceObjectId}'");

        if (!catalogObjectsById.ContainsKey(mapping.SourceObjectId))
            throw Invalid($"Mapping references unknown catalog object '{mapping.SourceObjectId}'.");
        if (!string.Equals(mapping.SourceObjectId, planObject.SourceObjectId, StringComparison.Ordinal))
        {
            throw Invalid(
                $"Plan object '{planObject.SourceObjectId}' cannot own mapping for '{mapping.SourceObjectId}'.");
        }
        if (!string.Equals(mapping.SourceNativeType, catalogObject.NativeType, StringComparison.Ordinal))
        {
            throw Invalid(
                $"Mapping '{mapping.SourceObjectId}' native type does not match the supplied catalog.");
        }

        if (mapping.Profile != planProfile)
        {
            throw Invalid(
                $"Mapping '{mapping.SourceObjectId}' uses profile '{mapping.Profile}' but the plan uses '{planProfile}'.");
        }

        if (planProfile != MigrationMappingProfile.Custom && mapping.RequestedTargetType is not null)
        {
            throw Invalid(
                $"Mapping '{mapping.SourceObjectId}' records a custom target outside the custom profile.");
        }
        if (mapping.RequestedTargetType == DbType.Null)
        {
            throw Invalid(
                $"Mapping '{mapping.SourceObjectId}' cannot request Null as a persistent target column type.");
        }

        if (mapping.Classification == MigrationMappingClassification.Unsupported)
        {
            if (mapping.TargetType is not null)
            {
                throw Invalid(
                    $"Unsupported mapping '{mapping.SourceObjectId}' cannot select a target type.");
            }

            if (planObject.Included)
            {
                throw Invalid(
                    $"Included plan object '{planObject.SourceObjectId}' contains unsupported mapping '{mapping.SourceObjectId}'.");
            }
        }
        else if (mapping.TargetType is null)
        {
            throw Invalid(
                $"Mapping '{mapping.SourceObjectId}' must select a target type unless it is unsupported.");
        }

        else if (mapping.TargetType == DbType.Null)
        {
            throw Invalid(
                $"Mapping '{mapping.SourceObjectId}' cannot select Null as a persistent target column type.");
        }

        if (mapping.Classification == MigrationMappingClassification.Exact && mapping.Conversion is not null)
            throw Invalid($"Exact mapping '{mapping.SourceObjectId}' cannot specify a conversion.");
        if (mapping.Classification is MigrationMappingClassification.LosslessReencoded or
            MigrationMappingClassification.Lossy)
        {
            ValidateConversion(mapping.SourceObjectId, mapping.Conversion);
        }
        else if (mapping.Classification == MigrationMappingClassification.Unsupported &&
                 mapping.Conversion is not null)
        {
            throw Invalid($"Unsupported mapping '{mapping.SourceObjectId}' cannot specify a conversion.");
        }

        if (mapping.Classification is MigrationMappingClassification.Lossy or
            MigrationMappingClassification.Unsupported)
        {
            RequireText(
                mapping.DiagnosticId,
                $"Diagnostic id for {mapping.Classification} mapping '{mapping.SourceObjectId}'");

            if (!diagnosticsById.TryGetValue(mapping.DiagnosticId!, out MigrationDiagnostic? diagnostic))
            {
                throw Invalid(
                    $"Mapping '{mapping.SourceObjectId}' references unknown diagnostic '{mapping.DiagnosticId}'.");
            }

            if (!string.Equals(diagnostic.ObjectId, mapping.SourceObjectId, StringComparison.Ordinal))
            {
                throw Invalid(
                    $"Mapping '{mapping.SourceObjectId}' diagnostic '{mapping.DiagnosticId}' targets a different object.");
            }

            if (mapping.Classification == MigrationMappingClassification.Lossy)
            {
                if (!diagnostic.CanOverride)
                {
                    throw Invalid(
                        $"Lossy mapping '{mapping.SourceObjectId}' references non-overrideable diagnostic '{mapping.DiagnosticId}'.");
                }
            }
        }

        ValidateCoverage(mapping.SourceObjectId, mapping.Coverage);

        MigrationTypeMappingDecision expected = mappingPolicy.Map(
            new MigrationTypeMappingRequest
            {
                SourceObject = catalogObject,
                Profile = planProfile,
                Coverage = mapping.Coverage,
                CustomTargetType = mapping.RequestedTargetType,
            });
        if (!MappingsEqual(mapping, expected.Mapping))
        {
            throw Invalid(
                $"Mapping '{mapping.SourceObjectId}' does not match mapping policy '{mappingPolicy.PolicyId}' version {mappingPolicy.PolicyVersion}.");
        }

        if (expected.Diagnostic is not null &&
            (!diagnosticsById.TryGetValue(
                expected.Diagnostic.DiagnosticId,
                out MigrationDiagnostic? actualDiagnostic) ||
             actualDiagnostic != expected.Diagnostic))
        {
            throw Invalid(
                $"Mapping '{mapping.SourceObjectId}' must retain mapping-policy diagnostic '{expected.Diagnostic.DiagnosticId}' unchanged.");
        }
    }

    private static bool MappingsEqual(MigrationTypeMapping left, MigrationTypeMapping right) =>
        string.Equals(left.SourceObjectId, right.SourceObjectId, StringComparison.Ordinal) &&
        string.Equals(left.SourceNativeType, right.SourceNativeType, StringComparison.Ordinal) &&
        left.TargetType == right.TargetType &&
        left.RequestedTargetType == right.RequestedTargetType &&
        left.Classification == right.Classification &&
        left.Profile == right.Profile &&
        left.Coverage == right.Coverage &&
        string.Equals(left.DiagnosticId, right.DiagnosticId, StringComparison.Ordinal) &&
        ConversionsEqual(left.Conversion, right.Conversion);

    private static bool ConversionsEqual(
        MigrationConversionDescriptor? left,
        MigrationConversionDescriptor? right)
    {
        if (left is null || right is null)
            return left is null && right is null;
        return string.Equals(left.ConversionId, right.ConversionId, StringComparison.Ordinal) &&
               left.Version == right.Version &&
               left.Parameters
                   .OrderBy(item => item.Name, StringComparer.Ordinal)
                   .ThenBy(item => item.Value, StringComparer.Ordinal)
                   .SequenceEqual(right.Parameters
                       .OrderBy(item => item.Name, StringComparer.Ordinal)
                       .ThenBy(item => item.Value, StringComparer.Ordinal));
    }

    private static IDataTypeMappingProvider ResolveBuiltInMappingPolicy(MigrationPlan plan)
    {
        if (string.Equals(
                plan.MappingPolicyId,
                StandardDataTypeMappingProvider.StandardPolicyId,
                StringComparison.Ordinal) &&
            plan.MappingPolicyVersion == StandardDataTypeMappingProvider.StandardPolicyVersion)
        {
            return new StandardDataTypeMappingProvider();
        }

        throw Invalid(
            $"Plan mapping policy '{plan.MappingPolicyId}' version {plan.MappingPolicyVersion} is not registered; supply the matching policy explicitly.");
    }

    private static void ValidateConversion(
        string sourceObjectId,
        MigrationConversionDescriptor? conversion)
    {
        if (conversion is null)
            throw Invalid($"Mapping '{sourceObjectId}' must specify a versioned conversion.");

        RequireText(conversion.ConversionId, $"Conversion id for mapping '{sourceObjectId}'");
        if (conversion.Version <= 0)
            throw Invalid($"Conversion for mapping '{sourceObjectId}' must have a positive version.");

        var parameterNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (MigrationCatalogFacet parameter in RequireList(
                     conversion.Parameters,
                     $"Conversion parameters for mapping '{sourceObjectId}'"))
        {
            if (parameter is null)
                throw Invalid($"Mapping '{sourceObjectId}' has a null conversion parameter.");
            RequireText(parameter.Name, $"Conversion parameter name for mapping '{sourceObjectId}'");
            if (!parameterNames.Add(parameter.Name))
            {
                throw Invalid(
                    $"Mapping '{sourceObjectId}' repeats conversion parameter '{parameter.Name}'.");
            }
        }
    }

    private static void ValidateCoverage(string sourceObjectId, MigrationProfileCoverage coverage)
    {
        if (coverage is null)
            throw Invalid($"Mapping '{sourceObjectId}' must include profile coverage.");

        if (coverage.ValuesExamined < 0 || coverage.TotalValues < 0)
            throw Invalid($"Mapping '{sourceObjectId}' has negative profile coverage counts.");

        if (coverage.TotalValues is long total && coverage.ValuesExamined > total)
        {
            throw Invalid(
                $"Mapping '{sourceObjectId}' examined more values than its reported total.");
        }

        if (coverage.Kind == MigrationCoverageKind.None && coverage.ValuesExamined != 0)
            throw Invalid($"Mapping '{sourceObjectId}' reports examined values with no profile coverage.");

        if (coverage.Kind == MigrationCoverageKind.Sample && !coverage.RequiresFullStreamValidation)
        {
            throw Invalid(
                $"Sample-derived mapping '{sourceObjectId}' must require full-stream validation during apply.");
        }

        if (coverage.Kind == MigrationCoverageKind.Full)
        {
            if (coverage.TotalValues is not long fullTotal)
                throw Invalid($"Full profile for mapping '{sourceObjectId}' must report its total value count.");
            if (coverage.ValuesExamined != fullTotal)
            {
                throw Invalid(
                    $"Full profile for mapping '{sourceObjectId}' must examine its reported total value count.");
            }
        }
    }

    private static void RequireSha256(string? value, string description)
    {
        RequireText(value, description);
        if (value!.Length != 64 || !value.All(Uri.IsHexDigit))
            throw Invalid($"{description} must be a 64-character hexadecimal SHA-256 digest.");
    }

    private static bool FixedTimeEquals(string left, string right)
    {
        byte[] leftBytes = Convert.FromHexString(left);
        byte[] rightBytes = Convert.FromHexString(right);
        return CryptographicOperations.FixedTimeEquals(leftBytes, rightBytes);
    }

    private static void ValidateAcyclic(
        IEnumerable<string> nodeIds,
        Func<string, IEnumerable<string>> getDependencies,
        string description)
    {
        string[] nodes = nodeIds.Distinct(StringComparer.Ordinal).ToArray();
        var indegree = nodes.ToDictionary(node => node, _ => 0, StringComparer.Ordinal);
        var dependents = nodes.ToDictionary(
            node => node,
            _ => new List<string>(),
            StringComparer.Ordinal);

        foreach (string node in nodes)
        {
            foreach (string dependency in getDependencies(node).Distinct(StringComparer.Ordinal))
            {
                if (!indegree.ContainsKey(dependency))
                    continue;
                indegree[node]++;
                dependents[dependency].Add(node);
            }
        }

        var ready = new Queue<string>(indegree
            .Where(item => item.Value == 0)
            .Select(item => item.Key)
            .OrderBy(item => item, StringComparer.Ordinal));
        int visited = 0;
        while (ready.Count > 0)
        {
            string node = ready.Dequeue();
            visited++;
            foreach (string dependent in dependents[node].OrderBy(item => item, StringComparer.Ordinal))
            {
                indegree[dependent]--;
                if (indegree[dependent] == 0)
                    ready.Enqueue(dependent);
            }
        }

        if (visited != nodes.Length)
            throw Invalid($"{description} contains a cycle.");
    }

    private static IReadOnlyList<T> RequireList<T>(IReadOnlyList<T>? values, string description) =>
        values ?? throw Invalid($"{description} cannot be null.");

    private static void RequireText(string? value, string description)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw Invalid($"{description} is required.");
    }

    private static InvalidDataException Invalid(string message) => new(message);
}
