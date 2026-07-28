using CSharpDB.Primitives;

namespace CSharpDB.Migration;

/// <summary>
/// Evaluates source facts and planned scalar mappings against the detailed
/// target capability rules. Conditional rules are accepted only when the
/// catalog carries enough structured evidence to prove the supported shape.
/// </summary>
internal sealed class CSharpDbTargetCapabilityEvaluator
{
    private readonly CSharpDbCapabilityCatalog _capabilities;

    public CSharpDbTargetCapabilityEvaluator(CSharpDbCapabilityCatalog capabilities)
    {
        ArgumentNullException.ThrowIfNull(capabilities);
        _capabilities = capabilities;
    }

    public bool CanEvaluateConditionalObject(MigrationCatalogObject item)
    {
        ArgumentNullException.ThrowIfNull(item);
        return item.Kind == MigrationObjectKind.Collection;
    }

    public string? GetExclusionReason(
        MigrationCatalogObject item,
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById,
        IReadOnlyDictionary<string, MigrationTypeMapping> mappingsByObjectId)
    {
        string? availabilityReason = EvaluateDataAvailability(item);
        if (availabilityReason is not null)
            return availabilityReason;

        return item.Kind switch
        {
            MigrationObjectKind.Collection => EvaluateCollection(item, objectsById, mappingsByObjectId),
            MigrationObjectKind.Column => EvaluateColumn(item, objectsById, mappingsByObjectId),
            MigrationObjectKind.Key => EvaluateKey(item, objectsById, mappingsByObjectId),
            MigrationObjectKind.ForeignKey => EvaluateForeignKey(item, objectsById, mappingsByObjectId),
            MigrationObjectKind.CheckConstraint => EvaluateCheck(item),
            MigrationObjectKind.Index => EvaluateIndex(item, objectsById, mappingsByObjectId),
            _ => null,
        };
    }

    private static string? EvaluateDataAvailability(
        MigrationCatalogObject item)
    {
        if (item.Kind is not (
                MigrationObjectKind.Table or
                MigrationObjectKind.Collection))
        {
            return null;
        }

        string? value = Facet(
            item,
            MigrationDataAvailabilityContract.AvailableFacet);
        if (value is null)
            return null;
        if (!bool.TryParse(value, out bool available))
        {
            return
                $"Source data availability for '{item.ObjectId}' is invalid.";
        }
        if (available)
            return null;

        string? reason = Facet(
            item,
            MigrationDataAvailabilityContract.UnavailableReasonFacet);
        return string.IsNullOrWhiteSpace(reason)
            ? $"Source data for '{item.ObjectId}' is not present in the retained migration package."
            : $"Source data for '{item.ObjectId}' is not present in the retained migration package: {reason}";
    }

    private string? EvaluateCollection(
        MigrationCatalogObject collection,
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById,
        IReadOnlyDictionary<string, MigrationTypeMapping> mappingsByObjectId)
    {
        CSharpDbCapabilityRule rule = Rule(
            MigrationObjectKind.Collection,
            CSharpDbCapabilityFeature.Object);
        string? statusReason = UnsupportedStatus(rule, "document collections");
        if (statusReason is not null)
            return statusReason;

        if (!MigrationDocumentCollectionContract.TryBindSupportedV1Collection(
                collection,
                objectsById,
                out MigrationDocumentCollectionBinding? binding,
                out string? bindingReason))
        {
            return Reject(rule, bindingReason!);
        }

        MigrationCatalogObject keyColumn = binding!.KeyColumn;
        MigrationCatalogObject documentColumn = binding.DocumentColumn;
        if (!mappingsByObjectId.TryGetValue(keyColumn!.ObjectId, out MigrationTypeMapping? keyMapping) ||
            keyMapping.TargetType != DbType.Text ||
            keyMapping.Classification != MigrationMappingClassification.Exact ||
            keyMapping.Conversion is not null)
        {
            return Reject(
                rule,
                $"requires collection key column '{keyColumn.ObjectId}' to map exactly to Text without conversion.");
        }

        if (!mappingsByObjectId.TryGetValue(
                documentColumn.ObjectId,
                out MigrationTypeMapping? documentMapping) ||
            documentMapping.TargetType != DbType.Text ||
            documentMapping.Classification != MigrationMappingClassification.LosslessReencoded ||
            documentMapping.Conversion is not
            {
                ConversionId: "canonical-text",
                Version: 1,
            } conversion ||
            conversion.Parameters.Count != 1 ||
            !conversion.Parameters.Any(parameter =>
                string.Equals(
                    parameter.Name,
                    MigrationDocumentCollectionContract.LogicalTypeFacet,
                    StringComparison.Ordinal) &&
                string.Equals(
                    parameter.Value,
                    MigrationDocumentCollectionContract.JsonLogicalType,
                    StringComparison.Ordinal)))
        {
            return Reject(
                rule,
                $"requires document column '{documentColumn.ObjectId}' to use the version 1 canonical-text JSON mapping.");
        }

        return null;
    }

    private string? EvaluateColumn(
        MigrationCatalogObject column,
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById,
        IReadOnlyDictionary<string, MigrationTypeMapping> mappingsByObjectId)
    {
        CSharpDbCapabilityRule typeRule = Rule(
            MigrationObjectKind.Column,
            CSharpDbCapabilityFeature.ColumnType);
        string? statusReason = UnsupportedStatus(typeRule, "persistent column types");
        if (statusReason is not null)
            return statusReason;

        if (!TryGetMappedType(column, mappingsByObjectId, out DbType targetType))
            return Reject(typeRule, $"does not have a supported mapped target type for '{column.ObjectId}'.");
        if (!_capabilities.IsColumnType(targetType) || !AllowsType(typeRule, targetType))
            return Reject(typeRule, $"does not allow mapped target type '{targetType}' for '{column.ObjectId}'.");

        CSharpDbCapabilityRule nullableRule = Rule(
            MigrationObjectKind.Column,
            CSharpDbCapabilityFeature.Nullable);
        statusReason = UnsupportedStatus(nullableRule, "column nullability");
        if (statusReason is not null)
            return statusReason;
        if (!TryOptionalBoolean(column, "nullable", out _, out string? booleanReason))
            return Reject(nullableRule, booleanReason!);

        string? defaultKind = Facet(column, "defaultKind");
        bool hasDefaultFacts = defaultKind is not null ||
            HasFacet(column, "defaultValue") ||
            HasFacet(column, "defaultExpression") ||
            HasFacet(column, "hasDefault");
        if (hasDefaultFacts)
        {
            CSharpDbCapabilityRule defaultRule = Rule(
                MigrationObjectKind.Column,
                CSharpDbCapabilityFeature.DefaultValue);
            statusReason = UnsupportedStatus(defaultRule, "column defaults");
            if (statusReason is not null)
                return statusReason;
            if (string.IsNullOrWhiteSpace(defaultKind))
                return Reject(defaultRule, $"cannot prove the default shape for '{column.ObjectId}' without a 'defaultKind' facet.");

            string normalizedDefaultKind = NormalizeToken(defaultKind);
            if (!AllowsValue(defaultRule, normalizedDefaultKind))
            {
                return Reject(
                    defaultRule,
                    $"does not allow default kind '{defaultKind}' for '{column.ObjectId}'.");
            }
        }

        if (!TryOptionalBoolean(column, "identity", out bool? identity, out booleanReason))
        {
            CSharpDbCapabilityRule identityRule = Rule(
                MigrationObjectKind.Column,
                CSharpDbCapabilityFeature.Identity);
            return Reject(identityRule, booleanReason!);
        }
        if (identity == true)
        {
            CSharpDbCapabilityRule identityRule = Rule(
                MigrationObjectKind.Column,
                CSharpDbCapabilityFeature.Identity);
            statusReason = UnsupportedStatus(identityRule, "identity columns");
            if (statusReason is not null)
                return statusReason;
            if (!AllowsType(identityRule, targetType))
                return Reject(identityRule, $"requires an allowed mapped type for identity column '{column.ObjectId}'.");

            int identityCount = objectsById.Values.Count(candidate =>
                candidate.Kind == MigrationObjectKind.Column &&
                string.Equals(candidate.ParentObjectId, column.ParentObjectId, StringComparison.Ordinal) &&
                IsTrue(candidate, "identity"));
            if (identityRule.MaxCount is int maximum && identityCount > maximum)
            {
                return Reject(
                    identityRule,
                    $"allows at most {maximum} identity column(s) in target parent '{column.ParentObjectId}'.");
            }

            if (!IsPrimaryKeyColumn(column, objectsById))
                return Reject(identityRule, $"requires identity column '{column.ObjectId}' to be part of the primary key.");
        }

        if (!TryOptionalBoolean(column, "rowVersion", out bool? rowVersion, out booleanReason))
        {
            CSharpDbCapabilityRule rowVersionRule = Rule(
                MigrationObjectKind.Column,
                CSharpDbCapabilityFeature.RowVersion);
            return Reject(rowVersionRule, booleanReason!);
        }
        if (rowVersion == true)
        {
            CSharpDbCapabilityRule rowVersionRule = Rule(
                MigrationObjectKind.Column,
                CSharpDbCapabilityFeature.RowVersion);
            statusReason = UnsupportedStatus(rowVersionRule, "rowversion columns");
            if (statusReason is not null)
                return statusReason;
            if (!AllowsType(rowVersionRule, targetType))
                return Reject(rowVersionRule, $"does not allow mapped type '{targetType}' for rowversion column '{column.ObjectId}'.");
            if (!TryOptionalBoolean(column, "nullable", out bool? nullable, out booleanReason) || nullable is not false)
                return Reject(rowVersionRule, $"requires rowversion column '{column.ObjectId}' to declare nullable=false.");

            int rowVersionCount = objectsById.Values.Count(candidate =>
                candidate.Kind == MigrationObjectKind.Column &&
                string.Equals(candidate.ParentObjectId, column.ParentObjectId, StringComparison.Ordinal) &&
                IsTrue(candidate, "rowVersion"));
            if (rowVersionRule.MaxCount is int maximum && rowVersionCount > maximum)
            {
                return Reject(
                    rowVersionRule,
                    $"allows at most {maximum} rowversion column(s) in target parent '{column.ParentObjectId}'.");
            }

            MigrationCatalogObject? usage = objectsById.Values
                .Where(candidate => candidate.Kind is MigrationObjectKind.Key or
                    MigrationObjectKind.ForeignKey or MigrationObjectKind.Index)
                .OrderBy(candidate => candidate.ObjectId, StringComparer.Ordinal)
                .FirstOrDefault(candidate => DependsOn(candidate, column.ObjectId, objectsById));
            if (usage is not null)
            {
                return Reject(
                    rowVersionRule,
                    $"does not allow rowversion column '{column.ObjectId}' to participate in '{usage.ObjectId}'.");
            }
        }

        return null;
    }

    private string? EvaluateKey(
        MigrationCatalogObject key,
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById,
        IReadOnlyDictionary<string, MigrationTypeMapping> mappingsByObjectId)
    {
        string? kind = Facet(key, "kind");
        CSharpDbCapabilityFeature feature = NormalizeToken(kind) switch
        {
            "primary" or "primary-key" => CSharpDbCapabilityFeature.PrimaryKey,
            "unique" or "unique-constraint" => CSharpDbCapabilityFeature.UniqueConstraint,
            _ => CSharpDbCapabilityFeature.Object,
        };
        CSharpDbCapabilityRule rule = Rule(MigrationObjectKind.Key, feature);
        if (feature == CSharpDbCapabilityFeature.Object)
            return Reject(rule, $"cannot prove whether key '{key.ObjectId}' is primary or unique.");

        string? statusReason = UnsupportedStatus(rule, feature == CSharpDbCapabilityFeature.PrimaryKey
            ? "primary keys"
            : "unique constraints");
        if (statusReason is not null)
            return statusReason;

        IReadOnlyList<MigrationCatalogObject>? columns = ResolveDirectColumns(key, objectsById);
        if (columns is null || columns.Count == 0)
            return Reject(rule, $"requires key '{key.ObjectId}' to depend only on one or more columns.");

        foreach (MigrationCatalogObject column in columns)
        {
            if (!string.Equals(column.ParentObjectId, key.ParentObjectId, StringComparison.Ordinal))
                return Reject(rule, $"requires key column '{column.ObjectId}' to belong to the key's target parent.");
            if (!TryGetMappedType(column, mappingsByObjectId, out DbType targetType) ||
                !AllowsType(rule, targetType))
            {
                return Reject(
                    rule,
                    $"does not allow the mapped target type for key column '{column.ObjectId}'.");
            }
            if (IsTrue(column, "rowVersion"))
                return Reject(rule, $"does not allow rowversion column '{column.ObjectId}' in a key.");
        }

        return null;
    }

    private string? EvaluateForeignKey(
        MigrationCatalogObject foreignKey,
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById,
        IReadOnlyDictionary<string, MigrationTypeMapping> mappingsByObjectId)
    {
        CSharpDbCapabilityRule rule = Rule(
            MigrationObjectKind.ForeignKey,
            CSharpDbCapabilityFeature.ForeignKey);
        string? statusReason = UnsupportedStatus(rule, "foreign keys");
        if (statusReason is not null)
            return statusReason;

        MigrationCatalogObject[] childColumns = ResolveForeignKeySourceColumns(foreignKey, objectsById);
        MigrationCatalogObject[] parentKeys = ResolveForeignKeyParentKeys(foreignKey, objectsById);
        if (childColumns.Length == 0 || parentKeys.Length != 1)
        {
            return Reject(
                rule,
                $"requires foreign key '{foreignKey.ObjectId}' to identify child columns and exactly one parent key dependency.");
        }

        MigrationCatalogObject parentKey = parentKeys[0];
        string parentKeyKind = NormalizeToken(Facet(parentKey, "kind"));
        if (parentKeyKind is not ("primary" or "primary-key" or "unique" or "unique-constraint"))
            return Reject(rule, $"requires parent dependency '{parentKey.ObjectId}' to be a primary or unique key.");

        IReadOnlyList<MigrationCatalogObject>? parentColumns = ResolveDirectColumns(parentKey, objectsById);
        if (parentColumns is null || parentColumns.Count != childColumns.Length)
            return Reject(rule, $"cannot prove equal child and parent column counts for '{foreignKey.ObjectId}'.");

        var childSignatures = new List<ColumnSignature>(childColumns.Length);
        var parentSignatures = new List<ColumnSignature>(parentColumns.Count);
        foreach (MigrationCatalogObject child in childColumns)
        {
            if (!TryCreateSignature(child, rule, mappingsByObjectId, out ColumnSignature signature))
                return Reject(rule, $"does not allow the mapped type of child column '{child.ObjectId}'.");
            childSignatures.Add(signature);
        }
        foreach (MigrationCatalogObject parent in parentColumns)
        {
            if (!TryCreateSignature(parent, rule, mappingsByObjectId, out ColumnSignature signature))
                return Reject(rule, $"does not allow the mapped type of parent column '{parent.ObjectId}'.");
            parentSignatures.Add(signature);
        }

        for (int ordinal = 0; ordinal < childSignatures.Count; ordinal++)
        {
            if (childSignatures[ordinal] != parentSignatures[ordinal])
            {
                return Reject(
                    rule,
                    $"requires matching child and parent target types and collations at ordinal {ordinal} for '{foreignKey.ObjectId}'.");
            }
        }

        if (!TryOptionalBoolean(foreignKey, "deferred", out bool? deferred, out string? booleanReason))
            return Reject(rule, booleanReason!);
        if (!TryOptionalBoolean(foreignKey, "deferrable", out bool? deferrable, out booleanReason))
            return Reject(rule, booleanReason!);
        if (deferred == true || deferrable == true)
            return Reject(rule, $"does not allow deferred foreign key '{foreignKey.ObjectId}'.");

        string timing = NormalizeToken(Facet(foreignKey, "timing") ?? "immediate");
        if (!AllowsValue(rule, timing))
            return Reject(rule, $"does not allow timing '{timing}' for '{foreignKey.ObjectId}'.");

        string match = NormalizeToken(Facet(foreignKey, "match") ?? "simple");
        string matchCapability = match.StartsWith("match-", StringComparison.Ordinal)
            ? match
            : $"match-{match}";
        if (!AllowsValue(rule, matchCapability))
            return Reject(rule, $"does not allow match mode '{match}' for '{foreignKey.ObjectId}'.");

        string onDelete = NormalizeToken(Facet(foreignKey, "onDelete") ?? "restrict");
        string onDeleteCapability = onDelete.StartsWith("on-delete-", StringComparison.Ordinal)
            ? onDelete
            : $"on-delete-{onDelete}";
        if (!AllowsValue(rule, onDeleteCapability))
            return Reject(rule, $"does not allow delete action '{onDelete}' for '{foreignKey.ObjectId}'.");
        if (onDeleteCapability == "on-delete-set-null")
        {
            foreach (MigrationCatalogObject child in childColumns)
            {
                if (!TryOptionalBoolean(child, "nullable", out bool? nullable, out booleanReason))
                    return Reject(rule, booleanReason!);
                if (nullable is not true)
                {
                    return Reject(
                        rule,
                        $"requires every child column of SET NULL foreign key '{foreignKey.ObjectId}' to prove nullable=true; '{child.ObjectId}' does not.");
                }
                if (IsPrimaryKeyColumn(child, objectsById))
                {
                    return Reject(
                        rule,
                        $"does not allow SET NULL foreign key '{foreignKey.ObjectId}' because child column '{child.ObjectId}' belongs to a primary key.");
                }
            }
        }

        string? onUpdate = Facet(foreignKey, "onUpdate");
        if (!string.IsNullOrWhiteSpace(onUpdate))
        {
            string normalizedOnUpdate = NormalizeToken(onUpdate);
            string onUpdateCapability =
                normalizedOnUpdate.StartsWith("on-update-", StringComparison.Ordinal)
                    ? normalizedOnUpdate
                    : $"on-update-{normalizedOnUpdate}";
            if (!AllowsValue(rule, onUpdateCapability))
            {
                return Reject(
                    rule,
                    $"does not allow update action '{normalizedOnUpdate}' for '{foreignKey.ObjectId}'.");
            }
        }

        return null;
    }

    private string? EvaluateCheck(MigrationCatalogObject check)
    {
        CSharpDbCapabilityRule rule = Rule(
            MigrationObjectKind.CheckConstraint,
            CSharpDbCapabilityFeature.CheckConstraint);
        string? statusReason = UnsupportedStatus(rule, "check constraints");
        if (statusReason is not null)
            return statusReason;

        if (!AllowsValue(rule, "deterministic") || !AllowsValue(rule, "row-local"))
            return Reject(rule, "does not advertise deterministic, row-local checks.");
        if (!TryOptionalBoolean(check, "deterministic", out bool? deterministic, out string? booleanReason) ||
            deterministic is not true)
        {
            return Reject(rule, $"requires check '{check.ObjectId}' to prove deterministic=true.");
        }
        if (!TryOptionalBoolean(check, "rowLocal", out bool? rowLocal, out booleanReason) || rowLocal is not true)
            return Reject(rule, $"requires check '{check.ObjectId}' to prove rowLocal=true.");

        foreach (string unsupportedFacet in new[] { "hasFunctions", "hasSubquery", "hasParameters" })
        {
            if (IsTrue(check, unsupportedFacet))
                return Reject(rule, $"does not allow {unsupportedFacet} on check '{check.ObjectId}'.");
        }

        return null;
    }

    private string? EvaluateIndex(
        MigrationCatalogObject index,
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById,
        IReadOnlyDictionary<string, MigrationTypeMapping> mappingsByObjectId)
    {
        CSharpDbCapabilityRule rule = Rule(
            MigrationObjectKind.Index,
            CSharpDbCapabilityFeature.Index);
        string? statusReason = UnsupportedStatus(rule, "indexes");
        if (statusReason is not null)
            return statusReason;

        IReadOnlyList<MigrationCatalogObject>? columns = ResolveDirectColumns(index, objectsById);
        if (columns is null || columns.Count == 0)
            return Reject(rule, $"requires index '{index.ObjectId}' to depend only on one or more columns.");

        foreach (MigrationCatalogObject column in columns)
        {
            if (!TryGetMappedType(column, mappingsByObjectId, out DbType targetType) ||
                !AllowsType(rule, targetType))
            {
                return Reject(
                    rule,
                    $"does not allow the mapped target type for index column '{column.ObjectId}'.");
            }
            if (IsTrue(column, "rowVersion"))
                return Reject(rule, $"does not allow rowversion column '{column.ObjectId}' in an index.");
        }

        if (!AllowsValue(rule, "column-only"))
            return Reject(rule, "does not advertise column-only index support.");
        if (columns.Count > 1 && !AllowsValue(rule, "composite"))
            return Reject(rule, "does not advertise composite index support.");

        if (!TryOptionalBoolean(index, "unique", out bool? unique, out string? booleanReason))
            return Reject(rule, booleanReason!);
        string uniqueness = unique == true ? "unique" : "nonunique";
        if (!AllowsValue(rule, uniqueness))
            return Reject(rule, $"does not advertise {uniqueness} index support.");

        string? kind = Facet(index, "kind");
        if (!string.IsNullOrWhiteSpace(kind) &&
            NormalizeToken(kind) is not ("standard" or "btree" or "b-tree"))
        {
            return Reject(rule, $"cannot prove index kind '{kind}' is a supported column index.");
        }

        foreach (string unsupportedFacet in new[]
                 {
                     "expression", "expressionSql", "partial", "predicate", "filter",
                     "where", "includedColumns", "include", "sortDirection", "sortDirections",
                 })
        {
            if (FacetHasUnsupportedValue(index, unsupportedFacet))
                return Reject(rule, $"does not allow facet '{unsupportedFacet}' on index '{index.ObjectId}'.");
        }

        return null;
    }

    private CSharpDbCapabilityRule Rule(
        MigrationObjectKind objectKind,
        CSharpDbCapabilityFeature feature) =>
        _capabilities.Rules.Single(rule => rule.ObjectKind == objectKind && rule.Feature == feature);

    private static string? UnsupportedStatus(CSharpDbCapabilityRule rule, string description) =>
        rule.Status is MigrationCompatibilityStatus.Unsupported or MigrationCompatibilityStatus.Unknown
            ? Reject(rule, $"reports {description} as {rule.Status}.")
            : null;

    private static bool AllowsType(CSharpDbCapabilityRule rule, DbType targetType) =>
        rule.AllowedTypes.Count == 0 || rule.AllowedTypes.Contains(targetType);

    private static bool AllowsValue(CSharpDbCapabilityRule rule, string value) =>
        rule.AllowedValues.Any(allowed => string.Equals(allowed, value, StringComparison.Ordinal));

    private static bool TryGetMappedType(
        MigrationCatalogObject column,
        IReadOnlyDictionary<string, MigrationTypeMapping> mappingsByObjectId,
        out DbType targetType)
    {
        if (mappingsByObjectId.TryGetValue(column.ObjectId, out MigrationTypeMapping? mapping) &&
            mapping.TargetType is DbType mapped &&
            mapping.Classification != MigrationMappingClassification.Unsupported)
        {
            targetType = mapped;
            return true;
        }

        targetType = default;
        return false;
    }

    private static IReadOnlyList<MigrationCatalogObject>? ResolveDirectColumns(
        MigrationCatalogObject item,
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById)
    {
        IEnumerable<string> columnIds = item.Members.Count > 0
            ? item.Members
                .Where(member => member.Role == MigrationObjectReferenceRoles.Column)
                .OrderBy(member => member.Ordinal)
                .Select(member => member.ObjectId)
            : item.DependsOn.OrderBy(id => id, StringComparer.Ordinal);
        var columns = new List<MigrationCatalogObject>();
        foreach (string dependency in columnIds)
        {
            if (!objectsById.TryGetValue(dependency, out MigrationCatalogObject? candidate) ||
                candidate.Kind != MigrationObjectKind.Column)
            {
                return null;
            }

            columns.Add(candidate);
        }

        return columns;
    }

    private static MigrationCatalogObject[] ResolveForeignKeySourceColumns(
        MigrationCatalogObject foreignKey,
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById)
    {
        IEnumerable<string> objectIds = foreignKey.Members.Count > 0
            ? foreignKey.Members
                .Where(member => member.Role == MigrationObjectReferenceRoles.SourceColumn)
                .OrderBy(member => member.Ordinal)
                .Select(member => member.ObjectId)
            : foreignKey.DependsOn
                .Where(objectsById.ContainsKey)
                .Where(id => objectsById[id].Kind == MigrationObjectKind.Column)
                .OrderBy(id => id, StringComparer.Ordinal);

        return objectIds
            .Where(objectsById.ContainsKey)
            .Select(id => objectsById[id])
            .Where(candidate => candidate.Kind == MigrationObjectKind.Column &&
                string.Equals(candidate.ParentObjectId, foreignKey.ParentObjectId, StringComparison.Ordinal))
            .ToArray();
    }

    private static MigrationCatalogObject[] ResolveForeignKeyParentKeys(
        MigrationCatalogObject foreignKey,
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById)
    {
        IEnumerable<string> objectIds = foreignKey.Members.Count > 0
            ? foreignKey.Members
                .Where(member => member.Role == MigrationObjectReferenceRoles.ReferencedKey)
                .OrderBy(member => member.Ordinal)
                .Select(member => member.ObjectId)
            : foreignKey.DependsOn
                .Where(objectsById.ContainsKey)
                .Where(id => objectsById[id].Kind == MigrationObjectKind.Key)
                .OrderBy(id => id, StringComparer.Ordinal);

        return objectIds
            .Where(objectsById.ContainsKey)
            .Select(id => objectsById[id])
            .Where(candidate => candidate.Kind == MigrationObjectKind.Key)
            .ToArray();
    }

    private static bool TryCreateSignature(
        MigrationCatalogObject column,
        CSharpDbCapabilityRule rule,
        IReadOnlyDictionary<string, MigrationTypeMapping> mappingsByObjectId,
        out ColumnSignature signature)
    {
        if (!TryGetMappedType(column, mappingsByObjectId, out DbType targetType) ||
            !AllowsType(rule, targetType) || IsTrue(column, "rowVersion"))
        {
            signature = default;
            return false;
        }

        signature = new ColumnSignature(
            targetType,
            NormalizeToken(Facet(column, "collation") ?? "default"));
        return true;
    }

    private static bool IsPrimaryKeyColumn(
        MigrationCatalogObject column,
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById)
    {
        if (IsTrue(column, "primaryKey"))
            return true;

        return objectsById.Values.Any(candidate =>
            candidate.Kind == MigrationObjectKind.Key &&
            string.Equals(candidate.ParentObjectId, column.ParentObjectId, StringComparison.Ordinal) &&
            NormalizeToken(Facet(candidate, "kind")) is "primary" or "primary-key" &&
            ResolveDirectColumns(candidate, objectsById)?.Any(
                member => member.ObjectId == column.ObjectId) == true);
    }

    private static bool DependsOn(
        MigrationCatalogObject item,
        string dependencyId,
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById)
    {
        var pending = new Stack<string>(item.DependsOn);
        var visited = new HashSet<string>(StringComparer.Ordinal);
        while (pending.TryPop(out string? current))
        {
            if (string.Equals(current, dependencyId, StringComparison.Ordinal))
                return true;
            if (!visited.Add(current) || !objectsById.TryGetValue(current, out MigrationCatalogObject? dependency))
                continue;
            foreach (string nested in dependency.DependsOn)
                pending.Push(nested);
        }

        return false;
    }

    private static bool TryOptionalBoolean(
        MigrationCatalogObject item,
        string facetName,
        out bool? value,
        out string? reason)
    {
        MigrationCatalogFacet? facet = item.Facets.FirstOrDefault(candidate =>
            string.Equals(candidate.Name, facetName, StringComparison.Ordinal));
        if (facet is null)
        {
            value = null;
            reason = null;
            return true;
        }

        if (bool.TryParse(facet.Value, out bool parsed))
        {
            value = parsed;
            reason = null;
            return true;
        }

        value = null;
        reason = $"requires facet '{facetName}' on '{item.ObjectId}' to be true or false.";
        return false;
    }

    private static bool IsTrue(MigrationCatalogObject item, string facetName) =>
        bool.TryParse(Facet(item, facetName), out bool parsed) && parsed;

    private static bool FacetHasUnsupportedValue(MigrationCatalogObject item, string facetName)
    {
        string? value = Facet(item, facetName);
        if (value is null)
            return false;
        if (bool.TryParse(value, out bool flag))
            return flag;

        string normalized = NormalizeToken(value);
        return normalized is not ("" or "none" or "default");
    }

    private static bool HasFacet(MigrationCatalogObject item, string facetName) =>
        item.Facets.Any(facet => string.Equals(facet.Name, facetName, StringComparison.Ordinal));

    private static string? Facet(MigrationCatalogObject item, string facetName) =>
        item.Facets.FirstOrDefault(facet =>
            string.Equals(facet.Name, facetName, StringComparison.Ordinal))?.Value;

    private static string NormalizeToken(string? value) =>
        (value ?? string.Empty).Trim().Replace('_', '-').ToLowerInvariant();

    private static string Reject(CSharpDbCapabilityRule rule, string detail) =>
        $"Capability rule '{rule.RuleId}' {detail}";

    private readonly record struct ColumnSignature(DbType Type, string Collation);
}
