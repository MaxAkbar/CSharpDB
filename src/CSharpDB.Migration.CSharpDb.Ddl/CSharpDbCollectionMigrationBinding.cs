using CSharpDB.Primitives;

namespace CSharpDB.Migration.CSharpDb;

internal sealed record CSharpDbCollectionMigrationBinding
{
    internal const string PhysicalTablePrefix =
        MigrationDocumentCollectionContract.CollectionPhysicalNamePrefix;

    internal required string SourceObjectId { get; init; }

    internal required string TargetName { get; init; }

    internal required string PhysicalTableName { get; init; }

    internal required string KeyColumnObjectId { get; init; }

    internal required string DocumentColumnObjectId { get; init; }

    internal required MigrationDocumentCollectionKeyMode KeyMode { get; init; }

    internal required int KeyValueIndex { get; init; }

    internal required int DocumentValueIndex { get; init; }

    internal static IReadOnlyDictionary<string, CSharpDbCollectionMigrationBinding> CreateAll(
        MigrationPlan plan,
        MigrationCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(catalog);

        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById = catalog.Objects
            .ToDictionary(item => item.ObjectId, StringComparer.Ordinal);
        IReadOnlyDictionary<string, MigrationPlanObject> plansById = plan.Objects
            .ToDictionary(item => item.SourceObjectId, StringComparer.Ordinal);
        var result = new Dictionary<string, CSharpDbCollectionMigrationBinding>(
            StringComparer.Ordinal);

        foreach (MigrationCatalogObject collection in catalog.Objects
                     .Where(item => item.Kind == MigrationObjectKind.Collection)
                     .Where(item =>
                         plansById.TryGetValue(item.ObjectId, out MigrationPlanObject? planned) &&
                         planned.Included)
                     .OrderBy(item => item.ObjectId, StringComparer.Ordinal))
        {
            if (!MigrationDocumentCollectionContract.TryBindSupportedV1Collection(
                    collection,
                    objectsById,
                    out MigrationDocumentCollectionBinding? contractBinding,
                    out string? reason))
            {
                throw new InvalidDataException(
                    $"Included collection '{collection.ObjectId}' does not satisfy a supported document contract: {reason}");
            }

            MigrationCatalogObject keyColumn = contractBinding!.KeyColumn;
            MigrationCatalogObject documentColumn =
                contractBinding.DocumentColumn;
            MigrationPlanObject collectionPlan = plansById[collection.ObjectId];
            MigrationPlanObject keyPlan = plansById[keyColumn.ObjectId];
            MigrationPlanObject documentPlan = plansById[documentColumn.ObjectId];
            if (!keyPlan.Included ||
                !documentPlan.Included ||
                keyPlan.TypeMappings.SingleOrDefault()?.TargetType != DbType.Text ||
                documentPlan.TypeMappings.SingleOrDefault()?.TargetType != DbType.Text ||
                !string.Equals(
                    keyPlan.TargetName,
                    MigrationDocumentCollectionContract.KeyColumnName,
                    StringComparison.Ordinal) ||
                !string.Equals(
                    documentPlan.TargetName,
                    MigrationDocumentCollectionContract.DocumentColumnName,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    $"Included collection '{collection.ObjectId}' has an invalid planned key/document projection.");
            }

            string targetName = collectionPlan.TargetName ??
                throw new InvalidDataException(
                    $"Included collection '{collection.ObjectId}' has no target name.");
            string physicalName = PhysicalName(targetName);
            string[] orderedColumnIds = catalog.Objects
                .Where(item =>
                    item.Kind == MigrationObjectKind.Column &&
                    string.Equals(
                        item.ParentObjectId,
                        collection.ObjectId,
                        StringComparison.Ordinal) &&
                    plansById[item.ObjectId].Included)
                .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
                .Select(item => item.ObjectId)
                .ToArray();
            int keyIndex = Array.IndexOf(orderedColumnIds, keyColumn.ObjectId);
            int documentIndex = Array.IndexOf(orderedColumnIds, documentColumn.ObjectId);
            if (orderedColumnIds.Length != 2 || keyIndex < 0 || documentIndex < 0)
            {
                throw new InvalidDataException(
                    $"Included collection '{collection.ObjectId}' has an incomplete planned row projection.");
            }

            result.Add(collection.ObjectId, new CSharpDbCollectionMigrationBinding
            {
                SourceObjectId = collection.ObjectId,
                TargetName = targetName,
                PhysicalTableName = physicalName,
                KeyColumnObjectId = keyColumn.ObjectId,
                DocumentColumnObjectId = documentColumn.ObjectId,
                KeyMode = contractBinding.KeyMode,
                KeyValueIndex = keyIndex,
                DocumentValueIndex = documentIndex,
            });
        }

        ValidatePhysicalNames(plan, catalog, result);
        return result;
    }

    internal static string PhysicalName(string targetName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(targetName);
        if (targetName.Length >
            MigrationDocumentCollectionContract.MaximumLogicalCollectionNameLength)
        {
            throw new InvalidDataException(
                $"Collection target name '{targetName}' is too long for its physical CSharpDB table.");
        }

        return PhysicalTablePrefix + targetName;
    }

    private static void ValidatePhysicalNames(
        MigrationPlan plan,
        MigrationCatalog catalog,
        IReadOnlyDictionary<string, CSharpDbCollectionMigrationBinding> bindings)
    {
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById = catalog.Objects
            .ToDictionary(item => item.ObjectId, StringComparer.Ordinal);
        var physicalNames = new Dictionary<string, string>(
            StringComparer.OrdinalIgnoreCase);
        foreach (MigrationPlanObject planned in plan.Objects
                     .Where(item => item.Included)
                     .Where(item =>
                         objectsById[item.SourceObjectId].Kind is
                             MigrationObjectKind.Table or
                             MigrationObjectKind.Collection)
                     .OrderBy(item => item.SourceObjectId, StringComparer.Ordinal))
        {
            string physicalName = bindings.TryGetValue(
                planned.SourceObjectId,
                out CSharpDbCollectionMigrationBinding? binding)
                ? binding.PhysicalTableName
                : planned.TargetName ??
                  throw new InvalidDataException(
                      $"Included data object '{planned.SourceObjectId}' has no target name.");
            if (!physicalNames.TryAdd(physicalName, planned.SourceObjectId))
            {
                throw new InvalidDataException(
                    $"Included data objects '{physicalNames[physicalName]}' and " +
                    $"'{planned.SourceObjectId}' collide on physical target name '{physicalName}'.");
            }
        }
    }
}
