using System.Security.Cryptography;
using System.Text.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.CSharpDb;

internal static class CSharpDbMigrationSql
{
    private const string CollectionActionPrefix =
        "csharpdb-migration-json-collection-action/v1:";

    internal const string StateTable = "__csharpdb_migration_state";
    internal const string StageTable = "__csharpdb_migration_stages";
    internal const string ReceiptTable = "__csharpdb_migration_receipts";
    internal const string RejectTable = "__csharpdb_migration_rejects";
    internal const string ValidationReceiptTable = "__csharpdb_migration_validation_receipt";

    internal const string LegacyTargetTag = "csharpdb-staged-migration-target/v1";
    internal const string OutcomeUnboundTargetTag = "csharpdb-staged-migration-target/v2";
    internal const string TargetTag = "csharpdb-staged-migration-target/v3";
    internal const string StageTag = "csharpdb-migration-schema-stage/v1";
    internal const string LegacyReceiptTag = "csharpdb-migration-batch-receipt/v1";
    internal const string ReceiptTag = "csharpdb-migration-batch-receipt/v2";
    internal const string RejectTag = "csharpdb-migration-reject-entry/v1";
    internal const string ValidationReceiptTag = MigrationValidationActivationReceipt.ContractVersion;

    internal const string CreatedState = "created";
    internal const string LoadingDataState = "loading-data";
    internal const string SecondaryIndexesState = "secondary-indexes";
    internal const string ConstraintsState = "constraints";
    internal const string ViewsState = "views";
    internal const string AwaitingValidationState = "awaiting-validation";
    internal const string ActivatedState = "activated";

    internal static IReadOnlyList<string> BuildInternalSchemaActions() =>
    [
        $"CREATE TABLE {Quote(StateTable)} (" +
        $"{Quote("singleton")} INTEGER PRIMARY KEY, " +
        $"{Quote("target_tag")} TEXT NOT NULL, " +
        $"{Quote("target_identity")} TEXT NOT NULL, " +
        $"{Quote("plan_digest")} TEXT NOT NULL, " +
        $"{Quote("catalog_digest")} TEXT NOT NULL, " +
        $"{Quote("capability_digest")} TEXT NOT NULL, " +
        $"{Quote("target_version")} TEXT NOT NULL, " +
        $"{Quote("source_kind")} TEXT NOT NULL, " +
        $"{Quote("source_identity")} TEXT NOT NULL, " +
        $"{Quote("source_fingerprint")} TEXT NOT NULL, " +
        $"{Quote("source_snapshot_identity")} TEXT NOT NULL, " +
        $"{Quote("lifecycle_state")} TEXT NOT NULL)",

        $"CREATE TABLE {Quote(StageTable)} (" +
        $"{Quote("stage_tag")} TEXT NOT NULL, " +
        $"{Quote("target_identity")} TEXT NOT NULL, " +
        $"{Quote("plan_digest")} TEXT NOT NULL, " +
        $"{Quote("stage_ordinal")} INTEGER NOT NULL, " +
        $"{Quote("stage_name")} TEXT NOT NULL, " +
        $"{Quote("stage_digest")} TEXT NOT NULL, " +
        $"{Quote("action_count")} INTEGER NOT NULL, " +
        $"CONSTRAINT {Quote("__csharpdb_migration_stages_pk")} " +
        $"PRIMARY KEY ({Quote("plan_digest")}, {Quote("stage_ordinal")}))",

        $"CREATE TABLE {Quote(ReceiptTable)} (" +
        $"{Quote("receipt_tag")} TEXT NOT NULL, " +
        $"{Quote("target_identity")} TEXT NOT NULL, " +
        $"{Quote("plan_digest")} TEXT NOT NULL, " +
        $"{Quote("catalog_digest")} TEXT NOT NULL, " +
        $"{Quote("source_fingerprint")} TEXT NOT NULL, " +
        $"{Quote("source_snapshot_identity")} TEXT NOT NULL, " +
        $"{Quote("source_object_id")} TEXT NOT NULL, " +
        $"{Quote("batch_ordinal")} INTEGER NOT NULL, " +
        $"{Quote("start_cursor")} TEXT, " +
        $"{Quote("next_cursor")} TEXT, " +
        $"{Quote("batch_digest")} TEXT NOT NULL, " +
        $"{Quote("reject_contract_version")} TEXT NOT NULL, " +
        $"{Quote("reject_digest")} TEXT NOT NULL, " +
        $"{Quote("row_count")} INTEGER NOT NULL, " +
        $"{Quote("rejected_row_count")} INTEGER NOT NULL, " +
        $"CONSTRAINT {Quote("__csharpdb_migration_receipts_pk")} " +
        $"PRIMARY KEY ({Quote("plan_digest")}, {Quote("source_object_id")}, {Quote("batch_ordinal")}))",

        $"CREATE TABLE {Quote(RejectTable)} (" +
        $"{Quote("reject_tag")} TEXT NOT NULL, " +
        $"{Quote("plan_digest")} TEXT NOT NULL, " +
        $"{Quote("source_object_id")} TEXT NOT NULL, " +
        $"{Quote("batch_ordinal")} INTEGER NOT NULL, " +
        $"{Quote("source_row_ordinal")} INTEGER NOT NULL, " +
        $"{Quote("rule_id")} TEXT NOT NULL, " +
        $"{Quote("column_object_id")} TEXT, " +
        $"{Quote("evidence_json")} TEXT NOT NULL, " +
        $"CONSTRAINT {Quote("__csharpdb_migration_rejects_pk")} " +
        $"PRIMARY KEY ({Quote("plan_digest")}, {Quote("source_object_id")}, " +
        $"{Quote("batch_ordinal")}, {Quote("source_row_ordinal")}))",

        $"CREATE TABLE {Quote(ValidationReceiptTable)} (" +
        $"{Quote("singleton")} INTEGER PRIMARY KEY, " +
        $"{Quote("receipt_tag")} TEXT NOT NULL, " +
        $"{Quote("target_identity")} TEXT NOT NULL, " +
        $"{Quote("plan_digest")} TEXT NOT NULL, " +
        $"{Quote("catalog_digest")} TEXT NOT NULL, " +
        $"{Quote("source_snapshot_identity")} TEXT NOT NULL, " +
        $"{Quote("target_snapshot_identity")} TEXT NOT NULL, " +
        $"{Quote("validation_level")} INTEGER NOT NULL, " +
        $"{Quote("canonicalization_version")} TEXT NOT NULL, " +
        $"{Quote("canonicalization_contract_digest")} TEXT NOT NULL, " +
        $"{Quote("report_digest")} TEXT NOT NULL)",
    ];

    internal static IReadOnlyList<string> BuildStageActions(
        MigrationPlan plan,
        MigrationCatalog catalog,
        MigrationSchemaStage stage)
    {
        IReadOnlyDictionary<string, MigrationPlanObject> planObjects = plan.Objects
            .ToDictionary(item => item.SourceObjectId, StringComparer.Ordinal);
        IReadOnlyDictionary<string, MigrationCatalogObject> catalogObjects = catalog.Objects
            .ToDictionary(item => item.ObjectId, StringComparer.Ordinal);

        return stage switch
        {
            MigrationSchemaStage.LoadEssential => BuildTables(planObjects, catalog),
            MigrationSchemaStage.SecondaryIndexes => BuildIndexes(planObjects, catalogObjects),
            MigrationSchemaStage.Constraints => BuildConstraints(planObjects, catalogObjects),
            MigrationSchemaStage.Views => BuildViews(planObjects, catalog),
            MigrationSchemaStage.Triggers => BuildTriggers(planObjects, catalog),
            _ => throw new ArgumentOutOfRangeException(nameof(stage), stage, "Unknown migration schema stage."),
        };
    }

    internal static string ComputeStageDigest(
        MigrationPlan plan,
        MigrationSchemaStage stage,
        IReadOnlyList<string> actions)
    {
        byte[] input = JsonSerializer.SerializeToUtf8Bytes(new
        {
            Format = StageTag,
            PlanDigest = MigrationArtifactSerializer.ComputePlanDigest(plan),
            Stage = stage.ToString(),
            ActionCount = actions.Count,
            Actions = actions,
        });
        return Convert.ToHexString(SHA256.HashData(input)).ToLowerInvariant();
    }

    internal static string Quote(string identifier) => SqlIdentifierRules.Quote(identifier);

    internal static string Literal(string value) => $"'{value.Replace("'", "''", StringComparison.Ordinal)}'";

    internal static string NullableLiteral(string? value) => value is null ? "NULL" : Literal(value);

    internal static bool TryParseCollectionAction(
        string action,
        out string collectionName)
    {
        ArgumentNullException.ThrowIfNull(action);
        if (!action.StartsWith(CollectionActionPrefix, StringComparison.Ordinal))
        {
            collectionName = string.Empty;
            return false;
        }

        collectionName = action[CollectionActionPrefix.Length..];
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new InvalidDataException("Migration collection action has no target name.");
        return true;
    }

    private static IReadOnlyList<string> BuildTables(
        IReadOnlyDictionary<string, MigrationPlanObject> planObjects,
        MigrationCatalog catalog)
    {
        var actions = new List<string>();
        foreach (MigrationCatalogObject table in catalog.Objects
                     .Where(item => item.Kind is MigrationObjectKind.Table or MigrationObjectKind.Collection)
                     .Where(item => planObjects[item.ObjectId].Included)
                     .OrderBy(item => item.ObjectId, StringComparer.Ordinal))
        {
            if (table.Kind == MigrationObjectKind.Collection)
            {
                actions.Add(
                    CollectionActionPrefix +
                    (planObjects[table.ObjectId].TargetName ??
                     throw new InvalidDataException(
                         $"Included collection '{table.ObjectId}' has no target name.")));
                continue;
            }

            MigrationCatalogObject[] columns = catalog.Objects
                .Where(item => item.Kind == MigrationObjectKind.Column &&
                    string.Equals(item.ParentObjectId, table.ObjectId, StringComparison.Ordinal) &&
                    planObjects[item.ObjectId].Included)
                .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
                .ToArray();
            if (columns.Length == 0)
                throw new InvalidDataException($"Included table '{table.ObjectId}' has no included columns.");

            string[] definitions = columns.Select(column =>
            {
                MigrationPlanObject planned = planObjects[column.ObjectId];
                MigrationTypeMapping mapping = planned.TypeMappings.Single();
                if (mapping.TargetType is not DbType targetType || targetType == DbType.Null)
                    throw new InvalidDataException($"Included column '{column.ObjectId}' has no persistent target type.");
                if (HasFacet(column, "defaultKind") || HasFacet(column, "defaultValue") ||
                    HasFacet(column, "defaultExpression") || IsTrue(column, "identity") || IsTrue(column, "rowVersion"))
                {
                    throw new NotSupportedException(
                        $"Included column '{column.ObjectId}' requires a default, identity, or rowversion lowering that is not in the Phase 2 staged slice.");
                }

                string definition = $"{Quote(planned.TargetName!)} {TypeName(targetType)}";
                string? collation = Facet(column, "collation");
                if (!string.IsNullOrWhiteSpace(collation))
                {
                    if (collation.Any(character => !(char.IsLetterOrDigit(character) || character is '_' or '-' or ':')))
                        throw new InvalidDataException($"Column '{column.ObjectId}' contains an unsafe collation token.");
                    definition += $" COLLATE {collation}";
                }
                if (!IsNullable(column))
                    definition += " NOT NULL";
                return definition;
            }).ToArray();

            actions.Add($"CREATE TABLE {Quote(planObjects[table.ObjectId].TargetName!)} ({string.Join(", ", definitions)})");
        }
        return actions;
    }

    private static IReadOnlyList<string> BuildIndexes(
        IReadOnlyDictionary<string, MigrationPlanObject> planObjects,
        IReadOnlyDictionary<string, MigrationCatalogObject> catalogObjects)
    {
        var actions = new List<string>();
        foreach (MigrationCatalogObject index in catalogObjects.Values
                     .Where(item => item.Kind == MigrationObjectKind.Index && planObjects[item.ObjectId].Included)
                     .OrderBy(item => item.ObjectId, StringComparer.Ordinal))
        {
            MigrationPlanObject table = ParentPlan(index, planObjects);
            string[] columns = OrderedMemberIds(index, MigrationObjectReferenceRoles.Column)
                .Select(id => Quote(planObjects[id].TargetName!))
                .ToArray();
            if (columns.Length == 0)
                throw new InvalidDataException($"Included index '{index.ObjectId}' has no ordered columns.");
            string unique = IsTrue(index, "unique") ? "UNIQUE " : string.Empty;
            actions.Add(
                $"CREATE {unique}INDEX {Quote(planObjects[index.ObjectId].TargetName!)} ON {Quote(table.TargetName!)} ({string.Join(", ", columns)})");
        }
        return actions;
    }

    private static IReadOnlyList<string> BuildConstraints(
        IReadOnlyDictionary<string, MigrationPlanObject> planObjects,
        IReadOnlyDictionary<string, MigrationCatalogObject> catalogObjects)
    {
        var actions = new List<string>();
        IEnumerable<MigrationCatalogObject> included = catalogObjects.Values
            .Where(item => planObjects[item.ObjectId].Included);

        foreach (MigrationCatalogObject key in included
                     .Where(item => item.Kind == MigrationObjectKind.Key)
                     .OrderBy(item => item.ObjectId, StringComparer.Ordinal))
        {
            string kind = Normalize(Facet(key, "kind"));
            string constraint = kind switch
            {
                "primary" or "primary-key" => "PRIMARY KEY",
                "unique" or "unique-constraint" => "UNIQUE",
                _ => throw new InvalidDataException($"Included key '{key.ObjectId}' has unknown kind '{kind}'."),
            };
            string[] columns = OrderedMemberIds(key, MigrationObjectReferenceRoles.Column)
                .Select(id => Quote(planObjects[id].TargetName!))
                .ToArray();
            if (columns.Length == 0)
                throw new InvalidDataException($"Included key '{key.ObjectId}' has no ordered columns.");
            MigrationPlanObject table = ParentPlan(key, planObjects);
            actions.Add(
                $"ALTER TABLE {Quote(table.TargetName!)} ADD CONSTRAINT {Quote(planObjects[key.ObjectId].TargetName!)} {constraint} ({string.Join(", ", columns)})");
        }

        foreach (MigrationCatalogObject foreignKey in included
                     .Where(item => item.Kind == MigrationObjectKind.ForeignKey)
                     .OrderBy(item => item.ObjectId, StringComparer.Ordinal))
        {
            string[] sourceColumns = OrderedMemberIds(foreignKey, MigrationObjectReferenceRoles.SourceColumn)
                .Select(id => Quote(planObjects[id].TargetName!))
                .ToArray();
            string referencedKeyId = OrderedMemberIds(foreignKey, MigrationObjectReferenceRoles.ReferencedKey).Single();
            MigrationCatalogObject referencedKey = catalogObjects[referencedKeyId];
            string[] referencedColumns = OrderedMemberIds(referencedKey, MigrationObjectReferenceRoles.Column)
                .Select(id => Quote(planObjects[id].TargetName!))
                .ToArray();
            if (sourceColumns.Length == 0 || sourceColumns.Length != referencedColumns.Length)
                throw new InvalidDataException($"Included foreign key '{foreignKey.ObjectId}' has inconsistent ordered members.");

            MigrationPlanObject sourceTable = ParentPlan(foreignKey, planObjects);
            MigrationPlanObject referencedTable = ParentPlan(referencedKey, planObjects);
            string action =
                $"ALTER TABLE {Quote(sourceTable.TargetName!)} ADD CONSTRAINT {Quote(planObjects[foreignKey.ObjectId].TargetName!)} " +
                $"FOREIGN KEY ({string.Join(", ", sourceColumns)}) REFERENCES {Quote(referencedTable.TargetName!)} ({string.Join(", ", referencedColumns)})";
            string onDelete = Normalize(Facet(foreignKey, "onDelete"));
            if (onDelete.StartsWith("on-delete-", StringComparison.Ordinal))
                onDelete = onDelete["on-delete-".Length..];
            if (!string.IsNullOrEmpty(onDelete) && onDelete != "restrict")
            {
                action += onDelete switch
                {
                    "cascade" => " ON DELETE CASCADE",
                    _ => throw new InvalidDataException(
                        $"Included foreign key '{foreignKey.ObjectId}' has unsupported delete action '{onDelete}'."),
                };
            }
            actions.Add(action);
        }

        foreach (MigrationCatalogObject check in included
                     .Where(item => item.Kind == MigrationObjectKind.CheckConstraint)
                     .OrderBy(item => item.ObjectId, StringComparer.Ordinal))
        {
            string expression = Facet(check, "targetSql") ??
                throw new NotSupportedException(
                    $"Included check '{check.ObjectId}' requires a scratch-validated 'targetSql' facet.");
            MigrationPlanObject table = ParentPlan(check, planObjects);
            actions.Add(
                $"ALTER TABLE {Quote(table.TargetName!)} ADD CONSTRAINT {Quote(planObjects[check.ObjectId].TargetName!)} CHECK ({expression})");
        }

        return actions;
    }

    private static IReadOnlyList<string> BuildViews(
        IReadOnlyDictionary<string, MigrationPlanObject> planObjects,
        MigrationCatalog catalog) => catalog.Objects
        .Where(item => item.Kind == MigrationObjectKind.View && planObjects[item.ObjectId].Included)
        .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
        .Select(item =>
        {
            string sql = Facet(item, "targetSql") ??
                throw new NotSupportedException(
                    $"Included view '{item.ObjectId}' requires a scratch-validated 'targetSql' facet.");
            return $"CREATE VIEW {Quote(planObjects[item.ObjectId].TargetName!)} AS {sql}";
        })
        .ToArray();

    private static IReadOnlyList<string> BuildTriggers(
        IReadOnlyDictionary<string, MigrationPlanObject> planObjects,
        MigrationCatalog catalog) => catalog.Objects
        .Where(item => item.Kind == MigrationObjectKind.Trigger && planObjects[item.ObjectId].Included)
        .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
        .Select(item => Facet(item, "targetSql") ??
            throw new NotSupportedException(
                $"Included trigger '{item.ObjectId}' requires a scratch-validated 'targetSql' facet."))
        .ToArray();

    private static MigrationPlanObject ParentPlan(
        MigrationCatalogObject item,
        IReadOnlyDictionary<string, MigrationPlanObject> planObjects)
    {
        if (item.ParentObjectId is null || !planObjects.TryGetValue(item.ParentObjectId, out MigrationPlanObject? parent))
            throw new InvalidDataException($"Catalog object '{item.ObjectId}' has no planned target parent.");
        return parent;
    }

    private static IEnumerable<string> OrderedMemberIds(MigrationCatalogObject item, string role)
    {
        MigrationObjectReference[] members = item.Members
            .Where(member => string.Equals(member.Role, role, StringComparison.Ordinal))
            .OrderBy(member => member.Ordinal)
            .ToArray();
        if (members.Length > 0)
            return members.Select(member => member.ObjectId);

        if (item.Kind is MigrationObjectKind.Key or MigrationObjectKind.Index && item.DependsOn.Count == 1)
            return item.DependsOn;

        throw new InvalidDataException(
            $"Catalog object '{item.ObjectId}' requires explicit ordered members for role '{role}'.");
    }

    private static string TypeName(DbType type) => type switch
    {
        DbType.Integer => "INTEGER",
        DbType.Real => "REAL",
        DbType.Text => "TEXT",
        DbType.Blob => "BLOB",
        _ => throw new InvalidDataException($"Unsupported persistent target type '{type}'."),
    };

    private static bool IsNullable(MigrationCatalogObject column) =>
        !bool.TryParse(Facet(column, "nullable"), out bool nullable) || nullable;

    private static bool IsTrue(MigrationCatalogObject item, string name) =>
        bool.TryParse(Facet(item, name), out bool value) && value;

    private static bool HasFacet(MigrationCatalogObject item, string name) =>
        item.Facets.Any(facet => string.Equals(facet.Name, name, StringComparison.Ordinal));

    private static string? Facet(MigrationCatalogObject item, string name) =>
        item.Facets.FirstOrDefault(facet => string.Equals(facet.Name, name, StringComparison.Ordinal))?.Value;

    private static string Normalize(string? value) =>
        (value ?? string.Empty).Trim().Replace('_', '-').ToLowerInvariant();
}
