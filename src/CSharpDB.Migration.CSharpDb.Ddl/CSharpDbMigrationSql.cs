using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.CSharpDb;

internal interface ICSharpDbMigrationActionObserver
{
    void BeginSqlAction(CancellationToken cancellationToken);

    void ObserveSqlSegment(
        string segment,
        CancellationToken cancellationToken);

    void ObserveCollectionAction(CancellationToken cancellationToken);
}

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
        MigrationSchemaStage stage) =>
        BuildStageActionsObserved(
            plan,
            catalog,
            stage,
            actionObserver: null,
            CancellationToken.None);

    internal static IReadOnlyList<string> BuildStageActionsObserved(
        MigrationPlan plan,
        MigrationCatalog catalog,
        MigrationSchemaStage stage,
        ICSharpDbMigrationActionObserver? actionObserver,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        IReadOnlyDictionary<string, MigrationPlanObject> planObjects = plan.Objects
            .ToDictionary(item => item.SourceObjectId, StringComparer.Ordinal);
        IReadOnlyDictionary<string, MigrationCatalogObject> catalogObjects = catalog.Objects
            .ToDictionary(item => item.ObjectId, StringComparer.Ordinal);
        cancellationToken.ThrowIfCancellationRequested();

        return stage switch
        {
            MigrationSchemaStage.LoadEssential => BuildTables(
                planObjects,
                catalog,
                actionObserver,
                cancellationToken),
            MigrationSchemaStage.SecondaryIndexes => BuildIndexes(
                planObjects,
                catalogObjects,
                actionObserver,
                cancellationToken),
            MigrationSchemaStage.Constraints => BuildConstraints(
                planObjects,
                catalogObjects,
                actionObserver,
                cancellationToken),
            MigrationSchemaStage.Views => BuildViews(
                planObjects,
                catalog,
                actionObserver,
                cancellationToken),
            MigrationSchemaStage.Triggers => BuildTriggers(
                planObjects,
                catalog,
                actionObserver,
                cancellationToken),
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
        MigrationCatalog catalog,
        ICSharpDbMigrationActionObserver? actionObserver,
        CancellationToken cancellationToken)
    {
        var actions = new List<string>();
        foreach (MigrationCatalogObject table in catalog.Objects
                     .Where(item => item.Kind is MigrationObjectKind.Table or MigrationObjectKind.Collection)
                     .Where(item => planObjects[item.ObjectId].Included)
                     .OrderBy(item => item.ObjectId, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (table.Kind == MigrationObjectKind.Collection)
            {
                AddCollectionAction(
                    actions,
                    planObjects[table.ObjectId].TargetName ??
                    throw new InvalidDataException(
                        $"Included collection '{table.ObjectId}' has no target name."),
                    actionObserver,
                    cancellationToken);
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

            var definitions =
                new (string Name, string Type, string? Collation, bool Nullable)[
                    columns.Length];
            for (int columnOrdinal = 0;
                 columnOrdinal < columns.Length;
                 columnOrdinal++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                MigrationCatalogObject column = columns[columnOrdinal];
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

                string? collation = Facet(column, "collation");
                if (!string.IsNullOrWhiteSpace(collation))
                {
                    if (collation.Any(character => !(char.IsLetterOrDigit(character) || character is '_' or '-' or ':')))
                        throw new InvalidDataException($"Column '{column.ObjectId}' contains an unsafe collation token.");
                }
                definitions[columnOrdinal] = (
                    Quote(planned.TargetName!),
                    TypeName(targetType),
                    collation,
                    IsNullable(column));
            }

            AddSqlAction(
                actions,
                actionObserver,
                cancellationToken,
                writer =>
                {
                    writer.Append("CREATE TABLE ");
                    writer.Append(Quote(planObjects[table.ObjectId].TargetName!));
                    writer.Append(" (");
                    for (int ordinal = 0; ordinal < definitions.Length; ordinal++)
                    {
                        if (ordinal > 0)
                            writer.Append(", ");
                        writer.Append(definitions[ordinal].Name);
                        writer.Append(" ");
                        writer.Append(definitions[ordinal].Type);
                        if (!string.IsNullOrWhiteSpace(
                                definitions[ordinal].Collation))
                        {
                            writer.Append(" COLLATE ");
                            writer.Append(definitions[ordinal].Collation!);
                        }
                        if (!definitions[ordinal].Nullable)
                            writer.Append(" NOT NULL");
                    }
                    writer.Append(")");
                });
        }
        return actions;
    }

    private static IReadOnlyList<string> BuildIndexes(
        IReadOnlyDictionary<string, MigrationPlanObject> planObjects,
        IReadOnlyDictionary<string, MigrationCatalogObject> catalogObjects,
        ICSharpDbMigrationActionObserver? actionObserver,
        CancellationToken cancellationToken)
    {
        var actions = new List<string>();
        foreach (MigrationCatalogObject index in catalogObjects.Values
                     .Where(item => item.Kind == MigrationObjectKind.Index && planObjects[item.ObjectId].Included)
                     .OrderBy(item => item.ObjectId, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            MigrationPlanObject table = ParentPlan(index, planObjects);
            string[] columns = OrderedMemberIds(index, MigrationObjectReferenceRoles.Column)
                .Select(id => Quote(planObjects[id].TargetName!))
                .ToArray();
            if (columns.Length == 0)
                throw new InvalidDataException($"Included index '{index.ObjectId}' has no ordered columns.");
            string unique = IsTrue(index, "unique") ? "UNIQUE " : string.Empty;
            AddSqlAction(
                actions,
                actionObserver,
                cancellationToken,
                writer =>
                {
                    writer.Append("CREATE ");
                    writer.Append(unique);
                    writer.Append("INDEX ");
                    writer.Append(Quote(planObjects[index.ObjectId].TargetName!));
                    writer.Append(" ON ");
                    writer.Append(Quote(table.TargetName!));
                    writer.Append(" (");
                    AppendJoined(writer, columns);
                    writer.Append(")");
                });
        }
        return actions;
    }

    private static IReadOnlyList<string> BuildConstraints(
        IReadOnlyDictionary<string, MigrationPlanObject> planObjects,
        IReadOnlyDictionary<string, MigrationCatalogObject> catalogObjects,
        ICSharpDbMigrationActionObserver? actionObserver,
        CancellationToken cancellationToken)
    {
        var actions = new List<string>();
        IEnumerable<MigrationCatalogObject> included = catalogObjects.Values
            .Where(item => planObjects[item.ObjectId].Included);

        foreach (MigrationCatalogObject key in included
                     .Where(item => item.Kind == MigrationObjectKind.Key)
                     .OrderBy(item => item.ObjectId, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
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
            AddSqlAction(
                actions,
                actionObserver,
                cancellationToken,
                writer =>
                {
                    writer.Append("ALTER TABLE ");
                    writer.Append(Quote(table.TargetName!));
                    writer.Append(" ADD CONSTRAINT ");
                    writer.Append(Quote(planObjects[key.ObjectId].TargetName!));
                    writer.Append(" ");
                    writer.Append(constraint);
                    writer.Append(" (");
                    AppendJoined(writer, columns);
                    writer.Append(")");
                });
        }

        foreach (MigrationCatalogObject foreignKey in included
                     .Where(item => item.Kind == MigrationObjectKind.ForeignKey)
                     .OrderBy(item => item.ObjectId, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
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
            string onDelete = Normalize(Facet(foreignKey, "onDelete"));
            if (onDelete.StartsWith("on-delete-", StringComparison.Ordinal))
                onDelete = onDelete["on-delete-".Length..];
            string onDeleteClause = string.Empty;
            if (!string.IsNullOrEmpty(onDelete) && onDelete != "restrict")
            {
                onDeleteClause = onDelete switch
                {
                    "cascade" => " ON DELETE CASCADE",
                    _ => throw new InvalidDataException(
                        $"Included foreign key '{foreignKey.ObjectId}' has unsupported delete action '{onDelete}'."),
                };
            }
            AddSqlAction(
                actions,
                actionObserver,
                cancellationToken,
                writer =>
                {
                    writer.Append("ALTER TABLE ");
                    writer.Append(Quote(sourceTable.TargetName!));
                    writer.Append(" ADD CONSTRAINT ");
                    writer.Append(Quote(
                        planObjects[foreignKey.ObjectId].TargetName!));
                    writer.Append(" FOREIGN KEY (");
                    AppendJoined(writer, sourceColumns);
                    writer.Append(") REFERENCES ");
                    writer.Append(Quote(referencedTable.TargetName!));
                    writer.Append(" (");
                    AppendJoined(writer, referencedColumns);
                    writer.Append(")");
                    writer.Append(onDeleteClause);
                });
        }

        foreach (MigrationCatalogObject check in included
                     .Where(item => item.Kind == MigrationObjectKind.CheckConstraint)
                     .OrderBy(item => item.ObjectId, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string expression = Facet(check, "targetSql") ??
                throw new NotSupportedException(
                    $"Included check '{check.ObjectId}' requires a scratch-validated 'targetSql' facet.");
            MigrationPlanObject table = ParentPlan(check, planObjects);
            AddSqlAction(
                actions,
                actionObserver,
                cancellationToken,
                writer =>
                {
                    writer.Append("ALTER TABLE ");
                    writer.Append(Quote(table.TargetName!));
                    writer.Append(" ADD CONSTRAINT ");
                    writer.Append(Quote(planObjects[check.ObjectId].TargetName!));
                    writer.Append(" CHECK (");
                    writer.Append(expression);
                    writer.Append(")");
                });
        }

        return actions;
    }

    private static IReadOnlyList<string> BuildViews(
        IReadOnlyDictionary<string, MigrationPlanObject> planObjects,
        MigrationCatalog catalog,
        ICSharpDbMigrationActionObserver? actionObserver,
        CancellationToken cancellationToken)
    {
        var actions = new List<string>();
        foreach (MigrationCatalogObject item in catalog.Objects
                     .Where(item => item.Kind == MigrationObjectKind.View &&
                         planObjects[item.ObjectId].Included)
                     .OrderBy(item => item.ObjectId, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string sql = Facet(item, "targetSql") ??
                throw new NotSupportedException(
                    $"Included view '{item.ObjectId}' requires a scratch-validated 'targetSql' facet.");
            AddSqlAction(
                actions,
                actionObserver,
                cancellationToken,
                writer =>
                {
                    writer.Append("CREATE VIEW ");
                    writer.Append(Quote(planObjects[item.ObjectId].TargetName!));
                    writer.Append(" AS ");
                    writer.Append(sql);
                });
        }
        return actions;
    }

    private static IReadOnlyList<string> BuildTriggers(
        IReadOnlyDictionary<string, MigrationPlanObject> planObjects,
        MigrationCatalog catalog,
        ICSharpDbMigrationActionObserver? actionObserver,
        CancellationToken cancellationToken)
    {
        var actions = new List<string>();
        foreach (MigrationCatalogObject item in catalog.Objects
                     .Where(item => item.Kind == MigrationObjectKind.Trigger &&
                         planObjects[item.ObjectId].Included)
                     .OrderBy(item => item.ObjectId, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string sql = Facet(item, "targetSql") ??
                throw new NotSupportedException(
                    $"Included trigger '{item.ObjectId}' requires a scratch-validated 'targetSql' facet.");
            AddSqlAction(
                actions,
                actionObserver,
                cancellationToken,
                writer => writer.Append(sql));
        }
        return actions;
    }

    private static void AddSqlAction(
        List<string> actions,
        ICSharpDbMigrationActionObserver? actionObserver,
        CancellationToken cancellationToken,
        Action<CSharpDbMigrationSqlActionWriter> render)
    {
        cancellationToken.ThrowIfCancellationRequested();
        actionObserver?.BeginSqlAction(cancellationToken);
        var writer = new CSharpDbMigrationSqlActionWriter(
            actionObserver,
            cancellationToken);
        render(writer);
        cancellationToken.ThrowIfCancellationRequested();
        actions.Add(writer.ToString());
    }

    private static void AddCollectionAction(
        List<string> actions,
        string collectionName,
        ICSharpDbMigrationActionObserver? actionObserver,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        actionObserver?.ObserveCollectionAction(cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        actions.Add(CollectionActionPrefix + collectionName);
    }

    private static void AppendJoined(
        CSharpDbMigrationSqlActionWriter writer,
        IReadOnlyList<string> segments)
    {
        for (int ordinal = 0; ordinal < segments.Count; ordinal++)
        {
            if (ordinal > 0)
                writer.Append(", ");
            writer.Append(segments[ordinal]);
        }
    }

    private static MigrationPlanObject ParentPlan(
        MigrationCatalogObject item,
        IReadOnlyDictionary<string, MigrationPlanObject> planObjects)
    {
        if (item.ParentObjectId is null || !planObjects.TryGetValue(item.ParentObjectId, out MigrationPlanObject? parent))
            throw new InvalidDataException($"Catalog object '{item.ObjectId}' has no planned target parent.");
        return parent;
    }

    private sealed class CSharpDbMigrationSqlActionWriter(
        ICSharpDbMigrationActionObserver? actionObserver,
        CancellationToken cancellationToken)
    {
        private readonly StringBuilder _builder = new();

        internal void Append(string segment)
        {
            cancellationToken.ThrowIfCancellationRequested();
            actionObserver?.ObserveSqlSegment(
                segment,
                cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
            _builder.Append(segment);
        }

        public override string ToString() => _builder.ToString();
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
