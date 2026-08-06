using System.Security.Cryptography;
using System.Text;
using CSharpDB.Engine;
using CSharpDB.Migration.Validation;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.CSharpDb;

internal sealed class CSharpDbActualSchemaReader
{
    private readonly Database _database;
    private readonly MigrationPlan _plan;
    private readonly IReadOnlyDictionary<string, MigrationPlanObject> _planObjects;
    private readonly IReadOnlyDictionary<string, MigrationCatalogObject> _catalogObjects;
    private readonly IReadOnlyDictionary<string, CSharpDbCollectionMigrationBinding>
        _collectionBindings;
    private readonly Func<string, bool>? _excludeUnexpectedTable;

    private CSharpDbActualSchemaReader(
        Database database,
        MigrationPlan plan,
        MigrationCatalog catalog,
        IReadOnlyDictionary<string, CSharpDbCollectionMigrationBinding> collectionBindings,
        Func<string, bool>? excludeUnexpectedTable)
    {
        _database = database;
        _plan = plan;
        _planObjects = plan.Objects.ToDictionary(item => item.SourceObjectId, StringComparer.Ordinal);
        _catalogObjects = catalog.Objects.ToDictionary(item => item.ObjectId, StringComparer.Ordinal);
        _collectionBindings = collectionBindings;
        _excludeUnexpectedTable = excludeUnexpectedTable;
    }

    internal static MigrationNormalizedSchema Capture(
        Database database,
        MigrationPlan plan,
        MigrationCatalog catalog,
        IReadOnlyDictionary<string, CSharpDbCollectionMigrationBinding> collectionBindings,
        Func<string, bool>? excludeUnexpectedTable,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(collectionBindings);
        cancellationToken.ThrowIfCancellationRequested();

        return new CSharpDbActualSchemaReader(
            database,
            plan,
            catalog,
            collectionBindings,
            excludeUnexpectedTable).CaptureCore(cancellationToken);
    }

    private MigrationNormalizedSchema CaptureCore(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        MigrationPlanObject[] included = _plan.Objects
            .Select(item => CheckCancellation(item, cancellationToken))
            .Where(item => item.Included)
            .OrderBy(item => item.SourceObjectId, StringComparer.Ordinal)
            .ToArray();
        IReadOnlyDictionary<string, TableSchema> tables = included
            .Select(item => CheckCancellation(item, cancellationToken))
            .Where(item => _catalogObjects[item.SourceObjectId].Kind is
                MigrationObjectKind.Table or MigrationObjectKind.Collection)
            .Select(item => (
                Plan: item,
                Schema: _database.GetTableSchema(ResolvePhysicalTableName(item))))
            .Where(item => item.Schema is not null)
            .ToDictionary(
                item => item.Plan.SourceObjectId,
                item => item.Schema!,
                StringComparer.Ordinal);
        IndexSchema[] indexes = _database.GetIndexes()
            .Select(item => CheckCancellation(item, cancellationToken))
            .ToArray();
        string[] viewNames = _database.GetViewNames()
            .Select(item => CheckCancellation(item, cancellationToken))
            .ToArray();
        TriggerSchema[] triggers = _database.GetTriggers()
            .Select(item => CheckCancellation(item, cancellationToken))
            .ToArray();
        var definitions = new List<MigrationNormalizedSchemaObject>(included.Length);

        foreach (MigrationPlanObject planned in included)
        {
            cancellationToken.ThrowIfCancellationRequested();
            MigrationCatalogObject catalogObject = _catalogObjects[planned.SourceObjectId];
            MigrationNormalizedSchemaObject? definition = catalogObject.Kind switch
            {
                MigrationObjectKind.Table or MigrationObjectKind.Collection =>
                    CaptureTable(catalogObject, tables),
                MigrationObjectKind.Column => CaptureColumn(catalogObject, tables),
                MigrationObjectKind.Index => CaptureIndex(catalogObject, indexes, tables),
                MigrationObjectKind.Key => CaptureKey(catalogObject, tables),
                MigrationObjectKind.ForeignKey => CaptureForeignKey(catalogObject, tables),
                MigrationObjectKind.CheckConstraint => CaptureCheck(catalogObject, tables),
                MigrationObjectKind.View => CaptureView(catalogObject, viewNames),
                MigrationObjectKind.Trigger => CaptureTrigger(catalogObject, triggers),
                _ => null,
            };
            if (definition is not null)
                definitions.Add(definition);
        }

        CaptureUnexpectedSchema(
            definitions,
            indexes,
            viewNames,
            triggers,
            cancellationToken);

        return MigrationNormalizedSchemaContract.Create(definitions);
    }

    private void CaptureUnexpectedSchema(
        ICollection<MigrationNormalizedSchemaObject> definitions,
        IReadOnlyList<IndexSchema> indexes,
        IReadOnlyList<string> viewNames,
        IReadOnlyList<TriggerSchema> triggers,
        CancellationToken cancellationToken)
    {
        var knownIds = definitions
            .Select(item => CheckCancellation(item, cancellationToken))
            .Select(item => item.ObjectId)
            .ToHashSet(StringComparer.Ordinal);
        TableSchema[] actualTables = _database.GetTableNames()
            .Select(name => CheckCancellation(name, cancellationToken))
            .Where(name => _excludeUnexpectedTable?.Invoke(name) != true)
            .Select(name => _database.GetTableSchema(name))
            .Select(schema => CheckCancellation(schema, cancellationToken))
            .Where(schema => schema is not null)
            .Cast<TableSchema>()
            .OrderBy(schema => schema.TableName, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var tableIds = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var columnIds = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var actualKeys = new List<(string TableName, KeyConstraintDefinition Key, string ObjectId)>();

        foreach (TableSchema table in actualTables)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string tableId = ResolvePlannedDataObjectId(table.TableName) ??
                ExtraObjectId(MigrationObjectKind.Table, null, table.TableName);
            tableIds.Add(table.TableName, tableId);
            if (knownIds.Add(tableId))
            {
                definitions.Add(MigrationNormalizedSchemaContract.CreateObject(
                    tableId,
                    MigrationObjectKind.Table,
                    parentObjectId: null,
                    targetName: tableId));
            }

            foreach (ColumnDefinition column in table.Columns)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string columnId = ResolvePlannedObjectId(
                        MigrationObjectKind.Column,
                        tableId,
                        column.Name) ??
                    ExtraObjectId(MigrationObjectKind.Column, tableId, column.Name);
                columnIds.Add(ColumnLookupKey(table.TableName, column.Name), columnId);
                if (!knownIds.Add(columnId))
                    continue;

                var attributes = new List<MigrationNormalizedSchemaAttribute>
                {
                    Attribute("targetType", column.Type.ToString()),
                    Attribute("nullable", BooleanToken(column.Nullable)),
                    Attribute("identity", BooleanToken(column.IsIdentity)),
                    Attribute("rowVersion", BooleanToken(column.IsRowVersion)),
                };
                if (column.Collation is not null)
                    attributes.Add(Attribute("collation", column.Collation));
                if (column.DefaultSql is not null)
                    attributes.Add(Attribute("defaultSqlDigest", SqlDigest(column.DefaultSql)));
                definitions.Add(MigrationNormalizedSchemaContract.CreateObject(
                    columnId,
                    MigrationObjectKind.Column,
                    tableId,
                    columnId,
                    attributes));
            }

            foreach (KeyConstraintDefinition key in table.KeyConstraints)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string discriminator = key.ConstraintName ??
                    $"{key.Kind}:{string.Join("\0", key.Columns)}";
                string keyId = ResolvePlannedObjectId(
                        MigrationObjectKind.Key,
                        tableId,
                        key.ConstraintName) ??
                    ExtraObjectId(MigrationObjectKind.Key, tableId, discriminator);
                actualKeys.Add((table.TableName, key, keyId));
                if (!knownIds.Add(keyId))
                    continue;

                definitions.Add(MigrationNormalizedSchemaContract.CreateObject(
                    keyId,
                    MigrationObjectKind.Key,
                    tableId,
                    keyId,
                    [Attribute("kind", key.Kind == KeyConstraintKind.PrimaryKey ? "primary" : "unique")],
                    key.Columns.Select((name, ordinal) => new MigrationNormalizedSchemaMember
                    {
                        Role = MigrationObjectReferenceRoles.Column,
                        Ordinal = ordinal,
                        ObjectId = ResolveActualColumnId(columnIds, table.TableName, name),
                    }).ToArray()));
            }

            foreach (CheckConstraintDefinition check in table.CheckConstraints)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string discriminator = check.ConstraintName ?? SqlDigest(check.ExpressionSql);
                string checkId = ResolvePlannedObjectId(
                        MigrationObjectKind.CheckConstraint,
                        tableId,
                        check.ConstraintName) ??
                    ExtraObjectId(MigrationObjectKind.CheckConstraint, tableId, discriminator);
                if (!knownIds.Add(checkId))
                    continue;
                definitions.Add(MigrationNormalizedSchemaContract.CreateObject(
                    checkId,
                    MigrationObjectKind.CheckConstraint,
                    tableId,
                    checkId,
                    [Attribute("targetSqlDigest", SqlDigest(check.ExpressionSql))]));
            }
        }

        foreach (TableSchema table in actualTables)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string tableId = tableIds[table.TableName];
            foreach (ForeignKeyDefinition foreignKey in table.ForeignKeys)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string foreignKeyId = ResolvePlannedObjectId(
                        MigrationObjectKind.ForeignKey,
                        tableId,
                        foreignKey.ConstraintName) ??
                    ExtraObjectId(
                        MigrationObjectKind.ForeignKey,
                        tableId,
                        foreignKey.ConstraintName);
                if (!knownIds.Add(foreignKeyId))
                    continue;

                IReadOnlyList<string> sourceColumns = foreignKey.ColumnNames.Count > 0
                    ? foreignKey.ColumnNames
                    : [foreignKey.ColumnName];
                IReadOnlyList<string> referencedColumns = foreignKey.ReferencedColumnNames.Count > 0
                    ? foreignKey.ReferencedColumnNames
                    : [foreignKey.ReferencedColumnName];
                var members = sourceColumns.Select((name, ordinal) =>
                    new MigrationNormalizedSchemaMember
                    {
                        Role = MigrationObjectReferenceRoles.SourceColumn,
                        Ordinal = ordinal,
                        ObjectId = ResolveActualColumnId(columnIds, table.TableName, name),
                    }).ToList();
                string? referencedKeyId = actualKeys
                    .Where(candidate => string.Equals(
                        candidate.TableName,
                        foreignKey.ReferencedTableName,
                        StringComparison.OrdinalIgnoreCase))
                    .Where(candidate => candidate.Key.Columns.SequenceEqual(
                        referencedColumns,
                        StringComparer.OrdinalIgnoreCase))
                    .Select(candidate => candidate.ObjectId)
                    .OrderBy(id => id, StringComparer.Ordinal)
                    .FirstOrDefault();
                if (referencedKeyId is not null)
                {
                    members.Add(new MigrationNormalizedSchemaMember
                    {
                        Role = MigrationObjectReferenceRoles.ReferencedKey,
                        Ordinal = 0,
                        ObjectId = referencedKeyId,
                    });
                }

                definitions.Add(MigrationNormalizedSchemaContract.CreateObject(
                    foreignKeyId,
                    MigrationObjectKind.ForeignKey,
                    tableId,
                    foreignKeyId,
                    [
                        Attribute(
                            "onDelete",
                            FormatReferentialAction(foreignKey.OnDelete)),
                        Attribute(
                            "onUpdate",
                            FormatReferentialAction(foreignKey.OnUpdate)),
                    ],
                    members));
            }
        }

        foreach (IndexSchema index in indexes
                     .Where(item =>
                         item.Kind == IndexKind.Sql ||
                         (item.Kind == IndexKind.Collection &&
                          _collectionBindings.Values.Any(binding =>
                              string.Equals(
                                  binding.PhysicalTableName,
                                  item.TableName,
                                  StringComparison.OrdinalIgnoreCase))))
                     .OrderBy(item => item.IndexName, StringComparer.OrdinalIgnoreCase))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!tableIds.TryGetValue(index.TableName, out string? tableId))
                continue;
            string indexId = ResolvePlannedObjectId(
                    MigrationObjectKind.Index,
                    tableId,
                    index.IndexName) ??
                ExtraObjectId(MigrationObjectKind.Index, tableId, index.IndexName);
            if (!knownIds.Add(indexId))
                continue;
            definitions.Add(MigrationNormalizedSchemaContract.CreateObject(
                indexId,
                MigrationObjectKind.Index,
                tableId,
                indexId,
                [Attribute("unique", BooleanToken(index.IsUnique))],
                index.Kind == IndexKind.Sql
                    ? index.Columns.Select((name, ordinal) =>
                        new MigrationNormalizedSchemaMember
                        {
                            Role = MigrationObjectReferenceRoles.Column,
                            Ordinal = ordinal,
                            ObjectId = ResolveActualColumnId(
                                columnIds,
                                index.TableName,
                                name),
                        }).ToArray()
                    : []));
        }

        foreach (string viewName in viewNames.OrderBy(name => name, StringComparer.OrdinalIgnoreCase))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string viewId = ResolvePlannedObjectId(
                    MigrationObjectKind.View,
                    parentObjectId: null,
                    viewName) ??
                ExtraObjectId(MigrationObjectKind.View, null, viewName);
            if (!knownIds.Add(viewId) || _database.GetViewSql(viewName) is not string sql)
                continue;
            definitions.Add(MigrationNormalizedSchemaContract.CreateObject(
                viewId,
                MigrationObjectKind.View,
                parentObjectId: null,
                targetName: viewId,
                [Attribute("targetSqlDigest", SqlDigest(sql))]));
        }

        foreach (TriggerSchema trigger in triggers.OrderBy(
                     item => item.TriggerName,
                     StringComparer.OrdinalIgnoreCase))
        {
            cancellationToken.ThrowIfCancellationRequested();
            tableIds.TryGetValue(trigger.TableName, out string? tableId);
            string triggerId = ResolvePlannedObjectId(
                    MigrationObjectKind.Trigger,
                    tableId,
                    trigger.TriggerName) ??
                ExtraObjectId(MigrationObjectKind.Trigger, tableId, trigger.TriggerName);
            if (!knownIds.Add(triggerId))
                continue;
            string structuralSql =
                $"CREATE TRIGGER {CSharpDbMigrationSql.Quote(trigger.TriggerName)} " +
                $"{trigger.Timing.ToString().ToUpperInvariant()} " +
                $"{trigger.Event.ToString().ToUpperInvariant()} ON " +
                $"{CSharpDbMigrationSql.Quote(trigger.TableName)} BEGIN {trigger.BodySql} END";
            definitions.Add(MigrationNormalizedSchemaContract.CreateObject(
                triggerId,
                MigrationObjectKind.Trigger,
                tableId,
                triggerId,
                [Attribute("targetSqlDigest", SqlDigest(structuralSql))]));
        }
    }

    private string? ResolvePlannedObjectId(
        MigrationObjectKind kind,
        string? parentObjectId,
        string? targetName)
    {
        if (string.IsNullOrEmpty(targetName))
            return null;
        return _plan.Objects
            .Where(item => item.Included && _catalogObjects[item.SourceObjectId].Kind == kind)
            .Where(item => parentObjectId is null || string.Equals(
                _catalogObjects[item.SourceObjectId].ParentObjectId,
                parentObjectId,
                StringComparison.Ordinal))
            .SingleOrDefault(item => string.Equals(
                item.TargetName,
                targetName,
                StringComparison.OrdinalIgnoreCase))
            ?.SourceObjectId;
    }

    private string? ResolvePlannedDataObjectId(string physicalTableName) =>
        _plan.Objects
            .Where(item => item.Included)
            .Where(item => _catalogObjects[item.SourceObjectId].Kind is
                MigrationObjectKind.Table or MigrationObjectKind.Collection)
            .SingleOrDefault(item => string.Equals(
                ResolvePhysicalTableName(item),
                physicalTableName,
                StringComparison.OrdinalIgnoreCase))
            ?.SourceObjectId;

    private static string ResolveActualColumnId(
        IReadOnlyDictionary<string, string> columnIds,
        string tableName,
        string columnName) =>
        columnIds.TryGetValue(ColumnLookupKey(tableName, columnName), out string? objectId)
            ? objectId
            : ExtraObjectId(MigrationObjectKind.Column, null, $"{tableName}\0{columnName}");

    private static string ColumnLookupKey(string tableName, string columnName) =>
        $"{tableName}\0{columnName}";

    private static string ExtraObjectId(
        MigrationObjectKind kind,
        string? parentObjectId,
        string discriminator)
    {
        string material = string.Join(
            '\0',
            "csharpdb-target-extra/v1",
            kind.ToString(),
            parentObjectId ?? string.Empty,
            discriminator.ToLowerInvariant());
        string digest = Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(material)))
            .ToLowerInvariant();
        return $"target-extra:{kind.ToString().ToLowerInvariant()}:{digest}";
    }

    private string ResolvePhysicalTableName(MigrationPlanObject planned) =>
        _collectionBindings.TryGetValue(
            planned.SourceObjectId,
            out CSharpDbCollectionMigrationBinding? binding)
            ? binding.PhysicalTableName
            : planned.TargetName ??
              throw new InvalidDataException(
                  $"Included data object '{planned.SourceObjectId}' has no target name.");

    private MigrationNormalizedSchemaObject? CaptureTable(
        MigrationCatalogObject item,
        IReadOnlyDictionary<string, TableSchema> tables)
    {
        if (!tables.TryGetValue(item.ObjectId, out TableSchema? table))
            return null;

        string targetName = item.Kind == MigrationObjectKind.Collection
            ? _planObjects[item.ObjectId].TargetName ??
              throw new InvalidDataException(
                  $"Included collection '{item.ObjectId}' has no target name.")
            : table.TableName;
        return CreateActualObject(item, targetName);
    }

    private MigrationNormalizedSchemaObject? CaptureColumn(
        MigrationCatalogObject item,
        IReadOnlyDictionary<string, TableSchema> tables)
    {
        if (item.ParentObjectId is null ||
            !tables.TryGetValue(item.ParentObjectId, out TableSchema? table))
        {
            return null;
        }

        string expectedName = _planObjects[item.ObjectId].TargetName!;
        ColumnDefinition? column = table.Columns.SingleOrDefault(candidate =>
            string.Equals(candidate.Name, expectedName, StringComparison.OrdinalIgnoreCase));
        if (column is null)
            return null;

        var attributes = new List<MigrationNormalizedSchemaAttribute>
        {
            Attribute("targetType", column.Type.ToString()),
            Attribute("nullable", BooleanToken(column.Nullable)),
            Attribute("identity", BooleanToken(column.IsIdentity)),
            Attribute("rowVersion", BooleanToken(column.IsRowVersion)),
        };
        if (CSharpDbDeclaredTypeContract.TryRead(
                item,
                out SqlTypeDescriptor declaredType) &&
            declaredType.StorageType == column.Type)
        {
            attributes.Add(Attribute(
                "declaredType",
                column.EffectiveType.ToSql()));
        }
        if (column.Collation is not null)
            attributes.Add(Attribute("collation", column.Collation));
        if (column.DefaultSql is not null)
            attributes.Add(Attribute("defaultSqlDigest", SqlDigest(column.DefaultSql)));
        return CreateActualObject(item, column.Name, attributes);
    }

    private MigrationNormalizedSchemaObject? CaptureIndex(
        MigrationCatalogObject item,
        IReadOnlyList<IndexSchema> indexes,
        IReadOnlyDictionary<string, TableSchema> tables)
    {
        if (item.ParentObjectId is null ||
            !tables.TryGetValue(item.ParentObjectId, out TableSchema? table))
        {
            return null;
        }

        string expectedName = _planObjects[item.ObjectId].TargetName!;
        IndexSchema? index = indexes.SingleOrDefault(candidate =>
            candidate.Kind == IndexKind.Sql &&
            string.Equals(candidate.IndexName, expectedName, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(candidate.TableName, table.TableName, StringComparison.OrdinalIgnoreCase));
        if (index is null)
            return null;

        MigrationNormalizedSchemaMember[] members = MapColumnMembers(
            item.ParentObjectId,
            index.Columns,
            MigrationObjectReferenceRoles.Column);
        return CreateActualObject(
            item,
            index.IndexName,
            [Attribute("unique", BooleanToken(index.IsUnique))],
            members);
    }

    private MigrationNormalizedSchemaObject? CaptureKey(
        MigrationCatalogObject item,
        IReadOnlyDictionary<string, TableSchema> tables)
    {
        if (item.ParentObjectId is null ||
            !tables.TryGetValue(item.ParentObjectId, out TableSchema? table))
        {
            return null;
        }

        string expectedName = _planObjects[item.ObjectId].TargetName!;
        KeyConstraintDefinition? key = table.KeyConstraints.SingleOrDefault(candidate =>
            string.Equals(candidate.ConstraintName, expectedName, StringComparison.OrdinalIgnoreCase));
        if (key is null)
            return null;

        string kind = key.Kind switch
        {
            KeyConstraintKind.PrimaryKey => "primary",
            KeyConstraintKind.Unique => "unique",
            _ => throw new InvalidDataException(
                $"Target key '{key.ConstraintName}' has unknown kind '{key.Kind}'."),
        };
        return CreateActualObject(
            item,
            key.ConstraintName!,
            [Attribute("kind", kind)],
            MapColumnMembers(item.ParentObjectId, key.Columns, MigrationObjectReferenceRoles.Column));
    }

    private MigrationNormalizedSchemaObject? CaptureForeignKey(
        MigrationCatalogObject item,
        IReadOnlyDictionary<string, TableSchema> tables)
    {
        if (item.ParentObjectId is null ||
            !tables.TryGetValue(item.ParentObjectId, out TableSchema? table))
        {
            return null;
        }

        string expectedName = _planObjects[item.ObjectId].TargetName!;
        ForeignKeyDefinition? foreignKey = table.ForeignKeys.SingleOrDefault(candidate =>
            string.Equals(candidate.ConstraintName, expectedName, StringComparison.OrdinalIgnoreCase));
        if (foreignKey is null)
            return null;

        IReadOnlyList<string> sourceColumns = foreignKey.ColumnNames.Count > 0
            ? foreignKey.ColumnNames
            : [foreignKey.ColumnName];
        var members = MapColumnMembers(
                item.ParentObjectId,
                sourceColumns,
                MigrationObjectReferenceRoles.SourceColumn)
            .ToList();
        string? referencedKeyId = ResolveReferencedKey(item, foreignKey, tables);
        if (referencedKeyId is not null)
        {
            members.Add(new MigrationNormalizedSchemaMember
            {
                Role = MigrationObjectReferenceRoles.ReferencedKey,
                Ordinal = 0,
                ObjectId = referencedKeyId,
            });
        }

        return CreateActualObject(
            item,
            foreignKey.ConstraintName,
            [
                Attribute(
                    "onDelete",
                    FormatReferentialAction(foreignKey.OnDelete)),
                Attribute(
                    "onUpdate",
                    FormatReferentialAction(foreignKey.OnUpdate)),
            ],
            members);
    }

    private static string FormatReferentialAction(
        ForeignKeyOnDeleteAction action) =>
        action switch
        {
            ForeignKeyOnDeleteAction.Restrict => "restrict",
            ForeignKeyOnDeleteAction.Cascade => "cascade",
            ForeignKeyOnDeleteAction.NoAction => "no-action",
            ForeignKeyOnDeleteAction.SetNull => "set-null",
            ForeignKeyOnDeleteAction.SetDefault => "set-default",
            _ => throw new InvalidDataException(
                $"Target foreign key has an unknown referential action '{action}'."),
        };

    private MigrationNormalizedSchemaObject? CaptureCheck(
        MigrationCatalogObject item,
        IReadOnlyDictionary<string, TableSchema> tables)
    {
        if (item.ParentObjectId is null ||
            !tables.TryGetValue(item.ParentObjectId, out TableSchema? table))
        {
            return null;
        }

        string expectedName = _planObjects[item.ObjectId].TargetName!;
        CheckConstraintDefinition? check = table.CheckConstraints.SingleOrDefault(candidate =>
            string.Equals(candidate.ConstraintName, expectedName, StringComparison.OrdinalIgnoreCase));
        return check is null
            ? null
            : CreateActualObject(
                item,
                check.ConstraintName!,
                [Attribute("targetSqlDigest", SqlDigest(check.ExpressionSql))]);
    }

    private MigrationNormalizedSchemaObject? CaptureView(
        MigrationCatalogObject item,
        IReadOnlyList<string> viewNames)
    {
        string expectedName = _planObjects[item.ObjectId].TargetName!;
        string? actualName = viewNames.SingleOrDefault(candidate =>
            string.Equals(candidate, expectedName, StringComparison.OrdinalIgnoreCase));
        if (actualName is null || _database.GetViewSql(actualName) is not string sql)
            return null;
        return CreateActualObject(
            item,
            actualName,
            [Attribute("targetSqlDigest", SqlDigest(sql))]);
    }

    private MigrationNormalizedSchemaObject? CaptureTrigger(
        MigrationCatalogObject item,
        IReadOnlyList<TriggerSchema> triggers)
    {
        string expectedName = _planObjects[item.ObjectId].TargetName!;
        TriggerSchema? trigger = triggers.SingleOrDefault(candidate =>
            string.Equals(candidate.TriggerName, expectedName, StringComparison.OrdinalIgnoreCase));
        if (trigger is null)
            return null;

        string structuralSql =
            $"CREATE TRIGGER {CSharpDbMigrationSql.Quote(trigger.TriggerName)} " +
            $"{trigger.Timing.ToString().ToUpperInvariant()} " +
            $"{trigger.Event.ToString().ToUpperInvariant()} ON " +
            $"{CSharpDbMigrationSql.Quote(trigger.TableName)} BEGIN {trigger.BodySql} END";
        return CreateActualObject(
            item,
            trigger.TriggerName,
            [Attribute("targetSqlDigest", SqlDigest(structuralSql))]);
    }

    private string? ResolveReferencedKey(
        MigrationCatalogObject foreignKeyObject,
        ForeignKeyDefinition foreignKey,
        IReadOnlyDictionary<string, TableSchema> tables)
    {
        string? referencedKeyId = foreignKeyObject.Members
            .Where(member => string.Equals(
                member.Role,
                MigrationObjectReferenceRoles.ReferencedKey,
                StringComparison.Ordinal))
            .OrderBy(member => member.Ordinal)
            .Select(member => member.ObjectId)
            .SingleOrDefault();
        if (referencedKeyId is null ||
            !_catalogObjects.TryGetValue(referencedKeyId, out MigrationCatalogObject? referencedKey) ||
            referencedKey.Kind != MigrationObjectKind.Key ||
            referencedKey.ParentObjectId is null ||
            !tables.TryGetValue(referencedKey.ParentObjectId, out TableSchema? table) ||
            !_planObjects.TryGetValue(referencedKeyId, out MigrationPlanObject? keyPlan) ||
            !keyPlan.Included)
        {
            return null;
        }

        IReadOnlyList<string> referencedColumns = foreignKey.ReferencedColumnNames.Count > 0
            ? foreignKey.ReferencedColumnNames
            : [foreignKey.ReferencedColumnName];
        KeyConstraintDefinition? key = table.KeyConstraints.SingleOrDefault(candidate =>
            string.Equals(
                candidate.ConstraintName,
                keyPlan.TargetName,
                StringComparison.OrdinalIgnoreCase));
        if (key is null ||
            !string.Equals(
                table.TableName,
                foreignKey.ReferencedTableName,
                StringComparison.OrdinalIgnoreCase) ||
            !key.Columns.SequenceEqual(referencedColumns, StringComparer.OrdinalIgnoreCase))
        {
            return null;
        }
        return referencedKeyId;
    }

    private MigrationNormalizedSchemaMember[] MapColumnMembers(
        string tableObjectId,
        IReadOnlyList<string> actualColumnNames,
        string role) => actualColumnNames
        .Select((columnName, ordinal) => new MigrationNormalizedSchemaMember
        {
            Role = role,
            Ordinal = ordinal,
            ObjectId = ResolveColumnObjectId(tableObjectId, columnName),
        })
        .ToArray();

    private string ResolveColumnObjectId(string tableObjectId, string targetColumnName)
    {
        MigrationPlanObject? column = _plan.Objects.SingleOrDefault(candidate =>
            candidate.Included &&
            _catalogObjects[candidate.SourceObjectId].Kind == MigrationObjectKind.Column &&
            string.Equals(
                _catalogObjects[candidate.SourceObjectId].ParentObjectId,
                tableObjectId,
                StringComparison.Ordinal) &&
            string.Equals(candidate.TargetName, targetColumnName, StringComparison.OrdinalIgnoreCase));
        return column?.SourceObjectId ??
            $"target-column:{tableObjectId}:{targetColumnName.ToLowerInvariant()}";
    }

    private static MigrationNormalizedSchemaObject CreateActualObject(
        MigrationCatalogObject item,
        string targetName,
        IReadOnlyList<MigrationNormalizedSchemaAttribute>? attributes = null,
        IReadOnlyList<MigrationNormalizedSchemaMember>? members = null) =>
        MigrationNormalizedSchemaContract.CreateObject(
            item.ObjectId,
            item.Kind,
            item.ParentObjectId,
            targetName,
            attributes,
            members);

    private static MigrationNormalizedSchemaAttribute Attribute(string name, string value) => new()
    {
        Name = name,
        Value = value,
    };

    private static string BooleanToken(bool value) => value ? "true" : "false";

    private static T CheckCancellation<T>(T value, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return value;
    }

    private static string SqlDigest(string sql) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(sql))).ToLowerInvariant();
}
