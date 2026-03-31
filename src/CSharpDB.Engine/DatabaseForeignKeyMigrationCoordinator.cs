using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Indexing;
using CSharpDB.Storage.Serialization;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Engine;

internal static class DatabaseForeignKeyMigrationCoordinator
{
    private const string CollectionPrefix = "_col_";
    private const string InternalTablePrefix = "__";
    private const string SystemTablePrefix = "sys.";
    private const string SystemAliasPrefix = "sys_";
    private const string MissingReferencedParentReason = "MissingReferencedParent";

    public static async ValueTask<DatabaseForeignKeyMigrationResult> MigrateAsync(
        string databasePath,
        DatabaseForeignKeyMigrationRequest request,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string fullPath = Path.GetFullPath(databasePath);
        int sampleLimit = request.ViolationSampleLimit < 0 ? 100 : request.ViolationSampleLimit;

        StorageEngineContext? context = null;
        try
        {
            context = await OpenStorageContextAsync(fullPath, ct);
            TableMigrationPlan[] plans = BuildMigrationPlans(context.Catalog, request).ToArray();
            var validation = await ValidateDataAsync(context, plans, sampleLimit, ct);

            var baseResult = new DatabaseForeignKeyMigrationResult
            {
                ValidateOnly = request.ValidateOnly,
                Succeeded = validation.ViolationCount == 0,
                BackupDestinationPath = null,
                AffectedTables = plans.Length,
                AppliedForeignKeys = plans.Sum(static plan => plan.NewForeignKeys.Length),
                CopiedRows = 0,
                ViolationCount = validation.ViolationCount,
                Violations = validation.Violations,
                AppliedConstraints = plans
                    .SelectMany(static plan => plan.NewForeignKeys)
                    .Select(static foreignKey => new DatabaseForeignKeyMigrationAppliedConstraint
                    {
                        TableName = foreignKey.TableName,
                        ColumnName = foreignKey.Definition.ColumnName,
                        ReferencedTableName = foreignKey.Definition.ReferencedTableName,
                        ReferencedColumnName = foreignKey.Definition.ReferencedColumnName,
                        ConstraintName = foreignKey.Definition.ConstraintName,
                        SupportingIndexName = foreignKey.Definition.SupportingIndexName,
                        OnDelete = foreignKey.Definition.OnDelete,
                    })
                    .ToArray(),
            };

            if (request.ValidateOnly || validation.ViolationCount > 0)
                return baseResult;

            await context.Pager.DisposeAsync();
            context = null;

            string? backupDestinationPath = null;
            if (!string.IsNullOrWhiteSpace(request.BackupDestinationPath))
            {
                backupDestinationPath = Path.GetFullPath(request.BackupDestinationPath);
                await using var backupDatabase = await Database.OpenAsync(fullPath, ct);
                _ = await DatabaseBackupCoordinator.BackupAsync(
                    backupDatabase,
                    fullPath,
                    backupDestinationPath,
                    withManifest: false,
                    ct);
            }

            context = await OpenStorageContextAsync(fullPath, ct);
            await context.Pager.BeginTransactionAsync(ct);
            try
            {
                long copiedRows = 0;
                foreach (TableMigrationPlan plan in plans)
                    copiedRows += await ApplyPlanAsync(context, plan, ct);

                await context.Catalog.PersistDirtyAdvisoryStatisticsAsync(ct);
                await context.Catalog.PersistAllRootPageChangesAsync(ct);
                await context.Pager.CommitAsync(ct);

                return new DatabaseForeignKeyMigrationResult
                {
                    ValidateOnly = baseResult.ValidateOnly,
                    Succeeded = true,
                    BackupDestinationPath = backupDestinationPath,
                    AffectedTables = baseResult.AffectedTables,
                    AppliedForeignKeys = baseResult.AppliedForeignKeys,
                    CopiedRows = copiedRows,
                    ViolationCount = baseResult.ViolationCount,
                    Violations = baseResult.Violations,
                    AppliedConstraints = baseResult.AppliedConstraints,
                };
            }
            catch
            {
                await context.Pager.RollbackAsync(ct);
                throw;
            }
        }
        finally
        {
            if (context is not null)
                await context.Pager.DisposeAsync();
        }
    }

    private static async ValueTask<long> ApplyPlanAsync(
        StorageEngineContext context,
        TableMigrationPlan plan,
        CancellationToken ct)
    {
        await context.Catalog.CreateTableExactAsync(plan.TempSchema, ct);

        long copiedRows = await CopyTableRowsAsync(
            context.Catalog.GetTableTree(plan.OriginalSchema.TableName),
            context.Catalog.GetTableTree(plan.TempSchema.TableName),
            ct);

        foreach (TriggerSchema trigger in plan.OriginalTriggers)
            await context.Catalog.DropTriggerAsync(trigger.TriggerName, ct);

        await context.Catalog.DropTableAsync(plan.OriginalSchema.TableName, ct);
        await context.Catalog.UpdateTableSchemaAsync(plan.TempSchema.TableName, plan.RenamedTempSchema, ct);

        foreach (IndexSchema index in plan.RecreatedIndexes)
        {
            await context.Catalog.CreateIndexAsync(index, ct);
            await BackfillIndexAsync(context, index, plan.RenamedTempSchema, ct);
        }

        await context.Catalog.UpdateTableSchemaAsync(plan.OriginalSchema.TableName, plan.FinalSchema, ct);
        foreach (ForeignKeyDefinition foreignKey in plan.FinalSchema.ForeignKeys)
            await CreateForeignKeySupportIndexAsync(context, plan.FinalSchema, foreignKey, ct);

        await context.Catalog.SetTableRowCountAsync(plan.OriginalSchema.TableName, copiedRows, ct);
        if (plan.ColumnStatistics.Length > 0)
            await context.Catalog.ReplaceColumnStatisticsAsync(plan.OriginalSchema.TableName, plan.ColumnStatistics, ct);

        foreach (TriggerSchema trigger in plan.OriginalTriggers)
            await context.Catalog.CreateTriggerAsync(trigger, ct);

        return copiedRows;
    }

    private static IEnumerable<TableMigrationPlan> BuildMigrationPlans(
        SchemaCatalog catalog,
        DatabaseForeignKeyMigrationRequest request)
    {
        if (request.Constraints.Count == 0)
            throw new CSharpDbException(ErrorCode.SyntaxError, "Foreign key migration requires at least one constraint.");

        var specsByTable = new Dictionary<string, List<DatabaseForeignKeyMigrationConstraintSpec>>(StringComparer.OrdinalIgnoreCase);
        var seenSpecKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var seenChildColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (DatabaseForeignKeyMigrationConstraintSpec spec in request.Constraints)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(spec.TableName);
            ArgumentException.ThrowIfNullOrWhiteSpace(spec.ColumnName);
            ArgumentException.ThrowIfNullOrWhiteSpace(spec.ReferencedTableName);

            string tableName = spec.TableName.Trim();
            string columnName = spec.ColumnName.Trim();
            string referencedTableName = spec.ReferencedTableName.Trim();
            string? referencedColumnName = string.IsNullOrWhiteSpace(spec.ReferencedColumnName)
                ? null
                : spec.ReferencedColumnName.Trim();

            ValidateUserTableName(tableName);
            ValidateUserTableName(referencedTableName);

            string specKey = BuildSpecKey(tableName, columnName, referencedTableName, referencedColumnName, spec.OnDelete);
            if (!seenSpecKeys.Add(specKey))
            {
                throw new CSharpDbException(
                    ErrorCode.ConstraintViolation,
                    $"Duplicate foreign key migration spec for '{tableName}.{columnName}' referencing '{referencedTableName}.{referencedColumnName ?? "<pk>"}'.");
            }

            string childColumnKey = $"{tableName}|{columnName}";
            if (!seenChildColumns.Add(childColumnKey))
            {
                throw new CSharpDbException(
                    ErrorCode.ConstraintViolation,
                    $"Foreign key migration only supports one constraint per child column. '{tableName}.{columnName}' was specified multiple times.");
            }

            if (!specsByTable.TryGetValue(tableName, out var specs))
            {
                specs = [];
                specsByTable[tableName] = specs;
            }

            specs.Add(new DatabaseForeignKeyMigrationConstraintSpec
            {
                TableName = tableName,
                ColumnName = columnName,
                ReferencedTableName = referencedTableName,
                ReferencedColumnName = referencedColumnName,
                OnDelete = spec.OnDelete,
            });
        }

        Dictionary<string, int> orderedTables = OrderTables(specsByTable)
            .Select((tableName, index) => (tableName, index))
            .ToDictionary(static item => item.tableName, static item => item.index, StringComparer.OrdinalIgnoreCase);

        return specsByTable
            .OrderBy(entry => orderedTables[entry.Key])
            .Select(entry => BuildPlan(catalog, entry.Key, entry.Value))
            .ToArray();
    }

    private static TableMigrationPlan BuildPlan(
        SchemaCatalog catalog,
        string tableName,
        IReadOnlyList<DatabaseForeignKeyMigrationConstraintSpec> requestedSpecs)
    {
        TableSchema originalSchema = catalog.GetTable(tableName)
            ?? throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{tableName}' not found.");

        foreach (DatabaseForeignKeyMigrationConstraintSpec spec in requestedSpecs)
        {
            if (originalSchema.GetColumnIndex(spec.ColumnName) < 0)
                throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{spec.ColumnName}' not found in table '{tableName}'.");

            if (originalSchema.ForeignKeys.Any(existing =>
                    string.Equals(existing.ColumnName, spec.ColumnName, StringComparison.OrdinalIgnoreCase)))
            {
                throw new CSharpDbException(
                    ErrorCode.ConstraintViolation,
                    $"Column '{tableName}.{spec.ColumnName}' already has a foreign key constraint.");
            }
        }

        TableSchema currentSchema = new()
        {
            TableName = tableName,
            Columns = CloneColumns(originalSchema.Columns),
            ForeignKeys = originalSchema.ForeignKeys.ToArray(),
            NextRowId = originalSchema.NextRowId,
        };

        var materializedForeignKeys = new List<MaterializedForeignKey>(requestedSpecs.Count);
        foreach (DatabaseForeignKeyMigrationConstraintSpec spec in requestedSpecs)
        {
            ForeignKeyDefinition definition = ValidateAndMaterializeForeignKey(
                catalog,
                tableName,
                currentSchema,
                spec.ColumnName,
                spec.ReferencedTableName,
                spec.ReferencedColumnName,
                spec.OnDelete);

            materializedForeignKeys.Add(new MaterializedForeignKey(spec, definition));
            currentSchema = new TableSchema
            {
                TableName = currentSchema.TableName,
                Columns = currentSchema.Columns,
                ForeignKeys = currentSchema.ForeignKeys.Concat([definition]).ToArray(),
                NextRowId = currentSchema.NextRowId,
            };
        }

        string tempTableName = GenerateTempTableName(catalog, tableName, materializedForeignKeys.Select(static fk => fk.Definition.ConstraintName));
        ColumnDefinition[] clonedColumns = CloneColumns(originalSchema.Columns);
        ColumnStatistics[] clonedStats = catalog.GetColumnStatistics(tableName)
            .Select(stat => new ColumnStatistics
            {
                TableName = tableName,
                ColumnName = stat.ColumnName,
                DistinctCount = stat.DistinctCount,
                NonNullCount = stat.NonNullCount,
                MinValue = stat.MinValue,
                MaxValue = stat.MaxValue,
                IsStale = stat.IsStale,
            })
            .ToArray();

        var tempSchema = new TableSchema
        {
            TableName = tempTableName,
            Columns = clonedColumns,
            ForeignKeys = Array.Empty<ForeignKeyDefinition>(),
            NextRowId = originalSchema.NextRowId,
        };

        var renamedTempSchema = new TableSchema
        {
            TableName = tableName,
            Columns = clonedColumns,
            ForeignKeys = Array.Empty<ForeignKeyDefinition>(),
            NextRowId = originalSchema.NextRowId,
        };

        IndexSchema[] recreatedIndexes = catalog.GetIndexesForTable(tableName)
            .Where(index => index.Kind != IndexKind.ForeignKeyInternal)
            .Select(index => CloneIndexSchema(index, tableName))
            .ToArray();

        return new TableMigrationPlan(
            originalSchema,
            tempSchema,
            renamedTempSchema,
            new TableSchema
            {
                TableName = tableName,
                Columns = clonedColumns,
                ForeignKeys = originalSchema.ForeignKeys.Concat(materializedForeignKeys.Select(static fk => fk.Definition)).ToArray(),
                NextRowId = originalSchema.NextRowId,
            },
            recreatedIndexes,
            catalog.GetTriggersForTable(tableName).ToArray(),
            clonedStats,
            materializedForeignKeys.ToArray());
    }

    private static async ValueTask<ValidationAccumulator> ValidateDataAsync(
        StorageEngineContext context,
        IReadOnlyList<TableMigrationPlan> plans,
        int sampleLimit,
        CancellationToken ct)
    {
        var accumulator = new ValidationAccumulator(sampleLimit);
        foreach (TableMigrationPlan plan in plans)
        {
            if (plan.NewForeignKeys.Length == 0)
                continue;

            BTree tableTree = context.Catalog.GetTableTree(plan.OriginalSchema.TableName);
            IRecordSerializer serializer = GetReadSerializer(context.RecordSerializer, plan.OriginalSchema);
            var cursor = tableTree.CreateCursor();
            int primaryKeyColumnIndex = plan.OriginalSchema.PrimaryKeyColumnIndex;
            string childKeyColumnName = primaryKeyColumnIndex >= 0
                ? plan.OriginalSchema.Columns[primaryKeyColumnIndex].Name
                : "rowid";

            while (await cursor.MoveNextAsync(ct))
            {
                DbValue[] row = serializer.Decode(cursor.CurrentValue.Span);

                foreach (MaterializedForeignKey foreignKey in plan.NewForeignKeys)
                {
                    int childColumnIndex = plan.OriginalSchema.GetColumnIndex(foreignKey.Definition.ColumnName);
                    if (childColumnIndex < 0 || childColumnIndex >= row.Length)
                        continue;

                    DbValue childValue = row[childColumnIndex];
                    if (childValue.IsNull)
                        continue;

                    if (await ParentExistsAsync(context, foreignKey.Definition, childValue, ct))
                        continue;

                    DbValue childKeyValue = primaryKeyColumnIndex >= 0 && primaryKeyColumnIndex < row.Length
                        ? row[primaryKeyColumnIndex]
                        : DbValue.FromInteger(cursor.CurrentKey);

                    accumulator.AddViolation(new DatabaseForeignKeyMigrationViolation
                    {
                        TableName = plan.OriginalSchema.TableName,
                        ColumnName = foreignKey.Definition.ColumnName,
                        ReferencedTableName = foreignKey.Definition.ReferencedTableName,
                        ReferencedColumnName = foreignKey.Definition.ReferencedColumnName,
                        ChildKeyColumnName = childKeyColumnName,
                        ChildKeyValue = childKeyValue,
                        ChildValue = childValue,
                        Reason = MissingReferencedParentReason,
                    });
                }
            }
        }

        return accumulator;
    }

    private static async ValueTask<bool> ParentExistsAsync(
        StorageEngineContext context,
        ForeignKeyDefinition foreignKey,
        DbValue expectedValue,
        CancellationToken ct)
    {
        TableSchema parentSchema = context.Catalog.GetTable(foreignKey.ReferencedTableName)
            ?? throw new CSharpDbException(ErrorCode.TableNotFound, $"Referenced table '{foreignKey.ReferencedTableName}' not found.");
        int parentColumnIndex = parentSchema.GetColumnIndex(foreignKey.ReferencedColumnName);
        if (parentColumnIndex < 0)
        {
            throw new CSharpDbException(
                ErrorCode.ColumnNotFound,
                $"Referenced column '{foreignKey.ReferencedTableName}.{foreignKey.ReferencedColumnName}' was not found.");
        }

        if (parentColumnIndex == parentSchema.PrimaryKeyColumnIndex &&
            parentSchema.Columns[parentColumnIndex].Type == DbType.Integer &&
            expectedValue.Type == DbType.Integer)
        {
            return await context.Catalog.GetTableTree(parentSchema.TableName).FindMemoryAsync(expectedValue.AsInteger, ct) is not null;
        }

        IndexSchema? lookupIndex = FindSingleColumnLookupIndex(context.Catalog, parentSchema, parentColumnIndex);
        if (lookupIndex is not null &&
            TryBuildForeignKeyLookup(lookupIndex, parentSchema, parentColumnIndex, expectedValue, out long lookupKey, out DbValue[]? keyComponents, out SqlIndexStorageMode storageMode, out bool usesDirectIntegerKey))
        {
            byte[]? payload = await context.Catalog.GetIndexStore(lookupIndex.IndexName).FindAsync(lookupKey, ct);
            ReadOnlyMemory<byte> rowIdPayload = GetMatchingIndexRowIds(payload, keyComponents, storageMode, usesDirectIntegerKey);
            if (!rowIdPayload.IsEmpty)
            {
                int rowIdCount = RowIdPayloadCodec.GetCount(rowIdPayload.Span);
                BTree tableTree = context.Catalog.GetTableTree(parentSchema.TableName);
                for (int i = 0; i < rowIdCount; i++)
                {
                    long rowId = RowIdPayloadCodec.ReadAt(rowIdPayload.Span, i);
                    if (await tableTree.FindMemoryAsync(rowId, ct) is not null)
                        return true;
                }
            }
        }

        string? parentCollation = parentSchema.Columns[parentColumnIndex].Type == DbType.Text
            ? CollationSupport.NormalizeMetadataName(parentSchema.Columns[parentColumnIndex].Collation)
            : null;
        BTree scanTree = context.Catalog.GetTableTree(parentSchema.TableName);
        IRecordSerializer serializer = GetReadSerializer(context.RecordSerializer, parentSchema);
        var cursor = scanTree.CreateCursor();
        while (await cursor.MoveNextAsync(ct))
        {
            DbValue[] row = serializer.Decode(cursor.CurrentValue.Span);
            if (parentColumnIndex < row.Length &&
                !row[parentColumnIndex].IsNull &&
                CollationSupport.Compare(row[parentColumnIndex], expectedValue, parentCollation) == 0)
            {
                return true;
            }
        }

        return false;
    }

    private static void ValidateUserTableName(string tableName)
    {
        if (IsInternalTableName(tableName))
        {
            throw new CSharpDbException(
                ErrorCode.ConstraintViolation,
                $"Foreign key migration only supports user SQL tables. '{tableName}' is not eligible.");
        }
    }

    private static bool IsInternalTableName(string tableName)
        => tableName.StartsWith(CollectionPrefix, StringComparison.OrdinalIgnoreCase) ||
           tableName.StartsWith(InternalTablePrefix, StringComparison.OrdinalIgnoreCase) ||
           tableName.StartsWith(SystemTablePrefix, StringComparison.OrdinalIgnoreCase) ||
           tableName.StartsWith(SystemAliasPrefix, StringComparison.OrdinalIgnoreCase);

    private static string BuildSpecKey(
        string tableName,
        string columnName,
        string referencedTableName,
        string? referencedColumnName,
        ForeignKeyOnDeleteAction onDelete)
        => string.Join("|", tableName, columnName, referencedTableName, referencedColumnName ?? string.Empty, onDelete.ToString());

    private static IEnumerable<string> OrderTables(
        IReadOnlyDictionary<string, List<DatabaseForeignKeyMigrationConstraintSpec>> specsByTable)
    {
        var dependencies = specsByTable.ToDictionary(
            static entry => entry.Key,
            entry => new HashSet<string>(
                entry.Value
                    .Select(static spec => spec.ReferencedTableName)
                    .Where(specsByTable.ContainsKey),
                StringComparer.OrdinalIgnoreCase),
            StringComparer.OrdinalIgnoreCase);

        var pending = new SortedSet<string>(specsByTable.Keys, StringComparer.OrdinalIgnoreCase);
        var ordered = new List<string>(specsByTable.Count);
        while (pending.Count > 0)
        {
            string? next = pending.FirstOrDefault(table =>
                dependencies[table].All(dep =>
                    string.Equals(dep, table, StringComparison.OrdinalIgnoreCase) ||
                    ordered.Contains(dep, StringComparer.OrdinalIgnoreCase)));
            if (next is null)
                next = pending.Min;

            ordered.Add(next!);
            pending.Remove(next!);
        }

        return ordered;
    }

    private static ForeignKeyDefinition ValidateAndMaterializeForeignKey(
        SchemaCatalog catalog,
        string tableName,
        TableSchema currentTableSchema,
        string columnName,
        string referencedTableName,
        string? referencedColumnName,
        ForeignKeyOnDeleteAction onDelete)
    {
        int childColumnIndex = currentTableSchema.GetColumnIndex(columnName);
        if (childColumnIndex < 0)
            throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{columnName}' not found in table '{tableName}'.");

        ColumnDefinition childColumn = currentTableSchema.Columns[childColumnIndex];
        if (childColumn.Type is not (DbType.Integer or DbType.Text))
        {
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                $"Foreign key column '{columnName}' must use INTEGER or TEXT.");
        }

        TableSchema parentSchema = string.Equals(referencedTableName, tableName, StringComparison.OrdinalIgnoreCase)
            ? currentTableSchema
            : catalog.GetTable(referencedTableName)
                ?? throw new CSharpDbException(ErrorCode.TableNotFound, $"Referenced table '{referencedTableName}' not found.");

        string resolvedReferencedColumn = string.IsNullOrWhiteSpace(referencedColumnName)
            ? ResolvePrimaryKeyColumnName(parentSchema, columnName)
            : referencedColumnName.Trim();

        int parentColumnIndex = parentSchema.GetColumnIndex(resolvedReferencedColumn);
        if (parentColumnIndex < 0)
        {
            throw new CSharpDbException(
                ErrorCode.ColumnNotFound,
                $"Referenced column '{resolvedReferencedColumn}' was not found on table '{parentSchema.TableName}'.");
        }

        ColumnDefinition parentColumn = parentSchema.Columns[parentColumnIndex];
        if (parentColumn.Type != childColumn.Type)
        {
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                $"Foreign key column '{columnName}' type '{childColumn.Type}' does not match referenced column '{parentSchema.TableName}.{resolvedReferencedColumn}' type '{parentColumn.Type}'.");
        }

        string? childCollation = childColumn.Type == DbType.Text
            ? CollationSupport.NormalizeMetadataName(childColumn.Collation)
            : null;
        if (!CanUseParentColumnForForeignKey(catalog, parentSchema, parentColumnIndex, childCollation, excludedIndexName: null))
        {
            throw new CSharpDbException(
                ErrorCode.ConstraintViolation,
                $"Referenced column '{parentSchema.TableName}.{resolvedReferencedColumn}' must be a single-column PRIMARY KEY or UNIQUE index with matching collation.");
        }

        string constraintName = GenerateForeignKeyConstraintName(
            tableName,
            columnName,
            parentSchema.TableName,
            resolvedReferencedColumn);

        return new ForeignKeyDefinition
        {
            ConstraintName = constraintName,
            ColumnName = columnName,
            ReferencedTableName = parentSchema.TableName,
            ReferencedColumnName = resolvedReferencedColumn,
            OnDelete = onDelete,
            SupportingIndexName = GenerateForeignKeySupportIndexName(constraintName, tableName, columnName),
        };
    }

    private static string ResolvePrimaryKeyColumnName(TableSchema parentSchema, string childColumnName)
    {
        int parentPrimaryKeyIndex = parentSchema.PrimaryKeyColumnIndex;
        if (parentPrimaryKeyIndex < 0)
        {
            throw new CSharpDbException(
                ErrorCode.ConstraintViolation,
                $"Foreign key column '{childColumnName}' references table '{parentSchema.TableName}', which does not have a primary key.");
        }

        return parentSchema.Columns[parentPrimaryKeyIndex].Name;
    }

    private static bool CanUseParentColumnForForeignKey(
        SchemaCatalog catalog,
        TableSchema parentSchema,
        int parentColumnIndex,
        string? expectedTextCollation,
        string? excludedIndexName)
    {
        ColumnDefinition parentColumn = parentSchema.Columns[parentColumnIndex];
        if (parentColumn.Type == DbType.Text &&
            !CollationSupport.SemanticallyEquals(
                expectedTextCollation,
                CollationSupport.NormalizeMetadataName(parentColumn.Collation)) &&
            !HasCompatibleUniqueParentIndex(catalog, parentSchema, parentColumnIndex, expectedTextCollation, excludedIndexName))
        {
            return false;
        }

        if (parentColumn.IsPrimaryKey)
            return true;

        return HasCompatibleUniqueParentIndex(catalog, parentSchema, parentColumnIndex, expectedTextCollation, excludedIndexName);
    }

    private static bool HasCompatibleUniqueParentIndex(
        SchemaCatalog catalog,
        TableSchema parentSchema,
        int parentColumnIndex,
        string? expectedTextCollation,
        string? excludedIndexName)
    {
        foreach (IndexSchema index in catalog.GetSqlIndexesForTable(parentSchema.TableName))
        {
            if (!index.IsUnique ||
                index.Columns.Count != 1 ||
                !string.Equals(index.Columns[0], parentSchema.Columns[parentColumnIndex].Name, StringComparison.OrdinalIgnoreCase) ||
                (excludedIndexName is not null && string.Equals(index.IndexName, excludedIndexName, StringComparison.OrdinalIgnoreCase)))
            {
                continue;
            }

            string?[] effectiveCollations = CollationSupport.GetEffectiveIndexColumnCollations(index, parentSchema, [parentColumnIndex]);
            string? effectiveCollation = effectiveCollations.Length > 0
                ? CollationSupport.NormalizeMetadataName(effectiveCollations[0])
                : null;
            if (parentSchema.Columns[parentColumnIndex].Type == DbType.Text &&
                !CollationSupport.SemanticallyEquals(expectedTextCollation, effectiveCollation))
            {
                continue;
            }

            return true;
        }

        return false;
    }

    private static IndexSchema? FindSingleColumnLookupIndex(
        SchemaCatalog catalog,
        TableSchema schema,
        int columnIndex)
    {
        string columnName = schema.Columns[columnIndex].Name;
        string? expectedCollation = schema.Columns[columnIndex].Type == DbType.Text
            ? CollationSupport.NormalizeMetadataName(schema.Columns[columnIndex].Collation)
            : null;

        IndexSchema? firstMatch = null;
        foreach (IndexSchema index in catalog.GetSqlIndexesForTable(schema.TableName))
        {
            if (index.Columns.Count != 1 ||
                !string.Equals(index.Columns[0], columnName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            string? indexCollation = index.ColumnCollations.Count > 0
                ? CollationSupport.NormalizeMetadataName(index.ColumnCollations[0])
                : expectedCollation;
            if (!CollationSupport.SemanticallyEquals(indexCollation, expectedCollation))
                continue;

            if (index.IsUnique)
                return index;

            firstMatch ??= index;
        }

        return firstMatch;
    }

    private static bool TryBuildForeignKeyLookup(
        IndexSchema index,
        TableSchema schema,
        int columnIndex,
        DbValue value,
        out long lookupKey,
        out DbValue[]? keyComponents,
        out SqlIndexStorageMode storageMode,
        out bool usesDirectIntegerKey)
    {
        lookupKey = 0;
        keyComponents = null;
        storageMode = SqlIndexStorageMode.Hashed;
        usesDirectIntegerKey = false;

        if (index.Columns.Count != 1 || value.IsNull)
            return false;

        int[] columnIndices = [columnIndex];
        string?[] indexColumnCollations = CollationSupport.GetEffectiveIndexColumnCollations(index, schema, columnIndices);
        storageMode = IndexMaintenanceHelper.ResolveSqlIndexStorageMode(index, schema);
        usesDirectIntegerKey = schema.Columns[columnIndex].Type == DbType.Integer;
        if (usesDirectIntegerKey)
        {
            if (value.Type != DbType.Integer)
                return false;

            lookupKey = value.AsInteger;
            return true;
        }

        if (value.Type is not (DbType.Integer or DbType.Text))
            return false;

        DbValue normalizedValue = CollationSupport.NormalizeIndexValue(
            value,
            indexColumnCollations.Length > 0 ? indexColumnCollations[0] : null);
        keyComponents = [normalizedValue];
        lookupKey = storageMode == SqlIndexStorageMode.OrderedText
            ? OrderedTextIndexKeyCodec.ComputeKey(normalizedValue.AsText)
            : IndexMaintenanceHelper.ComputeIndexKey(keyComponents);
        return true;
    }

    private static ReadOnlyMemory<byte> GetMatchingIndexRowIds(
        byte[]? payload,
        DbValue[]? keyComponents,
        SqlIndexStorageMode storageMode,
        bool usesDirectIntegerKey)
    {
        if (payload is null || payload.Length == 0)
            return ReadOnlyMemory<byte>.Empty;

        if (usesDirectIntegerKey)
            return payload;

        if (storageMode == SqlIndexStorageMode.OrderedText)
        {
            return keyComponents is [var keyComponent] &&
                   keyComponent.Type == DbType.Text &&
                   OrderedTextIndexPayloadCodec.TryGetMatchingRowIdPayloadSlice(payload, keyComponent.AsText, out ReadOnlyMemory<byte> orderedRowIds)
                ? orderedRowIds
                : ReadOnlyMemory<byte>.Empty;
        }

        if (keyComponents is null)
            return ReadOnlyMemory<byte>.Empty;

        if (!HashedIndexPayloadCodec.TryGetMatchingRowIds(payload, keyComponents, out byte[]? hashedRowIds))
            return ReadOnlyMemory<byte>.Empty;

        return hashedRowIds ?? ReadOnlyMemory<byte>.Empty;
    }

    private static async ValueTask CreateForeignKeySupportIndexAsync(
        StorageEngineContext context,
        TableSchema tableSchema,
        ForeignKeyDefinition foreignKey,
        CancellationToken ct)
    {
        int columnIndex = tableSchema.GetColumnIndex(foreignKey.ColumnName);
        ColumnDefinition column = tableSchema.Columns[columnIndex];
        string? columnCollation = column.Type == DbType.Text
            ? CollationSupport.NormalizeMetadataName(column.Collation)
            : null;

        var indexSchema = new IndexSchema
        {
            IndexName = foreignKey.SupportingIndexName,
            TableName = tableSchema.TableName,
            Columns = [foreignKey.ColumnName],
            ColumnCollations = new string?[] { columnCollation },
            IsUnique = false,
            Kind = IndexKind.ForeignKeyInternal,
            OwnerIndexName = foreignKey.ConstraintName,
        };

        await context.Catalog.CreateIndexAsync(indexSchema, ct);
        await BackfillIndexAsync(context, indexSchema, tableSchema, ct);
    }

    private static async ValueTask BackfillIndexAsync(
        StorageEngineContext context,
        IndexSchema indexSchema,
        TableSchema tableSchema,
        CancellationToken ct)
    {
        await IndexMaintenanceHelper.BackfillIndexAsync(
            context.Catalog,
            tableSchema,
            indexSchema,
            GetReadSerializer(context.RecordSerializer, tableSchema),
            ct);
    }

    private static IRecordSerializer GetReadSerializer(IRecordSerializer recordSerializer, TableSchema schema)
        => schema.TableName.StartsWith(CollectionPrefix, StringComparison.Ordinal)
            ? new CollectionAwareRecordSerializer(recordSerializer)
            : recordSerializer;

    private static ColumnDefinition[] CloneColumns(IReadOnlyList<ColumnDefinition> columns)
        => columns.Select(column => new ColumnDefinition
        {
            Name = column.Name,
            Type = column.Type,
            Nullable = column.Nullable,
            IsPrimaryKey = column.IsPrimaryKey,
            IsIdentity = column.IsIdentity,
            Collation = column.Collation,
        }).ToArray();

    private static IndexSchema CloneIndexSchema(IndexSchema index, string tableName)
        => new()
        {
            IndexName = index.IndexName,
            TableName = tableName,
            Columns = index.Columns.ToArray(),
            ColumnCollations = index.ColumnCollations.ToArray(),
            IsUnique = index.IsUnique,
            Kind = index.Kind,
            State = index.State,
            OwnerIndexName = index.OwnerIndexName,
            OptionsJson = index.OptionsJson,
        };

    private static string GenerateTempTableName(
        SchemaCatalog catalog,
        string tableName,
        IEnumerable<string> constraintNames)
    {
        string joined = string.Join("|", constraintNames.OrderBy(static value => value, StringComparer.OrdinalIgnoreCase));
        string suffix = ComputeStableNameSuffix($"{tableName}|{joined}");
        string baseName = $"__migrate_{SanitizeNameSegment(tableName)}_{suffix}";
        string candidate = baseName;
        int counter = 1;
        while (catalog.GetTable(candidate) is not null)
            candidate = $"{baseName}_{counter++}";
        return candidate;
    }

    private static string GenerateForeignKeyConstraintName(
        string tableName,
        string columnName,
        string referencedTableName,
        string referencedColumnName)
        => $"fk_{SanitizeNameSegment(tableName)}_{SanitizeNameSegment(columnName)}_{ComputeStableNameSuffix($"{tableName}|{columnName}|{referencedTableName}|{referencedColumnName}")}";

    private static string GenerateForeignKeySupportIndexName(
        string constraintName,
        string tableName,
        string columnName)
        => $"__fk_{SanitizeNameSegment(tableName)}_{SanitizeNameSegment(columnName)}_{ComputeStableNameSuffix(constraintName)}";

    private static string SanitizeNameSegment(string value)
    {
        char[] chars = value.ToCharArray();
        for (int i = 0; i < chars.Length; i++)
        {
            if (!char.IsLetterOrDigit(chars[i]) && chars[i] != '_')
                chars[i] = '_';
        }

        return new string(chars);
    }

    private static string ComputeStableNameSuffix(string value)
    {
        byte[] hash = System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(value));
        return Convert.ToHexString(hash.AsSpan(0, 4)).ToLowerInvariant();
    }

    private static async ValueTask<StorageEngineContext> OpenStorageContextAsync(string databasePath, CancellationToken ct)
    {
        var factory = new DefaultStorageEngineFactory();
        return await factory.OpenAsync(databasePath, new StorageEngineOptions(), ct);
    }

    private static async ValueTask<long> CopyTableRowsAsync(
        BTree sourceTree,
        BTree destinationTree,
        CancellationToken ct)
    {
        var cursor = sourceTree.CreateCursor();
        long count = 0;
        while (await cursor.MoveNextAsync(ct))
        {
            await destinationTree.InsertAsync(cursor.CurrentKey, cursor.CurrentValue, ct);
            count++;
        }

        return count;
    }

    private sealed class ValidationAccumulator(int sampleLimit)
    {
        private readonly List<DatabaseForeignKeyMigrationViolation> _violations = sampleLimit > 0
            ? new List<DatabaseForeignKeyMigrationViolation>(sampleLimit)
            : [];

        public int ViolationCount { get; private set; }

        public IReadOnlyList<DatabaseForeignKeyMigrationViolation> Violations => _violations;

        public void AddViolation(DatabaseForeignKeyMigrationViolation violation)
        {
            ViolationCount++;
            if (sampleLimit > 0 && _violations.Count < sampleLimit)
                _violations.Add(violation);
        }
    }

    private sealed record MaterializedForeignKey(
        DatabaseForeignKeyMigrationConstraintSpec Spec,
        ForeignKeyDefinition Definition)
    {
        public string TableName => Spec.TableName;
    }

    private sealed record TableMigrationPlan(
        TableSchema OriginalSchema,
        TableSchema TempSchema,
        TableSchema RenamedTempSchema,
        TableSchema FinalSchema,
        IndexSchema[] RecreatedIndexes,
        TriggerSchema[] OriginalTriggers,
        ColumnStatistics[] ColumnStatistics,
        MaterializedForeignKey[] NewForeignKeys);
}
