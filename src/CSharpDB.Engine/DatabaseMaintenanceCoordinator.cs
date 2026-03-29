using System.Text;
using System.Text.Json;
using CSharpDB.Primitives;
using CSharpDB.Execution;
using CSharpDB.Storage.Diagnostics;
using CSharpDB.Storage.Indexing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Serialization;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Engine;

public static class DatabaseMaintenanceCoordinator
{
    private const string CollectionPrefix = "_col_";
    private const string CollectionIndexPrefix = "_cidx_";
    private const string ProcedureTableName = "__procedures";
    private const string SavedQueryTableName = "__saved_queries";

    public static async ValueTask<DatabaseMaintenanceReport> GetMaintenanceReportAsync(
        string databasePath,
        CancellationToken ct = default)
    {
        string fullPath = Path.GetFullPath(databasePath);
        var dbReport = await DatabaseInspector.InspectAsync(
            fullPath,
            new DatabaseInspectOptions { IncludePages = true },
            ct);
        var walReport = await WalInspector.InspectAsync(fullPath, options: null, ct);

        int freelistPageCount = dbReport.PageTypeHistogram.TryGetValue("freelist", out int freelistCount)
            ? freelistCount
            : 0;
        int pageSizeBytes = dbReport.Header.PageSizeValid ? dbReport.Header.PageSize : PageConstants.PageSize;
        IReadOnlyList<PageReport> pages = dbReport.Pages ?? [];
        var btreePages = pages.Where(page => !string.Equals(page.PageTypeName, "freelist", StringComparison.OrdinalIgnoreCase)).ToArray();

        long btreeFreeBytes = 0;
        int pagesWithFreeSpace = 0;
        foreach (var page in btreePages)
        {
            if (page.FreeSpaceBytes <= 0)
                continue;

            btreeFreeBytes += page.FreeSpaceBytes;
            pagesWithFreeSpace++;
        }

        int tailFreelistPageCount = ComputeTailFreelistPageCount(pages, dbReport.Header.DeclaredPageCount);

        return new DatabaseMaintenanceReport
        {
            DatabasePath = fullPath,
            SpaceUsage = new DatabaseSpaceUsageReport
            {
                DatabaseFileBytes = dbReport.Header.FileLengthBytes,
                WalFileBytes = walReport.Exists ? walReport.FileLengthBytes : 0,
                PageSizeBytes = pageSizeBytes,
                PhysicalPageCount = dbReport.Header.PhysicalPageCount,
                DeclaredPageCount = dbReport.Header.DeclaredPageCount,
                FreelistPageCount = freelistPageCount,
                FreelistBytes = (long)freelistPageCount * pageSizeBytes,
            },
            Fragmentation = new DatabaseFragmentationReport
            {
                BTreeFreeBytes = btreeFreeBytes,
                PagesWithFreeSpace = pagesWithFreeSpace,
                TailFreelistPageCount = tailFreelistPageCount,
                TailFreelistBytes = (long)tailFreelistPageCount * pageSizeBytes,
            },
            PageTypeHistogram = new Dictionary<string, int>(dbReport.PageTypeHistogram, StringComparer.OrdinalIgnoreCase),
        };
    }

    public static async ValueTask<DatabaseReindexResult> ReindexAsync(
        string databasePath,
        DatabaseReindexRequest request,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string fullPath = Path.GetFullPath(databasePath);
        StorageEngineContext? context = null;

        try
        {
            context = await OpenStorageContextAsync(fullPath, ct);
            var indexes = ResolveTargetIndexes(context.Catalog, request).ToArray();
            var affectedTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            int recoveredCorruptIndexCount = 0;

            await context.Pager.BeginTransactionAsync(ct);
            try
            {
                foreach (var indexSchema in indexes)
                {
                    affectedTables.Add(indexSchema.TableName);

                    bool recoveredCorruptIndex = request.AllowCorruptIndexRecovery
                        ? await context.Catalog.DropIndexAllowCorruptReclaimAsync(indexSchema.IndexName, ct)
                        : false;
                    if (!request.AllowCorruptIndexRecovery)
                        await context.Catalog.DropIndexAsync(indexSchema.IndexName, ct);

                    await CreateAndBackfillIndexWithOrderedTextFallbackAsync(context, indexSchema, ct);

                    if (recoveredCorruptIndex)
                        recoveredCorruptIndexCount++;
                }

                foreach (string tableName in affectedTables)
                    await context.Catalog.PersistRootPageChangesAsync(tableName, ct);

                await context.Pager.CommitAsync(ct);
            }
            catch
            {
                await context.Pager.RollbackAsync(ct);
                throw;
            }

            return new DatabaseReindexResult
            {
                Scope = request.Scope,
                Name = request.Name,
                RebuiltIndexCount = indexes.Length,
                RecoveredCorruptIndexCount = recoveredCorruptIndexCount,
            };
        }
        finally
        {
            if (context is not null)
                await context.Pager.DisposeAsync();
        }
    }

    public static async ValueTask<DatabaseVacuumResult> VacuumAsync(
        string databasePath,
        CancellationToken ct = default)
        => await VacuumAsync(databasePath, ct, ReplaceVacuumedDatabaseFilesAsync);

    internal static async ValueTask<DatabaseVacuumResult> VacuumAsync(
        string databasePath,
        CancellationToken ct,
        Func<string, string, string, CancellationToken, ValueTask<bool>> replaceDatabaseFilesAsync,
        string? tempPathOverride = null,
        string? backupPathOverride = null)
    {
        string fullPath = Path.GetFullPath(databasePath);
        string tempPath = tempPathOverride ?? (fullPath + $".vacuum.{Guid.NewGuid():N}.tmp");
        string backupPath = backupPathOverride ?? (fullPath + $".vacuumbak.{Guid.NewGuid():N}.tmp");

        StorageEngineContext? source = null;
        StorageEngineContext? destination = null;
        DatabaseInspectReport? beforeReport = null;
        bool deleteBackupFiles = false;

        try
        {
            beforeReport = await DatabaseInspector.InspectAsync(fullPath, new DatabaseInspectOptions(), ct);
            source = await OpenStorageContextAsync(fullPath, ct);
            destination = await OpenStorageContextAsync(tempPath, ct);

            await destination.Pager.BeginTransactionAsync(ct);
            try
            {
                await CopyDatabaseAsync(source, destination, ct);
                await destination.Catalog.PersistDirtyAdvisoryStatisticsAsync(ct);
                await destination.Catalog.PersistAllRootPageChangesAsync(ct);
                await destination.Pager.CommitAsync(ct);
            }
            catch
            {
                await destination.Pager.RollbackAsync(ct);
                throw;
            }

            await destination.Pager.DisposeAsync();
            destination = null;

            // The vacuum copy path (CopyDatabaseAsync → CopyMetadataTableRowsAsync) already
            // preserves __saved_queries and __procedures rows with correct row counts.
            // Calling RestoreClientMetadataAsync here would DELETE+INSERT the same data,
            // triggering row-count guard violations. Skip the restore since the copy
            // handles metadata preservation.

            await source.Pager.DisposeAsync();
            source = null;

            deleteBackupFiles = await replaceDatabaseFilesAsync(fullPath, tempPath, backupPath, ct);

            var afterReport = await DatabaseInspector.InspectAsync(fullPath, new DatabaseInspectOptions(), ct);
            return new DatabaseVacuumResult
            {
                DatabaseFileBytesBefore = beforeReport.Header.FileLengthBytes,
                DatabaseFileBytesAfter = afterReport.Header.FileLengthBytes,
                PhysicalPageCountBefore = beforeReport.Header.PhysicalPageCount,
                PhysicalPageCountAfter = afterReport.Header.PhysicalPageCount,
            };
        }
        finally
        {
            if (destination is not null)
                await destination.Pager.DisposeAsync();
            if (source is not null)
                await source.Pager.DisposeAsync();

            TryDeleteFile(tempPath);
            TryDeleteFile(tempPath + ".wal");

            if (deleteBackupFiles)
            {
                TryDeleteFile(backupPath);
                TryDeleteFile(backupPath + ".wal");
            }
        }
    }

    private static ValueTask<bool> ReplaceVacuumedDatabaseFilesAsync(
        string fullPath,
        string tempPath,
        string backupPath,
        CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        string sourceWalPath = fullPath + ".wal";
        if (File.Exists(sourceWalPath))
            File.Delete(sourceWalPath);

        File.Move(fullPath, backupPath, overwrite: true);
        try
        {
            File.Move(tempPath, fullPath, overwrite: true);
            return ValueTask.FromResult(true);
        }
        catch
        {
            if (File.Exists(backupPath) && !File.Exists(fullPath))
            {
                try
                {
                    File.Move(backupPath, fullPath, overwrite: true);
                    return ValueTask.FromResult(true);
                }
                catch
                {
                    // Keep the backup in place when rollback fails.
                }
            }

            throw;
        }
    }

    private static async ValueTask CopyDatabaseAsync(
        StorageEngineContext source,
        StorageEngineContext destination,
        CancellationToken ct)
    {
        foreach (string tableName in GetTablesToCopy(source.Catalog))
        {
            var schema = source.Catalog.GetTable(tableName);
            if (schema is null)
                continue;

            await destination.Catalog.CreateTableExactAsync(CloneTableSchema(schema), ct);
            long copiedRowCount;
            if (IsClientMetadataTable(tableName))
                copiedRowCount = await CopyMetadataTableRowsAsync(source, destination, schema, ct);
            else
                copiedRowCount = await CopyTableRowsAsync(source, destination, tableName, ct);

            await destination.Catalog.SetTableRowCountAsync(tableName, copiedRowCount, ct);
            var columnStats = source.Catalog.GetColumnStatistics(tableName);
            if (columnStats.Count > 0)
                await destination.Catalog.ReplaceColumnStatisticsAsync(tableName, columnStats.ToArray(), ct);
            await destination.Catalog.PersistRootPageChangesAsync(tableName, ct);
        }

        foreach (var indexSchema in source.Catalog.GetIndexes()
                     .OrderBy(index => index.TableName, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(index => index.IndexName, StringComparer.OrdinalIgnoreCase))
        {
            await destination.Catalog.CreateIndexAsync(indexSchema, ct);
            await BackfillIndexAsync(destination, indexSchema, ct);
            await destination.Catalog.PersistRootPageChangesAsync(indexSchema.TableName, ct);
        }

        foreach (string viewName in source.Catalog.GetViewNames().OrderBy(name => name, StringComparer.OrdinalIgnoreCase))
        {
            string? sql = source.Catalog.GetViewSql(viewName);
            if (!string.IsNullOrWhiteSpace(sql))
                await destination.Catalog.CreateViewAsync(viewName, sql, ct);
        }

        foreach (var trigger in source.Catalog.GetTriggers().OrderBy(item => item.TriggerName, StringComparer.OrdinalIgnoreCase))
            await destination.Catalog.CreateTriggerAsync(trigger, ct);
    }

    private static IEnumerable<IndexSchema> ResolveTargetIndexes(
        SchemaCatalog catalog,
        DatabaseReindexRequest request)
    {
        return request.Scope switch
        {
            DatabaseReindexScope.All => catalog.GetIndexes().OrderBy(index => index.IndexName, StringComparer.OrdinalIgnoreCase),
            DatabaseReindexScope.Table => ResolveTableIndexes(catalog, request.Name),
            DatabaseReindexScope.Index => [ResolveSingleIndex(catalog, request.Name)],
            _ => throw new ArgumentOutOfRangeException(nameof(request.Scope), request.Scope, null),
        };
    }

    private static IEnumerable<IndexSchema> ResolveTableIndexes(SchemaCatalog catalog, string? tableName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        string normalized = tableName.Trim();
        if (catalog.GetTable(normalized) is null)
            throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{normalized}' not found.");

        return catalog.GetIndexesForTable(normalized)
            .OrderBy(index => index.IndexName, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static IndexSchema ResolveSingleIndex(SchemaCatalog catalog, string? indexName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(indexName);
        string normalized = indexName.Trim();
        return catalog.GetIndex(normalized)
            ?? throw new CSharpDbException(ErrorCode.TableNotFound, $"Index '{normalized}' not found.");
    }

    private static async ValueTask BackfillIndexAsync(
        StorageEngineContext context,
        IndexSchema indexSchema,
        CancellationToken ct)
    {
        if (IsCollectionIndexSchema(indexSchema))
        {
            await BackfillCollectionIndexAsync(context, indexSchema, ct);
            return;
        }

        var tableSchema = context.Catalog.GetTable(indexSchema.TableName)
            ?? throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{indexSchema.TableName}' not found.");
        await IndexMaintenanceHelper.BackfillIndexAsync(
            context.Catalog,
            tableSchema,
            indexSchema,
            GetReadSerializer(context.RecordSerializer, tableSchema),
            ct);
    }

    private static async ValueTask CreateAndBackfillIndexWithOrderedTextFallbackAsync(
        StorageEngineContext context,
        IndexSchema indexSchema,
        CancellationToken ct)
    {
        await context.Catalog.CreateIndexAsync(indexSchema, ct);
        try
        {
            await BackfillIndexAsync(context, indexSchema, ct);
        }
        catch (OrderedTextIndexOverflowException) when (TryCreateHashedFallbackSchema(context.Catalog, indexSchema, out IndexSchema? fallbackSchema))
        {
            await context.Catalog.DropIndexAsync(indexSchema.IndexName, ct);
            await context.Catalog.CreateIndexAsync(fallbackSchema!, ct);
            await BackfillIndexAsync(context, fallbackSchema!, ct);
        }
    }

    private static bool TryCreateHashedFallbackSchema(
        SchemaCatalog catalog,
        IndexSchema indexSchema,
        out IndexSchema? fallbackSchema)
    {
        fallbackSchema = null;
        if (indexSchema.Kind != IndexKind.Sql || indexSchema.Columns.Count != 1)
            return false;

        TableSchema? tableSchema = catalog.GetTable(indexSchema.TableName);
        if (tableSchema == null)
            return false;

        int columnIndex = tableSchema.GetColumnIndex(indexSchema.Columns[0]);
        if (columnIndex < 0 || columnIndex >= tableSchema.Columns.Count || tableSchema.Columns[columnIndex].Type != DbType.Text)
            return false;

        fallbackSchema = new IndexSchema
        {
            IndexName = indexSchema.IndexName,
            TableName = indexSchema.TableName,
            Columns = indexSchema.Columns,
            ColumnCollations = indexSchema.ColumnCollations,
            IsUnique = indexSchema.IsUnique,
            Kind = indexSchema.Kind,
            State = indexSchema.State,
            OwnerIndexName = indexSchema.OwnerIndexName,
            OptionsJson = null,
        };
        return true;
    }

    private static async ValueTask BackfillCollectionIndexAsync(
        StorageEngineContext context,
        IndexSchema indexSchema,
        CancellationToken ct)
    {
        string fieldPath = indexSchema.Columns[0];
        string? textCollation = indexSchema.ColumnCollations.Count == 0
            ? null
            : CollationSupport.NormalizeMetadataName(indexSchema.ColumnCollations[0]);
        var payloadAccessor = CollectionFieldAccessor.FromFieldPath(fieldPath);
        var tableTree = context.Catalog.GetTableTree(indexSchema.TableName);
        var indexStore = context.Catalog.GetIndexStore(indexSchema.IndexName);
        var cursor = tableTree.CreateCursor();
        var integerKeys = new HashSet<long>();
        var textValues = new HashSet<string>(StringComparer.Ordinal);
        var groupedRowIds = new SortedDictionary<long, List<long>>();
        var groupedTextRowIds = new SortedDictionary<long, SortedDictionary<string, List<long>>>();

        while (await cursor.MoveNextAsync(ct))
        {
            integerKeys.Clear();
            textValues.Clear();
            if (!TryCollectCollectionIndexEntries(
                    cursor.CurrentValue.Span,
                    payloadAccessor,
                    context.RecordSerializer,
                    textCollation,
                    integerKeys,
                    textValues))
            {
                continue;
            }

            foreach (long indexKey in integerKeys)
                AddGroupedRowId(groupedRowIds, indexKey, cursor.CurrentKey);

            foreach (string textValue in textValues)
                AddGroupedTextRowId(groupedTextRowIds, textValue, cursor.CurrentKey);
        }

        foreach (var entry in groupedRowIds)
            await indexStore.InsertAsync(entry.Key, RowIdPayloadCodec.CreateFromSorted(entry.Value), ct);

        foreach (var entry in groupedTextRowIds)
            await indexStore.InsertAsync(entry.Key, OrderedTextIndexPayloadCodec.CreateFromSorted(entry.Value), ct);
    }

    private static bool TryCollectCollectionIndexEntries(
        ReadOnlySpan<byte> payload,
        CollectionFieldAccessor payloadAccessor,
        IRecordSerializer recordSerializer,
        string? textCollation,
        HashSet<long> integerKeys,
        HashSet<string> textValues)
    {
        if (TryCollectCollectionIndexEntriesFromDirectPayload(
                payload,
                payloadAccessor,
                textCollation,
                integerKeys,
                textValues))
        {
            return true;
        }

        try
        {
            string json = recordSerializer.DecodeColumn(payload, 1).AsText;
            using var document = JsonDocument.Parse(json);
            return TryCollectCollectionIndexEntriesFromJson(
                document.RootElement,
                payloadAccessor,
                textCollation,
                integerKeys,
                textValues);
        }
        catch
        {
            return false;
        }
    }

    private static bool TryCollectCollectionIndexEntriesFromDirectPayload(
        ReadOnlySpan<byte> payload,
        CollectionFieldAccessor payloadAccessor,
        string? textCollation,
        HashSet<long> integerKeys,
        HashSet<string> textValues)
    {
        int startCount = integerKeys.Count + textValues.Count;

        if (payloadAccessor.TargetsArrayElements)
        {
            var values = new List<DbValue>();
            if (!payloadAccessor.TryReadIndexValues(payload, values))
                return false;

            for (int i = 0; i < values.Count; i++)
            {
                AddCollectionIndexEntry(values[i], textCollation, integerKeys, textValues);
            }

            return (integerKeys.Count + textValues.Count) != startCount;
        }

        if (!payloadAccessor.TryReadValue(payload, out var value))
            return false;

        AddCollectionIndexEntry(value, textCollation, integerKeys, textValues);

        return (integerKeys.Count + textValues.Count) != startCount;
    }

    private static bool TryCollectCollectionIndexEntriesFromJson(
        JsonElement document,
        CollectionFieldAccessor payloadAccessor,
        string? textCollation,
        HashSet<long> integerKeys,
        HashSet<string> textValues)
    {
        int startCount = integerKeys.Count + textValues.Count;
        if (document.ValueKind != JsonValueKind.Object)
            return false;

        if (!TryGetJsonProperty(document, payloadAccessor.JsonPathSegments, out JsonElement property))
            return false;

        if (payloadAccessor.TargetsArrayElements)
        {
            if (property.ValueKind != JsonValueKind.Array)
                return false;

            foreach (JsonElement element in property.EnumerateArray())
            {
                if (element.ValueKind == JsonValueKind.String &&
                    AddCollectionIndexEntry(
                        DbValue.FromText(element.GetString()!),
                        textCollation,
                        integerKeys,
                        textValues))
                {
                    continue;
                }

                if (element.ValueKind == JsonValueKind.Number &&
                    element.TryGetInt64(out long integerValue) &&
                    AddCollectionIndexEntry(
                        DbValue.FromInteger(integerValue),
                        textCollation,
                        integerKeys,
                        textValues))
                {
                    continue;
                }
            }

            return (integerKeys.Count + textValues.Count) != startCount;
        }

        return property.ValueKind switch
        {
            JsonValueKind.String => AddCollectionIndexEntry(
                DbValue.FromText(property.GetString()!),
                textCollation,
                integerKeys,
                textValues),
            JsonValueKind.Number when property.TryGetInt64(out long integerValue) &&
                                      AddCollectionIndexEntry(
                                          DbValue.FromInteger(integerValue),
                                          textCollation,
                                          integerKeys,
                                          textValues) => true,
            _ => false,
        };
    }

    private static bool AddCollectionIndexEntry(
        DbValue value,
        string? textCollation,
        HashSet<long> integerKeys,
        HashSet<string> textValues)
    {
        if (value.IsNull)
            return false;

        if (value.Type == DbType.Integer)
        {
            integerKeys.Add(value.AsInteger);
            return true;
        }

        if (value.Type != DbType.Text)
            return false;

        textValues.Add(CollationSupport.NormalizeText(value.AsText, textCollation));
        return true;
    }

    private static bool TryGetJsonProperty(JsonElement document, IReadOnlyList<string> propertyPath, out JsonElement property)
    {
        JsonElement current = document;
        for (int i = 0; i < propertyPath.Count; i++)
        {
            if (current.ValueKind != JsonValueKind.Object)
            {
                property = default;
                return false;
            }

            string propertyName = propertyPath[i];
            if (current.TryGetProperty(propertyName, out JsonElement directProperty))
            {
                current = directProperty;
                continue;
            }

            bool found = false;
            foreach (JsonProperty candidate in current.EnumerateObject())
            {
                if (!string.Equals(candidate.Name, propertyName, StringComparison.OrdinalIgnoreCase))
                    continue;

                current = candidate.Value;
                found = true;
                break;
            }

            if (!found)
            {
                property = default;
                return false;
            }
        }

        property = current;
        return true;
    }

    private static void AddGroupedRowId(
        SortedDictionary<long, List<long>> groupedRowIds,
        long indexKey,
        long rowId)
    {
        if (!groupedRowIds.TryGetValue(indexKey, out var rowIds))
        {
            rowIds = new List<long>();
            groupedRowIds[indexKey] = rowIds;
        }

        rowIds.Add(rowId);
    }

    private static void AddGroupedTextRowId(
        SortedDictionary<long, SortedDictionary<string, List<long>>> groupedTextRowIds,
        string text,
        long rowId)
    {
        long indexKey = OrderedTextIndexKeyCodec.ComputeKey(text);
        if (!groupedTextRowIds.TryGetValue(indexKey, out var textBuckets))
        {
            textBuckets = new SortedDictionary<string, List<long>>(StringComparer.Ordinal);
            groupedTextRowIds[indexKey] = textBuckets;
        }

        if (!textBuckets.TryGetValue(text, out var rowIds))
        {
            rowIds = new List<long>();
            textBuckets[text] = rowIds;
        }

        rowIds.Add(rowId);
    }

    private static bool IsCollectionIndexSchema(IndexSchema schema)
    {
        return schema.IndexName.StartsWith(CollectionIndexPrefix, StringComparison.Ordinal) &&
               schema.TableName.StartsWith(CollectionPrefix, StringComparison.Ordinal) &&
               schema.Columns.Count == 1;
    }

    private static int ComputeTailFreelistPageCount(IReadOnlyList<PageReport> pages, uint declaredPageCount)
    {
        if (pages.Count == 0 || declaredPageCount == 0)
            return 0;

        var freelistPageIds = new HashSet<uint>(
            pages.Where(page => string.Equals(page.PageTypeName, "freelist", StringComparison.OrdinalIgnoreCase))
                .Select(page => page.PageId));

        int count = 0;
        for (uint pageId = declaredPageCount; pageId > 0; pageId--)
        {
            uint zeroBasedPageId = pageId - 1;
            if (!freelistPageIds.Contains(zeroBasedPageId))
                break;
            count++;
        }

        return count;
    }

    private static TableSchema CloneTableSchema(TableSchema schema)
    {
        return new TableSchema
        {
            TableName = schema.TableName,
            Columns = schema.Columns.Select(column => new ColumnDefinition
            {
                Name = column.Name,
                Type = column.Type,
                Nullable = column.Nullable,
                IsPrimaryKey = column.IsPrimaryKey,
                IsIdentity = column.IsIdentity,
            }).ToArray(),
            QualifiedMappings = schema.QualifiedMappings is null
                ? null
                : new Dictionary<string, int>(schema.QualifiedMappings, StringComparer.OrdinalIgnoreCase),
            NextRowId = schema.NextRowId,
        };
    }

    private static IRecordSerializer GetReadSerializer(IRecordSerializer recordSerializer, TableSchema schema)
    {
        if (!IsCollectionBackingSchema(schema))
            return recordSerializer;

        return recordSerializer is DefaultRecordSerializer
            ? new CollectionAwareRecordSerializer(recordSerializer)
            : new CollectionAwareRecordSerializer(recordSerializer);
    }

    private static bool IsCollectionBackingSchema(TableSchema schema)
    {
        return schema.TableName.StartsWith(CollectionPrefix, StringComparison.Ordinal) &&
               schema.Columns.Count == 2 &&
               string.Equals(schema.Columns[0].Name, "_key", StringComparison.Ordinal) &&
               schema.Columns[0].Type == DbType.Text &&
               string.Equals(schema.Columns[1].Name, "_doc", StringComparison.Ordinal) &&
               schema.Columns[1].Type == DbType.Text;
    }

    private static async ValueTask<StorageEngineContext> OpenStorageContextAsync(string databasePath, CancellationToken ct)
    {
        var factory = new DefaultStorageEngineFactory();
        return await factory.OpenAsync(databasePath, new StorageEngineOptions(), ct);
    }

    private static IEnumerable<string> GetTablesToCopy(SchemaCatalog catalog)
    {
        return catalog.GetTableNames()
            .Concat([ProcedureTableName, SavedQueryTableName])
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase);
    }

    private static async ValueTask<long> CopyTableRowsAsync(
        StorageEngineContext source,
        StorageEngineContext destination,
        string tableName,
        CancellationToken ct)
    {
        var sourceTree = source.Catalog.GetTableTree(tableName);
        var destinationTree = destination.Catalog.GetTableTree(tableName);
        var cursor = sourceTree.CreateCursor();
        long count = 0;
        while (await cursor.MoveNextAsync(ct))
        {
            await destinationTree.InsertAsync(cursor.CurrentKey, cursor.CurrentValue, ct);
            count++;
        }

        return count;
    }

    private static async ValueTask<long> CopyMetadataTableRowsAsync(
        StorageEngineContext source,
        StorageEngineContext destination,
        TableSchema schema,
        CancellationToken ct)
    {
        var sourceTree = source.Catalog.GetTableTree(schema.TableName);
        var destinationTree = destination.Catalog.GetTableTree(schema.TableName);
        var cursor = sourceTree.CreateCursor();
        long count = 0;

        while (await cursor.MoveNextAsync(ct))
        {
            var values = source.RecordSerializer.Decode(cursor.CurrentValue.Span);
            byte[] payload = destination.RecordSerializer.Encode(values);
            await destinationTree.InsertAsync(cursor.CurrentKey, payload, ct);
            count++;
        }

        return count;
    }

    private static bool IsClientMetadataTable(string tableName)
    {
        return string.Equals(tableName, ProcedureTableName, StringComparison.OrdinalIgnoreCase) ||
               string.Equals(tableName, SavedQueryTableName, StringComparison.OrdinalIgnoreCase);
    }

    private static void TryDeleteFile(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch
        {
            // Best-effort cleanup for temporary maintenance files.
        }
    }
}
