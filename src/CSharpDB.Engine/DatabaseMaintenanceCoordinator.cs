using System.Text;
using System.Text.Json;
using CSharpDB.Core;
using CSharpDB.Execution;
using CSharpDB.Storage.Diagnostics;
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
    private const string ProcedureEnabledIndexName = "idx___procedures_is_enabled";
    private const string SavedQueryNameIndexName = "idx___saved_queries_name";

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

            await context.Pager.BeginTransactionAsync(ct);
            try
            {
                foreach (var indexSchema in indexes)
                {
                    affectedTables.Add(indexSchema.TableName);

                    await context.Catalog.DropIndexAsync(indexSchema.IndexName, ct);
                    await context.Catalog.CreateIndexAsync(indexSchema, ct);
                    await BackfillIndexAsync(context, indexSchema, ct);
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
    {
        string fullPath = Path.GetFullPath(databasePath);
        string tempPath = fullPath + $".vacuum.{Guid.NewGuid():N}.tmp";
        string backupPath = fullPath + $".vacuumbak.{Guid.NewGuid():N}.tmp";

        StorageEngineContext? source = null;
        StorageEngineContext? destination = null;
        DatabaseInspectReport? beforeReport = null;
        ClientMetadataSnapshot metadataSnapshot = await ReadClientMetadataSnapshotAsync(fullPath, ct);

        try
        {
            beforeReport = await DatabaseInspector.InspectAsync(fullPath, new DatabaseInspectOptions(), ct);
            source = await OpenStorageContextAsync(fullPath, ct);
            destination = await OpenStorageContextAsync(tempPath, ct);

            await destination.Pager.BeginTransactionAsync(ct);
            try
            {
                await CopyDatabaseAsync(source, destination, ct);
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

            await RestoreClientMetadataAsync(tempPath, metadataSnapshot, ct);

            await source.Pager.DisposeAsync();
            source = null;

            string sourceWalPath = fullPath + ".wal";
            if (File.Exists(sourceWalPath))
                File.Delete(sourceWalPath);

            File.Move(fullPath, backupPath, overwrite: true);
            try
            {
                File.Move(tempPath, fullPath, overwrite: true);
            }
            catch
            {
                if (File.Exists(backupPath) && !File.Exists(fullPath))
                    File.Move(backupPath, fullPath, overwrite: true);
                throw;
            }

            if (File.Exists(backupPath))
                File.Delete(backupPath);

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
            TryDeleteFile(backupPath);
            TryDeleteFile(backupPath + ".wal");
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
            if (IsClientMetadataTable(tableName))
                await CopyMetadataTableRowsAsync(source, destination, schema, ct);
            else
                await CopyTableRowsAsync(source, destination, tableName, ct);

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

    private static async ValueTask BackfillCollectionIndexAsync(
        StorageEngineContext context,
        IndexSchema indexSchema,
        CancellationToken ct)
    {
        string fieldPath = indexSchema.Columns[0];
        string jsonPropertyName = JsonNamingPolicy.CamelCase.ConvertName(fieldPath);
        var tableTree = context.Catalog.GetTableTree(indexSchema.TableName);
        var indexStore = context.Catalog.GetIndexStore(indexSchema.IndexName);
        var cursor = tableTree.CreateCursor();

        while (await cursor.MoveNextAsync(ct))
        {
            if (!TryBuildCollectionIndexKey(cursor.CurrentValue.Span, jsonPropertyName, context.RecordSerializer, out long indexKey))
                continue;

            await IndexMaintenanceHelper.InsertRowIdAsync(indexStore, indexKey, cursor.CurrentKey, ct);
        }
    }

    private static bool TryBuildCollectionIndexKey(
        ReadOnlySpan<byte> payload,
        string jsonPropertyName,
        IRecordSerializer recordSerializer,
        out long indexKey)
    {
        if (TryBuildCollectionIndexKeyFromDirectPayload(payload, jsonPropertyName, out indexKey))
            return true;

        try
        {
            string json = recordSerializer.DecodeColumn(payload, 1).AsText;
            using var document = JsonDocument.Parse(json);
            return TryBuildCollectionIndexKeyFromJson(document.RootElement, jsonPropertyName, out indexKey);
        }
        catch
        {
            indexKey = 0;
            return false;
        }
    }

    private static bool TryBuildCollectionIndexKeyFromDirectPayload(
        ReadOnlySpan<byte> payload,
        string jsonPropertyName,
        out long indexKey)
    {
        if (!CollectionIndexedFieldReader.TryReadValue(payload, jsonPropertyName, out var value))
        {
            indexKey = 0;
            return false;
        }

        return TryBuildCollectionIndexKeyFromValue(value, out indexKey);
    }

    private static bool TryBuildCollectionIndexKeyFromJson(
        JsonElement document,
        string jsonPropertyName,
        out long indexKey)
    {
        indexKey = 0;
        if (document.ValueKind != JsonValueKind.Object)
            return false;

        if (!TryGetJsonProperty(document, jsonPropertyName, out JsonElement property))
            return false;

        return property.ValueKind switch
        {
            JsonValueKind.String => TryBuildCollectionIndexKeyFromValue(DbValue.FromText(property.GetString()!), out indexKey),
            JsonValueKind.Number when property.TryGetInt64(out long integerValue) => TryBuildCollectionIndexKeyFromValue(DbValue.FromInteger(integerValue), out indexKey),
            _ => false,
        };
    }

    private static bool TryBuildCollectionIndexKeyFromValue(DbValue value, out long indexKey)
    {
        if (value.IsNull || value.Type is not (DbType.Integer or DbType.Text))
        {
            indexKey = 0;
            return false;
        }

        indexKey = IndexMaintenanceHelper.ComputeIndexKey([value]);
        return true;
    }

    private static bool TryGetJsonProperty(JsonElement document, string propertyName, out JsonElement property)
    {
        if (document.TryGetProperty(propertyName, out property))
            return true;

        foreach (JsonProperty candidate in document.EnumerateObject())
        {
            if (string.Equals(candidate.Name, propertyName, StringComparison.OrdinalIgnoreCase))
            {
                property = candidate.Value;
                return true;
            }
        }

        property = default;
        return false;
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

    private static async ValueTask CopyTableRowsAsync(
        StorageEngineContext source,
        StorageEngineContext destination,
        string tableName,
        CancellationToken ct)
    {
        var sourceTree = source.Catalog.GetTableTree(tableName);
        var destinationTree = destination.Catalog.GetTableTree(tableName);
        var cursor = sourceTree.CreateCursor();
        while (await cursor.MoveNextAsync(ct))
            await destinationTree.InsertAsync(cursor.CurrentKey, cursor.CurrentValue, ct);
    }

    private static async ValueTask CopyMetadataTableRowsAsync(
        StorageEngineContext source,
        StorageEngineContext destination,
        TableSchema schema,
        CancellationToken ct)
    {
        var sourceTree = source.Catalog.GetTableTree(schema.TableName);
        var destinationTree = destination.Catalog.GetTableTree(schema.TableName);
        var cursor = sourceTree.CreateCursor();

        while (await cursor.MoveNextAsync(ct))
        {
            var values = source.RecordSerializer.Decode(cursor.CurrentValue.Span);
            byte[] payload = destination.RecordSerializer.Encode(values);
            await destinationTree.InsertAsync(cursor.CurrentKey, payload, ct);
        }
    }

    private static bool IsClientMetadataTable(string tableName)
    {
        return string.Equals(tableName, ProcedureTableName, StringComparison.OrdinalIgnoreCase) ||
               string.Equals(tableName, SavedQueryTableName, StringComparison.OrdinalIgnoreCase);
    }

    private static async ValueTask<ClientMetadataSnapshot> ReadClientMetadataSnapshotAsync(string databasePath, CancellationToken ct)
    {
        await using var db = await Database.OpenAsync(databasePath, ct);
        return new ClientMetadataSnapshot(
            await ReadSavedQueriesAsync(db, ct),
            await ReadProceduresAsync(db, ct));
    }

    private static async ValueTask<IReadOnlyList<SavedQueryRow>> ReadSavedQueriesAsync(Database db, CancellationToken ct)
    {
        try
        {
            await using var result = await db.ExecuteAsync(
                $"SELECT id, name, sql_text, created_utc, updated_utc FROM {SavedQueryTableName} ORDER BY id;",
                ct);
            var rows = await result.ToListAsync(ct);
            return rows.Select(row => new SavedQueryRow(
                row[0].AsInteger,
                row[1].AsText,
                row[2].AsText,
                row[3].AsText,
                row[4].AsText)).ToArray();
        }
        catch (CSharpDbException ex) when (ex.Code == ErrorCode.TableNotFound)
        {
            return [];
        }
    }

    private static async ValueTask<IReadOnlyList<ProcedureRow>> ReadProceduresAsync(Database db, CancellationToken ct)
    {
        try
        {
            await using var result = await db.ExecuteAsync(
                $"SELECT name, body_sql, params_json, description, is_enabled, created_utc, updated_utc FROM {ProcedureTableName} ORDER BY name;",
                ct);
            var rows = await result.ToListAsync(ct);
            return rows.Select(row => new ProcedureRow(
                row[0].AsText,
                row[1].AsText,
                row[2].AsText,
                row[3].IsNull ? null : row[3].AsText,
                !row[4].IsNull && row[4].AsInteger != 0,
                row[5].AsText,
                row[6].AsText)).ToArray();
        }
        catch (CSharpDbException ex) when (ex.Code == ErrorCode.TableNotFound)
        {
            return [];
        }
    }

    private static async ValueTask RestoreClientMetadataAsync(
        string databasePath,
        ClientMetadataSnapshot snapshot,
        CancellationToken ct)
    {
        if (snapshot.SavedQueries.Count == 0 && snapshot.Procedures.Count == 0)
            return;

        await using var db = await Database.OpenAsync(databasePath, ct);
        await db.BeginTransactionAsync(ct);
        try
        {
            await EnsureClientMetadataTablesAsync(db, ct);
            await ExecuteStatementAsync(db, $"DELETE FROM {SavedQueryTableName};", ct);
            await ExecuteStatementAsync(db, $"DELETE FROM {ProcedureTableName};", ct);

            foreach (var savedQuery in snapshot.SavedQueries)
            {
                await ExecuteStatementAsync(
                    db,
                    $"""
                    INSERT INTO {SavedQueryTableName} (id, name, sql_text, created_utc, updated_utc)
                    VALUES ({savedQuery.Id}, {FormatSqlLiteral(savedQuery.Name)}, {FormatSqlLiteral(savedQuery.SqlText)}, {FormatSqlLiteral(savedQuery.CreatedUtc)}, {FormatSqlLiteral(savedQuery.UpdatedUtc)});
                    """,
                    ct);
            }

            foreach (var procedure in snapshot.Procedures)
            {
                string descriptionLiteral = procedure.Description is null ? "NULL" : FormatSqlLiteral(procedure.Description);
                await ExecuteStatementAsync(
                    db,
                    $"""
                    INSERT INTO {ProcedureTableName} (name, body_sql, params_json, description, is_enabled, created_utc, updated_utc)
                    VALUES ({FormatSqlLiteral(procedure.Name)}, {FormatSqlLiteral(procedure.BodySql)}, {FormatSqlLiteral(procedure.ParamsJson)}, {descriptionLiteral}, {(procedure.IsEnabled ? 1 : 0)}, {FormatSqlLiteral(procedure.CreatedUtc)}, {FormatSqlLiteral(procedure.UpdatedUtc)});
                    """,
                    ct);
            }

            await db.CommitAsync(ct);
        }
        catch
        {
            await db.RollbackAsync(ct);
            throw;
        }
    }

    private static async ValueTask EnsureClientMetadataTablesAsync(Database db, CancellationToken ct)
    {
        await ExecuteStatementAsync(
            db,
            $"""
            CREATE TABLE IF NOT EXISTS {ProcedureTableName} (
                name TEXT PRIMARY KEY,
                body_sql TEXT NOT NULL,
                params_json TEXT NOT NULL,
                description TEXT,
                is_enabled INTEGER NOT NULL,
                created_utc TEXT NOT NULL,
                updated_utc TEXT NOT NULL
            );
            """,
            ct);

        await ExecuteStatementAsync(
            db,
            $"""
            CREATE INDEX IF NOT EXISTS {ProcedureEnabledIndexName}
            ON {ProcedureTableName} (is_enabled);
            """,
            ct);

        await ExecuteStatementAsync(
            db,
            $"""
            CREATE TABLE IF NOT EXISTS {SavedQueryTableName} (
                id INTEGER PRIMARY KEY IDENTITY,
                name TEXT NOT NULL,
                sql_text TEXT NOT NULL,
                created_utc TEXT NOT NULL,
                updated_utc TEXT NOT NULL
            );
            """,
            ct);

        await ExecuteStatementAsync(
            db,
            $"""
            CREATE UNIQUE INDEX IF NOT EXISTS {SavedQueryNameIndexName}
            ON {SavedQueryTableName} (name);
            """,
            ct);
    }

    private static string FormatSqlLiteral(string value)
    {
        return $"'{value.Replace("'", "''", StringComparison.Ordinal)}'";
    }

    private static async ValueTask ExecuteStatementAsync(Database db, string sql, CancellationToken ct)
    {
        await using var result = await db.ExecuteAsync(sql, ct);
        if (result.IsQuery)
            _ = await result.ToListAsync(ct);
    }

    private sealed record ClientMetadataSnapshot(
        IReadOnlyList<SavedQueryRow> SavedQueries,
        IReadOnlyList<ProcedureRow> Procedures);

    private sealed record SavedQueryRow(
        long Id,
        string Name,
        string SqlText,
        string CreatedUtc,
        string UpdatedUtc);

    private sealed record ProcedureRow(
        string Name,
        string BodySql,
        string ParamsJson,
        string? Description,
        bool IsEnabled,
        string CreatedUtc,
        string UpdatedUtc);

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
