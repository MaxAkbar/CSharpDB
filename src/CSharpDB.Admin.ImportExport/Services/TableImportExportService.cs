using System.Collections;
using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Admin.ImportExport.Contracts;
using CSharpDB.Client;
using CSharpDB.ImportExport.Models;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Migration;
using CSharpDB.Migration.Canonicalization;
using CSharpDB.Migration.Validation;
using TableArchiveExportProgress = CSharpDB.Client.Models.TableArchiveExportProgress;
using ClientCheckConstraintDefinition = CSharpDB.Client.Models.CheckConstraintDefinition;
using ClientColumnDefinition = CSharpDB.Client.Models.ColumnDefinition;
using ClientDbType = CSharpDB.Client.Models.DbType;
using ClientForeignKeyDefinition = CSharpDB.Client.Models.ForeignKeyDefinition;
using ClientForeignKeyOnDeleteAction = CSharpDB.Client.Models.ForeignKeyOnDeleteAction;
using ClientIndexSchema = CSharpDB.Client.Models.IndexSchema;
using ClientKeyConstraintDefinition = CSharpDB.Client.Models.KeyConstraintDefinition;
using ClientKeyConstraintKind = CSharpDB.Client.Models.KeyConstraintKind;
using ClientTableSchema = CSharpDB.Client.Models.TableSchema;
using PrimitiveCheckConstraintDefinition = CSharpDB.Primitives.CheckConstraintDefinition;
using PrimitiveColumnDefinition = CSharpDB.Primitives.ColumnDefinition;
using PrimitiveDbType = CSharpDB.Primitives.DbType;
using PrimitiveDbValue = CSharpDB.Primitives.DbValue;
using PrimitiveForeignKeyDefinition = CSharpDB.Primitives.ForeignKeyDefinition;
using PrimitiveForeignKeyOnDeleteAction = CSharpDB.Primitives.ForeignKeyOnDeleteAction;
using PrimitiveIndexSchema = CSharpDB.Primitives.IndexSchema;
using PrimitiveKeyConstraintDefinition = CSharpDB.Primitives.KeyConstraintDefinition;
using PrimitiveKeyConstraintKind = CSharpDB.Primitives.KeyConstraintKind;
using PrimitiveTableSchema = CSharpDB.Primitives.TableSchema;
using SqlIdentifierRules = CSharpDB.Primitives.SqlIdentifierRules;

namespace CSharpDB.Admin.ImportExport.Services;

public sealed class TableImportExportService(
    ICSharpDbClient client,
    ITableArchiveDownloadStore downloads,
    TableArchiveRestoreOptions? restoreOptions = null) : ITableImportExportService
{
    private const int ExportPageSize = 1_000;
    private const int RestoreInsertBatchSize = 100;
    private const string RestoreJournalTableName = "__csharpdb_restore_journal_v1";
    private const string RestoreJournalContractConstraintName = "__csharpdb_restore_journal_contract_v1";
    private const string RestoreReceiptTableName = "__csharpdb_restore_receipts_v1";
    private const string RestoreReceiptContractConstraintName = "__csharpdb_restore_receipt_contract_v1";
    private const string RestoreStagingTablePrefix = "__csharpdb_restore_stage_v1_";
    private const string RestoreOwnerConstraintPrefix = "__csharpdb_restore_owner_v1_";
    private const string RestoreContractCheckExpression = "(1 = 1)";
    private static readonly TimeSpan RestoreLeaseTimeout = TimeSpan.FromMinutes(30);
    private readonly TableArchiveRestoreOptions _restoreOptions = ValidateRestoreOptions(
        restoreOptions ?? new TableArchiveRestoreOptions());

    public Task<string> GetDefaultServerExportPathAsync(string tableName, CancellationToken ct = default)
    {
        string databaseFolder = ResolveDatabaseFolder(client.DataSource);
        string fileName = $"{SanitizeFileName(tableName)}-{DateTime.Now:yyyyMMdd-HHmmss}.csdbtable";
        return Task.FromResult(Path.Combine(databaseFolder, "exports", fileName));
    }

    public async Task<IReadOnlyList<ExternalTableRegistrationInfo>> GetExternalTablesAsync(CancellationToken ct = default)
    {
        var result = await client.ExecuteSqlAsync(
            """
            SELECT table_name, path, source_table_name, row_count, created_utc
            FROM sys.external_tables
            ORDER BY table_name;
            """,
            ct);

        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new InvalidOperationException(result.Error);

        if (result.Rows is not { Count: > 0 })
            return Array.Empty<ExternalTableRegistrationInfo>();

        return result.Rows
            .Select(MapExternalTableRegistration)
            .ToArray();
    }

    public async Task<TableExportResult> ExportTableAsync(
        TableExportRequest request,
        IProgress<TableExportProgress>? progress = null,
        CancellationToken ct = default)
    {
        string tableName = RequireIdentifier(request.TableName, nameof(request.TableName));
        string path = request.Destination == TableExportDestination.Download
            ? CreateTemporaryArchivePath(tableName)
            : string.IsNullOrWhiteSpace(request.ServerPath)
                ? await GetDefaultServerExportPathAsync(tableName, ct)
                : request.ServerPath;

        ReportExportProgress(
            progress,
            tableName,
            "Preparing",
            "Preparing export target",
            rowsProcessed: 0,
            totalRows: null,
            path);

        long rowCount;
        if (client is ICSharpDbTableArchiveProgressExporter progressExporter && progressExporter.SupportsTableArchiveExport)
        {
            var archiveProgress = progress is null
                ? null
                : new Progress<TableArchiveExportProgress>(p => ReportExportProgress(
                    progress,
                    p.TableName,
                    p.Stage,
                    p.Message ?? "Writing table archive",
                    p.RowsExported,
                    p.TotalRows,
                    p.Path ?? path));
            var archiveExport = await progressExporter.ExportTableArchiveAsync(tableName, path, archiveProgress, ct);
            rowCount = archiveExport.RowCount;
        }
        else if (client is ICSharpDbTableArchiveExporter exporter && exporter.SupportsTableArchiveExport)
        {
            ReportExportProgress(
                progress,
                tableName,
                "Exporting",
                "Writing table archive",
                rowsProcessed: 0,
                totalRows: null,
                path);
            var archiveExport = await exporter.ExportTableArchiveAsync(tableName, path, ct);
            rowCount = archiveExport.RowCount;
        }
        else
        {
            ClientTableSchema clientSchema = await client.GetTableSchemaAsync(tableName, ct)
                ?? throw new InvalidOperationException($"Table '{tableName}' was not found.");
            PrimitiveTableSchema schema = MapSchema(clientSchema);
            PrimitiveIndexSchema[] secondaryIndexes = (await client.GetIndexesAsync(ct))
                .Where(index => string.Equals(index.TableName, tableName, StringComparison.OrdinalIgnoreCase))
                .Select(MapIndex)
                .ToArray();
            var manifest = await TableArchiveWriter.WriteAsync(
                path,
                schema,
                secondaryIndexes,
                EnumerateRowsAsync(clientSchema, path, progress, ct),
                ct);
            rowCount = manifest.RowCount;
        }

        string fileName = Path.GetFileName(path);
        string? downloadUrl = null;

        if (request.Destination == TableExportDestination.Download)
        {
            ReportExportProgress(
                progress,
                tableName,
                "Preparing download",
                "Preparing one-time download link",
                rowCount,
                rowCount,
                path);
            var download = downloads.Add(path, fileName);
            downloadUrl = $"/admin/import-export/download/{download.Token}";
        }

        ReportExportProgress(
            progress,
            tableName,
            "Complete",
            "Export complete",
            rowCount,
            rowCount,
            path);

        return new TableExportResult
        {
            TableName = tableName,
            FileName = fileName,
            Path = path,
            RowCount = rowCount,
            DownloadUrl = downloadUrl,
            IsDownload = request.Destination == TableExportDestination.Download,
        };
    }

    public Task RegisterExternalTableAsync(ExternalTableRegistrationRequest request, CancellationToken ct = default) =>
        RegisterExternalTableAsync(request, progress: null, ct);

    public async Task RegisterExternalTableAsync(
        ExternalTableRegistrationRequest request,
        IProgress<TableExportProgress>? progress,
        CancellationToken ct = default)
    {
        string tableName = RequireIdentifier(request.TableName, nameof(request.TableName));
        if (string.IsNullOrWhiteSpace(request.ArchivePath))
            throw new ArgumentException("Archive path is required.", nameof(request.ArchivePath));

        ReportExportProgress(
            progress,
            tableName,
            "Validating",
            "Reading archive manifest",
            rowsProcessed: 0,
            totalRows: 3,
            request.ArchivePath);
        await TableArchiveReader.ReadManifestAsync(ResolveArchivePath(request.ArchivePath), ct);

        if (request.ReplaceExisting)
        {
            ReportExportProgress(
                progress,
                tableName,
                "Replacing",
                "Dropping existing registration if present",
                rowsProcessed: 1,
                totalRows: 3,
                request.ArchivePath);
            await DropExternalTableAsync(tableName, ct);
        }

        ReportExportProgress(
            progress,
            tableName,
            "Registering",
            "Writing external table registration",
            rowsProcessed: 2,
            totalRows: 3,
            request.ArchivePath);
        string sql = $"CREATE EXTERNAL TABLE {tableName} FROM {FormatStringLiteral(request.ArchivePath)};";
        await ExecuteCheckedAsync(sql, ct);

        ReportExportProgress(
            progress,
            tableName,
            "Complete",
            "External table registered",
            rowsProcessed: 3,
            totalRows: 3,
            request.ArchivePath);
    }

    public async Task DropExternalTableAsync(string tableName, CancellationToken ct = default)
    {
        string normalizedTableName = RequireIdentifier(tableName, nameof(tableName));
        string sql = $"DROP EXTERNAL TABLE IF EXISTS {normalizedTableName};";
        await ExecuteCheckedAsync(sql, ct);
    }

    public async Task<RestoreTableResult> RestoreTableAsync(RestoreTableRequest request, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(request.ArchivePath))
            throw new ArgumentException("Archive path is required.", nameof(request.ArchivePath));

        if (client is not ICSharpDbTransactionalSnapshotReader
            {
                SupportsTransactionalSnapshotReads: true,
            } transactionalReader)
        {
            throw new NotSupportedException(
                "Safe table archive restore requires a direct CSharpDB transport with transactional snapshot reads.");
        }

        string sourceArchivePath = ResolveArchivePath(request.ArchivePath);
        await using ArchiveRestoreSnapshot archive = await ArchiveRestoreSnapshot.CreateAsync(
            sourceArchivePath,
            _restoreOptions.ScratchDirectory!,
            _restoreOptions.MaxArchiveSnapshotBytes,
            ct);
        return await RestoreTableSnapshotAsync(request, archive, transactionalReader, ct);
    }

    private async Task<RestoreTableResult> RestoreTableSnapshotAsync(
        RestoreTableRequest request,
        ArchiveRestoreSnapshot archive,
        ICSharpDbTransactionalSnapshotReader transactionalReader,
        CancellationToken ct)
    {
        (TableArchiveSchema archivedSchema, TableArchiveManifest manifest) =
            await TableArchiveReader.ReadMetadataAsync(archive.Stream, ct);
        bool regeneratesRowVersionTokens = archivedSchema.Columns.Any(static column => column.IsRowVersion);

        string targetTableName = string.IsNullOrWhiteSpace(request.TargetTableName)
            ? RequireArchiveIdentifier(archivedSchema.TableName, "Archived table name")
            : RequireArchiveIdentifier(request.TargetTableName.Trim(), nameof(request.TargetTableName));

        if (IsReservedRestoreTableName(targetTableName))
        {
            throw new InvalidOperationException(
                $"Table name '{targetTableName}' is reserved by the archive restore staging contract.");
        }

        if (await client.GetTableSchemaAsync(targetTableName, ct) is not null)
            throw new InvalidOperationException($"Table '{targetTableName}' already exists.");

        string targetKey = ComputeTargetKey(targetTableName);
        string archiveToken = ComputeArchiveToken(archive.Digest, targetTableName);
        string stagingTableName = RestoreStagingTablePrefix + targetKey;
        PrimitiveTableSchema restoreSchema = archivedSchema.ToTableSchema(stagingTableName);
        IReadOnlyList<PrimitiveIndexSchema> secondaryIndexes = archivedSchema.ToSecondaryIndexes(stagingTableName);
        await EnsureRestoreJournalAsync(ct);
        await EnsureRestoreReceiptTableAsync(ct);
        await RecoverAbandonedRestoreAsync(
            targetKey,
            targetTableName,
            stagingTableName,
            archiveToken,
            restoreSchema,
            secondaryIndexes,
            ct);

        string ownerToken = Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
        string ownerConstraintName = RestoreOwnerConstraintPrefix + ownerToken;
        await ClaimRestoreAsync(
            targetKey,
            stagingTableName,
            targetTableName,
            archiveToken,
            ownerToken,
            ct);

        bool activated = false;
        bool stagingCreated = false;
        long inserted = 0;
        try
        {
            await using var loadHeartbeat = new RestoreLeaseHeartbeat(
                heartbeatCt => RefreshRestoreLeaseAsync(targetKey, ownerToken, heartbeatCt),
                ct);
            try
            {
                await ExecuteCheckedAsync(
                    BuildCreateTableSql(restoreSchema, ownerConstraintName),
                    loadHeartbeat.Token);
                stagingCreated = true;
                await RefreshRestoreLeaseAsync(targetKey, ownerToken, loadHeartbeat.Token);

                var batch = new List<PrimitiveDbValue[]>(RestoreInsertBatchSize);
                await foreach (PrimitiveDbValue[] row in TableArchiveReader.ReadRowsAsync(
                                   archive.Stream,
                                   loadHeartbeat.Token))
                {
                    batch.Add(row);
                    if (batch.Count >= RestoreInsertBatchSize)
                    {
                        inserted += await InsertBatchAsync(
                            stagingTableName,
                            restoreSchema.Columns,
                            batch,
                            loadHeartbeat.Token);
                        batch.Clear();
                        await RefreshRestoreLeaseAsync(targetKey, ownerToken, loadHeartbeat.Token);
                    }
                }

                if (batch.Count > 0)
                {
                    inserted += await InsertBatchAsync(
                        stagingTableName,
                        restoreSchema.Columns,
                        batch,
                        loadHeartbeat.Token);
                    await RefreshRestoreLeaseAsync(targetKey, ownerToken, loadHeartbeat.Token);
                }

                foreach (PrimitiveForeignKeyDefinition foreignKey in restoreSchema.ForeignKeys)
                {
                    await ExecuteCheckedAsync(
                        BuildAddForeignKeySql(stagingTableName, foreignKey),
                        loadHeartbeat.Token);
                    await RefreshRestoreLeaseAsync(targetKey, ownerToken, loadHeartbeat.Token);
                }

                foreach (PrimitiveIndexSchema index in secondaryIndexes)
                {
                    await ExecuteCheckedAsync(BuildCreateIndexSql(index), loadHeartbeat.Token);
                    await RefreshRestoreLeaseAsync(targetKey, ownerToken, loadHeartbeat.Token);
                }

                if (archivedSchema.NextRowId > 0)
                {
                    await ExecuteCheckedAsync(
                        $"ALTER TABLE {QuoteIdentifier(stagingTableName)} RESEED {archivedSchema.NextRowId.ToString(CultureInfo.InvariantCulture)};",
                        loadHeartbeat.Token);
                    await RefreshRestoreLeaseAsync(targetKey, ownerToken, loadHeartbeat.Token);
                }

                long restoredCount = await ExecuteScalarInt64Async(
                    $"SELECT COUNT(*) FROM {QuoteIdentifier(stagingTableName)};",
                    loadHeartbeat.Token);
                if (inserted != manifest.RowCount || restoredCount != manifest.RowCount)
                {
                    throw new InvalidDataException(
                        $"Archive restore count mismatch: expected {manifest.RowCount}, inserted {inserted}, found {restoredCount}.");
                }

                await ValidateRestoredSchemaAsync(
                    restoreSchema,
                    secondaryIndexes,
                    ownerConstraintName,
                    loadHeartbeat.Token);
                await RefreshRestoreLeaseAsync(targetKey, ownerToken, loadHeartbeat.Token);
            }
            catch (OperationCanceledException) when (loadHeartbeat.HasFailed)
            {
                loadHeartbeat.ThrowIfFailed();
                throw;
            }

            await loadHeartbeat.StopAsync();
            await ValidateRowsAndActivateRestoreAsync(
                archive,
                restoreSchema,
                secondaryIndexes,
                manifest.RowCount,
                archiveToken,
                targetKey,
                ownerToken,
                ownerConstraintName,
                stagingTableName,
                targetTableName,
                transactionalReader,
                ct);
            activated = true;
        }
        catch
        {
            if (!activated)
            {
                await TryReleaseRestoreAsync(
                    targetKey,
                    ownerToken,
                    ownerConstraintName,
                    stagingTableName,
                    restoreSchema,
                    secondaryIndexes,
                    stagingCreated);
            }
            throw;
        }

        return new RestoreTableResult
        {
            TableName = targetTableName,
            RowsInserted = inserted,
            RowVersionTokensRegenerated = regeneratesRowVersionTokens,
        };
    }

    private async IAsyncEnumerable<PrimitiveDbValue[]> EnumerateRowsAsync(
        ClientTableSchema schema,
        string path,
        IProgress<TableExportProgress>? progress,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        int page = 1;
        int totalRows = int.MaxValue;
        int seen = 0;
        var interval = Stopwatch.StartNew();

        while (seen < totalRows)
        {
            ct.ThrowIfCancellationRequested();
            var result = await client.BrowseTableAsync(schema.TableName, page, ExportPageSize, ct);
            totalRows = result.TotalRows;
            if (result.Rows.Count == 0)
                yield break;

            foreach (object?[] row in result.Rows)
            {
                ct.ThrowIfCancellationRequested();
                yield return MapRow(schema, row);
                seen++;
            }

            if (seen >= totalRows || interval.ElapsedMilliseconds >= 500)
            {
                ReportExportProgress(
                    progress,
                    schema.TableName,
                    "Exporting",
                    "Writing table archive",
                    seen,
                    totalRows,
                    path);
                interval.Restart();
                await Task.Yield();
            }

            page++;
        }

        ReportExportProgress(
            progress,
            schema.TableName,
            "Exporting",
            "Writing table archive",
            seen,
            totalRows == int.MaxValue ? null : totalRows,
            path);
    }

    private static void ReportExportProgress(
        IProgress<TableExportProgress>? progress,
        string tableName,
        string stage,
        string message,
        long rowsProcessed,
        long? totalRows,
        string? path)
    {
        progress?.Report(new TableExportProgress
        {
            Operation = "Export table",
            Stage = stage,
            Message = message,
            TableName = tableName,
            Path = path,
            RowsProcessed = rowsProcessed,
            TotalRows = totalRows,
        });
    }

    private async Task<long> InsertBatchAsync(
        string tableName,
        IReadOnlyList<PrimitiveColumnDefinition> columns,
        IReadOnlyList<PrimitiveDbValue[]> rows,
        CancellationToken ct)
    {
        if (rows.Count == 0)
            return 0;

        for (int rowIndex = 0; rowIndex < rows.Count; rowIndex++)
            ValidateRestoreRow(columns, rows[rowIndex], rowIndex);

        int[] insertColumnIndexes = columns
            .Select(static (column, index) => (column, index))
            .Where(static pair => !pair.column.IsRowVersion)
            .Select(static pair => pair.index)
            .ToArray();
        if (insertColumnIndexes.Length == 0)
        {
            long inserted = 0;
            foreach (PrimitiveDbValue[] _ in rows)
            {
                var insertResult = await client.ExecuteSqlAsync(
                    $"INSERT INTO {QuoteIdentifier(tableName)} DEFAULT VALUES;",
                    ct);
                if (!string.IsNullOrWhiteSpace(insertResult.Error))
                    throw new InvalidOperationException(insertResult.Error);
                inserted += insertResult.RowsAffected;
            }

            return inserted;
        }

        var sql = new StringBuilder();
        sql.Append("INSERT INTO ").Append(QuoteIdentifier(tableName)).Append(" (");
        for (int i = 0; i < insertColumnIndexes.Length; i++)
        {
            if (i > 0)
                sql.Append(", ");
            sql.Append(QuoteIdentifier(columns[insertColumnIndexes[i]].Name));
        }

        sql.Append(") VALUES ");
        for (int rowIndex = 0; rowIndex < rows.Count; rowIndex++)
        {
            if (rowIndex > 0)
                sql.Append(", ");
            sql.Append('(');
            for (int insertIndex = 0; insertIndex < insertColumnIndexes.Length; insertIndex++)
            {
                if (insertIndex > 0)
                    sql.Append(", ");

                int columnIndex = insertColumnIndexes[insertIndex];
                PrimitiveDbValue value = rows[rowIndex][columnIndex];
                sql.Append(FormatLiteral(value, columns[columnIndex].Type));
            }

            sql.Append(')');
        }

        sql.Append(';');
        var result = await client.ExecuteSqlAsync(sql.ToString(), ct);
        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new InvalidOperationException(result.Error);

        return result.RowsAffected;
    }

    private static void ValidateRestoreRow(
        IReadOnlyList<PrimitiveColumnDefinition> columns,
        PrimitiveDbValue[] row,
        int rowIndex)
    {
        if (row.Length != columns.Count)
        {
            throw new InvalidDataException(
                $"Archived row {rowIndex} has {row.Length} values; expected {columns.Count}.");
        }

        for (int columnIndex = 0; columnIndex < row.Length; columnIndex++)
        {
            PrimitiveColumnDefinition column = columns[columnIndex];
            PrimitiveDbValue value = row[columnIndex];
            if (value.IsNull)
            {
                if (!column.Nullable || column.IsPrimaryKey || column.IsRowVersion)
                {
                    throw new InvalidDataException(
                        $"Archived row {rowIndex}, column '{column.Name}' cannot be NULL.");
                }
            }
            else if (value.Type != column.Type)
            {
                throw new InvalidDataException(
                    $"Archived row {rowIndex}, column '{column.Name}' has value tag {value.Type}; expected {column.Type}.");
            }
        }
    }

    private static PrimitiveTableSchema MapSchema(ClientTableSchema schema) => new()
    {
        TableName = schema.TableName,
        Columns = schema.Columns.Select(MapColumn).ToArray(),
        ForeignKeys = schema.ForeignKeys.Select(MapForeignKey).ToArray(),
        CheckConstraints = schema.CheckConstraints.Select(MapCheckConstraint).ToArray(),
        KeyConstraints = schema.KeyConstraints.Select(MapKeyConstraint).ToArray(),
        NextRowId = schema.NextRowId,
    };

    private static PrimitiveColumnDefinition MapColumn(ClientColumnDefinition column) => new()
    {
        Name = column.Name,
        Type = column.Type switch
        {
            ClientDbType.Integer => PrimitiveDbType.Integer,
            ClientDbType.Real => PrimitiveDbType.Real,
            ClientDbType.Text => PrimitiveDbType.Text,
            ClientDbType.Blob => PrimitiveDbType.Blob,
            _ => throw new InvalidOperationException($"Unsupported column type '{column.Type}'."),
        },
        Nullable = column.Nullable,
        IsPrimaryKey = column.IsPrimaryKey,
        IsIdentity = column.IsIdentity,
        IsRowVersion = column.IsRowVersion,
        Collation = column.Collation,
        DefaultSql = column.DefaultSql,
    };

    private static PrimitiveIndexSchema MapIndex(ClientIndexSchema index) => new()
    {
        IndexName = index.IndexName,
        TableName = index.TableName,
        Columns = index.Columns.ToArray(),
        ColumnCollations = index.ColumnCollations.ToArray(),
        IsUnique = index.IsUnique,
    };

    private static PrimitiveForeignKeyDefinition MapForeignKey(ClientForeignKeyDefinition foreignKey) => new()
    {
        ConstraintName = foreignKey.ConstraintName,
        ColumnName = foreignKey.ColumnName,
        ReferencedTableName = foreignKey.ReferencedTableName,
        ReferencedColumnName = foreignKey.ReferencedColumnName,
        ColumnNames = foreignKey.ColumnNames.Count > 0
            ? foreignKey.ColumnNames.ToArray()
            : [foreignKey.ColumnName],
        ReferencedColumnNames = foreignKey.ReferencedColumnNames.Count > 0
            ? foreignKey.ReferencedColumnNames.ToArray()
            : [foreignKey.ReferencedColumnName],
        OnDelete = foreignKey.OnDelete == ClientForeignKeyOnDeleteAction.Cascade
            ? PrimitiveForeignKeyOnDeleteAction.Cascade
            : PrimitiveForeignKeyOnDeleteAction.Restrict,
        SupportingIndexName = foreignKey.SupportingIndexName,
    };

    private static PrimitiveCheckConstraintDefinition MapCheckConstraint(
        ClientCheckConstraintDefinition check) => new()
    {
        ConstraintName = check.ConstraintName,
        ExpressionSql = check.ExpressionSql,
        ColumnName = check.ColumnName,
    };

    private static PrimitiveKeyConstraintDefinition MapKeyConstraint(
        ClientKeyConstraintDefinition key) => new()
    {
        ConstraintName = key.ConstraintName,
        Kind = key.Kind switch
        {
            ClientKeyConstraintKind.PrimaryKey => PrimitiveKeyConstraintKind.PrimaryKey,
            ClientKeyConstraintKind.Unique => PrimitiveKeyConstraintKind.Unique,
            _ => throw new InvalidOperationException($"Unsupported key constraint kind '{key.Kind}'."),
        },
        Columns = key.Columns.ToArray(),
        BackingIndexName = key.BackingIndexName,
    };

    private static PrimitiveDbValue[] MapRow(ClientTableSchema schema, object?[] row)
    {
        var values = new PrimitiveDbValue[schema.Columns.Count];
        for (int i = 0; i < values.Length; i++)
        {
            object? value = i < row.Length ? row[i] : null;
            values[i] = MapValue(schema.Columns[i].Type, value);
        }

        return values;
    }

    private static PrimitiveDbValue[] MapRow(
        IReadOnlyList<PrimitiveColumnDefinition> columns,
        object?[] row)
    {
        if (row.Length != columns.Count)
            throw new InvalidDataException("A restored validation row has an invalid field count.");

        var values = new PrimitiveDbValue[columns.Count];
        for (int i = 0; i < values.Length; i++)
            values[i] = MapValue(columns[i].Type, row[i]);
        return values;
    }

    private static PrimitiveDbValue MapValue(ClientDbType columnType, object? value)
    {
        PrimitiveDbType primitiveType = columnType switch
        {
            ClientDbType.Integer => PrimitiveDbType.Integer,
            ClientDbType.Real => PrimitiveDbType.Real,
            ClientDbType.Text => PrimitiveDbType.Text,
            ClientDbType.Blob => PrimitiveDbType.Blob,
            _ => throw new InvalidOperationException($"Unsupported column type '{columnType}'."),
        };
        return MapValue(primitiveType, value);
    }

    private static PrimitiveDbValue MapValue(PrimitiveDbType columnType, object? value)
    {
        if (value is null)
            return PrimitiveDbValue.Null;

        return columnType switch
        {
            PrimitiveDbType.Integer => PrimitiveDbValue.FromInteger(Convert.ToInt64(value, CultureInfo.InvariantCulture)),
            PrimitiveDbType.Real => PrimitiveDbValue.FromReal(Convert.ToDouble(value, CultureInfo.InvariantCulture)),
            PrimitiveDbType.Text => PrimitiveDbValue.FromText(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty),
            PrimitiveDbType.Blob => PrimitiveDbValue.FromBlob(ConvertToBytes(value)),
            _ => throw new InvalidOperationException($"Unsupported column type '{columnType}'."),
        };
    }

    private static byte[] ConvertToBytes(object value)
    {
        if (value is byte[] bytes)
            return bytes;

        if (value is IEnumerable<byte> byteEnumerable)
            return byteEnumerable.ToArray();

        if (value is string text)
            return Convert.FromBase64String(text);

        if (value is IEnumerable enumerable)
            return enumerable.Cast<object>().Select(item => Convert.ToByte(item, CultureInfo.InvariantCulture)).ToArray();

        throw new InvalidOperationException($"Cannot convert value of type '{value.GetType().Name}' to BLOB.");
    }

    private static ExternalTableRegistrationInfo MapExternalTableRegistration(object?[] row)
    {
        string createdText = row.Length > 4 ? Convert.ToString(row[4], CultureInfo.InvariantCulture) ?? string.Empty : string.Empty;
        DateTimeOffset? createdUtc = DateTimeOffset.TryParse(
            createdText,
            CultureInfo.InvariantCulture,
            DateTimeStyles.RoundtripKind,
            out var parsed)
                ? parsed
                : null;

        return new ExternalTableRegistrationInfo
        {
            TableName = row.Length > 0 ? Convert.ToString(row[0], CultureInfo.InvariantCulture) ?? string.Empty : string.Empty,
            Path = row.Length > 1 ? Convert.ToString(row[1], CultureInfo.InvariantCulture) ?? string.Empty : string.Empty,
            SourceTableName = row.Length > 2 ? Convert.ToString(row[2], CultureInfo.InvariantCulture) : null,
            RowCount = row.Length > 3 && row[3] is not null
                ? Convert.ToInt64(row[3], CultureInfo.InvariantCulture)
                : 0,
            CreatedUtc = createdUtc,
        };
    }

    private string ResolveArchivePath(string archivePath)
    {
        string trimmed = archivePath.Trim();
        if (Path.IsPathFullyQualified(trimmed))
            return trimmed;

        return Path.GetFullPath(Path.Combine(ResolveDatabaseFolder(client.DataSource), trimmed));
    }

    private static TableArchiveRestoreOptions ValidateRestoreOptions(
        TableArchiveRestoreOptions options)
    {
        if (options.MaxArchiveSnapshotBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(options.MaxArchiveSnapshotBytes));
        if (options.MaxValidationSpillBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(options.MaxValidationSpillBytes));

        string scratchDirectory = Path.TrimEndingDirectorySeparator(
            Path.GetFullPath(options.ScratchDirectory ?? Path.GetTempPath()));
        return options with { ScratchDirectory = scratchDirectory };
    }

    private static bool IsReservedRestoreTableName(string tableName) =>
        string.Equals(tableName, RestoreJournalTableName, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(tableName, RestoreReceiptTableName, StringComparison.OrdinalIgnoreCase) ||
        tableName.StartsWith(RestoreStagingTablePrefix, StringComparison.OrdinalIgnoreCase);

    private static string ComputeTargetKey(string targetTableName)
    {
        byte[] digest = SHA256.HashData(Encoding.UTF8.GetBytes(targetTableName.ToUpperInvariant()));
        return Convert.ToHexString(digest.AsSpan(0, 12)).ToLowerInvariant();
    }

    private static async Task<string> ComputeArchiveTokenAsync(
        Stream archiveStream,
        string targetTableName,
        CancellationToken ct)
    {
        archiveStream.Position = 0;
        byte[] archiveDigest = await SHA256.HashDataAsync(archiveStream, ct);
        return ComputeArchiveToken(archiveDigest, targetTableName);
    }

    private static string ComputeArchiveToken(
        ReadOnlySpan<byte> archiveDigest,
        string targetTableName)
    {
        byte[] targetBytes = Encoding.UTF8.GetBytes(targetTableName.ToUpperInvariant());
        byte[] identity = new byte[targetBytes.Length + 1 + archiveDigest.Length];
        targetBytes.CopyTo(identity, 0);
        archiveDigest.CopyTo(identity.AsSpan(targetBytes.Length + 1));
        byte[] operationDigest = SHA256.HashData(identity);
        return Convert.ToHexString(operationDigest.AsSpan(0, 16)).ToLowerInvariant();
    }

    private async Task EnsureRestoreJournalAsync(CancellationToken ct)
    {
        await ExecuteCheckedAsync(
            $"""
            CREATE TABLE IF NOT EXISTS {QuoteIdentifier(RestoreJournalTableName)} (
                "target_key" TEXT PRIMARY KEY,
                "staging_name" TEXT NOT NULL,
                "target_name" TEXT NOT NULL,
                "archive_token" TEXT NOT NULL,
                "owner_token" TEXT NOT NULL,
                "heartbeat_unix_ms" INTEGER NOT NULL,
                CONSTRAINT {QuoteIdentifier(RestoreJournalContractConstraintName)}
                    CHECK ({RestoreContractCheckExpression})
            );
            """,
            ct);

        ClientTableSchema schema = await client.GetTableSchemaAsync(RestoreJournalTableName, ct)
            ?? throw new InvalidDataException("Archive restore journal was not created.");
        string[] expectedColumns =
        [
            ColumnSignature("target_key", "Text", false, true, false, false, null, null),
            ColumnSignature("staging_name", "Text", false, false, false, false, null, null),
            ColumnSignature("target_name", "Text", false, false, false, false, null, null),
            ColumnSignature("archive_token", "Text", false, false, false, false, null, null),
            ColumnSignature("owner_token", "Text", false, false, false, false, null, null),
            ColumnSignature("heartbeat_unix_ms", "Integer", false, false, false, false, null, null),
        ];
        string[] actualColumns = schema.Columns.Select(ActualColumnSignature).ToArray();
        string expectedContractMarker = CheckSignature(
            RestoreJournalContractConstraintName,
            RestoreContractCheckExpression,
            columnName: null);
        string[] actualChecks = schema.CheckConstraints.Select(ActualCheckSignature).ToArray();
        string expectedPrimaryKey = KeySignature(
            name: null,
            ClientKeyConstraintKind.PrimaryKey.ToString(),
            ["target_key"]);
        string[] actualKeys = schema.KeyConstraints.Select(ActualKeySignature).ToArray();
        ClientIndexSchema[] journalIndexes = (await client.GetIndexesAsync(ct))
            .Where(index => string.Equals(index.TableName, RestoreJournalTableName, StringComparison.OrdinalIgnoreCase))
            .ToArray();
        if (!expectedColumns.SequenceEqual(actualColumns, StringComparer.Ordinal) ||
            actualChecks.Length != 1 ||
            !string.Equals(actualChecks[0], expectedContractMarker, StringComparison.Ordinal) ||
            actualKeys.Length != 1 ||
            !string.Equals(actualKeys[0], expectedPrimaryKey, StringComparison.Ordinal) ||
            schema.ForeignKeys.Count != 0 ||
            journalIndexes.Length != 0)
        {
            throw new InvalidDataException(
                $"Table '{RestoreJournalTableName}' exists but does not match the archive restore journal v1 contract; " +
                $"columns=[{string.Join(",", actualColumns)}], " +
                $"checks=[{string.Join(",", actualChecks)}], keys=[{string.Join(",", actualKeys)}]. " +
                "No restore tables were changed.");
        }
    }

    private async Task EnsureRestoreReceiptTableAsync(CancellationToken ct)
    {
        await ExecuteCheckedAsync(
            $"""
            CREATE TABLE IF NOT EXISTS {QuoteIdentifier(RestoreReceiptTableName)} (
                "target_key" TEXT PRIMARY KEY,
                "target_name" TEXT NOT NULL,
                "archive_token" TEXT NOT NULL,
                "receipt_token" TEXT NOT NULL,
                "completed_unix_ms" INTEGER NOT NULL,
                CONSTRAINT {QuoteIdentifier(RestoreReceiptContractConstraintName)}
                    CHECK ({RestoreContractCheckExpression})
            );
            """,
            ct);

        ClientTableSchema schema = await client.GetTableSchemaAsync(RestoreReceiptTableName, ct)
            ?? throw new InvalidDataException("Archive restore receipt table was not created.");
        string[] expectedColumns =
        [
            ColumnSignature("target_key", "Text", false, true, false, false, null, null),
            ColumnSignature("target_name", "Text", false, false, false, false, null, null),
            ColumnSignature("archive_token", "Text", false, false, false, false, null, null),
            ColumnSignature("receipt_token", "Text", false, false, false, false, null, null),
            ColumnSignature("completed_unix_ms", "Integer", false, false, false, false, null, null),
        ];
        string[] actualColumns = schema.Columns.Select(ActualColumnSignature).ToArray();
        string expectedContractMarker = CheckSignature(
            RestoreReceiptContractConstraintName,
            RestoreContractCheckExpression,
            columnName: null);
        string[] actualChecks = schema.CheckConstraints.Select(ActualCheckSignature).ToArray();
        string expectedPrimaryKey = KeySignature(
            name: null,
            ClientKeyConstraintKind.PrimaryKey.ToString(),
            ["target_key"]);
        string[] actualKeys = schema.KeyConstraints.Select(ActualKeySignature).ToArray();
        ClientIndexSchema[] receiptIndexes = (await client.GetIndexesAsync(ct))
            .Where(index => string.Equals(index.TableName, RestoreReceiptTableName, StringComparison.OrdinalIgnoreCase))
            .ToArray();
        if (!expectedColumns.SequenceEqual(actualColumns, StringComparer.Ordinal) ||
            actualChecks.Length != 1 ||
            !string.Equals(actualChecks[0], expectedContractMarker, StringComparison.Ordinal) ||
            actualKeys.Length != 1 ||
            !string.Equals(actualKeys[0], expectedPrimaryKey, StringComparison.Ordinal) ||
            schema.ForeignKeys.Count != 0 ||
            receiptIndexes.Length != 0)
        {
            throw new InvalidDataException(
                $"Table '{RestoreReceiptTableName}' exists but does not match the archive restore receipt v1 contract; " +
                $"columns=[{string.Join(",", actualColumns)}], " +
                $"checks=[{string.Join(",", actualChecks)}], keys=[{string.Join(",", actualKeys)}]. " +
                "No restore tables were changed.");
        }
    }

    private async Task RecoverAbandonedRestoreAsync(
        string targetKey,
        string targetTableName,
        string stagingTableName,
        string archiveToken,
        PrimitiveTableSchema expectedSchema,
        IReadOnlyList<PrimitiveIndexSchema> expectedIndexes,
        CancellationToken ct)
    {
        RestoreJournalRow? journal = await ReadRestoreJournalRowAsync(targetKey, ct);
        ClientTableSchema? stagedSchema = await client.GetTableSchemaAsync(stagingTableName, ct);
        if (journal is null)
        {
            if (stagedSchema is not null)
            {
                throw new InvalidOperationException(
                    $"Reserved restore staging table '{stagingTableName}' exists without an ownership journal entry; it was preserved.");
            }

            return;
        }

        if (!string.Equals(journal.StagingTableName, stagingTableName, StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(journal.TargetTableName, targetTableName, StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(journal.ArchiveToken, archiveToken, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"An archive restore claim already reserves target '{targetTableName}' for a different operation; it was preserved.");
        }

        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (journal.HeartbeatUnixMilliseconds > now - (long)RestoreLeaseTimeout.TotalMilliseconds)
        {
            throw new InvalidOperationException(
                $"An active archive restore already owns target '{targetTableName}'. Retry after its lease expires if the process was interrupted.");
        }

        string ownerConstraintName = RestoreOwnerConstraintPrefix + journal.OwnerToken;
        IReadOnlyList<ClientIndexSchema> stagedIndexes = stagedSchema is null
            ? Array.Empty<ClientIndexSchema>()
            : (await client.GetIndexesAsync(ct))
                .Where(index => string.Equals(index.TableName, stagingTableName, StringComparison.OrdinalIgnoreCase))
                .ToArray();
        if (stagedSchema is not null)
        {
            ValidateNormalizedSchema(
                stagedSchema,
                stagedIndexes,
                expectedSchema,
                expectedIndexes,
                ownerConstraintName,
                allowIncompletePostLoadObjects: true);
        }

        string cleanupOwner = Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
        var transaction = await client.BeginTransactionAsync(ct);
        bool committed = false;
        try
        {
            var takeover = await client.ExecuteInTransactionAsync(
                transaction.TransactionId,
                $"""
                UPDATE {QuoteIdentifier(RestoreJournalTableName)}
                SET "owner_token" = {FormatStringLiteral(cleanupOwner)},
                    "heartbeat_unix_ms" = {now.ToString(CultureInfo.InvariantCulture)}
                WHERE "target_key" = {FormatStringLiteral(targetKey)}
                  AND "owner_token" = {FormatStringLiteral(journal.OwnerToken)}
                  AND "heartbeat_unix_ms" = {journal.HeartbeatUnixMilliseconds.ToString(CultureInfo.InvariantCulture)};
                """,
                ct);
            if (!string.IsNullOrWhiteSpace(takeover.Error))
                throw new InvalidOperationException(takeover.Error);
            if (takeover.RowsAffected != 1)
            {
                throw new InvalidOperationException(
                    $"Archive restore ownership for target '{targetTableName}' changed concurrently; no staging table was removed.");
            }

            if (stagedSchema is not null)
            {
                await ExecuteInTransactionCheckedAsync(
                    transaction.TransactionId,
                    $"DROP TABLE {QuoteIdentifier(stagingTableName)};",
                    ct);
            }

            var delete = await client.ExecuteInTransactionAsync(
                transaction.TransactionId,
                $"""
                DELETE FROM {QuoteIdentifier(RestoreJournalTableName)}
                WHERE "target_key" = {FormatStringLiteral(targetKey)}
                  AND "owner_token" = {FormatStringLiteral(cleanupOwner)};
                """,
                ct);
            if (!string.IsNullOrWhiteSpace(delete.Error))
                throw new InvalidOperationException(delete.Error);
            if (delete.RowsAffected != 1)
                throw new InvalidOperationException("Archive restore stale-claim cleanup lost ownership.");

            await client.CommitTransactionAsync(transaction.TransactionId, ct);
            committed = true;
        }
        finally
        {
            if (!committed)
            {
                try
                {
                    await client.RollbackTransactionAsync(transaction.TransactionId, CancellationToken.None);
                }
                catch
                {
                    // Preserve the recovery failure. The transaction guarantees
                    // the journal owner and staging marker do not diverge.
                }
            }
        }
    }

    private async Task ClaimRestoreAsync(
        string targetKey,
        string stagingTableName,
        string targetTableName,
        string archiveToken,
        string ownerToken,
        CancellationToken ct)
    {
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var result = await client.ExecuteSqlAsync(
            $"""
            INSERT INTO {QuoteIdentifier(RestoreJournalTableName)}
                ("target_key", "staging_name", "target_name", "archive_token", "owner_token", "heartbeat_unix_ms")
            VALUES (
                {FormatStringLiteral(targetKey)},
                {FormatStringLiteral(stagingTableName)},
                {FormatStringLiteral(targetTableName)},
                {FormatStringLiteral(archiveToken)},
                {FormatStringLiteral(ownerToken)},
                {now.ToString(CultureInfo.InvariantCulture)});
            """,
            ct);
        if (!string.IsNullOrWhiteSpace(result.Error))
        {
            throw new InvalidOperationException(
                $"Could not claim archive restore target '{targetTableName}'. Another restore may have started concurrently. {result.Error}");
        }
        if (result.RowsAffected != 1)
            throw new InvalidOperationException($"Could not claim archive restore target '{targetTableName}'.");
    }

    private async Task RefreshRestoreLeaseAsync(
        string targetKey,
        string ownerToken,
        CancellationToken ct)
    {
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var result = await client.ExecuteSqlAsync(
            $"""
            UPDATE {QuoteIdentifier(RestoreJournalTableName)}
            SET "heartbeat_unix_ms" = {now.ToString(CultureInfo.InvariantCulture)}
            WHERE "target_key" = {FormatStringLiteral(targetKey)}
              AND "owner_token" = {FormatStringLiteral(ownerToken)};
            """,
            ct);
        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new InvalidOperationException(result.Error);
        if (result.RowsAffected != 1)
            throw new InvalidOperationException("Archive restore ownership was lost while loading the staging table.");
    }

    private async Task<RestoreJournalRow?> ReadRestoreJournalRowAsync(
        string targetKey,
        CancellationToken ct)
    {
        var result = await client.ExecuteSqlAsync(
            $"""
            SELECT "staging_name", "target_name", "archive_token", "owner_token", "heartbeat_unix_ms"
            FROM {QuoteIdentifier(RestoreJournalTableName)}
            WHERE "target_key" = {FormatStringLiteral(targetKey)};
            """,
            ct);
        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new InvalidOperationException(result.Error);
        if (result.Rows is not { Count: > 0 })
            return null;
        if (result.Rows.Count != 1 || result.Rows[0].Length != 5 || result.Rows[0].Any(static value => value is null))
            throw new InvalidDataException("Archive restore journal contains an invalid ownership row.");

        object?[] row = result.Rows[0];
        return new RestoreJournalRow(
            Convert.ToString(row[0], CultureInfo.InvariantCulture)!,
            Convert.ToString(row[1], CultureInfo.InvariantCulture)!,
            Convert.ToString(row[2], CultureInfo.InvariantCulture)!,
            Convert.ToString(row[3], CultureInfo.InvariantCulture)!,
            Convert.ToInt64(row[4], CultureInfo.InvariantCulture));
    }

    private async Task<RestoreActivationReceipt?> ReadRestoreActivationReceiptAsync(
        string targetKey,
        CancellationToken ct)
    {
        var result = await client.ExecuteSqlAsync(
            $"""
            SELECT "target_name", "archive_token", "receipt_token", "completed_unix_ms"
            FROM {QuoteIdentifier(RestoreReceiptTableName)}
            WHERE "target_key" = {FormatStringLiteral(targetKey)};
            """,
            ct);
        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new InvalidOperationException(result.Error);
        if (result.Rows is not { Count: > 0 })
            return null;
        if (result.Rows.Count != 1 || result.Rows[0].Length != 4 || result.Rows[0].Any(static value => value is null))
            throw new InvalidDataException("Archive restore receipt table contains an invalid activation row.");

        object?[] row = result.Rows[0];
        return new RestoreActivationReceipt(
            Convert.ToString(row[0], CultureInfo.InvariantCulture)!,
            Convert.ToString(row[1], CultureInfo.InvariantCulture)!,
            Convert.ToString(row[2], CultureInfo.InvariantCulture)!,
            Convert.ToInt64(row[3], CultureInfo.InvariantCulture));
    }

    private async Task DeleteRestoreJournalClaimAsync(
        string targetKey,
        string ownerToken,
        CancellationToken ct)
    {
        var result = await client.ExecuteSqlAsync(
            $"""
            DELETE FROM {QuoteIdentifier(RestoreJournalTableName)}
            WHERE "target_key" = {FormatStringLiteral(targetKey)}
              AND "owner_token" = {FormatStringLiteral(ownerToken)};
            """,
            ct);
        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new InvalidOperationException(result.Error);
        if (result.RowsAffected != 1)
            throw new InvalidOperationException("Archive restore journal ownership changed before cleanup completed.");
    }

    private async Task ValidateRestoredSchemaAsync(
        PrimitiveTableSchema expectedSchema,
        IReadOnlyList<PrimitiveIndexSchema> expectedIndexes,
        string ownerConstraintName,
        CancellationToken ct)
    {
        ClientTableSchema actualSchema = await client.GetTableSchemaAsync(expectedSchema.TableName, ct)
            ?? throw new InvalidDataException("Archive restore staging table disappeared before validation.");
        ClientIndexSchema[] actualIndexes = (await client.GetIndexesAsync(ct))
            .Where(index => string.Equals(index.TableName, expectedSchema.TableName, StringComparison.OrdinalIgnoreCase))
            .ToArray();
        ValidateNormalizedSchema(
            actualSchema,
            actualIndexes,
            expectedSchema,
            expectedIndexes,
            ownerConstraintName,
            allowIncompletePostLoadObjects: false);
    }

    private async Task ValidateRowsAndActivateRestoreAsync(
        ArchiveRestoreSnapshot archive,
        PrimitiveTableSchema restoreSchema,
        IReadOnlyList<PrimitiveIndexSchema> expectedIndexes,
        long expectedRowCount,
        string expectedArchiveToken,
        string targetKey,
        string ownerToken,
        string ownerConstraintName,
        string stagingTableName,
        string targetTableName,
        ICSharpDbTransactionalSnapshotReader transactionalReader,
        CancellationToken ct)
    {
        CanonicalRowContract contract = CanonicalRowProjector.CreateCSharpDbTableContract(restoreSchema);
        var validator = new PartitionedChecksumValidator();
        var transaction = new RestoreValidationTransaction(client);
        string activationReceiptToken = Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
        bool committed = false;
        try
        {
            PartitionedChecksumValidationResult validation = await validator.ValidateAsync(
                contract,
                EnumerateArchiveValidationRowsAsync(
                    archive,
                    targetKey,
                    ownerToken,
                    ct),
                EnumerateRestoredValidationRowsAsync(
                    transaction,
                    restoreSchema,
                    expectedIndexes,
                    ownerConstraintName,
                    transactionalReader,
                    ct),
                new PartitionedChecksumValidatorOptions
                {
                    SpillRootDirectory = _restoreOptions.ScratchDirectory!,
                    MaxSpillBytes = _restoreOptions.MaxValidationSpillBytes,
                    MaxMismatchDetailsPerPartition = 1,
                },
                ct);
            if (validation.Status != MigrationValidationStatus.Passed ||
                validation.SourceRowCount != expectedRowCount ||
                validation.TargetRowCount != expectedRowCount)
            {
                throw new InvalidDataException(
                    "Archive restore canonical row validation failed before activation.");
            }

            string transactionId = transaction.TransactionId
                ?? throw new InvalidOperationException("Archive restore target validation did not open a transaction.");
            string currentArchiveToken = await ComputeArchiveTokenAsync(archive.Stream, targetTableName, ct);
            if (!string.Equals(currentArchiveToken, expectedArchiveToken, StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    "The table archive changed while its staged restore was being validated.");
            }

            await ExecuteInTransactionCheckedAsync(
                transactionId,
                $"ALTER TABLE {QuoteIdentifier(stagingTableName)} DROP CONSTRAINT {QuoteIdentifier(ownerConstraintName)};",
                CancellationToken.None);
            await ExecuteInTransactionCheckedAsync(
                transactionId,
                $"ALTER TABLE {QuoteIdentifier(stagingTableName)} RENAME TO {QuoteIdentifier(targetTableName)};",
                CancellationToken.None);
            await ExecuteInTransactionCheckedAsync(
                transactionId,
                $"""
                DELETE FROM {QuoteIdentifier(RestoreReceiptTableName)}
                WHERE "target_key" = {FormatStringLiteral(targetKey)};
                """,
                CancellationToken.None);
            var receipt = await client.ExecuteInTransactionAsync(
                transactionId,
                $"""
                INSERT INTO {QuoteIdentifier(RestoreReceiptTableName)}
                    ("target_key", "target_name", "archive_token", "receipt_token", "completed_unix_ms")
                VALUES (
                    {FormatStringLiteral(targetKey)},
                    {FormatStringLiteral(targetTableName)},
                    {FormatStringLiteral(expectedArchiveToken)},
                    {FormatStringLiteral(activationReceiptToken)},
                    {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture)}
                );
                """,
                CancellationToken.None);
            if (!string.IsNullOrWhiteSpace(receipt.Error))
                throw new InvalidOperationException(receipt.Error);
            if (receipt.RowsAffected != 1)
                throw new InvalidOperationException("Archive restore activation receipt was not recorded.");

            var delete = await client.ExecuteInTransactionAsync(
                transactionId,
                $"""
                DELETE FROM {QuoteIdentifier(RestoreJournalTableName)}
                WHERE "target_key" = {FormatStringLiteral(targetKey)}
                  AND "owner_token" = {FormatStringLiteral(ownerToken)};
                """,
                CancellationToken.None);
            if (!string.IsNullOrWhiteSpace(delete.Error))
                throw new InvalidOperationException(delete.Error);
            if (delete.RowsAffected != 1)
                throw new InvalidOperationException("Archive restore ownership changed before activation.");

            try
            {
                await client.CommitTransactionAsync(transactionId, CancellationToken.None);
                committed = true;
            }
            catch
            {
                if (!await IsRestoreActivationCommittedAsync(
                        targetKey,
                        targetTableName,
                        expectedArchiveToken,
                        activationReceiptToken))
                {
                    throw;
                }

                committed = true;
            }
        }
        finally
        {
            if (!committed && transaction.TransactionId is string transactionId)
            {
                try
                {
                    await client.RollbackTransactionAsync(transactionId, CancellationToken.None);
                }
                catch
                {
                    // Preserve the activation failure; a committed rename has no
                    // staging table for the outer cleanup path to remove.
                }
            }
        }
    }

    private async IAsyncEnumerable<MigrationValidationRow> EnumerateArchiveValidationRowsAsync(
        ArchiveRestoreSnapshot archive,
        string targetKey,
        string ownerToken,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        await using var heartbeat = new RestoreLeaseHeartbeat(
            heartbeatCt => RefreshRestoreLeaseAsync(targetKey, ownerToken, heartbeatCt),
            ct);
        await using IAsyncEnumerator<PrimitiveDbValue[]> rows = TableArchiveReader
            .ReadRowsAsync(archive.Stream, heartbeat.Token)
            .GetAsyncEnumerator(heartbeat.Token);
        while (true)
        {
            bool hasNext;
            try
            {
                hasNext = await rows.MoveNextAsync().ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (heartbeat.HasFailed)
            {
                heartbeat.ThrowIfFailed();
                throw;
            }

            if (!hasNext)
                break;

            yield return new MigrationValidationRow { Values = rows.Current };
        }

        await heartbeat.StopAsync();
    }

    private async IAsyncEnumerable<MigrationValidationRow> EnumerateRestoredValidationRowsAsync(
        RestoreValidationTransaction transaction,
        PrimitiveTableSchema schema,
        IReadOnlyList<PrimitiveIndexSchema> expectedIndexes,
        string ownerConstraintName,
        ICSharpDbTransactionalSnapshotReader transactionalReader,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        string transactionId = await transaction.EnsureStartedAsync(ct);
        TransactionTableSnapshot snapshot = await transactionalReader.ReadTableSnapshotAsync(
                transactionId,
                schema.TableName,
                ct)
            ?? throw new InvalidDataException(
                "Archive restore staging table disappeared before transactional validation.");
        ValidateNormalizedSchema(
            snapshot.Schema,
            snapshot.Indexes,
            schema,
            expectedIndexes,
            ownerConstraintName,
            allowIncompletePostLoadObjects: false);

        string projection = string.Join(", ", schema.Columns.Select(column => QuoteIdentifier(column.Name)));
        await using ForwardOnlyQueryCursor cursor =
            await transactionalReader.TryOpenForwardOnlyQueryCursorAsync(
                transactionId,
                $"SELECT {projection} FROM {QuoteIdentifier(schema.TableName)};",
                ct)
            ?? throw new InvalidOperationException(
                "The direct CSharpDB transport could not open a transactional restore-validation cursor.");
        if (cursor.ColumnNames.Length != schema.Columns.Count ||
            !cursor.ColumnNames.SequenceEqual(
                schema.Columns.Select(static column => column.Name),
                StringComparer.OrdinalIgnoreCase))
        {
            throw new InvalidDataException(
                "Archive restore canonical validation returned an invalid target projection.");
        }

        while (true)
        {
            ct.ThrowIfCancellationRequested();
            List<object?[]> rows = await cursor.ReadNextAsync(1, ct);
            if (rows.Count == 0)
                yield break;

            object?[] row = rows[0];
            if (row.Length != schema.Columns.Count)
            {
                throw new InvalidDataException(
                    "Archive restore canonical validation returned a target row with an invalid field count.");
            }

            yield return new MigrationValidationRow
            {
                Values = MapRow(schema.Columns, row),
            };
        }
    }

    private sealed class RestoreValidationTransaction(ICSharpDbClient client)
    {
        public string? TransactionId { get; private set; }

        public async ValueTask<string> EnsureStartedAsync(CancellationToken ct)
        {
            if (TransactionId is null)
                TransactionId = (await client.BeginTransactionAsync(ct)).TransactionId;
            return TransactionId;
        }
    }

    private async Task ExecuteInTransactionCheckedAsync(
        string transactionId,
        string sql,
        CancellationToken ct)
    {
        var result = await client.ExecuteInTransactionAsync(transactionId, sql, ct);
        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new InvalidOperationException(result.Error);
    }

    private async Task<bool> IsRestoreActivationCommittedAsync(
        string targetKey,
        string targetTableName,
        string archiveToken,
        string receiptToken)
    {
        try
        {
            RestoreActivationReceipt? receipt = await ReadRestoreActivationReceiptAsync(
                targetKey,
                CancellationToken.None);
            return receipt is not null &&
                   string.Equals(receipt.TargetTableName, targetTableName, StringComparison.OrdinalIgnoreCase) &&
                   string.Equals(receipt.ArchiveToken, archiveToken, StringComparison.Ordinal) &&
                   string.Equals(receipt.ReceiptToken, receiptToken, StringComparison.Ordinal);
        }
        catch
        {
            return false;
        }
    }

    private async Task TryReleaseRestoreAsync(
        string targetKey,
        string ownerToken,
        string ownerConstraintName,
        string stagingTableName,
        PrimitiveTableSchema expectedSchema,
        IReadOnlyList<PrimitiveIndexSchema> expectedIndexes,
        bool stagingCreated)
    {
        try
        {
            if (stagingCreated)
            {
                ClientTableSchema? stagedSchema = await client.GetTableSchemaAsync(
                    stagingTableName,
                    CancellationToken.None);
                if (stagedSchema is not null)
                {
                    ClientIndexSchema[] stagedIndexes = (await client.GetIndexesAsync(CancellationToken.None))
                        .Where(index => string.Equals(index.TableName, stagingTableName, StringComparison.OrdinalIgnoreCase))
                        .ToArray();
                    ValidateNormalizedSchema(
                        stagedSchema,
                        stagedIndexes,
                        expectedSchema,
                        expectedIndexes,
                        ownerConstraintName,
                        allowIncompletePostLoadObjects: true);
                    await ExecuteCheckedAsync(
                        $"DROP TABLE {QuoteIdentifier(stagingTableName)};",
                        CancellationToken.None);
                }
            }

            if (!stagingCreated ||
                await client.GetTableSchemaAsync(stagingTableName, CancellationToken.None) is null)
            {
                await DeleteRestoreJournalClaimAsync(targetKey, ownerToken, CancellationToken.None);
            }
        }
        catch
        {
            // Keep the original restore failure. Any claim that could not be
            // proven safe to clean remains journaled for a later recovery.
        }
    }

    private static void ValidateNormalizedSchema(
        ClientTableSchema actualSchema,
        IReadOnlyList<ClientIndexSchema> actualIndexes,
        PrimitiveTableSchema expectedSchema,
        IReadOnlyList<PrimitiveIndexSchema> expectedIndexes,
        string? ownerConstraintName,
        bool allowIncompletePostLoadObjects)
    {
        var differences = new List<string>();
        CompareExact(
            "columns",
            expectedSchema.Columns.Select(column => ExpectedColumnSignature(
                column,
                hasExplicitPrimaryKey: expectedSchema.KeyConstraints.Any(
                    static key => key.Kind == PrimitiveKeyConstraintKind.PrimaryKey))),
            actualSchema.Columns.Select(ActualColumnSignature),
            preserveOrder: true,
            differences);
        CompareExact(
            "key constraints",
            ExpectedKeySignatures(expectedSchema),
            actualSchema.KeyConstraints.Select(ActualKeySignature),
            preserveOrder: false,
            differences);

        IEnumerable<string> expectedChecks = expectedSchema.CheckConstraints.Select(ExpectedCheckSignature);
        if (ownerConstraintName is not null)
        {
            expectedChecks = expectedChecks.Append(
                CheckSignature(ownerConstraintName, RestoreContractCheckExpression, columnName: null));
        }
        CompareExact(
            "check constraints",
            expectedChecks,
            actualSchema.CheckConstraints.Select(ActualCheckSignature),
            preserveOrder: false,
            differences);

        CompareCompleteOrSubset(
            "foreign keys",
            expectedSchema.ForeignKeys.Select(ExpectedForeignKeySignature),
            actualSchema.ForeignKeys.Select(ActualForeignKeySignature),
            allowIncompletePostLoadObjects,
            differences);
        CompareCompleteOrSubset(
            "secondary indexes",
            expectedIndexes.Select(ExpectedIndexSignature),
            actualIndexes.Select(ActualIndexSignature),
            allowIncompletePostLoadObjects,
            differences);

        if (!allowIncompletePostLoadObjects &&
            expectedSchema.NextRowId > 0 &&
            actualSchema.NextRowId != expectedSchema.NextRowId)
        {
            differences.Add(
                $"next-row id expected {expectedSchema.NextRowId.ToString(CultureInfo.InvariantCulture)}, " +
                $"found {actualSchema.NextRowId.ToString(CultureInfo.InvariantCulture)}");
        }

        if (differences.Count > 0)
        {
            throw new InvalidDataException(
                $"Archive restore schema validation failed for staging table '{actualSchema.TableName}': " +
                string.Join("; ", differences));
        }
    }

    private static void CompareExact(
        string description,
        IEnumerable<string> expected,
        IEnumerable<string> actual,
        bool preserveOrder,
        ICollection<string> differences)
    {
        string[] expectedValues = preserveOrder
            ? expected.ToArray()
            : expected.Order(StringComparer.Ordinal).ToArray();
        string[] actualValues = preserveOrder
            ? actual.ToArray()
            : actual.Order(StringComparer.Ordinal).ToArray();
        if (!expectedValues.SequenceEqual(actualValues, StringComparer.Ordinal))
        {
            differences.Add(
                $"{description} do not match the archive " +
                $"(expected [{string.Join(";", expectedValues)}], found [{string.Join(";", actualValues)}])");
        }
    }

    private static void CompareCompleteOrSubset(
        string description,
        IEnumerable<string> expected,
        IEnumerable<string> actual,
        bool allowSubset,
        ICollection<string> differences)
    {
        var expectedValues = expected.ToHashSet(StringComparer.Ordinal);
        var actualValues = actual.ToHashSet(StringComparer.Ordinal);
        bool matches = allowSubset
            ? actualValues.IsSubsetOf(expectedValues)
            : actualValues.SetEquals(expectedValues);
        if (!matches)
            differences.Add($"{description} do not match the archive");
    }

    private static string ExpectedColumnSignature(
        PrimitiveColumnDefinition column,
        bool hasExplicitPrimaryKey) =>
        ColumnSignature(
            column.Name,
            column.Type.ToString(),
            column.Nullable,
            column.IsPrimaryKey,
            column.IsIdentity ||
                (!hasExplicitPrimaryKey && column.IsPrimaryKey && column.Type == PrimitiveDbType.Integer),
            column.IsRowVersion,
            column.Collation,
            column.DefaultSql);

    private static string ActualColumnSignature(ClientColumnDefinition column) =>
        ColumnSignature(
            column.Name,
            column.Type.ToString(),
            column.Nullable,
            column.IsPrimaryKey,
            column.IsIdentity,
            column.IsRowVersion,
            column.Collation,
            column.DefaultSql);

    private static string ColumnSignature(
        string name,
        string type,
        bool nullable,
        bool primaryKey,
        bool identity,
        bool rowVersion,
        string? collation,
        string? defaultSql) =>
        $"{NormalizeIdentifier(name)}|{type}|{nullable}|{primaryKey}|{identity}|{rowVersion}|" +
        $"{NormalizeIdentifier(collation)}|{NormalizeSql(defaultSql)}";

    private static string ExpectedCheckSignature(PrimitiveCheckConstraintDefinition check) =>
        CheckSignature(check.ConstraintName, check.ExpressionSql, check.ColumnName);

    private static string ActualCheckSignature(ClientCheckConstraintDefinition check) =>
        CheckSignature(check.ConstraintName, check.ExpressionSql, check.ColumnName);

    private static string CheckSignature(string? name, string sql, string? columnName) =>
        $"{NormalizeIdentifier(name)}|{NormalizeIdentifier(columnName)}|{NormalizeSql(sql)}";

    private static string ExpectedKeySignature(PrimitiveKeyConstraintDefinition key) =>
        KeySignature(key.ConstraintName, key.Kind.ToString(), key.Columns);

    private static IEnumerable<string> ExpectedKeySignatures(PrimitiveTableSchema schema)
    {
        foreach (PrimitiveKeyConstraintDefinition key in schema.KeyConstraints)
            yield return ExpectedKeySignature(key);

        if (!schema.KeyConstraints.Any(static key => key.Kind == PrimitiveKeyConstraintKind.PrimaryKey))
        {
            string[] primaryKeyColumns = schema.Columns
                .Where(static column => column.IsPrimaryKey)
                .Select(static column => column.Name)
                .ToArray();
            if (primaryKeyColumns.Length > 0)
                yield return KeySignature(name: null, PrimitiveKeyConstraintKind.PrimaryKey.ToString(), primaryKeyColumns);
        }
    }

    private static string ActualKeySignature(ClientKeyConstraintDefinition key) =>
        KeySignature(key.ConstraintName, key.Kind.ToString(), key.Columns);

    private static string KeySignature(string? name, string kind, IReadOnlyList<string> columns) =>
        $"{NormalizeIdentifier(name)}|{kind}|{string.Join(",", columns.Select(NormalizeIdentifier))}";

    private static string ExpectedForeignKeySignature(PrimitiveForeignKeyDefinition foreignKey) =>
        ForeignKeySignature(
            foreignKey.ConstraintName,
            foreignKey.ColumnNames.Count > 0 ? foreignKey.ColumnNames : [foreignKey.ColumnName],
            foreignKey.ReferencedTableName,
            foreignKey.ReferencedColumnNames.Count > 0
                ? foreignKey.ReferencedColumnNames
                : [foreignKey.ReferencedColumnName],
            foreignKey.OnDelete.ToString());

    private static string ActualForeignKeySignature(ClientForeignKeyDefinition foreignKey) =>
        ForeignKeySignature(
            foreignKey.ConstraintName,
            foreignKey.ColumnNames.Count > 0 ? foreignKey.ColumnNames : [foreignKey.ColumnName],
            foreignKey.ReferencedTableName,
            foreignKey.ReferencedColumnNames.Count > 0
                ? foreignKey.ReferencedColumnNames
                : [foreignKey.ReferencedColumnName],
            foreignKey.OnDelete.ToString());

    private static string ForeignKeySignature(
        string name,
        IReadOnlyList<string> columns,
        string referencedTable,
        IReadOnlyList<string> referencedColumns,
        string onDelete) =>
        $"{NormalizeIdentifier(name)}|{string.Join(",", columns.Select(NormalizeIdentifier))}|" +
        $"{NormalizeIdentifier(referencedTable)}|" +
        $"{string.Join(",", referencedColumns.Select(NormalizeIdentifier))}|{onDelete}";

    private static string ExpectedIndexSignature(PrimitiveIndexSchema index) =>
        IndexSignature(index.IndexName, index.Columns, index.ColumnCollations, index.IsUnique);

    private static string ActualIndexSignature(ClientIndexSchema index) =>
        IndexSignature(index.IndexName, index.Columns, index.ColumnCollations, index.IsUnique);

    private static string IndexSignature(
        string name,
        IReadOnlyList<string> columns,
        IReadOnlyList<string?> collations,
        bool unique) =>
        $"{NormalizeIdentifier(name)}|{string.Join(",", columns.Select(NormalizeIdentifier))}|" +
        $"{string.Join(",", collations.Select(NormalizeIdentifier))}|{unique}";

    private static string NormalizeIdentifier(string? value) =>
        value is null ? "<NULL>" : value.ToUpperInvariant();

    private static string NormalizeSql(string? value) =>
        value is null
            ? "<NULL>"
            : string.Join(' ', value.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

    private sealed record RestoreJournalRow(
        string StagingTableName,
        string TargetTableName,
        string ArchiveToken,
        string OwnerToken,
        long HeartbeatUnixMilliseconds);

    private sealed record RestoreActivationReceipt(
        string TargetTableName,
        string ArchiveToken,
        string ReceiptToken,
        long CompletedUnixMilliseconds);

    private sealed class RestoreLeaseHeartbeat : IAsyncDisposable
    {
        private static readonly TimeSpan s_interval = TimeSpan.FromMinutes(1);

        private readonly CancellationTokenSource _work;
        private readonly CancellationTokenSource _stop;
        private readonly Task _background;
        private Exception? _failure;
        private int _stopped;

        public RestoreLeaseHeartbeat(
            Func<CancellationToken, Task> refresh,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(refresh);
            _work = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _background = RunAsync(refresh, _stop.Token);
        }

        public CancellationToken Token => _work.Token;

        public bool HasFailed => Volatile.Read(ref _failure) is not null;

        public void ThrowIfFailed()
        {
            Exception? failure = Volatile.Read(ref _failure);
            if (failure is not null)
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
        }

        public async ValueTask StopAsync()
        {
            RequestStop();
            await _background.ConfigureAwait(false);
            ThrowIfFailed();
        }

        public async ValueTask DisposeAsync()
        {
            RequestStop();
            try
            {
                await _background.ConfigureAwait(false);
            }
            catch
            {
                // StopAsync reports heartbeat failures on the success path. On
                // exceptional paths, preserve the original restore failure.
            }
            finally
            {
                _work.Dispose();
                _stop.Dispose();
            }
        }

        private async Task RunAsync(
            Func<CancellationToken, Task> refresh,
            CancellationToken cancellationToken)
        {
            try
            {
                while (true)
                {
                    await Task.Delay(s_interval, cancellationToken).ConfigureAwait(false);
                    await refresh(cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Normal caller cancellation or explicit shutdown.
            }
            catch (Exception exception)
            {
                _failure = exception;
                _work.Cancel();
            }
        }

        private void RequestStop()
        {
            if (Interlocked.Exchange(ref _stopped, 1) == 0)
                _stop.Cancel();
        }
    }

    private sealed class ArchiveRestoreSnapshot : IAsyncDisposable
    {
        private FileStream? _lease;

        private ArchiveRestoreSnapshot(string path, byte[] digest, FileStream lease)
        {
            Path = path;
            Digest = digest;
            _lease = lease;
        }

        public string Path { get; }

        public byte[] Digest { get; }

        public FileStream Stream => _lease
            ?? throw new ObjectDisposedException(nameof(ArchiveRestoreSnapshot));

        public static async Task<ArchiveRestoreSnapshot> CreateAsync(
            string sourcePath,
            string scratchDirectory,
            long maximumBytes,
            CancellationToken ct)
        {
            Directory.CreateDirectory(scratchDirectory);
            string snapshotPath = System.IO.Path.Combine(
                scratchDirectory,
                $"csharpdb-restore-archive-{Guid.NewGuid():N}.snapshot");
            FileStream? snapshot = null;
            try
            {
                await using var source = new FileStream(
                    sourcePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read,
                    bufferSize: 64 * 1024,
                    FileOptions.Asynchronous | FileOptions.SequentialScan);
                if (source.Length > maximumBytes)
                {
                    throw new InvalidDataException(
                        $"The table archive is {source.Length.ToString(CultureInfo.InvariantCulture)} bytes, " +
                        $"which exceeds the configured {maximumBytes.ToString(CultureInfo.InvariantCulture)}-byte " +
                        "restore snapshot limit.");
                }

                snapshot = new FileStream(
                    snapshotPath,
                    FileMode.CreateNew,
                    FileAccess.ReadWrite,
                    FileShare.Read,
                    bufferSize: 64 * 1024,
                    FileOptions.Asynchronous);
                using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
                var buffer = new byte[64 * 1024];
                long copiedBytes = 0;
                while (true)
                {
                    int read = await source.ReadAsync(buffer, ct).ConfigureAwait(false);
                    if (read == 0)
                        break;

                    copiedBytes = checked(copiedBytes + read);
                    if (copiedBytes > maximumBytes)
                    {
                        throw new InvalidDataException(
                            $"The table archive exceeds the configured " +
                            $"{maximumBytes.ToString(CultureInfo.InvariantCulture)}-byte restore snapshot limit.");
                    }

                    hasher.AppendData(buffer, 0, read);
                    await snapshot.WriteAsync(buffer.AsMemory(0, read), ct).ConfigureAwait(false);
                }

                await snapshot.FlushAsync(ct).ConfigureAwait(false);
                snapshot.Flush(flushToDisk: true);
                byte[] digest = hasher.GetHashAndReset();
                snapshot.Position = 0;
                var result = new ArchiveRestoreSnapshot(snapshotPath, digest, snapshot);
                snapshot = null;
                return result;
            }
            catch
            {
                if (snapshot is not null)
                    await snapshot.DisposeAsync().ConfigureAwait(false);
                try
                {
                    File.Delete(snapshotPath);
                }
                catch (Exception cleanupException) when (
                    cleanupException is IOException or UnauthorizedAccessException)
                {
                    // Preserve the snapshot/copy failure.
                }

                throw;
            }
        }

        public async ValueTask DisposeAsync()
        {
            FileStream? lease = Interlocked.Exchange(ref _lease, null);
            if (lease is null)
                return;

            try
            {
                await lease.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
                // Cleanup must not turn an already committed activation into a
                // reported restore failure.
            }

            try
            {
                File.Delete(Path);
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
                // The private snapshot can be reclaimed by normal temp cleanup.
            }
        }
    }

    private static string BuildCreateTableSql(
        PrimitiveTableSchema schema,
        string? restoreOwnerConstraintName = null)
    {
        if (schema.Columns.Count == 0)
            throw new InvalidDataException("An archived table must contain at least one column.");

        PrimitiveKeyConstraintDefinition? declaredPrimaryKey = schema.KeyConstraints.FirstOrDefault(
            static key => key.Kind == PrimitiveKeyConstraintKind.PrimaryKey);
        var definitions = new List<string>(
            schema.Columns.Count + schema.KeyConstraints.Count + schema.CheckConstraints.Count);

        foreach (PrimitiveColumnDefinition column in schema.Columns)
        {
            var definition = new StringBuilder();
            definition.Append(QuoteIdentifier(column.Name))
                .Append(' ')
                .Append(column.Type.ToString().ToUpperInvariant());

            if (column.IsRowVersion)
                definition.Append(" ROWVERSION");

            bool inlinePrimaryKey = declaredPrimaryKey is null && column.IsPrimaryKey;
            if (inlinePrimaryKey)
                definition.Append(" PRIMARY KEY");

            if (column.IsIdentity)
            {
                if (declaredPrimaryKey is null)
                {
                    definition.Append(" IDENTITY");
                }
                else if (declaredPrimaryKey.Columns.Count != 1 ||
                         !string.Equals(
                             declaredPrimaryKey.Columns[0],
                             column.Name,
                             StringComparison.OrdinalIgnoreCase) ||
                         column.Type != PrimitiveDbType.Integer)
                {
                    throw new InvalidDataException(
                        $"Archived identity column '{column.Name}' does not match its INTEGER primary key.");
                }
                // A table-level single INTEGER primary key restores the engine's
                // identity semantics without adding a duplicate inline key.
            }

            if (!column.Nullable && !inlinePrimaryKey)
                definition.Append(" NOT NULL");
            if (!string.IsNullOrWhiteSpace(column.Collation))
                definition.Append(" COLLATE ").Append(QuoteIdentifier(column.Collation));
            if (!string.IsNullOrWhiteSpace(column.DefaultSql))
            {
                if (column.IsRowVersion)
                    throw new InvalidDataException($"Archived ROWVERSION column '{column.Name}' cannot have a default.");
                definition.Append(" DEFAULT ").Append(column.DefaultSql);
            }

            foreach (PrimitiveCheckConstraintDefinition check in schema.CheckConstraints.Where(check =>
                         check.ColumnName is not null &&
                         string.Equals(check.ColumnName, column.Name, StringComparison.OrdinalIgnoreCase)))
            {
                AppendCheckConstraint(definition, check);
            }

            definitions.Add(definition.ToString());
        }

        foreach (PrimitiveCheckConstraintDefinition check in schema.CheckConstraints)
        {
            if (check.ColumnName is not null &&
                !schema.Columns.Any(column => string.Equals(
                    column.Name,
                    check.ColumnName,
                    StringComparison.OrdinalIgnoreCase)))
            {
                throw new InvalidDataException(
                    $"Archived CHECK constraint references missing column '{check.ColumnName}'.");
            }
        }

        foreach (PrimitiveKeyConstraintDefinition key in schema.KeyConstraints)
        {
            if (key.Columns.Count == 0)
                throw new InvalidDataException("An archived key constraint has no columns.");

            var definition = new StringBuilder();
            if (key.ConstraintName is not null)
                definition.Append("CONSTRAINT ").Append(QuoteIdentifier(key.ConstraintName)).Append(' ');
            definition.Append(key.Kind switch
            {
                PrimitiveKeyConstraintKind.PrimaryKey => "PRIMARY KEY",
                PrimitiveKeyConstraintKind.Unique => "UNIQUE",
                _ => throw new InvalidDataException($"Unsupported archived key constraint kind '{key.Kind}'."),
            });
            definition.Append(" (")
                .Append(string.Join(", ", key.Columns.Select(QuoteIdentifier)))
                .Append(')');
            definitions.Add(definition.ToString());
        }

        foreach (PrimitiveCheckConstraintDefinition check in schema.CheckConstraints.Where(
                     static check => check.ColumnName is null))
        {
            var definition = new StringBuilder();
            AppendCheckConstraint(definition, check, includeLeadingSpace: false);
            definitions.Add(definition.ToString());
        }

        if (restoreOwnerConstraintName is not null)
        {
            definitions.Add(
                $"CONSTRAINT {QuoteIdentifier(restoreOwnerConstraintName)} " +
                $"CHECK ({RestoreContractCheckExpression})");
        }

        return $"CREATE TABLE {QuoteIdentifier(schema.TableName)} ({string.Join(", ", definitions)});";
    }

    private static void AppendCheckConstraint(
        StringBuilder sql,
        PrimitiveCheckConstraintDefinition check,
        bool includeLeadingSpace = true)
    {
        if (string.IsNullOrWhiteSpace(check.ExpressionSql))
            throw new InvalidDataException("An archived CHECK constraint has no expression.");

        if (includeLeadingSpace)
            sql.Append(' ');
        if (check.ConstraintName is not null)
            sql.Append("CONSTRAINT ").Append(QuoteIdentifier(check.ConstraintName)).Append(' ');
        sql.Append("CHECK (").Append(check.ExpressionSql).Append(')');
    }

    private static string BuildAddForeignKeySql(
        string tableName,
        PrimitiveForeignKeyDefinition foreignKey)
    {
        IReadOnlyList<string> sourceColumns = foreignKey.ColumnNames.Count > 0
            ? foreignKey.ColumnNames
            : [foreignKey.ColumnName];
        IReadOnlyList<string> referencedColumns = foreignKey.ReferencedColumnNames.Count > 0
            ? foreignKey.ReferencedColumnNames
            : [foreignKey.ReferencedColumnName];
        if (sourceColumns.Count == 0 || sourceColumns.Count != referencedColumns.Count)
        {
            throw new InvalidDataException(
                $"Archived foreign key '{foreignKey.ConstraintName}' has inconsistent column lists.");
        }

        string onDelete = foreignKey.OnDelete switch
        {
            PrimitiveForeignKeyOnDeleteAction.Restrict => "RESTRICT",
            PrimitiveForeignKeyOnDeleteAction.Cascade => "CASCADE",
            _ => throw new InvalidDataException(
                $"Unsupported archived foreign key delete action '{foreignKey.OnDelete}'."),
        };
        return
            $"ALTER TABLE {QuoteIdentifier(tableName)} " +
            $"ADD CONSTRAINT {QuoteIdentifier(foreignKey.ConstraintName)} " +
            $"FOREIGN KEY ({string.Join(", ", sourceColumns.Select(QuoteIdentifier))}) " +
            $"REFERENCES {QuoteIdentifier(foreignKey.ReferencedTableName)} " +
            $"({string.Join(", ", referencedColumns.Select(QuoteIdentifier))}) " +
            $"ON DELETE {onDelete};";
    }

    private static string BuildCreateIndexSql(PrimitiveIndexSchema index)
    {
        if (index.Columns.Count == 0)
            throw new InvalidDataException($"Archived secondary index '{index.IndexName}' has no columns.");
        if (index.ColumnCollations.Count != 0 && index.ColumnCollations.Count != index.Columns.Count)
        {
            throw new InvalidDataException(
                $"Archived secondary index '{index.IndexName}' has inconsistent collation metadata.");
        }

        string[] columns = new string[index.Columns.Count];
        for (int i = 0; i < columns.Length; i++)
        {
            columns[i] = QuoteIdentifier(index.Columns[i]);
            string? collation = index.ColumnCollations.Count == 0 ? null : index.ColumnCollations[i];
            if (collation is not null)
                columns[i] += $" COLLATE {QuoteIdentifier(collation)}";
        }

        string unique = index.IsUnique ? "UNIQUE " : string.Empty;
        return
            $"CREATE {unique}INDEX {QuoteIdentifier(index.IndexName)} " +
            $"ON {QuoteIdentifier(index.TableName)} ({string.Join(", ", columns)});";
    }

    private static string FormatLiteral(PrimitiveDbValue value, PrimitiveDbType columnType)
    {
        if (value.IsNull)
            return "NULL";

        return columnType switch
        {
            PrimitiveDbType.Integer => value.AsInteger.ToString(CultureInfo.InvariantCulture),
            PrimitiveDbType.Real => value.AsReal.ToString("R", CultureInfo.InvariantCulture),
            PrimitiveDbType.Blob => "X'" + Convert.ToHexString(value.AsBlob) + "'",
            _ => FormatStringLiteral(value.Type == PrimitiveDbType.Text ? value.AsText : value.ToString()),
        };
    }

    private static string FormatStringLiteral(string value) => "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

    private async Task ExecuteCheckedAsync(string sql, CancellationToken ct)
    {
        var result = await client.ExecuteSqlAsync(sql, ct);
        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new InvalidOperationException(result.Error);
    }

    private async Task<long> ExecuteScalarInt64Async(string sql, CancellationToken ct)
    {
        var result = await client.ExecuteSqlAsync(sql, ct);
        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new InvalidOperationException(result.Error);
        if (result.Rows is not { Count: 1 } || result.Rows[0].Length != 1 || result.Rows[0][0] is null)
            throw new InvalidDataException("Archive restore validation did not return one scalar count.");

        return Convert.ToInt64(result.Rows[0][0], CultureInfo.InvariantCulture);
    }

    private static string RequireArchiveIdentifier(string value, string description)
    {
        SqlIdentifierRules.Validate(value, description);
        return value;
    }

    private static string QuoteIdentifier(string value) => SqlIdentifierRules.Quote(value);

    private static string RequireIdentifier(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new ArgumentException("Identifier is required.", parameterName);

        string trimmed = value.Trim();
        if (!IsIdentifier(trimmed))
            throw new ArgumentException($"'{trimmed}' is not a valid CSharpDB identifier.", parameterName);

        return trimmed;
    }

    private static bool IsIdentifier(string value)
    {
        if (value.Length == 0 || !(char.IsLetter(value[0]) || value[0] == '_'))
            return false;

        for (int i = 1; i < value.Length; i++)
        {
            char c = value[i];
            if (!char.IsLetterOrDigit(c) && c != '_')
                return false;
        }

        return true;
    }

    private static string ResolveDatabaseFolder(string dataSource)
    {
        if (string.IsNullOrWhiteSpace(dataSource) ||
            dataSource.StartsWith(":memory:", StringComparison.OrdinalIgnoreCase) ||
            (Uri.TryCreate(dataSource, UriKind.Absolute, out var uri) && !uri.IsFile))
        {
            return Directory.GetCurrentDirectory();
        }

        string path = Uri.TryCreate(dataSource, UriKind.Absolute, out var fileUri) && fileUri.IsFile
            ? fileUri.LocalPath
            : Path.GetFullPath(dataSource);
        string? directory = Path.GetDirectoryName(path);
        return string.IsNullOrWhiteSpace(directory) ? Directory.GetCurrentDirectory() : directory;
    }

    private static string CreateTemporaryArchivePath(string tableName)
    {
        string fileName = $"{SanitizeFileName(tableName)}-{DateTime.Now:yyyyMMdd-HHmmss}-{Guid.NewGuid():N}.csdbtable";
        string directory = Path.Combine(Path.GetTempPath(), "csharpdb-admin-exports");
        Directory.CreateDirectory(directory);
        return Path.Combine(directory, fileName);
    }

    private static string SanitizeFileName(string value)
    {
        char[] invalid = Path.GetInvalidFileNameChars();
        var builder = new StringBuilder(value.Length);
        foreach (char c in value)
            builder.Append(invalid.Contains(c) ? '_' : c);
        return builder.Length == 0 ? "table" : builder.ToString();
    }
}
