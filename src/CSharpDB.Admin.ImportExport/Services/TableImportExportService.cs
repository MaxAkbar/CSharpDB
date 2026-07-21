using System.Collections;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using CSharpDB.Admin.ImportExport.Contracts;
using CSharpDB.Client;
using CSharpDB.ImportExport.TableArchives;
using TableArchiveExportProgress = CSharpDB.Client.Models.TableArchiveExportProgress;
using ClientColumnDefinition = CSharpDB.Client.Models.ColumnDefinition;
using ClientDbType = CSharpDB.Client.Models.DbType;
using ClientForeignKeyDefinition = CSharpDB.Client.Models.ForeignKeyDefinition;
using ClientForeignKeyOnDeleteAction = CSharpDB.Client.Models.ForeignKeyOnDeleteAction;
using ClientTableSchema = CSharpDB.Client.Models.TableSchema;
using PrimitiveColumnDefinition = CSharpDB.Primitives.ColumnDefinition;
using PrimitiveDbType = CSharpDB.Primitives.DbType;
using PrimitiveDbValue = CSharpDB.Primitives.DbValue;
using PrimitiveForeignKeyDefinition = CSharpDB.Primitives.ForeignKeyDefinition;
using PrimitiveForeignKeyOnDeleteAction = CSharpDB.Primitives.ForeignKeyOnDeleteAction;
using PrimitiveTableSchema = CSharpDB.Primitives.TableSchema;

namespace CSharpDB.Admin.ImportExport.Services;

public sealed class TableImportExportService(
    ICSharpDbClient client,
    ITableArchiveDownloadStore downloads) : ITableImportExportService
{
    private const int ExportPageSize = 1_000;
    private const int RestoreInsertBatchSize = 100;

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
            var manifest = await TableArchiveWriter.WriteAsync(
                path,
                schema,
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

        PrimitiveTableSchema archiveSchema = await TableArchiveReader.ReadTableSchemaAsync(request.ArchivePath, ct: ct);
        bool regeneratesRowVersionTokens = archiveSchema.Columns.Any(static column => column.IsRowVersion);

        string targetTableName = string.IsNullOrWhiteSpace(request.TargetTableName)
            ? RequireIdentifier(archiveSchema.TableName, nameof(request.TargetTableName))
            : RequireIdentifier(request.TargetTableName, nameof(request.TargetTableName));

        var restoreSchema = new PrimitiveTableSchema
        {
            TableName = targetTableName,
            Columns = archiveSchema.Columns,
            ForeignKeys = archiveSchema.ForeignKeys,
            NextRowId = archiveSchema.NextRowId,
        };

        await ExecuteCheckedAsync(BuildCreateTableSql(restoreSchema), ct);

        long inserted = 0;
        var batch = new List<PrimitiveDbValue[]>(RestoreInsertBatchSize);
        await foreach (PrimitiveDbValue[] row in TableArchiveReader.ReadRowsAsync(request.ArchivePath, ct))
        {
            batch.Add(row);
            if (batch.Count >= RestoreInsertBatchSize)
            {
                inserted += await InsertBatchAsync(targetTableName, archiveSchema.Columns, batch, ct);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
            inserted += await InsertBatchAsync(targetTableName, archiveSchema.Columns, batch, ct);

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
                var insertResult = await client.ExecuteSqlAsync($"INSERT INTO {tableName} DEFAULT VALUES;", ct);
                if (!string.IsNullOrWhiteSpace(insertResult.Error))
                    throw new InvalidOperationException(insertResult.Error);
                inserted += insertResult.RowsAffected;
            }

            return inserted;
        }

        var sql = new StringBuilder();
        sql.Append("INSERT INTO ").Append(tableName).Append(" (");
        for (int i = 0; i < insertColumnIndexes.Length; i++)
        {
            if (i > 0)
                sql.Append(", ");
            sql.Append(columns[insertColumnIndexes[i]].Name);
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
                PrimitiveDbValue value = columnIndex < rows[rowIndex].Length
                    ? rows[rowIndex][columnIndex]
                    : PrimitiveDbValue.Null;
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

    private static PrimitiveTableSchema MapSchema(ClientTableSchema schema) => new()
    {
        TableName = schema.TableName,
        Columns = schema.Columns.Select(MapColumn).ToArray(),
        ForeignKeys = schema.ForeignKeys.Select(MapForeignKey).ToArray(),
        NextRowId = 1,
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

    private static PrimitiveDbValue MapValue(ClientDbType columnType, object? value)
    {
        if (value is null)
            return PrimitiveDbValue.Null;

        return columnType switch
        {
            ClientDbType.Integer => PrimitiveDbValue.FromInteger(Convert.ToInt64(value, CultureInfo.InvariantCulture)),
            ClientDbType.Real => PrimitiveDbValue.FromReal(Convert.ToDouble(value, CultureInfo.InvariantCulture)),
            ClientDbType.Text => PrimitiveDbValue.FromText(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty),
            ClientDbType.Blob => PrimitiveDbValue.FromBlob(ConvertToBytes(value)),
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

    private static string BuildCreateTableSql(PrimitiveTableSchema schema)
    {
        var sql = new StringBuilder();
        sql.Append("CREATE TABLE ").Append(RequireIdentifier(schema.TableName, nameof(schema.TableName))).Append(" (");
        for (int i = 0; i < schema.Columns.Count; i++)
        {
            PrimitiveColumnDefinition column = schema.Columns[i];
            if (i > 0)
                sql.Append(", ");

            sql.Append(RequireIdentifier(column.Name, nameof(column.Name)))
                .Append(' ')
                .Append(column.Type.ToString().ToUpperInvariant());

            if (column.IsRowVersion)
                sql.Append(" ROWVERSION");
            if (column.IsPrimaryKey)
                sql.Append(" PRIMARY KEY");
            if (column.IsIdentity)
                sql.Append(" IDENTITY");
            if (!column.Nullable && !column.IsPrimaryKey)
                sql.Append(" NOT NULL");
            if (!string.IsNullOrWhiteSpace(column.Collation))
                sql.Append(" COLLATE ").Append(RequireIdentifier(column.Collation, nameof(column.Collation)));
        }

        sql.Append(");");
        return sql.ToString();
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
