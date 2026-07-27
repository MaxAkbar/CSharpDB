using System.Collections.ObjectModel;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using CSharpDB.Migration;
using Microsoft.Data.Sqlite;

namespace CSharpDB.Migration.Sqlite;

public static class SqliteMigrationDataRules
{
    public const string NullNotAllowed = "MIG-SQLITE-DATA-NULL-001";

    public const string StorageClassMismatch = "MIG-SQLITE-DATA-TYPE-001";

    public const string NonFiniteReal = "MIG-SQLITE-DATA-REAL-001";

    public const string ValueSizeExceeded = "MIG-SQLITE-DATA-VALUE-SIZE-001";

    public const string RowSizeExceeded = "MIG-SQLITE-DATA-ROW-SIZE-001";

    public const string InvalidTextEncoding = "MIG-SQLITE-DATA-TEXT-001";
}

/// <summary>
/// Streams a catalog-bound retained SQLite backup in deterministic rowid order.
/// Only Tier 1 ordinary rowid tables and visible scalar columns are accepted.
/// </summary>
public sealed class SqliteMigrationDataSource :
    IMigrationDataSource,
    IMigrationCatalogBoundDataSource
{
    private const int MaximumBufferedRows = 65_536;
    private const int MaximumBufferedScalarValues = 65_536;
    private const long MaximumBufferedCanonicalBytes = 64L * 1024 * 1024;

    private readonly SqliteBackupSnapshot snapshot;
    private readonly IReadOnlyDictionary<string, MigrationCatalogObject> objects;
    private readonly object gate = new();
    private int activeReaders;
    private int disposed;
    private Task? disposeTask;
    private TaskCompletionSource? readersDrained;

    private SqliteMigrationDataSource(
        SqliteBackupSnapshot snapshot,
        MigrationCatalog catalog,
        string catalogDigest)
    {
        this.snapshot = snapshot;
        objects = catalog.Objects.ToDictionary(item => item.ObjectId, StringComparer.Ordinal);
        CatalogDigest = catalogDigest;
    }

    public MigrationSourceIdentity Source => snapshot.Source;

    public string SnapshotIdentity => snapshot.SnapshotIdentity;

    public string CatalogDigest { get; }

    public static async ValueTask<SqliteMigrationDataSource> CreateAsync(
        SqliteBackupSnapshot snapshot,
        MigrationCatalog catalog,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(catalog);
        MigrationContractValidator.ValidateCatalog(catalog);
        if (catalog.Source.Kind != MigrationSourceKind.Sqlite ||
            catalog.Source != snapshot.Source)
        {
            throw new ArgumentException(
                "The SQLite migration catalog belongs to a different retained snapshot.",
                nameof(catalog));
        }

        MigrationCatalogObject? mainNamespace = catalog.Objects.SingleOrDefault(
            item => item.Kind == MigrationObjectKind.Namespace &&
                string.Equals(item.SourceName, "main", StringComparison.Ordinal));
        if (mainNamespace is null ||
            !string.Equals(
                Facet(mainNamespace, "sqliteCatalogContract"),
                "csharpdb-sqlite-catalog-v1",
                StringComparison.Ordinal) ||
            !string.Equals(
                Facet(mainNamespace, "sqliteTextEncoding"),
                "UTF-8",
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The SQLite migration catalog contract is unsupported.",
                nameof(catalog));
        }

        await using SqliteConnection verification =
            await snapshot.OpenVerifiedReadOnlyConnectionAsync(cancellationToken)
                .ConfigureAwait(false);
        string digest = MigrationArtifactSerializer.ComputeCatalogDigest(catalog);
        return new SqliteMigrationDataSource(snapshot, catalog, digest);
    }

    public IAsyncEnumerable<MigrationDataBatch> ReadAsync(
        MigrationReadRequest request,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);
        ValidatedRead validated = Validate(request);
        return ReadCoreAsync(validated, cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        lock (gate)
        {
            disposeTask ??= DisposeCoreAsync();
            return new ValueTask(disposeTask);
        }
    }

    private async Task DisposeCoreAsync()
    {
        Task readersCompleted;
        lock (gate)
        {
            Volatile.Write(ref disposed, 1);
            readersCompleted = activeReaders == 0
                ? Task.CompletedTask
                : (readersDrained ??= new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously)).Task;
        }

        await readersCompleted.ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    private void AcquireReader()
    {
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(disposed != 0, this);
            activeReaders++;
        }
    }

    private void ReleaseReader()
    {
        TaskCompletionSource? completed = null;
        lock (gate)
        {
            if (activeReaders <= 0)
            {
                throw new InvalidOperationException(
                    "The SQLite source reader lease is not active.");
            }

            activeReaders--;
            if (disposed != 0 && activeReaders == 0)
                completed = readersDrained;
        }

        completed?.TrySetResult();
    }

    private async IAsyncEnumerable<MigrationDataBatch> ReadCoreAsync(
        ValidatedRead request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        AcquireReader();
        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            await using SqliteConnection connection =
                await snapshot.OpenVerifiedReadOnlyConnectionAsync(cancellationToken)
                    .ConfigureAwait(false);
            await using SqliteTransaction transaction =
                connection.BeginTransaction(deferred: true);
            if (request.Resume is not null)
            {
                await ValidateResumeBoundaryAsync(
                        connection,
                        transaction,
                        request,
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            await using SqliteCommand command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = BuildReadSql(request);
            if (request.Resume is not null)
                command.Parameters.AddWithValue("$afterRowId", request.Resume.LastRowId);

            await using SqliteDataReader reader =
                await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

            var rows = NewBuffer(request.EffectiveMaximumRows);
            long batchBytes = 0;
            long batchOrdinal = request.Resume?.BatchOrdinal ?? 0;
            string? startCursor = request.Resume?.Original;
            long lastBufferedRowId = 0;
            bool hasBufferedRowId = false;
            long sourceRowOrdinal = request.Resume?.SourceRowOrdinal ?? 0;

            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                long rowId = reader.GetInt64(0);
                NormalizedRow normalized = NormalizeRow(
                    reader,
                    request,
                    batchOrdinal,
                    sourceRowOrdinal);

                if (rows.Count > 0 &&
                    (rows.Count >= request.EffectiveMaximumRows ||
                     checked(batchBytes + normalized.CanonicalBytes) >
                        request.EffectiveMaximumBatchBytes))
                {
                    string nextCursor = SqliteCursorCodec.Encode(
                        lastBufferedRowId,
                        checked(batchOrdinal + 1),
                        sourceRowOrdinal,
                        request.ScopeDigest);
                    yield return CreateBatch(
                        request,
                        rows,
                        batchOrdinal,
                        startCursor,
                        nextCursor);

                    rows = NewBuffer(request.EffectiveMaximumRows);
                    batchBytes = 0;
                    batchOrdinal++;
                    startCursor = nextCursor;
                    hasBufferedRowId = false;
                }

                rows.Add(normalized.Row);
                batchBytes = checked(batchBytes + normalized.CanonicalBytes);
                lastBufferedRowId = rowId;
                hasBufferedRowId = true;
                sourceRowOrdinal++;
            }

            if (rows.Count > 0)
            {
                if (!hasBufferedRowId)
                    throw new InvalidDataException("SQLite rowid buffering became inconsistent.");
                yield return CreateBatch(
                    request,
                    rows,
                    batchOrdinal,
                    startCursor,
                    nextCursor: null);
            }
        }
        finally
        {
            ReleaseReader();
        }
    }

    private async ValueTask ValidateResumeBoundaryAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ValidatedRead request,
        CancellationToken cancellationToken)
    {
        SqliteCursorCodec.Position resume = request.Resume ??
            throw new InvalidOperationException(
                "A SQLite resume boundary cannot be validated without a cursor.");
        await using SqliteCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = BuildReadSql(request with { Resume = null });
        await using SqliteDataReader reader =
            await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        int rowsInBatch = 0;
        long batchBytes = 0;
        long batchOrdinal = 0;
        long sourceRowOrdinal = 0;
        long lastRowId = 0;
        bool hasRowId = false;

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            long rowId = reader.GetInt64(0);
            NormalizedRow normalized = NormalizeRow(
                reader,
                request,
                batchOrdinal,
                sourceRowOrdinal);
            if (rowsInBatch > 0 &&
                (rowsInBatch >= request.EffectiveMaximumRows ||
                 checked(batchBytes + normalized.CanonicalBytes) >
                    request.EffectiveMaximumBatchBytes))
            {
                batchOrdinal++;
                if (hasRowId && lastRowId == resume.LastRowId)
                {
                    if (batchOrdinal != resume.BatchOrdinal ||
                        sourceRowOrdinal != resume.SourceRowOrdinal)
                    {
                        throw InvalidResumeBoundary();
                    }

                    return;
                }

                rowsInBatch = 0;
                batchBytes = 0;
                hasRowId = false;
            }

            rowsInBatch++;
            batchBytes = checked(batchBytes + normalized.CanonicalBytes);
            lastRowId = rowId;
            hasRowId = true;
            sourceRowOrdinal++;
        }

        throw InvalidResumeBoundary();
    }

    private ValidatedRead Validate(MigrationReadRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.BatchSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(request), "The batch size must be positive.");
        if (request.MaxBatchBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "The maximum batch bytes must be positive.");
        }
        if (request.MaxValueBytes <= 0 || request.MaxValueBytes > request.MaxBatchBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "The maximum value bytes must be positive and no greater than the batch bound.");
        }
        MigrationRejectReadPolicyValidator.Validate(request);
        if (!string.Equals(
                request.RejectContractVersion,
                MigrationRejectContract.DeterministicFailFastV1,
                StringComparison.Ordinal))
        {
            throw new NotSupportedException(
                "The SQLite MVP source supports deterministic fail-fast replay only.");
        }
        if (request.SnapshotToken is not null &&
            !string.Equals(request.SnapshotToken, SnapshotIdentity, StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The SQLite read request snapshot token does not match the retained backup.");
        }
        if (!objects.TryGetValue(request.SourceObjectId, out MigrationCatalogObject? table) ||
            table.Kind != MigrationObjectKind.Table ||
            !string.Equals(Facet(table, "sqliteTableType"), "table", StringComparison.Ordinal) ||
            IsTrue(table, "sqliteWithoutRowId"))
        {
            throw new ArgumentException(
                "The SQLite source object is not a supported ordinary rowid table.",
                nameof(request));
        }

        string? rowIdAlias = Facet(table, "sqliteRowIdAlias");
        if (rowIdAlias is not ("rowid" or "_rowid_" or "oid"))
        {
            throw new ArgumentException(
                "The SQLite table does not expose a deterministic rowid cursor.",
                nameof(request));
        }
        if (request.ColumnObjectIds is null || request.ColumnObjectIds.Count == 0)
        {
            throw new ArgumentException(
                "At least one SQLite column must be requested.",
                nameof(request));
        }

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var columns = new ProjectedColumn[request.ColumnObjectIds.Count];
        var columnIds = new string[request.ColumnObjectIds.Count];
        for (int index = 0; index < request.ColumnObjectIds.Count; index++)
        {
            string? objectId = request.ColumnObjectIds[index];
            if (string.IsNullOrWhiteSpace(objectId) ||
                !seen.Add(objectId) ||
                !objects.TryGetValue(objectId, out MigrationCatalogObject? column) ||
                column.Kind != MigrationObjectKind.Column ||
                !string.Equals(column.ParentObjectId, table.ObjectId, StringComparison.Ordinal) ||
                !string.Equals(Facet(column, "sqliteHidden"), "0", StringComparison.Ordinal))
            {
                throw new ArgumentException(
                    "The SQLite column projection contains an unknown, duplicate, or unsupported identifier.",
                    nameof(request));
            }

            string logicalType = Facet(column, "logicalType") ?? string.Empty;
            string expectedStorageClass = logicalType switch
            {
                "signedInteger" => "integer",
                "floatingPoint" => "real",
                "text" => "text",
                "binary" => "blob",
                _ => throw new ArgumentException(
                    "The SQLite column projection contains an unsupported logical type.",
                    nameof(request)),
            };
            string[] observed = (Facet(column, "sqliteStorageClasses") ?? string.Empty)
                .Split(',', StringSplitOptions.RemoveEmptyEntries);
            if (observed.Length > 1 ||
                (observed.Length == 1 &&
                 !string.Equals(observed[0], expectedStorageClass, StringComparison.Ordinal)))
            {
                throw new ArgumentException(
                    "The SQLite column projection is not backed by one supported storage class.",
                    nameof(request));
            }

            columnIds[index] = objectId;
            columns[index] = new ProjectedColumn(
                objectId,
                column.SourceName,
                expectedStorageClass,
                IsTrue(column, "nullable"));
        }

        int maximumRowsByScalarCount = Math.Max(
            1,
            MaximumBufferedScalarValues / columns.Length);
        int maximumRows = Math.Min(
            request.BatchSize,
            Math.Min(MaximumBufferedRows, maximumRowsByScalarCount));
        long maximumBatchBytes = Math.Min(
            request.MaxBatchBytes,
            MaximumBufferedCanonicalBytes);
        int maximumValueBytes = checked((int)Math.Min(
            request.MaxValueBytes,
            maximumBatchBytes));
        ReadOnlyCollection<string> frozenIds = Array.AsReadOnly(columnIds);
        string scopeDigest = SqliteCursorCodec.ComputeScope(
            Source.Fingerprint,
            SnapshotIdentity,
            CatalogDigest,
            table.ObjectId,
            frozenIds,
            request.BatchSize,
            request.MaxBatchBytes,
            request.MaxValueBytes);

        SqliteCursorCodec.Position? resume = null;
        if (request.ResumeCursor is not null)
        {
            if (!string.Equals(request.SnapshotToken, SnapshotIdentity, StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    "A SQLite resume cursor requires the exact retained snapshot token.");
            }
            resume = SqliteCursorCodec.Parse(request.ResumeCursor, scopeDigest);
        }

        return new ValidatedRead(
            table.ObjectId,
            table.SourceName,
            rowIdAlias,
            frozenIds,
            columns,
            maximumRows,
            maximumBatchBytes,
            maximumValueBytes,
            scopeDigest,
            resume);
    }

    private static string BuildReadSql(ValidatedRead request)
    {
        var sql = new StringBuilder("SELECT ");
        sql.Append(QuoteIdentifier(request.RowIdAlias));
        foreach (ProjectedColumn column in request.Columns)
        {
            sql.Append(", typeof(");
            sql.Append(QuoteIdentifier(column.SourceName));
            sql.Append("), CASE WHEN typeof(");
            sql.Append(QuoteIdentifier(column.SourceName));
            sql.Append(") IN ('text', 'blob') THEN length(CAST(");
            sql.Append(QuoteIdentifier(column.SourceName));
            sql.Append(" AS BLOB)) ELSE 0 END, ");
            if (string.Equals(
                    column.ExpectedStorageClass,
                    "text",
                    StringComparison.Ordinal))
            {
                sql.Append("CAST(");
                sql.Append(QuoteIdentifier(column.SourceName));
                sql.Append(" AS BLOB)");
            }
            else
            {
                sql.Append(QuoteIdentifier(column.SourceName));
            }
        }
        sql.Append(" FROM ");
        sql.Append(QuoteIdentifier(request.TableSourceName));
        if (request.Resume is not null)
        {
            sql.Append(" WHERE ");
            sql.Append(QuoteIdentifier(request.RowIdAlias));
            sql.Append(" > $afterRowId");
        }
        sql.Append(" ORDER BY ");
        sql.Append(QuoteIdentifier(request.RowIdAlias));
        sql.Append(';');
        return sql.ToString();
    }

    private static NormalizedRow NormalizeRow(
        SqliteDataReader reader,
        ValidatedRead request,
        long batchOrdinal,
        long sourceRowOrdinal)
    {
        var values = new MigrationSourceValue[request.Columns.Length];
        long rowBytes = 0;
        for (int index = 0; index < request.Columns.Length; index++)
        {
            ProjectedColumn column = request.Columns[index];
            int typeOrdinal = checked(1 + index * 3);
            int lengthOrdinal = checked(typeOrdinal + 1);
            int valueOrdinal = checked(typeOrdinal + 2);
            string storageClass = reader.GetString(typeOrdinal);

            MigrationSourceValue value;
            long valueBytes;
            switch (storageClass)
            {
                case "null":
                    if (!column.Nullable)
                    {
                        throw Reject(
                            SqliteMigrationDataRules.NullNotAllowed,
                            request.TableObjectId,
                            column.ObjectId,
                            batchOrdinal,
                            sourceRowOrdinal);
                    }
                    value = new MigrationSourceValue
                    {
                        Kind = MigrationSourceValueKind.Null,
                    };
                    valueBytes = 1;
                    break;

                case "integer" when column.ExpectedStorageClass == "integer":
                    value = new MigrationSourceValue
                    {
                        Kind = MigrationSourceValueKind.SignedInteger,
                        CanonicalText = reader.GetInt64(valueOrdinal)
                            .ToString(CultureInfo.InvariantCulture),
                    };
                    valueBytes = 9;
                    break;

                case "real" when column.ExpectedStorageClass == "real":
                    double real = reader.GetDouble(valueOrdinal);
                    if (!double.IsFinite(real))
                    {
                        throw Reject(
                            SqliteMigrationDataRules.NonFiniteReal,
                            request.TableObjectId,
                            column.ObjectId,
                            batchOrdinal,
                            sourceRowOrdinal);
                    }
                    value = new MigrationSourceValue
                    {
                        Kind = MigrationSourceValueKind.FloatingPoint,
                        CanonicalText = real.ToString("R", CultureInfo.InvariantCulture),
                    };
                    valueBytes = 9;
                    break;

                case "text" when column.ExpectedStorageClass == "text":
                    long textByteLength =
                        reader.GetInt64(lengthOrdinal);
                    if (textByteLength > request.MaximumValueBytes - 5L)
                    {
                        throw Reject(
                            SqliteMigrationDataRules.ValueSizeExceeded,
                            request.TableObjectId,
                            column.ObjectId,
                            batchOrdinal,
                            sourceRowOrdinal);
                    }
                    byte[] textBytes =
                        reader.GetFieldValue<byte[]>(valueOrdinal);
                    string text;
                    try
                    {
                        text = StrictUtf8.GetString(textBytes);
                    }
                    catch (DecoderFallbackException)
                    {
                        throw Reject(
                            SqliteMigrationDataRules.InvalidTextEncoding,
                            request.TableObjectId,
                            column.ObjectId,
                            batchOrdinal,
                            sourceRowOrdinal);
                    }
                    value = new MigrationSourceValue
                    {
                        Kind = MigrationSourceValueKind.Text,
                        CanonicalText = text,
                    };
                    valueBytes = checked(5L + textBytes.Length);
                    break;

                case "blob" when column.ExpectedStorageClass == "blob":
                    long blobLength = reader.GetInt64(lengthOrdinal);
                    if (blobLength > request.MaximumValueBytes - 5L)
                    {
                        throw Reject(
                            SqliteMigrationDataRules.ValueSizeExceeded,
                            request.TableObjectId,
                            column.ObjectId,
                            batchOrdinal,
                            sourceRowOrdinal);
                    }
                    byte[] bytes = reader.GetFieldValue<byte[]>(valueOrdinal);
                    value = new MigrationSourceValue
                    {
                        Kind = MigrationSourceValueKind.Binary,
                        BinaryValue = bytes,
                    };
                    valueBytes = checked(5L + bytes.Length);
                    break;

                default:
                    throw Reject(
                        SqliteMigrationDataRules.StorageClassMismatch,
                        request.TableObjectId,
                        column.ObjectId,
                        batchOrdinal,
                        sourceRowOrdinal);
            }

            if (valueBytes > request.MaximumValueBytes)
            {
                throw Reject(
                    SqliteMigrationDataRules.ValueSizeExceeded,
                    request.TableObjectId,
                    column.ObjectId,
                    batchOrdinal,
                    sourceRowOrdinal);
            }
            rowBytes = checked(rowBytes + valueBytes);
            if (rowBytes > request.EffectiveMaximumBatchBytes)
            {
                throw Reject(
                    SqliteMigrationDataRules.RowSizeExceeded,
                    request.TableObjectId,
                    column.ObjectId,
                    batchOrdinal,
                    sourceRowOrdinal);
            }
            values[index] = value;
        }

        return new NormalizedRow(
            new MigrationDataRow
            {
                StableKey = null,
                Values = Array.AsReadOnly(values),
            },
            rowBytes);
    }

    private MigrationDataBatch CreateBatch(
        ValidatedRead request,
        List<MigrationDataRow> rows,
        long batchOrdinal,
        string? startCursor,
        string? nextCursor) => new()
        {
            SourceObjectId = request.TableObjectId,
            SnapshotIdentity = SnapshotIdentity,
            ColumnObjectIds = request.ColumnObjectIds,
            BatchOrdinal = batchOrdinal,
            StartCursor = startCursor,
            NextCursor = nextCursor,
            Rows = rows.AsReadOnly(),
        };

    private static MigrationRowRejectedException Reject(
        string ruleId,
        string tableObjectId,
        string columnObjectId,
        long batchOrdinal,
        long sourceRowOrdinal) => MigrationRowRejectedException.CreateForSource(
            ruleId,
            tableObjectId,
            columnObjectId,
            batchOrdinal,
            sourceRowOrdinal);

    private static InvalidDataException InvalidResumeBoundary() => new(
        "The SQLite resume cursor does not identify an emitted batch boundary.");

    private static string QuoteIdentifier(string identifier) =>
        "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private static string? Facet(MigrationCatalogObject item, string name) =>
        item.Facets.FirstOrDefault(
            facet => string.Equals(facet.Name, name, StringComparison.Ordinal))?.Value;

    private static bool IsTrue(MigrationCatalogObject item, string name) =>
        string.Equals(Facet(item, name), "true", StringComparison.Ordinal);

    private static List<MigrationDataRow> NewBuffer(int maximumRows) =>
        new(Math.Min(maximumRows, 1_024));

    private static UTF8Encoding StrictUtf8 { get; } =
        new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private sealed record ProjectedColumn(
        string ObjectId,
        string SourceName,
        string ExpectedStorageClass,
        bool Nullable);

    private sealed record ValidatedRead(
        string TableObjectId,
        string TableSourceName,
        string RowIdAlias,
        ReadOnlyCollection<string> ColumnObjectIds,
        ProjectedColumn[] Columns,
        int EffectiveMaximumRows,
        long EffectiveMaximumBatchBytes,
        int MaximumValueBytes,
        string ScopeDigest,
        SqliteCursorCodec.Position? Resume);

    private sealed record NormalizedRow(
        MigrationDataRow Row,
        long CanonicalBytes);
}
