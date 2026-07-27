using System.Buffers.Binary;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration;
using LiteDB;

namespace CSharpDB.Migration.LiteDb;

/// <summary>
/// Stable fail-fast rules raised while replaying LiteDB collection documents.
/// </summary>
public static class LiteDbMigrationDataRules
{
    public const string InvalidDocumentId =
        "MIG-LITEDB-DATA-ID-001";

    public const string ValueSizeExceeded =
        "MIG-LITEDB-DATA-VALUE-SIZE-001";

    public const string RowSizeExceeded =
        "MIG-LITEDB-DATA-ROW-SIZE-001";

    public const string OrderingViolation =
        "MIG-LITEDB-DATA-ORDER-001";
}

/// <summary>
/// Replays exact LiteDB document-collection projections from a retained,
/// content-pinned snapshot in ascending built-in <c>_id</c> index order.
/// </summary>
public sealed class LiteDbMigrationDataSource :
    IMigrationDataSource,
    IMigrationCatalogBoundDataSource
{
    private const int MaximumBufferedRows = 65_536;
    private const long MaximumBufferedCanonicalBytes =
        64L * 1024 * 1024;
    private const int MaximumCursorCharacters = 192;
    private const string CursorTokenAlgorithm =
        "csharpdb-litedb-collection-cursor-token/v1";
    private const string ScopeDigestAlgorithm =
        "csharpdb-litedb-collection-scope-digest/v1";

    private static readonly UTF8Encoding StrictUtf8 =
        new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private readonly LiteDbRetainedSnapshot snapshot;
    private readonly IReadOnlyDictionary<string, CollectionBinding> collections;
    private readonly object gate = new();
    private int activeReaders;
    private int disposed;
    private Task? disposeTask;
    private TaskCompletionSource? readersDrained;

    private LiteDbMigrationDataSource(
        LiteDbRetainedSnapshot snapshot,
        IReadOnlyDictionary<string, CollectionBinding> collections,
        string catalogDigest)
    {
        this.snapshot = snapshot;
        this.collections = collections;
        CatalogDigest = catalogDigest;
    }

    public MigrationSourceIdentity Source => snapshot.Source;

    public string SnapshotIdentity => snapshot.SnapshotIdentity;

    public string CatalogDigest { get; }

    /// <summary>
    /// Binds replay to an exact retained LiteDB snapshot and inspected catalog.
    /// The caller retains ownership of <paramref name="snapshot"/>.
    /// </summary>
    public static async ValueTask<LiteDbMigrationDataSource> CreateAsync(
        LiteDbRetainedSnapshot snapshot,
        MigrationCatalog catalog,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(catalog);
        cancellationToken.ThrowIfCancellationRequested();

        MigrationContractValidator.ValidateCatalog(catalog);
        if (catalog.Source.Kind != MigrationSourceKind.LiteDb ||
            catalog.Source != snapshot.Source)
        {
            throw new ArgumentException(
                "The LiteDB migration catalog belongs to a different retained snapshot.",
                nameof(catalog));
        }

        IReadOnlyDictionary<string, MigrationCatalogObject> objects =
            new ReadOnlyDictionary<string, MigrationCatalogObject>(
                catalog.Objects.ToDictionary(
                    static item => item.ObjectId,
                    StringComparer.Ordinal));
        ValidateNamespaceContract(catalog);
        IReadOnlyDictionary<string, CollectionBinding> collections =
            BindCollections(catalog, objects);

        using LiteDatabase verification =
            await snapshot
                .OpenVerifiedReadOnlyDatabaseAsync(cancellationToken)
                .ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();

        return new LiteDbMigrationDataSource(
            snapshot,
            collections,
            MigrationArtifactSerializer.ComputeCatalogDigest(catalog));
    }

    public IAsyncEnumerable<MigrationDataBatch> ReadAsync(
        MigrationReadRequest request,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref disposed) != 0,
            this);
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
                    "The LiteDB source reader lease is not active.");
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
            using LiteDatabase database =
                await snapshot
                    .OpenVerifiedReadOnlyDatabaseAsync(cancellationToken)
                    .ConfigureAwait(false);
            ILiteCollection<BsonDocument> collection =
                database.GetCollection(
                    request.CollectionSourceName,
                    BsonAutoId.ObjectId);

            long sourceRowOrdinal = 0;
            long batchOrdinal = 0;
            long batchStartRowOrdinal = 0;
            long batchBytes = 0;
            bool resumeBoundaryFound = request.Resume is null;
            BsonValue? previousId = null;
            var rows = NewRowBuffer(request.EffectiveMaximumRows);

            IEnumerable<BsonDocument> documents = collection.Find(
                Query.All("_id", Query.Ascending),
                skip: 0,
                limit: int.MaxValue);
            foreach (BsonDocument document in documents)
            {
                cancellationToken.ThrowIfCancellationRequested();
                BsonValue id = GetAndValidateId(
                    document,
                    previousId,
                    database.Collation);
                previousId = id;

                NormalizedRow normalized = NormalizeRow(
                    document,
                    id,
                    request);
                if (rows.Count > 0 &&
                    (rows.Count >= request.EffectiveMaximumRows ||
                     checked(batchBytes + normalized.CanonicalBytes) >
                        request.EffectiveMaximumBatchBytes))
                {
                    string nextCursor = EncodeCursor(
                        sourceRowOrdinal,
                        checked(batchOrdinal + 1),
                        request.ScopeDigest);
                    MigrationDataBatch completed = CreateBatch(
                        request,
                        rows,
                        batchStartRowOrdinal,
                        batchOrdinal,
                        nextCursor);
                    if (ShouldYield(
                            completed,
                            request.Resume,
                            ref resumeBoundaryFound))
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        yield return completed;
                    }

                    batchOrdinal = checked(batchOrdinal + 1);
                    batchStartRowOrdinal = sourceRowOrdinal;
                    batchBytes = 0;
                    rows = NewRowBuffer(
                        request.EffectiveMaximumRows);
                }

                rows.Add(normalized.Row);
                batchBytes = checked(
                    batchBytes + normalized.CanonicalBytes);
                sourceRowOrdinal = checked(sourceRowOrdinal + 1);
            }

            if (sourceRowOrdinal != request.ExpectedDocumentCount)
            {
                throw new InvalidDataException(
                    "The LiteDB collection replay count does not match the bound catalog.");
            }

            if (rows.Count > 0)
            {
                MigrationDataBatch final = CreateBatch(
                    request,
                    rows,
                    batchStartRowOrdinal,
                    batchOrdinal,
                    nextCursor: null);
                if (ShouldYield(
                        final,
                        request.Resume,
                        ref resumeBoundaryFound))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    yield return final;
                }

                batchOrdinal = checked(batchOrdinal + 1);
            }

            if (request.Resume is CursorPosition resume &&
                !resumeBoundaryFound)
            {
                if (resume.RowOrdinal == sourceRowOrdinal &&
                    resume.BatchOrdinal == batchOrdinal)
                {
                    yield break;
                }

                throw new InvalidDataException(
                    "The LiteDB resume cursor does not identify an emitted batch boundary in this snapshot.");
            }
        }
        finally
        {
            ReleaseReader();
        }
    }

    private ValidatedRead Validate(MigrationReadRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.BatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "The batch size must be positive.");
        }
        if (request.MaxBatchBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "The maximum batch bytes must be positive.");
        }
        if (request.MaxValueBytes <= 0 ||
            request.MaxValueBytes > request.MaxBatchBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "The maximum value bytes must be positive and no greater than the batch bound.");
        }

        MigrationRejectReadPolicyValidator.Validate(request);
        if (!string.Equals(
                request.RejectContractVersion,
                MigrationRejectContract.DeterministicFailFastV1,
                StringComparison.Ordinal) ||
            request.RejectPolicy is not null)
        {
            throw new NotSupportedException(
                "The LiteDB document projection supports deterministic fail-fast replay only.");
        }
        if (request.SnapshotToken is not null &&
            !string.Equals(
                request.SnapshotToken,
                SnapshotIdentity,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The LiteDB read request snapshot token does not match the retained snapshot.");
        }
        if (!collections.TryGetValue(
                request.SourceObjectId,
                out CollectionBinding? collection))
        {
            throw new ArgumentException(
                "The LiteDB source object is not an exact supported document collection.",
                nameof(request));
        }
        IReadOnlyList<string> requestedColumnIds =
            request.ColumnObjectIds ?? [];
        bool keyFirst = requestedColumnIds.Count == 2 &&
            string.Equals(
                requestedColumnIds[0],
                collection.KeyColumnObjectId,
                StringComparison.Ordinal) &&
            string.Equals(
                requestedColumnIds[1],
                collection.DocumentColumnObjectId,
                StringComparison.Ordinal);
        bool documentFirst =
            requestedColumnIds.Count == 2 &&
            string.Equals(
                requestedColumnIds[0],
                collection.DocumentColumnObjectId,
                StringComparison.Ordinal) &&
            string.Equals(
                requestedColumnIds[1],
                collection.KeyColumnObjectId,
                StringComparison.Ordinal);
        if (!keyFirst && !documentFirst)
        {
            throw new ArgumentException(
                "The LiteDB collection requires exactly its bound key and document bridge columns.",
                nameof(request));
        }

        ReadOnlyCollection<string> frozenColumnIds =
            Array.AsReadOnly(
            [
                requestedColumnIds[0],
                requestedColumnIds[1],
            ]);
        long effectiveMaximumBatchBytes = Math.Min(
            request.MaxBatchBytes,
            MaximumBufferedCanonicalBytes);
        int maximumValueBytes = checked((int)Math.Min(
            request.MaxValueBytes,
            effectiveMaximumBatchBytes));
        string scopeDigest = ComputeScopeDigest(
            request,
            collection,
            frozenColumnIds);

        CursorPosition? resume = null;
        if (request.ResumeCursor is not null)
        {
            if (!string.Equals(
                    request.SnapshotToken,
                    SnapshotIdentity,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    "A LiteDB resume cursor requires the exact retained snapshot token.");
            }

            resume = ParseCursor(
                request.ResumeCursor,
                scopeDigest);
        }

        return new ValidatedRead(
            collection.CollectionObjectId,
            collection.CollectionSourceName,
            collection.ExpectedDocumentCount,
            frozenColumnIds,
            Math.Min(
                request.BatchSize,
                MaximumBufferedRows),
            effectiveMaximumBatchBytes,
            maximumValueBytes,
            keyFirst,
            scopeDigest,
            resume);
    }

    private string ComputeScopeDigest(
        MigrationReadRequest request,
        CollectionBinding collection,
        IReadOnlyList<string> columnObjectIds)
    {
        var components = new List<string?>(
            28 + columnObjectIds.Count)
        {
            ScopeDigestAlgorithm,
            MigrationLiteDbDocumentCollectionContract.CursorContract,
            CursorTokenAlgorithm,
            Source.Identity,
            Source.Fingerprint,
            snapshot.ContentDigest,
            SnapshotIdentity,
            CatalogDigest,
            collection.CollectionObjectId,
            collection.CollectionSourceName,
            collection.ExpectedDocumentCount.ToString(
                CultureInfo.InvariantCulture),
            MigrationLiteDbDocumentCollectionContract
                .ProjectionContract,
            MigrationLiteDbDocumentCollectionContract.SchemaContract,
            MigrationLiteDbDocumentCollectionContract.RowContract,
            MigrationLiteDbDocumentCollectionContract.KeyContract,
            MigrationLiteDbDocumentCollectionContract.DocumentEncoding,
            request.BatchSize.ToString(
                CultureInfo.InvariantCulture),
            request.MaxBatchBytes.ToString(
                CultureInfo.InvariantCulture),
            request.MaxValueBytes.ToString(
                CultureInfo.InvariantCulture),
            request.RejectContractVersion,
            "reject-policy:none",
            columnObjectIds.Count.ToString(
                CultureInfo.InvariantCulture),
        };
        components.AddRange(columnObjectIds);
        return ComputeStableDigest(components);
    }

    private NormalizedRow NormalizeRow(
        BsonDocument document,
        BsonValue id,
        ValidatedRead request)
    {
        string key = LiteDbCanonicalBsonCodec.EncodeTypedKey(id);
        string canonicalDocument =
            LiteDbCanonicalBsonCodec.EncodeDocument(document);
        long keyBytes = StrictUtf8.GetByteCount(key);
        long documentBytes =
            StrictUtf8.GetByteCount(canonicalDocument);

        if (keyBytes > request.MaximumValueBytes ||
            documentBytes > request.MaximumValueBytes)
        {
            throw new InvalidDataException(
                $"{LiteDbMigrationDataRules.ValueSizeExceeded}: a canonical LiteDB collection value exceeds the requested value bound.");
        }

        long rowBytes = checked(keyBytes + documentBytes);
        if (rowBytes > request.EffectiveMaximumBatchBytes)
        {
            throw new InvalidDataException(
                $"{LiteDbMigrationDataRules.RowSizeExceeded}: one LiteDB collection row exceeds the bounded batch payload.");
        }

        MigrationSourceValue keyValue = new()
        {
            Kind = MigrationSourceValueKind.Text,
            CanonicalText = key,
        };
        MigrationSourceValue documentValue = new()
        {
            Kind = MigrationSourceValueKind.Json,
            CanonicalText = canonicalDocument,
        };
        MigrationSourceValue[] values = request.KeyFirst
            ?
        [
            keyValue,
            documentValue,
        ]
            :
        [
            documentValue,
            keyValue,
        ];
        return new NormalizedRow(
            new MigrationDataRow
            {
                StableKey = key,
                Values = Array.AsReadOnly(values),
            },
            rowBytes);
    }

    private static BsonValue GetAndValidateId(
        BsonDocument document,
        BsonValue? previousId,
        Collation collation)
    {
        if (!document.TryGetValue("_id", out BsonValue? id) ||
            id is null ||
            id.IsNull ||
            id.IsDocument ||
            id.IsArray)
        {
            throw new InvalidDataException(
                $"{LiteDbMigrationDataRules.InvalidDocumentId}: a LiteDB document does not have a non-null scalar _id.");
        }
        if (previousId is not null &&
            previousId.CompareTo(id, collation) >= 0)
        {
            throw new InvalidDataException(
                $"{LiteDbMigrationDataRules.OrderingViolation}: LiteDB _id replay order is not strictly ascending.");
        }

        return id;
    }

    private MigrationDataBatch CreateBatch(
        ValidatedRead request,
        List<MigrationDataRow> rows,
        long startRowOrdinal,
        long batchOrdinal,
        string? nextCursor) =>
        new()
        {
            SourceObjectId = request.CollectionObjectId,
            SnapshotIdentity = SnapshotIdentity,
            ColumnObjectIds = request.ColumnObjectIds,
            BatchOrdinal = batchOrdinal,
            StartCursor = batchOrdinal == 0
                ? null
                : EncodeCursor(
                    startRowOrdinal,
                    batchOrdinal,
                    request.ScopeDigest),
            NextCursor = nextCursor,
            Rows = rows.AsReadOnly(),
            RejectedRows = [],
        };

    private string EncodeCursor(
        long rowOrdinal,
        long batchOrdinal,
        string scopeDigest) =>
        string.Join(
            '/',
            MigrationLiteDbDocumentCollectionContract.CursorContract,
            rowOrdinal.ToString(CultureInfo.InvariantCulture),
            batchOrdinal.ToString(
                CultureInfo.InvariantCulture),
            ComputeCursorToken(
                scopeDigest,
                rowOrdinal,
                batchOrdinal));

    private CursorPosition ParseCursor(
        string cursor,
        string expectedScopeDigest)
    {
        string prefix =
            MigrationLiteDbDocumentCollectionContract.CursorContract +
            "/";
        if (cursor.Length > MaximumCursorCharacters ||
            !cursor.StartsWith(
                prefix,
                StringComparison.Ordinal))
        {
            throw InvalidCursor();
        }

        string[] parts = cursor[prefix.Length..].Split('/');
        if (parts.Length != 3 ||
            !TryParseCanonicalInt64(
                parts[0],
                out long rowOrdinal) ||
            !TryParseCanonicalInt64(
                parts[1],
                out long batchOrdinal) ||
            (rowOrdinal == 0 && batchOrdinal == 0) ||
            parts[2].Length != 64 ||
            parts[2].Any(static character =>
                character is not (>= '0' and <= '9') and
                    not (>= 'a' and <= 'f')) ||
            !string.Equals(
                parts[2],
                ComputeCursorToken(
                    expectedScopeDigest,
                    rowOrdinal,
                    batchOrdinal),
                StringComparison.Ordinal))
        {
            throw InvalidCursor();
        }

        return new CursorPosition(
            cursor,
            rowOrdinal,
            batchOrdinal);
    }

    private static string ComputeCursorToken(
        string scopeDigest,
        long rowOrdinal,
        long batchOrdinal)
    {
        if (scopeDigest.Length != 71 ||
            !scopeDigest.StartsWith(
                "sha256:",
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The LiteDB cursor scope digest is invalid.");
        }

        string digest = ComputeStableDigest(
        [
            CursorTokenAlgorithm,
            scopeDigest,
            rowOrdinal.ToString(
                CultureInfo.InvariantCulture),
            batchOrdinal.ToString(
                CultureInfo.InvariantCulture),
        ]);
        return digest[7..];
    }

    private static string ComputeStableDigest(
        IEnumerable<string?> components)
    {
        using IncrementalHash hash =
            IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> length = stackalloc byte[sizeof(int)];
        foreach (string? component in components)
        {
            if (component is null)
            {
                BinaryPrimitives.WriteInt32BigEndian(length, -1);
                hash.AppendData(length);
                continue;
            }

            byte[] bytes = StrictUtf8.GetBytes(component);
            BinaryPrimitives.WriteInt32BigEndian(
                length,
                bytes.Length);
            hash.AppendData(length);
            hash.AppendData(bytes);
        }

        return "sha256:" +
            Convert.ToHexString(hash.GetHashAndReset())
                .ToLowerInvariant();
    }

    private static bool ShouldYield(
        MigrationDataBatch batch,
        CursorPosition? resume,
        ref bool resumeBoundaryFound)
    {
        if (resumeBoundaryFound)
            return true;
        if (resume is null)
        {
            throw new InvalidOperationException(
                "LiteDB resume state is inconsistent.");
        }
        if (batch.BatchOrdinal == resume.BatchOrdinal &&
            batch.StartCursor is not null &&
            string.Equals(
                batch.StartCursor,
                resume.Original,
                StringComparison.Ordinal))
        {
            resumeBoundaryFound = true;
            return true;
        }

        return false;
    }

    private static bool TryParseCanonicalInt64(
        string text,
        out long value)
    {
        value = 0;
        return text.Length > 0 &&
            (text.Length == 1 || text[0] != '0') &&
            long.TryParse(
                text,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out value);
    }

    private static InvalidDataException InvalidCursor() =>
        new(
            "The LiteDB resume cursor is malformed or does not match this source, snapshot, catalog, object, or read policy.");

    private static void ValidateNamespaceContract(
        MigrationCatalog catalog)
    {
        MigrationCatalogObject? main = catalog.Objects.SingleOrDefault(
            static item =>
                item.Kind == MigrationObjectKind.Namespace &&
                string.Equals(
                    item.SourceName,
                    "main",
                    StringComparison.Ordinal));
        if (main is null ||
            !string.Equals(
                Facet(main, "liteDbCatalogContract"),
                LiteDbMigrationSourceInspector.CatalogContract,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The LiteDB migration catalog contract is unsupported.",
                nameof(catalog));
        }
    }

    private static IReadOnlyDictionary<string, CollectionBinding>
        BindCollections(
            MigrationCatalog catalog,
            IReadOnlyDictionary<string, MigrationCatalogObject> objects)
    {
        var result = new Dictionary<string, CollectionBinding>(
            StringComparer.Ordinal);
        foreach (MigrationCatalogObject collection in catalog.Objects.Where(
                     static item =>
                         item.Kind == MigrationObjectKind.Collection))
        {
            if (!MigrationLiteDbDocumentCollectionContract
                    .TryBindExactV1Collection(
                        collection,
                        objects,
                        out MigrationCatalogObject? keyColumn,
                        out MigrationCatalogObject? documentColumn,
                        out string? reason))
            {
                throw new ArgumentException(
                    $"The LiteDB migration catalog contains an unsupported collection projection: {reason}",
                    nameof(catalog));
            }

            string? countText =
                Facet(collection, "liteDbDocumentCount");
            if (!TryParseCanonicalInt64(
                    countText ?? string.Empty,
                    out long expectedDocumentCount))
            {
                throw new ArgumentException(
                    "The LiteDB migration catalog contains an invalid collection document count.",
                    nameof(catalog));
            }

            result.Add(
                collection.ObjectId,
                new CollectionBinding(
                    collection.ObjectId,
                    collection.SourceName,
                    keyColumn!.ObjectId,
                    documentColumn!.ObjectId,
                    expectedDocumentCount));
        }

        return new ReadOnlyDictionary<string, CollectionBinding>(
            result);
    }

    private static string? Facet(
        MigrationCatalogObject item,
        string name) =>
        item.Facets.FirstOrDefault(
            facet => string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal))?.Value;

    private static List<MigrationDataRow> NewRowBuffer(
        int maximumRows) =>
        new(Math.Min(maximumRows, 1_024));

    private sealed record CollectionBinding(
        string CollectionObjectId,
        string CollectionSourceName,
        string KeyColumnObjectId,
        string DocumentColumnObjectId,
        long ExpectedDocumentCount);

    private sealed record ValidatedRead(
        string CollectionObjectId,
        string CollectionSourceName,
        long ExpectedDocumentCount,
        ReadOnlyCollection<string> ColumnObjectIds,
        int EffectiveMaximumRows,
        long EffectiveMaximumBatchBytes,
        int MaximumValueBytes,
        bool KeyFirst,
        string ScopeDigest,
        CursorPosition? Resume);

    private sealed record NormalizedRow(
        MigrationDataRow Row,
        long CanonicalBytes);

    private sealed record CursorPosition(
        string Original,
        long RowOrdinal,
        long BatchOrdinal);
}
