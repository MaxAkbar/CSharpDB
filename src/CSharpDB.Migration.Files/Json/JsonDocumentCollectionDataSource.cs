using System.Collections.ObjectModel;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Stable fail-fast rules raised while replaying JSON collection documents.
/// </summary>
public static class JsonDocumentCollectionDataRules
{
    public const string ValueSizeExceeded =
        "MIG-JSON-COLLECTION-VALUE-SIZE-001";

    public const string RowSizeExceeded =
        "MIG-JSON-COLLECTION-ROW-SIZE-001";
}

/// <summary>
/// Replays every complete value in a catalog-bound JSON snapshot as one
/// deterministic document-collection row.
/// </summary>
public sealed class JsonDocumentCollectionDataSource :
    IMigrationDataSource,
    IMigrationCatalogBoundDataSource
{
    private const int MaximumBufferedRows = 65_536;
    private const long MaximumBufferedCanonicalBytes =
        64L * 1024 * 1024;
    private const int MaximumCursorCharacters = 192;
    private const string CursorTokenAlgorithm =
        "csharpdb-json-collection-cursor-token/v1";

    private readonly JsonDocumentCollectionProjectionResult
        projection;
    private readonly JsonSourceSnapshot snapshot;
    private int disposed;

    private JsonDocumentCollectionDataSource(
        JsonDocumentCollectionProjectionResult projection,
        JsonSourceSnapshot snapshot,
        string catalogDigest)
    {
        this.projection = projection;
        this.snapshot = snapshot;
        CatalogDigest = catalogDigest;
    }

    public MigrationSourceIdentity Source => projection.Source;

    public string SnapshotIdentity =>
        projection.SnapshotIdentity;

    public string CatalogDigest { get; }

    public static async ValueTask<
        JsonDocumentCollectionDataSource> CreateAsync(
        JsonDocumentCollectionProjectionResult projection,
        JsonSourceSnapshot snapshot,
        MigrationCatalog catalog,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string catalogDigest = ValidateCatalogBinding(
            projection,
            snapshot,
            catalog);
        cancellationToken.ThrowIfCancellationRequested();
        await snapshot
            .VerifyIntegrityAsync(cancellationToken)
            .ConfigureAwait(false);
        return new JsonDocumentCollectionDataSource(
            projection,
            snapshot,
            catalogDigest);
    }

    internal static JsonDocumentCollectionDataSource
        CreateFromVerifiedSnapshot(
            JsonDocumentCollectionProjectionResult projection,
            JsonSourceSnapshot snapshot,
            MigrationCatalog catalog) =>
        new(
            projection,
            snapshot,
            ValidateCatalogBinding(
                projection,
                snapshot,
                catalog));

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
        Interlocked.Exchange(ref disposed, 1);
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    internal static string ValidateCatalogBinding(
        JsonDocumentCollectionProjectionResult projection,
        JsonSourceSnapshot snapshot,
        MigrationCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(projection);
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(catalog);

        JsonSourceBinding binding = projection.Binding;
        if (!string.Equals(
                binding.SnapshotIdentity,
                snapshot.SnapshotIdentity,
                StringComparison.Ordinal) ||
            !string.Equals(
                binding.ContentDigest,
                snapshot.ContentDigest,
                StringComparison.Ordinal) ||
            binding.ContentLength != snapshot.ContentLength)
        {
            throw new ArgumentException(
                "The JSON collection projection belongs to a different source snapshot.",
                nameof(snapshot));
        }

        MigrationContractValidator.ValidateCatalog(catalog);
        if (catalog.Source != projection.Source)
        {
            throw new ArgumentException(
                "The JSON collection catalog belongs to a different source.",
                nameof(catalog));
        }

        MigrationCatalog expectedCatalog =
            projection.CreateCatalog(
                catalog.TargetCSharpDbVersion);
        string catalogDigest =
            MigrationArtifactSerializer
                .ComputeCatalogDigest(catalog);
        string expectedCatalogDigest =
            MigrationArtifactSerializer
                .ComputeCatalogDigest(expectedCatalog);
        if (!string.Equals(
                catalogDigest,
                expectedCatalogDigest,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The JSON collection catalog does not match the exact projection policy.",
                nameof(catalog));
        }

        return catalogDigest;
    }

    private async IAsyncEnumerable<MigrationDataBatch>
        ReadCoreAsync(
            ValidatedRead request,
            [EnumeratorCancellation]
            CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref disposed) != 0,
            this);
        cancellationToken.ThrowIfCancellationRequested();

        await using JsonStreamingReader reader = await projection
            .Binding
            .OpenReaderAsync(snapshot, cancellationToken)
            .ConfigureAwait(false);

        long expectedRowOrdinal = 0;
        long batchOrdinal = 0;
        long batchStartRowOrdinal = 0;
        long batchBytes = 0;
        bool resumeBoundaryFound = request.Resume is null;
        var rows = NewRowBuffer(request.EffectiveMaximumRows);

        await foreach (JsonLogicalRecord record in reader
                           .ReadValuesAsync(cancellationToken)
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            long sourceRowOrdinal =
                checked(record.RecordOrdinal - 1);
            if (sourceRowOrdinal != expectedRowOrdinal)
            {
                throw new InvalidDataException(
                    "JSON collection source row ordinals are not contiguous.");
            }

            if (rows.Count >= request.EffectiveMaximumRows ||
                batchBytes == request.EffectiveMaximumBatchBytes)
            {
                MigrationDataBatch completed = CreateBatch(
                    request,
                    rows,
                    batchStartRowOrdinal,
                    batchOrdinal,
                    EncodeCursor(
                        sourceRowOrdinal,
                        checked(batchOrdinal + 1),
                        request.ScopeDigest));
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

            string key =
                MigrationDocumentCollectionContract
                    .FormatOrdinalKey(sourceRowOrdinal);
            byte[] documentBytes =
                JsonCanonicalValueSerializer
                    .SerializeToUtf8Bytes(
                        record.Value,
                        cancellationToken);
            if (documentBytes.LongLength >
                request.MaximumValueBytes)
            {
                throw new InvalidDataException(
                    $"{JsonDocumentCollectionDataRules.ValueSizeExceeded}: the canonical JSON document exceeds the bound value size.");
            }

            long keyBytes = Encoding.UTF8.GetByteCount(key);
            if (keyBytes > request.MaximumValueBytes)
            {
                throw new InvalidDataException(
                    $"{JsonDocumentCollectionDataRules.ValueSizeExceeded}: the canonical JSON key exceeds the bound value size.");
            }

            long rowBytes = checked(
                keyBytes + documentBytes.LongLength);
            if (rowBytes >
                request.EffectiveMaximumBatchBytes)
            {
                throw new InvalidDataException(
                    $"{JsonDocumentCollectionDataRules.RowSizeExceeded}: one JSON collection row exceeds the bounded batch payload.");
            }

            if (rows.Count > 0 &&
                checked(batchBytes + rowBytes) >
                    request.EffectiveMaximumBatchBytes)
            {
                MigrationDataBatch completed = CreateBatch(
                    request,
                    rows,
                    batchStartRowOrdinal,
                    batchOrdinal,
                    EncodeCursor(
                        sourceRowOrdinal,
                        checked(batchOrdinal + 1),
                        request.ScopeDigest));
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

            string document = Encoding.UTF8.GetString(
                documentBytes);
            rows.Add(CreateRow(key, document));
            batchBytes = checked(batchBytes + rowBytes);
            expectedRowOrdinal = checked(
                expectedRowOrdinal + 1);
        }

        if (expectedRowOrdinal != projection.TotalRecords)
        {
            throw new InvalidDataException(
                "The JSON collection replay record count does not match the bound projection.");
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
            if (resume.RowOrdinal == expectedRowOrdinal &&
                resume.BatchOrdinal == batchOrdinal)
            {
                yield break;
            }

            throw new InvalidDataException(
                "The JSON collection resume cursor does not identify a batch boundary in this snapshot.");
        }
    }

    private ValidatedRead Validate(MigrationReadRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!string.Equals(
                request.SourceObjectId,
                JsonDocumentCollectionObjectIds.Collection,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The JSON collection source object identifier is not supported.",
                nameof(request));
        }
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
                "The JSON collection projection supports fail-fast reads only.");
        }

        if (request.SnapshotToken is not null &&
            !string.Equals(
                request.SnapshotToken,
                SnapshotIdentity,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The JSON collection read snapshot token does not match the bound snapshot.");
        }
        if (request.ColumnObjectIds is null ||
            request.ColumnObjectIds.Count != 2 ||
            !string.Equals(
                request.ColumnObjectIds[0],
                JsonDocumentCollectionObjectIds.KeyColumn,
                StringComparison.Ordinal) ||
            !string.Equals(
                request.ColumnObjectIds[1],
                JsonDocumentCollectionObjectIds
                    .DocumentColumn,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The JSON collection projection requires the exact key-then-document row bridge.",
                nameof(request));
        }

        ReadOnlyCollection<string> frozenColumnIds =
            Array.AsReadOnly(
            [
                JsonDocumentCollectionObjectIds.KeyColumn,
                JsonDocumentCollectionObjectIds.DocumentColumn,
            ]);
        long effectiveMaximumBatchBytes = Math.Min(
            request.MaxBatchBytes,
            MaximumBufferedCanonicalBytes);
        int maximumValueBytes = checked((int)Math.Min(
            request.MaxValueBytes,
            effectiveMaximumBatchBytes));
        string scopeDigest = ComputeScopeDigest(
            request,
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
                    "A JSON collection resume cursor requires the exact bound snapshot token.");
            }

            resume = ParseCursor(
                request.ResumeCursor,
                scopeDigest);
        }

        return new ValidatedRead(
            frozenColumnIds,
            Math.Min(
                request.BatchSize,
                MaximumBufferedRows),
            effectiveMaximumBatchBytes,
            maximumValueBytes,
            scopeDigest,
            resume);
    }

    private string ComputeScopeDigest(
        MigrationReadRequest request,
        IReadOnlyList<string> columnObjectIds)
    {
        var components = new List<string?>(
            24 + columnObjectIds.Count)
        {
            MigrationDocumentCollectionContract.CursorContract,
            CursorTokenAlgorithm,
            Source.Fingerprint,
            SnapshotIdentity,
            CatalogDigest,
            JsonDocumentCollectionObjectIds.Collection,
            projection.CollectionName,
            projection.Binding.OptionsDigest,
            MigrationDocumentCollectionContract
                .ProjectionContract,
            MigrationDocumentCollectionContract.SchemaContract,
            MigrationDocumentCollectionContract.RowContract,
            MigrationDocumentCollectionContract.KeyContract,
            MigrationDocumentCollectionContract
                .DocumentEncoding,
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
        return JsonStableDigest.Compute(
            components.ToArray());
    }

    private MigrationDataBatch CreateBatch(
        ValidatedRead request,
        List<MigrationDataRow> rows,
        long startRowOrdinal,
        long batchOrdinal,
        string? nextCursor) =>
        new()
        {
            SourceObjectId =
                JsonDocumentCollectionObjectIds.Collection,
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

    private static MigrationDataRow CreateRow(
        string key,
        string document)
    {
        MigrationSourceValue[] values =
        [
            new()
            {
                Kind = MigrationSourceValueKind.Text,
                CanonicalText = key,
            },
            new()
            {
                Kind = MigrationSourceValueKind.Json,
                CanonicalText = document,
            },
        ];

        return new MigrationDataRow
        {
            StableKey = key,
            Values = Array.AsReadOnly(values),
        };
    }

    private string EncodeCursor(
        long rowOrdinal,
        long batchOrdinal,
        string scopeDigest) =>
        string.Join(
            '/',
            MigrationDocumentCollectionContract
                .CursorContract,
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
            MigrationDocumentCollectionContract
                .CursorContract + "/";
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
            parts[2].Any(character =>
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
                "The JSON collection cursor scope digest is invalid.");
        }

        string digest = JsonStableDigest.Compute(
            CursorTokenAlgorithm,
            scopeDigest,
            rowOrdinal.ToString(
                CultureInfo.InvariantCulture),
            batchOrdinal.ToString(
                CultureInfo.InvariantCulture));
        return digest[7..];
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
                "JSON collection resume state is inconsistent.");
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
            "The JSON collection resume cursor is malformed or does not match this read policy.");

    private static List<MigrationDataRow> NewRowBuffer(
        int maximumRows) =>
        new(Math.Min(maximumRows, 1_024));

    private sealed record ValidatedRead(
        ReadOnlyCollection<string> ColumnObjectIds,
        int EffectiveMaximumRows,
        long EffectiveMaximumBatchBytes,
        int MaximumValueBytes,
        string ScopeDigest,
        CursorPosition? Resume);

    private sealed record CursorPosition(
        string Original,
        long RowOrdinal,
        long BatchOrdinal);
}
