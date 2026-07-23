using System.Collections.Frozen;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Json;

/// <summary>Stable row-local and fatal rules raised while streaming JSON table rows.</summary>
public static class JsonMigrationDataRules
{
    public const string NonObjectRow = "MIG-JSON-DATA-ROW-001";
    public const string MissingProperty = "MIG-JSON-DATA-MISSING-001";
    public const string NullNotAllowed = "MIG-JSON-DATA-NULL-001";
    public const string TypeMismatch = "MIG-JSON-DATA-TYPE-001";
    public const string ValueSizeExceeded = "MIG-JSON-DATA-VALUE-SIZE-001";
    public const string RowSizeExceeded = "MIG-JSON-DATA-ROW-SIZE-001";
    public const string SchemaDrift = "MIG-JSON-DATA-SCHEMA-001";
}

/// <summary>
/// Replays one immutable, catalog-bound JSON snapshot as deterministic
/// relational migration batches. The snapshot remains caller-owned and must
/// outlive this source and every active enumeration.
/// </summary>
public sealed class JsonMigrationDataSource :
    IMigrationDataSource,
    IMigrationCatalogBoundDataSource,
    IMigrationRejectAwareDataSource
{
    public const string CursorAlgorithmId = "csharpdb-json-cursor/v1";

    private const int MaximumBufferedRows = 65_536;
    private const long MaximumBufferedCanonicalBytes = 64L * 1024 * 1024;
    private const int MaximumCursorCharacters = 160;

    private static readonly FrozenSet<string> SupportedRuleIds = new[]
    {
        JsonMigrationDataRules.NonObjectRow,
        JsonMigrationDataRules.MissingProperty,
        JsonMigrationDataRules.NullNotAllowed,
        JsonMigrationDataRules.TypeMismatch,
    }.ToFrozenSet(StringComparer.Ordinal);

    private readonly JsonTableSchemaInferenceResult schema;
    private readonly JsonSourceSnapshot snapshot;
    private readonly FrozenDictionary<string, int> schemaIndexesByName;
    private int disposed;

    private JsonMigrationDataSource(
        JsonTableSchemaInferenceResult schema,
        JsonSourceSnapshot snapshot,
        string catalogDigest)
    {
        this.schema = schema;
        this.snapshot = snapshot;
        CatalogDigest = catalogDigest;
        schemaIndexesByName = schema.Columns.ToFrozenDictionary(
            column => column.OriginalPropertyName,
            column => column.ColumnIndex,
            StringComparer.Ordinal);
    }

    public MigrationSourceIdentity Source => schema.Source;

    public string SnapshotIdentity => schema.SnapshotIdentity;

    public string CatalogDigest { get; }

    public string RejectContractVersion =>
        MigrationRejectContract.DeterministicRejectsV1;

    public IReadOnlySet<string> SupportedRejectRuleIds => SupportedRuleIds;

    /// <summary>
    /// Creates a repeatable source after checking the exact snapshot and
    /// byte-for-byte catalog policy bound to the inference result.
    /// </summary>
    public static async ValueTask<JsonMigrationDataSource> CreateAsync(
        JsonTableSchemaInferenceResult schema,
        JsonSourceSnapshot snapshot,
        MigrationCatalog catalog,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string catalogDigest = ValidateCatalogBinding(schema, snapshot, catalog);
        cancellationToken.ThrowIfCancellationRequested();
        await snapshot.VerifyIntegrityAsync(cancellationToken).ConfigureAwait(false);
        return new JsonMigrationDataSource(schema, snapshot, catalogDigest);
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
        Interlocked.Exchange(ref disposed, 1);
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    private static string ValidateCatalogBinding(
        JsonTableSchemaInferenceResult schema,
        JsonSourceSnapshot snapshot,
        MigrationCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(catalog);

        JsonSourceBinding binding = schema.Binding;
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
                "The JSON table schema belongs to a different source snapshot.",
                nameof(snapshot));
        }

        MigrationContractValidator.ValidateCatalog(catalog);
        if (catalog.Source != schema.Source)
        {
            throw new ArgumentException(
                "The JSON migration catalog belongs to a different source.",
                nameof(catalog));
        }

        MigrationCatalog expectedCatalog =
            schema.CreateCatalog(catalog.TargetCSharpDbVersion);
        string catalogDigest =
            MigrationArtifactSerializer.ComputeCatalogDigest(catalog);
        string expectedCatalogDigest =
            MigrationArtifactSerializer.ComputeCatalogDigest(expectedCatalog);
        if (!string.Equals(
                catalogDigest,
                expectedCatalogDigest,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The JSON migration catalog does not match the table-schema policy.",
                nameof(catalog));
        }

        return catalogDigest;
    }

    private async IAsyncEnumerable<MigrationDataBatch> ReadCoreAsync(
        ValidatedRead request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();

        await using JsonStreamingReader reader = await schema.Binding
            .OpenReaderAsync(snapshot, cancellationToken)
            .ConfigureAwait(false);

        long expectedRowOrdinal = 0;
        long batchOrdinal = 0;
        long batchStartRowOrdinal = 0;
        long batchBytes = 0;
        long batchRawValueBytes = 0;
        long rejectedRowsInRun = 0;
        long rawValueBytesInRun = 0;
        long artifactBytesInRun = request.RejectPolicy is null
            ? 0
            : MigrationRejectLedgerCodec.MinimumCanonicalArtifactBytes;
        bool resumeBoundaryFound = request.Resume is null;
        var rows = NewRowBuffer(request.EffectiveMaximumRows);
        var rejectedRows = NewRejectBuffer(request.EffectiveMaximumRows);
        var indexedValues = new JsonLogicalValue?[schema.Columns.Count];
        var propertyOrdinals = new int[schema.Columns.Count];

        await foreach (JsonLogicalRecord record in reader
                           .ReadValuesAsync(cancellationToken)
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            long sourceRowOrdinal = checked(record.RecordOrdinal - 1);
            if (sourceRowOrdinal != expectedRowOrdinal)
            {
                throw new InvalidDataException(
                    "JSON source row ordinals are not contiguous.");
            }

            int outcomeCount = checked(rows.Count + rejectedRows.Count);
            if (outcomeCount >= request.EffectiveMaximumRows ||
                batchBytes == request.EffectiveMaximumBatchBytes)
            {
                string nextCursor = EncodeCursor(
                    sourceRowOrdinal,
                    checked(batchOrdinal + 1),
                    request.ScopeDigest);
                MigrationDataBatch completed = CreateBatch(
                    request,
                    rows,
                    rejectedRows,
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
                batchRawValueBytes = 0;
                rows = NewRowBuffer(request.EffectiveMaximumRows);
                rejectedRows = NewRejectBuffer(request.EffectiveMaximumRows);
                outcomeCount = 0;
            }

            NormalizedOutcome normalized = Normalize(
                record,
                request,
                batchOrdinal,
                sourceRowOrdinal,
                indexedValues,
                propertyOrdinals,
                cancellationToken);

            long outcomeBytes = GetOutcomeCanonicalBytes(
                normalized,
                batchOrdinal);
            if (outcomeBytes > request.EffectiveMaximumBatchBytes)
            {
                throw new InvalidDataException(
                    "A JSON row outcome exceeds the bounded batch payload.");
            }

            bool splitForRejectPolicy = normalized.RejectedRow is not null &&
                outcomeCount > 0 &&
                (rejectedRows.Count >=
                    request.RejectPolicy!.MaxRejectedRowsPerBatch ||
                 checked(batchRawValueBytes + normalized.RawValueBytes) >
                    request.RejectPolicy.MaxRawValueBytesPerBatch);
            if (outcomeCount > 0 &&
                (checked(batchBytes + outcomeBytes) >
                    request.EffectiveMaximumBatchBytes ||
                 splitForRejectPolicy))
            {
                string nextCursor = EncodeCursor(
                    sourceRowOrdinal,
                    checked(batchOrdinal + 1),
                    request.ScopeDigest);
                MigrationDataBatch completed = CreateBatch(
                    request,
                    rows,
                    rejectedRows,
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
                batchRawValueBytes = 0;
                rows = NewRowBuffer(request.EffectiveMaximumRows);
                rejectedRows = NewRejectBuffer(request.EffectiveMaximumRows);
                outcomeBytes = GetOutcomeCanonicalBytes(
                    normalized,
                    batchOrdinal);
                if (outcomeBytes > request.EffectiveMaximumBatchBytes)
                {
                    throw new InvalidDataException(
                        "A JSON row outcome exceeds the bounded batch payload.");
                }
            }

            if (normalized.RejectedRow is MigrationRejectedRow rejectedRow)
            {
                MigrationDeterministicRejectPolicy policy =
                    request.RejectPolicy ??
                    throw new InvalidOperationException(
                        "JSON deterministic reject state is inconsistent.");
                if (normalized.RawValueBytes > policy.MaxRawValueBytes)
                    throw RejectLimitExceeded("per-row raw-value byte");
                if (rejectedRows.Count >= policy.MaxRejectedRowsPerBatch)
                    throw RejectLimitExceeded("per-batch rejected-row");
                if (checked(
                        batchRawValueBytes +
                        normalized.RawValueBytes) >
                    policy.MaxRawValueBytesPerBatch)
                {
                    throw RejectLimitExceeded(
                        "per-batch raw-value byte");
                }
                if (checked(rejectedRowsInRun + 1) >
                    policy.MaxRejectedRowsPerRun)
                {
                    throw RejectLimitExceeded("per-run rejected-row");
                }
                if (checked(
                        rawValueBytesInRun +
                        normalized.RawValueBytes) >
                    policy.MaxRawValueBytesPerRun)
                {
                    throw RejectLimitExceeded("per-run raw-value byte");
                }
                if (checked(artifactBytesInRun + outcomeBytes) >
                    policy.MaxArtifactBytes)
                {
                    throw RejectLimitExceeded("reject-artifact byte");
                }

                rejectedRows.Add(rejectedRow);
                batchRawValueBytes = checked(
                    batchRawValueBytes +
                    normalized.RawValueBytes);
                rejectedRowsInRun = checked(rejectedRowsInRun + 1);
                rawValueBytesInRun = checked(
                    rawValueBytesInRun +
                    normalized.RawValueBytes);
                artifactBytesInRun = checked(
                    artifactBytesInRun +
                    outcomeBytes);
            }
            else
            {
                rows.Add(normalized.Row ??
                    throw new InvalidOperationException(
                        "JSON normalized row outcome is inconsistent."));
            }

            batchBytes = checked(batchBytes + outcomeBytes);
            expectedRowOrdinal = checked(expectedRowOrdinal + 1);
        }

        if (rows.Count > 0 || rejectedRows.Count > 0)
        {
            MigrationDataBatch final = CreateBatch(
                request,
                rows,
                rejectedRows,
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
                "The JSON resume cursor does not identify a batch boundary in this snapshot.");
        }
    }

    private ValidatedRead Validate(MigrationReadRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!string.Equals(
                request.SourceObjectId,
                JsonMigrationObjectIds.Table,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The JSON source object identifier is not supported.",
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
        MigrationDeterministicRejectPolicy? rejectPolicy = null;
        IReadOnlySet<string> allowedRejectRuleIds =
            FrozenSet<string>.Empty;
        if (string.Equals(
                request.RejectContractVersion,
                MigrationRejectContract.DeterministicRejectsV1,
                StringComparison.Ordinal))
        {
            MigrationDeterministicRejectPolicy supplied =
                request.RejectPolicy ??
                throw new InvalidDataException(
                    "JSON deterministic reject replay requires a reject policy.");
            string[] frozenRuleIds = supplied.AllowedRuleIds
                .OrderBy(ruleId => ruleId, StringComparer.Ordinal)
                .ToArray();
            foreach (string ruleId in frozenRuleIds)
            {
                if (!SupportedRuleIds.Contains(ruleId))
                {
                    throw new InvalidDataException(
                        "The JSON reject policy contains a rule that this source does not support.");
                }
            }

            rejectPolicy = supplied with
            {
                AllowedRuleIds = Array.AsReadOnly(frozenRuleIds),
            };
            allowedRejectRuleIds =
                frozenRuleIds.ToFrozenSet(StringComparer.Ordinal);
        }

        if (request.SnapshotToken is not null &&
            !string.Equals(
                request.SnapshotToken,
                SnapshotIdentity,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The JSON read request snapshot token does not match the bound snapshot.");
        }
        if (request.ColumnObjectIds is null ||
            request.ColumnObjectIds.Count == 0)
        {
            throw new ArgumentException(
                "At least one JSON column must be requested.",
                nameof(request));
        }
        if (request.ColumnObjectIds.Count > schema.Columns.Count)
        {
            throw new ArgumentException(
                "The JSON column projection contains too many entries.",
                nameof(request));
        }

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var projected =
            new ProjectedColumn[request.ColumnObjectIds.Count];
        var columnObjectIds =
            new string[request.ColumnObjectIds.Count];
        for (int index = 0;
             index < request.ColumnObjectIds.Count;
             index++)
        {
            string? objectId = request.ColumnObjectIds[index];
            if (string.IsNullOrWhiteSpace(objectId) ||
                !seen.Add(objectId) ||
                !JsonMigrationObjectIds.TryParseColumn(
                    objectId,
                    out int schemaIndex) ||
                (uint)schemaIndex >= (uint)schema.Columns.Count)
            {
                throw new ArgumentException(
                    "The JSON column projection contains an unknown, duplicate, or noncanonical identifier.",
                    nameof(request));
            }

            columnObjectIds[index] = objectId;
            projected[index] = new ProjectedColumn(
                objectId,
                schemaIndex,
                schema.Columns[schemaIndex]);
        }

        ReadOnlyCollection<string> frozenColumnIds =
            Array.AsReadOnly(columnObjectIds);
        string scopeDigest =
            ComputeScopeDigest(request, projected, rejectPolicy);
        CursorPosition? resume = null;
        if (request.ResumeCursor is not null)
        {
            if (!string.Equals(
                    request.SnapshotToken,
                    SnapshotIdentity,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    "A JSON resume cursor requires the exact bound snapshot token.");
            }

            resume = ParseCursor(
                request.ResumeCursor,
                scopeDigest);
        }

        long effectiveMaximumBatchBytes = Math.Min(
            request.MaxBatchBytes,
            MaximumBufferedCanonicalBytes);
        return new ValidatedRead(
            frozenColumnIds,
            projected,
            Math.Min(request.BatchSize, MaximumBufferedRows),
            effectiveMaximumBatchBytes,
            checked((int)Math.Min(
                request.MaxValueBytes,
                effectiveMaximumBatchBytes)),
            scopeDigest,
            resume,
            rejectPolicy,
            allowedRejectRuleIds);
    }

    private NormalizedOutcome Normalize(
        JsonLogicalRecord record,
        ValidatedRead request,
        long batchOrdinal,
        long sourceRowOrdinal,
        JsonLogicalValue?[] indexedValues,
        int[] propertyOrdinals,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (record.Value.Kind != JsonLogicalValueKind.Object)
        {
            return RecoverableReject(
                JsonMigrationDataRules.NonObjectRow,
                projected: null,
                record,
                record.Value,
                propertyOrdinal: null,
                request,
                batchOrdinal,
                sourceRowOrdinal,
                cancellationToken);
        }

        foreach (JsonLogicalProperty property in record.Value.Properties)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!schemaIndexesByName.TryGetValue(
                    property.Name,
                    out int schemaIndex))
            {
                throw SchemaDrift();
            }

            indexedValues[schemaIndex] = property.Value;
            propertyOrdinals[schemaIndex] = property.Ordinal;
        }

        try
        {
            var values =
                new MigrationSourceValue[request.Columns.Length];
            long rowBytes = 0;
            for (int outputIndex = 0;
                 outputIndex < request.Columns.Length;
                 outputIndex++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ProjectedColumn projected =
                    request.Columns[outputIndex];
                JsonLogicalValue? logicalValue =
                    indexedValues[projected.SchemaIndex];
                if (logicalValue is null)
                {
                    if (projected.Schema.MissingPolicy ==
                        JsonMissingPropertyPolicy.AsNull)
                    {
                        values[outputIndex] = NullValue();
                        rowBytes = checked(rowBytes + 1);
                        if (rowBytes >
                            request.EffectiveMaximumBatchBytes)
                        {
                            throw Reject(
                                JsonMigrationDataRules.RowSizeExceeded,
                                projected.ObjectId,
                                batchOrdinal,
                                sourceRowOrdinal);
                        }
                        continue;
                    }

                    return RecoverableReject(
                        JsonMigrationDataRules.MissingProperty,
                        projected,
                        record,
                        value: null,
                        propertyOrdinal: null,
                        request,
                        batchOrdinal,
                        sourceRowOrdinal,
                        cancellationToken);
                }

                int propertyOrdinal =
                    propertyOrdinals[projected.SchemaIndex];
                if (logicalValue.Kind ==
                    JsonLogicalValueKind.Null)
                {
                    if (!projected.Schema.Nullable)
                    {
                        return RecoverableReject(
                            JsonMigrationDataRules.NullNotAllowed,
                            projected,
                            record,
                            logicalValue,
                            propertyOrdinal,
                            request,
                            batchOrdinal,
                            sourceRowOrdinal,
                            cancellationToken);
                    }

                    values[outputIndex] = NullValue();
                    rowBytes = checked(rowBytes + 1);
                    if (rowBytes >
                        request.EffectiveMaximumBatchBytes)
                    {
                        throw Reject(
                            JsonMigrationDataRules.RowSizeExceeded,
                            projected.ObjectId,
                            batchOrdinal,
                            sourceRowOrdinal);
                    }
                    continue;
                }

                if (!JsonTableScalarPolicy.IsCompatible(
                        logicalValue,
                        projected.Schema.LogicalType,
                        cancellationToken))
                {
                    return RecoverableReject(
                        JsonMigrationDataRules.TypeMismatch,
                        projected,
                        record,
                        logicalValue,
                        propertyOrdinal,
                        request,
                        batchOrdinal,
                        sourceRowOrdinal,
                        cancellationToken);
                }

                ProjectedValue value = ProjectValue(
                    logicalValue,
                    projected,
                    request,
                    batchOrdinal,
                    sourceRowOrdinal,
                    cancellationToken);
                rowBytes = checked(
                    rowBytes +
                    value.CanonicalBatchBytes);
                if (rowBytes >
                    request.EffectiveMaximumBatchBytes)
                {
                    throw Reject(
                        JsonMigrationDataRules.RowSizeExceeded,
                        projected.ObjectId,
                        batchOrdinal,
                        sourceRowOrdinal);
                }
                values[outputIndex] = value.Value;
            }

            return new NormalizedOutcome(
                new MigrationDataRow
                {
                    StableKey = null,
                    Values = Array.AsReadOnly(values),
                },
                RejectedRow: null,
                AcceptedCanonicalBytes: rowBytes,
                RawValueBytes: 0);
        }
        finally
        {
            foreach (JsonLogicalProperty property in
                     record.Value.Properties)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (schemaIndexesByName.TryGetValue(
                        property.Name,
                        out int schemaIndex))
                {
                    indexedValues[schemaIndex] = null;
                    propertyOrdinals[schemaIndex] = 0;
                }
            }
        }
    }

    private static ProjectedValue ProjectValue(
        JsonLogicalValue logicalValue,
        ProjectedColumn projected,
        ValidatedRead request,
        long batchOrdinal,
        long sourceRowOrdinal,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        MigrationSourceValueKind sourceKind;
        string canonicalText;
        long canonicalValueBytes;

        switch (projected.Schema.LogicalType)
        {
            case JsonTableColumnLogicalType.Text:
                sourceKind = MigrationSourceValueKind.Text;
                canonicalText = logicalValue.StringValue;
                canonicalValueBytes = checked(
                    5L +
                    Utf8ByteCount(
                        canonicalText,
                        cancellationToken));
                break;

            case JsonTableColumnLogicalType.Boolean:
                sourceKind = MigrationSourceValueKind.Boolean;
                canonicalText = logicalValue.BooleanValue
                    ? "true"
                    : "false";
                canonicalValueBytes = checked(
                    5L + canonicalText.Length);
                break;

            case JsonTableColumnLogicalType.SignedInteger:
                sourceKind =
                    MigrationSourceValueKind.SignedInteger;
                canonicalText = logicalValue.NumberLexeme;
                canonicalValueBytes = checked(
                    5L + canonicalText.Length);
                break;

            case JsonTableColumnLogicalType.UnsignedInteger:
                sourceKind =
                    MigrationSourceValueKind.UnsignedInteger;
                canonicalText = logicalValue.NumberLexeme;
                canonicalValueBytes = checked(
                    5L + canonicalText.Length);
                break;

            case JsonTableColumnLogicalType.Decimal:
                sourceKind = MigrationSourceValueKind.Decimal;
                canonicalText = logicalValue.NumberLexeme;
                canonicalValueBytes = checked(
                    5L + canonicalText.Length);
                break;

            case JsonTableColumnLogicalType.Json:
                sourceKind = MigrationSourceValueKind.Text;
                long canonicalJsonBytes =
                    JsonTableScalarPolicy
                        .GetCanonicalUtf8ByteCount(
                            logicalValue,
                            cancellationToken);
                canonicalValueBytes = checked(
                    5L + canonicalJsonBytes);
                if (canonicalValueBytes >
                    request.MaximumValueBytes)
                {
                    throw Reject(
                        JsonMigrationDataRules.ValueSizeExceeded,
                        projected.ObjectId,
                        batchOrdinal,
                        sourceRowOrdinal);
                }
                canonicalText =
                    JsonCanonicalValueSerializer.SerializeToString(
                        logicalValue,
                        cancellationToken);
                break;

            default:
                throw new ArgumentOutOfRangeException(
                    nameof(projected));
        }

        cancellationToken.ThrowIfCancellationRequested();
        if (canonicalValueBytes >
            request.MaximumValueBytes)
        {
            throw Reject(
                JsonMigrationDataRules.ValueSizeExceeded,
                projected.ObjectId,
                batchOrdinal,
                sourceRowOrdinal);
        }

        long canonicalBatchBytes =
            CanonicalBatchUpperBound(
                projected.Schema.LogicalType,
                canonicalValueBytes);
        return new ProjectedValue(
            new MigrationSourceValue
            {
                Kind = sourceKind,
                CanonicalText = canonicalText,
            },
            canonicalBatchBytes);
    }

    private static NormalizedOutcome RecoverableReject(
        string ruleId,
        ProjectedColumn? projected,
        JsonLogicalRecord record,
        JsonLogicalValue? value,
        int? propertyOrdinal,
        ValidatedRead request,
        long batchOrdinal,
        long sourceRowOrdinal,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (request.RejectPolicy is null ||
            !request.AllowedRejectRuleIds.Contains(ruleId))
        {
            throw Reject(
                ruleId,
                projected?.ObjectId ??
                    JsonMigrationObjectIds.Table,
                batchOrdinal,
                sourceRowOrdinal);
        }

        string? rawValue = null;
        if (value is not null)
        {
            long canonicalBytes =
                JsonTableScalarPolicy.GetCanonicalUtf8ByteCount(
                    value,
                    cancellationToken);
            if (canonicalBytes >
                request.RejectPolicy.MaxRawValueBytes)
            {
                throw RejectLimitExceeded(
                    "per-row raw-value byte");
            }

            rawValue =
                JsonCanonicalValueSerializer.SerializeToString(
                    value,
                    cancellationToken);
        }

        string jsonValueKind =
            value?.Kind.ToString() ?? "Missing";
        var evidence = new List<MigrationRejectEvidence>(9);
        if (projected is not null)
        {
            evidence.Add(new MigrationRejectEvidence
            {
                Name = "columnIndex",
                Value = projected.SchemaIndex.ToString(
                    CultureInfo.InvariantCulture),
            });
        }
        evidence.Add(new MigrationRejectEvidence
        {
            Name = "jsonValueKind",
            Value = jsonValueKind,
        });
        if (projected is not null)
        {
            evidence.Add(new MigrationRejectEvidence
            {
                Name = "propertyOrdinal",
                Value = propertyOrdinal?.ToString(
                    CultureInfo.InvariantCulture),
            });
        }
        evidence.Add(new MigrationRejectEvidence
        {
            Name =
                MigrationRejectLedgerCodec.RawValueEvidenceName,
            Value = rawValue,
        });
        evidence.Add(new MigrationRejectEvidence
        {
            Name = "recordByteLength",
            Value = record.RawByteLength.ToString(
                CultureInfo.InvariantCulture),
        });
        evidence.Add(new MigrationRejectEvidence
        {
            Name = "recordOrdinal",
            Value = record.RecordOrdinal.ToString(
                CultureInfo.InvariantCulture),
        });
        evidence.Add(new MigrationRejectEvidence
        {
            Name = "startByteOffset",
            Value = record.StartByteOffset.ToString(
                CultureInfo.InvariantCulture),
        });
        evidence.Add(new MigrationRejectEvidence
        {
            Name = "startBytePositionInLine",
            Value = record.StartBytePositionInLine.ToString(
                CultureInfo.InvariantCulture),
        });
        evidence.Add(new MigrationRejectEvidence
        {
            Name = "startLineNumber",
            Value = record.StartLineNumber.ToString(
                CultureInfo.InvariantCulture),
        });

        cancellationToken.ThrowIfCancellationRequested();
        var rejectedRow = new MigrationRejectedRow
        {
            SourceRowOrdinal = sourceRowOrdinal,
            RuleId = ruleId,
            ColumnObjectId = projected?.ObjectId,
            Evidence = evidence.AsReadOnly(),
        };
        return new NormalizedOutcome(
            Row: null,
            rejectedRow,
            AcceptedCanonicalBytes: 0,
            RawValueBytes:
                MigrationRejectLedgerCodec.GetRawValueByteCount(
                    rejectedRow));
    }

    private string ComputeScopeDigest(
        MigrationReadRequest request,
        IReadOnlyList<ProjectedColumn> projected,
        MigrationDeterministicRejectPolicy? rejectPolicy)
    {
        var components = new List<string?>(
            32 + schema.Columns.Count * 10 + projected.Count)
        {
            CursorAlgorithmId,
            Source.Fingerprint,
            SnapshotIdentity,
            CatalogDigest,
            JsonMigrationObjectIds.Table,
            schema.TableName,
            JsonSourceSnapshot.IdentityAlgorithm,
            JsonSourceBinding.SourceFingerprintAlgorithm,
            JsonSourceBinding.OptionsAlgorithm,
            schema.Binding.OptionsDigest,
            JsonTableSchemaInferenceResult.AlgorithmId,
            JsonTableSchemaInferenceResult.ScalarPolicyId,
            JsonInputContracts.CanonicalNestedJsonVersion,
            JsonInputContracts.PropertyOrderPolicy,
            JsonInputContracts.NumberLexemePolicy,
            request.BatchSize.ToString(
                CultureInfo.InvariantCulture),
            request.MaxBatchBytes.ToString(
                CultureInfo.InvariantCulture),
            request.MaxValueBytes.ToString(
                CultureInfo.InvariantCulture),
        };

        if (rejectPolicy is not null)
        {
            components.Add(request.RejectContractVersion);
            components.Add(rejectPolicy.ContractVersion);
            components.Add(
                rejectPolicy.AllowedRuleIds.Count.ToString(
                    CultureInfo.InvariantCulture));
            foreach (string ruleId in
                     rejectPolicy.AllowedRuleIds)
            {
                components.Add(ruleId);
            }
            components.Add(
                rejectPolicy.MaxRejectedRowsPerBatch.ToString(
                    CultureInfo.InvariantCulture));
            components.Add(
                rejectPolicy.MaxRejectedRowsPerRun.ToString(
                    CultureInfo.InvariantCulture));
            components.Add(
                rejectPolicy.MaxRawValueBytes.ToString(
                    CultureInfo.InvariantCulture));
            components.Add(
                rejectPolicy.MaxRawValueBytesPerBatch.ToString(
                    CultureInfo.InvariantCulture));
            components.Add(
                rejectPolicy.MaxRawValueBytesPerRun.ToString(
                    CultureInfo.InvariantCulture));
            components.Add(
                rejectPolicy.MaxArtifactBytes.ToString(
                    CultureInfo.InvariantCulture));
        }

        foreach (JsonTableColumnSchema column in schema.Columns)
        {
            components.Add(
                JsonMigrationObjectIds.Column(
                    column.ColumnIndex));
            components.Add(column.OriginalPropertyName);
            components.Add(column.LogicalType.ToString());
            components.Add(column.Resolution.ToString());
            components.Add(column.Reason.ToString());
            components.Add(column.Confidence.ToString());
            components.Add(
                column.Nullable
                    ? "nullable"
                    : "required");
            components.Add(column.MissingPolicy.ToString());
            components.Add(
                column.OverrideValidation.ToString());
        }
        foreach (ProjectedColumn column in projected)
            components.Add(column.ObjectId);

        return JsonStableDigest.Compute(components.ToArray());
    }

    private MigrationDataBatch CreateBatch(
        ValidatedRead request,
        List<MigrationDataRow> rows,
        List<MigrationRejectedRow> rejectedRows,
        long startRowOrdinal,
        long batchOrdinal,
        string? nextCursor) => new()
        {
            SourceObjectId = JsonMigrationObjectIds.Table,
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
            RejectedRows = rejectedRows.AsReadOnly(),
        };

    private static long GetOutcomeCanonicalBytes(
        NormalizedOutcome outcome,
        long batchOrdinal) =>
        outcome.RejectedRow is null
            ? outcome.AcceptedCanonicalBytes
            : MigrationRejectLedgerCodec
                .GetCanonicalArtifactEntryByteCount(
                    JsonMigrationObjectIds.Table,
                    batchOrdinal,
                    outcome.RejectedRow);

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
                "JSON resume state is inconsistent.");
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

    private static string EncodeCursor(
        long rowOrdinal,
        long batchOrdinal,
        string scopeDigest) =>
        string.Join(
            '/',
            CursorAlgorithmId,
            rowOrdinal.ToString(CultureInfo.InvariantCulture),
            batchOrdinal.ToString(CultureInfo.InvariantCulture),
            ComputeCursorToken(
                scopeDigest,
                rowOrdinal,
                batchOrdinal));

    private static CursorPosition ParseCursor(
        string cursor,
        string expectedScopeDigest)
    {
        string prefix = CursorAlgorithmId + "/";
        if (cursor.Length > MaximumCursorCharacters ||
            !cursor.StartsWith(prefix, StringComparison.Ordinal))
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
        if (!scopeDigest.StartsWith(
                "sha256:",
                StringComparison.Ordinal) ||
            scopeDigest.Length != 71)
        {
            throw new InvalidDataException(
                "The JSON cursor scope digest is invalid.");
        }

        string digest = JsonStableDigest.Compute(
            "csharpdb-json-cursor-token-v1",
            scopeDigest,
            rowOrdinal.ToString(CultureInfo.InvariantCulture),
            batchOrdinal.ToString(CultureInfo.InvariantCulture));
        return digest[7..];
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
            "The JSON resume cursor is malformed or does not match this read policy.");

    private static long CanonicalBatchUpperBound(
        JsonTableColumnLogicalType logicalType,
        long sourceCanonicalBytes) => logicalType switch
        {
            JsonTableColumnLogicalType.Text or
            JsonTableColumnLogicalType.Json =>
                sourceCanonicalBytes,
            JsonTableColumnLogicalType.Boolean or
            JsonTableColumnLogicalType.SignedInteger or
            JsonTableColumnLogicalType.UnsignedInteger or
            JsonTableColumnLogicalType.Decimal =>
                Math.Max(9L, sourceCanonicalBytes),
            _ => throw new ArgumentOutOfRangeException(
                nameof(logicalType)),
        };

    private static MigrationSourceValue NullValue() =>
        new()
        {
            Kind = MigrationSourceValueKind.Null,
        };

    private static long Utf8ByteCount(
        string value,
        CancellationToken cancellationToken)
    {
        long count = 0;
        for (int index = 0; index < value.Length;)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Rune rune = Rune.GetRuneAt(value, index);
            count = checked(count + rune.Utf8SequenceLength);
            index += rune.Utf16SequenceLength;
        }
        return count;
    }

    private static MigrationRowRejectedException Reject(
        string code,
        string columnObjectId,
        long batchOrdinal,
        long sourceRowOrdinal) =>
        MigrationRowRejectedException.CreateForSource(
            code,
            JsonMigrationObjectIds.Table,
            columnObjectId,
            batchOrdinal,
            sourceRowOrdinal);

    private static InvalidDataException RejectLimitExceeded(
        string limit) =>
        new(
            $"The JSON deterministic reject {limit} limit was exceeded.");

    private static InvalidDataException SchemaDrift() =>
        new(
            $"{JsonMigrationDataRules.SchemaDrift}: the JSON row contains a property outside the bound table schema.");

    private static List<MigrationDataRow> NewRowBuffer(
        int maximumRows) =>
        new(Math.Min(maximumRows, 1_024));

    private static List<MigrationRejectedRow> NewRejectBuffer(
        int maximumRows) =>
        new(Math.Min(maximumRows, 1_024));

    private sealed record ProjectedColumn(
        string ObjectId,
        int SchemaIndex,
        JsonTableColumnSchema Schema);

    private sealed record ProjectedValue(
        MigrationSourceValue Value,
        long CanonicalBatchBytes);

    private sealed record ValidatedRead(
        ReadOnlyCollection<string> ColumnObjectIds,
        ProjectedColumn[] Columns,
        int EffectiveMaximumRows,
        long EffectiveMaximumBatchBytes,
        int MaximumValueBytes,
        string ScopeDigest,
        CursorPosition? Resume,
        MigrationDeterministicRejectPolicy? RejectPolicy,
        IReadOnlySet<string> AllowedRejectRuleIds);

    private sealed record CursorPosition(
        string Original,
        long RowOrdinal,
        long BatchOrdinal);

    private sealed record NormalizedOutcome(
        MigrationDataRow? Row,
        MigrationRejectedRow? RejectedRow,
        long AcceptedCanonicalBytes,
        long RawValueBytes);
}
