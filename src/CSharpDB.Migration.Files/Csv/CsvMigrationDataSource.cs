using System.Collections.Frozen;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>Stable row-local rules raised while streaming CSV migration rows.</summary>
public static class CsvMigrationDataRules
{
    public const string MissingField = "MIG-CSV-DATA-MISSING-001";
    public const string NullNotAllowed = "MIG-CSV-DATA-NULL-001";
    public const string TypeMismatch = "MIG-CSV-DATA-TYPE-001";
    public const string ValueSizeExceeded = "MIG-CSV-DATA-VALUE-SIZE-001";
    public const string RowSizeExceeded = "MIG-CSV-DATA-ROW-SIZE-001";
}

/// <summary>
/// Replays one immutable, schema-bound CSV snapshot as deterministic migration
/// batches. The snapshot remains caller-owned and must outlive this source and
/// every active enumeration.
/// </summary>
public sealed class CsvMigrationDataSource :
    IMigrationDataSource,
    IMigrationCatalogBoundDataSource,
    IMigrationRejectAwareDataSource
{
    public const string CursorAlgorithmId = "csharpdb-csv-cursor-v1";

    // These safety ceilings bound adapter object overhead even when a direct
    // caller supplies impractically large request limits. Both are permitted
    // early-split boundaries under MigrationReadRequest's maximum semantics.
    private const int MaximumBufferedRows = 65_536;
    private const long MaximumBufferedCanonicalBytes = 64L * 1024 * 1024;
    private const int MaximumCursorCharacters = 160;

    private static readonly FrozenSet<string> SupportedRuleIds = new[]
    {
        CsvMigrationDataRules.MissingField,
        CsvMigrationDataRules.NullNotAllowed,
        CsvMigrationDataRules.TypeMismatch,
    }.ToFrozenSet(StringComparer.Ordinal);

    private readonly CsvSchemaInferenceResult schema;
    private readonly CsvSourceSnapshot snapshot;
    private int disposed;

    private CsvMigrationDataSource(
        CsvSchemaInferenceResult schema,
        CsvSourceSnapshot snapshot,
        string catalogDigest)
    {
        this.schema = schema;
        this.snapshot = snapshot;
        CatalogDigest = catalogDigest;
    }

    public MigrationSourceIdentity Source => schema.Source;

    public string SnapshotIdentity => schema.SnapshotIdentity;

    public string CatalogDigest { get; }

    public string RejectContractVersion =>
        MigrationRejectContract.DeterministicRejectsV1;

    public IReadOnlySet<string> SupportedRejectRuleIds => SupportedRuleIds;

    /// <summary>
    /// Creates a repeatable source after checking the exact snapshot and
    /// catalog policy bound to the inference result. The caller retains
    /// ownership of the snapshot.
    /// </summary>
    public static async ValueTask<CsvMigrationDataSource> CreateAsync(
        CsvSchemaInferenceResult schema,
        CsvSourceSnapshot snapshot,
        MigrationCatalog catalog,
        CancellationToken cancellationToken = default)
    {
        string catalogDigest = ValidateCatalogBinding(schema, snapshot, catalog);
        await snapshot.VerifyIntegrityAsync(cancellationToken).ConfigureAwait(false);
        return new CsvMigrationDataSource(schema, snapshot, catalogDigest);
    }

    /// <summary>
    /// Creates a source when the immediate caller has already verified the
    /// snapshot bytes while copying them. This avoids another full-file hash
    /// without weakening the public factory's integrity boundary.
    /// </summary>
    internal static CsvMigrationDataSource CreateFromVerifiedSnapshot(
        CsvSchemaInferenceResult schema,
        CsvSourceSnapshot snapshot,
        MigrationCatalog catalog) => new(
            schema,
            snapshot,
            ValidateCatalogBinding(schema, snapshot, catalog));

    internal static string ValidateCatalogBinding(
        CsvSchemaInferenceResult schema,
        CsvSourceSnapshot snapshot,
        MigrationCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(catalog);
        CsvSourceBinding binding = schema.Binding;
        if (!string.Equals(binding.SnapshotIdentity, snapshot.SnapshotIdentity, StringComparison.Ordinal) ||
            !string.Equals(binding.ContentDigest, snapshot.ContentDigest, StringComparison.Ordinal) ||
            binding.ContentLength != snapshot.ContentLength)
        {
            throw new ArgumentException(
                "The CSV schema inference result belongs to a different source snapshot.",
                nameof(snapshot));
        }

        MigrationContractValidator.ValidateCatalog(catalog);
        if (catalog.Source != schema.Source)
            throw new ArgumentException("The CSV migration catalog belongs to a different source.", nameof(catalog));
        MigrationCatalog expectedCatalog = schema.CreateCatalog(catalog.TargetCSharpDbVersion);
        string catalogDigest = MigrationArtifactSerializer.ComputeCatalogDigest(catalog);
        string expectedCatalogDigest = MigrationArtifactSerializer.ComputeCatalogDigest(expectedCatalog);
        if (!string.Equals(catalogDigest, expectedCatalogDigest, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The CSV migration catalog does not match the schema inference policy.",
                nameof(catalog));
        }

        return catalogDigest;
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

    private async IAsyncEnumerable<MigrationDataBatch> ReadCoreAsync(
        ValidatedRead request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();

        await using CsvStreamingReader reader = await schema.Binding
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

        await foreach (CsvLogicalRecord record in reader
                           .ReadRecordsAsync(cancellationToken)
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            long sourceRowOrdinal = checked(record.DataRecordNumber - 1);
            if (sourceRowOrdinal != expectedRowOrdinal)
                throw new InvalidDataException("CSV data-record ordinals are not contiguous.");

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
                if (ShouldYield(completed, request.Resume, ref resumeBoundaryFound))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    yield return completed;
                }

                batchOrdinal++;
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
                sourceRowOrdinal);

            long outcomeBytes = GetOutcomeCanonicalBytes(normalized, batchOrdinal);
            if (outcomeBytes > request.EffectiveMaximumBatchBytes)
            {
                throw new InvalidDataException(
                    "A CSV row outcome exceeds the bounded batch payload.");
            }

            bool splitForRejectPolicy = normalized.RejectedRow is not null &&
                outcomeCount > 0 &&
                (rejectedRows.Count >= request.RejectPolicy!.MaxRejectedRowsPerBatch ||
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
                if (ShouldYield(completed, request.Resume, ref resumeBoundaryFound))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    yield return completed;
                }

                batchOrdinal++;
                batchStartRowOrdinal = sourceRowOrdinal;
                batchBytes = 0;
                batchRawValueBytes = 0;
                rows = NewRowBuffer(request.EffectiveMaximumRows);
                rejectedRows = NewRejectBuffer(request.EffectiveMaximumRows);
                outcomeBytes = GetOutcomeCanonicalBytes(normalized, batchOrdinal);
                if (outcomeBytes > request.EffectiveMaximumBatchBytes)
                {
                    throw new InvalidDataException(
                        "A CSV row outcome exceeds the bounded batch payload.");
                }
            }

            if (normalized.RejectedRow is MigrationRejectedRow rejectedRow)
            {
                MigrationDeterministicRejectPolicy policy = request.RejectPolicy ??
                    throw new InvalidOperationException(
                        "CSV deterministic reject state is inconsistent.");
                if (normalized.RawValueBytes > policy.MaxRawValueBytes)
                {
                    throw RejectLimitExceeded("per-row raw-value byte");
                }
                if (rejectedRows.Count >= policy.MaxRejectedRowsPerBatch)
                {
                    throw RejectLimitExceeded("per-batch rejected-row");
                }
                if (checked(batchRawValueBytes + normalized.RawValueBytes) >
                    policy.MaxRawValueBytesPerBatch)
                {
                    throw RejectLimitExceeded("per-batch raw-value byte");
                }
                if (checked(rejectedRowsInRun + 1) > policy.MaxRejectedRowsPerRun)
                {
                    throw RejectLimitExceeded("per-run rejected-row");
                }
                if (checked(rawValueBytesInRun + normalized.RawValueBytes) >
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
                    batchRawValueBytes + normalized.RawValueBytes);
                rejectedRowsInRun++;
                rawValueBytesInRun = checked(
                    rawValueBytesInRun + normalized.RawValueBytes);
                artifactBytesInRun = checked(artifactBytesInRun + outcomeBytes);
            }
            else
            {
                rows.Add(normalized.Row ??
                    throw new InvalidOperationException(
                        "CSV normalized row outcome is inconsistent."));
            }

            batchBytes = checked(batchBytes + outcomeBytes);
            expectedRowOrdinal++;
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
            if (ShouldYield(final, request.Resume, ref resumeBoundaryFound))
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return final;
            }
            batchOrdinal++;
        }

        if (request.Resume is CursorPosition resume && !resumeBoundaryFound)
        {
            // A canonical cursor at the exact end is accepted as an empty
            // suffix. It is never emitted as NextCursor because terminal
            // batches use null to prove EOF.
            if (resume.RowOrdinal == expectedRowOrdinal && resume.BatchOrdinal == batchOrdinal)
                yield break;

            throw new InvalidDataException(
                "The CSV resume cursor does not identify a batch boundary in this snapshot.");
        }
    }

    private ValidatedRead Validate(MigrationReadRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!string.Equals(request.SourceObjectId, CsvMigrationObjectIds.Table, StringComparison.Ordinal))
            throw new ArgumentException("The CSV source object identifier is not supported.", nameof(request));
        if (request.BatchSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(request), "The batch size must be positive.");
        if (request.MaxBatchBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(request), "The maximum batch bytes must be positive.");
        if (request.MaxValueBytes <= 0 || request.MaxValueBytes > request.MaxBatchBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "The maximum value bytes must be positive and no greater than the batch bound.");
        }
        MigrationRejectReadPolicyValidator.Validate(request);
        MigrationDeterministicRejectPolicy? rejectPolicy = null;
        IReadOnlySet<string> allowedRejectRuleIds = FrozenSet<string>.Empty;
        if (string.Equals(
                request.RejectContractVersion,
                MigrationRejectContract.DeterministicRejectsV1,
                StringComparison.Ordinal))
        {
            MigrationDeterministicRejectPolicy supplied = request.RejectPolicy ??
                throw new InvalidDataException(
                    "CSV deterministic reject replay requires a reject policy.");
            string[] frozenRuleIds = supplied.AllowedRuleIds
                .OrderBy(ruleId => ruleId, StringComparer.Ordinal)
                .ToArray();
            foreach (string ruleId in frozenRuleIds)
            {
                if (!SupportedRuleIds.Contains(ruleId))
                {
                    throw new InvalidDataException(
                        "The CSV reject policy contains a rule that this source does not support.");
                }
            }

            rejectPolicy = supplied with
            {
                AllowedRuleIds = Array.AsReadOnly(frozenRuleIds),
            };
            allowedRejectRuleIds = frozenRuleIds.ToFrozenSet(StringComparer.Ordinal);
        }
        if (request.SnapshotToken is not null &&
            !string.Equals(request.SnapshotToken, SnapshotIdentity, StringComparison.Ordinal))
        {
            throw new InvalidDataException("The CSV read request snapshot token does not match the bound snapshot.");
        }
        if (request.ColumnObjectIds is null || request.ColumnObjectIds.Count == 0)
            throw new ArgumentException("At least one CSV column must be requested.", nameof(request));
        if (request.ColumnObjectIds.Count > schema.Columns.Count)
            throw new ArgumentException("The CSV column projection contains too many entries.", nameof(request));

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var projected = new ProjectedColumn[request.ColumnObjectIds.Count];
        var columnObjectIds = new string[request.ColumnObjectIds.Count];
        for (int index = 0; index < request.ColumnObjectIds.Count; index++)
        {
            string? objectId = request.ColumnObjectIds[index];
            if (string.IsNullOrWhiteSpace(objectId) ||
                !seen.Add(objectId) ||
                !CsvMigrationObjectIds.TryParseColumn(objectId, out int schemaIndex) ||
                (uint)schemaIndex >= (uint)schema.Columns.Count)
            {
                throw new ArgumentException(
                    "The CSV column projection contains an unknown, duplicate, or noncanonical identifier.",
                    nameof(request));
            }

            columnObjectIds[index] = objectId;
            projected[index] = new ProjectedColumn(objectId, schemaIndex, schema.Columns[schemaIndex]);
        }

        ReadOnlyCollection<string> frozenColumnIds = Array.AsReadOnly(columnObjectIds);
        string scopeDigest = ComputeScopeDigest(request, projected, rejectPolicy);
        CursorPosition? resume = null;
        if (request.ResumeCursor is not null)
        {
            if (!string.Equals(request.SnapshotToken, SnapshotIdentity, StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    "A CSV resume cursor requires the exact bound snapshot token.");
            }

            resume = ParseCursor(request.ResumeCursor, scopeDigest);
        }

        long effectiveMaximumBatchBytes = Math.Min(
            request.MaxBatchBytes,
            MaximumBufferedCanonicalBytes);
        return new ValidatedRead(
            frozenColumnIds,
            projected,
            Math.Min(request.BatchSize, MaximumBufferedRows),
            effectiveMaximumBatchBytes,
            checked((int)Math.Min(request.MaxValueBytes, effectiveMaximumBatchBytes)),
            scopeDigest,
            resume,
            rejectPolicy,
            allowedRejectRuleIds);
    }

    private NormalizedOutcome Normalize(
        CsvLogicalRecord record,
        ValidatedRead request,
        long batchOrdinal,
        long sourceRowOrdinal)
    {
        var values = new MigrationSourceValue[request.Columns.Length];
        long rowBytes = 0;
        for (int outputIndex = 0; outputIndex < request.Columns.Length; outputIndex++)
        {
            ProjectedColumn projected = request.Columns[outputIndex];
            if ((uint)projected.SchemaIndex >= (uint)record.Fields.Count)
            {
                return RecoverableReject(
                    CsvMigrationDataRules.MissingField,
                    projected,
                    record,
                    field: null,
                    request,
                    batchOrdinal,
                    sourceRowOrdinal);
            }

            CsvLogicalField field = record.Fields[projected.SchemaIndex];
            MigrationSourceValue value;
            long valueBytes;
            long batchBytes;
            switch (field.Kind)
            {
                case CsvFieldKind.Missing:
                    return RecoverableReject(
                        CsvMigrationDataRules.MissingField,
                        projected,
                        record,
                        field,
                        request,
                        batchOrdinal,
                        sourceRowOrdinal);

                case CsvFieldKind.Null:
                    if (!projected.Schema.Nullable)
                    {
                        return RecoverableReject(
                            CsvMigrationDataRules.NullNotAllowed,
                            projected,
                            record,
                            field,
                            request,
                            batchOrdinal,
                            sourceRowOrdinal);
                    }
                    value = new MigrationSourceValue { Kind = MigrationSourceValueKind.Null };
                    valueBytes = 1;
                    batchBytes = valueBytes;
                    break;

                case CsvFieldKind.Empty:
                case CsvFieldKind.Text:
                    string text = field.Value!;
                    if (!schema.TryNormalizeScalar(projected.SchemaIndex, text, out string? canonical))
                    {
                        return RecoverableReject(
                            CsvMigrationDataRules.TypeMismatch,
                            projected,
                            record,
                            field,
                            request,
                            batchOrdinal,
                            sourceRowOrdinal);
                    }
                    value = new MigrationSourceValue
                    {
                        Kind = SourceKind(projected.Schema.LogicalType),
                        CanonicalText = canonical!,
                    };
                    valueBytes = CanonicalValueByteCount(canonical!);
                    batchBytes = CanonicalBatchUpperBound(
                        projected.Schema.LogicalType,
                        valueBytes);
                    break;

                default:
                    throw new InvalidDataException("The CSV field kind is not supported.");
            }

            if (valueBytes > request.MaximumValueBytes)
            {
                throw Reject(
                    CsvMigrationDataRules.ValueSizeExceeded,
                    projected.ObjectId,
                    batchOrdinal,
                    sourceRowOrdinal);
            }

            rowBytes = checked(rowBytes + batchBytes);
            if (rowBytes > request.EffectiveMaximumBatchBytes)
            {
                throw Reject(
                    CsvMigrationDataRules.RowSizeExceeded,
                    projected.ObjectId,
                    batchOrdinal,
                    sourceRowOrdinal);
            }
            values[outputIndex] = value;
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

    private static NormalizedOutcome RecoverableReject(
        string ruleId,
        ProjectedColumn projected,
        CsvLogicalRecord record,
        CsvLogicalField? field,
        ValidatedRead request,
        long batchOrdinal,
        long sourceRowOrdinal)
    {
        if (request.RejectPolicy is null ||
            !request.AllowedRejectRuleIds.Contains(ruleId))
        {
            throw Reject(
                ruleId,
                projected.ObjectId,
                batchOrdinal,
                sourceRowOrdinal);
        }

        CsvFieldKind fieldKind = field?.Kind ?? CsvFieldKind.Missing;
        MigrationRejectEvidence[] evidence =
        [
            new MigrationRejectEvidence
            {
                Name = "columnIndex",
                Value = projected.SchemaIndex.ToString(CultureInfo.InvariantCulture),
            },
            new MigrationRejectEvidence
            {
                Name = "dataRecordNumber",
                Value = record.DataRecordNumber.ToString(CultureInfo.InvariantCulture),
            },
            new MigrationRejectEvidence
            {
                Name = "endPhysicalLine",
                Value = record.EndPhysicalLine.ToString(CultureInfo.InvariantCulture),
            },
            new MigrationRejectEvidence
            {
                Name = "fieldKind",
                Value = fieldKind.ToString(),
            },
            new MigrationRejectEvidence
            {
                Name = "logicalRecordNumber",
                Value = record.LogicalRecordNumber.ToString(CultureInfo.InvariantCulture),
            },
            new MigrationRejectEvidence
            {
                Name = MigrationRejectLedgerCodec.RawValueEvidenceName,
                Value = field?.RawValue,
            },
            new MigrationRejectEvidence
            {
                Name = "startPhysicalLine",
                Value = record.StartPhysicalLine.ToString(CultureInfo.InvariantCulture),
            },
            new MigrationRejectEvidence
            {
                Name = "wasQuoted",
                Value = field?.WasQuoted == true ? "true" : "false",
            },
        ];
        var rejectedRow = new MigrationRejectedRow
        {
            SourceRowOrdinal = sourceRowOrdinal,
            RuleId = ruleId,
            ColumnObjectId = projected.ObjectId,
            Evidence = Array.AsReadOnly(evidence),
        };
        return new NormalizedOutcome(
            Row: null,
            rejectedRow,
            AcceptedCanonicalBytes: 0,
            RawValueBytes: MigrationRejectLedgerCodec.GetRawValueByteCount(rejectedRow));
    }

    private string ComputeScopeDigest(
        MigrationReadRequest request,
        IReadOnlyList<ProjectedColumn> projected,
        MigrationDeterministicRejectPolicy? rejectPolicy)
    {
        var components = new List<string?>(24 + schema.Columns.Count * 5 + projected.Count)
        {
            CursorAlgorithmId,
            Source.Fingerprint,
            SnapshotIdentity,
            CatalogDigest,
            CsvMigrationObjectIds.Table,
            schema.TableName,
            CsvSchemaInferenceResult.AlgorithmId,
            CsvSchemaInferenceResult.ScalarPolicyId,
            request.BatchSize.ToString(CultureInfo.InvariantCulture),
            request.MaxBatchBytes.ToString(CultureInfo.InvariantCulture),
            request.MaxValueBytes.ToString(CultureInfo.InvariantCulture),
        };
        if (rejectPolicy is not null)
        {
            components.Add(request.RejectContractVersion);
            components.Add(rejectPolicy.ContractVersion);
            components.Add(rejectPolicy.AllowedRuleIds.Count.ToString(
                CultureInfo.InvariantCulture));
            foreach (string ruleId in rejectPolicy.AllowedRuleIds)
                components.Add(ruleId);
            components.Add(rejectPolicy.MaxRejectedRowsPerBatch.ToString(
                CultureInfo.InvariantCulture));
            components.Add(rejectPolicy.MaxRejectedRowsPerRun.ToString(
                CultureInfo.InvariantCulture));
            components.Add(rejectPolicy.MaxRawValueBytes.ToString(
                CultureInfo.InvariantCulture));
            components.Add(rejectPolicy.MaxRawValueBytesPerBatch.ToString(
                CultureInfo.InvariantCulture));
            components.Add(rejectPolicy.MaxRawValueBytesPerRun.ToString(
                CultureInfo.InvariantCulture));
            components.Add(rejectPolicy.MaxArtifactBytes.ToString(
                CultureInfo.InvariantCulture));
        }
        foreach (CsvColumnSchema column in schema.Columns)
        {
            components.Add(CsvMigrationObjectIds.Column(column.ColumnIndex));
            components.Add(column.LogicalType.ToString());
            components.Add(column.Resolution.ToString());
            components.Add(column.Nullable ? "nullable" : "required");
            components.Add(column.OriginalHeader);
        }
        foreach (ProjectedColumn column in projected)
            components.Add(column.ObjectId);
        return CsvStableDigest.Compute(components.ToArray());
    }

    private MigrationDataBatch CreateBatch(
        ValidatedRead request,
        List<MigrationDataRow> rows,
        List<MigrationRejectedRow> rejectedRows,
        long startRowOrdinal,
        long batchOrdinal,
        string? nextCursor) => new()
        {
            SourceObjectId = CsvMigrationObjectIds.Table,
            SnapshotIdentity = SnapshotIdentity,
            ColumnObjectIds = request.ColumnObjectIds,
            BatchOrdinal = batchOrdinal,
            StartCursor = batchOrdinal == 0
            ? null
            : EncodeCursor(startRowOrdinal, batchOrdinal, request.ScopeDigest),
            NextCursor = nextCursor,
            Rows = rows.AsReadOnly(),
            RejectedRows = rejectedRows.AsReadOnly(),
        };

    private static long GetOutcomeCanonicalBytes(
        NormalizedOutcome outcome,
        long batchOrdinal) =>
        outcome.RejectedRow is null
            ? outcome.AcceptedCanonicalBytes
            : MigrationRejectLedgerCodec.GetCanonicalArtifactEntryByteCount(
                CsvMigrationObjectIds.Table,
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
            throw new InvalidOperationException("CSV resume state is inconsistent.");
        if (batch.BatchOrdinal == resume.BatchOrdinal &&
            batch.StartCursor is not null &&
            string.Equals(batch.StartCursor, resume.Original, StringComparison.Ordinal))
        {
            resumeBoundaryFound = true;
            return true;
        }

        return false;
    }

    private static string EncodeCursor(long rowOrdinal, long batchOrdinal, string scopeDigest)
    {
        return string.Join(
            '/',
            CursorAlgorithmId,
            rowOrdinal.ToString(CultureInfo.InvariantCulture),
            batchOrdinal.ToString(CultureInfo.InvariantCulture),
            ComputeCursorToken(scopeDigest, rowOrdinal, batchOrdinal));
    }

    private static CursorPosition ParseCursor(string cursor, string expectedScopeDigest)
    {
        if (cursor.Length > MaximumCursorCharacters)
        {
            throw new InvalidDataException(
                "The CSV resume cursor is malformed or does not match this read policy.");
        }

        string[] parts = cursor.Split('/');
        if (parts.Length != 4 ||
            !string.Equals(parts[0], CursorAlgorithmId, StringComparison.Ordinal) ||
            !TryParseCanonicalInt64(parts[1], out long rowOrdinal) ||
            !TryParseCanonicalInt64(parts[2], out long batchOrdinal) ||
            (rowOrdinal == 0 && batchOrdinal == 0) ||
            parts[3].Length != 64 ||
            parts[3].Any(character => character is not (>= '0' and <= '9') and
                not (>= 'a' and <= 'f')) ||
            !string.Equals(
                parts[3],
                ComputeCursorToken(expectedScopeDigest, rowOrdinal, batchOrdinal),
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The CSV resume cursor is malformed or does not match this read policy.");
        }

        return new CursorPosition(cursor, rowOrdinal, batchOrdinal);
    }

    private static string ComputeCursorToken(
        string scopeDigest,
        long rowOrdinal,
        long batchOrdinal)
    {
        if (!scopeDigest.StartsWith("sha256:", StringComparison.Ordinal) || scopeDigest.Length != 71)
            throw new InvalidDataException("The CSV cursor scope digest is invalid.");
        string digest = CsvStableDigest.Compute(
            "csharpdb-csv-cursor-token-v1",
            scopeDigest,
            rowOrdinal.ToString(CultureInfo.InvariantCulture),
            batchOrdinal.ToString(CultureInfo.InvariantCulture));
        return digest[7..];
    }

    private static bool TryParseCanonicalInt64(string text, out long value)
    {
        value = 0;
        return text.Length > 0 &&
            (text.Length == 1 || text[0] != '0') &&
            long.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out value);
    }

    private static long CanonicalValueByteCount(string canonicalText) =>
        checked(5L + Encoding.UTF8.GetByteCount(canonicalText));

    private static long CanonicalBatchUpperBound(
        CsvColumnLogicalType logicalType,
        long sourceCanonicalBytes) => logicalType switch
        {
            CsvColumnLogicalType.Text or
            CsvColumnLogicalType.Guid or
            CsvColumnLogicalType.Date or
            CsvColumnLogicalType.Time or
            CsvColumnLogicalType.DateTime or
            CsvColumnLogicalType.DateTimeOffset => sourceCanonicalBytes,
            CsvColumnLogicalType.Boolean or
            CsvColumnLogicalType.SignedInteger or
            CsvColumnLogicalType.UnsignedInteger or
            CsvColumnLogicalType.Decimal or
            CsvColumnLogicalType.FloatingPoint => Math.Max(9L, sourceCanonicalBytes),
            _ => throw new ArgumentOutOfRangeException(nameof(logicalType)),
        };

    private static MigrationSourceValueKind SourceKind(CsvColumnLogicalType logicalType) =>
        logicalType switch
        {
            CsvColumnLogicalType.Text => MigrationSourceValueKind.Text,
            CsvColumnLogicalType.Boolean => MigrationSourceValueKind.Boolean,
            CsvColumnLogicalType.SignedInteger => MigrationSourceValueKind.SignedInteger,
            CsvColumnLogicalType.UnsignedInteger => MigrationSourceValueKind.UnsignedInteger,
            CsvColumnLogicalType.Decimal => MigrationSourceValueKind.Decimal,
            CsvColumnLogicalType.FloatingPoint => MigrationSourceValueKind.FloatingPoint,
            CsvColumnLogicalType.Guid => MigrationSourceValueKind.Guid,
            CsvColumnLogicalType.Date => MigrationSourceValueKind.Date,
            CsvColumnLogicalType.Time => MigrationSourceValueKind.Time,
            CsvColumnLogicalType.DateTime => MigrationSourceValueKind.DateTime,
            CsvColumnLogicalType.DateTimeOffset => MigrationSourceValueKind.DateTimeOffset,
            _ => throw new ArgumentOutOfRangeException(nameof(logicalType)),
        };

    private static MigrationRowRejectedException Reject(
        string code,
        string columnObjectId,
        long batchOrdinal,
        long sourceRowOrdinal) => MigrationRowRejectedException.CreateForSource(
            code,
            CsvMigrationObjectIds.Table,
            columnObjectId,
            batchOrdinal,
            sourceRowOrdinal);

    private static InvalidDataException RejectLimitExceeded(string limit) =>
        new($"The CSV deterministic reject {limit} limit was exceeded.");

    private static List<MigrationDataRow> NewRowBuffer(int maximumRows) =>
        new(Math.Min(maximumRows, 1_024));

    private static List<MigrationRejectedRow> NewRejectBuffer(int maximumRows) =>
        new(Math.Min(maximumRows, 1_024));

    private sealed record ProjectedColumn(
        string ObjectId,
        int SchemaIndex,
        CsvColumnSchema Schema);

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
