using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Performs bounded, value-free column profiling over an immutable CSV
/// binding. Only fixed-size counters and candidate flags are retained.
/// </summary>
public static class CsvSchemaInferer
{
    /// <summary>Absolute record-count ceiling for a bounded inference pass.</summary>
    public const int MaximumSupportedDataRecords = 1_000_000;

    private const string DefaultTextRuleId = "MIG-CSV-SCHEMA-TEXT-001";
    private const string MissingFieldRuleId = "MIG-CSV-SCHEMA-MISSING-001";
    private const string OverrideMismatchRuleId = "MIG-CSV-SCHEMA-OVERRIDE-001";
    private const string HeaderRuleId = "MIG-CSV-SCHEMA-HEADER-001";
    private const string SampleRuleId = "MIG-CSV-SCHEMA-SAMPLE-001";

    private const CsvScalarCandidate AllCandidates =
        CsvScalarCandidate.Boolean |
        CsvScalarCandidate.SignedInteger |
        CsvScalarCandidate.UnsignedInteger |
        CsvScalarCandidate.Decimal |
        CsvScalarCandidate.Guid |
        CsvScalarCandidate.Date |
        CsvScalarCandidate.Time |
        CsvScalarCandidate.DateTime |
        CsvScalarCandidate.DateTimeOffset;

    public static ValueTask<CsvSchemaInferenceResult> InferAsync(
        CsvSourceBinding binding,
        CsvSourceSnapshot snapshot,
        int maxDataRecords,
        CsvSchemaInferenceOptions? options = null,
        CancellationToken cancellationToken = default) =>
        InferCoreAsync(
            binding,
            snapshot,
            maxDataRecords,
            options ?? new CsvSchemaInferenceOptions(),
            collectProfile: true,
            cancellationToken);

    internal static ValueTask<CsvSchemaInferenceResult> DiscoverAsync(
        CsvSourceBinding binding,
        CsvSourceSnapshot snapshot,
        CsvSchemaInferenceOptions options,
        CancellationToken cancellationToken) =>
        InferCoreAsync(
            binding,
            snapshot,
            maxDataRecords: 1,
            options,
            collectProfile: false,
            cancellationToken);

    internal static ValueTask<CsvSchemaInferenceResult> ReplayAsync(
        CsvSourceBinding binding,
        CsvSourceSnapshot snapshot,
        CsvSchemaInferenceRecipe recipe,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(recipe);
        return InferCoreAsync(
            binding,
            snapshot,
            recipe.MaxDataRecords,
            recipe.ToOptions(),
            recipe.CollectProfile,
            cancellationToken);
    }

    private static async ValueTask<CsvSchemaInferenceResult> InferCoreAsync(
        CsvSourceBinding binding,
        CsvSourceSnapshot snapshot,
        int maxDataRecords,
        CsvSchemaInferenceOptions options,
        bool collectProfile,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(binding);
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(options);
        if (maxDataRecords <= 0 || maxDataRecords > MaximumSupportedDataRecords)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxDataRecords),
                $"The schema profile record limit must be between 1 and {MaximumSupportedDataRecords}.");
        }
        if (string.IsNullOrWhiteSpace(options.TableName) || options.TableName.Length > 1024)
        {
            throw new ArgumentException(
                "The CSV table name must be nonblank and at most 1024 characters.",
                nameof(options));
        }
        if (options.ColumnOverrides is null)
            throw new ArgumentException("CSV column overrides cannot be null.", nameof(options));
        if (options.ColumnOverrides.Count > CsvSchemaInferenceOptions.MaximumSupportedColumnOverrides)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                $"CSV schema inference accepts at most {CsvSchemaInferenceOptions.MaximumSupportedColumnOverrides} column overrides.");
        }
        if (options.MaxProfileCharacters <= 0 ||
            options.MaxProfileCharacters > CsvSchemaInferenceOptions.MaximumSupportedProfileCharacters)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                $"The cumulative CSV profile character limit must be between 1 and {CsvSchemaInferenceOptions.MaximumSupportedProfileCharacters}.");
        }
        cancellationToken.ThrowIfCancellationRequested();

        Dictionary<int, CsvColumnSchemaOverride> validatedOverrides = ValidateOverrides(options);
        var recipe = new CsvSchemaInferenceRecipe(
            collectProfile,
            maxDataRecords,
            options.TableName,
            options.MaxProfileCharacters,
            validatedOverrides.Values);
        Dictionary<int, CsvColumnSchemaOverride> overrides = recipe.ColumnOverrides
            .ToDictionary(item => item.ColumnIndex);
        await using CsvStreamingReader reader = await binding
            .OpenReaderAsync(snapshot, cancellationToken)
            .ConfigureAwait(false);

        string[]? headers = reader.Header?.Fields.ToArray();
        ColumnAccumulator[]? accumulators = reader.FieldCount is int knownWidth
            ? CreateAccumulators(knownWidth, headers, overrides)
            : null;
        long recordsExamined = 0;
        long profileCharactersExamined = 0;
        bool profileCharacterLimitReached = false;
        bool reachedEnd = false;
        bool sampleLimitExceeded = false;

        await using IAsyncEnumerator<CsvLogicalRecord> records = reader
            .ReadRecordsAsync(cancellationToken)
            .GetAsyncEnumerator(cancellationToken);

        if (!collectProfile)
        {
            if (accumulators is null && await records.MoveNextAsync().ConfigureAwait(false))
            {
                accumulators = CreateAccumulators(
                    records.Current.Fields.Count,
                    headers: null,
                    overrides);
            }
        }
        else
        {
            while (await records.MoveNextAsync().ConfigureAwait(false))
            {
                CsvLogicalRecord record = records.Current;
                accumulators ??= CreateAccumulators(
                    record.Fields.Count,
                    headers: null,
                    overrides);

                // One bounded look-ahead record distinguishes an exact EOF
                // from a sampled prefix, but never contributes evidence.
                if (recordsExamined == maxDataRecords)
                {
                    sampleLimitExceeded = true;
                    break;
                }

                long recordCharacters = CountDecodedCharacters(record, binding.Format.NullToken);
                if (recordCharacters > options.MaxProfileCharacters - profileCharactersExamined)
                {
                    profileCharacterLimitReached = true;
                    break;
                }

                foreach (CsvLogicalField field in record.Fields)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    accumulators[field.ColumnIndex].Observe(
                        field,
                        record,
                        binding.Culture);
                }

                profileCharactersExamined += recordCharacters;
                recordsExamined++;
            }

            reachedEnd = !sampleLimitExceeded && !profileCharacterLimitReached;
        }

        accumulators ??= CreateAccumulators(0, headers, overrides);
        MigrationProfileCoverage coverage = collectProfile
            ? new MigrationProfileCoverage
            {
                Kind = reachedEnd ? MigrationCoverageKind.Full : MigrationCoverageKind.Sample,
                ValuesExamined = recordsExamined,
                TotalValues = reachedEnd ? recordsExamined : null,
                RequiresFullStreamValidation = !reachedEnd,
            }
            : new MigrationProfileCoverage
            {
                Kind = MigrationCoverageKind.None,
                ValuesExamined = 0,
                RequiresFullStreamValidation = true,
            };

        var diagnostics = new List<MigrationDiagnostic>();
        CsvColumnSchema[] columns = accumulators
            .Select(accumulator => accumulator.Resolve(coverage, diagnostics, binding))
            .ToArray();
        AddHeaderDiagnostics(columns, diagnostics, binding);

        return new CsvSchemaInferenceResult(
            binding,
            recipe,
            recordsExamined,
            profileCharactersExamined,
            profileCharacterLimitReached,
            reachedEnd,
            coverage,
            columns,
            diagnostics.OrderBy(item => item.DiagnosticId, StringComparer.Ordinal).ToArray());
    }

    private static long CountDecodedCharacters(
        CsvLogicalRecord record,
        string? nullToken)
    {
        long total = 0;
        foreach (CsvLogicalField field in record.Fields)
        {
            total = checked(total + field.Kind switch
            {
                CsvFieldKind.Text or CsvFieldKind.Empty => field.Value!.Length,
                CsvFieldKind.Null => nullToken?.Length ?? 0,
                CsvFieldKind.Missing => 0,
                _ => throw new InvalidDataException("Unknown CSV logical field kind."),
            });
        }

        return total;
    }

    private static Dictionary<int, CsvColumnSchemaOverride> ValidateOverrides(
        CsvSchemaInferenceOptions options)
    {
        var result = new Dictionary<int, CsvColumnSchemaOverride>();
        foreach (CsvColumnSchemaOverride schemaOverride in options.ColumnOverrides)
        {
            if (schemaOverride is null)
                throw new ArgumentException("CSV column overrides cannot contain null values.", nameof(options));
            if (schemaOverride.ColumnIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(options), "CSV override indexes cannot be negative.");
            if (schemaOverride.ExpectedHeader is not null &&
                schemaOverride.ExpectedHeader.Length > CsvReaderOptions.MaximumSupportedFieldCharacters)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(options),
                    $"CSV override header guards cannot exceed {CsvReaderOptions.MaximumSupportedFieldCharacters} characters.");
            }
            if (!Enum.IsDefined(schemaOverride.LogicalType))
                throw new ArgumentOutOfRangeException(nameof(options), "A CSV override logical type is invalid.");
            if (!result.TryAdd(schemaOverride.ColumnIndex, schemaOverride))
            {
                throw new ArgumentException(
                    $"CSV column {schemaOverride.ColumnIndex} has more than one schema override.",
                    nameof(options));
            }
        }

        return result;
    }

    private static ColumnAccumulator[] CreateAccumulators(
        int width,
        string[]? headers,
        IReadOnlyDictionary<int, CsvColumnSchemaOverride> overrides)
    {
        if (headers is not null && headers.Length != width)
            throw new InvalidDataException("The bound CSV header width changed during schema inference.");
        if (overrides.Keys.Any(index => index >= width))
        {
            int invalid = overrides.Keys.Where(index => index >= width).Min();
            throw new ArgumentOutOfRangeException(
                nameof(overrides),
                $"CSV schema override column {invalid} is outside the source width {width}.");
        }

        var result = new ColumnAccumulator[width];
        for (int index = 0; index < width; index++)
        {
            string? originalHeader = headers?[index];
            string sourceName = string.IsNullOrWhiteSpace(originalHeader)
                ? $"column_{index + 1}"
                : originalHeader;
            overrides.TryGetValue(index, out CsvColumnSchemaOverride? schemaOverride);
            if (schemaOverride?.ExpectedHeader is not null &&
                !string.Equals(schemaOverride.ExpectedHeader, originalHeader, StringComparison.Ordinal))
            {
                throw new ArgumentException(
                    $"CSV schema override column {index} does not match its exact expected header.",
                    nameof(overrides));
            }

            result[index] = new ColumnAccumulator(
                index,
                sourceName,
                originalHeader,
                schemaOverride);
        }

        return result;
    }

    private static void AddHeaderDiagnostics(
        IReadOnlyList<CsvColumnSchema> columns,
        ICollection<MigrationDiagnostic> diagnostics,
        CsvSourceBinding binding)
    {
        var duplicateIndexes = columns
            .Where(column => column.OriginalHeader is not null)
            .GroupBy(column => column.OriginalHeader!, StringComparer.OrdinalIgnoreCase)
            .Where(group => group.Count() > 1)
            .SelectMany(group => group.Select(column => column.ColumnIndex))
            .ToHashSet();

        foreach (CsvColumnSchema column in columns)
        {
            string? reason = string.IsNullOrWhiteSpace(column.OriginalHeader) &&
                             column.OriginalHeader is not null
                ? "blank"
                : duplicateIndexes.Contains(column.ColumnIndex)
                    ? "duplicate"
                    : null;
            if (reason is null)
                continue;

            diagnostics.Add(CreateDiagnostic(
                binding,
                HeaderRuleId,
                column.ColumnIndex,
                reason,
                MigrationDiagnosticSeverity.Information,
                MigrationCompatibilityStatus.Compatible,
                "A CSV header requires deterministic target-name handling.",
                reason == "blank"
                    ? "The exact blank or whitespace-only header is preserved as a facet and an ordinal source name is used for the catalog."
                    : "The exact duplicate or case-colliding header remains a source fact; target naming resolves the collision deterministically.",
                canOverride: false));
        }
    }

    private static MigrationDiagnostic CreateDiagnostic(
        CsvSourceBinding binding,
        string ruleId,
        int columnIndex,
        string reason,
        MigrationDiagnosticSeverity severity,
        MigrationCompatibilityStatus status,
        string summary,
        string explanation,
        bool canOverride,
        long? physicalLine = null)
    {
        string digest = CsvStableDigest.Compute(
            "csharpdb-csv-schema-diagnostic-v1",
            ruleId,
            binding.Source.Fingerprint,
            columnIndex.ToString(System.Globalization.CultureInfo.InvariantCulture),
            reason);
        string shortDigest = digest["sha256:".Length..][..16];
        return new MigrationDiagnostic
        {
            DiagnosticId = $"diag:{ruleId.ToLowerInvariant()}:{shortDigest}",
            RuleId = ruleId,
            Severity = severity,
            Status = status,
            Evidence = MigrationEvidenceLevel.Parsed,
            Summary = summary,
            Explanation = explanation,
            ObjectId = $"csv:column:{columnIndex}",
            SourceSpan = physicalLine is > 0 and <= int.MaxValue
                ? new MigrationSourceSpan
                {
                    SourceId = binding.Source.Identity,
                    Line = (int)physicalLine.Value,
                    Column = columnIndex + 1,
                }
                : null,
            Remediation = ruleId switch
            {
                MissingFieldRuleId => "Choose an explicit missing-field reject or coercion policy before apply.",
                OverrideMismatchRuleId => "Correct or remove the source-schema override before apply.",
                DefaultTextRuleId => "Keep Text for lossless migration, or add a compatible ordinal schema override.",
                _ => null,
            },
            CanOverride = canOverride,
        };
    }

    private sealed class ColumnAccumulator
    {
        private CsvScalarCandidate intersection = AllCandidates;
        private CsvScalarCandidate union;
        private long plainTextCount;
        private bool sawTrue;
        private bool sawFalse;
        private int maxIntegralDigits;
        private int maxScale;
        private long? firstMissingPhysicalLine;
        private long? firstOverrideMismatchPhysicalLine;

        public ColumnAccumulator(
            int columnIndex,
            string sourceName,
            string? originalHeader,
            CsvColumnSchemaOverride? schemaOverride)
        {
            ColumnIndex = columnIndex;
            SourceName = sourceName;
            OriginalHeader = originalHeader;
            Override = schemaOverride;
        }

        public int ColumnIndex { get; }

        public string SourceName { get; }

        public string? OriginalHeader { get; }

        public CsvColumnSchemaOverride? Override { get; }

        public long SubstantiveValueCount { get; private set; }

        public long NullCount { get; private set; }

        public long EmptyCount { get; private set; }

        public long MissingCount { get; private set; }

        public long QuotedCount { get; private set; }

        public long NonCanonicalNumericCount { get; private set; }

        public int ObservedMaxLength { get; private set; }

        public bool OverrideCompatible { get; private set; } = true;

        public long? FirstMissingDataRecordNumber { get; private set; }

        public long? FirstOverrideMismatchDataRecordNumber { get; private set; }

        public void Observe(
            CsvLogicalField field,
            CsvLogicalRecord record,
            System.Globalization.CultureInfo culture)
        {
            if (field.WasQuoted)
                QuotedCount++;

            switch (field.Kind)
            {
                case CsvFieldKind.Missing:
                    MissingCount++;
                    FirstMissingDataRecordNumber ??= record.DataRecordNumber;
                    firstMissingPhysicalLine ??= record.StartPhysicalLine;
                    return;
                case CsvFieldKind.Null:
                    NullCount++;
                    if (Override?.Nullable == false)
                        MarkOverrideMismatch(record);
                    return;
                case CsvFieldKind.Empty:
                    EmptyCount++;
                    if (Override is { LogicalType: not CsvColumnLogicalType.Text })
                        MarkOverrideMismatch(record);
                    return;
                case CsvFieldKind.Text:
                    break;
                default:
                    throw new InvalidDataException("Unknown CSV logical field kind.");
            }

            string text = field.Value!;
            SubstantiveValueCount++;
            ObservedMaxLength = Math.Max(ObservedMaxLength, text.Length);
            CsvScalarClassification classification = CsvScalarLexicalPolicy.Classify(text, culture);
            intersection &= classification.Candidates;
            union |= classification.Candidates;
            if (classification.Candidates == CsvScalarCandidate.None)
                plainTextCount++;
            if (classification.RequiresLexicalPreservation)
                NonCanonicalNumericCount++;
            sawTrue |= classification.IsTrue;
            sawFalse |= classification.IsFalse;
            if ((classification.Candidates & CsvScalarCandidate.Decimal) != 0)
            {
                maxIntegralDigits = Math.Max(maxIntegralDigits, classification.IntegralDigits);
                maxScale = Math.Max(maxScale, classification.Scale);
            }

            if (Override is not null &&
                !CsvScalarLexicalPolicy.TryNormalize(
                    text,
                    Override.LogicalType,
                    culture,
                    allowLexicalNormalization: true,
                    out _))
            {
                MarkOverrideMismatch(record);
            }
        }

        public CsvColumnSchema Resolve(
            MigrationProfileCoverage coverage,
            ICollection<MigrationDiagnostic> diagnostics,
            CsvSourceBinding binding)
        {
            CsvColumnLogicalType logicalType;
            CsvColumnLogicalType? suggested = null;
            CsvColumnSchemaResolution resolution;
            CsvColumnInferenceReason reason;
            CsvInferenceConfidence confidence;

            if (Override is not null)
            {
                logicalType = Override.LogicalType;
                resolution = CsvColumnSchemaResolution.ExplicitOverride;
                reason = CsvColumnInferenceReason.ExplicitOverride;
                confidence = CsvInferenceConfidence.Explicit;
            }
            else if (EmptyCount > 0)
            {
                logicalType = CsvColumnLogicalType.Text;
                resolution = CsvColumnSchemaResolution.DefaultedToText;
                reason = CsvColumnInferenceReason.EmptyValue;
                confidence = CsvInferenceConfidence.None;
            }
            else if (NonCanonicalNumericCount > 0)
            {
                logicalType = CsvColumnLogicalType.Text;
                resolution = CsvColumnSchemaResolution.DefaultedToText;
                reason = CsvColumnInferenceReason.LexicalPreservation;
                confidence = CsvInferenceConfidence.None;
            }
            else if (SubstantiveValueCount == 0)
            {
                logicalType = CsvColumnLogicalType.Text;
                resolution = CsvColumnSchemaResolution.DefaultedToText;
                reason = CsvColumnInferenceReason.InsufficientEvidence;
                confidence = CsvInferenceConfidence.None;
            }
            else if (TrySelectCandidate(intersection, out CsvColumnLogicalType candidate))
            {
                if (SubstantiveValueCount == 1)
                {
                    logicalType = CsvColumnLogicalType.Text;
                    suggested = candidate;
                    resolution = CsvColumnSchemaResolution.DefaultedToText;
                    reason = CsvColumnInferenceReason.InsufficientEvidence;
                    confidence = CsvInferenceConfidence.Low;
                }
                else
                {
                    logicalType = candidate;
                    resolution = CsvColumnSchemaResolution.Inferred;
                    reason = CsvColumnInferenceReason.ExactEvidence;
                    if (candidate == CsvColumnLogicalType.Boolean && !(sawTrue && sawFalse))
                    {
                        logicalType = CsvColumnLogicalType.Text;
                        suggested = CsvColumnLogicalType.Boolean;
                        resolution = CsvColumnSchemaResolution.DefaultedToText;
                        reason = CsvColumnInferenceReason.InsufficientEvidence;
                        confidence = CsvInferenceConfidence.Low;
                    }
                    else
                    {
                        logicalType = candidate;
                        resolution = CsvColumnSchemaResolution.Inferred;
                        reason = CsvColumnInferenceReason.ExactEvidence;
                        confidence = coverage.Kind == MigrationCoverageKind.Full
                            ? CsvInferenceConfidence.High
                            : CsvInferenceConfidence.Medium;
                    }
                }
            }
            else if (union == CsvScalarCandidate.None && plainTextCount == SubstantiveValueCount)
            {
                logicalType = CsvColumnLogicalType.Text;
                resolution = CsvColumnSchemaResolution.Inferred;
                reason = CsvColumnInferenceReason.ExactEvidence;
                confidence = SubstantiveValueCount == 1
                    ? CsvInferenceConfidence.Low
                    : coverage.Kind == MigrationCoverageKind.Full
                        ? CsvInferenceConfidence.High
                        : CsvInferenceConfidence.Medium;
            }
            else
            {
                logicalType = CsvColumnLogicalType.Text;
                resolution = CsvColumnSchemaResolution.DefaultedToText;
                reason = CsvColumnInferenceReason.MixedKinds;
                confidence = CsvInferenceConfidence.None;
            }

            bool hasPresentEvidence = SubstantiveValueCount + NullCount + EmptyCount > 0;
            bool nullable = Override?.Nullable ??
                (coverage.Kind != MigrationCoverageKind.Full ||
                 !hasPresentEvidence ||
                 NullCount > 0);
            CsvOverrideValidationStatus overrideValidation = Override is null
                ? CsvOverrideValidationStatus.NotApplicable
                : !OverrideCompatible
                    ? CsvOverrideValidationStatus.Incompatible
                    : coverage.Kind == MigrationCoverageKind.None ||
                      coverage.Kind == MigrationCoverageKind.Sample && coverage.ValuesExamined == 0
                        ? CsvOverrideValidationStatus.NotProfiled
                    : coverage.Kind switch
                    {
                        MigrationCoverageKind.Sample => CsvOverrideValidationStatus.SampleCompatible,
                        MigrationCoverageKind.Full => CsvOverrideValidationStatus.FullCompatible,
                        _ => throw new InvalidDataException("Unknown migration profile coverage kind."),
                    };
            int? observedPrecision = maxIntegralDigits == 0 && maxScale == 0
                ? null
                : checked(maxIntegralDigits + maxScale);
            int? observedScale = observedPrecision is null ? null : maxScale;

            if (resolution == CsvColumnSchemaResolution.DefaultedToText)
            {
                diagnostics.Add(CreateDiagnostic(
                    binding,
                    DefaultTextRuleId,
                    ColumnIndex,
                    reason.ToString(),
                    MigrationDiagnosticSeverity.Information,
                    MigrationCompatibilityStatus.Compatible,
                    "CSV schema inference safely selected Text.",
                    "The bounded evidence was insufficient, mixed, empty, or lexically significant, so no narrower source type was activated.",
                    canOverride: false));
            }
            if (MissingCount > 0)
            {
                diagnostics.Add(CreateDiagnostic(
                    binding,
                    MissingFieldRuleId,
                    ColumnIndex,
                    "missing-field",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Conditional,
                    "A profiled CSV record is structurally missing this field.",
                    "Missing fields remain distinct from NULL and empty strings; no coercion policy has been selected.",
                    canOverride: false,
                    firstMissingPhysicalLine));
            }
            if (!OverrideCompatible)
            {
                diagnostics.Add(CreateDiagnostic(
                    binding,
                    OverrideMismatchRuleId,
                    ColumnIndex,
                    $"override-mismatch:{Override!.LogicalType}",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Conditional,
                    "A profiled CSV value contradicts the explicit source-schema override.",
                    "The override remains visible in the catalog, but apply is blocked until the declaration matches the bound source grammar.",
                    canOverride: false,
                    firstOverrideMismatchPhysicalLine));
            }
            if (coverage.Kind == MigrationCoverageKind.Sample &&
                logicalType != CsvColumnLogicalType.Text)
            {
                diagnostics.Add(CreateDiagnostic(
                    binding,
                    SampleRuleId,
                    ColumnIndex,
                    logicalType.ToString(),
                    MigrationDiagnosticSeverity.Information,
                    MigrationCompatibilityStatus.Conditional,
                    "The CSV source type is derived from a bounded sample.",
                    "Every streamed value must be checked by the same versioned scalar policy before a target batch commits.",
                    canOverride: false));
            }

            return new CsvColumnSchema(
                ColumnIndex,
                SourceName,
                OriginalHeader,
                logicalType,
                suggested,
                resolution,
                reason,
                confidence,
                nullable,
                overrideValidation,
                coverage,
                SubstantiveValueCount,
                NullCount,
                EmptyCount,
                MissingCount,
                QuotedCount,
                NonCanonicalNumericCount,
                ObservedMaxLength,
                observedPrecision,
                observedScale,
                FirstMissingDataRecordNumber,
                FirstOverrideMismatchDataRecordNumber);
        }

        private void MarkOverrideMismatch(CsvLogicalRecord record)
        {
            OverrideCompatible = false;
            FirstOverrideMismatchDataRecordNumber ??= record.DataRecordNumber;
            firstOverrideMismatchPhysicalLine ??= record.StartPhysicalLine;
        }

        private static bool TrySelectCandidate(
            CsvScalarCandidate candidates,
            out CsvColumnLogicalType logicalType)
        {
            (CsvScalarCandidate Candidate, CsvColumnLogicalType Type)[] precedence =
            [
                (CsvScalarCandidate.Boolean, CsvColumnLogicalType.Boolean),
                (CsvScalarCandidate.SignedInteger, CsvColumnLogicalType.SignedInteger),
                (CsvScalarCandidate.UnsignedInteger, CsvColumnLogicalType.UnsignedInteger),
                (CsvScalarCandidate.Decimal, CsvColumnLogicalType.Decimal),
                (CsvScalarCandidate.Guid, CsvColumnLogicalType.Guid),
                (CsvScalarCandidate.Date, CsvColumnLogicalType.Date),
                (CsvScalarCandidate.Time, CsvColumnLogicalType.Time),
                (CsvScalarCandidate.DateTimeOffset, CsvColumnLogicalType.DateTimeOffset),
                (CsvScalarCandidate.DateTime, CsvColumnLogicalType.DateTime),
            ];
            foreach ((CsvScalarCandidate candidate, CsvColumnLogicalType type) in precedence)
            {
                if ((candidates & candidate) != 0)
                {
                    logicalType = type;
                    return true;
                }
            }

            logicalType = default;
            return false;
        }
    }
}
