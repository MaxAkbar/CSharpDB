using System.Buffers;
using System.Security.Cryptography;
using System.Text;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Performs bounded, deterministic format inspection over an immutable CSV
/// snapshot. It never guesses legacy encodings or breaks delimiter ties by
/// candidate order or current culture.
/// </summary>
public static class CsvFormatInspector
{
    private const int MaxDelimiterCandidates = 16;
    private const int MaxInspectionBytes = 64 * 1024 * 1024;
    private const int MaxInspectionCharacters = 64 * 1024 * 1024;
    private const int MaxInspectionRecords = 10_000;

    public static async ValueTask<CsvFormatInspection> InspectAsync(
        CsvSourceSnapshot snapshot,
        CsvReaderOptions? readerOptions = null,
        CsvInspectionOptions? inspectionOptions = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        CsvReaderOptions sourceOptions = readerOptions ?? new CsvReaderOptions();
        CsvInspectionOptions limits = inspectionOptions ?? new CsvInspectionOptions();
        CandidateSettings[] candidates = ValidateAndNormalizeCandidates(sourceOptions, limits);

        int sampleLength = (int)Math.Min(snapshot.ContentLength, limits.MaxSampleBytes);
        bool byteLimited = snapshot.ContentLength > sampleLength;
        byte[] sample = await ReadSampleAsync(snapshot, sampleLength, cancellationToken)
            .ConfigureAwait(false);
        try
        {
            CsvReaderSettings firstSettings = candidates[0].Settings;
            CsvEncodingResolution encodingResolution = CsvEncodingResolver.Resolve(
                sample.AsSpan(0, Math.Min(sample.Length, 4)),
                firstSettings.Encoding,
                firstSettings.DetectEncodingFromByteOrderMarks);
            EncodingValidation validation = ValidateEncoding(
                sample.AsSpan(encodingResolution.PreambleLength),
                encodingResolution.Encoding,
                byteLimited,
                limits.MaxSampleCharacters);
            CsvEncodingInspection encoding = CreateEncodingInspection(
                sample,
                encodingResolution,
                validation,
                byteLimited);

            if (!validation.IsValid || validation.CharacterLimitReached)
            {
                CsvInspectionResolution resolution = validation.IsValid
                    ? CsvInspectionResolution.InsufficientData
                    : CsvInspectionResolution.Invalid;
                string diagnostic = validation.IsValid
                    ? CsvDiagnosticRules.InspectionCharacterLimitExceeded
                    : CsvDiagnosticRules.InvalidEncoding;
                CsvDelimiterCandidateEvidence[] invalidEvidence = candidates
                    .Select(candidate => new CsvDelimiterCandidateEvidence(
                        candidate.Delimiter.ToString(),
                        validation.IsValid
                            ? CsvDelimiterCandidateStatus.Truncated
                            : CsvDelimiterCandidateStatus.Incompatible,
                        null,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        diagnostic))
                    .ToArray();
                var delimiter = new CsvDelimiterInspection(
                    resolution,
                    CsvInspectionConfidence.None,
                    null,
                    null,
                    invalidEvidence,
                    logicalRecordLimitReached: false);
                return new CsvFormatInspection(
                    snapshot.SnapshotIdentity,
                    snapshot.ContentDigest,
                    snapshot.ContentLength,
                    encoding,
                    delimiter,
                    null,
                    null,
                    sample.Length,
                    byteLimited);
            }

            var evidence = new CsvDelimiterCandidateEvidence[candidates.Length];
            bool recordLimitReached = false;
            for (int index = 0; index < candidates.Length; index++)
            {
                CandidateProbe probe = await ProbeCandidateAsync(
                        sample,
                        byteLimited,
                        encodingResolution.Encoding,
                        candidates[index],
                        limits.MaxLogicalRecords,
                        cancellationToken)
                    .ConfigureAwait(false);
                evidence[index] = probe.Evidence;
                recordLimitReached |= probe.LogicalRecordLimitReached;
            }

            DelimiterDecision decision = Decide(evidence, candidates.Length == 1, byteLimited);
            var delimiterInspection = new CsvDelimiterInspection(
                decision.Resolution,
                decision.Confidence,
                decision.SelectedDelimiter,
                decision.SuggestedDelimiter,
                evidence,
                recordLimitReached);

            CsvResolvedFormat? format = null;
            CsvReaderOptions? resolvedOptions = null;
            if (decision.SelectedDelimiter is not null)
            {
                CsvReaderSettings selectedSettings = CsvReaderSettings.Create(
                    sourceOptions with { Delimiter = decision.SelectedDelimiter });
                resolvedOptions = selectedSettings.ToOptions(leaveOpen: false);
                format = new CsvResolvedFormat(
                    decision.SelectedDelimiter,
                    selectedSettings.Quote,
                    selectedSettings.HasHeaderRecord,
                    encodingResolution.Encoding.WebName,
                    encodingResolution.Encoding.CodePage,
                    encodingResolution.HasByteOrderMark,
                    selectedSettings.Culture.Name,
                    CsvCulturePolicy.ComputeDigest(selectedSettings.Culture),
                    selectedSettings.NullToken,
                    selectedSettings.NullTokenMatchesQuotedFields,
                    selectedSettings.ExpectedFieldCount);
            }

            return new CsvFormatInspection(
                snapshot.SnapshotIdentity,
                snapshot.ContentDigest,
                snapshot.ContentLength,
                encoding,
                delimiterInspection,
                format,
                resolvedOptions,
                sample.Length,
                byteLimited);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(sample);
        }
    }

    private static CandidateSettings[] ValidateAndNormalizeCandidates(
        CsvReaderOptions readerOptions,
        CsvInspectionOptions inspectionOptions)
    {
        ArgumentNullException.ThrowIfNull(inspectionOptions.DelimiterCandidates);
        if (readerOptions.NullToken is not null && readerOptions.NullToken.Length == 0)
        {
            throw new ArgumentException(
                "The null token cannot be empty because empty and null must remain distinct.",
                nameof(readerOptions));
        }
        if (readerOptions.NullToken is not null &&
            !readerOptions.NullTokenMatchesQuotedFields &&
            readerOptions.NullToken.IndexOfAny([readerOptions.Quote, '\r', '\n']) >= 0)
        {
            throw new ArgumentException(
                "A reversible unquoted null token cannot contain quote or newline characters.",
                nameof(readerOptions));
        }
        if (inspectionOptions.MaxSampleBytes is < 4 or > MaxInspectionBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(inspectionOptions),
                $"The inspection byte limit must be between 4 and {MaxInspectionBytes} bytes.");
        }
        if (inspectionOptions.MaxSampleCharacters is < 1 or > MaxInspectionCharacters)
        {
            throw new ArgumentOutOfRangeException(
                nameof(inspectionOptions),
                $"The inspection character limit must be between 1 and {MaxInspectionCharacters} characters.");
        }
        if (inspectionOptions.MaxLogicalRecords is < 1 or > MaxInspectionRecords)
        {
            throw new ArgumentOutOfRangeException(
                nameof(inspectionOptions),
                $"The inspection record limit must be between 1 and {MaxInspectionRecords} records.");
        }
        if (inspectionOptions.DelimiterCandidates.Count is < 1 or > MaxDelimiterCandidates)
        {
            throw new ArgumentOutOfRangeException(
                nameof(inspectionOptions),
                $"Delimiter inspection requires between 1 and {MaxDelimiterCandidates} candidates.");
        }

        var normalized = new SortedSet<string>(StringComparer.Ordinal);
        foreach (string? candidate in inspectionOptions.DelimiterCandidates)
        {
            if (candidate is null || candidate.Length != 1)
            {
                throw new ArgumentException(
                    "Every delimiter candidate must contain exactly one character.",
                    nameof(inspectionOptions));
            }

            char value = candidate[0];
            if (value is '\r' or '\n' or '\0' || value == readerOptions.Quote)
            {
                throw new ArgumentException(
                    "A delimiter candidate is not valid for the configured quote character.",
                    nameof(inspectionOptions));
            }
            if (!normalized.Add(candidate))
            {
                throw new ArgumentException(
                    "Delimiter candidates must be unique.",
                    nameof(inspectionOptions));
            }
        }

        return normalized
            .Select(candidate => new CandidateSettings(
                candidate[0],
                CsvReaderSettings.Create(readerOptions with
                {
                    Delimiter = candidate,
                    NullToken = null,
                    NullTokenMatchesQuotedFields = false,
                })))
            .ToArray();
    }

    private static async ValueTask<byte[]> ReadSampleAsync(
        CsvSourceSnapshot snapshot,
        int sampleLength,
        CancellationToken cancellationToken)
    {
        byte[] sample = new byte[sampleLength];
        await using Stream stream = snapshot.OpenRead();
        int offset = 0;
        while (offset < sample.Length)
        {
            int read = await stream.ReadAsync(sample.AsMemory(offset), cancellationToken)
                .ConfigureAwait(false);
            if (read == 0)
            {
                throw new CsvSourceSnapshotException(
                    CsvSnapshotDiagnosticRules.IntegrityMismatch,
                    "The private CSV snapshot ended before its recorded content length.");
            }
            offset += read;
        }

        return sample;
    }

    private static EncodingValidation ValidateEncoding(
        ReadOnlySpan<byte> bytes,
        Encoding encoding,
        bool sampleWasTruncated,
        int maximumCharacters)
    {
        Decoder decoder = encoding.GetDecoder();
        char[] buffer = ArrayPool<char>.Shared.Rent(Math.Min(4096, maximumCharacters + 1));
        int byteOffset = 0;
        int characters = 0;
        bool completed = sampleWasTruncated && bytes.Length == 0;
        try
        {
            while (byteOffset < bytes.Length || (!sampleWasTruncated && !completed))
            {
                decoder.Convert(
                    bytes[byteOffset..],
                    buffer,
                    flush: !sampleWasTruncated,
                    out int bytesUsed,
                    out int charsUsed,
                    out completed);
                byteOffset += bytesUsed;
                characters += charsUsed;
                if (characters > maximumCharacters)
                    return new EncodingValidation(true, characters, true);
                if (completed)
                    break;
                if (bytesUsed == 0 && charsUsed == 0)
                    throw new InvalidOperationException("The configured decoder made no progress.");
            }

            return new EncodingValidation(true, characters, false);
        }
        catch (DecoderFallbackException)
        {
            return new EncodingValidation(false, characters, false);
        }
        finally
        {
            ArrayPool<char>.Shared.Return(buffer, clearArray: true);
        }
    }

    private static CsvEncodingInspection CreateEncodingInspection(
        byte[] sample,
        CsvEncodingResolution resolution,
        EncodingValidation validation,
        bool sampleWasTruncated)
    {
        CsvEncodingEvidenceKind evidenceKind;
        CsvInspectionConfidence confidence;
        if (resolution.HasByteOrderMark)
        {
            evidenceKind = CsvEncodingEvidenceKind.ByteOrderMark;
            confidence = CsvInspectionConfidence.High;
        }
        else if (string.Equals(resolution.Encoding.WebName, "utf-8", StringComparison.OrdinalIgnoreCase))
        {
            bool hasNonAscii = sample.Any(value => value >= 0x80);
            evidenceKind = hasNonAscii
                ? CsvEncodingEvidenceKind.StrictUtf8Sample
                : CsvEncodingEvidenceKind.AsciiCompatibleSample;
            confidence = hasNonAscii
                ? CsvInspectionConfidence.Medium
                : CsvInspectionConfidence.Low;
        }
        else
        {
            evidenceKind = CsvEncodingEvidenceKind.ConfiguredFallback;
            confidence = CsvInspectionConfidence.Explicit;
        }

        if (!validation.IsValid)
            confidence = CsvInspectionConfidence.None;

        return new CsvEncodingInspection(
            resolution.Encoding.WebName,
            resolution.HasByteOrderMark,
            evidenceKind,
            confidence,
            sample.Length,
            validation.DecodedCharacters,
            sampleWasTruncated,
            validation.IsValid,
            validation.CharacterLimitReached);
    }

    private static async ValueTask<CandidateProbe> ProbeCandidateAsync(
        byte[] sample,
        bool sampleWasByteLimited,
        Encoding resolvedEncoding,
        CandidateSettings candidate,
        int maximumLogicalRecords,
        CancellationToken cancellationToken)
    {
        var records = new List<RecordShape>(Math.Min(maximumLogicalRecords, 256));
        bool recordLimitReached = false;
        CsvDelimiterCandidateStatus status = CsvDelimiterCandidateStatus.Compatible;
        string? diagnosticRuleId = null;
        int extraRecords = 0;

        var stream = new MemoryStream(sample, writable: false);
        CsvReaderOptions probeOptions = candidate.Settings.ToOptions(
            delimiter: candidate.Delimiter,
            hasHeaderRecord: false,
            leaveOpen: false);
        await using CsvStreamingReader reader = await CsvStreamingReader.OpenAsync(
                stream,
                probeOptions,
                cancellationToken)
            .ConfigureAwait(false);
        try
        {
            await foreach (CsvLogicalRecord record in reader.ReadRecordsAsync(cancellationToken)
                .ConfigureAwait(false))
            {
                records.Add(new RecordShape(
                    record.PresentFieldCount,
                    record.Fields.Count(field => field.WasQuoted),
                    record.EndPhysicalLine > record.StartPhysicalLine));
                if (records.Count == maximumLogicalRecords)
                {
                    recordLimitReached = true;
                    break;
                }
            }
        }
        catch (CsvReadException exception)
        {
            diagnosticRuleId = exception.Diagnostic.RuleId;
            if (exception.Diagnostic.RuleId == CsvDiagnosticRules.ExtraFields)
                extraRecords = 1;

            bool artificialTail = sampleWasByteLimited &&
                exception.Diagnostic.RuleId is CsvDiagnosticRules.MalformedData or
                    CsvDiagnosticRules.InvalidEncoding;
            status = artificialTail
                ? CsvDelimiterCandidateStatus.Truncated
                : CsvDelimiterCandidateStatus.Incompatible;
        }

        if (sampleWasByteLimited &&
            !recordLimitReached &&
            status == CsvDelimiterCandidateStatus.Compatible &&
            records.Count > 0 &&
            !EndsWithRecordTerminator(sample, resolvedEncoding))
        {
            records.RemoveAt(records.Count - 1);
            status = CsvDelimiterCandidateStatus.Truncated;
        }

        int? expectedFieldCount = candidate.Settings.ExpectedFieldCount ??
            (records.Count == 0 ? null : records[0].FieldCount);
        int exactRecords = 0;
        int shortRecords = 0;
        int observedExtraRecords = extraRecords;
        if (expectedFieldCount is not null)
        {
            foreach (RecordShape record in records)
            {
                if (record.FieldCount == expectedFieldCount.Value)
                    exactRecords++;
                else if (record.FieldCount < expectedFieldCount.Value)
                    shortRecords++;
                else
                    observedExtraRecords++;
            }
        }

        int consistency = records.Count == 0
            ? 0
            : (int)(10_000L * exactRecords / records.Count);
        var evidence = new CsvDelimiterCandidateEvidence(
            candidate.Delimiter.ToString(),
            status,
            expectedFieldCount,
            records.Count,
            exactRecords,
            shortRecords,
            observedExtraRecords,
            consistency,
            records.Sum(record => record.QuotedFieldCount),
            records.Count(record => record.IsMultiline),
            diagnosticRuleId);
        return new CandidateProbe(evidence, recordLimitReached);
    }

    private static bool EndsWithRecordTerminator(byte[] sample, Encoding encoding)
    {
        if (sample.Length == 0)
            return false;

        byte[] lineFeed = encoding.GetBytes("\n");
        byte[] carriageReturn = encoding.GetBytes("\r");
        ReadOnlySpan<byte> bytes = sample;
        return bytes.EndsWith(lineFeed) || bytes.EndsWith(carriageReturn);
    }

    private static DelimiterDecision Decide(
        CsvDelimiterCandidateEvidence[] evidence,
        bool singleCandidateIsExplicit,
        bool sampleWasByteLimited)
    {
        if (singleCandidateIsExplicit)
        {
            CsvDelimiterCandidateEvidence candidate = evidence[0];
            if (candidate.Status == CsvDelimiterCandidateStatus.Incompatible)
            {
                return new DelimiterDecision(
                    CsvInspectionResolution.Invalid,
                    CsvInspectionConfidence.None,
                    null,
                    null);
            }

            return new DelimiterDecision(
                CsvInspectionResolution.Resolved,
                CsvInspectionConfidence.Explicit,
                candidate.Delimiter,
                null);
        }

        CsvDelimiterCandidateEvidence[] strong = evidence.Where(IsStrong).ToArray();
        if (strong.Length > 1)
        {
            return new DelimiterDecision(
                CsvInspectionResolution.Ambiguous,
                CsvInspectionConfidence.None,
                null,
                null);
        }

        if (strong.Length == 1)
        {
            CsvDelimiterCandidateEvidence winner = strong[0];
            bool hasNearCompetitor = evidence.Any(candidate =>
                !ReferenceEquals(candidate, winner) &&
                IsCompatibleMultiField(candidate) &&
                candidate.ExactWidthRecords >= 2 &&
                candidate.ConsistencyBasisPoints >= winner.ConsistencyBasisPoints - 1000);
            if (hasNearCompetitor)
            {
                return new DelimiterDecision(
                    CsvInspectionResolution.Ambiguous,
                    CsvInspectionConfidence.None,
                    null,
                    null);
            }

            CsvInspectionConfidence confidence =
                winner.ExactWidthRecords >= 8 && winner.ConsistencyBasisPoints == 10_000
                    ? CsvInspectionConfidence.High
                    : CsvInspectionConfidence.Medium;
            return new DelimiterDecision(
                CsvInspectionResolution.Resolved,
                confidence,
                winner.Delimiter,
                null);
        }

        CsvDelimiterCandidateEvidence[] weak = evidence
            .Where(candidate => IsCompatibleMultiField(candidate) && candidate.ExactWidthRecords >= 1)
            .OrderByDescending(candidate => candidate.ConsistencyBasisPoints)
            .ThenByDescending(candidate => candidate.ExactWidthRecords)
            .ThenByDescending(candidate => candidate.CompleteLogicalRecords)
            .ThenBy(candidate => candidate.Delimiter, StringComparer.Ordinal)
            .ToArray();
        if (weak.Length > 1 && SameScore(weak[0], weak[1]))
        {
            return new DelimiterDecision(
                CsvInspectionResolution.Ambiguous,
                CsvInspectionConfidence.None,
                null,
                null);
        }
        if (weak.Length == 1 || weak.Length > 1)
        {
            return new DelimiterDecision(
                CsvInspectionResolution.InsufficientData,
                CsvInspectionConfidence.Low,
                null,
                weak[0].Delimiter);
        }

        bool allIncompatible = evidence.All(candidate =>
            candidate.Status == CsvDelimiterCandidateStatus.Incompatible);
        return new DelimiterDecision(
            allIncompatible && !sampleWasByteLimited
                ? CsvInspectionResolution.Invalid
                : CsvInspectionResolution.InsufficientData,
            CsvInspectionConfidence.None,
            null,
            null);
    }

    private static bool IsStrong(CsvDelimiterCandidateEvidence candidate) =>
        IsCompatibleMultiField(candidate) &&
        candidate.ExactWidthRecords >= 2 &&
        candidate.ConsistencyBasisPoints >= 8000;

    private static bool IsCompatibleMultiField(CsvDelimiterCandidateEvidence candidate) =>
        candidate.Status != CsvDelimiterCandidateStatus.Incompatible &&
        candidate.ExpectedFieldCount >= 2 &&
        candidate.ExtraRecords == 0;

    private static bool SameScore(
        CsvDelimiterCandidateEvidence left,
        CsvDelimiterCandidateEvidence right) =>
        left.ConsistencyBasisPoints == right.ConsistencyBasisPoints &&
        left.ExactWidthRecords == right.ExactWidthRecords &&
        left.CompleteLogicalRecords == right.CompleteLogicalRecords;

    private sealed record CandidateSettings(char Delimiter, CsvReaderSettings Settings);

    private sealed record RecordShape(int FieldCount, int QuotedFieldCount, bool IsMultiline);

    private sealed record CandidateProbe(
        CsvDelimiterCandidateEvidence Evidence,
        bool LogicalRecordLimitReached);

    private sealed record EncodingValidation(
        bool IsValid,
        int DecodedCharacters,
        bool CharacterLimitReached);

    private sealed record DelimiterDecision(
        CsvInspectionResolution Resolution,
        CsvInspectionConfidence Confidence,
        string? SelectedDelimiter,
        string? SuggestedDelimiter);
}
