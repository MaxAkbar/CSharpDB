using System.Collections.ObjectModel;

namespace CSharpDB.Migration.Files.Csv;

public enum CsvInspectionResolution
{
    Resolved,
    Ambiguous,
    InsufficientData,
    Invalid,
}

public enum CsvInspectionConfidence
{
    None,
    Low,
    Medium,
    High,
    Explicit,
}

public enum CsvEncodingEvidenceKind
{
    ByteOrderMark,
    StrictUtf8Sample,
    AsciiCompatibleSample,
    ConfiguredFallback,
}

public enum CsvDelimiterCandidateStatus
{
    Compatible,
    Incompatible,
    Truncated,
}

/// <summary>Bounds deterministic format inspection independently of file size.</summary>
public sealed record CsvInspectionOptions
{
    public IReadOnlyList<string> DelimiterCandidates { get; init; } = [",", ";", "\t", "|"];

    public int MaxSampleBytes { get; init; } = 1024 * 1024;

    public int MaxSampleCharacters { get; init; } = 1024 * 1024;

    public int MaxLogicalRecords { get; init; } = 100;
}

public sealed class CsvEncodingInspection
{
    internal CsvEncodingInspection(
        string resolvedEncodingName,
        bool hasByteOrderMark,
        CsvEncodingEvidenceKind evidenceKind,
        CsvInspectionConfidence confidence,
        int sampledBytes,
        int decodedCharacters,
        bool sampleWasTruncated,
        bool sampleIsValid,
        bool characterLimitReached)
    {
        ResolvedEncodingName = resolvedEncodingName;
        HasByteOrderMark = hasByteOrderMark;
        EvidenceKind = evidenceKind;
        Confidence = confidence;
        SampledBytes = sampledBytes;
        DecodedCharacters = decodedCharacters;
        SampleWasTruncated = sampleWasTruncated;
        SampleIsValid = sampleIsValid;
        CharacterLimitReached = characterLimitReached;
    }

    public string ResolvedEncodingName { get; }

    public bool HasByteOrderMark { get; }

    public CsvEncodingEvidenceKind EvidenceKind { get; }

    public CsvInspectionConfidence Confidence { get; }

    public int SampledBytes { get; }

    public int DecodedCharacters { get; }

    public bool SampleWasTruncated { get; }

    public bool SampleIsValid { get; }

    public bool CharacterLimitReached { get; }
}

public sealed class CsvDelimiterCandidateEvidence
{
    internal CsvDelimiterCandidateEvidence(
        string delimiter,
        CsvDelimiterCandidateStatus status,
        int? expectedFieldCount,
        int completeLogicalRecords,
        int exactWidthRecords,
        int shortRecords,
        int extraRecords,
        int consistencyBasisPoints,
        int quotedFields,
        int multilineRecords,
        string? diagnosticRuleId)
    {
        Delimiter = delimiter;
        Status = status;
        ExpectedFieldCount = expectedFieldCount;
        CompleteLogicalRecords = completeLogicalRecords;
        ExactWidthRecords = exactWidthRecords;
        ShortRecords = shortRecords;
        ExtraRecords = extraRecords;
        ConsistencyBasisPoints = consistencyBasisPoints;
        QuotedFields = quotedFields;
        MultilineRecords = multilineRecords;
        DiagnosticRuleId = diagnosticRuleId;
    }

    public string Delimiter { get; }

    public CsvDelimiterCandidateStatus Status { get; }

    public int? ExpectedFieldCount { get; }

    public int CompleteLogicalRecords { get; }

    public int ExactWidthRecords { get; }

    public int ShortRecords { get; }

    public int ExtraRecords { get; }

    public int ConsistencyBasisPoints { get; }

    public int QuotedFields { get; }

    public int MultilineRecords { get; }

    /// <summary>A stable value-free rule ID when the candidate was rejected.</summary>
    public string? DiagnosticRuleId { get; }
}

public sealed class CsvDelimiterInspection
{
    internal CsvDelimiterInspection(
        CsvInspectionResolution resolution,
        CsvInspectionConfidence confidence,
        string? selectedDelimiter,
        string? suggestedDelimiter,
        CsvDelimiterCandidateEvidence[] candidates,
        bool logicalRecordLimitReached)
    {
        Resolution = resolution;
        Confidence = confidence;
        SelectedDelimiter = selectedDelimiter;
        SuggestedDelimiter = suggestedDelimiter;
        Candidates = Array.AsReadOnly(candidates);
        LogicalRecordLimitReached = logicalRecordLimitReached;
    }

    public CsvInspectionResolution Resolution { get; }

    public CsvInspectionConfidence Confidence { get; }

    /// <summary>Non-null only when the format is safe to use automatically.</summary>
    public string? SelectedDelimiter { get; }

    /// <summary>A low-confidence hint that still requires an explicit choice.</summary>
    public string? SuggestedDelimiter { get; }

    public ReadOnlyCollection<CsvDelimiterCandidateEvidence> Candidates { get; }

    public bool LogicalRecordLimitReached { get; }
}

/// <summary>Normalized semantic CSV settings included in source identity.</summary>
public sealed class CsvResolvedFormat
{
    internal CsvResolvedFormat(
        string delimiter,
        char quote,
        bool hasHeaderRecord,
        string encodingName,
        int encodingCodePage,
        bool hasByteOrderMark,
        string cultureName,
        string culturePolicyDigest,
        string? nullToken,
        bool nullTokenMatchesQuotedFields,
        int? expectedFieldCount)
    {
        Delimiter = delimiter;
        Quote = quote;
        HasHeaderRecord = hasHeaderRecord;
        EncodingName = encodingName;
        EncodingCodePage = encodingCodePage;
        HasByteOrderMark = hasByteOrderMark;
        CultureName = cultureName;
        CulturePolicyDigest = culturePolicyDigest;
        NullToken = nullToken;
        NullTokenMatchesQuotedFields = nullTokenMatchesQuotedFields;
        ExpectedFieldCount = expectedFieldCount;
    }

    public string Delimiter { get; }

    public char Quote { get; }

    public bool HasHeaderRecord { get; }

    public string EncodingName { get; }

    public int EncodingCodePage { get; }

    public bool HasByteOrderMark { get; }

    public string CultureName { get; }

    public string CulturePolicyDigest { get; }

    public string? NullToken { get; }

    public bool NullTokenMatchesQuotedFields { get; }

    public int? ExpectedFieldCount { get; }

    public string NewlinePolicy => "common-auto";
}

public sealed class CsvFormatInspection
{
    internal CsvFormatInspection(
        string snapshotIdentity,
        string contentDigest,
        long contentLength,
        CsvEncodingInspection encoding,
        CsvDelimiterInspection delimiter,
        CsvResolvedFormat? format,
        CsvReaderOptions? resolvedReaderOptions,
        int sampledBytes,
        bool sampleWasByteLimited)
    {
        SnapshotIdentity = snapshotIdentity;
        ContentDigest = contentDigest;
        ContentLength = contentLength;
        Encoding = encoding;
        Delimiter = delimiter;
        Format = format;
        ResolvedReaderOptions = resolvedReaderOptions;
        SampledBytes = sampledBytes;
        SampleWasByteLimited = sampleWasByteLimited;
    }

    public const string AlgorithmId = "csharpdb-csv-inspect-v1";

    public string SnapshotIdentity { get; }

    public string ContentDigest { get; }

    public long ContentLength { get; }

    public CsvEncodingInspection Encoding { get; }

    public CsvDelimiterInspection Delimiter { get; }

    public CsvResolvedFormat? Format { get; }

    public int SampledBytes { get; }

    public bool SampleWasByteLimited { get; }

    internal CsvReaderOptions? ResolvedReaderOptions { get; }
}
