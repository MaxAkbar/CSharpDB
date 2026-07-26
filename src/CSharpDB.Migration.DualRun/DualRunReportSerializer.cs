using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CSharpDB.Migration.DualRun;

public static class DualRunReportSerializer
{
    private const int MaxReportCharacters = 16 * 1024 * 1024;
    private const int MaxJsonDepth = 64;
    private const int HardMaxRows = 1_000_000;
    private const int HardMaxColumns = 1_024;
    private const int HardMaxCellBytes = 64 * 1024 * 1024;
    private const long HardMaxTotalBytes = 1024L * 1024 * 1024;
    private const int HardMaxMismatchDetails = 1_000;
    private const long HardMaxTimeoutMilliseconds = 10 * 60 * 1_000;
    private static readonly JsonSerializerOptions CompactOptions = CreateOptions(writeIndented: false);
    private static readonly JsonSerializerOptions IndentedOptions = CreateOptions(writeIndented: true);

    public static string Serialize(DualRunReport report, bool writeIndented = true)
    {
        ArgumentNullException.ThrowIfNull(report);
        ValidateReport(report);

        string digest = ComputeDigest(report);
        var envelope = new DualRunReportEnvelope
        {
            Format = DualRunReportFormats.V1,
            DigestAlgorithm = DualRunReportFormats.DigestAlgorithm,
            Digest = digest,
            Payload = report,
        };
        string json = JsonSerializer.Serialize(
            envelope,
            writeIndented ? IndentedOptions : CompactOptions);
        if (json.Length > MaxReportCharacters)
            throw new InvalidDataException($"Dual-run report exceeds {MaxReportCharacters} characters.");
        return json;
    }

    public static DualRunReport Deserialize(string json)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(json);
        if (json.Length > MaxReportCharacters)
            throw new InvalidDataException($"Dual-run report exceeds {MaxReportCharacters} characters.");

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(json, new JsonDocumentOptions
            {
                AllowTrailingCommas = false,
                CommentHandling = JsonCommentHandling.Disallow,
                MaxDepth = MaxJsonDepth,
            });
        }
        catch (JsonException ex)
        {
            throw new InvalidDataException("Dual-run report JSON is invalid.", ex);
        }

        using (document)
        {
            RejectDuplicateProperties(document.RootElement, "$");
            DualRunReportEnvelope envelope;
            try
            {
                envelope = document.RootElement.Deserialize<DualRunReportEnvelope>(CompactOptions)
                    ?? throw new InvalidDataException("Dual-run report envelope is missing.");
            }
            catch (JsonException ex)
            {
                throw new InvalidDataException("Dual-run report JSON is invalid.", ex);
            }

            if (!string.Equals(envelope.Format, DualRunReportFormats.V1, StringComparison.Ordinal))
                throw new InvalidDataException($"Unsupported dual-run report format '{envelope.Format}'.");
            if (!string.Equals(
                    envelope.DigestAlgorithm,
                    DualRunReportFormats.DigestAlgorithm,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    $"Unsupported dual-run digest algorithm '{envelope.DigestAlgorithm}'.");
            }

            ValidateReport(envelope.Payload);
            string expected = ComputeDigest(envelope.Payload);
            if (!IsSha256(envelope.Digest) ||
                !CryptographicOperations.FixedTimeEquals(
                    Convert.FromHexString(envelope.Digest),
                    Convert.FromHexString(expected)))
            {
                throw new InvalidDataException("Dual-run report digest verification failed.");
            }

            return envelope.Payload;
        }
    }

    private static string ComputeDigest(DualRunReport report)
    {
        byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(
            new DigestInput
            {
                Format = DualRunReportFormats.V1,
                DigestAlgorithm = DualRunReportFormats.DigestAlgorithm,
                Payload = report,
            },
            CompactOptions);
        return Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
    }

    private static bool IsSha256(string? value) =>
        value is { Length: 64 } && value.All(Uri.IsHexDigit);

    private static void ValidateReport(DualRunReport report)
    {
        if (report is null)
            throw new InvalidDataException("The dual-run report payload is missing.");
        RequireVisibleId(report.CaseId, nameof(report.CaseId), 128);
        if (!string.Equals(
                report.CanonicalizationId,
                DualRunReportFormats.CanonicalizationId,
                StringComparison.Ordinal) ||
            !string.Equals(
                report.CanonicalizationContractHash,
                DualRunReportFormats.CanonicalizationContractHash,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The report canonicalization contract does not match this binary.");
        }
        RequireDigest(report.InvocationDigest, nameof(report.InvocationDigest));
        if (!Enum.IsDefined(report.Ordering))
            throw new InvalidDataException("The report ordering mode is invalid.");
        if (!Enum.IsDefined(report.Status))
            throw new InvalidDataException("The report validation status is invalid.");
        if (report.Limits is null ||
            report.Source is null ||
            report.Target is null ||
            report.Differences is null)
        {
            throw new InvalidDataException("The dual-run report is missing required evidence.");
        }
        ValidateLimits(report.Limits);
        ValidateEndpoint(report.Source, report.Limits);
        ValidateEndpoint(report.Target, report.Limits);
        ValidateDifferences(report);
        ValidateStatusInvariants(report);
    }

    private static void ValidateEndpoint(
        DualRunEndpointEvidence endpoint,
        DualRunReportLimits limits)
    {
        RequireVisibleId(endpoint.ProviderId, nameof(endpoint.ProviderId), 128);
        RequireSnapshotIdentity(endpoint.SnapshotIdentity);
        RequireVisibleId(
            endpoint.ReadOnlyValidatorId,
            nameof(endpoint.ReadOnlyValidatorId),
            128);
        if (!Enum.IsDefined(endpoint.ReadOnlyEnforcement))
            throw new InvalidDataException("The endpoint read-only enforcement mode is invalid.");
        if (!Enum.IsDefined(endpoint.Status))
            throw new InvalidDataException("The endpoint status is invalid.");

        if (endpoint.Status == DualRunEndpointStatus.Succeeded)
        {
            if (endpoint.ColumnCount is null or < 0 ||
                endpoint.ColumnCount > limits.MaxColumns ||
                endpoint.RowCount is null or < 0 ||
                endpoint.RowCount > limits.MaxRows)
            {
                throw new InvalidDataException("Successful endpoint evidence is missing bounded counts.");
            }
            RequireDigest(endpoint.SchemaDigest, nameof(endpoint.SchemaDigest));
            RequireDigest(endpoint.ResultDigest, nameof(endpoint.ResultDigest));
            if (endpoint.Error is not null)
                throw new InvalidDataException("Successful endpoint evidence cannot contain an error.");
        }
        else
        {
            if (endpoint.ColumnCount is not null ||
                endpoint.RowCount is not null ||
                endpoint.SchemaDigest is not null ||
                endpoint.ResultDigest is not null)
            {
                throw new InvalidDataException(
                    "Failed endpoint evidence cannot contain successful result evidence.");
            }
            if (endpoint.Error is null)
                throw new InvalidDataException("Failed endpoint evidence must contain a stable error.");
            if (!Enum.IsDefined(endpoint.Error.Kind))
                throw new InvalidDataException("The endpoint error kind is invalid.");
            RequireStableCode(endpoint.Error.Code, nameof(endpoint.Error.Code));
        }
    }

    private static void ValidateLimits(DualRunReportLimits limits)
    {
        if (limits.MaxRows < 1 || limits.MaxRows > HardMaxRows ||
            limits.MaxColumns < 1 || limits.MaxColumns > HardMaxColumns ||
            limits.MaxCellBytes < 1 || limits.MaxCellBytes > HardMaxCellBytes ||
            limits.MaxTotalCanonicalBytesPerEndpoint < limits.MaxCellBytes ||
            limits.MaxTotalCanonicalBytesPerEndpoint > HardMaxTotalBytes ||
            limits.MaxMismatchDetails < 1 ||
            limits.MaxMismatchDetails > HardMaxMismatchDetails ||
            limits.TimeoutPerEndpointMilliseconds < 1 ||
            limits.TimeoutPerEndpointMilliseconds > HardMaxTimeoutMilliseconds)
        {
            throw new InvalidDataException("The report contains invalid or unbounded limits.");
        }
    }

    private static void ValidateDifferences(DualRunReport report)
    {
        if (report.Differences.Count > report.Limits.MaxMismatchDetails + 3)
            throw new InvalidDataException("The report exceeds its bounded difference-detail limit.");

        int rowDifferences = 0;
        var unique = new HashSet<string>(StringComparer.Ordinal);
        foreach (DualRunDifference difference in report.Differences)
        {
            if (difference is null)
                throw new InvalidDataException("The report contains a missing difference.");

            string key;
            switch (difference.Code)
            {
                case DualRunDifferenceCodes.EndpointFailed:
                    RequireEndpointName(difference.Endpoint);
                    RequireNullDifferenceFields(
                        difference,
                        allowEndpoint: true,
                        allowRowOrdinal: false,
                        allowRowDigest: false,
                        allowCounts: false);
                    key = $"{difference.Code}:{difference.Endpoint}";
                    break;

                case DualRunDifferenceCodes.SchemaMismatch:
                    if (difference.Endpoint is not null)
                        RequireEndpointName(difference.Endpoint);
                    RequireNullDifferenceFields(
                        difference,
                        allowEndpoint: true,
                        allowRowOrdinal: false,
                        allowRowDigest: false,
                        allowCounts: false);
                    key = $"{difference.Code}:{difference.Endpoint ?? "both"}";
                    break;

                case DualRunDifferenceCodes.RowCountMismatch:
                    if (difference.Endpoint is not null ||
                        difference.RowOrdinal is not null ||
                        difference.RowDigest is not null ||
                        difference.SourceCount is null or < 0 ||
                        difference.TargetCount is null or < 0 ||
                        difference.SourceCount == difference.TargetCount)
                    {
                        throw new InvalidDataException("The row-count mismatch evidence is invalid.");
                    }
                    key = difference.Code;
                    break;

                case DualRunDifferenceCodes.OrderedRowMismatch:
                    if (report.Ordering != DualRunOrdering.Ordered ||
                        difference.Endpoint is not null ||
                        difference.RowOrdinal is null or < 0 ||
                        difference.RowOrdinal >= report.Limits.MaxRows ||
                        difference.SourceCount is not null ||
                        difference.TargetCount is not null)
                    {
                        throw new InvalidDataException("The ordered-row mismatch evidence is invalid.");
                    }
                    RequireDigest(difference.RowDigest, nameof(difference.RowDigest));
                    rowDifferences++;
                    key = $"{difference.Code}:{difference.RowOrdinal}";
                    break;

                case DualRunDifferenceCodes.UnorderedRowMultiplicityMismatch:
                    if (report.Ordering != DualRunOrdering.Unordered ||
                        difference.Endpoint is not null ||
                        difference.RowOrdinal is not null ||
                        difference.SourceCount is null or < 0 ||
                        difference.TargetCount is null or < 0 ||
                        difference.SourceCount == difference.TargetCount)
                    {
                        throw new InvalidDataException(
                            "The unordered-row mismatch evidence is invalid.");
                    }
                    RequireDigest(difference.RowDigest, nameof(difference.RowDigest));
                    rowDifferences++;
                    key = $"{difference.Code}:{difference.RowDigest}";
                    break;

                default:
                    throw new InvalidDataException(
                        $"Unsupported dual-run difference code '{difference.Code}'.");
            }

            if (!unique.Add(key))
                throw new InvalidDataException("The report contains duplicate difference evidence.");
        }

        if (rowDifferences > report.Limits.MaxMismatchDetails)
            throw new InvalidDataException("The report exceeds its row-mismatch detail limit.");
    }

    private static void ValidateStatusInvariants(DualRunReport report)
    {
        bool sourceSucceeded = report.Source.Status == DualRunEndpointStatus.Succeeded;
        bool targetSucceeded = report.Target.Status == DualRunEndpointStatus.Succeeded;

        switch (report.Status)
        {
            case DualRunValidationStatus.Passed:
                if (!sourceSucceeded ||
                    !targetSucceeded ||
                    report.Differences.Count != 0 ||
                    !string.Equals(
                        report.Source.SchemaDigest,
                        report.Target.SchemaDigest,
                        StringComparison.Ordinal) ||
                    !string.Equals(
                        report.Source.ResultDigest,
                        report.Target.ResultDigest,
                        StringComparison.Ordinal))
                {
                    throw new InvalidDataException(
                        "A Passed report must contain two equal successful endpoint results.");
                }
                break;

            case DualRunValidationStatus.Different:
                if (!sourceSucceeded || !targetSucceeded || report.Differences.Count == 0)
                {
                    throw new InvalidDataException(
                        "A Different report must contain two successful endpoints and mismatch evidence.");
                }
                if (report.Differences.Any(static difference =>
                        difference.Code == DualRunDifferenceCodes.EndpointFailed))
                {
                    throw new InvalidDataException(
                        "A Different report cannot contain endpoint-failure evidence.");
                }
                ValidateSuccessfulDifferenceConsistency(report);
                break;

            case DualRunValidationStatus.Inconclusive:
                ValidateInconclusiveDifferences(report, sourceSucceeded, targetSucceeded);
                break;

            default:
                throw new InvalidDataException("The report validation status is invalid.");
        }
    }

    private static void ValidateSuccessfulDifferenceConsistency(DualRunReport report)
    {
        bool countsDiffer = report.Source.RowCount != report.Target.RowCount;
        DualRunDifference? countDifference = report.Differences.SingleOrDefault(
            static difference => difference.Code == DualRunDifferenceCodes.RowCountMismatch);
        if (countsDiffer)
        {
            if (countDifference is null ||
                countDifference.SourceCount != report.Source.RowCount ||
                countDifference.TargetCount != report.Target.RowCount)
            {
                throw new InvalidDataException(
                    "The row-count mismatch does not match the endpoint evidence.");
            }
        }
        else if (countDifference is not null)
        {
            throw new InvalidDataException(
                "The report claims a row-count mismatch for equal endpoint counts.");
        }

        bool schemasDiffer = !string.Equals(
            report.Source.SchemaDigest,
            report.Target.SchemaDigest,
            StringComparison.Ordinal);
        bool hasSchemaDifference = report.Differences.Any(
            static difference => difference.Code == DualRunDifferenceCodes.SchemaMismatch);
        if (schemasDiffer && !hasSchemaDifference)
            throw new InvalidDataException("The schema mismatch evidence is missing.");

        bool resultsDiffer = !string.Equals(
            report.Source.ResultDigest,
            report.Target.ResultDigest,
            StringComparison.Ordinal);
        if (resultsDiffer &&
            !countsDiffer &&
            !hasSchemaDifference &&
            !report.Differences.Any(static difference =>
                difference.Code is DualRunDifferenceCodes.OrderedRowMismatch or
                    DualRunDifferenceCodes.UnorderedRowMultiplicityMismatch))
        {
            throw new InvalidDataException("The result mismatch evidence is missing.");
        }
    }

    private static void ValidateInconclusiveDifferences(
        DualRunReport report,
        bool sourceSucceeded,
        bool targetSucceeded)
    {
        if (sourceSucceeded && targetSucceeded)
            throw new InvalidDataException("An Inconclusive report must contain a failed endpoint.");
        if (report.Differences.Any(static difference =>
                difference.Code != DualRunDifferenceCodes.EndpointFailed))
        {
            throw new InvalidDataException(
                "An Inconclusive report can contain only endpoint-failure evidence.");
        }

        string[] expected = (sourceSucceeded, targetSucceeded) switch
        {
            (false, false) => ["source", "target"],
            (false, true) => ["source"],
            (true, false) => ["target"],
            _ => [],
        };
        string[] actual = report.Differences
            .Select(static difference => difference.Endpoint!)
            .Order(StringComparer.Ordinal)
            .ToArray();
        if (!actual.SequenceEqual(expected.Order(StringComparer.Ordinal), StringComparer.Ordinal))
        {
            throw new InvalidDataException(
                "Endpoint-failure differences do not match the endpoint statuses.");
        }
    }

    private static void RequireNullDifferenceFields(
        DualRunDifference difference,
        bool allowEndpoint,
        bool allowRowOrdinal,
        bool allowRowDigest,
        bool allowCounts)
    {
        if ((!allowEndpoint && difference.Endpoint is not null) ||
            (!allowRowOrdinal && difference.RowOrdinal is not null) ||
            (!allowRowDigest && difference.RowDigest is not null) ||
            (!allowCounts &&
             (difference.SourceCount is not null || difference.TargetCount is not null)))
        {
            throw new InvalidDataException("The difference contains fields invalid for its code.");
        }
    }

    private static void RequireEndpointName(string? value)
    {
        if (value is not ("source" or "target"))
            throw new InvalidDataException("The difference endpoint must be source or target.");
    }

    private static void RequireVisibleId(string? value, string name, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value) ||
            value.Length > maxLength ||
            value.Any(char.IsControl))
        {
            throw new InvalidDataException(
                $"{name} must be 1-{maxLength} visible characters.");
        }
    }

    private static void RequireSnapshotIdentity(string? value)
    {
        if (string.IsNullOrWhiteSpace(value) ||
            value.Length > 512 ||
            value.Any(static character =>
                char.IsControl(character) || char.IsWhiteSpace(character)))
        {
            throw new InvalidDataException(
                "Snapshot identities must be 1-512 non-whitespace visible characters.");
        }
    }

    private static void RequireStableCode(string? value, string name)
    {
        if (string.IsNullOrWhiteSpace(value) ||
            value.Length > 128 ||
            value.Any(static character =>
                !(char.IsAsciiLetterOrDigit(character) ||
                  character is '_' or '-' or '.')))
        {
            throw new InvalidDataException($"{name} is not a stable diagnostic code.");
        }
    }

    private static void RequireDigest(string? value, string name)
    {
        if (value is null || !IsSha256(value))
            throw new InvalidDataException($"{name} must be a lowercase or uppercase SHA-256 digest.");
    }

    private static void RejectDuplicateProperties(JsonElement element, string path)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
            {
                var names = new HashSet<string>(StringComparer.Ordinal);
                foreach (JsonProperty property in element.EnumerateObject())
                {
                    if (!names.Add(property.Name))
                        throw new InvalidDataException($"Duplicate JSON property at {path}.{property.Name}.");
                    RejectDuplicateProperties(property.Value, $"{path}.{property.Name}");
                }
                break;
            }
            case JsonValueKind.Array:
            {
                int index = 0;
                foreach (JsonElement item in element.EnumerateArray())
                    RejectDuplicateProperties(item, $"{path}[{index++}]");
                break;
            }
        }
    }

    private static JsonSerializerOptions CreateOptions(bool writeIndented)
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = false,
            WriteIndented = writeIndented,
            MaxDepth = MaxJsonDepth,
            UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
        };
        options.Converters.Add(new JsonStringEnumConverter(
            namingPolicy: null,
            allowIntegerValues: false));
        return options;
    }

    private sealed record DigestInput
    {
        public required string Format { get; init; }

        public required string DigestAlgorithm { get; init; }

        public required DualRunReport Payload { get; init; }
    }
}
