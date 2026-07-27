using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CSharpDB.Migration;

/// <summary>
/// Serializes deterministic, self-digesting migration validation reports.
/// </summary>
public static class MigrationValidationReportSerializer
{
    /// <summary>
    /// Maximum UTF-8 file size accepted before a validation report is read
    /// into memory. The parsed character count is capped at the same value.
    /// </summary>
    public const int MaximumArtifactBytes = 256 * 1024 * 1024;

    private const int MaxArtifactCharacters = MaximumArtifactBytes;
    private const int MaxJsonDepth = 64;
    private const int MaxPartitionId = 255;

    private static readonly JsonSerializerOptions s_compactOptions = CreateOptions(writeIndented: false);
    private static readonly JsonSerializerOptions s_indentedOptions = CreateOptions(writeIndented: true);

    public static MigrationValidationReport Normalize(MigrationValidationReport report)
    {
        ArgumentNullException.ThrowIfNull(report);

        MigrationValidationBinding binding = report.Binding
            ?? throw Invalid("Validation report binding is required.");
        MigrationSnapshotConsistencyEvidence snapshotConsistency = report.SnapshotConsistency
            ?? throw Invalid("Validation report snapshot-consistency evidence is required.");
        MigrationSchemaValidationEvidence schema = report.Schema
            ?? throw Invalid("Validation report schema evidence is required.");

        MigrationSchemaDifferenceEvidence[] differences = RequireList(
                schema.Differences,
                "Schema differences")
            .Select(NormalizeDifference)
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .ThenBy(item => item.Kind)
            .ToArray();

        MigrationObjectValidationEvidence[] objects = RequireList(
                report.Objects,
                "Validation object evidence")
            .Select(NormalizeObject)
            .OrderBy(item => item.SourceObjectId, StringComparer.Ordinal)
            .ThenBy(item => item.TargetObjectId, StringComparer.Ordinal)
            .ToArray();

        MigrationValidationDiagnosticEvidence[] diagnostics = RequireList(
                report.Diagnostics,
                "Validation diagnostics")
            .Select(NormalizeDiagnostic)
            .OrderBy(item => item.DiagnosticId, StringComparer.Ordinal)
            .ThenBy(item => item.RuleId, StringComparer.Ordinal)
            .ToArray();

        var normalized = report with
        {
            Binding = binding with
            {
                TargetCSharpDbVersion = RequireText(
                    binding.TargetCSharpDbVersion,
                    "Target CSharpDB version"),
                PlanDigest = NormalizeSha256(binding.PlanDigest, "Plan digest"),
                CatalogDigest = NormalizeSha256(binding.CatalogDigest, "Catalog digest"),
                CapabilityDigest = NormalizeSha256(binding.CapabilityDigest, "Capability digest"),
                SourceIdentity = RequireText(binding.SourceIdentity, "Source identity"),
                SourceFingerprint = RequireText(binding.SourceFingerprint, "Source fingerprint"),
                TargetIdentity = RequireText(binding.TargetIdentity, "Target identity"),
                SourceSnapshotIdentity = RequireText(
                    binding.SourceSnapshotIdentity,
                    "Source snapshot identity"),
                TargetSnapshotIdentity = RequireText(
                    binding.TargetSnapshotIdentity,
                    "Target snapshot identity"),
                CanonicalizationVersion = RequireText(
                    binding.CanonicalizationVersion,
                    "Canonicalization version"),
                CanonicalizationContractDigest = NormalizeSha256(
                    binding.CanonicalizationContractDigest,
                    "Canonicalization contract digest"),
            },
            SnapshotConsistency = snapshotConsistency,
            Schema = schema with
            {
                SourceSchemaDigest = NormalizeSha256(
                    schema.SourceSchemaDigest,
                    "Source normalized schema digest"),
                TargetSchemaDigest = NormalizeSha256(
                    schema.TargetSchemaDigest,
                    "Target normalized schema digest"),
                Differences = differences,
            },
            Objects = objects,
            Diagnostics = diagnostics,
        };

        Validate(normalized);
        MigrationArtifactSerializer.ValidateNoSecrets(
            JsonSerializer.SerializeToElement(normalized, s_compactOptions));
        return normalized;
    }

    public static string Serialize(MigrationValidationReport report, bool writeIndented = true)
    {
        MigrationValidationReport normalized = Normalize(report);
        JsonElement payload = JsonSerializer.SerializeToElement(normalized, s_compactOptions);
        string digest = ComputeDigest(payload);
        var envelope = new MigrationArtifactEnvelope<JsonElement>
        {
            Format = MigrationArtifactFormats.ValidationReportV1,
            DigestAlgorithm = MigrationArtifactFormats.DigestAlgorithm,
            Digest = digest,
            Payload = payload,
        };

        return JsonSerializer.Serialize(
            envelope,
            writeIndented ? s_indentedOptions : s_compactOptions);
    }

    public static MigrationValidationReport Deserialize(string json)
    {
        JsonElement payload = ReadVerifiedPayload(json);
        MigrationValidationReport supplied;
        try
        {
            supplied = payload.Deserialize<MigrationValidationReport>(s_compactOptions)
                ?? throw Invalid("Validation report payload is missing.");
        }
        catch (JsonException ex)
        {
            throw Invalid("Validation report payload is invalid.", ex);
        }

        MigrationValidationReport normalized = Normalize(supplied);
        RequireCanonicalPayload(payload, normalized);
        return normalized;
    }

    public static string ComputeDigest(MigrationValidationReport report)
    {
        MigrationValidationReport normalized = Normalize(report);
        JsonElement payload = JsonSerializer.SerializeToElement(normalized, s_compactOptions);
        return ComputeDigest(payload);
    }

    private static MigrationSchemaDifferenceEvidence NormalizeDifference(
        MigrationSchemaDifferenceEvidence difference)
    {
        if (difference is null)
            throw Invalid("Schema differences cannot contain null entries.");

        return difference with
        {
            ObjectId = RequireText(difference.ObjectId, "Schema difference object identity"),
            SourceDefinitionDigest = NormalizeOptionalSha256(
                difference.SourceDefinitionDigest,
                "Source schema definition digest"),
            TargetDefinitionDigest = NormalizeOptionalSha256(
                difference.TargetDefinitionDigest,
                "Target schema definition digest"),
        };
    }

    private static MigrationObjectValidationEvidence NormalizeObject(
        MigrationObjectValidationEvidence evidence)
    {
        if (evidence is null)
            throw Invalid("Validation object evidence cannot contain null entries.");

        MigrationValidationPartitionEvidence[] partitions = RequireList(
                evidence.Partitions,
                $"Validation partitions for object '{evidence.SourceObjectId}'")
            .Select(NormalizePartition)
            .OrderBy(item => item.PartitionId)
            .ToArray();

        return evidence with
        {
            SourceObjectId = RequireText(evidence.SourceObjectId, "Source object identity"),
            TargetObjectId = RequireText(evidence.TargetObjectId, "Target object identity"),
            CanonicalTypeContractDigest = NormalizeSha256(
                evidence.CanonicalTypeContractDigest,
                "Canonical type-contract digest"),
            ObjectContractDigest = NormalizeSha256(
                evidence.ObjectContractDigest,
                "Validation object-contract digest"),
            SourceChecksum = NormalizeOptionalSha256(evidence.SourceChecksum, "Source object checksum"),
            TargetChecksum = NormalizeOptionalSha256(evidence.TargetChecksum, "Target object checksum"),
            Partitions = partitions,
        };
    }

    private static MigrationValidationPartitionEvidence NormalizePartition(
        MigrationValidationPartitionEvidence partition)
    {
        if (partition is null)
            throw Invalid("Validation partitions cannot contain null entries.");

        MigrationValidationMismatchEvidence[] mismatches = RequireList(
                partition.Mismatches,
                $"Validation mismatches for partition '{partition.PartitionId}'")
            .Select(NormalizeMismatch)
            .OrderBy(item => item.KeyHash is null ? 0 : 1)
            .ThenBy(item => item.KeyHash, StringComparer.Ordinal)
            .ThenBy(item => item.Kind)
            .ThenBy(item => item.SourceRowHash, StringComparer.Ordinal)
            .ThenBy(item => item.TargetRowHash, StringComparer.Ordinal)
            .ThenBy(item => item.SourceMultiplicity)
            .ThenBy(item => item.TargetMultiplicity)
            .ToArray();

        return partition with
        {
            SourceDigest = NormalizeSha256(partition.SourceDigest, "Source partition digest"),
            TargetDigest = NormalizeSha256(partition.TargetDigest, "Target partition digest"),
            Mismatches = mismatches,
        };
    }

    private static MigrationValidationMismatchEvidence NormalizeMismatch(
        MigrationValidationMismatchEvidence mismatch)
    {
        if (mismatch is null)
            throw Invalid("Validation mismatches cannot contain null entries.");

        return mismatch with
        {
            KeyHash = NormalizeOptionalSha256(mismatch.KeyHash, "Validation mismatch key hash"),
            SourceRowHash = NormalizeOptionalSha256(
                mismatch.SourceRowHash,
                "Validation mismatch source row hash"),
            TargetRowHash = NormalizeOptionalSha256(
                mismatch.TargetRowHash,
                "Validation mismatch target row hash"),
        };
    }

    private static MigrationValidationDiagnosticEvidence NormalizeDiagnostic(
        MigrationValidationDiagnosticEvidence diagnostic)
    {
        if (diagnostic is null)
            throw Invalid("Validation diagnostics cannot contain null entries.");

        return diagnostic with
        {
            DiagnosticId = RequireText(diagnostic.DiagnosticId, "Validation diagnostic identity"),
            RuleId = RequireText(diagnostic.RuleId, "Validation diagnostic rule identity"),
            ObjectId = NormalizeOptionalText(diagnostic.ObjectId, "Validation diagnostic object identity"),
        };
    }

    private static void Validate(MigrationValidationReport report)
    {
        if (report.Level is < MigrationValidationLevel.Schema or > MigrationValidationLevel.Checksum)
            throw Invalid($"Validation report level '{report.Level}' is not supported by this format version.");
        if (!Enum.IsDefined(report.SnapshotConsistency.Status))
            throw Invalid("Validation report snapshot-consistency status is undefined.");

        MigrationSchemaDifferenceEvidence[] differences = report.Schema.Differences.ToArray();
        RequireUnique(
            differences,
            item => item.ObjectId,
            "schema difference object identity");
        foreach (MigrationSchemaDifferenceEvidence difference in differences)
            ValidateDifference(difference);

        MigrationObjectValidationEvidence[] objects = report.Objects.ToArray();
        RequireUnique(objects, item => item.SourceObjectId, "source validation object identity");
        RequireUnique(objects, item => item.TargetObjectId, "target validation object identity");

        foreach (MigrationObjectValidationEvidence evidence in objects)
            ValidateObject(report.Level, evidence);

        MigrationValidationDiagnosticEvidence[] diagnostics = report.Diagnostics.ToArray();
        RequireUnique(diagnostics, item => item.DiagnosticId, "validation diagnostic identity");
        ValidateDiagnosticReferences(objects, diagnostics);
        ValidateSchemaCoherence(report.Schema);
        ValidateOutcomeCoherence(report);
    }

    private static void ValidateSchemaCoherence(MigrationSchemaValidationEvidence schema)
    {
        bool passed = schema.Differences.Count == 0 && string.Equals(
            schema.SourceSchemaDigest,
            schema.TargetSchemaDigest,
            StringComparison.Ordinal);
        MigrationValidationStatus expected = passed
            ? MigrationValidationStatus.Passed
            : MigrationValidationStatus.Different;
        if (schema.Status != expected)
        {
            throw Invalid(
                $"Schema validation status '{schema.Status}' contradicts its digests or differences.");
        }
    }

    private static void ValidateOutcomeCoherence(MigrationValidationReport report)
    {
        MigrationValidationStatus expected = report.Objects.Any(item =>
                item.Status == MigrationValidationStatus.Error)
            ? MigrationValidationStatus.Error
            : report.SnapshotConsistency.Status != MigrationSnapshotConsistencyStatus.Established
                ? MigrationValidationStatus.Inconclusive
                : report.Schema.Status == MigrationValidationStatus.Different ||
                  report.Objects.Any(item => item.Status == MigrationValidationStatus.Different)
                    ? MigrationValidationStatus.Different
                    : MigrationValidationStatus.Passed;
        if (report.Outcome != expected)
        {
            throw Invalid(
                $"Validation report outcome '{report.Outcome}' contradicts its consistency, schema, or object evidence; expected '{expected}'.");
        }
    }

    private static void ValidateDifference(MigrationSchemaDifferenceEvidence difference)
    {
        switch (difference.Kind)
        {
            case MigrationSchemaDifferenceKind.MissingFromSource:
                if (difference.SourceDefinitionDigest is not null ||
                    difference.TargetDefinitionDigest is null)
                {
                    throw Invalid(
                        $"Schema object '{difference.ObjectId}' missing from the source must carry only the target definition digest.");
                }

                break;

            case MigrationSchemaDifferenceKind.MissingFromTarget:
                if (difference.SourceDefinitionDigest is null ||
                    difference.TargetDefinitionDigest is not null)
                {
                    throw Invalid(
                        $"Schema object '{difference.ObjectId}' missing from the target must carry only the source definition digest.");
                }

                break;

            case MigrationSchemaDifferenceKind.DefinitionMismatch:
                if (difference.SourceDefinitionDigest is null ||
                    difference.TargetDefinitionDigest is null)
                {
                    throw Invalid(
                        $"Schema object '{difference.ObjectId}' with a definition mismatch requires both definition digests.");
                }

                if (string.Equals(
                        difference.SourceDefinitionDigest,
                        difference.TargetDefinitionDigest,
                        StringComparison.Ordinal))
                {
                    throw Invalid(
                        $"Schema object '{difference.ObjectId}' is marked different but its definition digests match.");
                }

                break;

            default:
                throw Invalid($"Schema object '{difference.ObjectId}' has an unknown difference kind.");
        }
    }

    private static void ValidateObject(
        MigrationValidationLevel level,
        MigrationObjectValidationEvidence evidence)
    {
        if (evidence.SourceRowCount is < 0 || evidence.TargetRowCount is < 0)
            throw Invalid($"Validation object '{evidence.SourceObjectId}' has a negative row count.");

        MigrationValidationPartitionEvidence[] partitions = evidence.Partitions.ToArray();
        RequireUnique(
            partitions,
            item => item.PartitionId,
            $"partition identity for validation object '{evidence.SourceObjectId}'");

        foreach (MigrationValidationPartitionEvidence partition in partitions)
        {
            if (partition.PartitionId is < 0 or > MaxPartitionId)
            {
                throw Invalid(
                    $"Validation object '{evidence.SourceObjectId}' has partition '{partition.PartitionId}' outside the supported range 0..{MaxPartitionId}.");
            }

            if (partition.SourceRowCount < 0 || partition.TargetRowCount < 0)
            {
                throw Invalid(
                    $"Validation object '{evidence.SourceObjectId}' partition '{partition.PartitionId}' has a negative row count.");
            }

            MigrationValidationMismatchEvidence[] mismatches = partition.Mismatches.ToArray();
            RequireUnique(
                mismatches,
                MismatchIdentity,
                $"mismatch identity for validation object '{evidence.SourceObjectId}' partition '{partition.PartitionId}'");
            foreach (MigrationValidationMismatchEvidence mismatch in mismatches)
                ValidateMismatch(evidence.SourceObjectId, partition.PartitionId, mismatch);

            bool partitionPassed = partition.SourceRowCount == partition.TargetRowCount &&
                string.Equals(partition.SourceDigest, partition.TargetDigest, StringComparison.Ordinal);
            MigrationValidationStatus expectedPartitionStatus = partitionPassed
                ? MigrationValidationStatus.Passed
                : MigrationValidationStatus.Different;
            if (partition.Status != expectedPartitionStatus)
            {
                throw Invalid(
                    $"Validation object '{evidence.SourceObjectId}' partition '{partition.PartitionId}' status contradicts its counts or digests.");
            }
            if (partitionPassed && mismatches.Length != 0)
            {
                throw Invalid(
                    $"Validation object '{evidence.SourceObjectId}' partition '{partition.PartitionId}' passed but contains mismatch evidence.");
            }
        }

        ValidateObjectLevelEvidence(level, evidence, partitions);
    }

    private static void ValidateObjectLevelEvidence(
        MigrationValidationLevel level,
        MigrationObjectValidationEvidence evidence,
        IReadOnlyList<MigrationValidationPartitionEvidence> partitions)
    {
        switch (level)
        {
            case MigrationValidationLevel.Schema:
                if (evidence.Status != MigrationValidationStatus.Skipped ||
                    evidence.SourceRowCount is not null || evidence.TargetRowCount is not null ||
                    evidence.SourceChecksum is not null || evidence.TargetChecksum is not null ||
                    partitions.Count != 0)
                {
                    throw Invalid(
                        $"Schema-level validation object '{evidence.SourceObjectId}' contains count or checksum evidence.");
                }
                return;

            case MigrationValidationLevel.Count:
                if (evidence.SourceRowCount is null || evidence.TargetRowCount is null ||
                    evidence.SourceChecksum is not null || evidence.TargetChecksum is not null ||
                    partitions.Count != 0)
                {
                    throw Invalid(
                        $"Count-level validation object '{evidence.SourceObjectId}' has incomplete or excess evidence.");
                }
                MigrationValidationStatus expectedCountStatus =
                    evidence.SourceRowCount == evidence.TargetRowCount
                        ? MigrationValidationStatus.Passed
                        : MigrationValidationStatus.Different;
                if (evidence.Status != expectedCountStatus)
                {
                    throw Invalid(
                        $"Count-level validation object '{evidence.SourceObjectId}' status contradicts its row counts.");
                }
                return;

            case MigrationValidationLevel.Checksum:
                ValidateChecksumObject(evidence, partitions);
                return;

            default:
                throw Invalid($"Validation level '{level}' is not supported by this report format.");
        }
    }

    private static void ValidateChecksumObject(
        MigrationObjectValidationEvidence evidence,
        IReadOnlyList<MigrationValidationPartitionEvidence> partitions)
    {
        if (evidence.SourceRowCount is null || evidence.TargetRowCount is null ||
            evidence.SourceChecksum is null || evidence.TargetChecksum is null ||
            partitions.Count != MaxPartitionId + 1 ||
            partitions.Where((partition, index) => partition.PartitionId != index).Any())
        {
            throw Invalid(
                $"Checksum validation object '{evidence.SourceObjectId}' must contain counts, checksums, and all 256 ordered partitions.");
        }

        long partitionSourceCount;
        long partitionTargetCount;
        try
        {
            partitionSourceCount = partitions.Aggregate(
                0L,
                (total, partition) => checked(total + partition.SourceRowCount));
            partitionTargetCount = partitions.Aggregate(
                0L,
                (total, partition) => checked(total + partition.TargetRowCount));
        }
        catch (OverflowException ex)
        {
            throw Invalid(
                $"Checksum validation object '{evidence.SourceObjectId}' partition counts overflow Int64.",
                ex);
        }

        bool countCoherent = partitionSourceCount == evidence.SourceRowCount &&
            partitionTargetCount == evidence.TargetRowCount;
        MigrationValidationStatus expected = !countCoherent
            ? MigrationValidationStatus.Error
            : evidence.SourceRowCount == evidence.TargetRowCount &&
              string.Equals(evidence.SourceChecksum, evidence.TargetChecksum, StringComparison.Ordinal) &&
              partitions.All(partition => partition.Status == MigrationValidationStatus.Passed)
                ? MigrationValidationStatus.Passed
                : MigrationValidationStatus.Different;
        if (evidence.Status != expected)
        {
            throw Invalid(
                $"Checksum validation object '{evidence.SourceObjectId}' status contradicts its counts, checksums, or partitions; expected '{expected}'.");
        }
    }

    private static void ValidateMismatch(
        string objectId,
        int partitionId,
        MigrationValidationMismatchEvidence mismatch)
    {
        if (mismatch.SourceMultiplicity < 0 || mismatch.TargetMultiplicity < 0)
        {
            throw Invalid(
                $"Validation object '{objectId}' partition '{partitionId}' has a negative mismatch multiplicity.");
        }

        switch (mismatch.Kind)
        {
            case MigrationValidationMismatchKind.SourceOnly:
                if (mismatch.SourceMultiplicity <= 0 ||
                    mismatch.TargetMultiplicity != 0 ||
                    mismatch.SourceRowHash is null ||
                    mismatch.TargetRowHash is not null)
                {
                    throw Invalid(
                        $"Validation object '{objectId}' partition '{partitionId}' has invalid source-only mismatch evidence.");
                }

                break;

            case MigrationValidationMismatchKind.TargetOnly:
                if (mismatch.SourceMultiplicity != 0 ||
                    mismatch.TargetMultiplicity <= 0 ||
                    mismatch.SourceRowHash is not null ||
                    mismatch.TargetRowHash is null)
                {
                    throw Invalid(
                        $"Validation object '{objectId}' partition '{partitionId}' has invalid target-only mismatch evidence.");
                }

                break;

            case MigrationValidationMismatchKind.Changed:
                if (mismatch.KeyHash is null ||
                    mismatch.SourceMultiplicity <= 0 ||
                    mismatch.TargetMultiplicity <= 0 ||
                    mismatch.SourceRowHash is null ||
                    mismatch.TargetRowHash is null ||
                    string.Equals(mismatch.SourceRowHash, mismatch.TargetRowHash, StringComparison.Ordinal))
                {
                    throw Invalid(
                        $"Validation object '{objectId}' partition '{partitionId}' has invalid changed-row mismatch evidence.");
                }

                break;

            default:
                throw Invalid(
                    $"Validation object '{objectId}' partition '{partitionId}' has an unknown mismatch kind.");
        }
    }

    private static void ValidateDiagnosticReferences(
        IReadOnlyList<MigrationObjectValidationEvidence> objects,
        IReadOnlyList<MigrationValidationDiagnosticEvidence> diagnostics)
    {
        IReadOnlyDictionary<string, MigrationObjectValidationEvidence> byObject = objects.ToDictionary(
            item => item.SourceObjectId,
            StringComparer.Ordinal);

        foreach (MigrationValidationDiagnosticEvidence diagnostic in diagnostics)
        {
            if (diagnostic.ObjectId is null)
            {
                if (diagnostic.PartitionId is not null)
                {
                    throw Invalid(
                        $"Validation diagnostic '{diagnostic.DiagnosticId}' has a partition but no object identity.");
                }

                continue;
            }

            if (!byObject.TryGetValue(diagnostic.ObjectId, out MigrationObjectValidationEvidence? evidence))
            {
                throw Invalid(
                    $"Validation diagnostic '{diagnostic.DiagnosticId}' references unknown object '{diagnostic.ObjectId}'.");
            }

            if (diagnostic.PartitionId is int partitionId &&
                !evidence.Partitions.Any(item => item.PartitionId == partitionId))
            {
                throw Invalid(
                    $"Validation diagnostic '{diagnostic.DiagnosticId}' references unknown partition '{partitionId}' for object '{diagnostic.ObjectId}'.");
            }
        }
    }

    private static string MismatchIdentity(MigrationValidationMismatchEvidence mismatch) => string.Join(
        '|',
        mismatch.Kind,
        mismatch.KeyHash ?? string.Empty,
        mismatch.SourceRowHash ?? string.Empty,
        mismatch.TargetRowHash ?? string.Empty);

    private static JsonElement ReadVerifiedPayload(string json)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(json);
        if (json.Length > MaxArtifactCharacters)
        {
            throw Invalid(
                $"Validation report exceeds the {MaxArtifactCharacters}-character safety limit.");
        }

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
            throw Invalid("Validation report JSON is invalid.", ex);
        }

        using (document)
        {
            RejectDuplicateProperties(document.RootElement, path: "$");

            MigrationArtifactEnvelope<JsonElement> envelope;
            try
            {
                envelope = document.RootElement.Deserialize<MigrationArtifactEnvelope<JsonElement>>(
                        s_compactOptions)
                    ?? throw Invalid("Validation report did not contain an artifact envelope.");
            }
            catch (JsonException ex)
            {
                throw Invalid("Validation report envelope is invalid.", ex);
            }

            if (!string.Equals(
                    envelope.Format,
                    MigrationArtifactFormats.ValidationReportV1,
                    StringComparison.Ordinal))
            {
                throw Invalid(
                    $"Validation report format '{envelope.Format}' does not match expected format '{MigrationArtifactFormats.ValidationReportV1}'.");
            }

            if (!string.Equals(
                    envelope.DigestAlgorithm,
                    MigrationArtifactFormats.DigestAlgorithm,
                    StringComparison.Ordinal))
            {
                throw Invalid(
                    $"Validation report digest algorithm '{envelope.DigestAlgorithm}' is not supported.");
            }

            if (envelope.Payload.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
                throw Invalid("Validation report payload is missing.");

            VerifyDigest(envelope.Digest, ComputeDigest(envelope.Payload));
            return envelope.Payload.Clone();
        }
    }

    private static void RequireCanonicalPayload(
        JsonElement supplied,
        MigrationValidationReport normalized)
    {
        byte[] suppliedBytes = JsonSerializer.SerializeToUtf8Bytes(supplied, s_compactOptions);
        byte[] normalizedBytes = JsonSerializer.SerializeToUtf8Bytes(normalized, s_compactOptions);
        if (!suppliedBytes.AsSpan().SequenceEqual(normalizedBytes))
        {
            throw Invalid(
                "Validation report payload is not in the required deterministic order or shape.");
        }
    }

    private static string ComputeDigest(JsonElement payload)
    {
        byte[] canonicalBytes = JsonSerializer.SerializeToUtf8Bytes(
            new MigrationDigestInput
            {
                Format = MigrationArtifactFormats.ValidationReportV1,
                DigestAlgorithm = MigrationArtifactFormats.DigestAlgorithm,
                Payload = payload,
            },
            s_compactOptions);

        return Convert.ToHexString(SHA256.HashData(canonicalBytes)).ToLowerInvariant();
    }

    private static void VerifyDigest(string? suppliedDigest, string expectedDigest)
    {
        string normalizedSupplied = NormalizeSha256(suppliedDigest, "Validation report digest");
        byte[] suppliedBytes = Convert.FromHexString(normalizedSupplied);
        byte[] expectedBytes = Convert.FromHexString(expectedDigest);
        if (!CryptographicOperations.FixedTimeEquals(suppliedBytes, expectedBytes))
            throw Invalid("Validation report digest does not match its payload.");
    }

    private static void RejectDuplicateProperties(JsonElement element, string path)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                var names = new HashSet<string>(StringComparer.Ordinal);
                foreach (JsonProperty property in element.EnumerateObject())
                {
                    if (!names.Add(property.Name))
                    {
                        throw Invalid(
                            $"Validation report contains duplicate property '{path}.{property.Name}'.");
                    }

                    RejectDuplicateProperties(property.Value, $"{path}.{property.Name}");
                }

                break;

            case JsonValueKind.Array:
                int index = 0;
                foreach (JsonElement item in element.EnumerateArray())
                {
                    RejectDuplicateProperties(item, $"{path}[{index}]");
                    index++;
                }

                break;
        }
    }

    private static string NormalizeSha256(string? value, string description)
    {
        if (string.IsNullOrWhiteSpace(value) ||
            value.Length != 64 ||
            !value.All(Uri.IsHexDigit))
        {
            throw Invalid($"{description} must be a 64-character hexadecimal SHA-256 value.");
        }

        try
        {
            _ = Convert.FromHexString(value);
        }
        catch (FormatException ex)
        {
            throw Invalid($"{description} is not valid hexadecimal SHA-256.", ex);
        }

        return value.ToLowerInvariant();
    }

    private static string? NormalizeOptionalSha256(string? value, string description) =>
        value is null ? null : NormalizeSha256(value, description);

    private static string RequireText(string? value, string description)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw Invalid($"{description} is required.");
        if (!string.Equals(value, value.Trim(), StringComparison.Ordinal))
            throw Invalid($"{description} cannot have leading or trailing whitespace.");
        return value;
    }

    private static string? NormalizeOptionalText(string? value, string description) =>
        value is null ? null : RequireText(value, description);

    private static IReadOnlyList<T> RequireList<T>(IReadOnlyList<T>? items, string description) =>
        items ?? throw Invalid($"{description} collection is required.");

    private static void RequireUnique<T, TKey>(
        IEnumerable<T> items,
        Func<T, TKey> keySelector,
        string description)
        where TKey : notnull
    {
        var seen = new HashSet<TKey>();
        foreach (T item in items)
        {
            TKey key = keySelector(item);
            if (!seen.Add(key))
                throw Invalid($"Validation report contains duplicate {description} '{key}'.");
        }
    }

    private static JsonSerializerOptions CreateOptions(bool writeIndented)
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            WriteIndented = writeIndented,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            PropertyNameCaseInsensitive = false,
            UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
            AllowTrailingCommas = false,
            ReadCommentHandling = JsonCommentHandling.Disallow,
            MaxDepth = MaxJsonDepth,
        };
        options.Converters.Add(
            new JsonStringEnumConverter(JsonNamingPolicy.CamelCase, allowIntegerValues: false));
        return options;
    }

    private static InvalidDataException Invalid(string message, Exception? innerException = null) =>
        new(message, innerException);

    private sealed record MigrationDigestInput
    {
        public required string Format { get; init; }

        public required string DigestAlgorithm { get; init; }

        public required JsonElement Payload { get; init; }
    }
}
