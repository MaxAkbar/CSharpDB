using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration.Canonicalization;

namespace CSharpDB.Migration.Validation;

/// <summary>
/// Safe report binding exposed to a caller-owned prerequisite immediately
/// before validation activation. The report is normalized, digested, and
/// durably published before this context is created.
/// </summary>
public sealed record MigrationValidationPreActivationContext
{
    public required MigrationValidationReport Report { get; init; }

    public required string ReportDigest { get; init; }

    public required string ReportPath { get; init; }
}

public sealed record MigrationValidationRunRequest
{
    public required MigrationPlan Plan { get; init; }

    public required MigrationCatalog Catalog { get; init; }

    public required IMigrationEvidenceValidationSnapshot SourceSnapshot { get; init; }

    public required IMigrationTarget Target { get; init; }

    public required MigrationValidationLevel Level { get; init; }

    public required string ReportOutputPath { get; init; }

    public required PartitionedChecksumValidatorOptions ChecksumOptions { get; init; }

    public bool ActivateOnSuccess { get; init; } = true;

    /// <summary>
    /// Optional caller-owned prerequisite invoked only after a passing report
    /// is durably published and activation has been requested. Failure or
    /// cancellation prevents activation. No activation permit is exposed.
    /// </summary>
    public Func<
        MigrationValidationPreActivationContext,
        CancellationToken,
        ValueTask>? BeforeActivationAsync
    { get; init; }
}

public sealed record MigrationValidationRunResult
{
    public required MigrationValidationReport Report { get; init; }

    public required string ReportDigest { get; init; }

    public required string ReportPath { get; init; }

    public bool Activated { get; init; }

    public long PeakSpillBytes { get; init; }
}

/// <summary>
/// Runs schema, count, and checksum validation against exactly one source and
/// one target snapshot, durably publishes the digested report, and only then
/// asks a staged target to activate it.
/// </summary>
public sealed class MigrationValidationRunner
{
    internal const string RejectActivationFailureMessage =
        "Deterministic migration activation could not verify the published validation report.";

    public async ValueTask<MigrationValidationRunResult> ValidateAsync(
        MigrationValidationRunRequest request,
        CancellationToken cancellationToken = default)
    {
        string targetIdentity = ValidateRequest(request);
        string reportPath = ValidateReportPath(request.ReportOutputPath);
        MigrationValidationReport report;
        long peakSpillBytes;

        try
        {
            await using IValidationSnapshot openedTarget = await request.Target
                .OpenValidationSnapshotAsync(cancellationToken)
                .ConfigureAwait(false);
            if (openedTarget is not IMigrationEvidenceValidationSnapshot targetSnapshot)
            {
                throw new NotSupportedException(
                    "The migration target does not expose Phase 3 schema and consistency evidence.");
            }

            if (request.Plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects)
            {
                if (request.SourceSnapshot is not IMigrationRejectReplayValidationSnapshot sourceReplay ||
                    openedTarget is not IMigrationRejectTargetValidationSnapshot rejectTargetSnapshot)
                {
                    throw new NotSupportedException(
                        "Deterministic reject validation requires immutable source replay and target outcome snapshots.");
                }

                await new MigrationRejectOutcomeComparer().CompareAsync(
                    request.Plan,
                    request.Catalog,
                    targetIdentity,
                    sourceReplay,
                    rejectTargetSnapshot,
                    cancellationToken).ConfigureAwait(false);
            }

            (report, peakSpillBytes) = await BuildReportAsync(
                request,
                targetSnapshot,
                targetIdentity,
                cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);
        }
        catch (Exception error) when (
            request.Plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects &&
            error is not (OutOfMemoryException or StackOverflowException or AccessViolationException))
        {
            // Once reject-aware provider snapshots are opened, any provider
            // message could contain rejected evidence. Keep the complete
            // pre-publication evidence phase behind one value-free boundary.
            throw new InvalidDataException(MigrationRejectOutcomeComparer.MismatchMessage);
        }

        cancellationToken.ThrowIfCancellationRequested();
        report = MigrationValidationReportSerializer.Normalize(report);
        string reportDigest = MigrationValidationReportSerializer.ComputeDigest(report);
        string json = MigrationValidationReportSerializer.Serialize(report, writeIndented: true);
        await PublishReportAsync(reportPath, json, reportDigest, cancellationToken).ConfigureAwait(false);

        bool activated = false;
        if (report.Outcome == MigrationValidationStatus.Passed && request.ActivateOnSuccess)
        {
            if (request.Target is not IMigrationValidationActivationTarget activationTarget)
            {
                throw new NotSupportedException(
                    "The validation report was published, but the migration target has no activation capability.");
            }

            if (request.BeforeActivationAsync is not null)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    await request.BeforeActivationAsync(
                            new MigrationValidationPreActivationContext
                            {
                                Report = report,
                                ReportDigest = reportDigest,
                                ReportPath = reportPath,
                            },
                            cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException(cancellationToken);
                }
                cancellationToken.ThrowIfCancellationRequested();
            }

            var receipt = new MigrationValidationActivationReceipt
            {
                TargetIdentity = targetIdentity,
                PlanDigest = report.Binding.PlanDigest,
                CatalogDigest = report.Binding.CatalogDigest,
                SourceSnapshotIdentity = report.Binding.SourceSnapshotIdentity,
                TargetSnapshotIdentity = report.Binding.TargetSnapshotIdentity,
                Level = report.Level,
                CanonicalizationVersion = report.Binding.CanonicalizationVersion,
                CanonicalizationContractDigest = report.Binding.CanonicalizationContractDigest,
                ReportDigest = reportDigest,
            };
            var permit = new MigrationValidationActivationPermit(receipt, reportPath);
            try
            {
                await activationTarget.ActivateAsync(permit, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(cancellationToken);
            }
            catch (Exception error) when (
                request.Plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects &&
                error is not (OutOfMemoryException or StackOverflowException or AccessViolationException))
            {
                // A target may re-read authoritative outcomes while verifying
                // the permit. Keep that post-publication provider boundary
                // value-free as well.
                throw new InvalidDataException(RejectActivationFailureMessage);
            }
            activated = true;
        }

        return new MigrationValidationRunResult
        {
            Report = report,
            ReportDigest = reportDigest,
            ReportPath = reportPath,
            Activated = activated,
            PeakSpillBytes = peakSpillBytes,
        };
    }

    private static async ValueTask<(MigrationValidationReport Report, long PeakSpillBytes)> BuildReportAsync(
        MigrationValidationRunRequest request,
        IMigrationEvidenceValidationSnapshot targetSnapshot,
        string targetIdentity,
        CancellationToken cancellationToken)
    {
        MigrationPlan plan = request.Plan;
        MigrationCatalog catalog = request.Catalog;
        IMigrationEvidenceValidationSnapshot sourceSnapshot = request.SourceSnapshot;

        if (string.IsNullOrWhiteSpace(sourceSnapshot.SnapshotIdentity) ||
            string.IsNullOrWhiteSpace(targetSnapshot.SnapshotIdentity))
        {
            throw new InvalidDataException("Validation snapshot identities are required.");
        }

        MigrationSnapshotConsistencyStatus consistency = CombineConsistency(
            sourceSnapshot.ConsistencyStatus,
            targetSnapshot.ConsistencyStatus);
        MigrationNormalizedSchema sourceSchema = await sourceSnapshot
            .ReadSchemaAsync(cancellationToken)
            .ConfigureAwait(false);
        MigrationNormalizedSchema targetSchema = await targetSnapshot
            .ReadSchemaAsync(cancellationToken)
            .ConfigureAwait(false);
        IReadOnlyList<MigrationNormalizedSchemaDifference> schemaDifferences =
            MigrationNormalizedSchemaContract.Compare(sourceSchema, targetSchema);
        MigrationSchemaValidationEvidence schemaEvidence = CreateSchemaEvidence(
            sourceSchema,
            targetSchema,
            schemaDifferences);

        MigrationCatalogObject[] validationObjects = catalog.Objects
            .Where(item => item.Kind is MigrationObjectKind.Table or MigrationObjectKind.Collection)
            .Where(item => plan.Objects.Single(candidate =>
                string.Equals(candidate.SourceObjectId, item.ObjectId, StringComparison.Ordinal)).Included)
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .ToArray();
        var objectEvidence = new List<MigrationObjectValidationEvidence>(validationObjects.Length);
        var diagnostics = new List<MigrationValidationDiagnosticEvidence>();
        bool evidenceError = false;
        long peakSpillBytes = 0;

        if (consistency != MigrationSnapshotConsistencyStatus.Established)
            diagnostics.Add(ConsistencyDiagnostic(consistency));

        foreach (MigrationCatalogObject item in validationObjects)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CanonicalRowContract contract = CanonicalRowProjector.CreateContract(plan, catalog, item.ObjectId);
            long? sourceCount = null;
            long? targetCount = null;
            string? sourceChecksum = null;
            string? targetChecksum = null;
            IReadOnlyList<MigrationValidationPartitionEvidence> partitions = [];
            MigrationValidationStatus status = MigrationValidationStatus.Skipped;

            if (request.Level >= MigrationValidationLevel.Count)
            {
                sourceCount = await sourceSnapshot.CountAsync(item.ObjectId, cancellationToken).ConfigureAwait(false);
                targetCount = await targetSnapshot.CountAsync(item.ObjectId, cancellationToken).ConfigureAwait(false);
                status = sourceCount == targetCount
                    ? MigrationValidationStatus.Passed
                    : MigrationValidationStatus.Different;
            }

            if (request.Level >= MigrationValidationLevel.Checksum)
            {
                PartitionedChecksumValidationResult checksum = await new PartitionedChecksumValidator()
                    .ValidateAsync(
                        contract,
                        sourceSnapshot.ReadRowsAsync(item.ObjectId, cancellationToken),
                        targetSnapshot.ReadRowsAsync(item.ObjectId, cancellationToken),
                        request.ChecksumOptions,
                        cancellationToken)
                    .ConfigureAwait(false);
                peakSpillBytes = Math.Max(peakSpillBytes, checksum.PeakSpillBytes);
                sourceChecksum = checksum.SourceChecksum;
                targetChecksum = checksum.TargetChecksum;
                partitions = checksum.Partitions;
                status = checksum.Status;

                if (sourceCount != checksum.SourceRowCount || targetCount != checksum.TargetRowCount)
                {
                    evidenceError = true;
                    status = MigrationValidationStatus.Error;
                    diagnostics.Add(CountCoherenceDiagnostic(item.ObjectId));
                }
            }

            objectEvidence.Add(new MigrationObjectValidationEvidence
            {
                SourceObjectId = item.ObjectId,
                TargetObjectId = contract.TargetObjectId,
                Status = status,
                CanonicalTypeContractDigest = CanonicalRowCodec.ContractHashHex,
                ObjectContractDigest = contract.ObjectContractDigest,
                SourceRowCount = sourceCount,
                TargetRowCount = targetCount,
                SourceChecksum = sourceChecksum,
                TargetChecksum = targetChecksum,
                Partitions = partitions,
            });
        }

        MigrationValidationStatus outcome = evidenceError
            ? MigrationValidationStatus.Error
            : consistency != MigrationSnapshotConsistencyStatus.Established
                ? MigrationValidationStatus.Inconclusive
                : schemaEvidence.Status == MigrationValidationStatus.Different ||
                  objectEvidence.Any(item => item.Status == MigrationValidationStatus.Different)
                    ? MigrationValidationStatus.Different
                    : MigrationValidationStatus.Passed;

        var report = new MigrationValidationReport
        {
            Binding = new MigrationValidationBinding
            {
                TargetCSharpDbVersion = plan.TargetCSharpDbVersion,
                PlanDigest = MigrationArtifactSerializer.ComputePlanDigest(plan),
                CatalogDigest = plan.CatalogDigest,
                CapabilityDigest = plan.CapabilityDigest,
                SourceIdentity = plan.Source.Identity,
                SourceFingerprint = plan.Source.Fingerprint,
                TargetIdentity = targetIdentity,
                SourceSnapshotIdentity = sourceSnapshot.SnapshotIdentity,
                TargetSnapshotIdentity = targetSnapshot.SnapshotIdentity,
                CanonicalizationVersion = plan.Validation.CanonicalizationVersion,
                CanonicalizationContractDigest = CanonicalRowCodec.ContractHashHex,
            },
            Level = request.Level,
            Outcome = outcome,
            SnapshotConsistency = new MigrationSnapshotConsistencyEvidence { Status = consistency },
            Schema = schemaEvidence,
            Objects = objectEvidence,
            Diagnostics = diagnostics,
        };
        return (report, peakSpillBytes);
    }

    private static MigrationSchemaValidationEvidence CreateSchemaEvidence(
        MigrationNormalizedSchema source,
        MigrationNormalizedSchema target,
        IReadOnlyList<MigrationNormalizedSchemaDifference> differences) => new()
        {
            Status = differences.Count == 0
            ? MigrationValidationStatus.Passed
            : MigrationValidationStatus.Different,
            SourceSchemaDigest = source.Digest,
            TargetSchemaDigest = target.Digest,
            Differences = differences.Select(item => new MigrationSchemaDifferenceEvidence
            {
                ObjectId = item.ObjectId,
                Kind = item.SourceDefinitionDigest is null
                    ? MigrationSchemaDifferenceKind.MissingFromSource
                    : item.TargetDefinitionDigest is null
                        ? MigrationSchemaDifferenceKind.MissingFromTarget
                        : MigrationSchemaDifferenceKind.DefinitionMismatch,
                SourceDefinitionDigest = item.SourceDefinitionDigest,
                TargetDefinitionDigest = item.TargetDefinitionDigest,
            }).ToArray(),
        };

    private static MigrationSnapshotConsistencyStatus CombineConsistency(
        MigrationSnapshotConsistencyStatus source,
        MigrationSnapshotConsistencyStatus target)
    {
        if (!Enum.IsDefined(source) || !Enum.IsDefined(target))
        {
            throw new InvalidDataException(
                "Validation snapshot returned an unknown consistency status.");
        }
        if (source == MigrationSnapshotConsistencyStatus.Unavailable ||
            target == MigrationSnapshotConsistencyStatus.Unavailable)
        {
            return MigrationSnapshotConsistencyStatus.Unavailable;
        }
        if (source == MigrationSnapshotConsistencyStatus.NotEstablished ||
            target == MigrationSnapshotConsistencyStatus.NotEstablished)
        {
            return MigrationSnapshotConsistencyStatus.NotEstablished;
        }
        return MigrationSnapshotConsistencyStatus.Established;
    }

    private static MigrationValidationDiagnosticEvidence ConsistencyDiagnostic(
        MigrationSnapshotConsistencyStatus consistency) => new()
        {
            DiagnosticId = consistency == MigrationSnapshotConsistencyStatus.Unavailable
            ? "validation:consistency:unavailable"
            : "validation:consistency:not-established",
            RuleId = "MIG-VALIDATE-CONSISTENCY-001",
            Severity = consistency == MigrationSnapshotConsistencyStatus.Unavailable
            ? MigrationDiagnosticSeverity.Error
            : MigrationDiagnosticSeverity.Warning,
            Status = MigrationValidationStatus.Inconclusive,
            Evidence = MigrationEvidenceLevel.CapabilityMatched,
        };

    private static MigrationValidationDiagnosticEvidence CountCoherenceDiagnostic(string objectId) => new()
    {
        DiagnosticId = $"validation:count-coherence:{StableSuffix(objectId)}",
        RuleId = "MIG-VALIDATE-SNAPSHOT-001",
        Severity = MigrationDiagnosticSeverity.Error,
        Status = MigrationValidationStatus.Error,
        Evidence = MigrationEvidenceLevel.DifferentiallyValidated,
        ObjectId = objectId,
    };

    private static string StableSuffix(string value) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant()[..16];

    private static string ValidateRequest(MigrationValidationRunRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Plan);
        ArgumentNullException.ThrowIfNull(request.Catalog);
        ArgumentNullException.ThrowIfNull(request.SourceSnapshot);
        ArgumentNullException.ThrowIfNull(request.Target);
        ArgumentNullException.ThrowIfNull(request.ChecksumOptions);
        MigrationPlanReadinessValidator.ValidateForApply(request.Plan, request.Catalog);
        MigrationValidationPolicyValidator.ValidateForExecution(
            request.Plan,
            request.SourceSnapshot,
            request.Target);
        string targetIdentity;
        try
        {
            targetIdentity = request.Target.TargetIdentity;
        }
        catch (Exception error) when (
            request.Plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects &&
            error is not (OutOfMemoryException or StackOverflowException or AccessViolationException))
        {
            throw new InvalidDataException(MigrationRejectOutcomeComparer.MismatchMessage);
        }
        if (string.IsNullOrWhiteSpace(targetIdentity))
            throw new InvalidDataException("Migration validation target identity is required.");
        if (request.Level is < MigrationValidationLevel.Schema or > MigrationValidationLevel.Checksum)
            throw new NotSupportedException("Phase 3 supports schema, count, and checksum validation levels.");

        MigrationValidationLevel required = request.Plan.Validation.ValidateChecksums
            ? MigrationValidationLevel.Checksum
            : request.Plan.Validation.ValidateCounts
                ? MigrationValidationLevel.Count
                : MigrationValidationLevel.Schema;
        if (request.Level < required)
        {
            throw new InvalidDataException(
                $"The migration plan requires validation level '{required}' or stronger.");
        }
        if (!string.Equals(
                request.Plan.Validation.CanonicalizationVersion,
                CanonicalRowCodec.CanonicalizationId,
                StringComparison.Ordinal))
        {
            throw new NotSupportedException(
                $"Canonicalization version '{request.Plan.Validation.CanonicalizationVersion}' is not supported.");
        }
        return targetIdentity;
    }

    private static string ValidateReportPath(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        string fullPath = Path.GetFullPath(path);
        string? directory = Path.GetDirectoryName(fullPath);
        if (string.IsNullOrWhiteSpace(directory) || !Directory.Exists(directory))
            throw new DirectoryNotFoundException($"Validation report directory '{directory}' does not exist.");
        return fullPath;
    }

    private static async ValueTask PublishReportAsync(
        string reportPath,
        string json,
        string reportDigest,
        CancellationToken cancellationToken)
    {
        if (File.Exists(reportPath))
        {
            await RequireExistingReportAsync(reportPath, reportDigest, cancellationToken).ConfigureAwait(false);
            return;
        }

        string directory = Path.GetDirectoryName(reportPath)!;
        string temporaryPath = Path.Combine(
            directory,
            $".{Path.GetFileName(reportPath)}.{Guid.NewGuid():N}.tmp");
        try
        {
            byte[] bytes = new UTF8Encoding(false, true).GetBytes(json);
            if (bytes.LongLength > MigrationValidationReportSerializer.MaximumArtifactBytes)
            {
                throw new InvalidDataException(
                    "Validation report exceeds the maximum artifact byte length.");
            }
            await using (var stream = new FileStream(
                temporaryPath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                64 * 1024,
                FileOptions.Asynchronous | FileOptions.WriteThrough))
            {
                await stream.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                stream.Flush(flushToDisk: true);
            }

            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                File.Move(temporaryPath, reportPath, overwrite: false);
            }
            catch (IOException) when (File.Exists(reportPath))
            {
                await RequireExistingReportAsync(reportPath, reportDigest, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            try
            {
                File.Delete(temporaryPath);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }

    private static async ValueTask RequireExistingReportAsync(
        string reportPath,
        string expectedDigest,
        CancellationToken cancellationToken)
    {
        RequireRegularBoundedReportFile(reportPath);
        string existing = await File.ReadAllTextAsync(reportPath, cancellationToken).ConfigureAwait(false);
        MigrationValidationReport report = MigrationValidationReportSerializer.Deserialize(existing);
        string existingDigest = MigrationValidationReportSerializer.ComputeDigest(report);
        if (!string.Equals(existingDigest, expectedDigest, StringComparison.Ordinal))
        {
            throw new IOException(
                "Validation report path already contains a different digested report.");
        }
    }

    private static void RequireRegularBoundedReportFile(string reportPath)
    {
        var info = new FileInfo(reportPath);
        FileAttributes unsupported =
            FileAttributes.Directory | FileAttributes.Device | FileAttributes.ReparsePoint;
        if (!info.Exists || (info.Attributes & unsupported) != 0)
            throw new InvalidDataException("Validation report path is not a regular file.");
        if (info.Length > MigrationValidationReportSerializer.MaximumArtifactBytes)
        {
            throw new InvalidDataException(
                "Validation report exceeds the maximum artifact byte length.");
        }
    }
}
