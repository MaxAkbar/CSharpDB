using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Text;

namespace CSharpDB.Migration;

public sealed record MigrationRejectArtifactWriteRequest
{
    public required MigrationPlan Plan { get; init; }

    public required MigrationCatalog Catalog { get; init; }

    public required IMigrationTarget Target { get; init; }

    /// <summary>
    /// Fully qualified normalized destination inside a stable,
    /// caller-controlled parent directory. The caller must prevent other
    /// actors from replacing directory entries while publication is active.
    /// </summary>
    public required string OutputPath { get; init; }
}

public sealed record MigrationRejectArtifactWriteResult
{
    public required string ArtifactPath { get; init; }

    public required string PlanDigest { get; init; }

    public required string TargetIdentity { get; init; }

    public required string TargetSnapshotIdentity { get; init; }

    public required string ArtifactDigest { get; init; }

    public long RejectedRowCount { get; init; }

    public long ArtifactBytes { get; init; }

    public bool ReusedExistingArtifact { get; init; }
}

internal enum MigrationRejectArtifactFaultPoint
{
    AfterTemporaryHeaderDurablyFlushed,
    AfterPublishBeforeResult,
}

internal interface IMigrationRejectArtifactFaultInjector
{
    ValueTask InjectAsync(
        MigrationRejectArtifactFaultPoint point,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Projects one immutable target-owned receipt and reject-ledger snapshot into
/// the canonical sensitive JSON Lines artifact. The target remains the only
/// resume authority; this artifact is an idempotent operator-facing view.
/// </summary>
/// <remarks>
/// The target snapshot implementation remains responsible for binding the
/// accepted-row portion of each batch digest. This writer validates receipt
/// syntax and bindings and recomputes the reject digest from the authoritative
/// ledger; accepted row payloads are intentionally absent from the artifact.
/// </remarks>
public sealed class MigrationRejectArtifactWriter
{
    public const string UnsupportedRejectModeCode =
        "MIG-ARTIFACT-POLICY-REJECT-001";

    internal const string EvidenceFailureMessage =
        "The authoritative migration reject evidence cannot be materialized.";

    private const string InvalidRecordedMappingMessage =
        "The migration plan contains invalid recorded type mappings.";

    private static readonly UTF8Encoding StrictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    private static readonly byte[] LineFeed = "\n"u8.ToArray();

    private readonly IMigrationRejectArtifactFaultInjector? _faultInjector;

    public MigrationRejectArtifactWriter()
    {
    }

    internal MigrationRejectArtifactWriter(
        IMigrationRejectArtifactFaultInjector faultInjector)
    {
        ArgumentNullException.ThrowIfNull(faultInjector);
        _faultInjector = faultInjector;
    }

    public async ValueTask<MigrationRejectArtifactWriteResult> WriteAsync(
        MigrationRejectArtifactWriteRequest request,
        CancellationToken cancellationToken = default)
    {
        try
        {
            return await WriteCoreAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);
        }
    }

    private async ValueTask<MigrationRejectArtifactWriteResult> WriteCoreAsync(
        MigrationRejectArtifactWriteRequest request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Plan);
        ArgumentNullException.ThrowIfNull(request.Catalog);
        ArgumentNullException.ThrowIfNull(request.Target);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.OutputPath);

        MigrationPlanReadinessValidator.ValidateForApply(
            request.Plan,
            request.Catalog,
            CreateRecordedMappingProvider(request.Plan));
        if (request.Plan.Load.RejectMode != MigrationRejectMode.DeterministicRejects ||
            request.Plan.Load.RejectPolicy is null)
        {
            throw new MigrationExecutionPolicyException(
                UnsupportedRejectModeCode,
                "Reject artifacts require the deterministic-reject migration contract.");
        }

        MigrationDeterministicRejectPolicy policy = request.Plan.Load.RejectPolicy;
        string planDigest = MigrationArtifactSerializer.ComputePlanDigest(request.Plan);
        string targetIdentity = ReadProviderValue(
            () => request.Target.TargetIdentity,
            cancellationToken);
        Require(!string.IsNullOrWhiteSpace(targetIdentity));

        if (request.Target is not IMigrationBatchDigestContractTarget digestTarget)
            throw EvidenceFailure();
        string batchDigestFormat = ReadProviderValue(
            () => digestTarget.BatchDigestFormat,
            cancellationToken);
        Require(string.Equals(
            batchDigestFormat,
            MigrationBatchDigest.Format,
            StringComparison.Ordinal));

        await using MigrationRejectArtifactPublication publication =
            await MigrationRejectArtifactPublication.OpenAsync(
                request.OutputPath,
                planDigest,
                policy.MaxArtifactBytes,
                cancellationToken).ConfigureAwait(false);

        ProjectionResult projection = await WithTargetSnapshotAsync(
            request.Target,
            async openedSnapshot =>
            {
                if (openedSnapshot is not IMigrationRejectTargetValidationSnapshot snapshot)
                    throw EvidenceFailure();
                return await ProjectAsync(
                    request.Plan,
                    request.Catalog,
                    planDigest,
                    targetIdentity,
                    snapshot,
                    publication,
                    cancellationToken).ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);

        string finalTargetIdentity = ReadProviderValue(
            () => request.Target.TargetIdentity,
            cancellationToken);
        Require(string.Equals(targetIdentity, finalTargetIdentity, StringComparison.Ordinal));

        await publication.FlushAsync(cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        bool reusedExistingArtifact = await publication
            .PublishOrReuseAsync(cancellationToken)
            .ConfigureAwait(false);
        if (!reusedExistingArtifact)
        {
            await InjectFaultAsync(
                MigrationRejectArtifactFaultPoint.AfterPublishBeforeResult,
                cancellationToken).ConfigureAwait(false);
        }

        return new MigrationRejectArtifactWriteResult
        {
            ArtifactPath = publication.DestinationPath,
            PlanDigest = planDigest,
            TargetIdentity = targetIdentity,
            TargetSnapshotIdentity = projection.TargetSnapshotIdentity,
            ArtifactDigest = projection.ArtifactDigest,
            RejectedRowCount = projection.RejectedRowCount,
            ArtifactBytes = projection.ArtifactBytes,
            ReusedExistingArtifact = reusedExistingArtifact,
        };
    }

    private async ValueTask<ProjectionResult> ProjectAsync(
        MigrationPlan plan,
        MigrationCatalog catalog,
        string planDigest,
        string targetIdentity,
        IMigrationRejectTargetValidationSnapshot snapshot,
        MigrationRejectArtifactPublication publication,
        CancellationToken cancellationToken)
    {
        Stream output = publication.Stream;
        MigrationDeterministicRejectPolicy policy = plan.Load.RejectPolicy ??
            throw EvidenceFailure();
        string snapshotIdentity = ReadProviderValue(
            () => snapshot.SnapshotIdentity,
            cancellationToken);
        Require(!string.IsNullOrWhiteSpace(snapshotIdentity));
        MigrationSnapshotConsistencyStatus consistencyStatus = ReadProviderValue(
            () => snapshot.ConsistencyStatus,
            cancellationToken);
        Require(consistencyStatus == MigrationSnapshotConsistencyStatus.Established);

        IReadOnlyDictionary<string, MigrationPlanObject> plannedObjects = plan.Objects
            .ToDictionary(item => item.SourceObjectId, StringComparer.Ordinal);
        ExpectedObject[] expectedObjects = catalog.Objects
            .Where(item => item.Kind is MigrationObjectKind.Table or MigrationObjectKind.Collection)
            .Where(item => plannedObjects.TryGetValue(
                    item.ObjectId,
                    out MigrationPlanObject? planned) &&
                planned.Included)
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .Select(item => new ExpectedObject(
                item.ObjectId,
                catalog.Objects
                    .Where(candidate =>
                        candidate.Kind == MigrationObjectKind.Column &&
                        string.Equals(
                            candidate.ParentObjectId,
                            item.ObjectId,
                            StringComparison.Ordinal) &&
                        plannedObjects.TryGetValue(
                            candidate.ObjectId,
                            out MigrationPlanObject? plannedColumn) &&
                        plannedColumn.Included)
                    .OrderBy(candidate => candidate.ObjectId, StringComparer.Ordinal)
                    .Select(candidate => candidate.ObjectId)
                    .ToHashSet(StringComparer.Ordinal)))
            .ToArray();
        var objectOrdinals = expectedObjects
            .Select((item, index) => (item.SourceObjectId, index))
            .ToDictionary(item => item.SourceObjectId, item => item.index, StringComparer.Ordinal);
        var allowedRuleIds = policy.AllowedRuleIds.ToHashSet(StringComparer.Ordinal);

        using IncrementalHash artifactHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        long artifactBytes = 0;
        long rejectedRowsInRun = 0;
        long rawValueBytesInRun = 0;

        string header = ReadEvidenceValue(
            () => MigrationRejectLedgerCodec.SerializeArtifactHeader(planDigest));
        artifactBytes = await WriteLineAsync(
            output,
            header,
            artifactHash,
            artifactBytes,
            policy.MaxArtifactBytes,
            cancellationToken).ConfigureAwait(false);
        if (_faultInjector is not null)
        {
            await publication.FlushAsync(cancellationToken).ConfigureAwait(false);
            await InjectFaultAsync(
                MigrationRejectArtifactFaultPoint.AfterTemporaryHeaderDurablyFlushed,
                cancellationToken).ConfigureAwait(false);
        }

        IAsyncEnumerator<MigrationBatchReceipt>? receipts = null;
        IAsyncEnumerator<MigrationRejectLedgerEntry>? ledger = null;
        ExceptionDispatchInfo? operationFailure = null;
        try
        {
            receipts = GetProviderEnumerator(
                () => snapshot.ReadOutcomeReceiptsAsync(planDigest, cancellationToken),
                cancellationToken);
            ledger = GetProviderEnumerator(
                () => snapshot.ReadRejectLedgerAsync(planDigest, cancellationToken),
                cancellationToken);
            int currentObjectOrdinal = -1;
            long expectedBatchOrdinal = 0;
            long expectedFirstSourceRowOrdinal = 0;
            string? expectedStartCursor = null;
            bool currentObjectTerminated = false;
            string? sourceSnapshotIdentity = null;

            while (await MoveNextProviderAsync(receipts, cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                MigrationBatchReceipt receipt = ReadProviderValue(
                    () => receipts.Current,
                    cancellationToken) ?? throw EvidenceFailure();
                Require(objectOrdinals.TryGetValue(
                    receipt.SourceObjectId,
                    out int objectOrdinal));
                Require(objectOrdinal >= currentObjectOrdinal);

                if (objectOrdinal != currentObjectOrdinal)
                {
                    Require(currentObjectOrdinal < 0 || currentObjectTerminated);
                    currentObjectOrdinal = objectOrdinal;
                    expectedBatchOrdinal = 0;
                    expectedFirstSourceRowOrdinal = 0;
                    expectedStartCursor = null;
                    currentObjectTerminated = false;
                }
                else
                {
                    Require(!currentObjectTerminated);
                }

                ExpectedObject expectedObject = expectedObjects[currentObjectOrdinal];
                ValidateReceipt(
                    receipt,
                    plan,
                    planDigest,
                    targetIdentity,
                    expectedObject.SourceObjectId,
                    expectedBatchOrdinal,
                    expectedStartCursor,
                    sourceSnapshotIdentity,
                    policy,
                    out long attemptedRows);
                sourceSnapshotIdentity ??= receipt.SourceSnapshotIdentity;
                long intervalEnd = checked(expectedFirstSourceRowOrdinal + attemptedRows);

                long rawValueBytesInBatch = 0;
                long canonicalRejectBytesInBatch = 0;
                long previousRejectedRowOrdinal = -1;
                using MigrationRejectDigest.Accumulator rejectHash =
                    ReadEvidenceValue(() => MigrationRejectDigest.CreateAccumulator(receipt));
                for (long rejectIndex = 0;
                     rejectIndex < receipt.RejectedRowCount;
                     rejectIndex++)
                {
                    Require(await MoveNextProviderAsync(
                        ledger,
                        cancellationToken).ConfigureAwait(false));
                    MigrationRejectLedgerEntry entry = ReadProviderValue(
                        () => ledger.Current,
                        cancellationToken) ?? throw EvidenceFailure();
                    ValidateLedgerEntry(
                        entry,
                        planDigest,
                        receipt,
                        expectedObject,
                        expectedFirstSourceRowOrdinal,
                        intervalEnd,
                        previousRejectedRowOrdinal,
                        allowedRuleIds,
                        policy,
                        out MigrationRejectedRow rejectedRow,
                        out int rawValueBytes,
                        out int canonicalEntryBytes,
                        out string canonicalEntry);

                    previousRejectedRowOrdinal = rejectedRow.SourceRowOrdinal;
                    rawValueBytesInBatch = checked(rawValueBytesInBatch + rawValueBytes);
                    rawValueBytesInRun = checked(rawValueBytesInRun + rawValueBytes);
                    rejectedRowsInRun = checked(rejectedRowsInRun + 1);
                    canonicalRejectBytesInBatch = checked(
                        canonicalRejectBytesInBatch + canonicalEntryBytes);
                    Require(rawValueBytesInBatch <= policy.MaxRawValueBytesPerBatch);
                    Require(rawValueBytesInRun <= policy.MaxRawValueBytesPerRun);
                    Require(rejectedRowsInRun <= policy.MaxRejectedRowsPerRun);
                    Require(canonicalRejectBytesInBatch <= plan.Load.MaxBatchBytes);

                    ReadEvidenceValue(() =>
                    {
                        rejectHash.Append(rejectedRow);
                        return true;
                    });
                    artifactBytes = await WriteLineAsync(
                        output,
                        canonicalEntry,
                        artifactHash,
                        artifactBytes,
                        policy.MaxArtifactBytes,
                        cancellationToken).ConfigureAwait(false);
                }

                string computedRejectDigest = ReadEvidenceValue(rejectHash.Complete);
                Require(MigrationBatchOutcomeValidator.FixedTimeSha256Equals(
                    computedRejectDigest,
                    receipt.RejectDigest));

                expectedFirstSourceRowOrdinal = intervalEnd;
                expectedBatchOrdinal = checked(expectedBatchOrdinal + 1);
                expectedStartCursor = receipt.NextCursor;
                currentObjectTerminated = receipt.NextCursor is null;
            }

            Require(currentObjectOrdinal < 0 || currentObjectTerminated);
            Require(!await MoveNextProviderAsync(ledger, cancellationToken).ConfigureAwait(false));
            string finalSnapshotIdentity = ReadProviderValue(
                () => snapshot.SnapshotIdentity,
                cancellationToken);
            Require(string.Equals(
                snapshotIdentity,
                finalSnapshotIdentity,
                StringComparison.Ordinal));
            Require(ReadProviderValue(
                () => snapshot.ConsistencyStatus,
                cancellationToken) == consistencyStatus);
        }
        catch (Exception error)
        {
            operationFailure = ExceptionDispatchInfo.Capture(error);
        }

        ExceptionDispatchInfo? disposalFailure = null;
        if (ledger is not null)
        {
            try
            {
                await DisposeProviderEnumeratorAsync(
                    ledger,
                    operationFailure,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception error) when (error is not
                (OutOfMemoryException or StackOverflowException or AccessViolationException))
            {
                disposalFailure = ExceptionDispatchInfo.Capture(error);
            }
        }
        if (receipts is not null)
        {
            try
            {
                await DisposeProviderEnumeratorAsync(
                    receipts,
                    operationFailure,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception error) when (error is not
                (OutOfMemoryException or StackOverflowException or AccessViolationException))
            {
                disposalFailure ??= ExceptionDispatchInfo.Capture(error);
            }
        }
        operationFailure?.Throw();
        disposalFailure?.Throw();

        return new ProjectionResult(
            snapshotIdentity,
            Convert.ToHexString(artifactHash.GetHashAndReset()).ToLowerInvariant(),
            rejectedRowsInRun,
            artifactBytes);
    }

    private ValueTask InjectFaultAsync(
        MigrationRejectArtifactFaultPoint point,
        CancellationToken cancellationToken) =>
        _faultInjector?.InjectAsync(point, cancellationToken) ?? ValueTask.CompletedTask;

    private static void ValidateReceipt(
        MigrationBatchReceipt receipt,
        MigrationPlan plan,
        string planDigest,
        string targetIdentity,
        string expectedSourceObjectId,
        long expectedBatchOrdinal,
        string? expectedStartCursor,
        string? expectedSourceSnapshotIdentity,
        MigrationDeterministicRejectPolicy policy,
        out long attemptedRows)
    {
        try
        {
            attemptedRows = checked(receipt.RowCount + receipt.RejectedRowCount);
        }
        catch (OverflowException)
        {
            throw EvidenceFailure();
        }

        Require(string.Equals(receipt.TargetIdentity, targetIdentity, StringComparison.Ordinal));
        Require(string.Equals(receipt.PlanDigest, planDigest, StringComparison.Ordinal));
        Require(string.Equals(receipt.CatalogDigest, plan.CatalogDigest, StringComparison.Ordinal));
        Require(string.Equals(
            receipt.SourceFingerprint,
            plan.Source.Fingerprint,
            StringComparison.Ordinal));
        Require(!string.IsNullOrWhiteSpace(receipt.SourceSnapshotIdentity));
        Require(expectedSourceSnapshotIdentity is null || string.Equals(
            receipt.SourceSnapshotIdentity,
            expectedSourceSnapshotIdentity,
            StringComparison.Ordinal));
        Require(string.Equals(
            receipt.SourceObjectId,
            expectedSourceObjectId,
            StringComparison.Ordinal));
        Require(receipt.BatchOrdinal == expectedBatchOrdinal);
        Require(string.Equals(receipt.StartCursor, expectedStartCursor, StringComparison.Ordinal));
        Require(receipt.RowCount >= 0);
        Require(receipt.RejectedRowCount >= 0);
        Require(attemptedRows > 0 && attemptedRows <= plan.Load.BatchSize);
        Require(receipt.RejectedRowCount <= policy.MaxRejectedRowsPerBatch);
        Require(string.Equals(
            receipt.RejectContractVersion,
            policy.ContractVersion,
            StringComparison.Ordinal));
        Require(IsLowerSha256(receipt.BatchDigest));
        Require(IsLowerSha256(receipt.RejectDigest));
    }

    private static void ValidateLedgerEntry(
        MigrationRejectLedgerEntry entry,
        string planDigest,
        MigrationBatchReceipt receipt,
        ExpectedObject expectedObject,
        long intervalStart,
        long intervalEnd,
        long previousRejectedRowOrdinal,
        IReadOnlySet<string> allowedRuleIds,
        MigrationDeterministicRejectPolicy policy,
        out MigrationRejectedRow frozenRejectedRow,
        out int rawValueBytes,
        out int canonicalEntryBytes,
        out string canonicalEntry)
    {
        Require(string.Equals(entry.PlanDigest, planDigest, StringComparison.Ordinal));
        Require(string.Equals(
            entry.SourceObjectId,
            receipt.SourceObjectId,
            StringComparison.Ordinal));
        Require(entry.BatchOrdinal == receipt.BatchOrdinal);
        MigrationRejectedRow frozen = ReadEvidenceValue(() =>
            FreezeRejectedRow(entry.RejectedRow ?? throw EvidenceFailure()));
        Require(frozen.SourceRowOrdinal >= intervalStart);
        Require(frozen.SourceRowOrdinal < intervalEnd);
        Require(frozen.SourceRowOrdinal > previousRejectedRowOrdinal);
        Require(allowedRuleIds.Contains(frozen.RuleId));
        Require(frozen.ColumnObjectId is null ||
            expectedObject.ColumnObjectIds.Contains(frozen.ColumnObjectId));

        rawValueBytes = ReadEvidenceValue(
            () => MigrationRejectLedgerCodec.GetRawValueByteCount(frozen));
        Require(rawValueBytes <= policy.MaxRawValueBytes);
        string serializedEntry = ReadEvidenceValue(() => MigrationRejectLedgerCodec.SerializeEntry(
            entry.SourceObjectId,
            entry.BatchOrdinal,
            frozen));
        canonicalEntryBytes = ReadEvidenceValue(() => StrictUtf8.GetByteCount(serializedEntry));
        Require(entry.RawValueByteCount == rawValueBytes);
        Require(entry.CanonicalEntryByteCount == canonicalEntryBytes);
        frozenRejectedRow = frozen;
        canonicalEntry = serializedEntry;
    }

    private static MigrationRejectedRow FreezeRejectedRow(MigrationRejectedRow source)
    {
        IReadOnlyList<MigrationRejectEvidence> sourceEvidence = source.Evidence ??
            throw EvidenceFailure();
        int count = sourceEvidence.Count;
        Require(count <= MigrationRejectContract.MaximumEvidenceEntriesPerRow);
        var evidence = new MigrationRejectEvidence[count];
        for (int index = 0; index < count; index++)
        {
            MigrationRejectEvidence item = sourceEvidence[index] ?? throw EvidenceFailure();
            evidence[index] = new MigrationRejectEvidence
            {
                Name = item.Name,
                Value = item.Value,
            };
        }

        return new MigrationRejectedRow
        {
            SourceRowOrdinal = source.SourceRowOrdinal,
            RuleId = source.RuleId,
            ColumnObjectId = source.ColumnObjectId,
            Evidence = evidence,
        };
    }

    private static async ValueTask<long> WriteLineAsync(
        Stream output,
        string canonicalJson,
        IncrementalHash hash,
        long totalBytes,
        long maximumBytes,
        CancellationToken cancellationToken)
    {
        byte[] bytes = ReadEvidenceValue(() => StrictUtf8.GetBytes(canonicalJson));
        long nextTotal;
        try
        {
            nextTotal = checked(totalBytes + bytes.LongLength + 1L);
        }
        catch (OverflowException)
        {
            throw EvidenceFailure();
        }
        Require(nextTotal <= maximumBytes);

        await output.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
        await output.WriteAsync(LineFeed, cancellationToken).ConfigureAwait(false);
        hash.AppendData(bytes);
        hash.AppendData("\n"u8);
        return nextTotal;
    }

    private static async ValueTask<T> WithTargetSnapshotAsync<T>(
        IMigrationTarget target,
        Func<IValidationSnapshot, ValueTask<T>> action,
        CancellationToken cancellationToken)
    {
        IValidationSnapshot snapshot = await ReadProviderValueAsync(
            () => target.OpenValidationSnapshotAsync(cancellationToken),
            cancellationToken).ConfigureAwait(false);
        ExceptionDispatchInfo? operationFailure = null;
        T? result = default;
        try
        {
            result = await action(snapshot).ConfigureAwait(false);
        }
        catch (Exception error)
        {
            operationFailure = ExceptionDispatchInfo.Capture(error);
        }

        try
        {
            await snapshot.DisposeAsync().ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            if (operationFailure is null)
                throw new OperationCanceledException(cancellationToken);
        }
        catch (Exception error) when (error is not
            (OutOfMemoryException or StackOverflowException or AccessViolationException))
        {
            if (operationFailure is null)
                throw EvidenceFailure();
        }

        operationFailure?.Throw();
        return result!;
    }

    private static IAsyncEnumerator<T> GetProviderEnumerator<T>(
        Func<IAsyncEnumerable<T>> factory,
        CancellationToken cancellationToken)
    {
        try
        {
            IAsyncEnumerable<T> source = factory() ?? throw EvidenceFailure();
            return source.GetAsyncEnumerator(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);
        }
        catch (Exception error) when (error is not
            (OutOfMemoryException or StackOverflowException or AccessViolationException))
        {
            throw EvidenceFailure();
        }
    }

    private static async ValueTask<bool> MoveNextProviderAsync<T>(
        IAsyncEnumerator<T> enumerator,
        CancellationToken cancellationToken)
    {
        try
        {
            return await enumerator.MoveNextAsync().ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);
        }
        catch (Exception error) when (error is not
            (OutOfMemoryException or StackOverflowException or AccessViolationException))
        {
            throw EvidenceFailure();
        }
    }

    private static async ValueTask DisposeProviderEnumeratorAsync<T>(
        IAsyncEnumerator<T> enumerator,
        ExceptionDispatchInfo? operationFailure,
        CancellationToken cancellationToken)
    {
        try
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            if (operationFailure is null)
                throw new OperationCanceledException(cancellationToken);
        }
        catch (Exception error) when (error is not
            (OutOfMemoryException or StackOverflowException or AccessViolationException))
        {
            if (operationFailure is null)
                throw EvidenceFailure();
        }
    }

    private static T ReadProviderValue<T>(
        Func<T> read,
        CancellationToken cancellationToken)
    {
        try
        {
            return read();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);
        }
        catch (Exception error) when (error is not
            (OutOfMemoryException or StackOverflowException or AccessViolationException))
        {
            throw EvidenceFailure();
        }
    }

    private static async ValueTask<T> ReadProviderValueAsync<T>(
        Func<ValueTask<T>> read,
        CancellationToken cancellationToken)
    {
        try
        {
            return await read().ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);
        }
        catch (Exception error) when (error is not
            (OutOfMemoryException or StackOverflowException or AccessViolationException))
        {
            throw EvidenceFailure();
        }
    }

    private static T ReadEvidenceValue<T>(Func<T> read)
    {
        try
        {
            return read();
        }
        catch (Exception error) when (error is not
            (OutOfMemoryException or StackOverflowException or AccessViolationException))
        {
            throw EvidenceFailure();
        }
    }

    private static void Require(bool condition)
    {
        if (!condition)
            throw EvidenceFailure();
    }

    private static bool IsLowerSha256(string? value) =>
        value is { Length: 64 } &&
        value.All(character =>
            character is >= '0' and <= '9' or
                >= 'a' and <= 'f');

    private static InvalidDataException EvidenceFailure() => new(EvidenceFailureMessage);

    private static RecordedMappingProvider CreateRecordedMappingProvider(MigrationPlan plan)
    {
        try
        {
            return new RecordedMappingProvider(plan);
        }
        catch (Exception error) when (error is not
            (OutOfMemoryException or StackOverflowException or AccessViolationException))
        {
            throw new InvalidDataException(InvalidRecordedMappingMessage);
        }
    }

    private sealed record ExpectedObject(
        string SourceObjectId,
        IReadOnlySet<string> ColumnObjectIds);

    private sealed record ProjectionResult(
        string TargetSnapshotIdentity,
        string ArtifactDigest,
        long RejectedRowCount,
        long ArtifactBytes);

    /// <summary>
    /// Artifact regeneration verifies the already-recorded plan structurally
    /// without requiring the original custom mapping plugin to be installed.
    /// No conversion is executed while projecting target-owned reject rows.
    /// </summary>
    private sealed class RecordedMappingProvider : IDataTypeMappingProvider
    {
        private readonly IReadOnlyDictionary<string, MigrationTypeMapping> _mappings;
        private readonly IReadOnlyDictionary<string, MigrationDiagnostic> _diagnostics;

        internal RecordedMappingProvider(MigrationPlan plan)
        {
            PolicyId = plan.MappingPolicyId;
            PolicyVersion = plan.MappingPolicyVersion;
            _mappings = plan.Objects
                .SelectMany(item => item.TypeMappings)
                .ToDictionary(item => item.SourceObjectId, StringComparer.Ordinal);
            _diagnostics = plan.Diagnostics
                .ToDictionary(item => item.DiagnosticId, StringComparer.Ordinal);
        }

        public string PolicyId { get; }

        public int PolicyVersion { get; }

        public MigrationTypeMappingDecision Map(MigrationTypeMappingRequest request)
        {
            ArgumentNullException.ThrowIfNull(request);
            if (!_mappings.TryGetValue(
                    request.SourceObject.ObjectId,
                    out MigrationTypeMapping? mapping))
            {
                throw EvidenceFailure();
            }
            MigrationDiagnostic? diagnostic = mapping.DiagnosticId is not null &&
                _diagnostics.TryGetValue(mapping.DiagnosticId, out MigrationDiagnostic? found)
                    ? found
                    : null;
            return new MigrationTypeMappingDecision
            {
                Mapping = mapping,
                Diagnostic = diagnostic,
            };
        }
    }
}
