using System.Runtime.CompilerServices;
using CSharpDB.Migration.Validation;

namespace CSharpDB.Migration;

/// <summary>
/// Snapshot extension required by the Phase 3 runner. Schema, counts, and rows
/// must all be read from this one snapshot instance.
/// </summary>
public interface IMigrationEvidenceValidationSnapshot : IMigrationSchemaValidationSnapshot
{
    MigrationSnapshotConsistencyStatus ConsistencyStatus { get; }
}

/// <summary>
/// Source-snapshot capability for reproducing the complete ordered batch
/// outcome stream used by deterministic-reject apply. The stream covers every
/// included data object in canonical object and batch order and is bound to
/// this snapshot's <see cref="IValidationSnapshot.SnapshotIdentity"/>.
/// </summary>
public interface IMigrationRejectReplayValidationSnapshot :
    IMigrationEvidenceValidationSnapshot
{
    IAsyncEnumerable<MigrationTargetBatch> ReplayOutcomeBatchesAsync(
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Target-snapshot capability for reading all plan-scoped receipts and reject
/// entries from the same immutable reader snapshot used for schema, counts,
/// and rows. Implementations must expose complete ordered streams, including
/// enough state for callers to detect foreign or orphan records, and must bind
/// their complete contents into <see cref="IValidationSnapshot.SnapshotIdentity"/>.
/// Activation must not change that identity.
/// </summary>
public interface IMigrationRejectTargetValidationSnapshot :
    IMigrationEvidenceValidationSnapshot
{
    IAsyncEnumerable<MigrationBatchReceipt> ReadOutcomeReceiptsAsync(
        string planDigest,
        CancellationToken cancellationToken = default);

    IAsyncEnumerable<MigrationRejectLedgerEntry> ReadRejectLedgerAsync(
        string planDigest,
        CancellationToken cancellationToken = default);
}

public sealed record MigrationValidationActivationReceipt
{
    public const string ContractVersion = "csharpdb-migration-activation/v1";

    public required string TargetIdentity { get; init; }

    public required string PlanDigest { get; init; }

    public required string CatalogDigest { get; init; }

    public required string SourceSnapshotIdentity { get; init; }

    public required string TargetSnapshotIdentity { get; init; }

    public required MigrationValidationLevel Level { get; init; }

    public required string CanonicalizationVersion { get; init; }

    public required string CanonicalizationContractDigest { get; init; }

    public required string ReportDigest { get; init; }
}

/// <summary>
/// Opaque, runner-issued authority to ask a staged target to activate a
/// published validation report. Public callers can pass permits through but
/// cannot construct one without completing the validation runner's publish
/// boundary.
/// </summary>
public sealed class MigrationValidationActivationPermit
{
    internal MigrationValidationActivationPermit(
        MigrationValidationActivationReceipt receipt,
        string publishedReportPath)
    {
        ArgumentNullException.ThrowIfNull(receipt);
        ArgumentException.ThrowIfNullOrWhiteSpace(publishedReportPath);
        Receipt = receipt;
        PublishedReportPath = Path.GetFullPath(publishedReportPath);
    }

    public MigrationValidationActivationReceipt Receipt { get; }

    public string PublishedReportPath { get; }
}

/// <summary>
/// Narrow capability implemented by staged targets that can atomically persist
/// the validation receipt and transition to their activated lifecycle state.
/// </summary>
public interface IMigrationValidationActivationTarget
{
    ValueTask<MigrationValidationActivationReceipt?> ReadActivationReceiptAsync(
        CancellationToken cancellationToken = default);

    ValueTask ActivateAsync(
        MigrationValidationActivationPermit permit,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Replays a migration data source's immutable bound snapshot through the same
/// planned conversions used during apply, yielding target-shaped rows for
/// canonical logical comparison.
/// </summary>
public sealed class MigrationDataSourceValidationSnapshot :
    IMigrationRejectReplayValidationSnapshot
{
    private readonly MigrationPlan _plan;
    private readonly MigrationCatalog _catalog;
    private readonly MigrationOutcomeBatchReplayer _replayer;
    private bool _disposed;

    public MigrationDataSourceValidationSnapshot(
        MigrationPlan plan,
        MigrationCatalog catalog,
        IMigrationDataSource source)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(source);
        MigrationPlanReadinessValidator.ValidateForApply(plan, catalog);
        if (plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects &&
            source is not IMigrationRejectAwareDataSource)
        {
            // Preserve the established validation-policy failure for sources
            // that do not opt into replay. Reject-aware sources proceed to the
            // exact contract and rule-registry gate in the shared replayer.
            MigrationValidationPolicyValidator.ValidateForExecution(plan);
        }

        _plan = plan;
        _catalog = catalog;
        _replayer = new MigrationOutcomeBatchReplayer(plan, catalog, source);
        SnapshotIdentity = _replayer.SnapshotIdentity;
        ConsistencyStatus = plan.Source.Consistency.Kind switch
        {
            MigrationConsistencyKind.Immutable or
            MigrationConsistencyKind.Snapshot or
            MigrationConsistencyKind.Backup or
            MigrationConsistencyKind.Transaction or
            MigrationConsistencyKind.Watermark => MigrationSnapshotConsistencyStatus.Established,
            MigrationConsistencyKind.BestEffort => MigrationSnapshotConsistencyStatus.NotEstablished,
            MigrationConsistencyKind.Unavailable => MigrationSnapshotConsistencyStatus.Unavailable,
            _ => MigrationSnapshotConsistencyStatus.Unavailable,
        };
    }

    public string SnapshotIdentity { get; }

    public MigrationSnapshotConsistencyStatus ConsistencyStatus { get; }

    public ValueTask<MigrationNormalizedSchema> ReadSchemaAsync(
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(MigrationNormalizedSchemaContract.CreateExpected(_plan, _catalog));
    }

    public async ValueTask<long> CountAsync(
        string objectId,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        long count = 0;
        await foreach (MigrationReplayedOutcomeBatch replayedBatch in _replayer
                           .ReplayObjectAsync(objectId, cancellationToken)
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            count = checked(count + replayedBatch.Batch.Rows.Count);
        }
        return count;
    }

    public async IAsyncEnumerable<MigrationValidationRow> ReadRowsAsync(
        string objectId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await foreach (MigrationReplayedOutcomeBatch replayedBatch in _replayer
                           .ReplayObjectAsync(objectId, cancellationToken)
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            foreach (MigrationTargetRow row in replayedBatch.Batch.Rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return new MigrationValidationRow
                {
                    StableKey = row.StableKey,
                    Values = row.Values,
                };
            }
        }
    }

    public async IAsyncEnumerable<MigrationTargetBatch> ReplayOutcomeBatchesAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await foreach (MigrationReplayedOutcomeBatch replayedBatch in _replayer
                           .ReplayAsync(cancellationToken)
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            ThrowIfDisposed();
            yield return replayedBatch.Batch;
        }
    }

    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return ValueTask.CompletedTask;
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);
}
