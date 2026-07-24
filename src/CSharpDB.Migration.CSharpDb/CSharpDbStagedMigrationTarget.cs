using System.Buffers.Binary;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Engine;
using CSharpDB.Migration.Canonicalization;
using CSharpDB.Migration.Validation;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.CSharpDb;

/// <summary>
/// A migration-owned CSharpDB file that records schema stages and batch
/// receipts inside the same durability boundary as the data they describe.
/// A successful validation receipt is persisted atomically with activation;
/// replacement of a user-owned database remains outside this target.
/// </summary>
public sealed class CSharpDbStagedMigrationTarget :
    IMigrationTarget,
    IMigrationBatchDigestContractTarget,
    IMigrationRejectLedgerTarget,
    IMigrationValidationActivationTarget
{
    private const int MigrationPageCachePages = 2048;
    private const string OutcomeDigestFormat = "csharpdb-migration-target-outcomes/v1";

    private static readonly UTF8Encoding StrictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    private readonly string _targetPath;
    private readonly string _leasePath;
    private readonly FileStream _lease;
    private readonly Database _database;
    private readonly MigrationPlan _plan;
    private readonly MigrationCatalog _catalog;
    private readonly string _planDigest;
    private readonly string _snapshotIdentity;
    private readonly string _targetTag;
    private readonly IReadOnlyDictionary<string, MigrationPlanObject> _planObjects;
    private readonly IReadOnlyDictionary<string, MigrationCatalogObject> _catalogObjects;
    private readonly IReadOnlyDictionary<string, CSharpDbCollectionMigrationBinding>
        _collectionBindings;
    private readonly ICSharpDbMigrationFaultInjector _faultInjector;
    private readonly SemaphoreSlim _mutationGate = new(1, 1);
    private readonly Dictionary<string, ObjectBatchProgress> _validatedObjectProgress =
        new(StringComparer.Ordinal);
    private long _validatedRejectedRows;
    private long _validatedRawValueBytes;
    private long _validatedArtifactBytes;
    private string _validatedOutcomeDigest = string.Empty;
    private bool _requiresReopen;
    private bool _disposed;

    private CSharpDbStagedMigrationTarget(
        string targetPath,
        FileStream lease,
        Database database,
        MigrationPlan plan,
        MigrationCatalog catalog,
        string snapshotIdentity,
        string targetIdentity,
        string targetTag,
        ICSharpDbMigrationFaultInjector? faultInjector)
    {
        _targetPath = targetPath;
        _leasePath = LeasePath(targetPath);
        _lease = lease;
        _database = database;
        _plan = plan;
        _catalog = catalog;
        _planDigest = MigrationArtifactSerializer.ComputePlanDigest(plan);
        _snapshotIdentity = snapshotIdentity;
        _targetTag = targetTag;
        TargetIdentity = targetIdentity;
        _planObjects = plan.Objects.ToDictionary(item => item.SourceObjectId, StringComparer.Ordinal);
        _catalogObjects = catalog.Objects.ToDictionary(item => item.ObjectId, StringComparer.Ordinal);
        _collectionBindings = CSharpDbCollectionMigrationBinding.CreateAll(plan, catalog);
        _faultInjector = faultInjector ?? NoOpMigrationFaultInjector.Instance;
        _validatedArtifactBytes = plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects
            ? MigrationRejectLedgerCodec.GetArtifactHeaderByteCount(_planDigest)
            : 0;
    }

    public string TargetIdentity { get; }

    private bool IsLegacyTarget => string.Equals(
        _targetTag,
        CSharpDbMigrationSql.LegacyTargetTag,
        StringComparison.Ordinal);

    private bool AllowsLegacyValidationSnapshotIdentity => _targetTag is
        CSharpDbMigrationSql.LegacyTargetTag or
        CSharpDbMigrationSql.OutcomeUnboundTargetTag;

    public string BatchDigestFormat => IsLegacyTarget
        ? MigrationBatchDigest.LegacyFormat
        : MigrationBatchDigest.Format;

    private string ExpectedRejectContract => _plan.Load.RejectMode switch
    {
        MigrationRejectMode.FailFast => MigrationRejectContract.DeterministicFailFastV1,
        MigrationRejectMode.DeterministicRejects => MigrationRejectContract.DeterministicRejectsV1,
        _ => throw new InvalidDataException("Migration plan reject mode is unsupported."),
    };

    public static async ValueTask<CSharpDbStagedMigrationTarget> CreateNewAsync(
        string targetPath,
        MigrationPlan plan,
        MigrationCatalog catalog,
        string sourceSnapshotIdentity,
        ICSharpDbMigrationFaultInjector? faultInjector = null,
        CancellationToken cancellationToken = default)
    {
        string fullPath = ValidateFactoryInputs(targetPath, plan, catalog, sourceSnapshotIdentity);
        FileStream lease = AcquireLease(fullPath);
        Database? database = null;
        bool databaseCreated = false;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (File.Exists(fullPath))
                throw new IOException($"Staged migration target '{fullPath}' already exists.");
            if (File.Exists(WalPath(fullPath)))
                throw new IOException($"Staged migration target companion WAL '{WalPath(fullPath)}' already exists.");

            database = await Database.CreateNewAsync(
                fullPath,
                CreateDatabaseOptions(),
                cancellationToken).ConfigureAwait(false);
            databaseCreated = true;
            string targetIdentity = Guid.NewGuid().ToString("D", CultureInfo.InvariantCulture);
            await InitializeAsync(
                database,
                plan,
                sourceSnapshotIdentity,
                targetIdentity,
                cancellationToken).ConfigureAwait(false);
            return new CSharpDbStagedMigrationTarget(
                fullPath,
                lease,
                database,
                plan,
                catalog,
                sourceSnapshotIdentity,
                targetIdentity,
                CSharpDbMigrationSql.TargetTag,
                faultInjector);
        }
        catch
        {
            if (database is not null)
            {
                try
                {
                    await database.DisposeAsync().ConfigureAwait(false);
                }
                catch
                {
                }
            }
            lease.Dispose();
            TryDelete(LeasePath(fullPath));
            if (databaseCreated)
            {
                TryDelete(WalPath(fullPath));
                TryDelete(fullPath);
            }
            throw;
        }
    }

    public static async ValueTask<CSharpDbStagedMigrationTarget> OpenResumeAsync(
        string targetPath,
        MigrationPlan plan,
        MigrationCatalog catalog,
        string sourceSnapshotIdentity,
        ICSharpDbMigrationFaultInjector? faultInjector = null,
        CancellationToken cancellationToken = default)
    {
        string fullPath = ValidateFactoryInputs(targetPath, plan, catalog, sourceSnapshotIdentity);
        FileStream lease = AcquireLease(fullPath);
        Database? database = null;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!File.Exists(fullPath))
                throw new FileNotFoundException("Staged migration target does not exist for resume.", fullPath);

            database = await Database.OpenAsync(
                fullPath,
                CreateDatabaseOptions(),
                cancellationToken).ConfigureAwait(false);
            TargetState state = await ReadStateAsync(database, cancellationToken).ConfigureAwait(false);
            ValidateState(state, plan, sourceSnapshotIdentity);
            ValidateInternalReceiptSchema(database, state.TargetTag);
            var target = new CSharpDbStagedMigrationTarget(
                fullPath,
                lease,
                database,
                plan,
                catalog,
                sourceSnapshotIdentity,
                state.TargetIdentity,
                state.TargetTag,
                faultInjector);
            await target.ValidateStoredBatchStateAsync(cancellationToken).ConfigureAwait(false);
            MigrationValidationActivationReceipt? activation =
                await target.ReadActivationReceiptAsync(cancellationToken).ConfigureAwait(false);
            bool activated = string.Equals(
                state.LifecycleState,
                CSharpDbMigrationSql.ActivatedState,
                StringComparison.Ordinal);
            if (activated != (activation is not null))
            {
                throw new InvalidDataException(
                    "Migration target lifecycle and validation receipt are not atomically consistent.");
            }
            return target;
        }
        catch
        {
            if (database is not null)
            {
                try
                {
                    await database.DisposeAsync().ConfigureAwait(false);
                }
                catch
                {
                }
            }
            lease.Dispose();
            TryDelete(LeasePath(fullPath));
            throw;
        }
    }

    public async ValueTask ApplySchemaAsync(
        MigrationPlan plan,
        MigrationCatalog catalog,
        MigrationSchemaStage stage,
        CancellationToken cancellationToken = default)
    {
        ThrowIfMutationUnavailable();
        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfMutationUnavailable();
            await ApplySchemaCoreAsync(plan, catalog, stage, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private async ValueTask ApplySchemaCoreAsync(
        MigrationPlan plan,
        MigrationCatalog catalog,
        MigrationSchemaStage stage,
        CancellationToken cancellationToken)
    {
        ValidateSuppliedArtifacts(plan, catalog);
        IReadOnlyList<string> actions = CSharpDbMigrationSql.BuildStageActions(plan, catalog, stage);
        string stageDigest = CSharpDbMigrationSql.ComputeStageDigest(plan, stage, actions);
        StageReceipt? existing = await ReadStageAsync(stage, cancellationToken).ConfigureAwait(false);
        if (existing is not null)
        {
            ValidateStageReceipt(existing, stage, stageDigest, actions.Count);
            return;
        }

        await RequireMutableLifecycleAsync(cancellationToken).ConfigureAwait(false);

        MigrationSchemaStage? previous = PreviousStage(stage);
        if (previous is MigrationSchemaStage previousStage)
        {
            IReadOnlyList<string> previousActions = CSharpDbMigrationSql.BuildStageActions(
                plan,
                catalog,
                previousStage);
            StageReceipt? previousReceipt = await ReadStageAsync(
                previousStage,
                cancellationToken).ConfigureAwait(false);
            if (previousReceipt is null)
                throw new InvalidDataException($"Migration schema stage '{stage}' cannot run before '{previousStage}'.");
            ValidateStageReceipt(
                previousReceipt,
                previousStage,
                CSharpDbMigrationSql.ComputeStageDigest(plan, previousStage, previousActions),
                previousActions.Count);
        }

        if (stage == MigrationSchemaStage.SecondaryIndexes)
        {
            await ValidateStoredBatchStateAsync(cancellationToken).ConfigureAwait(false);
            RequireCompleteDataBatchChains();
        }

        bool transactionStarted = false;
        bool commitInvoked = false;
        try
        {
            await _database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            transactionStarted = true;
            foreach (string action in actions)
            {
                if (CSharpDbMigrationSql.TryParseCollectionAction(
                        action,
                        out string collectionName))
                {
                    await _database.EnsureJsonDocumentCollectionAsync(
                            collectionName,
                            cancellationToken)
                        .ConfigureAwait(false);
                }
                else
                {
                    await ExecuteNonQueryAsync(
                            _database,
                            action,
                            cancellationToken)
                        .ConfigureAwait(false);
                }
            }

            InsertBatch stageInsert = _database.PrepareInsertBatch(CSharpDbMigrationSql.StageTable, 1);
            stageInsert.AddRow(
                DbValue.FromText(CSharpDbMigrationSql.StageTag),
                DbValue.FromText(TargetIdentity),
                DbValue.FromText(_planDigest),
                DbValue.FromInteger((long)stage),
                DbValue.FromText(stage.ToString()),
                DbValue.FromText(stageDigest),
                DbValue.FromInteger(actions.Count));
            if (await stageInsert.ExecuteAsync(cancellationToken).ConfigureAwait(false) != 1)
                throw new InvalidDataException($"Migration schema stage '{stage}' did not persist one receipt.");

            string lifecycle = LifecycleAfter(stage);
            await using (var update = await _database.ExecuteAsync(
                $"UPDATE {CSharpDbMigrationSql.Quote(CSharpDbMigrationSql.StateTable)} " +
                $"SET {CSharpDbMigrationSql.Quote("lifecycle_state")} = {CSharpDbMigrationSql.Literal(lifecycle)} " +
                $"WHERE {CSharpDbMigrationSql.Quote("singleton")} = 1",
                cancellationToken).ConfigureAwait(false))
            {
                if (update.RowsAffected != 1)
                    throw new InvalidDataException("Migration target state row is missing or duplicated.");
            }

            cancellationToken.ThrowIfCancellationRequested();
            commitInvoked = true;
            await _database.CommitAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            if (commitInvoked)
                _requiresReopen = true;
            if (transactionStarted && !commitInvoked)
                await TryRollbackAsync(_database).ConfigureAwait(false);
            throw;
        }
    }

    public async ValueTask<MigrationBatchReceipt> WriteBatchAsync(
        MigrationTargetBatch batch,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ThrowIfMutationUnavailable();
        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfMutationUnavailable();
            return await WriteBatchCoreAsync(batch, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private async ValueTask<MigrationBatchReceipt> WriteBatchCoreAsync(
        MigrationTargetBatch batch,
        CancellationToken cancellationToken)
    {
        ValidateTargetBatch(batch);

        MigrationBatchReceipt? existing = await ReadReceiptAsync(
            batch.PlanDigest,
            batch.SourceObjectId,
            batch.BatchOrdinal,
            cancellationToken).ConfigureAwait(false);
        if (existing is not null)
        {
            ValidateReceiptAgainstBatch(existing, batch);
            await ValidateStoredBatchStateAsync(cancellationToken).ConfigureAwait(false);
            return existing;
        }

        await RequireMutableLifecycleAsync(cancellationToken).ConfigureAwait(false);
        ObjectBatchProgress batchProgress = await ValidateNewBatchSequenceAsync(
            batch,
            cancellationToken).ConfigureAwait(false);
        RejectBatchStatistics rejectStatistics = ValidateProjectedRejectRunLimits(batch);
        ObjectBatchProgress committedProgress = batchProgress.Advance(batch);

        await RequireStageAsync(MigrationSchemaStage.LoadEssential, cancellationToken).ConfigureAwait(false);
        if (await ReadStageAsync(MigrationSchemaStage.SecondaryIndexes, cancellationToken).ConfigureAwait(false) is not null)
        {
            throw new InvalidDataException(
                "A missing data batch cannot be appended after post-load schema stages have begun.");
        }

        MigrationPlanObject tablePlan = _planObjects[batch.SourceObjectId];
        _collectionBindings.TryGetValue(
            batch.SourceObjectId,
            out CSharpDbCollectionMigrationBinding? collectionBinding);
        InsertBatch? dataInsert = collectionBinding is null
            ? _database.PrepareInsertBatch(tablePlan.TargetName!, batch.Rows.Count)
            : null;
        if (dataInsert is not null)
        {
            foreach (MigrationTargetRow row in batch.Rows)
                dataInsert.AddRow(row.Values.ToArray());
        }

        InsertBatch? rejectInsert = IsLegacyTarget
            ? null
            : _database.PrepareInsertBatch(
                CSharpDbMigrationSql.RejectTable,
                batch.RejectedRows.Count);
        foreach (MigrationRejectedRow rejectedRow in batch.RejectedRows)
        {
            rejectInsert!.AddRow(
                DbValue.FromText(CSharpDbMigrationSql.RejectTag),
                DbValue.FromText(batch.PlanDigest),
                DbValue.FromText(batch.SourceObjectId),
                DbValue.FromInteger(batch.BatchOrdinal),
                DbValue.FromInteger(rejectedRow.SourceRowOrdinal),
                DbValue.FromText(rejectedRow.RuleId),
                rejectedRow.ColumnObjectId is null
                    ? DbValue.Null
                    : DbValue.FromText(rejectedRow.ColumnObjectId),
                DbValue.FromText(MigrationRejectLedgerCodec.SerializeEvidence(rejectedRow.Evidence)));
        }

        var receipt = new MigrationBatchReceipt
        {
            TargetIdentity = TargetIdentity,
            PlanDigest = batch.PlanDigest,
            CatalogDigest = batch.CatalogDigest,
            SourceFingerprint = batch.SourceFingerprint,
            SourceSnapshotIdentity = batch.SourceSnapshotIdentity,
            SourceObjectId = batch.SourceObjectId,
            BatchOrdinal = batch.BatchOrdinal,
            StartCursor = batch.StartCursor,
            NextCursor = batch.NextCursor,
            BatchDigest = batch.BatchDigest,
            RejectContractVersion = batch.RejectContractVersion,
            RejectDigest = batch.RejectDigest,
            RowCount = batch.Rows.Count,
            RejectedRowCount = batch.RejectedRows.Count,
        };
        InsertBatch receiptInsert = _database.PrepareInsertBatch(CSharpDbMigrationSql.ReceiptTable, 1);
        receiptInsert.AddRow(ReceiptValues(receipt));

        bool transactionStarted = false;
        bool commitInvoked = false;
        try
        {
            await _database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            transactionStarted = true;
            MigrationBatchReceipt? concurrent = await ReadReceiptAsync(
                batch.PlanDigest,
                batch.SourceObjectId,
                batch.BatchOrdinal,
                cancellationToken).ConfigureAwait(false);
            if (concurrent is not null)
            {
                ValidateReceiptAgainstBatch(concurrent, batch);
                await _database.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                transactionStarted = false;
                return concurrent;
            }
            _ = await ValidateNewBatchSequenceAsync(batch, cancellationToken).ConfigureAwait(false);
            await _faultInjector.InjectAsync(
                CSharpDbMigrationFaultPoint.BeforeRows,
                batch,
                cancellationToken).ConfigureAwait(false);
            int rowsAffected;
            if (collectionBinding is null)
            {
                rowsAffected = await dataInsert!.ExecuteAsync(cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                rowsAffected = 0;
                foreach (MigrationTargetRow row in batch.Rows)
                {
                    string key = row.Values[collectionBinding.KeyValueIndex].AsText;
                    string document =
                        row.Values[collectionBinding.DocumentValueIndex].AsText;
                    byte[] documentBytes;
                    try
                    {
                        documentBytes = StrictUtf8.GetBytes(document);
                    }
                    catch (EncoderFallbackException error)
                    {
                        throw new InvalidDataException(
                            "Migration collection document contains invalid Unicode scalar data.",
                            error);
                    }

                    await _database.InsertCanonicalJsonDocumentAsync(
                            collectionBinding.TargetName,
                            key,
                            documentBytes,
                            cancellationToken)
                        .ConfigureAwait(false);
                    rowsAffected++;
                }
            }
            if (rowsAffected != batch.Rows.Count)
                throw new InvalidDataException("Target row count differs from the converted migration batch.");
            await _faultInjector.InjectAsync(
                CSharpDbMigrationFaultPoint.AfterRowsBeforeReceipt,
                batch,
                cancellationToken).ConfigureAwait(false);
            int rejectsAffected = rejectInsert is null
                ? 0
                : await rejectInsert.ExecuteAsync(cancellationToken).ConfigureAwait(false);
            if (rejectsAffected != batch.RejectedRows.Count)
                throw new InvalidDataException("Target reject count differs from the migration batch.");
            await _faultInjector.InjectAsync(
                CSharpDbMigrationFaultPoint.AfterRejectsBeforeReceipt,
                batch,
                cancellationToken).ConfigureAwait(false);
            if (await receiptInsert.ExecuteAsync(cancellationToken).ConfigureAwait(false) != 1)
                throw new InvalidDataException("Migration batch receipt was not persisted.");
            await _faultInjector.InjectAsync(
                CSharpDbMigrationFaultPoint.AfterReceiptBeforeCommit,
                batch,
                cancellationToken).ConfigureAwait(false);

            cancellationToken.ThrowIfCancellationRequested();
            commitInvoked = true;
            await _database.CommitAsync(CancellationToken.None).ConfigureAwait(false);
            _validatedRejectedRows = checked(_validatedRejectedRows + rejectStatistics.RejectedRows);
            _validatedRawValueBytes = checked(_validatedRawValueBytes + rejectStatistics.RawValueBytes);
            _validatedArtifactBytes = checked(
                _validatedArtifactBytes + rejectStatistics.CanonicalArtifactBytes);
            _validatedObjectProgress[batch.SourceObjectId] = committedProgress;
        }
        catch
        {
            if (commitInvoked)
                _requiresReopen = true;
            if (transactionStarted && !commitInvoked)
                await TryRollbackAsync(_database).ConfigureAwait(false);
            throw;
        }

        await _faultInjector.InjectAsync(
            CSharpDbMigrationFaultPoint.AfterCommit,
            batch,
            cancellationToken).ConfigureAwait(false);
        return receipt;
    }

    public async ValueTask<MigrationBatchReceipt?> ReadReceiptAsync(
        string planDigest,
        string sourceObjectId,
        long batchOrdinal,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (!string.Equals(planDigest, _planDigest, StringComparison.Ordinal))
            throw new InvalidDataException("Receipt lookup plan digest does not match the staged target binding.");
        if (batchOrdinal < 0)
            throw new ArgumentOutOfRangeException(nameof(batchOrdinal));

        string sql = ReceiptSelect() +
            $" WHERE {CSharpDbMigrationSql.Quote("plan_digest")} = {CSharpDbMigrationSql.Literal(planDigest)}" +
            $" AND {CSharpDbMigrationSql.Quote("source_object_id")} = {CSharpDbMigrationSql.Literal(sourceObjectId)}" +
            $" AND {CSharpDbMigrationSql.Quote("batch_ordinal")} = {batchOrdinal.ToString(CultureInfo.InvariantCulture)}";
        await using var result = await _database.ExecuteAsync(sql, cancellationToken).ConfigureAwait(false);
        if (!await result.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            return null;
        MigrationBatchReceipt receipt = MapReceipt(result.Current);
        if (await result.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            throw new InvalidDataException("Migration target contains duplicate batch receipts.");
        ValidateStoredReceipt(receipt);
        await ValidateReceiptLedgerAsync(receipt, cancellationToken).ConfigureAwait(false);
        return receipt;
    }

    public async IAsyncEnumerable<MigrationBatchReceipt> ReadReceiptsAsync(
        string planDigest,
        string sourceObjectId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (!string.Equals(planDigest, _planDigest, StringComparison.Ordinal))
            throw new InvalidDataException("Receipt lookup plan digest does not match the staged target binding.");

        string sql = ReceiptSelect() +
            $" WHERE {CSharpDbMigrationSql.Quote("plan_digest")} = {CSharpDbMigrationSql.Literal(planDigest)}" +
            $" AND {CSharpDbMigrationSql.Quote("source_object_id")} = {CSharpDbMigrationSql.Literal(sourceObjectId)}" +
            $" ORDER BY {CSharpDbMigrationSql.Quote("batch_ordinal")}";
        await using var result = await _database.ExecuteAsync(sql, cancellationToken).ConfigureAwait(false);
        long expectedOrdinal = 0;
        await foreach (DbValue[] row in result.GetRowsAsync(cancellationToken).ConfigureAwait(false))
        {
            MigrationBatchReceipt receipt = MapReceipt(row);
            ValidateStoredReceipt(receipt);
            if (receipt.BatchOrdinal != expectedOrdinal)
                throw new InvalidDataException("Migration target receipt ordinals are not contiguous from zero.");
            expectedOrdinal++;
            yield return receipt;
        }
    }

    public async IAsyncEnumerable<MigrationRejectLedgerEntry> ReadRejectLedgerAsync(
        string planDigest,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (!string.Equals(planDigest, _planDigest, StringComparison.Ordinal))
            throw new InvalidDataException("Reject-ledger lookup plan digest does not match the staged target binding.");
        if (IsLegacyTarget)
            yield break;

        string sql = RejectSelect() +
            $" WHERE {CSharpDbMigrationSql.Quote("plan_digest")} = {CSharpDbMigrationSql.Literal(planDigest)}" +
            $" ORDER BY {CSharpDbMigrationSql.Quote("source_object_id")}, " +
            $"{CSharpDbMigrationSql.Quote("batch_ordinal")}, " +
            $"{CSharpDbMigrationSql.Quote("source_row_ordinal")}";
        await using var result = await _database.ExecuteAsync(sql, cancellationToken).ConfigureAwait(false);
        await foreach (DbValue[] row in result.GetRowsAsync(cancellationToken).ConfigureAwait(false))
            yield return MapRejectLedgerEntry(row);
    }

    private async ValueTask ValidateStoredBatchStateAsync(CancellationToken cancellationToken)
    {
        long receiptCount = 0;
        long expectedRejectCount = 0;
        long artifactBytes = _plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects
            ? MigrationRejectLedgerCodec.GetArtifactHeaderByteCount(_planDigest)
            : 0;
        var objectProgress = new Dictionary<string, ObjectBatchProgress>(StringComparer.Ordinal);
        bool loadSchemaExists = await ReadStageAsync(
            MigrationSchemaStage.LoadEssential,
            cancellationToken).ConfigureAwait(false) is not null;
        bool postLoadSchemaExists = await ReadStageAsync(
            MigrationSchemaStage.SecondaryIndexes,
            cancellationToken).ConfigureAwait(false) is not null;
        MigrationPlanObject[] dataObjects = _plan.Objects
            .Where(planObject => planObject.Included &&
                _catalogObjects.TryGetValue(
                    planObject.SourceObjectId,
                    out MigrationCatalogObject? catalogObject) &&
                catalogObject.Kind is MigrationObjectKind.Table or MigrationObjectKind.Collection)
            .OrderBy(item => item.SourceObjectId, StringComparer.Ordinal)
            .ToArray();
        using IncrementalHash outcomeHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendOutcomeString(outcomeHash, OutcomeDigestFormat);
        AppendOutcomeString(outcomeHash, TargetIdentity);
        AppendOutcomeString(outcomeHash, _planDigest);
        AppendOutcomeInt32(outcomeHash, dataObjects.Length);

        foreach (MigrationPlanObject objectPlan in dataObjects)
        {
            string sourceObjectId = objectPlan.SourceObjectId;
            string receiptWhere =
                $"{CSharpDbMigrationSql.Quote("plan_digest")} = {CSharpDbMigrationSql.Literal(_planDigest)} " +
                $"AND {CSharpDbMigrationSql.Quote("source_object_id")} = " +
                CSharpDbMigrationSql.Literal(sourceObjectId);
            long objectReceiptCount = await CountRowsAsync(
                CSharpDbMigrationSql.ReceiptTable,
                receiptWhere,
                cancellationToken).ConfigureAwait(false);
            AppendOutcomeString(outcomeHash, sourceObjectId);
            AppendOutcomeInt64(outcomeHash, objectReceiptCount);
            long acceptedRows = 0;
            long attemptedRows = 0;
            string? expectedStartCursor = null;
            for (long ordinal = 0; ordinal < objectReceiptCount; ordinal++)
            {
                MigrationBatchReceipt receipt = await ReadReceiptAsync(
                        _planDigest,
                        sourceObjectId,
                        ordinal,
                        cancellationToken)
                    .ConfigureAwait(false) ??
                    throw new InvalidDataException("Stored migration receipt ordinals are not contiguous from zero.");
                if (!string.Equals(receipt.StartCursor, expectedStartCursor, StringComparison.Ordinal))
                    throw new InvalidDataException("Stored migration receipt cursor chain is invalid.");
                await ValidateStoredRejectOrdinalsAsync(
                    receipt,
                    attemptedRows,
                    cancellationToken).ConfigureAwait(false);
                AppendOutcomeReceipt(outcomeHash, receipt);
                expectedStartCursor = receipt.NextCursor;
                acceptedRows = checked(acceptedRows + receipt.RowCount);
                attemptedRows = checked(
                    attemptedRows + receipt.RowCount + receipt.RejectedRowCount);
                expectedRejectCount = checked(expectedRejectCount + receipt.RejectedRowCount);
            }
            receiptCount = checked(receiptCount + objectReceiptCount);
            objectProgress[sourceObjectId] = new ObjectBatchProgress(
                objectReceiptCount,
                attemptedRows,
                expectedStartCursor);

            if (postLoadSchemaExists && objectReceiptCount > 0 && expectedStartCursor is not null)
            {
                throw new InvalidDataException(
                    "Stored migration receipt chain advanced to post-load schema before end of source.");
            }

            if (loadSchemaExists)
            {
                long physicalRows = await CountRowsAsync(
                    ResolvePhysicalTableName(objectPlan),
                    where: null,
                    cancellationToken).ConfigureAwait(false);
                if (physicalRows != acceptedRows)
                {
                    throw new InvalidDataException(
                        "Stored migration receipt row totals do not match the staged target data.");
                }
            }
        }

        long totalReceipts = await CountRowsAsync(
            CSharpDbMigrationSql.ReceiptTable,
            where: null,
            cancellationToken).ConfigureAwait(false);
        if (totalReceipts != receiptCount)
            throw new InvalidDataException("Stored migration target contains an orphan or foreign batch receipt.");

        long rejectCount = 0;
        long rawValueBytes = 0;
        await foreach (MigrationRejectLedgerEntry entry in ReadRejectLedgerAsync(
                           _planDigest,
                           cancellationToken).ConfigureAwait(false))
        {
            rejectCount = checked(rejectCount + 1);
            rawValueBytes = checked(rawValueBytes + entry.RawValueByteCount);
            artifactBytes = checked(
                artifactBytes + entry.CanonicalEntryByteCount + 1L);
        }
        long totalRejects = IsLegacyTarget
            ? 0
            : await CountRowsAsync(
                CSharpDbMigrationSql.RejectTable,
                where: null,
                cancellationToken).ConfigureAwait(false);
        if (rejectCount != expectedRejectCount || totalRejects != expectedRejectCount)
            throw new InvalidDataException("Stored migration target contains an orphan reject ledger entry.");

        if (_plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects)
        {
            MigrationDeterministicRejectPolicy policy = _plan.Load.RejectPolicy ??
                throw new InvalidDataException("Deterministic reject policy is missing from the staged target plan.");
            if (rejectCount > policy.MaxRejectedRowsPerRun ||
                rawValueBytes > policy.MaxRawValueBytesPerRun ||
                artifactBytes > policy.MaxArtifactBytes)
            {
                throw new InvalidDataException(
                    "Stored migration reject ledger exceeds the plan-bound run or artifact limits.");
            }
        }

        _validatedObjectProgress.Clear();
        foreach ((string sourceObjectId, ObjectBatchProgress progress) in objectProgress)
            _validatedObjectProgress.Add(sourceObjectId, progress);
        _validatedRejectedRows = rejectCount;
        _validatedRawValueBytes = rawValueBytes;
        _validatedArtifactBytes = artifactBytes;
        _validatedOutcomeDigest = Convert.ToHexString(outcomeHash.GetHashAndReset()).ToLowerInvariant();
    }

    private async ValueTask<long> CountRowsAsync(
        string tableName,
        string? where,
        CancellationToken cancellationToken)
    {
        string sql = $"SELECT COUNT(*) FROM {CSharpDbMigrationSql.Quote(tableName)}";
        if (where is not null)
            sql += $" WHERE {where}";
        await using var result = await _database.ExecuteAsync(sql, cancellationToken).ConfigureAwait(false);
        if (!await result.MoveNextAsync(cancellationToken).ConfigureAwait(false) ||
            result.Current.Length != 1)
        {
            throw new InvalidDataException("Migration internal row count returned an invalid shape.");
        }
        long count = result.Current[0].AsInteger;
        if (count < 0 || await result.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            throw new InvalidDataException("Migration internal row count is invalid.");
        return count;
    }

    public async ValueTask<IValidationSnapshot> OpenValidationSnapshotAsync(
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await RequireStageAsync(MigrationSchemaStage.Triggers, cancellationToken).ConfigureAwait(false);
        TargetState state = await ReadStateAsync(_database, cancellationToken).ConfigureAwait(false);
        ValidateState(state, _plan, _snapshotIdentity);
        if (state.LifecycleState is not (
                CSharpDbMigrationSql.AwaitingValidationState or
                CSharpDbMigrationSql.ActivatedState))
        {
            throw new InvalidDataException(
                $"Migration validation cannot open while the target lifecycle is '{state.LifecycleState}'.");
        }

        await ValidateStoredBatchStateAsync(cancellationToken).ConfigureAwait(false);
        MigrationNormalizedSchema schema = CSharpDbActualSchemaReader.Capture(
            _database,
            _plan,
            _catalog,
            _collectionBindings,
            IsMigrationMetadataTable,
            cancellationToken);
        string snapshotIdentity = ValidationSnapshotIdentity(
            TargetIdentity,
            CSharpDbMigrationSql.AwaitingValidationState,
            _validatedOutcomeDigest);
        if (AllowsLegacyValidationSnapshotIdentity &&
            string.Equals(
                state.LifecycleState,
                CSharpDbMigrationSql.ActivatedState,
                StringComparison.Ordinal))
        {
            MigrationValidationActivationReceipt activation =
                await ReadActivationReceiptCoreAsync(cancellationToken).ConfigureAwait(false) ??
                throw new InvalidDataException(
                    "Activated migration target is missing its validation receipt.");
            ValidateActivationReceiptBinding(
                activation,
                allowLegacySnapshotIdentity: true);
            snapshotIdentity = activation.TargetSnapshotIdentity;
        }
        return new CSharpDbValidationSnapshot(
            _database.CreateReaderSession(),
            snapshotIdentity,
            TargetIdentity,
            _planDigest,
            _plan.CatalogDigest,
            _plan.Source.Fingerprint,
            _snapshotIdentity,
            IsLegacyTarget,
            _plan.Load,
            _validatedObjectProgress.ToDictionary(
                item => item.Key,
                item => item.Value.ReceiptCount,
                StringComparer.Ordinal),
            _validatedRejectedRows,
            _planObjects,
            _collectionBindings,
            _catalog,
            schema);
    }

    public async ValueTask<MigrationValidationActivationReceipt?> ReadActivationReceiptAsync(
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        MigrationValidationActivationReceipt? receipt = await ReadActivationReceiptCoreAsync(
            cancellationToken).ConfigureAwait(false);
        if (receipt is not null)
        {
            ValidateActivationReceiptBinding(
                receipt,
                allowLegacySnapshotIdentity: AllowsLegacyValidationSnapshotIdentity);
        }
        return receipt;
    }

    public async ValueTask ActivateAsync(
        MigrationValidationActivationPermit permit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(permit);
        ThrowIfMutationUnavailable();
        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfMutationUnavailable();
            await ActivateCoreAsync(permit, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private async ValueTask ActivateCoreAsync(
        MigrationValidationActivationPermit permit,
        CancellationToken cancellationToken)
    {
        await ValidateStoredBatchStateAsync(cancellationToken).ConfigureAwait(false);
        TargetState initialState = await ReadStateAsync(_database, cancellationToken).ConfigureAwait(false);
        ValidateState(initialState, _plan, _snapshotIdentity);
        if (initialState.LifecycleState is not (
                CSharpDbMigrationSql.AwaitingValidationState or
                CSharpDbMigrationSql.ActivatedState))
        {
            throw new InvalidDataException(
                $"Migration target cannot activate from lifecycle '{initialState.LifecycleState}'.");
        }
        MigrationValidationActivationReceipt receipt = permit.Receipt;
        ValidateActivationReceiptBinding(
            receipt,
            allowLegacySnapshotIdentity: AllowsLegacyValidationSnapshotIdentity);
        await ReadAndValidateActivationReportAsync(
            permit.PublishedReportPath,
            receipt,
            cancellationToken).ConfigureAwait(false);

        MigrationValidationActivationReceipt? existing = await ReadActivationReceiptCoreAsync(
            cancellationToken).ConfigureAwait(false);
        if (existing is not null)
        {
            ValidateActivationReceiptBinding(
                existing,
                allowLegacySnapshotIdentity: AllowsLegacyValidationSnapshotIdentity);
            RequireSameActivationReceipt(existing, receipt);
            TargetState activatedState = await ReadStateAsync(_database, cancellationToken).ConfigureAwait(false);
            ValidateState(activatedState, _plan, _snapshotIdentity);
            if (!string.Equals(
                    activatedState.LifecycleState,
                    CSharpDbMigrationSql.ActivatedState,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    "Migration validation receipt exists without the activated lifecycle state.");
            }
            return;
        }

        TargetState state = await ReadStateAsync(_database, cancellationToken).ConfigureAwait(false);
        ValidateState(state, _plan, _snapshotIdentity);
        if (!string.Equals(
                state.LifecycleState,
                CSharpDbMigrationSql.AwaitingValidationState,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                $"Migration target cannot activate from lifecycle '{state.LifecycleState}'.");
        }

        bool transactionStarted = false;
        bool commitInvoked = false;
        try
        {
            await _database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            transactionStarted = true;

            InsertBatch receiptInsert = _database.PrepareInsertBatch(
                CSharpDbMigrationSql.ValidationReceiptTable,
                1);
            receiptInsert.AddRow(
                DbValue.FromInteger(1),
                DbValue.FromText(CSharpDbMigrationSql.ValidationReceiptTag),
                DbValue.FromText(receipt.TargetIdentity),
                DbValue.FromText(receipt.PlanDigest),
                DbValue.FromText(receipt.CatalogDigest),
                DbValue.FromText(receipt.SourceSnapshotIdentity),
                DbValue.FromText(receipt.TargetSnapshotIdentity),
                DbValue.FromInteger((long)receipt.Level),
                DbValue.FromText(receipt.CanonicalizationVersion),
                DbValue.FromText(receipt.CanonicalizationContractDigest),
                DbValue.FromText(receipt.ReportDigest));
            if (await receiptInsert.ExecuteAsync(cancellationToken).ConfigureAwait(false) != 1)
                throw new InvalidDataException("Migration validation receipt was not persisted.");

            await using (var update = await _database.ExecuteAsync(
                $"UPDATE {CSharpDbMigrationSql.Quote(CSharpDbMigrationSql.StateTable)} " +
                $"SET {CSharpDbMigrationSql.Quote("lifecycle_state")} = " +
                $"{CSharpDbMigrationSql.Literal(CSharpDbMigrationSql.ActivatedState)} " +
                $"WHERE {CSharpDbMigrationSql.Quote("singleton")} = 1 " +
                $"AND {CSharpDbMigrationSql.Quote("target_identity")} = " +
                $"{CSharpDbMigrationSql.Literal(TargetIdentity)} " +
                $"AND {CSharpDbMigrationSql.Quote("plan_digest")} = " +
                $"{CSharpDbMigrationSql.Literal(_planDigest)} " +
                $"AND {CSharpDbMigrationSql.Quote("lifecycle_state")} = " +
                $"{CSharpDbMigrationSql.Literal(CSharpDbMigrationSql.AwaitingValidationState)}",
                cancellationToken).ConfigureAwait(false))
            {
                if (update.RowsAffected != 1)
                {
                    throw new InvalidDataException(
                        "Migration target lifecycle changed before validation activation committed.");
                }
            }

            cancellationToken.ThrowIfCancellationRequested();
            commitInvoked = true;
            await _database.CommitAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            if (commitInvoked)
                _requiresReopen = true;
            if (transactionStarted && !commitInvoked)
                await TryRollbackAsync(_database).ConfigureAwait(false);
            throw;
        }
    }

    private async ValueTask ReadAndValidateActivationReportAsync(
        string reportPath,
        MigrationValidationActivationReceipt receipt,
        CancellationToken cancellationToken)
    {
        string json;
        try
        {
            var info = new FileInfo(reportPath);
            FileAttributes unsupported =
                FileAttributes.Directory | FileAttributes.Device | FileAttributes.ReparsePoint;
            if (!info.Exists || (info.Attributes & unsupported) != 0)
                throw new InvalidDataException("Published validation report is not a regular file.");
            if (info.Length > MigrationValidationReportSerializer.MaximumArtifactBytes)
            {
                throw new InvalidDataException(
                    "Published validation report exceeds the maximum artifact byte length.");
            }
            json = await File.ReadAllTextAsync(reportPath, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw new InvalidDataException(
                "The published migration validation report could not be read.",
                ex);
        }

        MigrationValidationReport report = MigrationValidationReportSerializer.Deserialize(json);
        string reportDigest = MigrationValidationReportSerializer.ComputeDigest(report);
        MigrationValidationBinding binding = report.Binding;
        if (report.Outcome != MigrationValidationStatus.Passed ||
            report.SnapshotConsistency.Status != MigrationSnapshotConsistencyStatus.Established ||
            report.Level != receipt.Level ||
            !FixedTimeSha256Equals(reportDigest, receipt.ReportDigest) ||
            !string.Equals(
                binding.TargetCSharpDbVersion,
                _plan.TargetCSharpDbVersion,
                StringComparison.Ordinal) ||
            !string.Equals(binding.PlanDigest, receipt.PlanDigest, StringComparison.Ordinal) ||
            !string.Equals(binding.CatalogDigest, receipt.CatalogDigest, StringComparison.Ordinal) ||
            !string.Equals(binding.CapabilityDigest, _plan.CapabilityDigest, StringComparison.Ordinal) ||
            !string.Equals(binding.SourceIdentity, _plan.Source.Identity, StringComparison.Ordinal) ||
            !string.Equals(binding.SourceFingerprint, _plan.Source.Fingerprint, StringComparison.Ordinal) ||
            !string.Equals(binding.TargetIdentity, receipt.TargetIdentity, StringComparison.Ordinal) ||
            !string.Equals(
                binding.SourceSnapshotIdentity,
                receipt.SourceSnapshotIdentity,
                StringComparison.Ordinal) ||
            !string.Equals(
                binding.TargetSnapshotIdentity,
                receipt.TargetSnapshotIdentity,
                StringComparison.Ordinal) ||
            !string.Equals(
                binding.CanonicalizationVersion,
                receipt.CanonicalizationVersion,
                StringComparison.Ordinal) ||
            !string.Equals(
                binding.CanonicalizationContractDigest,
                receipt.CanonicalizationContractDigest,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The published validation report is not a passed report bound to this staged target and receipt.");
        }
    }

    private static bool FixedTimeSha256Equals(string left, string right)
    {
        if (!IsLowerSha256(left) || !IsLowerSha256(right))
            return false;
        return CryptographicOperations.FixedTimeEquals(
            Convert.FromHexString(left),
            Convert.FromHexString(right));
    }

    private async ValueTask<MigrationValidationActivationReceipt?> ReadActivationReceiptCoreAsync(
        CancellationToken cancellationToken)
    {
        string[] columns =
        [
            "singleton", "receipt_tag", "target_identity", "plan_digest", "catalog_digest",
            "source_snapshot_identity", "target_snapshot_identity", "validation_level",
            "canonicalization_version", "canonicalization_contract_digest", "report_digest",
        ];
        string sql = $"SELECT {string.Join(", ", columns.Select(CSharpDbMigrationSql.Quote))} " +
            $"FROM {CSharpDbMigrationSql.Quote(CSharpDbMigrationSql.ValidationReceiptTable)}";
        await using var result = await _database.ExecuteAsync(sql, cancellationToken).ConfigureAwait(false);
        if (!await result.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            return null;

        DbValue[] row = result.Current;
        long validationLevel = row.Length == columns.Length ? row[7].AsInteger : -1;
        if (row.Length != columns.Length || row[0].AsInteger != 1 ||
            !string.Equals(
                row[1].AsText,
                CSharpDbMigrationSql.ValidationReceiptTag,
                StringComparison.Ordinal) ||
            validationLevel < int.MinValue || validationLevel > int.MaxValue ||
            !Enum.IsDefined((MigrationValidationLevel)(int)validationLevel))
        {
            throw new InvalidDataException("Migration validation receipt shape or format is invalid.");
        }

        var receipt = new MigrationValidationActivationReceipt
        {
            TargetIdentity = row[2].AsText,
            PlanDigest = row[3].AsText,
            CatalogDigest = row[4].AsText,
            SourceSnapshotIdentity = row[5].AsText,
            TargetSnapshotIdentity = row[6].AsText,
            Level = (MigrationValidationLevel)(int)validationLevel,
            CanonicalizationVersion = row[8].AsText,
            CanonicalizationContractDigest = row[9].AsText,
            ReportDigest = row[10].AsText,
        };
        if (await result.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            throw new InvalidDataException("Migration target contains multiple validation receipts.");
        return receipt;
    }

    private void ValidateActivationReceiptBinding(
        MigrationValidationActivationReceipt receipt,
        bool allowLegacySnapshotIdentity = false)
    {
        MigrationValidationLevel requiredLevel = _plan.Validation.ValidateChecksums
            ? MigrationValidationLevel.Checksum
            : _plan.Validation.ValidateCounts
                ? MigrationValidationLevel.Count
                : MigrationValidationLevel.Schema;
        string expectedTargetSnapshot = ValidationSnapshotIdentity(
            TargetIdentity,
            CSharpDbMigrationSql.AwaitingValidationState,
            _validatedOutcomeDigest);
        string legacyTargetSnapshot = LegacyValidationSnapshotIdentity(
            TargetIdentity,
            CSharpDbMigrationSql.AwaitingValidationState);
        bool targetSnapshotMatches = string.Equals(
                receipt.TargetSnapshotIdentity,
                expectedTargetSnapshot,
                StringComparison.Ordinal) ||
            (allowLegacySnapshotIdentity && string.Equals(
                receipt.TargetSnapshotIdentity,
                legacyTargetSnapshot,
                StringComparison.Ordinal));

        if (!string.Equals(receipt.TargetIdentity, TargetIdentity, StringComparison.Ordinal) ||
            !string.Equals(receipt.PlanDigest, _planDigest, StringComparison.Ordinal) ||
            !string.Equals(receipt.CatalogDigest, _plan.CatalogDigest, StringComparison.Ordinal) ||
            !string.Equals(receipt.SourceSnapshotIdentity, _snapshotIdentity, StringComparison.Ordinal) ||
            !targetSnapshotMatches ||
            receipt.Level is < MigrationValidationLevel.Schema or > MigrationValidationLevel.Checksum ||
            receipt.Level < requiredLevel ||
            !string.Equals(
                receipt.CanonicalizationVersion,
                _plan.Validation.CanonicalizationVersion,
                StringComparison.Ordinal) ||
            !string.Equals(
                receipt.CanonicalizationVersion,
                CanonicalRowCodec.CanonicalizationId,
                StringComparison.Ordinal) ||
            !string.Equals(
                receipt.CanonicalizationContractDigest,
                CanonicalRowCodec.ContractHashHex,
                StringComparison.Ordinal) ||
            !IsLowerSha256(receipt.ReportDigest))
        {
            throw new InvalidDataException(
                "Migration validation receipt does not match the staged target binding or validation policy.");
        }
    }

    private static void RequireSameActivationReceipt(
        MigrationValidationActivationReceipt existing,
        MigrationValidationActivationReceipt supplied)
    {
        if (existing != supplied)
        {
            throw new InvalidDataException(
                "Migration target is already activated with a different validation receipt.");
        }
    }

    private static bool IsLowerSha256(string? value)
    {
        if (value is null || value.Length != 64 ||
            !string.Equals(value, value.ToLowerInvariant(), StringComparison.Ordinal))
            return false;
        try
        {
            return Convert.FromHexString(value).Length == 32;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    private static string ValidationSnapshotIdentity(
        string targetIdentity,
        string lifecycle,
        string outcomeDigest)
    {
        if (!IsLowerSha256(outcomeDigest))
            throw new InvalidDataException("Migration target outcome digest is unavailable or invalid.");
        return $"staged-target:{targetIdentity}:{lifecycle}:outcomes:{outcomeDigest}";
    }

    private static string LegacyValidationSnapshotIdentity(
        string targetIdentity,
        string lifecycle) =>
        $"staged-target:{targetIdentity}:{lifecycle}";

    private static void AppendOutcomeReceipt(
        IncrementalHash hash,
        MigrationBatchReceipt receipt)
    {
        AppendOutcomeString(hash, receipt.TargetIdentity);
        AppendOutcomeString(hash, receipt.PlanDigest);
        AppendOutcomeString(hash, receipt.CatalogDigest);
        AppendOutcomeString(hash, receipt.SourceFingerprint);
        AppendOutcomeString(hash, receipt.SourceSnapshotIdentity);
        AppendOutcomeString(hash, receipt.SourceObjectId);
        AppendOutcomeInt64(hash, receipt.BatchOrdinal);
        AppendOutcomeNullableString(hash, receipt.StartCursor);
        AppendOutcomeNullableString(hash, receipt.NextCursor);
        AppendOutcomeString(hash, receipt.BatchDigest);
        AppendOutcomeString(hash, receipt.RejectContractVersion);
        AppendOutcomeString(hash, receipt.RejectDigest);
        AppendOutcomeInt64(hash, receipt.RowCount);
        AppendOutcomeInt64(hash, receipt.RejectedRowCount);
    }

    private static void AppendOutcomeNullableString(IncrementalHash hash, string? value)
    {
        if (value is null)
        {
            AppendOutcomeInt32(hash, -1);
            return;
        }
        AppendOutcomeString(hash, value);
    }

    private static void AppendOutcomeString(IncrementalHash hash, string value)
    {
        byte[] bytes;
        try
        {
            bytes = StrictUtf8.GetBytes(value);
        }
        catch (EncoderFallbackException error)
        {
            throw new InvalidDataException(
                "Migration target outcome bindings must contain valid Unicode scalar data.",
                error);
        }
        AppendOutcomeInt32(hash, bytes.Length);
        hash.AppendData(bytes);
    }

    private static void AppendOutcomeInt32(IncrementalHash hash, int value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        hash.AppendData(bytes);
    }

    private static void AppendOutcomeInt64(IncrementalHash hash, long value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(bytes, value);
        hash.AppendData(bytes);
    }

    private bool IsMigrationMetadataTable(string tableName) =>
        tableName is
            CSharpDbMigrationSql.StateTable or
            CSharpDbMigrationSql.StageTable or
            CSharpDbMigrationSql.ReceiptTable or
            CSharpDbMigrationSql.ValidationReceiptTable ||
        (!IsLegacyTarget && string.Equals(
            tableName,
            CSharpDbMigrationSql.RejectTable,
            StringComparison.Ordinal));

    private string ResolvePhysicalTableName(MigrationPlanObject planned) =>
        _collectionBindings.TryGetValue(
            planned.SourceObjectId,
            out CSharpDbCollectionMigrationBinding? binding)
            ? binding.PhysicalTableName
            : planned.TargetName ??
              throw new InvalidDataException(
                  $"Included data object '{planned.SourceObjectId}' has no target name.");

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;
        try
        {
            await _database.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _lease.Dispose();
            TryDelete(_leasePath);
        }
    }

    private static async ValueTask InitializeAsync(
        Database database,
        MigrationPlan plan,
        string snapshotIdentity,
        string targetIdentity,
        CancellationToken cancellationToken)
    {
        bool transactionStarted = false;
        bool commitInvoked = false;
        try
        {
            await database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            transactionStarted = true;
            foreach (string action in CSharpDbMigrationSql.BuildInternalSchemaActions())
                await ExecuteNonQueryAsync(database, action, cancellationToken).ConfigureAwait(false);

            InsertBatch stateInsert = database.PrepareInsertBatch(CSharpDbMigrationSql.StateTable, 1);
            stateInsert.AddRow(
                DbValue.FromInteger(1),
                DbValue.FromText(CSharpDbMigrationSql.TargetTag),
                DbValue.FromText(targetIdentity),
                DbValue.FromText(MigrationArtifactSerializer.ComputePlanDigest(plan)),
                DbValue.FromText(plan.CatalogDigest),
                DbValue.FromText(plan.CapabilityDigest),
                DbValue.FromText(plan.TargetCSharpDbVersion),
                DbValue.FromText(plan.Source.Kind.ToString()),
                DbValue.FromText(plan.Source.Identity),
                DbValue.FromText(plan.Source.Fingerprint),
                DbValue.FromText(snapshotIdentity),
                DbValue.FromText(CSharpDbMigrationSql.CreatedState));
            if (await stateInsert.ExecuteAsync(cancellationToken).ConfigureAwait(false) != 1)
                throw new InvalidDataException("Migration target state was not initialized.");

            cancellationToken.ThrowIfCancellationRequested();
            commitInvoked = true;
            await database.CommitAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            if (transactionStarted && !commitInvoked)
                await TryRollbackAsync(database).ConfigureAwait(false);
            throw;
        }
    }

    private static async ValueTask<TargetState> ReadStateAsync(
        Database database,
        CancellationToken cancellationToken)
    {
        string[] columns =
        [
            "singleton", "target_tag", "target_identity", "plan_digest", "catalog_digest",
            "capability_digest", "target_version", "source_kind", "source_identity",
            "source_fingerprint", "source_snapshot_identity", "lifecycle_state",
        ];
        string sql = $"SELECT {string.Join(", ", columns.Select(CSharpDbMigrationSql.Quote))} " +
            $"FROM {CSharpDbMigrationSql.Quote(CSharpDbMigrationSql.StateTable)}";
        await using var result = await database.ExecuteAsync(sql, cancellationToken).ConfigureAwait(false);
        if (!await result.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            throw new InvalidDataException("Database is not a staged migration target.");
        DbValue[] row = result.Current;
        if (row.Length != columns.Length || row[0].AsInteger != 1)
            throw new InvalidDataException("Staged migration target state row is invalid.");
        var state = new TargetState(
            row[1].AsText,
            row[2].AsText,
            row[3].AsText,
            row[4].AsText,
            row[5].AsText,
            row[6].AsText,
            row[7].AsText,
            row[8].AsText,
            row[9].AsText,
            row[10].AsText,
            row[11].AsText);
        if (await result.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            throw new InvalidDataException("Staged migration target contains multiple state rows.");
        return state;
    }

    private static void ValidateState(
        TargetState state,
        MigrationPlan plan,
        string snapshotIdentity)
    {
        if (state.TargetTag is not
                (CSharpDbMigrationSql.LegacyTargetTag or
                 CSharpDbMigrationSql.OutcomeUnboundTargetTag or
                 CSharpDbMigrationSql.TargetTag) ||
            string.IsNullOrWhiteSpace(state.TargetIdentity) ||
            !string.Equals(state.PlanDigest, MigrationArtifactSerializer.ComputePlanDigest(plan), StringComparison.Ordinal) ||
            !string.Equals(state.CatalogDigest, plan.CatalogDigest, StringComparison.Ordinal) ||
            !string.Equals(state.CapabilityDigest, plan.CapabilityDigest, StringComparison.Ordinal) ||
            !string.Equals(state.TargetVersion, plan.TargetCSharpDbVersion, StringComparison.Ordinal) ||
            !string.Equals(state.SourceKind, plan.Source.Kind.ToString(), StringComparison.Ordinal) ||
            !string.Equals(state.SourceIdentity, plan.Source.Identity, StringComparison.Ordinal) ||
            !string.Equals(state.SourceFingerprint, plan.Source.Fingerprint, StringComparison.Ordinal) ||
            !string.Equals(state.SourceSnapshotIdentity, snapshotIdentity, StringComparison.Ordinal) ||
            !KnownLifecycleStates.Contains(state.LifecycleState))
        {
            throw new InvalidDataException(
                "Staged migration target binding does not match the plan, source snapshot, or lifecycle contract.");
        }
    }

    private void ValidateSuppliedArtifacts(MigrationPlan plan, MigrationCatalog catalog)
    {
        MigrationPlanReadinessValidator.ValidateForApply(plan, catalog);
        if (!string.Equals(MigrationArtifactSerializer.ComputePlanDigest(plan), _planDigest, StringComparison.Ordinal) ||
            !string.Equals(MigrationArtifactSerializer.ComputeCatalogDigest(catalog), _plan.CatalogDigest, StringComparison.Ordinal))
        {
            throw new InvalidDataException("Supplied migration artifacts do not match the staged target binding.");
        }
    }

    private void ValidateTargetBatch(MigrationTargetBatch batch)
    {
        if (!string.Equals(batch.PlanDigest, _planDigest, StringComparison.Ordinal) ||
            !string.Equals(batch.CatalogDigest, _plan.CatalogDigest, StringComparison.Ordinal) ||
            !string.Equals(batch.SourceFingerprint, _plan.Source.Fingerprint, StringComparison.Ordinal) ||
            !string.Equals(batch.SourceSnapshotIdentity, _snapshotIdentity, StringComparison.Ordinal))
        {
            throw new InvalidDataException("Migration target batch identity does not match the staged target binding.");
        }
        if (!string.Equals(batch.RejectContractVersion, ExpectedRejectContract, StringComparison.Ordinal) ||
            (IsLegacyTarget && !string.Equals(
                batch.RejectContractVersion,
                MigrationRejectContract.DeterministicFailFastV1,
                StringComparison.Ordinal)))
        {
            throw new InvalidDataException(
                "Migration target batch reject contract does not match the staged target plan.");
        }
        if (batch.Rows is null || batch.RejectedRows is null)
            throw new InvalidDataException("Migration target batch outcomes cannot be null.");
        long attemptedRows = checked((long)batch.Rows.Count + batch.RejectedRows.Count);
        if (batch.BatchOrdinal < 0 || attemptedRows <= 0 || attemptedRows > _plan.Load.BatchSize)
            throw new InvalidDataException("Migration target batch attempted-row count or ordinal is invalid.");

        long expectedFirstOrdinal = long.MaxValue;
        foreach (MigrationTargetRow? row in batch.Rows)
        {
            if (row is null)
                throw new InvalidDataException("Migration target batch rows cannot contain null values.");
            expectedFirstOrdinal = Math.Min(expectedFirstOrdinal, row.SourceRowOrdinal);
        }
        foreach (MigrationRejectedRow? rejectedRow in batch.RejectedRows)
        {
            if (rejectedRow is null)
                throw new InvalidDataException("Migration target batch rejects cannot contain null values.");
            expectedFirstOrdinal = Math.Min(expectedFirstOrdinal, rejectedRow.SourceRowOrdinal);
            ValidateRejectColumn(batch.SourceObjectId, rejectedRow.ColumnObjectId);
        }
        _ = GetRejectBatchStatistics(batch);
        MigrationBatchOutcomeValidator.Validate(
            batch,
            expectedFirstOrdinal,
            _plan.Load.BatchSize);

        if (!string.Equals(
                MigrationBatchDigest.Compute(batch, BatchDigestFormat),
                batch.BatchDigest,
                StringComparison.Ordinal))
            throw new InvalidDataException("Migration target batch digest does not match its converted payload.");
        if (!_catalogObjects.TryGetValue(batch.SourceObjectId, out MigrationCatalogObject? table) ||
            table.Kind is not (MigrationObjectKind.Table or MigrationObjectKind.Collection) ||
            !_planObjects[batch.SourceObjectId].Included)
        {
            throw new InvalidDataException($"Migration target batch references unplanned object '{batch.SourceObjectId}'.");
        }

        MigrationCatalogObject[] columns = _catalog.Objects
            .Where(item => item.Kind == MigrationObjectKind.Column &&
                string.Equals(item.ParentObjectId, table.ObjectId, StringComparison.Ordinal) &&
                _planObjects[item.ObjectId].Included)
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .ToArray();
        string[] columnIds = columns.Select(item => item.ObjectId).ToArray();
        if (batch.ColumnObjectIds is null ||
            !batch.ColumnObjectIds.SequenceEqual(columnIds, StringComparer.Ordinal))
        {
            throw new InvalidDataException("Migration target batch column order does not match the staged schema.");
        }

        foreach (MigrationTargetRow row in batch.Rows)
        {
            if (row is null || row.Values is null || row.Values.Count != columns.Length ||
                row.SourceRowOrdinal < 0)
            {
                throw new InvalidDataException("Migration target row shape or source ordinal is invalid.");
            }
            for (int index = 0; index < columns.Length; index++)
            {
                DbValue value = row.Values[index];
                MigrationTypeMapping mapping = _planObjects[columns[index].ObjectId].TypeMappings.Single();
                if (value.IsNull)
                {
                    if (!IsNullable(columns[index]))
                        throw new InvalidDataException($"Migration target row contains NULL for required column '{columns[index].ObjectId}'.");
                    continue;
                }
                if (mapping.TargetType != value.Type)
                    throw new InvalidDataException($"Migration target value tag does not match column '{columns[index].ObjectId}'.");
                if (value.Type == DbType.Real && !double.IsFinite(value.AsReal))
                    throw new InvalidDataException($"Migration target value for '{columns[index].ObjectId}' is not finite.");
                if (MigrationValueConverter.GetCanonicalByteCount(value) > _plan.Load.MaxValueBytes)
                    throw new InvalidDataException($"Migration target value for '{columns[index].ObjectId}' exceeds MaxValueBytes.");
            }
        }

        if (_collectionBindings.TryGetValue(
                batch.SourceObjectId,
                out CSharpDbCollectionMigrationBinding? collectionBinding))
        {
            if (batch.RejectedRows.Count != 0 ||
                !string.Equals(
                    batch.RejectContractVersion,
                    MigrationRejectContract.DeterministicFailFastV1,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    "Document collection migration accepts only fail-fast data batches.");
            }

            foreach (MigrationTargetRow row in batch.Rows)
            {
                DbValue keyValue = row.Values[collectionBinding.KeyValueIndex];
                DbValue documentValue =
                    row.Values[collectionBinding.DocumentValueIndex];
                string? expectedKey = collectionBinding.KeyMode switch
                {
                    MigrationDocumentCollectionKeyMode.SourceOrdinal =>
                        MigrationDocumentCollectionContract.FormatOrdinalKey(
                            row.SourceRowOrdinal),
                    MigrationDocumentCollectionKeyMode.StableSourceKey =>
                        row.StableKey,
                    _ => throw new InvalidDataException(
                        "Migration collection key mode is unsupported."),
                };
                if (keyValue.IsNull ||
                    keyValue.Type != DbType.Text ||
                    documentValue.IsNull ||
                    documentValue.Type != DbType.Text ||
                    expectedKey is null ||
                    !string.Equals(row.StableKey, expectedKey, StringComparison.Ordinal) ||
                    !string.Equals(keyValue.AsText, expectedKey, StringComparison.Ordinal))
                {
                    throw new InvalidDataException(
                        collectionBinding.KeyMode ==
                        MigrationDocumentCollectionKeyMode.SourceOrdinal
                            ? "Migration collection row does not match its ordinal key and canonical document contract."
                            : "Migration collection row does not match its bound stable key and canonical document contract.");
                }

                if (collectionBinding.KeyMode ==
                        MigrationDocumentCollectionKeyMode.StableSourceKey &&
                    !MigrationLiteDbDocumentCollectionContract.TryValidateTypedKey(
                        expectedKey,
                        out _))
                {
                    throw new InvalidDataException(
                        "Migration collection row does not contain a valid version 1 typed stable key.");
                }
            }
        }
    }

    private RejectBatchStatistics ValidateProjectedRejectRunLimits(MigrationTargetBatch batch)
    {
        RejectBatchStatistics statistics = GetRejectBatchStatistics(batch);
        if (_plan.Load.RejectMode != MigrationRejectMode.DeterministicRejects)
            return statistics;

        MigrationDeterministicRejectPolicy policy = _plan.Load.RejectPolicy ??
            throw new InvalidDataException("Deterministic reject policy is missing from the staged target plan.");
        if (checked(_validatedRejectedRows + statistics.RejectedRows) > policy.MaxRejectedRowsPerRun ||
            checked(_validatedRawValueBytes + statistics.RawValueBytes) > policy.MaxRawValueBytesPerRun ||
            checked(_validatedArtifactBytes + statistics.CanonicalArtifactBytes) > policy.MaxArtifactBytes)
        {
            throw new InvalidDataException(
                "Migration reject run limits or canonical artifact limit would be exceeded.");
        }
        return statistics;
    }

    private RejectBatchStatistics GetRejectBatchStatistics(MigrationTargetBatch batch)
    {
        if (_plan.Load.RejectMode != MigrationRejectMode.DeterministicRejects)
            return new RejectBatchStatistics(batch.RejectedRows.Count, 0, 0);

        MigrationDeterministicRejectPolicy policy = _plan.Load.RejectPolicy ??
            throw new InvalidDataException("Deterministic reject policy is missing from the staged target plan.");
        if (batch.RejectedRows.Count > policy.MaxRejectedRowsPerBatch)
            throw new InvalidDataException("Migration reject batch count exceeds the plan-bound limit.");

        long rawValueBytes = 0;
        long canonicalArtifactBytes = 0;
        foreach (MigrationRejectedRow rejectedRow in batch.RejectedRows)
        {
            if (!policy.AllowedRuleIds.Contains(rejectedRow.RuleId, StringComparer.Ordinal))
                throw new InvalidDataException("Migration reject rule is not allowed by the plan-bound registry.");
            int rowRawValueBytes = MigrationRejectLedgerCodec.GetRawValueByteCount(rejectedRow);
            if (rowRawValueBytes > policy.MaxRawValueBytes)
                throw new InvalidDataException("Migration reject raw value exceeds the plan-bound per-value limit.");
            rawValueBytes = checked(rawValueBytes + rowRawValueBytes);
            if (rawValueBytes > policy.MaxRawValueBytesPerBatch)
                throw new InvalidDataException("Migration reject raw values exceed the plan-bound batch limit.");
            canonicalArtifactBytes = checked(
                canonicalArtifactBytes +
                MigrationRejectLedgerCodec.GetCanonicalArtifactEntryByteCount(
                    batch.SourceObjectId,
                    batch.BatchOrdinal,
                    rejectedRow));
        }
        return new RejectBatchStatistics(
            batch.RejectedRows.Count,
            rawValueBytes,
            canonicalArtifactBytes);
    }

    private async ValueTask<ObjectBatchProgress> ValidateNewBatchSequenceAsync(
        MigrationTargetBatch batch,
        CancellationToken cancellationToken)
    {
        ObjectBatchProgress progress = _validatedObjectProgress.TryGetValue(
            batch.SourceObjectId,
            out ObjectBatchProgress existingProgress)
            ? existingProgress
            : default;
        if (batch.BatchOrdinal != progress.ReceiptCount)
        {
            throw new InvalidDataException(
                "Migration target batch ordinals must be contiguous from zero.");
        }
        if (progress.ReceiptCount > 0 && progress.NextCursor is null)
            throw new InvalidDataException("Migration target cannot append after an end-of-source batch.");
        if (!string.Equals(batch.StartCursor, progress.NextCursor, StringComparison.Ordinal))
            throw new InvalidDataException("Migration target batch start cursor breaks the receipt chain.");

        if (progress.ReceiptCount > 0)
        {
            MigrationBatchReceipt prior = await ReadReceiptAsync(
                    batch.PlanDigest,
                    batch.SourceObjectId,
                    progress.ReceiptCount - 1,
                    cancellationToken)
                .ConfigureAwait(false) ??
                throw new InvalidDataException("Migration target batch predecessor is missing.");
            if (!string.Equals(prior.NextCursor, batch.StartCursor, StringComparison.Ordinal))
                throw new InvalidDataException("Migration target batch predecessor cursor is invalid.");
        }

        MigrationBatchOutcomeValidator.Validate(
            batch,
            progress.AttemptedRows,
            _plan.Load.BatchSize);
        return progress;
    }

    private async ValueTask ValidateStoredRejectOrdinalsAsync(
        MigrationBatchReceipt receipt,
        long expectedFirstOrdinal,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<MigrationRejectedRow> rejectedRows = IsLegacyTarget
            ? []
            : await ReadRejectedRowsForReceiptAsync(receipt, cancellationToken).ConfigureAwait(false);
        long attemptedRows = checked(receipt.RowCount + receipt.RejectedRowCount);
        long expectedEndOrdinal = checked(expectedFirstOrdinal + attemptedRows);
        foreach (MigrationRejectedRow rejectedRow in rejectedRows)
        {
            if (rejectedRow.SourceRowOrdinal < expectedFirstOrdinal ||
                rejectedRow.SourceRowOrdinal >= expectedEndOrdinal)
            {
                throw new InvalidDataException(
                    "Stored migration reject ordinal is outside its receipt input interval.");
            }
        }
    }

    private void RequireCompleteDataBatchChains()
    {
        foreach (ObjectBatchProgress progress in _validatedObjectProgress.Values)
        {
            if (progress.ReceiptCount > 0 && progress.NextCursor is not null)
            {
                throw new InvalidDataException(
                    "Migration post-load schema cannot begin before every nonempty source reaches end of source.");
            }
        }
    }

    private async ValueTask RequireStageAsync(
        MigrationSchemaStage stage,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<string> actions = CSharpDbMigrationSql.BuildStageActions(_plan, _catalog, stage);
        StageReceipt? receipt = await ReadStageAsync(stage, cancellationToken).ConfigureAwait(false);
        if (receipt is null)
            throw new InvalidDataException($"Migration schema stage '{stage}' is not complete.");
        ValidateStageReceipt(
            receipt,
            stage,
            CSharpDbMigrationSql.ComputeStageDigest(_plan, stage, actions),
            actions.Count);
    }

    private async ValueTask RequireMutableLifecycleAsync(CancellationToken cancellationToken)
    {
        TargetState state = await ReadStateAsync(_database, cancellationToken).ConfigureAwait(false);
        ValidateState(state, _plan, _snapshotIdentity);
        if (string.Equals(
                state.LifecycleState,
                CSharpDbMigrationSql.ActivatedState,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "Activated migration targets refuse further schema or data mutation.");
        }
    }

    private async ValueTask<StageReceipt?> ReadStageAsync(
        MigrationSchemaStage stage,
        CancellationToken cancellationToken)
    {
        string[] columns =
        [
            "stage_tag", "target_identity", "plan_digest", "stage_ordinal",
            "stage_name", "stage_digest", "action_count",
        ];
        string sql = $"SELECT {string.Join(", ", columns.Select(CSharpDbMigrationSql.Quote))} " +
            $"FROM {CSharpDbMigrationSql.Quote(CSharpDbMigrationSql.StageTable)} " +
            $"WHERE {CSharpDbMigrationSql.Quote("plan_digest")} = {CSharpDbMigrationSql.Literal(_planDigest)} " +
            $"AND {CSharpDbMigrationSql.Quote("stage_ordinal")} = {((long)stage).ToString(CultureInfo.InvariantCulture)}";
        await using var result = await _database.ExecuteAsync(sql, cancellationToken).ConfigureAwait(false);
        if (!await result.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            return null;
        DbValue[] row = result.Current;
        if (row.Length != columns.Length)
            throw new InvalidDataException("Migration schema-stage receipt shape is invalid.");
        var receipt = new StageReceipt(
            row[0].AsText,
            row[1].AsText,
            row[2].AsText,
            row[3].AsInteger,
            row[4].AsText,
            row[5].AsText,
            row[6].AsInteger);
        if (await result.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            throw new InvalidDataException("Migration target contains duplicate schema-stage receipts.");
        return receipt;
    }

    private void ValidateStageReceipt(
        StageReceipt receipt,
        MigrationSchemaStage stage,
        string digest,
        int actionCount)
    {
        if (!string.Equals(receipt.StageTag, CSharpDbMigrationSql.StageTag, StringComparison.Ordinal) ||
            !string.Equals(receipt.TargetIdentity, TargetIdentity, StringComparison.Ordinal) ||
            !string.Equals(receipt.PlanDigest, _planDigest, StringComparison.Ordinal) ||
            receipt.StageOrdinal != (long)stage ||
            !string.Equals(receipt.StageName, stage.ToString(), StringComparison.Ordinal) ||
            !string.Equals(receipt.StageDigest, digest, StringComparison.Ordinal) ||
            receipt.ActionCount != actionCount)
        {
            throw new InvalidDataException($"Migration schema-stage receipt for '{stage}' does not match its bound actions.");
        }
    }

    private void ValidateStoredReceipt(MigrationBatchReceipt receipt)
    {
        long attemptedRows;
        try
        {
            attemptedRows = checked(receipt.RowCount + receipt.RejectedRowCount);
        }
        catch (OverflowException error)
        {
            throw new InvalidDataException("Stored migration receipt counts overflow.", error);
        }

        if (!string.Equals(receipt.TargetIdentity, TargetIdentity, StringComparison.Ordinal) ||
            !string.Equals(receipt.PlanDigest, _planDigest, StringComparison.Ordinal) ||
            !string.Equals(receipt.CatalogDigest, _plan.CatalogDigest, StringComparison.Ordinal) ||
            !string.Equals(receipt.SourceFingerprint, _plan.Source.Fingerprint, StringComparison.Ordinal) ||
            !string.Equals(receipt.SourceSnapshotIdentity, _snapshotIdentity, StringComparison.Ordinal) ||
            !string.Equals(
                receipt.RejectContractVersion,
                ExpectedRejectContract,
                StringComparison.Ordinal) ||
            (IsLegacyTarget && !string.Equals(
                receipt.RejectContractVersion,
                MigrationRejectContract.DeterministicFailFastV1,
                StringComparison.Ordinal)) ||
            !IsLowerSha256(receipt.RejectDigest) ||
            receipt.BatchOrdinal < 0 || receipt.RowCount < 0 || receipt.RejectedRowCount < 0 ||
            attemptedRows <= 0 || attemptedRows > _plan.Load.BatchSize ||
            receipt.RejectedRowCount > MigrationRejectContract.MaximumRejectedRowsPerBatch ||
            (string.Equals(
                    receipt.RejectContractVersion,
                    MigrationRejectContract.DeterministicFailFastV1,
                    StringComparison.Ordinal) &&
                receipt.RejectedRowCount != 0))
        {
            throw new InvalidDataException("Stored migration receipt does not match the staged target binding.");
        }
    }

    private static string ComputeEmptyRejectDigest(MigrationBatchReceipt receipt) =>
        MigrationRejectDigest.Compute(new MigrationTargetBatch
        {
            PlanDigest = receipt.PlanDigest,
            CatalogDigest = receipt.CatalogDigest,
            SourceFingerprint = receipt.SourceFingerprint,
            SourceSnapshotIdentity = receipt.SourceSnapshotIdentity,
            SourceObjectId = receipt.SourceObjectId,
            BatchOrdinal = receipt.BatchOrdinal,
            StartCursor = receipt.StartCursor,
            NextCursor = receipt.NextCursor,
            BatchDigest = receipt.BatchDigest,
            RejectContractVersion = receipt.RejectContractVersion,
        });

    private async ValueTask ValidateReceiptLedgerAsync(
        MigrationBatchReceipt receipt,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<MigrationRejectedRow> rejectedRows = IsLegacyTarget
            ? []
            : await ReadRejectedRowsForReceiptAsync(receipt, cancellationToken).ConfigureAwait(false);
        if (rejectedRows.Count != receipt.RejectedRowCount)
            throw new InvalidDataException("Stored migration receipt reject count does not match its ledger.");

        var ledgerBatch = new MigrationTargetBatch
        {
            PlanDigest = receipt.PlanDigest,
            CatalogDigest = receipt.CatalogDigest,
            SourceFingerprint = receipt.SourceFingerprint,
            SourceSnapshotIdentity = receipt.SourceSnapshotIdentity,
            SourceObjectId = receipt.SourceObjectId,
            BatchOrdinal = receipt.BatchOrdinal,
            StartCursor = receipt.StartCursor,
            NextCursor = receipt.NextCursor,
            BatchDigest = receipt.BatchDigest,
            RejectContractVersion = receipt.RejectContractVersion,
            RejectedRows = rejectedRows,
        };
        _ = GetRejectBatchStatistics(ledgerBatch);
        string expectedRejectDigest = MigrationRejectDigest.Compute(ledgerBatch);
        if (!FixedTimeSha256Equals(expectedRejectDigest, receipt.RejectDigest))
            throw new InvalidDataException("Stored migration receipt reject digest is invalid.");
    }

    private async ValueTask<IReadOnlyList<MigrationRejectedRow>> ReadRejectedRowsForReceiptAsync(
        MigrationBatchReceipt receipt,
        CancellationToken cancellationToken)
    {
        string sql = RejectSelect() +
            $" WHERE {CSharpDbMigrationSql.Quote("plan_digest")} = {CSharpDbMigrationSql.Literal(receipt.PlanDigest)}" +
            $" AND {CSharpDbMigrationSql.Quote("source_object_id")} = {CSharpDbMigrationSql.Literal(receipt.SourceObjectId)}" +
            $" AND {CSharpDbMigrationSql.Quote("batch_ordinal")} = " +
            receipt.BatchOrdinal.ToString(CultureInfo.InvariantCulture) +
            $" ORDER BY {CSharpDbMigrationSql.Quote("source_row_ordinal")}";
        await using var result = await _database.ExecuteAsync(sql, cancellationToken).ConfigureAwait(false);
        var rejectedRows = new List<MigrationRejectedRow>();
        await foreach (DbValue[] row in result.GetRowsAsync(cancellationToken).ConfigureAwait(false))
        {
            MigrationRejectLedgerEntry entry = MapRejectLedgerEntry(row);
            if (!string.Equals(entry.PlanDigest, receipt.PlanDigest, StringComparison.Ordinal) ||
                !string.Equals(entry.SourceObjectId, receipt.SourceObjectId, StringComparison.Ordinal) ||
                entry.BatchOrdinal != receipt.BatchOrdinal)
            {
                throw new InvalidDataException("Stored migration reject ledger key is invalid.");
            }
            rejectedRows.Add(entry.RejectedRow);
            if (rejectedRows.Count > MigrationRejectContract.MaximumRejectedRowsPerBatch)
                throw new InvalidDataException("Stored migration reject count exceeds the contract ceiling.");
        }
        return rejectedRows;
    }

    private MigrationRejectLedgerEntry MapRejectLedgerEntry(DbValue[] row)
    {
        if (row.Length != RejectColumns.Length ||
            !string.Equals(row[0].AsText, CSharpDbMigrationSql.RejectTag, StringComparison.Ordinal) ||
            !string.Equals(row[1].AsText, _planDigest, StringComparison.Ordinal))
        {
            throw new InvalidDataException("Stored migration reject ledger entry shape or binding is invalid.");
        }

        string sourceObjectId = row[2].AsText;
        long batchOrdinal = row[3].AsInteger;
        var rejectedRow = new MigrationRejectedRow
        {
            SourceRowOrdinal = row[4].AsInteger,
            RuleId = row[5].AsText,
            ColumnObjectId = row[6].IsNull ? null : row[6].AsText,
            Evidence = MigrationRejectLedgerCodec.DeserializeEvidence(row[7].AsText),
        };
        if (batchOrdinal < 0 ||
            !_catalogObjects.TryGetValue(sourceObjectId, out MigrationCatalogObject? sourceObject) ||
            sourceObject.Kind is not (MigrationObjectKind.Table or MigrationObjectKind.Collection) ||
            !_planObjects.TryGetValue(sourceObjectId, out MigrationPlanObject? sourcePlan) ||
            !sourcePlan.Included)
        {
            throw new InvalidDataException("Stored migration reject ledger source binding is invalid.");
        }
        ValidateRejectColumn(sourceObjectId, rejectedRow.ColumnObjectId);
        return new MigrationRejectLedgerEntry
        {
            PlanDigest = _planDigest,
            SourceObjectId = sourceObjectId,
            BatchOrdinal = batchOrdinal,
            RejectedRow = rejectedRow,
            RawValueByteCount = MigrationRejectLedgerCodec.GetRawValueByteCount(rejectedRow),
            CanonicalEntryByteCount = MigrationRejectLedgerCodec.GetCanonicalEntryByteCount(
                sourceObjectId,
                batchOrdinal,
                rejectedRow),
        };
    }

    private void ValidateRejectColumn(string sourceObjectId, string? columnObjectId)
    {
        if (columnObjectId is null)
            return;
        if (!_catalogObjects.TryGetValue(columnObjectId, out MigrationCatalogObject? column) ||
            column.Kind != MigrationObjectKind.Column ||
            !string.Equals(column.ParentObjectId, sourceObjectId, StringComparison.Ordinal) ||
            !_planObjects.TryGetValue(columnObjectId, out MigrationPlanObject? columnPlan) ||
            !columnPlan.Included)
        {
            throw new InvalidDataException("Migration reject column is not part of the planned batch projection.");
        }
    }

    private static void ValidateReceiptAgainstBatch(
        MigrationBatchReceipt receipt,
        MigrationTargetBatch batch)
    {
        if (!string.Equals(receipt.PlanDigest, batch.PlanDigest, StringComparison.Ordinal) ||
            !string.Equals(receipt.CatalogDigest, batch.CatalogDigest, StringComparison.Ordinal) ||
            !string.Equals(receipt.SourceFingerprint, batch.SourceFingerprint, StringComparison.Ordinal) ||
            !string.Equals(receipt.SourceSnapshotIdentity, batch.SourceSnapshotIdentity, StringComparison.Ordinal) ||
            !string.Equals(receipt.SourceObjectId, batch.SourceObjectId, StringComparison.Ordinal) ||
            receipt.BatchOrdinal != batch.BatchOrdinal ||
            !string.Equals(receipt.StartCursor, batch.StartCursor, StringComparison.Ordinal) ||
            !string.Equals(receipt.NextCursor, batch.NextCursor, StringComparison.Ordinal) ||
            !string.Equals(receipt.BatchDigest, batch.BatchDigest, StringComparison.Ordinal) ||
            !string.Equals(
                receipt.RejectContractVersion,
                batch.RejectContractVersion,
                StringComparison.Ordinal) ||
            !FixedTimeSha256Equals(batch.RejectDigest, receipt.RejectDigest) ||
            receipt.RowCount != batch.Rows.Count ||
            receipt.RejectedRowCount != batch.RejectedRows.Count)
        {
            throw new InvalidDataException(
                $"Stored migration receipt for '{batch.SourceObjectId}' batch {batch.BatchOrdinal} does not match the replayed batch.");
        }
    }

    private DbValue[] ReceiptValues(MigrationBatchReceipt receipt)
    {
        var values = new List<DbValue>(15)
        {
            DbValue.FromText(IsLegacyTarget
                ? CSharpDbMigrationSql.LegacyReceiptTag
                : CSharpDbMigrationSql.ReceiptTag),
            DbValue.FromText(receipt.TargetIdentity),
            DbValue.FromText(receipt.PlanDigest),
            DbValue.FromText(receipt.CatalogDigest),
            DbValue.FromText(receipt.SourceFingerprint),
            DbValue.FromText(receipt.SourceSnapshotIdentity),
            DbValue.FromText(receipt.SourceObjectId),
            DbValue.FromInteger(receipt.BatchOrdinal),
            receipt.StartCursor is null ? DbValue.Null : DbValue.FromText(receipt.StartCursor),
            receipt.NextCursor is null ? DbValue.Null : DbValue.FromText(receipt.NextCursor),
            DbValue.FromText(receipt.BatchDigest),
        };
        if (!IsLegacyTarget)
        {
            values.Add(DbValue.FromText(receipt.RejectContractVersion));
            values.Add(DbValue.FromText(receipt.RejectDigest));
        }
        values.Add(DbValue.FromInteger(receipt.RowCount));
        values.Add(DbValue.FromInteger(receipt.RejectedRowCount));
        return values.ToArray();
    }

    private string ReceiptSelect()
    {
        string[] columns = ReceiptColumns(IsLegacyTarget);
        return $"SELECT {string.Join(", ", columns.Select(CSharpDbMigrationSql.Quote))} " +
            $"FROM {CSharpDbMigrationSql.Quote(CSharpDbMigrationSql.ReceiptTable)}";
    }

    private static string RejectSelect() =>
        $"SELECT {string.Join(", ", RejectColumns.Select(CSharpDbMigrationSql.Quote))} " +
        $"FROM {CSharpDbMigrationSql.Quote(CSharpDbMigrationSql.RejectTable)}";

    private static readonly string[] RejectColumns =
    [
        "reject_tag", "plan_digest", "source_object_id", "batch_ordinal",
        "source_row_ordinal", "rule_id", "column_object_id", "evidence_json",
    ];

    private static string[] ReceiptColumns(bool legacy) => legacy
        ?
        [
            "receipt_tag", "target_identity", "plan_digest", "catalog_digest",
            "source_fingerprint", "source_snapshot_identity", "source_object_id",
            "batch_ordinal", "start_cursor", "next_cursor", "batch_digest",
            "row_count", "rejected_row_count",
        ]
        :
        [
            "receipt_tag", "target_identity", "plan_digest", "catalog_digest",
            "source_fingerprint", "source_snapshot_identity", "source_object_id",
            "batch_ordinal", "start_cursor", "next_cursor", "batch_digest",
            "reject_contract_version", "reject_digest", "row_count", "rejected_row_count",
        ];

    private static void ValidateInternalReceiptSchema(Database database, string targetTag)
    {
        bool legacy = string.Equals(
            targetTag,
            CSharpDbMigrationSql.LegacyTargetTag,
            StringComparison.Ordinal);
        TableSchema? receipt = database.GetTableSchema(CSharpDbMigrationSql.ReceiptTable);
        if (receipt is null ||
            !receipt.Columns.Select(column => column.Name).SequenceEqual(
                ReceiptColumns(legacy),
                StringComparer.Ordinal))
        {
            throw new InvalidDataException(
                "Migration receipt table does not match the target contract version.");
        }

        if (legacy)
            return;

        TableSchema? rejects = database.GetTableSchema(CSharpDbMigrationSql.RejectTable);
        if (rejects is null ||
            !rejects.Columns.Select(column => column.Name).SequenceEqual(
                RejectColumns,
                StringComparer.Ordinal))
        {
            throw new InvalidDataException(
                "Migration reject table does not match the target contract version.");
        }
    }

    private MigrationBatchReceipt MapReceipt(DbValue[] row)
    {
        int expectedLength = IsLegacyTarget ? 13 : 15;
        string expectedTag = IsLegacyTarget
            ? CSharpDbMigrationSql.LegacyReceiptTag
            : CSharpDbMigrationSql.ReceiptTag;
        if (row.Length != expectedLength ||
            !string.Equals(row[0].AsText, expectedTag, StringComparison.Ordinal))
        {
            throw new InvalidDataException("Migration batch receipt shape or format tag is invalid.");
        }

        var receipt = new MigrationBatchReceipt
        {
            TargetIdentity = row[1].AsText,
            PlanDigest = row[2].AsText,
            CatalogDigest = row[3].AsText,
            SourceFingerprint = row[4].AsText,
            SourceSnapshotIdentity = row[5].AsText,
            SourceObjectId = row[6].AsText,
            BatchOrdinal = row[7].AsInteger,
            StartCursor = row[8].IsNull ? null : row[8].AsText,
            NextCursor = row[9].IsNull ? null : row[9].AsText,
            BatchDigest = row[10].AsText,
            RejectContractVersion = IsLegacyTarget
                ? MigrationRejectContract.DeterministicFailFastV1
                : row[11].AsText,
            RejectDigest = IsLegacyTarget ? string.Empty : row[12].AsText,
            RowCount = row[IsLegacyTarget ? 11 : 13].AsInteger,
            RejectedRowCount = row[IsLegacyTarget ? 12 : 14].AsInteger,
        };
        return IsLegacyTarget
            ? receipt with { RejectDigest = ComputeEmptyRejectDigest(receipt) }
            : receipt;
    }

    private static async ValueTask ExecuteNonQueryAsync(
        Database database,
        string sql,
        CancellationToken cancellationToken)
    {
        await using var result = await database.ExecuteAsync(sql, cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask TryRollbackAsync(Database database)
    {
        try
        {
            await database.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
        }
    }

    private static string ValidateFactoryInputs(
        string targetPath,
        MigrationPlan plan,
        MigrationCatalog catalog,
        string sourceSnapshotIdentity)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(targetPath);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceSnapshotIdentity);
        MigrationPlanReadinessValidator.ValidateForApply(plan, catalog);
        MigrationStagedTargetPolicyValidator.ValidateForBinding(plan);
        CSharpDbDdlPreviewBuilder.ValidateAttachedGeneratedDdlDigest(
            plan,
            catalog);
        string fullPath = Path.GetFullPath(targetPath);
        string? directory = Path.GetDirectoryName(fullPath);
        if (string.IsNullOrWhiteSpace(directory) || !Directory.Exists(directory))
            throw new DirectoryNotFoundException($"Staged migration target directory '{directory}' does not exist.");
        return fullPath;
    }

    private static DatabaseOptions CreateDatabaseOptions() =>
        new DatabaseOptions().ConfigureStorageEngine(builder => builder
            .UsePrimaryFileShare(FileShare.Read)
            .UseHybridFileCachePreset(MigrationPageCachePages)
            .UseWriteOptimizedPreset());

    private static FileStream AcquireLease(string targetPath)
    {
        try
        {
            return new FileStream(
                LeasePath(targetPath),
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.None,
                bufferSize: 1,
                FileOptions.None);
        }
        catch (IOException ex)
        {
            throw new IOException("Staged migration target is already leased by another process.", ex);
        }
    }

    private static MigrationSchemaStage? PreviousStage(MigrationSchemaStage stage) => stage switch
    {
        MigrationSchemaStage.LoadEssential => null,
        MigrationSchemaStage.SecondaryIndexes => MigrationSchemaStage.LoadEssential,
        MigrationSchemaStage.Constraints => MigrationSchemaStage.SecondaryIndexes,
        MigrationSchemaStage.Views => MigrationSchemaStage.Constraints,
        MigrationSchemaStage.Triggers => MigrationSchemaStage.Views,
        _ => throw new ArgumentOutOfRangeException(nameof(stage)),
    };

    private static string LifecycleAfter(MigrationSchemaStage stage) => stage switch
    {
        MigrationSchemaStage.LoadEssential => CSharpDbMigrationSql.LoadingDataState,
        MigrationSchemaStage.SecondaryIndexes => CSharpDbMigrationSql.SecondaryIndexesState,
        MigrationSchemaStage.Constraints => CSharpDbMigrationSql.ConstraintsState,
        MigrationSchemaStage.Views => CSharpDbMigrationSql.ViewsState,
        MigrationSchemaStage.Triggers => CSharpDbMigrationSql.AwaitingValidationState,
        _ => throw new ArgumentOutOfRangeException(nameof(stage)),
    };

    private static readonly IReadOnlySet<string> KnownLifecycleStates = new HashSet<string>(
    [
        CSharpDbMigrationSql.CreatedState,
        CSharpDbMigrationSql.LoadingDataState,
        CSharpDbMigrationSql.SecondaryIndexesState,
        CSharpDbMigrationSql.ConstraintsState,
        CSharpDbMigrationSql.ViewsState,
        CSharpDbMigrationSql.AwaitingValidationState,
        CSharpDbMigrationSql.ActivatedState,
    ],
        StringComparer.Ordinal);

    private static bool IsNullable(MigrationCatalogObject column) =>
        !bool.TryParse(
            column.Facets.FirstOrDefault(facet => facet.Name == "nullable")?.Value,
            out bool nullable) || nullable;

    private static string WalPath(string targetPath) => targetPath + ".wal";

    private static string LeasePath(string targetPath) => targetPath + ".migration.lock";

    private static void TryDelete(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    private void ThrowIfMutationUnavailable()
    {
        ThrowIfDisposed();
        if (_requiresReopen)
        {
            throw new InvalidOperationException(
                "The staged migration target must be disposed and reopened after an indeterminate commit.");
        }
    }

    private readonly record struct RejectBatchStatistics(
        long RejectedRows,
        long RawValueBytes,
        long CanonicalArtifactBytes);

    private readonly record struct ObjectBatchProgress(
        long ReceiptCount,
        long AttemptedRows,
        string? NextCursor)
    {
        internal ObjectBatchProgress Advance(MigrationTargetBatch batch) => new(
            checked(ReceiptCount + 1),
            checked(AttemptedRows + batch.Rows.Count + batch.RejectedRows.Count),
            batch.NextCursor);
    }

    private sealed record TargetState(
        string TargetTag,
        string TargetIdentity,
        string PlanDigest,
        string CatalogDigest,
        string CapabilityDigest,
        string TargetVersion,
        string SourceKind,
        string SourceIdentity,
        string SourceFingerprint,
        string SourceSnapshotIdentity,
        string LifecycleState);

    private sealed record StageReceipt(
        string StageTag,
        string TargetIdentity,
        string PlanDigest,
        long StageOrdinal,
        string StageName,
        string StageDigest,
        long ActionCount);

    private sealed class CSharpDbValidationSnapshot :
        IMigrationRejectTargetValidationSnapshot
    {
        private readonly Database.ReaderSession _session;
        private readonly MigrationNormalizedSchema _schema;
        private readonly IReadOnlyDictionary<string, ValidationDataObjectBinding> _dataObjects;
        private readonly string[] _dataObjectIds;
        private readonly IReadOnlyDictionary<string, long> _receiptCounts;
        private readonly string _targetIdentity;
        private readonly string _planDigest;
        private readonly string _catalogDigest;
        private readonly string _sourceFingerprint;
        private readonly string _sourceSnapshotIdentity;
        private readonly bool _legacy;
        private readonly int _batchSize;
        private readonly MigrationRejectMode _rejectMode;
        private readonly string _expectedRejectContract;
        private readonly HashSet<string> _allowedRuleIds;
        private readonly long _maxRejectedRowsPerBatch;
        private readonly long _maxRejectedRowsPerRun;
        private readonly long _maxRawValueBytes;
        private readonly long _maxRawValueBytesPerBatch;
        private readonly long _maxRawValueBytesPerRun;
        private readonly long _maxArtifactBytes;
        private readonly long _expectedReceiptCount;
        private readonly long _expectedRejectCount;
        private bool _disposed;

        internal CSharpDbValidationSnapshot(
            Database.ReaderSession session,
            string snapshotIdentity,
            string targetIdentity,
            string planDigest,
            string catalogDigest,
            string sourceFingerprint,
            string sourceSnapshotIdentity,
            bool legacy,
            MigrationLoadPolicy load,
            IReadOnlyDictionary<string, long> receiptCounts,
            long expectedRejectCount,
            IReadOnlyDictionary<string, MigrationPlanObject> planObjects,
            IReadOnlyDictionary<string, CSharpDbCollectionMigrationBinding>
                collectionBindings,
            MigrationCatalog catalog,
            MigrationNormalizedSchema schema)
        {
            _session = session;
            _targetIdentity = targetIdentity;
            _planDigest = planDigest;
            _catalogDigest = catalogDigest;
            _sourceFingerprint = sourceFingerprint;
            _sourceSnapshotIdentity = sourceSnapshotIdentity;
            _legacy = legacy;
            _batchSize = load.BatchSize;
            _rejectMode = load.RejectMode;
            _expectedRejectContract = load.RejectMode switch
            {
                MigrationRejectMode.FailFast => MigrationRejectContract.DeterministicFailFastV1,
                MigrationRejectMode.DeterministicRejects => MigrationRejectContract.DeterministicRejectsV1,
                _ => throw new InvalidDataException("Migration validation reject mode is unsupported."),
            };
            MigrationDeterministicRejectPolicy? rejectPolicy = load.RejectPolicy;
            _allowedRuleIds = rejectPolicy is null
                ? new HashSet<string>(StringComparer.Ordinal)
                : new HashSet<string>(rejectPolicy.AllowedRuleIds, StringComparer.Ordinal);
            _maxRejectedRowsPerBatch = rejectPolicy?.MaxRejectedRowsPerBatch ?? 0;
            _maxRejectedRowsPerRun = rejectPolicy?.MaxRejectedRowsPerRun ?? 0;
            _maxRawValueBytes = rejectPolicy?.MaxRawValueBytes ?? 0;
            _maxRawValueBytesPerBatch = rejectPolicy?.MaxRawValueBytesPerBatch ?? 0;
            _maxRawValueBytesPerRun = rejectPolicy?.MaxRawValueBytesPerRun ?? 0;
            _maxArtifactBytes = rejectPolicy?.MaxArtifactBytes ?? 0;
            _expectedRejectCount = expectedRejectCount;
            _schema = schema;

            _dataObjects = catalog.Objects
                .Where(item => item.Kind is MigrationObjectKind.Table or MigrationObjectKind.Collection)
                .Where(item =>
                    planObjects.TryGetValue(item.ObjectId, out MigrationPlanObject? planned) &&
                    planned.Included)
                .ToDictionary(
                    item => item.ObjectId,
                    item =>
                    {
                        MigrationPlanObject planned = planObjects[item.ObjectId];
                        string[] columnObjectIds = catalog.Objects
                            .Where(column => column.Kind == MigrationObjectKind.Column &&
                                string.Equals(
                                    column.ParentObjectId,
                                    item.ObjectId,
                                    StringComparison.Ordinal) &&
                                planObjects.TryGetValue(
                                    column.ObjectId,
                                    out MigrationPlanObject? columnPlan) &&
                                columnPlan.Included)
                            .OrderBy(column => column.ObjectId, StringComparer.Ordinal)
                            .Select(column => column.ObjectId)
                            .ToArray();
                        return new ValidationDataObjectBinding(
                            collectionBindings.TryGetValue(
                                item.ObjectId,
                                out CSharpDbCollectionMigrationBinding? collectionBinding)
                                ? collectionBinding.PhysicalTableName
                                : planned.TargetName ??
                                  throw new InvalidDataException(
                                      "Migration validation table binding is incomplete."),
                            columnObjectIds
                                .Select(columnObjectId =>
                                    planObjects[columnObjectId].TargetName ??
                                    throw new InvalidDataException(
                                        "Migration validation column binding is incomplete."))
                                .ToArray(),
                            new HashSet<string>(columnObjectIds, StringComparer.Ordinal));
                    },
                    StringComparer.Ordinal);
            _dataObjectIds = _dataObjects.Keys
                .OrderBy(item => item, StringComparer.Ordinal)
                .ToArray();
            _receiptCounts = _dataObjectIds.ToDictionary(
                objectId => objectId,
                objectId => receiptCounts.TryGetValue(objectId, out long count)
                    ? count
                    : throw new InvalidDataException(
                        "Migration validation receipt progress is incomplete."),
                StringComparer.Ordinal);
            if (receiptCounts.Count != _receiptCounts.Count ||
                expectedRejectCount < 0 ||
                _receiptCounts.Values.Any(count => count < 0))
            {
                throw new InvalidDataException(
                    "Migration validation outcome progress is invalid.");
            }
            _expectedReceiptCount = _receiptCounts.Values.Aggregate(
                0L,
                static (total, count) => checked(total + count));

            // Activation changes only target lifecycle metadata. This immutable
            // reader and identity remain bound to the pre-activation outcomes.
            SnapshotIdentity = snapshotIdentity;
        }

        public string SnapshotIdentity { get; }

        public MigrationSnapshotConsistencyStatus ConsistencyStatus =>
            MigrationSnapshotConsistencyStatus.Established;

        public ValueTask<MigrationNormalizedSchema> ReadSchemaAsync(
            CancellationToken cancellationToken = default)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(_schema);
        }

        public async ValueTask<long> CountAsync(
            string objectId,
            CancellationToken cancellationToken = default)
        {
            string tableName = ResolveTable(objectId).TargetName;
            return await CountRowsAsync(
                tableName,
                where: null,
                cancellationToken).ConfigureAwait(false);
        }

        public async IAsyncEnumerable<MigrationValidationRow> ReadRowsAsync(
            string objectId,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ValidationDataObjectBinding binding = ResolveTable(objectId);
            string sql =
                $"SELECT {string.Join(", ", binding.ProjectedColumnNames.Select(CSharpDbMigrationSql.Quote))} " +
                $"FROM {CSharpDbMigrationSql.Quote(binding.TargetName)}";
            await using var result = await _session.ExecuteReadAsync(
                sql,
                cancellationToken).ConfigureAwait(false);
            await foreach (DbValue[] row in result
                               .GetRowsAsync(cancellationToken)
                               .ConfigureAwait(false))
            {
                yield return new MigrationValidationRow
                {
                    Values = row.ToArray(),
                };
            }
        }

        public async IAsyncEnumerable<MigrationBatchReceipt> ReadOutcomeReceiptsAsync(
            string planDigest,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            RequirePlanDigest(planDigest);

            long emitted = 0;
            foreach (string sourceObjectId in _dataObjectIds)
            {
                long receiptCount = _receiptCounts[sourceObjectId];
                string? expectedStartCursor = null;
                for (long batchOrdinal = 0; batchOrdinal < receiptCount; batchOrdinal++)
                {
                    MigrationBatchReceipt receipt = await ReadReceiptAsync(
                        sourceObjectId,
                        batchOrdinal,
                        cancellationToken).ConfigureAwait(false);
                    if (receipt.BatchOrdinal != batchOrdinal ||
                        !string.Equals(
                            receipt.StartCursor,
                            expectedStartCursor,
                            StringComparison.Ordinal))
                    {
                        throw new InvalidDataException(
                            "Migration validation receipt sequence is invalid.");
                    }

                    expectedStartCursor = receipt.NextCursor;
                    emitted = checked(emitted + 1);
                    yield return receipt;
                }

                if (receiptCount > 0 && expectedStartCursor is not null)
                {
                    throw new InvalidDataException(
                        "Migration validation receipt chain is not terminal.");
                }
            }

            long stored = await CountRowsAsync(
                CSharpDbMigrationSql.ReceiptTable,
                where: null,
                cancellationToken).ConfigureAwait(false);
            if (emitted != _expectedReceiptCount || stored != _expectedReceiptCount)
            {
                throw new InvalidDataException(
                    "Migration validation receipt stream is incomplete.");
            }
        }

        public async IAsyncEnumerable<MigrationRejectLedgerEntry> ReadRejectLedgerAsync(
            string planDigest,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            RequirePlanDigest(planDigest);
            if (_legacy)
            {
                if (_expectedRejectCount != 0)
                {
                    throw new InvalidDataException(
                        "Legacy migration validation contains reject outcomes.");
                }
                yield break;
            }

            long emitted = 0;
            long totalRawValueBytes = 0;
            long artifactBytes = _rejectMode == MigrationRejectMode.DeterministicRejects
                ? MigrationRejectLedgerCodec.GetArtifactHeaderByteCount(_planDigest)
                : 0;
            foreach (string sourceObjectId in _dataObjectIds)
            {
                long attemptedRows = 0;
                long receiptCount = _receiptCounts[sourceObjectId];
                for (long batchOrdinal = 0; batchOrdinal < receiptCount; batchOrdinal++)
                {
                    MigrationBatchReceipt receipt = await ReadReceiptAsync(
                        sourceObjectId,
                        batchOrdinal,
                        cancellationToken).ConfigureAwait(false);
                    long attemptedInBatch = checked(
                        receipt.RowCount + receipt.RejectedRowCount);
                    long intervalEnd = checked(attemptedRows + attemptedInBatch);
                    long previousSourceRowOrdinal = -1;
                    long batchRawValueBytes = 0;
                    long batchArtifactBytes = 0;

                    for (long rejectIndex = 0;
                         rejectIndex < receipt.RejectedRowCount;
                         rejectIndex++)
                    {
                        MigrationRejectLedgerEntry entry =
                            await ReadNextRejectLedgerEntryAsync(
                                sourceObjectId,
                                batchOrdinal,
                                previousSourceRowOrdinal,
                                cancellationToken).ConfigureAwait(false) ??
                            throw new InvalidDataException(
                                "Migration validation reject ledger is incomplete.");
                        if (entry.RejectedRow.SourceRowOrdinal < attemptedRows ||
                            entry.RejectedRow.SourceRowOrdinal >= intervalEnd)
                        {
                            throw new InvalidDataException(
                                "Migration validation reject ordinal is outside its receipt interval.");
                        }

                        ValidateRejectEntry(
                            entry,
                            ref batchRawValueBytes,
                            ref batchArtifactBytes);
                        previousSourceRowOrdinal = entry.RejectedRow.SourceRowOrdinal;
                        emitted = checked(emitted + 1);
                        totalRawValueBytes = checked(
                            totalRawValueBytes + entry.RawValueByteCount);
                        artifactBytes = checked(
                            artifactBytes +
                            MigrationRejectLedgerCodec.GetCanonicalArtifactEntryByteCount(
                                entry.SourceObjectId,
                                entry.BatchOrdinal,
                                entry.RejectedRow));
                        yield return entry;
                    }

                    if (await ReadNextRejectLedgerEntryAsync(
                            sourceObjectId,
                            batchOrdinal,
                            previousSourceRowOrdinal,
                            cancellationToken).ConfigureAwait(false) is not null)
                    {
                        throw new InvalidDataException(
                            "Migration validation reject ledger contains an orphan entry.");
                    }
                    attemptedRows = intervalEnd;
                }
            }

            long stored = await CountRowsAsync(
                CSharpDbMigrationSql.RejectTable,
                where: null,
                cancellationToken).ConfigureAwait(false);
            if (emitted != _expectedRejectCount || stored != _expectedRejectCount)
            {
                throw new InvalidDataException(
                    "Migration validation reject-ledger stream is incomplete.");
            }
            if (_rejectMode == MigrationRejectMode.DeterministicRejects &&
                (emitted > _maxRejectedRowsPerRun ||
                 totalRawValueBytes > _maxRawValueBytesPerRun ||
                 artifactBytes > _maxArtifactBytes))
            {
                throw new InvalidDataException(
                    "Migration validation reject ledger exceeds plan-bound run limits.");
            }
        }

        public ValueTask DisposeAsync()
        {
            if (!_disposed)
            {
                _disposed = true;
                _session.Dispose();
            }
            return ValueTask.CompletedTask;
        }

        private async ValueTask<MigrationBatchReceipt> ReadReceiptAsync(
            string sourceObjectId,
            long batchOrdinal,
            CancellationToken cancellationToken)
        {
            string[] columns = ReceiptColumns(_legacy);
            string sql =
                $"SELECT {string.Join(", ", columns.Select(CSharpDbMigrationSql.Quote))} " +
                $"FROM {CSharpDbMigrationSql.Quote(CSharpDbMigrationSql.ReceiptTable)} " +
                $"WHERE {CSharpDbMigrationSql.Quote("plan_digest")} = " +
                $"{CSharpDbMigrationSql.Literal(_planDigest)} " +
                $"AND {CSharpDbMigrationSql.Quote("source_object_id")} = " +
                $"{CSharpDbMigrationSql.Literal(sourceObjectId)} " +
                $"AND {CSharpDbMigrationSql.Quote("batch_ordinal")} = " +
                batchOrdinal.ToString(CultureInfo.InvariantCulture);
            DbValue[] row;
            await using (var result = await _session.ExecuteReadAsync(
                             sql,
                             cancellationToken).ConfigureAwait(false))
            {
                if (!await result.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                {
                    throw new InvalidDataException(
                        "Migration validation receipt stream is missing an expected row.");
                }
                row = result.Current.ToArray();
                if (await result.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                {
                    throw new InvalidDataException(
                        "Migration validation receipt stream contains a duplicate key.");
                }
            }
            return MapReceipt(row);
        }

        private async ValueTask<MigrationRejectLedgerEntry?> ReadNextRejectLedgerEntryAsync(
            string sourceObjectId,
            long batchOrdinal,
            long previousSourceRowOrdinal,
            CancellationToken cancellationToken)
        {
            string sql =
                $"SELECT {string.Join(", ", RejectColumns.Select(CSharpDbMigrationSql.Quote))} " +
                $"FROM {CSharpDbMigrationSql.Quote(CSharpDbMigrationSql.RejectTable)} " +
                $"WHERE {CSharpDbMigrationSql.Quote("plan_digest")} = " +
                $"{CSharpDbMigrationSql.Literal(_planDigest)} " +
                $"AND {CSharpDbMigrationSql.Quote("source_object_id")} = " +
                $"{CSharpDbMigrationSql.Literal(sourceObjectId)} " +
                $"AND {CSharpDbMigrationSql.Quote("batch_ordinal")} = " +
                $"{batchOrdinal.ToString(CultureInfo.InvariantCulture)} " +
                $"AND {CSharpDbMigrationSql.Quote("source_row_ordinal")} > " +
                $"{previousSourceRowOrdinal.ToString(CultureInfo.InvariantCulture)} " +
                $"ORDER BY {CSharpDbMigrationSql.Quote("source_row_ordinal")} LIMIT 1";
            DbValue[]? row = null;
            await using (var result = await _session.ExecuteReadAsync(
                             sql,
                             cancellationToken).ConfigureAwait(false))
            {
                if (await result.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                    row = result.Current.ToArray();
                if (await result.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                {
                    throw new InvalidDataException(
                        "Migration validation reject lookup exceeded its bounded result.");
                }
            }
            return row is null ? null : MapRejectLedgerEntry(row);
        }

        private MigrationBatchReceipt MapReceipt(DbValue[] row)
        {
            int expectedLength = _legacy ? 13 : 15;
            if (row.Length != expectedLength)
            {
                throw new InvalidDataException(
                    "Migration validation receipt row shape is invalid.");
            }

            var receipt = new MigrationBatchReceipt
            {
                TargetIdentity = Text(row, 1, "Migration validation receipt row type is invalid."),
                PlanDigest = Text(row, 2, "Migration validation receipt row type is invalid."),
                CatalogDigest = Text(row, 3, "Migration validation receipt row type is invalid."),
                SourceFingerprint = Text(row, 4, "Migration validation receipt row type is invalid."),
                SourceSnapshotIdentity = Text(row, 5, "Migration validation receipt row type is invalid."),
                SourceObjectId = Text(row, 6, "Migration validation receipt row type is invalid."),
                BatchOrdinal = Integer(row, 7, "Migration validation receipt row type is invalid."),
                StartCursor = NullableText(row, 8, "Migration validation receipt row type is invalid."),
                NextCursor = NullableText(row, 9, "Migration validation receipt row type is invalid."),
                BatchDigest = Text(row, 10, "Migration validation receipt row type is invalid."),
                RejectContractVersion = _legacy
                    ? MigrationRejectContract.DeterministicFailFastV1
                    : Text(row, 11, "Migration validation receipt row type is invalid."),
                RejectDigest = _legacy
                    ? string.Empty
                    : Text(row, 12, "Migration validation receipt row type is invalid."),
                RowCount = Integer(
                    row,
                    _legacy ? 11 : 13,
                    "Migration validation receipt row type is invalid."),
                RejectedRowCount = Integer(
                    row,
                    _legacy ? 12 : 14,
                    "Migration validation receipt row type is invalid."),
            };
            string expectedTag = _legacy
                ? CSharpDbMigrationSql.LegacyReceiptTag
                : CSharpDbMigrationSql.ReceiptTag;
            long attemptedRows;
            try
            {
                attemptedRows = checked(receipt.RowCount + receipt.RejectedRowCount);
            }
            catch (OverflowException error)
            {
                throw new InvalidDataException(
                    "Migration validation receipt counts overflow.",
                    error);
            }
            if (!string.Equals(Text(
                    row,
                    0,
                    "Migration validation receipt row type is invalid."),
                    expectedTag,
                    StringComparison.Ordinal) ||
                !string.Equals(receipt.TargetIdentity, _targetIdentity, StringComparison.Ordinal) ||
                !string.Equals(receipt.PlanDigest, _planDigest, StringComparison.Ordinal) ||
                !string.Equals(receipt.CatalogDigest, _catalogDigest, StringComparison.Ordinal) ||
                !string.Equals(
                    receipt.SourceFingerprint,
                    _sourceFingerprint,
                    StringComparison.Ordinal) ||
                !string.Equals(
                    receipt.SourceSnapshotIdentity,
                    _sourceSnapshotIdentity,
                    StringComparison.Ordinal) ||
                !_dataObjects.ContainsKey(receipt.SourceObjectId) ||
                receipt.BatchOrdinal < 0 ||
                receipt.RowCount < 0 ||
                receipt.RejectedRowCount < 0 ||
                attemptedRows <= 0 ||
                attemptedRows > _batchSize ||
                receipt.RejectedRowCount > MigrationRejectContract.MaximumRejectedRowsPerBatch ||
                (_rejectMode == MigrationRejectMode.DeterministicRejects &&
                 receipt.RejectedRowCount > _maxRejectedRowsPerBatch) ||
                (_rejectMode == MigrationRejectMode.FailFast && receipt.RejectedRowCount != 0) ||
                !string.Equals(
                    receipt.RejectContractVersion,
                    _expectedRejectContract,
                    StringComparison.Ordinal) ||
                !IsLowerSha256(receipt.BatchDigest) ||
                (!_legacy && !IsLowerSha256(receipt.RejectDigest)))
            {
                throw new InvalidDataException(
                    "Migration validation receipt binding or format is invalid.");
            }

            if (_legacy)
                receipt = receipt with { RejectDigest = ComputeEmptyRejectDigest(receipt) };
            else if (receipt.RejectedRowCount == 0 &&
                !FixedTimeSha256Equals(ComputeEmptyRejectDigest(receipt), receipt.RejectDigest))
            {
                throw new InvalidDataException(
                    "Migration validation receipt empty reject digest is invalid.");
            }
            return receipt;
        }

        private MigrationRejectLedgerEntry MapRejectLedgerEntry(DbValue[] row)
        {
            if (row.Length != RejectColumns.Length)
            {
                throw new InvalidDataException(
                    "Migration validation reject-ledger row shape is invalid.");
            }

            string sourceObjectId = Text(
                row,
                2,
                "Migration validation reject-ledger row type is invalid.");
            long batchOrdinal = Integer(
                row,
                3,
                "Migration validation reject-ledger row type is invalid.");
            var rejectedRow = new MigrationRejectedRow
            {
                SourceRowOrdinal = Integer(
                    row,
                    4,
                    "Migration validation reject-ledger row type is invalid."),
                RuleId = Text(
                    row,
                    5,
                    "Migration validation reject-ledger row type is invalid."),
                ColumnObjectId = NullableText(
                    row,
                    6,
                    "Migration validation reject-ledger row type is invalid."),
                Evidence = MigrationRejectLedgerCodec.DeserializeEvidence(
                    Text(
                        row,
                        7,
                        "Migration validation reject-ledger row type is invalid.")),
            };
            if (!string.Equals(
                    Text(
                        row,
                        0,
                        "Migration validation reject-ledger row type is invalid."),
                    CSharpDbMigrationSql.RejectTag,
                    StringComparison.Ordinal) ||
                !string.Equals(
                    Text(
                        row,
                        1,
                        "Migration validation reject-ledger row type is invalid."),
                    _planDigest,
                    StringComparison.Ordinal) ||
                !_dataObjects.TryGetValue(
                    sourceObjectId,
                    out ValidationDataObjectBinding? sourceBinding) ||
                batchOrdinal < 0 ||
                rejectedRow.SourceRowOrdinal < 0 ||
                (rejectedRow.ColumnObjectId is not null &&
                 !sourceBinding.ProjectedColumnObjectIds.Contains(
                     rejectedRow.ColumnObjectId)))
            {
                throw new InvalidDataException(
                    "Migration validation reject-ledger binding or format is invalid.");
            }

            int rawValueByteCount =
                MigrationRejectLedgerCodec.GetRawValueByteCount(rejectedRow);
            int canonicalEntryByteCount =
                MigrationRejectLedgerCodec.GetCanonicalEntryByteCount(
                    sourceObjectId,
                    batchOrdinal,
                    rejectedRow);
            return new MigrationRejectLedgerEntry
            {
                PlanDigest = _planDigest,
                SourceObjectId = sourceObjectId,
                BatchOrdinal = batchOrdinal,
                RejectedRow = rejectedRow,
                RawValueByteCount = rawValueByteCount,
                CanonicalEntryByteCount = canonicalEntryByteCount,
            };
        }

        private void ValidateRejectEntry(
            MigrationRejectLedgerEntry entry,
            ref long batchRawValueBytes,
            ref long batchArtifactBytes)
        {
            if (_rejectMode != MigrationRejectMode.DeterministicRejects ||
                !_allowedRuleIds.Contains(entry.RejectedRow.RuleId) ||
                entry.RawValueByteCount > _maxRawValueBytes)
            {
                throw new InvalidDataException(
                    "Migration validation reject-ledger policy binding is invalid.");
            }
            batchRawValueBytes = checked(
                batchRawValueBytes + entry.RawValueByteCount);
            batchArtifactBytes = checked(
                batchArtifactBytes +
                MigrationRejectLedgerCodec.GetCanonicalArtifactEntryByteCount(
                    entry.SourceObjectId,
                    entry.BatchOrdinal,
                    entry.RejectedRow));
            if (batchRawValueBytes > _maxRawValueBytesPerBatch ||
                batchArtifactBytes > _maxArtifactBytes)
            {
                throw new InvalidDataException(
                    "Migration validation reject ledger exceeds plan-bound batch limits.");
            }
        }

        private async ValueTask<long> CountRowsAsync(
            string tableName,
            string? where,
            CancellationToken cancellationToken)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            string sql = $"SELECT COUNT(*) FROM {CSharpDbMigrationSql.Quote(tableName)}";
            if (where is not null)
                sql += $" WHERE {where}";
            await using var result = await _session.ExecuteReadAsync(
                sql,
                cancellationToken).ConfigureAwait(false);
            if (!await result.MoveNextAsync(cancellationToken).ConfigureAwait(false) ||
                result.Current.Length != 1 ||
                result.Current[0].Type != DbType.Integer)
            {
                throw new InvalidDataException(
                    "Migration validation internal row count shape is invalid.");
            }
            long count = result.Current[0].AsInteger;
            if (count < 0 ||
                await result.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            {
                throw new InvalidDataException(
                    "Migration validation internal row count is invalid.");
            }
            return count;
        }

        private void RequirePlanDigest(string planDigest)
        {
            if (!string.Equals(planDigest, _planDigest, StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    "Migration validation outcome lookup does not match the snapshot plan.");
            }
        }

        private ValidationDataObjectBinding ResolveTable(string objectId)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (!_dataObjects.TryGetValue(
                    objectId,
                    out ValidationDataObjectBinding? binding))
            {
                throw new InvalidDataException(
                    "Migration validation object is not an included target table.");
            }
            return binding;
        }

        private static string Text(
            DbValue[] row,
            int index,
            string errorMessage)
        {
            if (row[index].Type != DbType.Text)
                throw new InvalidDataException(errorMessage);
            return row[index].AsText;
        }

        private static long Integer(
            DbValue[] row,
            int index,
            string errorMessage)
        {
            if (row[index].Type != DbType.Integer)
                throw new InvalidDataException(errorMessage);
            return row[index].AsInteger;
        }

        private static string? NullableText(
            DbValue[] row,
            int index,
            string errorMessage)
        {
            if (row[index].IsNull)
                return null;
            if (row[index].Type != DbType.Text)
                throw new InvalidDataException(errorMessage);
            return row[index].AsText;
        }

        private sealed record ValidationDataObjectBinding(
            string TargetName,
            string[] ProjectedColumnNames,
            HashSet<string> ProjectedColumnObjectIds);
    }
}
