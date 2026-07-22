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
    IMigrationValidationActivationTarget
{
    private const int MigrationPageCachePages = 2048;

    private readonly string _targetPath;
    private readonly string _leasePath;
    private readonly FileStream _lease;
    private readonly Database _database;
    private readonly MigrationPlan _plan;
    private readonly MigrationCatalog _catalog;
    private readonly string _planDigest;
    private readonly string _snapshotIdentity;
    private readonly IReadOnlyDictionary<string, MigrationPlanObject> _planObjects;
    private readonly IReadOnlyDictionary<string, MigrationCatalogObject> _catalogObjects;
    private readonly ICSharpDbMigrationFaultInjector _faultInjector;
    private bool _disposed;

    private CSharpDbStagedMigrationTarget(
        string targetPath,
        FileStream lease,
        Database database,
        MigrationPlan plan,
        MigrationCatalog catalog,
        string snapshotIdentity,
        string targetIdentity,
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
        TargetIdentity = targetIdentity;
        _planObjects = plan.Objects.ToDictionary(item => item.SourceObjectId, StringComparer.Ordinal);
        _catalogObjects = catalog.Objects.ToDictionary(item => item.ObjectId, StringComparer.Ordinal);
        _faultInjector = faultInjector ?? NoOpMigrationFaultInjector.Instance;
    }

    public string TargetIdentity { get; }

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
            var target = new CSharpDbStagedMigrationTarget(
                fullPath,
                lease,
                database,
                plan,
                catalog,
                sourceSnapshotIdentity,
                state.TargetIdentity,
                faultInjector);
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
        ThrowIfDisposed();
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

        bool transactionStarted = false;
        bool commitInvoked = false;
        try
        {
            await _database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            transactionStarted = true;
            foreach (string action in actions)
                await ExecuteNonQueryAsync(_database, action, cancellationToken).ConfigureAwait(false);

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
            if (transactionStarted && !commitInvoked)
                await TryRollbackAsync(_database).ConfigureAwait(false);
            throw;
        }
    }

    public async ValueTask<MigrationBatchReceipt> WriteBatchAsync(
        MigrationTargetBatch batch,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(batch);
        ValidateTargetBatch(batch);

        MigrationBatchReceipt? existing = await ReadReceiptAsync(
            batch.PlanDigest,
            batch.SourceObjectId,
            batch.BatchOrdinal,
            cancellationToken).ConfigureAwait(false);
        if (existing is not null)
        {
            ValidateReceiptAgainstBatch(existing, batch);
            return existing;
        }

        await RequireMutableLifecycleAsync(cancellationToken).ConfigureAwait(false);

        await RequireStageAsync(MigrationSchemaStage.LoadEssential, cancellationToken).ConfigureAwait(false);
        if (await ReadStageAsync(MigrationSchemaStage.SecondaryIndexes, cancellationToken).ConfigureAwait(false) is not null)
        {
            throw new InvalidDataException(
                "A missing data batch cannot be appended after post-load schema stages have begun.");
        }

        MigrationPlanObject tablePlan = _planObjects[batch.SourceObjectId];
        InsertBatch dataInsert = _database.PrepareInsertBatch(tablePlan.TargetName!, batch.Rows.Count);
        foreach (MigrationTargetRow row in batch.Rows)
            dataInsert.AddRow(row.Values.ToArray());

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
            RowCount = batch.Rows.Count,
            RejectedRowCount = 0,
        };
        InsertBatch receiptInsert = _database.PrepareInsertBatch(CSharpDbMigrationSql.ReceiptTable, 1);
        receiptInsert.AddRow(ReceiptValues(receipt));

        bool transactionStarted = false;
        bool commitInvoked = false;
        try
        {
            await _database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            transactionStarted = true;
            await _faultInjector.InjectAsync(
                CSharpDbMigrationFaultPoint.BeforeRows,
                batch,
                cancellationToken).ConfigureAwait(false);
            int rowsAffected = await dataInsert.ExecuteAsync(cancellationToken).ConfigureAwait(false);
            if (rowsAffected != batch.Rows.Count)
                throw new InvalidDataException("Target row count differs from the converted migration batch.");
            await _faultInjector.InjectAsync(
                CSharpDbMigrationFaultPoint.AfterRowsBeforeReceipt,
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
        }
        catch
        {
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

        MigrationNormalizedSchema schema = CaptureActualSchema();
        return new CSharpDbValidationSnapshot(
            _database.CreateReaderSession(),
            TargetIdentity,
            _planObjects,
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
            ValidateActivationReceiptBinding(receipt);
        return receipt;
    }

    public async ValueTask ActivateAsync(
        MigrationValidationActivationPermit permit,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(permit);
        MigrationValidationActivationReceipt receipt = permit.Receipt;
        ValidateActivationReceiptBinding(receipt);
        await ReadAndValidateActivationReportAsync(
            permit.PublishedReportPath,
            receipt,
            cancellationToken).ConfigureAwait(false);

        MigrationValidationActivationReceipt? existing = await ReadActivationReceiptCoreAsync(
            cancellationToken).ConfigureAwait(false);
        if (existing is not null)
        {
            ValidateActivationReceiptBinding(existing);
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

    private void ValidateActivationReceiptBinding(MigrationValidationActivationReceipt receipt)
    {
        MigrationValidationLevel requiredLevel = _plan.Validation.ValidateChecksums
            ? MigrationValidationLevel.Checksum
            : _plan.Validation.ValidateCounts
                ? MigrationValidationLevel.Count
                : MigrationValidationLevel.Schema;
        string expectedTargetSnapshot = ValidationSnapshotIdentity(
            TargetIdentity,
            CSharpDbMigrationSql.AwaitingValidationState);

        if (!string.Equals(receipt.TargetIdentity, TargetIdentity, StringComparison.Ordinal) ||
            !string.Equals(receipt.PlanDigest, _planDigest, StringComparison.Ordinal) ||
            !string.Equals(receipt.CatalogDigest, _plan.CatalogDigest, StringComparison.Ordinal) ||
            !string.Equals(receipt.SourceSnapshotIdentity, _snapshotIdentity, StringComparison.Ordinal) ||
            !string.Equals(receipt.TargetSnapshotIdentity, expectedTargetSnapshot, StringComparison.Ordinal) ||
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

    private static string ValidationSnapshotIdentity(string targetIdentity, string lifecycle) =>
        $"staged-target:{targetIdentity}:{lifecycle}";

    private MigrationNormalizedSchema CaptureActualSchema()
    {
        MigrationPlanObject[] included = _plan.Objects
            .Where(item => item.Included)
            .OrderBy(item => item.SourceObjectId, StringComparer.Ordinal)
            .ToArray();
        IReadOnlyDictionary<string, TableSchema> tables = included
            .Where(item => _catalogObjects[item.SourceObjectId].Kind is
                MigrationObjectKind.Table or MigrationObjectKind.Collection)
            .Select(item => (Plan: item, Schema: _database.GetTableSchema(item.TargetName!)))
            .Where(item => item.Schema is not null)
            .ToDictionary(
                item => item.Plan.SourceObjectId,
                item => item.Schema!,
                StringComparer.Ordinal);
        IndexSchema[] indexes = _database.GetIndexes().ToArray();
        string[] viewNames = _database.GetViewNames().ToArray();
        TriggerSchema[] triggers = _database.GetTriggers().ToArray();
        var definitions = new List<MigrationNormalizedSchemaObject>(included.Length);

        foreach (MigrationPlanObject planned in included)
        {
            MigrationCatalogObject catalogObject = _catalogObjects[planned.SourceObjectId];
            MigrationNormalizedSchemaObject? definition = catalogObject.Kind switch
            {
                MigrationObjectKind.Table or MigrationObjectKind.Collection =>
                    CaptureTable(catalogObject, tables),
                MigrationObjectKind.Column => CaptureColumn(catalogObject, tables),
                MigrationObjectKind.Index => CaptureIndex(catalogObject, indexes, tables),
                MigrationObjectKind.Key => CaptureKey(catalogObject, tables),
                MigrationObjectKind.ForeignKey => CaptureForeignKey(catalogObject, tables),
                MigrationObjectKind.CheckConstraint => CaptureCheck(catalogObject, tables),
                MigrationObjectKind.View => CaptureView(catalogObject, viewNames),
                MigrationObjectKind.Trigger => CaptureTrigger(catalogObject, triggers),
                _ => null,
            };
            if (definition is not null)
                definitions.Add(definition);
        }

        CaptureUnexpectedSchema(definitions, indexes, viewNames, triggers);

        return MigrationNormalizedSchemaContract.Create(definitions);
    }

    private void CaptureUnexpectedSchema(
        ICollection<MigrationNormalizedSchemaObject> definitions,
        IReadOnlyList<IndexSchema> indexes,
        IReadOnlyList<string> viewNames,
        IReadOnlyList<TriggerSchema> triggers)
    {
        var knownIds = definitions
            .Select(item => item.ObjectId)
            .ToHashSet(StringComparer.Ordinal);
        TableSchema[] actualTables = _database.GetTableNames()
            .Where(name => !IsMigrationMetadataTable(name))
            .Select(name => _database.GetTableSchema(name))
            .Where(schema => schema is not null)
            .Cast<TableSchema>()
            .OrderBy(schema => schema.TableName, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var tableIds = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var columnIds = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var actualKeys = new List<(string TableName, KeyConstraintDefinition Key, string ObjectId)>();

        foreach (TableSchema table in actualTables)
        {
            string tableId = ResolvePlannedObjectId(
                    MigrationObjectKind.Table,
                    parentObjectId: null,
                    table.TableName) ??
                ResolvePlannedObjectId(
                    MigrationObjectKind.Collection,
                    parentObjectId: null,
                    table.TableName) ??
                ExtraObjectId(MigrationObjectKind.Table, null, table.TableName);
            tableIds.Add(table.TableName, tableId);
            if (knownIds.Add(tableId))
            {
                definitions.Add(MigrationNormalizedSchemaContract.CreateObject(
                    tableId,
                    MigrationObjectKind.Table,
                    parentObjectId: null,
                    targetName: tableId));
            }

            foreach (ColumnDefinition column in table.Columns)
            {
                string columnId = ResolvePlannedObjectId(
                        MigrationObjectKind.Column,
                        tableId,
                        column.Name) ??
                    ExtraObjectId(MigrationObjectKind.Column, tableId, column.Name);
                columnIds.Add(ColumnLookupKey(table.TableName, column.Name), columnId);
                if (!knownIds.Add(columnId))
                    continue;

                var attributes = new List<MigrationNormalizedSchemaAttribute>
                {
                    Attribute("targetType", column.Type.ToString()),
                    Attribute("nullable", BooleanToken(column.Nullable)),
                    Attribute("identity", BooleanToken(column.IsIdentity)),
                    Attribute("rowVersion", BooleanToken(column.IsRowVersion)),
                };
                if (column.Collation is not null)
                    attributes.Add(Attribute("collation", column.Collation));
                if (column.DefaultSql is not null)
                    attributes.Add(Attribute("defaultSqlDigest", SqlDigest(column.DefaultSql)));
                definitions.Add(MigrationNormalizedSchemaContract.CreateObject(
                    columnId,
                    MigrationObjectKind.Column,
                    tableId,
                    columnId,
                    attributes));
            }

            foreach (KeyConstraintDefinition key in table.KeyConstraints)
            {
                string discriminator = key.ConstraintName ??
                    $"{key.Kind}:{string.Join("\0", key.Columns)}";
                string keyId = ResolvePlannedObjectId(
                        MigrationObjectKind.Key,
                        tableId,
                        key.ConstraintName) ??
                    ExtraObjectId(MigrationObjectKind.Key, tableId, discriminator);
                actualKeys.Add((table.TableName, key, keyId));
                if (!knownIds.Add(keyId))
                    continue;

                definitions.Add(MigrationNormalizedSchemaContract.CreateObject(
                    keyId,
                    MigrationObjectKind.Key,
                    tableId,
                    keyId,
                    [Attribute("kind", key.Kind == KeyConstraintKind.PrimaryKey ? "primary" : "unique")],
                    key.Columns.Select((name, ordinal) => new MigrationNormalizedSchemaMember
                    {
                        Role = MigrationObjectReferenceRoles.Column,
                        Ordinal = ordinal,
                        ObjectId = ResolveActualColumnId(columnIds, table.TableName, name),
                    }).ToArray()));
            }

            foreach (CheckConstraintDefinition check in table.CheckConstraints)
            {
                string discriminator = check.ConstraintName ?? SqlDigest(check.ExpressionSql);
                string checkId = ResolvePlannedObjectId(
                        MigrationObjectKind.CheckConstraint,
                        tableId,
                        check.ConstraintName) ??
                    ExtraObjectId(MigrationObjectKind.CheckConstraint, tableId, discriminator);
                if (!knownIds.Add(checkId))
                    continue;
                definitions.Add(MigrationNormalizedSchemaContract.CreateObject(
                    checkId,
                    MigrationObjectKind.CheckConstraint,
                    tableId,
                    checkId,
                    [Attribute("targetSqlDigest", SqlDigest(check.ExpressionSql))]));
            }
        }

        foreach (TableSchema table in actualTables)
        {
            string tableId = tableIds[table.TableName];
            foreach (ForeignKeyDefinition foreignKey in table.ForeignKeys)
            {
                string foreignKeyId = ResolvePlannedObjectId(
                        MigrationObjectKind.ForeignKey,
                        tableId,
                        foreignKey.ConstraintName) ??
                    ExtraObjectId(
                        MigrationObjectKind.ForeignKey,
                        tableId,
                        foreignKey.ConstraintName);
                if (!knownIds.Add(foreignKeyId))
                    continue;

                IReadOnlyList<string> sourceColumns = foreignKey.ColumnNames.Count > 0
                    ? foreignKey.ColumnNames
                    : [foreignKey.ColumnName];
                IReadOnlyList<string> referencedColumns = foreignKey.ReferencedColumnNames.Count > 0
                    ? foreignKey.ReferencedColumnNames
                    : [foreignKey.ReferencedColumnName];
                var members = sourceColumns.Select((name, ordinal) =>
                    new MigrationNormalizedSchemaMember
                    {
                        Role = MigrationObjectReferenceRoles.SourceColumn,
                        Ordinal = ordinal,
                        ObjectId = ResolveActualColumnId(columnIds, table.TableName, name),
                    }).ToList();
                string? referencedKeyId = actualKeys
                    .Where(candidate => string.Equals(
                        candidate.TableName,
                        foreignKey.ReferencedTableName,
                        StringComparison.OrdinalIgnoreCase))
                    .Where(candidate => candidate.Key.Columns.SequenceEqual(
                        referencedColumns,
                        StringComparer.OrdinalIgnoreCase))
                    .Select(candidate => candidate.ObjectId)
                    .OrderBy(id => id, StringComparer.Ordinal)
                    .FirstOrDefault();
                if (referencedKeyId is not null)
                {
                    members.Add(new MigrationNormalizedSchemaMember
                    {
                        Role = MigrationObjectReferenceRoles.ReferencedKey,
                        Ordinal = 0,
                        ObjectId = referencedKeyId,
                    });
                }

                definitions.Add(MigrationNormalizedSchemaContract.CreateObject(
                    foreignKeyId,
                    MigrationObjectKind.ForeignKey,
                    tableId,
                    foreignKeyId,
                    [
                        Attribute(
                            "onDelete",
                            foreignKey.OnDelete == ForeignKeyOnDeleteAction.Cascade
                                ? "cascade"
                                : "restrict"),
                        Attribute("onUpdate", "restrict"),
                    ],
                    members));
            }
        }

        foreach (IndexSchema index in indexes
                     .Where(item => item.Kind == IndexKind.Sql)
                     .OrderBy(item => item.IndexName, StringComparer.OrdinalIgnoreCase))
        {
            if (!tableIds.TryGetValue(index.TableName, out string? tableId))
                continue;
            string indexId = ResolvePlannedObjectId(
                    MigrationObjectKind.Index,
                    tableId,
                    index.IndexName) ??
                ExtraObjectId(MigrationObjectKind.Index, tableId, index.IndexName);
            if (!knownIds.Add(indexId))
                continue;
            definitions.Add(MigrationNormalizedSchemaContract.CreateObject(
                indexId,
                MigrationObjectKind.Index,
                tableId,
                indexId,
                [Attribute("unique", BooleanToken(index.IsUnique))],
                index.Columns.Select((name, ordinal) => new MigrationNormalizedSchemaMember
                {
                    Role = MigrationObjectReferenceRoles.Column,
                    Ordinal = ordinal,
                    ObjectId = ResolveActualColumnId(columnIds, index.TableName, name),
                }).ToArray()));
        }

        foreach (string viewName in viewNames.OrderBy(name => name, StringComparer.OrdinalIgnoreCase))
        {
            string viewId = ResolvePlannedObjectId(
                    MigrationObjectKind.View,
                    parentObjectId: null,
                    viewName) ??
                ExtraObjectId(MigrationObjectKind.View, null, viewName);
            if (!knownIds.Add(viewId) || _database.GetViewSql(viewName) is not string sql)
                continue;
            definitions.Add(MigrationNormalizedSchemaContract.CreateObject(
                viewId,
                MigrationObjectKind.View,
                parentObjectId: null,
                targetName: viewId,
                [Attribute("targetSqlDigest", SqlDigest(sql))]));
        }

        foreach (TriggerSchema trigger in triggers.OrderBy(
                     item => item.TriggerName,
                     StringComparer.OrdinalIgnoreCase))
        {
            tableIds.TryGetValue(trigger.TableName, out string? tableId);
            string triggerId = ResolvePlannedObjectId(
                    MigrationObjectKind.Trigger,
                    tableId,
                    trigger.TriggerName) ??
                ExtraObjectId(MigrationObjectKind.Trigger, tableId, trigger.TriggerName);
            if (!knownIds.Add(triggerId))
                continue;
            string structuralSql =
                $"CREATE TRIGGER {CSharpDbMigrationSql.Quote(trigger.TriggerName)} " +
                $"{trigger.Timing.ToString().ToUpperInvariant()} " +
                $"{trigger.Event.ToString().ToUpperInvariant()} ON " +
                $"{CSharpDbMigrationSql.Quote(trigger.TableName)} BEGIN {trigger.BodySql} END";
            definitions.Add(MigrationNormalizedSchemaContract.CreateObject(
                triggerId,
                MigrationObjectKind.Trigger,
                tableId,
                triggerId,
                [Attribute("targetSqlDigest", SqlDigest(structuralSql))]));
        }
    }

    private string? ResolvePlannedObjectId(
        MigrationObjectKind kind,
        string? parentObjectId,
        string? targetName)
    {
        if (string.IsNullOrEmpty(targetName))
            return null;
        return _plan.Objects
            .Where(item => item.Included && _catalogObjects[item.SourceObjectId].Kind == kind)
            .Where(item => parentObjectId is null || string.Equals(
                _catalogObjects[item.SourceObjectId].ParentObjectId,
                parentObjectId,
                StringComparison.Ordinal))
            .SingleOrDefault(item => string.Equals(
                item.TargetName,
                targetName,
                StringComparison.OrdinalIgnoreCase))
            ?.SourceObjectId;
    }

    private static string ResolveActualColumnId(
        IReadOnlyDictionary<string, string> columnIds,
        string tableName,
        string columnName) =>
        columnIds.TryGetValue(ColumnLookupKey(tableName, columnName), out string? objectId)
            ? objectId
            : ExtraObjectId(MigrationObjectKind.Column, null, $"{tableName}\0{columnName}");

    private static string ColumnLookupKey(string tableName, string columnName) =>
        $"{tableName}\0{columnName}";

    private static string ExtraObjectId(
        MigrationObjectKind kind,
        string? parentObjectId,
        string discriminator)
    {
        string material = string.Join(
            '\0',
            "csharpdb-target-extra/v1",
            kind.ToString(),
            parentObjectId ?? string.Empty,
            discriminator.ToLowerInvariant());
        string digest = Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(material)))
            .ToLowerInvariant();
        return $"target-extra:{kind.ToString().ToLowerInvariant()}:{digest}";
    }

    private static bool IsMigrationMetadataTable(string tableName) => tableName is
        CSharpDbMigrationSql.StateTable or
        CSharpDbMigrationSql.StageTable or
        CSharpDbMigrationSql.ReceiptTable or
        CSharpDbMigrationSql.ValidationReceiptTable;

    private MigrationNormalizedSchemaObject? CaptureTable(
        MigrationCatalogObject item,
        IReadOnlyDictionary<string, TableSchema> tables) =>
        tables.TryGetValue(item.ObjectId, out TableSchema? table)
            ? CreateActualObject(item, table.TableName)
            : null;

    private MigrationNormalizedSchemaObject? CaptureColumn(
        MigrationCatalogObject item,
        IReadOnlyDictionary<string, TableSchema> tables)
    {
        if (item.ParentObjectId is null ||
            !tables.TryGetValue(item.ParentObjectId, out TableSchema? table))
        {
            return null;
        }

        string expectedName = _planObjects[item.ObjectId].TargetName!;
        ColumnDefinition? column = table.Columns.SingleOrDefault(candidate =>
            string.Equals(candidate.Name, expectedName, StringComparison.OrdinalIgnoreCase));
        if (column is null)
            return null;

        var attributes = new List<MigrationNormalizedSchemaAttribute>
        {
            Attribute("targetType", column.Type.ToString()),
            Attribute("nullable", BooleanToken(column.Nullable)),
            Attribute("identity", BooleanToken(column.IsIdentity)),
            Attribute("rowVersion", BooleanToken(column.IsRowVersion)),
        };
        if (column.Collation is not null)
            attributes.Add(Attribute("collation", column.Collation));
        if (column.DefaultSql is not null)
            attributes.Add(Attribute("defaultSqlDigest", SqlDigest(column.DefaultSql)));
        return CreateActualObject(item, column.Name, attributes);
    }

    private MigrationNormalizedSchemaObject? CaptureIndex(
        MigrationCatalogObject item,
        IReadOnlyList<IndexSchema> indexes,
        IReadOnlyDictionary<string, TableSchema> tables)
    {
        if (item.ParentObjectId is null ||
            !tables.TryGetValue(item.ParentObjectId, out TableSchema? table))
        {
            return null;
        }

        string expectedName = _planObjects[item.ObjectId].TargetName!;
        IndexSchema? index = indexes.SingleOrDefault(candidate =>
            candidate.Kind == IndexKind.Sql &&
            string.Equals(candidate.IndexName, expectedName, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(candidate.TableName, table.TableName, StringComparison.OrdinalIgnoreCase));
        if (index is null)
            return null;

        MigrationNormalizedSchemaMember[] members = MapColumnMembers(
            item.ParentObjectId,
            index.Columns,
            MigrationObjectReferenceRoles.Column);
        return CreateActualObject(
            item,
            index.IndexName,
            [Attribute("unique", BooleanToken(index.IsUnique))],
            members);
    }

    private MigrationNormalizedSchemaObject? CaptureKey(
        MigrationCatalogObject item,
        IReadOnlyDictionary<string, TableSchema> tables)
    {
        if (item.ParentObjectId is null ||
            !tables.TryGetValue(item.ParentObjectId, out TableSchema? table))
        {
            return null;
        }

        string expectedName = _planObjects[item.ObjectId].TargetName!;
        KeyConstraintDefinition? key = table.KeyConstraints.SingleOrDefault(candidate =>
            string.Equals(candidate.ConstraintName, expectedName, StringComparison.OrdinalIgnoreCase));
        if (key is null)
            return null;

        string kind = key.Kind switch
        {
            KeyConstraintKind.PrimaryKey => "primary",
            KeyConstraintKind.Unique => "unique",
            _ => throw new InvalidDataException(
                $"Target key '{key.ConstraintName}' has unknown kind '{key.Kind}'."),
        };
        return CreateActualObject(
            item,
            key.ConstraintName!,
            [Attribute("kind", kind)],
            MapColumnMembers(item.ParentObjectId, key.Columns, MigrationObjectReferenceRoles.Column));
    }

    private MigrationNormalizedSchemaObject? CaptureForeignKey(
        MigrationCatalogObject item,
        IReadOnlyDictionary<string, TableSchema> tables)
    {
        if (item.ParentObjectId is null ||
            !tables.TryGetValue(item.ParentObjectId, out TableSchema? table))
        {
            return null;
        }

        string expectedName = _planObjects[item.ObjectId].TargetName!;
        ForeignKeyDefinition? foreignKey = table.ForeignKeys.SingleOrDefault(candidate =>
            string.Equals(candidate.ConstraintName, expectedName, StringComparison.OrdinalIgnoreCase));
        if (foreignKey is null)
            return null;

        IReadOnlyList<string> sourceColumns = foreignKey.ColumnNames.Count > 0
            ? foreignKey.ColumnNames
            : [foreignKey.ColumnName];
        var members = MapColumnMembers(
                item.ParentObjectId,
                sourceColumns,
                MigrationObjectReferenceRoles.SourceColumn)
            .ToList();
        string? referencedKeyId = ResolveReferencedKey(item, foreignKey, tables);
        if (referencedKeyId is not null)
        {
            members.Add(new MigrationNormalizedSchemaMember
            {
                Role = MigrationObjectReferenceRoles.ReferencedKey,
                Ordinal = 0,
                ObjectId = referencedKeyId,
            });
        }

        return CreateActualObject(
            item,
            foreignKey.ConstraintName,
            [
                Attribute("onDelete", foreignKey.OnDelete switch
                {
                    ForeignKeyOnDeleteAction.Restrict => "restrict",
                    ForeignKeyOnDeleteAction.Cascade => "cascade",
                    _ => throw new InvalidDataException(
                        $"Target foreign key '{foreignKey.ConstraintName}' has an unknown delete action."),
                }),
                Attribute("onUpdate", "restrict"),
            ],
            members);
    }

    private MigrationNormalizedSchemaObject? CaptureCheck(
        MigrationCatalogObject item,
        IReadOnlyDictionary<string, TableSchema> tables)
    {
        if (item.ParentObjectId is null ||
            !tables.TryGetValue(item.ParentObjectId, out TableSchema? table))
        {
            return null;
        }

        string expectedName = _planObjects[item.ObjectId].TargetName!;
        CheckConstraintDefinition? check = table.CheckConstraints.SingleOrDefault(candidate =>
            string.Equals(candidate.ConstraintName, expectedName, StringComparison.OrdinalIgnoreCase));
        return check is null
            ? null
            : CreateActualObject(
                item,
                check.ConstraintName!,
                [Attribute("targetSqlDigest", SqlDigest(check.ExpressionSql))]);
    }

    private MigrationNormalizedSchemaObject? CaptureView(
        MigrationCatalogObject item,
        IReadOnlyList<string> viewNames)
    {
        string expectedName = _planObjects[item.ObjectId].TargetName!;
        string? actualName = viewNames.SingleOrDefault(candidate =>
            string.Equals(candidate, expectedName, StringComparison.OrdinalIgnoreCase));
        if (actualName is null || _database.GetViewSql(actualName) is not string sql)
            return null;
        return CreateActualObject(
            item,
            actualName,
            [Attribute("targetSqlDigest", SqlDigest(sql))]);
    }

    private MigrationNormalizedSchemaObject? CaptureTrigger(
        MigrationCatalogObject item,
        IReadOnlyList<TriggerSchema> triggers)
    {
        string expectedName = _planObjects[item.ObjectId].TargetName!;
        TriggerSchema? trigger = triggers.SingleOrDefault(candidate =>
            string.Equals(candidate.TriggerName, expectedName, StringComparison.OrdinalIgnoreCase));
        if (trigger is null)
            return null;

        string structuralSql =
            $"CREATE TRIGGER {CSharpDbMigrationSql.Quote(trigger.TriggerName)} " +
            $"{trigger.Timing.ToString().ToUpperInvariant()} " +
            $"{trigger.Event.ToString().ToUpperInvariant()} ON " +
            $"{CSharpDbMigrationSql.Quote(trigger.TableName)} BEGIN {trigger.BodySql} END";
        return CreateActualObject(
            item,
            trigger.TriggerName,
            [Attribute("targetSqlDigest", SqlDigest(structuralSql))]);
    }

    private string? ResolveReferencedKey(
        MigrationCatalogObject foreignKeyObject,
        ForeignKeyDefinition foreignKey,
        IReadOnlyDictionary<string, TableSchema> tables)
    {
        string? referencedKeyId = foreignKeyObject.Members
            .Where(member => string.Equals(
                member.Role,
                MigrationObjectReferenceRoles.ReferencedKey,
                StringComparison.Ordinal))
            .OrderBy(member => member.Ordinal)
            .Select(member => member.ObjectId)
            .SingleOrDefault();
        if (referencedKeyId is null ||
            !_catalogObjects.TryGetValue(referencedKeyId, out MigrationCatalogObject? referencedKey) ||
            referencedKey.Kind != MigrationObjectKind.Key ||
            referencedKey.ParentObjectId is null ||
            !tables.TryGetValue(referencedKey.ParentObjectId, out TableSchema? table) ||
            !_planObjects.TryGetValue(referencedKeyId, out MigrationPlanObject? keyPlan) ||
            !keyPlan.Included)
        {
            return null;
        }

        IReadOnlyList<string> referencedColumns = foreignKey.ReferencedColumnNames.Count > 0
            ? foreignKey.ReferencedColumnNames
            : [foreignKey.ReferencedColumnName];
        KeyConstraintDefinition? key = table.KeyConstraints.SingleOrDefault(candidate =>
            string.Equals(
                candidate.ConstraintName,
                keyPlan.TargetName,
                StringComparison.OrdinalIgnoreCase));
        if (key is null ||
            !string.Equals(
                table.TableName,
                foreignKey.ReferencedTableName,
                StringComparison.OrdinalIgnoreCase) ||
            !key.Columns.SequenceEqual(referencedColumns, StringComparer.OrdinalIgnoreCase))
        {
            return null;
        }
        return referencedKeyId;
    }

    private MigrationNormalizedSchemaMember[] MapColumnMembers(
        string tableObjectId,
        IReadOnlyList<string> actualColumnNames,
        string role) => actualColumnNames
        .Select((columnName, ordinal) => new MigrationNormalizedSchemaMember
        {
            Role = role,
            Ordinal = ordinal,
            ObjectId = ResolveColumnObjectId(tableObjectId, columnName),
        })
        .ToArray();

    private string ResolveColumnObjectId(string tableObjectId, string targetColumnName)
    {
        MigrationPlanObject? column = _plan.Objects.SingleOrDefault(candidate =>
            candidate.Included &&
            _catalogObjects[candidate.SourceObjectId].Kind == MigrationObjectKind.Column &&
            string.Equals(
                _catalogObjects[candidate.SourceObjectId].ParentObjectId,
                tableObjectId,
                StringComparison.Ordinal) &&
            string.Equals(candidate.TargetName, targetColumnName, StringComparison.OrdinalIgnoreCase));
        return column?.SourceObjectId ??
            $"target-column:{tableObjectId}:{targetColumnName.ToLowerInvariant()}";
    }

    private static MigrationNormalizedSchemaObject CreateActualObject(
        MigrationCatalogObject item,
        string targetName,
        IReadOnlyList<MigrationNormalizedSchemaAttribute>? attributes = null,
        IReadOnlyList<MigrationNormalizedSchemaMember>? members = null) =>
        MigrationNormalizedSchemaContract.CreateObject(
            item.ObjectId,
            item.Kind,
            item.ParentObjectId,
            targetName,
            attributes,
            members);

    private static MigrationNormalizedSchemaAttribute Attribute(string name, string value) => new()
    {
        Name = name,
        Value = value,
    };

    private static string BooleanToken(bool value) => value ? "true" : "false";

    private static string SqlDigest(string sql) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(sql))).ToLowerInvariant();

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
        if (!string.Equals(state.TargetTag, CSharpDbMigrationSql.TargetTag, StringComparison.Ordinal) ||
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
        if (!string.Equals(MigrationBatchDigest.Compute(batch), batch.BatchDigest, StringComparison.Ordinal))
            throw new InvalidDataException("Migration target batch digest does not match its converted payload.");
        if (batch.BatchOrdinal < 0 || batch.Rows is null || batch.Rows.Count == 0 ||
            batch.Rows.Count > _plan.Load.BatchSize)
        {
            throw new InvalidDataException("Migration target batch row count or ordinal is invalid.");
        }
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

        long previousRowOrdinal = -1;
        foreach (MigrationTargetRow row in batch.Rows)
        {
            if (row is null || row.Values is null || row.Values.Count != columns.Length ||
                row.SourceRowOrdinal < 0 ||
                (previousRowOrdinal >= 0 && row.SourceRowOrdinal != previousRowOrdinal + 1))
            {
                throw new InvalidDataException("Migration target row shape or source ordinal is invalid.");
            }
            previousRowOrdinal = row.SourceRowOrdinal;
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
        if (!string.Equals(receipt.TargetIdentity, TargetIdentity, StringComparison.Ordinal) ||
            !string.Equals(receipt.PlanDigest, _planDigest, StringComparison.Ordinal) ||
            !string.Equals(receipt.CatalogDigest, _plan.CatalogDigest, StringComparison.Ordinal) ||
            !string.Equals(receipt.SourceFingerprint, _plan.Source.Fingerprint, StringComparison.Ordinal) ||
            !string.Equals(receipt.SourceSnapshotIdentity, _snapshotIdentity, StringComparison.Ordinal) ||
            receipt.BatchOrdinal < 0 || receipt.RowCount < 0 || receipt.RejectedRowCount != 0)
        {
            throw new InvalidDataException("Stored migration receipt does not match the staged target binding.");
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
            receipt.RowCount != batch.Rows.Count || receipt.RejectedRowCount != 0)
        {
            throw new InvalidDataException(
                $"Stored migration receipt for '{batch.SourceObjectId}' batch {batch.BatchOrdinal} does not match the replayed batch.");
        }
    }

    private static DbValue[] ReceiptValues(MigrationBatchReceipt receipt) =>
    [
        DbValue.FromText(CSharpDbMigrationSql.ReceiptTag),
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
        DbValue.FromInteger(receipt.RowCount),
        DbValue.FromInteger(receipt.RejectedRowCount),
    ];

    private static string ReceiptSelect()
    {
        string[] columns =
        [
            "receipt_tag", "target_identity", "plan_digest", "catalog_digest",
            "source_fingerprint", "source_snapshot_identity", "source_object_id",
            "batch_ordinal", "start_cursor", "next_cursor", "batch_digest",
            "row_count", "rejected_row_count",
        ];
        return $"SELECT {string.Join(", ", columns.Select(CSharpDbMigrationSql.Quote))} " +
            $"FROM {CSharpDbMigrationSql.Quote(CSharpDbMigrationSql.ReceiptTable)}";
    }

    private static MigrationBatchReceipt MapReceipt(DbValue[] row)
    {
        if (row.Length != 13 ||
            !string.Equals(row[0].AsText, CSharpDbMigrationSql.ReceiptTag, StringComparison.Ordinal))
        {
            throw new InvalidDataException("Migration batch receipt shape or format tag is invalid.");
        }
        return new MigrationBatchReceipt
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
            RowCount = row[11].AsInteger,
            RejectedRowCount = row[12].AsInteger,
        };
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
        MigrationApplyPolicyValidator.ValidateForExecution(plan);
        foreach (MigrationSchemaStage stage in Enum.GetValues<MigrationSchemaStage>())
            _ = CSharpDbMigrationSql.BuildStageActions(plan, catalog, stage);
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

    private sealed class CSharpDbValidationSnapshot : IMigrationEvidenceValidationSnapshot
    {
        private readonly Database.ReaderSession _session;
        private readonly IReadOnlyDictionary<string, MigrationPlanObject> _planObjects;
        private readonly MigrationCatalog _catalog;
        private readonly MigrationNormalizedSchema _schema;
        private bool _disposed;

        internal CSharpDbValidationSnapshot(
            Database.ReaderSession session,
            string targetIdentity,
            IReadOnlyDictionary<string, MigrationPlanObject> planObjects,
            MigrationCatalog catalog,
            MigrationNormalizedSchema schema)
        {
            _session = session;
            _planObjects = planObjects;
            _catalog = catalog;
            _schema = schema;
            // Activation must not change the identity bound into the durable
            // report; reopening and validating the same target is idempotent.
            SnapshotIdentity = ValidationSnapshotIdentity(
                targetIdentity,
                CSharpDbMigrationSql.AwaitingValidationState);
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
            string tableName = ResolveTable(objectId);
            await using var result = await _session.ExecuteReadAsync(
                $"SELECT COUNT(*) FROM {CSharpDbMigrationSql.Quote(tableName)}",
                cancellationToken).ConfigureAwait(false);
            if (!await result.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                throw new InvalidDataException($"Validation count for '{objectId}' returned no row.");
            long count = result.Current[0].AsInteger;
            if (await result.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                throw new InvalidDataException($"Validation count for '{objectId}' returned multiple rows.");
            return count;
        }

        public async IAsyncEnumerable<MigrationValidationRow> ReadRowsAsync(
            string objectId,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            string tableName = ResolveTable(objectId);
            string[] columns = _catalog.Objects
                .Where(item => item.Kind == MigrationObjectKind.Column &&
                    string.Equals(item.ParentObjectId, objectId, StringComparison.Ordinal) &&
                    _planObjects[item.ObjectId].Included)
                .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
                .Select(item => _planObjects[item.ObjectId].TargetName!)
                .ToArray();
            string sql = $"SELECT {string.Join(", ", columns.Select(CSharpDbMigrationSql.Quote))} " +
                $"FROM {CSharpDbMigrationSql.Quote(tableName)}";
            await using var result = await _session.ExecuteReadAsync(sql, cancellationToken).ConfigureAwait(false);
            await foreach (DbValue[] row in result.GetRowsAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return new MigrationValidationRow
                {
                    Values = row.ToArray(),
                };
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

        private string ResolveTable(string objectId)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (!_planObjects.TryGetValue(objectId, out MigrationPlanObject? planned) ||
                !planned.Included || string.IsNullOrWhiteSpace(planned.TargetName))
            {
                throw new InvalidDataException($"Validation object '{objectId}' is not an included target table.");
            }
            MigrationCatalogObject catalogObject = _catalog.Objects.Single(item => item.ObjectId == objectId);
            if (catalogObject.Kind is not (MigrationObjectKind.Table or MigrationObjectKind.Collection))
                throw new InvalidDataException($"Validation object '{objectId}' is not a table or collection.");
            return planned.TargetName;
        }
    }
}
