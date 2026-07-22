using System.Globalization;
using System.Runtime.CompilerServices;
using CSharpDB.Engine;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.CSharpDb;

/// <summary>
/// A migration-owned CSharpDB file that records schema stages and batch
/// receipts inside the same durability boundary as the data they describe.
/// It deliberately has no activation/replace operation.
/// </summary>
public sealed class CSharpDbStagedMigrationTarget : IMigrationTarget
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
            return new CSharpDbStagedMigrationTarget(
                fullPath,
                lease,
                database,
                plan,
                catalog,
                sourceSnapshotIdentity,
                state.TargetIdentity,
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
        return new CSharpDbValidationSnapshot(
            _database.CreateReaderSession(),
            TargetIdentity,
            _planObjects,
            _catalog);
    }

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

    private sealed class CSharpDbValidationSnapshot : IValidationSnapshot
    {
        private readonly Database.ReaderSession _session;
        private readonly IReadOnlyDictionary<string, MigrationPlanObject> _planObjects;
        private readonly MigrationCatalog _catalog;
        private bool _disposed;

        internal CSharpDbValidationSnapshot(
            Database.ReaderSession session,
            string targetIdentity,
            IReadOnlyDictionary<string, MigrationPlanObject> planObjects,
            MigrationCatalog catalog)
        {
            _session = session;
            _planObjects = planObjects;
            _catalog = catalog;
            SnapshotIdentity = $"staged-target:{targetIdentity}:awaiting-validation";
        }

        public string SnapshotIdentity { get; }

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
