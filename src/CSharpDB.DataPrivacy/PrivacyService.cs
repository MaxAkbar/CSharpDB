using System.Collections.Concurrent;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Primitives;
using static CSharpDB.DataPrivacy.PrivacyValues;
using static CSharpDB.DataPrivacy.PrivacyPolicyStore;

namespace CSharpDB.DataPrivacy;

/// <summary>Preview is read-only. Apply uses one pinned client and one transaction.</summary>
public sealed class PrivacyService(PrivacyLimits? limits = null)
{
    public PrivacyLimits Limits { get; } = limits ?? new();
    private static readonly ConcurrentDictionary<string, Guid> Unresolved = new(StringComparer.OrdinalIgnoreCase);
    private static readonly ConcurrentDictionary<string, SemaphoreSlim> Gates = new(StringComparer.OrdinalIgnoreCase);
    public Guid? UnresolvedRun(ICSharpDbClient client) => Unresolved.TryGetValue(client.DataSource, out Guid id) ? id : null;

    /// <summary>Read-only validation of a saved policy against the current database.</summary>
    public async Task<PrivacyValidation> ValidateAsync(ICSharpDbClient client, PrivacyPolicy policy, CancellationToken ct = default)
    {
        try { using var preview = await PreviewAsync(client, policy, ct: ct); return new(true, preview.Warnings); }
        catch (PrivacyException error) { return new(false, [error.Message]); }
    }

    public async Task<PrivacyPreview> PreviewAsync(ICSharpDbClient client, PrivacyPolicy policy, DateTimeOffset? referenceUtc = null,
        IProgress<PrivacyProgress>? progress = null, CancellationToken ct = default)
    {
        Limits.Validate(); RequireSupported(client);
        if (UnresolvedRun(client) is not null) throw new PrivacyException("Reconcile the previous run before previewing another run.");
        var frozen = PrivacyPolicy.FromJson(policy.ToJson());
        var reference = referenceUtc?.ToUniversalTime() ?? DateTimeOffset.UtcNow;
        if (frozen.Revision < 1) throw new PrivacyException("Save the policy before previewing.");
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct); timeout.CancelAfter(TimeSpan.FromSeconds(Limits.TimeoutSeconds)); ct = timeout.Token;
        var identity = client;
        await using var exclusive = await OpenExclusiveAsync(client, ct);
        client = exclusive.Client;
        string tx = (await client.BeginTransactionAsync(ct)).TransactionId;
        try
        {
            await AssertCurrentAsync(client, tx, frozen, ct);
            if (await PendingAsync(client, tx, ct) is not null) throw new PrivacyException("Reconcile the previous pending run before previewing.");
            var (tables, hash) = await ReadAsync(client, tx, frozen, progress, ct);
            return await Task.Run(() => new PrivacyPlanner(frozen, tables, Limits, reference, ct).Build(identity, hash), ct);
        }
        finally { await client.RollbackTransactionAsync(tx, CancellationToken.None); }
    }

    public async Task<PrivacyReceipt> ApplyAsync(ICSharpDbClient client, PrivacyPreview preview, PrivacyPolicy currentPolicy,
        IProgress<PrivacyProgress>? progress = null, Func<bool>? targetIsCurrent = null, CancellationToken ct = default)
    {
        RequireSupported(client); Limits.Validate();
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct); timeout.CancelAfter(TimeSpan.FromSeconds(Limits.TimeoutSeconds)); ct = timeout.Token;
        var gate = Gates.GetOrAdd(client.DataSource, _ => new(1, 1));
        await gate.WaitAsync(ct);
        PrivacyReceipt? observedOutcome = null;
        try
        {
            if (preview.Consumed || !ReferenceEquals(client, preview.ClientIdentity) || currentPolicy.Hash != preview.PolicyHash || UnresolvedRun(client) is not null)
                throw new PrivacyException("This preview is stale, already used, or awaiting reconciliation. Preview again.");
            preview.Consumed = true;
            await using var exclusive = await OpenExclusiveAsync(client, ct);
            client = exclusive.Client;
            bool committing = false;
            var counts = preview.Tables.ToDictionary(t => t.Table, t => t.ChangedRows, StringComparer.OrdinalIgnoreCase);
            PrivacyReceipt Receipt(string status, string? message = null)
            {
                var receipt = new PrivacyReceipt(preview.RunId, preview.Policy.Id, preview.Policy.Revision, preview.ReferenceUtc,
                    DateTimeOffset.UtcNow, status, status == "Committed" ? counts : new Dictionary<string, int>(), message);
                if (status == "Unknown") observedOutcome = receipt;
                return receipt;
            }
            // Persist only a run identity and aggregate metadata before opening the write transaction.
            // A restart can discover this intent even when the commit acknowledgement was lost.
            await BeginRunAsync(client, Receipt("Pending"), ct);
            string tx;
            try { tx = (await client.BeginTransactionAsync(ct)).TransactionId; }
            catch { Unresolved[client.DataSource] = preview.RunId; preview.Dispose(); return Receipt("Unknown", "Run preparation was interrupted. Reconcile the run before retrying."); }
            try
            {
                void CheckTarget() { ct.ThrowIfCancellationRequested(); if (targetIsCurrent?.Invoke() == false) throw new OperationCanceledException(); }
                CheckTarget();
                await AssertCurrentAsync(client, tx, preview.Policy, ct);
                if ((await ReadReceiptAsync(client, tx, preview.RunId, ct))?.Status != "Pending") throw new PrivacyException("The run was already reconciled. Preview again.");
                var (tables, hash) = await ReadAsync(client, tx, preview.Policy, progress, ct);
                if (hash != preview.Fingerprint) throw new PrivacyException("Schema, source data, relationships or eligibility changed. Preview again.");
                int written = 0;
                foreach (var batch in preview.Changes.Chunk(Limits.BatchSize))
                {
                    foreach (var change in batch)
                    {
                        CheckTarget();
                        var table = tables[change.Table];
                        var masks = preview.Policy.Targets.Single(t => Same(t.Table, change.Table)).Columns;
                        string assignments = string.Join(", ", masks.Select(m => $"{SqlIdentifierRules.Quote(m.Column)} = {Literal(change.After[m.Column])}"));
                        string predicate = string.Join(" AND ", table.IdentityColumns.Select(c => $"{SqlIdentifierRules.Quote(c)} = {Literal(change.Before[c])}"));
                        string sql = $"UPDATE {SqlIdentifierRules.Quote(change.Table)} SET {assignments} WHERE {predicate};";
                        if (Encoding.UTF8.GetByteCount(sql) > Limits.MaxStatementBytes) throw new PrivacyException("A privacy update exceeds the statement size limit.");
                        var result = await CheckedAsync(client, tx, sql, ct);
                        if (result.RowsAffected != 1) throw new PrivacyException("The database did not update exactly one reviewed row.");
                        written++;
                    }
                    progress?.Report(new("Updating (uncommitted)", written, preview.Changes.Count));
                }
                var (afterTables, _) = await ReadAsync(client, tx, preview.Policy, progress, ct);
                foreach (var table in tables.Values.Where(t => t.Rows.Count != 0 || afterTables[t.Schema.TableName].Rows.Count != 0))
                {
                    CheckTarget();
                    var actual = afterTables[table.Schema.TableName];
                    var changed = preview.Changes.Where(c => Same(c.Table, table.Schema.TableName)).ToDictionary(c => c.Identity, StringComparer.Ordinal);
                    var expectedRows = table.Rows.Select(row => table.IdentityColumns.Count > 0 && changed.TryGetValue(table.Identity(row), out var change) ? change.After : row);
                    string Canonical(Dictionary<string, object?> row) => JsonSerializer.Serialize(table.Schema.Columns.Where(c => !c.IsRowVersion).Select(c => Literal(row[c.Name])));
                    if (!expectedRows.Select(Canonical).Order(StringComparer.Ordinal).SequenceEqual(actual.Rows.Select(Canonical).Order(StringComparer.Ordinal)))
                        throw new PrivacyException("Final rows differ from the reviewed changes. The run will be rolled back.");
                }
                CheckTarget();
                var receipt = Receipt("Committed");
                await RecordAsync(client, tx, receipt, ct);
                CheckTarget(); committing = true;
                progress?.Report(new("Committing", written, written));
                await client.CommitTransactionAsync(tx, CancellationToken.None);
                observedOutcome = receipt;
                return receipt;
            }
            catch (Exception ex)
            {
                if (committing)
                {
                    Unresolved[client.DataSource] = preview.RunId; preview.Uncertain = true;
                    return Receipt("Unknown", "Commit outcome is unknown. Reconcile this run ID before retrying.");
                }
                try { await client.RollbackTransactionAsync(tx, CancellationToken.None); }
                catch
                {
                    Unresolved[client.DataSource] = preview.RunId; preview.Uncertain = true;
                    return Receipt("Unknown", "Rollback could not be confirmed. Reconcile this run before retrying.");
                }
                var rolledBack = Receipt("Rolled back", ex is OperationCanceledException ? "Cancelled; all pending updates were rolled back." : ex is PrivacyException ? ex.Message : "Privacy validation or execution failed; all updates were rolled back.");
                try { await FinishRollbackAsync(client, rolledBack); }
                catch { Unresolved[client.DataSource] = preview.RunId; return Receipt("Unknown", "Updates were rolled back, but the run journal was not confirmed. Reconcile this run."); }
                observedOutcome = rolledBack;
                return rolledBack;
            }
            finally { preview.Dispose(); }
        }
        catch when (observedOutcome is not null)
        {
            return observedOutcome with { Message = $"{observedOutcome.Message} Session cleanup was not confirmed. Reopen the database; reconcile any unknown run before retrying.".Trim() };
        }
        finally { gate.Release(); }
    }

    public async Task<PrivacyReceipt?> ReconcileAsync(ICSharpDbClient client, Guid runId, CancellationToken ct = default)
    {
        RequireSupported(client); Limits.Validate();
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct); timeout.CancelAfter(TimeSpan.FromSeconds(Limits.TimeoutSeconds)); ct = timeout.Token;
        var gate = Gates.GetOrAdd(client.DataSource, _ => new(1, 1)); await gate.WaitAsync(ct);
        try
        {
            await using var exclusive = await OpenExclusiveAsync(client, ct);
            client = exclusive.Client;
            // The direct client's explicit transaction holds the database writer lock.
            // Once acquired, a Pending journal row cannot have a still-running masking transaction.
            string tx = (await client.BeginTransactionAsync(ct)).TransactionId;
            try
            {
                var receipt = await ReadReceiptAsync(client, tx, runId, ct);
                if (receipt?.Status == "Pending")
                {
                    receipt = receipt with { Status = "Rolled back", CompletedUtc = DateTimeOffset.UtcNow, Message = "Reconciled under the writer lock; no masking commit was recorded." };
                    await RecordAsync(client, tx, receipt, ct);
                }
                await client.CommitTransactionAsync(tx, CancellationToken.None);
                if (receipt?.Status is "Committed" or "Rolled back") Unresolved.TryRemove(new KeyValuePair<string, Guid>(client.DataSource, runId));
                return receipt;
            }
            catch { try { await client.RollbackTransactionAsync(tx, CancellationToken.None); } catch { } throw new PrivacyException("The run is still unresolved. Close the original database session and retry reconciliation."); }
        }
        finally { gate.Release(); }
    }

    public static void RequireSupported(ICSharpDbClient client)
    {
        if (client is not ICSharpDbTransactionalSnapshotReader { SupportsTransactionalSnapshotReads: true }
            || client is not ICSharpDbExclusiveSessionProvider { SupportsExclusiveSessions: true })
            throw new PrivacyException("Privacy policies require a standard direct file or durable hybrid connection with exclusive snapshots. Remote, memory-only, custom-storage and sharded targets are not enabled.");
    }

    internal static async ValueTask<CSharpDbExclusiveSession> OpenExclusiveAsync(ICSharpDbClient client, CancellationToken ct)
    {
        RequireSupported(client);
        try { return await ((ICSharpDbExclusiveSessionProvider)client).OpenExclusiveSessionAsync(ct); }
        catch (OperationCanceledException) { throw; }
        catch { throw new PrivacyException("Exclusive database access was not acquired. Finish active transactions/readers and close other applications using this file, then retry."); }
    }

    private async Task<(Dictionary<string, PrivacyTable> Tables, string Hash)> ReadAsync(ICSharpDbClient client, string tx,
        PrivacyPolicy policy, IProgress<PrivacyProgress>? progress, CancellationToken ct)
    {
        var reader = (ICSharpDbTransactionalSnapshotReader)client;
        var names = policy.Targets.Select(t => t.Table).Append(policy.RootTable).Concat(policy.Relationships.SelectMany(r => new[] { r.SourceTable, r.TargetTable })).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var tables = new Dictionary<string, PrivacyTable>(StringComparer.OrdinalIgnoreCase);
        int total = 0; long bytes = 0;
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var catalog = await CheckedAsync(client, tx, "SELECT table_name FROM sys.tables ORDER BY table_name;", ct);
        foreach (string name in (catalog.Rows ?? []).Select(r => (string)r[0]!).Where(n => !DbInternalTableRegistry.IsInternalTable(n)).Order(StringComparer.OrdinalIgnoreCase))
        {
            ct.ThrowIfCancellationRequested();
            var metadata = await reader.ReadTableSnapshotAsync(tx, name, ct);
            if (metadata is null) continue;
            hash.AppendData(JsonSerializer.SerializeToUtf8Bytes(metadata, PrivacyPolicy.JsonOptions));
            var table = new PrivacyTable { Schema = PrivacyPlanner.Map(metadata.Schema), Metadata = metadata };
            var primary = table.Schema.KeyConstraints.FirstOrDefault(k => k.Kind == KeyConstraintKind.PrimaryKey)?.Columns
                ?? table.Schema.Columns.Where(c => c.IsPrimaryKey).Select(c => c.Name).ToArray();
            table.IdentityColumns = primary.ToList();
            if (table.IdentityColumns.Count == 0)
                table.IdentityColumns = table.Schema.KeyConstraints.Where(k => k.Kind == KeyConstraintKind.Unique).Select(k => k.Columns)
                    .Concat(metadata.Indexes.Where(i => i.IsUnique).Select(i => i.Columns)).FirstOrDefault(cols => cols.All(c => !Column(table.Schema, c).Nullable))?.ToList() ?? [];
            tables[name] = table;
            if (!names.Contains(name)) continue;
            string sql = $"SELECT {string.Join(", ", table.Schema.Columns.Select(c => SqlIdentifierRules.Quote(c.Name)))} FROM {SqlIdentifierRules.Quote(name)};";
            await using var cursor = await reader.TryOpenForwardOnlyQueryCursorAsync(tx, sql, ct) ?? throw new PrivacyException("This target cannot stream a consistent privacy snapshot.");
            while (true)
            {
                var batch = await cursor.ReadNextAsync(128, ct);
                if (batch.Count == 0) break;
                foreach (var values in batch)
                {
                    if (++total > Limits.MaxEvaluatedRows) throw new PrivacyException("The evaluated-row limit was exceeded. Split the data into a smaller database or increase the configured limit.");
                    bytes += Size(values) + 128;
                    if (bytes > Limits.MaxPreparedBytes) throw new PrivacyException("The snapshot exceeds the prepared-data memory limit.");
                    table.Rows.Add(table.Schema.Columns.Select((c, i) => (c.Name, Value: values[i])).ToDictionary(p => p.Name, p => p.Value, StringComparer.OrdinalIgnoreCase));
                }
                progress?.Report(new("Reading snapshot", total, Limits.MaxEvaluatedRows));
            }
            // Order-independent row digests avoid retaining another copy of all personal values.
            foreach (string digest in table.Rows.Select(row => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(table.Schema.Columns.Select(c => Literal(row[c.Name]))))))).Order(StringComparer.Ordinal))
                hash.AppendData(Encoding.ASCII.GetBytes(digest));
        }
        if (names.Any(n => !tables.ContainsKey(n))) throw new PrivacyException("A policy references an unavailable, internal, temporary or external table.");
        return (tables, Convert.ToHexString(hash.GetHashAndReset()));
    }
}
