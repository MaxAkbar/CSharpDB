using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.DataGeneration;
using CSharpDB.Primitives;
using ClientModels = CSharpDB.Client.Models;

namespace CSharpDB.Admin.Services;

public sealed record PreparedTestData(GenerationPlan Plan, object DatabaseIdentity, string Target);

public sealed class TestDataGenerationAdminService(DatabaseClientHolder holder, DatabaseChangeService changes, GenerationLimits limits)
{
    public GenerationLimits Limits => limits;
    public bool IsSupported => !holder.SupportsShardAdmin && holder.SupportsTransactionalSnapshotReads;

    public async Task<IReadOnlyList<GenerationTableSnapshot>> ReadCatalogAsync(CancellationToken ct = default)
    {
        RequireSupported();
        await using var lease = holder.CaptureClient();
        var client = lease.Client;
        var indexes = await client.GetIndexesAsync(ct);
        var result = new List<GenerationTableSnapshot>();
        foreach (string name in await client.GetTableNamesAsync(ct))
        {
            if (!GenerationPlan.IsUserTable(name)) continue;
            var schema = await client.GetTableSchemaAsync(name, ct);
            if (schema is null) continue;
            result.Add(new GenerationTableSnapshot
            {
                Schema = MapSchema(schema),
                Indexes = indexes.Where(i => Same(i.TableName, name)).Select(MapIndex).ToArray(),
                RowCount = await client.GetRowCountAsync(name, ct),
            });
        }
        if (!lease.IsCurrent) throw new InvalidOperationException("The database changed. Reload the generator.");
        return result;
    }

    public async Task<PreparedTestData> PreviewAsync(GenerationProfile profile, IProgress<GenerationProgress>? progress = null, CancellationToken ct = default)
    {
        RequireSupported();
        GenerationProfile frozen = GenerationProfile.FromJson(profile.ToJson());
        await using var lease = holder.CaptureClient();
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(limits.TimeoutSeconds));
        ct = timeout.Token;
        string transaction = (await lease.Client.BeginTransactionAsync(ct)).TransactionId;
        IReadOnlyList<GenerationTableSnapshot> snapshots;
        try { snapshots = await ReadSnapshotsAsync(lease.Client, transaction, frozen, ct); }
        finally { await lease.Client.RollbackTransactionAsync(transaction, CancellationToken.None); }
        // CPU work stays outside the circuit, and outside the database transaction.
        GenerationPlan plan = await Task.Run(() => GenerationPlan.Build(frozen, snapshots, limits, ct, progress), ct);
        if (!lease.IsCurrent) throw new InvalidOperationException("The database changed. Preview again.");
        return new(plan, lease.DatabaseIdentity, lease.Client.DataSource);
    }

    public async Task<GenerationReceipt> ExecuteAsync(PreparedTestData prepared, string currentProfileHash,
        IProgress<GenerationProgress>? progress = null, CancellationToken ct = default)
    {
        RequireSupported();
        if (prepared.Plan.ProfileHash != currentProfileHash) throw new InvalidOperationException("The configuration changed. Preview again before generating.");
        await using var lease = holder.CaptureClient();
        if (!ReferenceEquals(lease.DatabaseIdentity, prepared.DatabaseIdentity)) throw new InvalidOperationException("The database changed. Preview again before generating.");
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(limits.TimeoutSeconds));
        ct = timeout.Token;
        var timer = Stopwatch.StartNew();
        GenerationReceipt Receipt(string status, IReadOnlyDictionary<string, int> inserted, string? message)
            => new(status, prepared.Plan.Seed, prepared.Plan.ProfileHash, StableRandom.Version, prepared.Plan.ReferenceUtc,
                prepared.Target, inserted, timer.Elapsed, message)
            { RequestedRows = prepared.Plan.Counts, SnapshotFingerprints = prepared.Plan.SnapshotFingerprints };
        string transaction = (await lease.Client.BeginTransactionAsync(ct)).TransactionId;
        bool committing = false;
        var counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        try
        {
            var snapshots = await ReadSnapshotsAsync(lease.Client, transaction, GenerationProfile.FromJson(prepared.Plan.ProfileJson), ct);
            foreach (var snapshot in snapshots)
                if (!prepared.Plan.SnapshotFingerprints.TryGetValue(snapshot.Schema.TableName, out string? fingerprint) || snapshot.Fingerprint != fingerprint)
                    throw new InvalidOperationException($"{snapshot.Schema.TableName}: schema, identity state, row count, triggers, or existing keys changed. Preview again.");
            long processed = 0, total = prepared.Plan.Counts.Values.Sum(v => (long)v);
            foreach (string table in prepared.Plan.Order)
            {
                var schema = prepared.Plan.Schema(table);
                string[] columns = schema.Columns.Where(c => !c.IsRowVersion).Select(c => c.Name).ToArray();
                var batch = new List<IReadOnlyDictionary<string, object?>>(limits.BatchSize);
                int overhead = Encoding.UTF8.GetByteCount(GenerationValues.InsertPrefix(table, columns)) + 1;
                int bytes = overhead;
                foreach (var row in prepared.Plan.Rows(table, ct: ct))
                {
                    ct.ThrowIfCancellationRequested();
                    if (!lease.IsCurrent) throw new OperationCanceledException("The database changed during generation.");
                    int rowBytes = Encoding.UTF8.GetByteCount(GenerationValues.RowLiteral(columns, row));
                    if (rowBytes + overhead > limits.MaxStatementBytes) throw new InvalidOperationException($"{table}: one row exceeds the insert statement limit.");
                    if (batch.Count > 0 && (batch.Count >= limits.BatchSize || bytes + rowBytes + 2 > limits.MaxStatementBytes))
                    {
                        await WriteBatchAsync(lease.Client, transaction, table, columns, batch, ct);
                        processed += batch.Count; batch.Clear(); bytes = overhead;
                        progress?.Report(new(table, processed, total, "Inserting (uncommitted)"));
                    }
                    bytes += rowBytes + (batch.Count > 0 ? 2 : 0); batch.Add(row);
                }
                if (batch.Count > 0)
                {
                    await WriteBatchAsync(lease.Client, transaction, table, columns, batch, ct);
                    processed += batch.Count;
                    progress?.Report(new(table, processed, total, "Inserting (uncommitted)"));
                }
                counts[table] = prepared.Plan.Counts[table];
                long after = await ScalarAsync(lease.Client, transaction, $"SELECT COUNT(*) FROM {SqlIdentifierRules.Quote(table)};", ct);
                long before = snapshots.Single(s => Same(s.Schema.TableName, table)).RowCount;
                if (after != before + counts[table]) throw new InvalidOperationException($"{table}: inserted row count did not match the plan.");
            }
            var finalSnapshots = await ReadSnapshotsAsync(lease.Client, transaction, GenerationProfile.FromJson(prepared.Plan.ProfileJson), ct, afterInsert: true);
            foreach (var snapshot in finalSnapshots.Where(s => s.KeyProjection.Count > 0))
            {
                string Key(IReadOnlyDictionary<string, object?> row) => JsonSerializer.Serialize(snapshot.KeyProjection
                    .Select(c => GenerationValues.Literal(GenerationValues.ToDbValue(row[c]))).ToArray());
                var expected = prepared.Plan.ExpectedFinalKeys(snapshot.Schema.TableName).Select(Key).Order(StringComparer.Ordinal);
                var actual = snapshot.ExistingKeys.Select(Key).Order(StringComparer.Ordinal);
                if (!expected.SequenceEqual(actual)) throw new InvalidOperationException($"{snapshot.Schema.TableName}: persisted keys and relationships differ from the validated plan.");
            }
            if (!lease.IsCurrent) throw new OperationCanceledException("The database changed during generation.");
            ct.ThrowIfCancellationRequested();
            committing = true;
            progress?.Report(new("", total, total, "Committing"));
            await lease.Client.CommitTransactionAsync(transaction, CancellationToken.None);
            string? refreshWarning = null;
            try { changes.NotifyChanged(); }
            catch (Exception refreshError) { refreshWarning = $"Rows committed. Refresh affected views manually: {refreshError.Message}"; }
            return Receipt("Committed", counts, refreshWarning);
        }
        catch (Exception error)
        {
            if (committing)
                return Receipt("Unknown", new Dictionary<string, int>(), $"Commit outcome is unknown. Inspect the database before starting another run. {error.Message}");
            try { await lease.Client.RollbackTransactionAsync(transaction, CancellationToken.None); }
            catch (Exception rollbackError)
            {
                return Receipt("Unknown", new Dictionary<string, int>(), $"Rollback could not be confirmed. {error.Message} {rollbackError.Message}");
            }
            return Receipt("Rolled back", new Dictionary<string, int>(), error is OperationCanceledException ? "Generation was cancelled; all inserts were rolled back." : error.Message);
        }
    }

    private void RequireSupported()
    {
        if (!IsSupported) throw new InvalidOperationException("Test data generation currently requires a direct connection to one database. Sharded and remote connections are not enabled.");
    }

    private async Task<IReadOnlyList<GenerationTableSnapshot>> ReadSnapshotsAsync(ICSharpDbClient client, string transaction, GenerationProfile profile, CancellationToken ct, bool afterInsert = false)
    {
        if (client is not ICSharpDbTransactionalSnapshotReader { SupportsTransactionalSnapshotReads: true } reader)
            throw new InvalidOperationException("This connection cannot read a transaction-consistent schema snapshot.");
        var names = profile.Tables.Select(t => t.TableName).Concat(profile.Tables.SelectMany(t => t.Relationships.Select(r => r.ParentTable)))
            .Distinct(StringComparer.OrdinalIgnoreCase).Order(StringComparer.OrdinalIgnoreCase).ToArray();
        var results = new List<GenerationTableSnapshot>();
        long totalKeyBytes = 0;
        foreach (string name in names)
        {
            ct.ThrowIfCancellationRequested();
            if (!GenerationPlan.IsUserTable(name)) throw new InvalidOperationException("Internal and system tables are excluded from test data generation.");
            var source = await reader.ReadTableSnapshotAsync(transaction, name, ct)
                ?? throw new InvalidOperationException($"'{name}' is not a writable user table.");
            var schema = MapSchema(source.Schema);
            var metadata = new GenerationTableSnapshot { Schema = schema, Indexes = source.Indexes.Select(MapIndex).ToArray() };
            var needed = GenerationPlan.KeyColumns(metadata).Concat(profile.Tables.SelectMany(t => t.Relationships)
                .Where(r => Same(r.ParentTable, name)).SelectMany(r => r.ParentColumns))
                .Concat(profile.Tables.Where(t => Same(t.TableName, name)).SelectMany(t => t.Relationships).SelectMany(r => r.Columns))
                .Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
            if (needed.Any(c => schema.GetColumnIndex(c) < 0)) throw new InvalidOperationException($"{name}: a referenced key column no longer exists.");
            long count = await ScalarAsync(client, transaction, $"SELECT COUNT(*) FROM {SqlIdentifierRules.Quote(name)};", ct);
            var keys = new List<IReadOnlyDictionary<string, object?>>();
            string RawKey(IReadOnlyDictionary<string, object?> row) => JsonSerializer.Serialize(needed
                .Select(column => GenerationValues.Literal(GenerationValues.ToDbValue(row[column]))).ToArray());
            if (needed.Length > 0)
            {
                if (count > limits.MaxExistingKeyRows + (afterInsert ? limits.MaxRows : 0)) throw new InvalidOperationException($"{name}: existing key pool exceeds the {limits.MaxExistingKeyRows:N0}-row limit.");
                string sql = $"SELECT {string.Join(", ", needed.Select(SqlIdentifierRules.Quote))} FROM {SqlIdentifierRules.Quote(name)};";
                await using var cursor = await reader.TryOpenForwardOnlyQueryCursorAsync(transaction, sql, ct)
                    ?? throw new InvalidOperationException("The connection cannot stream existing keys.");
                while (true)
                {
                    var batch = await cursor.ReadNextAsync(256, ct);
                    if (batch.Count == 0) break;
                    foreach (var row in batch)
                    {
                        totalKeyBytes += GenerationValues.Size(row);
                        if (totalKeyBytes > limits.MaxKeyBytes) throw new InvalidOperationException("Existing keys exceed the configured memory limit.");
                        keys.Add(needed.Select((column, i) => (column, value: row[i])).ToDictionary(p => p.column, p => p.value, StringComparer.OrdinalIgnoreCase));
                    }
                }
                keys.Sort((a, b) => string.CompareOrdinal(RawKey(a), RawKey(b)));
            }
            string[] triggers = source.Triggers.Where(t => t.Event == ClientModels.TriggerEvent.Insert).Select(t => t.TriggerName).ToArray();
            using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            hash.AppendData(JsonSerializer.SerializeToUtf8Bytes(new { source.Schema, source.Indexes, source.Triggers, Count = count, Columns = needed }));
            foreach (var row in keys) hash.AppendData(Encoding.UTF8.GetBytes(RawKey(row) + "\n"));
            results.Add(new GenerationTableSnapshot
            {
                Schema = schema, Indexes = metadata.Indexes, RowCount = count, ExistingKeys = keys,
                KeyProjection = needed,
                InsertTriggers = triggers, Fingerprint = Convert.ToHexString(hash.GetHashAndReset()),
            });
        }
        return results;
    }

    private async Task WriteBatchAsync(ICSharpDbClient client, string transaction, string table, string[] columns,
        IReadOnlyList<IReadOnlyDictionary<string, object?>> rows, CancellationToken ct)
    {
        if (columns.Length == 0)
        {
            foreach (var _ in rows) await CheckedAsync(client, transaction, $"INSERT INTO {SqlIdentifierRules.Quote(table)} DEFAULT VALUES;", ct);
            return;
        }
        string sql = GenerationValues.InsertPrefix(table, columns)
            + string.Join(", ", rows.Select(row => GenerationValues.RowLiteral(columns, row))) + ";";
        if (Encoding.UTF8.GetByteCount(sql) > limits.MaxStatementBytes) throw new InvalidOperationException($"{table}: insert statement exceeds the configured byte limit.");
        var result = await CheckedAsync(client, transaction, sql, ct);
        if (result.RowsAffected != rows.Count) throw new InvalidOperationException($"{table}: the database did not acknowledge every row in the batch.");
    }

    private static async Task<ClientModels.SqlExecutionResult> CheckedAsync(ICSharpDbClient client, string transaction, string sql, CancellationToken ct)
    {
        var result = await client.ExecuteInTransactionAsync(transaction, sql, ct);
        if (!string.IsNullOrEmpty(result.Error)) throw new InvalidOperationException(result.Error);
        return result;
    }
    private static async Task<long> ScalarAsync(ICSharpDbClient client, string transaction, string sql, CancellationToken ct)
    {
        var result = await CheckedAsync(client, transaction, sql, ct);
        if (result.Rows is not { Count: 1 } || result.Rows[0].Length != 1) throw new InvalidOperationException("Expected one scalar result.");
        return Convert.ToInt64(result.Rows[0][0], CultureInfo.InvariantCulture);
    }
    private static bool Same(string a, string b) => string.Equals(a, b, StringComparison.OrdinalIgnoreCase);

    public static TableSchema MapSchema(ClientModels.TableSchema source) => new()
    {
        SchemaId = source.SchemaId, TableName = source.TableName, NextRowId = source.NextRowId,
        Columns = source.Columns.Select(c => new ColumnDefinition
        {
            SchemaId = c.SchemaId, Name = c.Name, Type = Enum.Parse<DbType>(c.Type.ToString()), Nullable = c.Nullable,
            DeclaredType = c.DeclaredType is null ? null : new SqlTypeDescriptor(Enum.Parse<SqlTypeKind>(c.DeclaredType.Kind.ToString()),
                c.DeclaredType.Length, c.DeclaredType.Precision, c.DeclaredType.Scale, c.DeclaredType.FractionalSecondsPrecision),
            IsPrimaryKey = c.IsPrimaryKey, IsIdentity = c.IsIdentity, IsRowVersion = c.IsRowVersion, DefaultSql = c.DefaultSql, Collation = c.Collation,
        }).ToArray(),
        KeyConstraints = source.KeyConstraints.Select(k => new KeyConstraintDefinition { SchemaId = k.SchemaId, ConstraintName = k.ConstraintName,
            Kind = Enum.Parse<KeyConstraintKind>(k.Kind.ToString()), Columns = k.Columns, BackingIndexName = k.BackingIndexName }).ToArray(),
        CheckConstraints = source.CheckConstraints.Select(c => new CheckConstraintDefinition { SchemaId = c.SchemaId, ConstraintName = c.ConstraintName, ExpressionSql = c.ExpressionSql, ColumnName = c.ColumnName }).ToArray(),
        ForeignKeys = source.ForeignKeys.Select(f => new ForeignKeyDefinition
        {
            SchemaId = f.SchemaId, ConstraintName = f.ConstraintName, ColumnName = f.ColumnName, ColumnNames = f.ColumnNames,
            ReferencedTableName = f.ReferencedTableName, ReferencedColumnName = f.ReferencedColumnName, ReferencedColumnNames = f.ReferencedColumnNames,
            ColumnSchemaIds = f.ColumnSchemaIds, ReferencedColumnSchemaIds = f.ReferencedColumnSchemaIds, ReferencedTableSchemaId = f.ReferencedTableSchemaId,
            ReferencedKeySchemaId = f.ReferencedKeySchemaId, SupportingIndexName = f.SupportingIndexName,
            OnDelete = Enum.Parse<ForeignKeyOnDeleteAction>(f.OnDelete.ToString()), OnUpdate = Enum.Parse<ForeignKeyOnDeleteAction>(f.OnUpdate.ToString()),
        }).ToArray(),
    };
    public static IndexSchema MapIndex(ClientModels.IndexSchema source) => new()
    {
        IndexName = source.IndexName, TableName = source.TableName, Columns = source.Columns,
        IsUnique = source.IsUnique, ColumnCollations = source.ColumnCollations,
    };
}
