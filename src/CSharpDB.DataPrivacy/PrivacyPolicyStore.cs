using System.Globalization;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using static CSharpDB.DataPrivacy.PrivacyValues;

namespace CSharpDB.DataPrivacy;

public sealed class PrivacyPolicyStore
{
    public async Task<IReadOnlyList<PrivacyPolicy>> ListAsync(ICSharpDbClient client, CancellationToken ct = default)
    {
        if (!await ExistsAsync(client, "__privacy_policies", ct)) return [];
        var result = await CheckedAsync(client, null, "SELECT policy_json FROM __privacy_policies ORDER BY policy_id;", ct);
        return (result.Rows ?? []).Select(r => PrivacyPolicy.FromJson((string)r[0]!)).ToArray();
    }
    public async Task<PrivacyPolicy> SaveAsync(ICSharpDbClient client, PrivacyPolicy policy, CancellationToken ct = default)
    {
        var saved = PrivacyPolicy.FromJson(policy.ToJson());
        if (string.IsNullOrWhiteSpace(saved.Name) || saved.Name.Length > 200) throw new PrivacyException("Policy names must contain 1–200 characters.");
        long previous = saved.Revision; saved.Revision = checked(previous + 1);
        await using var exclusive = await PrivacyService.OpenExclusiveAsync(client, ct);
        client = exclusive.Client;
        string tx = (await client.BeginTransactionAsync(ct)).TransactionId;
        try
        {
            await EnsureAsync(client, tx, ct);
            string sql = previous == 0
                ? $"INSERT INTO __privacy_policies (policy_id, revision, policy_json) VALUES ({Literal(saved.Id.ToString())}, {saved.Revision}, {Literal(saved.ToJson())});"
                : $"UPDATE __privacy_policies SET revision = {saved.Revision}, policy_json = {Literal(saved.ToJson())} WHERE policy_id = {Literal(saved.Id.ToString())} AND revision = {previous};";
            var result = await CheckedAsync(client, tx, sql, ct);
            if (result.RowsAffected != 1) throw new PrivacyException("The saved policy changed. Reload it before saving your draft.");
            ct.ThrowIfCancellationRequested();
            await client.CommitTransactionAsync(tx, CancellationToken.None);
            return saved;
        }
        catch
        {
            try { await client.RollbackTransactionAsync(tx, CancellationToken.None); } catch { }
            throw new PrivacyException("Policy save was not confirmed. Reload saved policies before retrying.");
        }
    }
    internal static async Task AssertCurrentAsync(ICSharpDbClient client, string tx, PrivacyPolicy policy, CancellationToken ct)
    {
        var result = await CheckedAsync(client, tx, $"SELECT policy_json FROM __privacy_policies WHERE policy_id = {Literal(policy.Id.ToString())};", ct);
        if (result.Rows is not { Count: 1 } || PrivacyPolicy.FromJson((string)result.Rows[0][0]!).Hash != policy.Hash)
            throw new PrivacyException("The saved policy changed. Save or reload the policy and preview again.");
    }
    internal static async Task EnsureAsync(ICSharpDbClient client, string tx, CancellationToken ct)
    {
        await CheckedAsync(client, tx, "CREATE TABLE IF NOT EXISTS __privacy_policies (policy_id TEXT PRIMARY KEY, revision BIGINT NOT NULL, policy_json TEXT NOT NULL);", ct);
        await CheckedAsync(client, tx, "CREATE TABLE IF NOT EXISTS __privacy_runs (run_id TEXT PRIMARY KEY, status TEXT NOT NULL, receipt_json TEXT NOT NULL);", ct);
    }
    internal static async Task RecordAsync(ICSharpDbClient client, string tx, PrivacyReceipt receipt, CancellationToken ct)
    {
        var result = await CheckedAsync(client, tx, $"UPDATE __privacy_runs SET status = {Literal(receipt.Status)}, receipt_json = {Literal(JsonSerializer.Serialize(receipt))} WHERE run_id = {Literal(receipt.RunId.ToString())} AND status = 'Pending';", ct);
        if (result.RowsAffected != 1) throw new PrivacyException("The run is no longer pending. Reconcile its outcome before retrying.");
    }
    internal static async Task BeginRunAsync(ICSharpDbClient client, PrivacyReceipt pending, CancellationToken ct)
    {
        string tx = (await client.BeginTransactionAsync(ct)).TransactionId;
        try
        {
            if (await PendingAsync(client, tx, ct) is not null) throw new PrivacyException("A previous run requires reconciliation.");
            await CheckedAsync(client, tx, $"INSERT INTO __privacy_runs (run_id,status,receipt_json) VALUES ({Literal(pending.RunId.ToString())},'Pending',{Literal(JsonSerializer.Serialize(pending))});", ct);
            ct.ThrowIfCancellationRequested(); await client.CommitTransactionAsync(tx, CancellationToken.None);
        }
        catch
        {
            try { await client.RollbackTransactionAsync(tx, CancellationToken.None); } catch { }
            throw new PrivacyException("The run intent was not confirmed. Refresh and reconcile pending runs before retrying.");
        }
    }
    internal static async Task FinishRollbackAsync(ICSharpDbClient client, PrivacyReceipt receipt)
    {
        string tx = (await client.BeginTransactionAsync(CancellationToken.None)).TransactionId;
        try { await RecordAsync(client, tx, receipt, CancellationToken.None); await client.CommitTransactionAsync(tx, CancellationToken.None); }
        catch { try { await client.RollbackTransactionAsync(tx, CancellationToken.None); } catch { } throw; }
    }
    internal static async Task<Guid?> PendingAsync(ICSharpDbClient client, string? tx, CancellationToken ct)
    {
        var result = await CheckedAsync(client, tx, "SELECT run_id FROM __privacy_runs WHERE status = 'Pending' LIMIT 1;", ct);
        return result.Rows is { Count: > 0 } ? Guid.Parse((string)result.Rows[0][0]!) : null;
    }
    public async Task<Guid?> FindPendingRunAsync(ICSharpDbClient client, CancellationToken ct = default)
        => await ExistsAsync(client, "__privacy_runs", ct) ? await PendingAsync(client, null, ct) : null;
    internal static async Task<PrivacyReceipt?> ReadReceiptAsync(ICSharpDbClient client, string? tx, Guid runId, CancellationToken ct)
    {
        var result = await CheckedAsync(client, tx, $"SELECT receipt_json FROM __privacy_runs WHERE run_id = {Literal(runId.ToString())};", ct);
        return result.Rows is { Count: 1 } ? JsonSerializer.Deserialize<PrivacyReceipt>((string)result.Rows[0][0]!) : null;
    }
    public async Task<PrivacyReceipt?> FindReceiptAsync(ICSharpDbClient client, Guid runId, CancellationToken ct = default)
    {
        if (!await ExistsAsync(client, "__privacy_runs", ct)) return null;
        return await ReadReceiptAsync(client, null, runId, ct);
    }
    public async Task<IReadOnlyList<PrivacyReceipt>> ListReceiptsAsync(ICSharpDbClient client, CancellationToken ct = default)
    {
        if (!await ExistsAsync(client, "__privacy_runs", ct)) return [];
        var result = await CheckedAsync(client, null, "SELECT receipt_json FROM __privacy_runs WHERE status = 'Committed' ORDER BY run_id DESC LIMIT 100;", ct);
        return (result.Rows ?? []).Select(r => JsonSerializer.Deserialize<PrivacyReceipt>((string)r[0]!)!).OrderByDescending(r => r.CompletedUtc).ToArray();
    }
    private static async Task<bool> ExistsAsync(ICSharpDbClient client, string table, CancellationToken ct)
    {
        var result = await CheckedAsync(client, null, $"SELECT COUNT(*) FROM sys.internal_tables WHERE table_name = {Literal(table)};", ct);
        return Convert.ToInt64(result.Rows![0][0], CultureInfo.InvariantCulture) != 0;
    }
    internal static async Task<SqlExecutionResult> CheckedAsync(ICSharpDbClient client, string? tx, string sql, CancellationToken ct)
    {
        SqlExecutionResult result;
        try { result = tx is null ? await client.ExecuteSqlAsync(sql, ct) : await client.ExecuteInTransactionAsync(tx, sql, ct); }
        catch (OperationCanceledException) { throw; }
        catch { throw new PrivacyException("The database operation failed. No source values are included in this diagnostic."); }
        if (!string.IsNullOrEmpty(result.Error)) throw new PrivacyException("The database rejected the privacy operation. Check policy, schema and constraints.");
        return result;
    }
}
