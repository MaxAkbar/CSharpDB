using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.DataPrivacy;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Services;

public sealed class PrivacyAdminService(DatabaseClientHolder holder, DatabaseChangeService changes, PrivacyLimits limits)
{
    private readonly PrivacyService _privacy = new(limits);
    private readonly PrivacyPolicyStore _store = new();
    public bool IsSupported => !holder.SupportsShardAdmin && holder.SupportsTransactionalSnapshotReads && holder.SupportsExclusiveSessions;
    public PrivacyLimits Limits => limits;
    public async Task<(IReadOnlyList<Client.Models.TableSchema> Catalog, IReadOnlyList<PrivacyPolicy> Policies, IReadOnlyList<PrivacyReceipt> Runs, Guid? Pending)> LoadAsync(CancellationToken ct)
    {
        RequireSupported();
        await using var lease = holder.CaptureClient();
        // Engine catalog paths can synchronously wait on asynchronous storage reads.
        // Keep all database work off the Blazor dispatcher, including save and reload.
        var loaded = await Task.Run(async () =>
        {
            var catalog = new List<Client.Models.TableSchema>();
            foreach (string name in await lease.Client.GetTableNamesAsync(ct))
                if (!DbInternalTableRegistry.IsInternalTable(name) && await lease.Client.GetTableSchemaAsync(name, ct) is { } schema) catalog.Add(schema);
            var policies = await _store.ListAsync(lease.Client, ct);
            var runs = await _store.ListReceiptsAsync(lease.Client, ct);
            var pending = await _store.FindPendingRunAsync(lease.Client, ct) ?? _privacy.UnresolvedRun(lease.Client);
            return (catalog, policies, runs, pending);
        }, ct);
        Check(lease);
        return loaded;
    }
    public async Task<PrivacyPolicy> SaveAsync(PrivacyPolicy policy, CancellationToken ct)
    {
        RequireSupported(); await using var lease = holder.CaptureClient();
        var saved = await Task.Run(() => _store.SaveAsync(lease.Client, policy, ct), ct); Check(lease); return saved;
    }
    public async Task<PrivacyPreview> PreviewAsync(PrivacyPolicy policy, IProgress<PrivacyProgress> progress, CancellationToken ct)
    {
        RequireSupported(); await using var lease = holder.CaptureClient();
        var preview = await Task.Run(() => _privacy.PreviewAsync(lease.Client, policy, progress: progress, ct: ct), ct);
        if (!lease.IsCurrent) { preview.Dispose(); Check(lease); }
        return preview;
    }
    public async Task<PrivacyReceipt> ApplyAsync(PrivacyPreview preview, PrivacyPolicy policy, IProgress<PrivacyProgress> progress, CancellationToken ct)
    {
        RequireSupported(); await using var lease = holder.CaptureClient();
        var receipt = await Task.Run(() => _privacy.ApplyAsync(lease.Client, preview, policy, progress, () => lease.IsCurrent, ct), ct);
        if (receipt.Status == "Committed")
            try { changes.NotifyChanged(); } catch { return receipt with { Message = "Changes committed. Refresh affected views manually." }; }
        return receipt;
    }
    public async Task<PrivacyReceipt?> ReconcileAsync(Guid id, CancellationToken ct)
    {
        RequireSupported(); await using var lease = holder.CaptureClient();
        var receipt = await Task.Run(() => _privacy.ReconcileAsync(lease.Client, id, ct), ct); Check(lease); return receipt;
    }
    private static void Check(DatabaseClientHolder.ClientLease lease)
    { if (!lease.IsCurrent) throw new PrivacyException("The database changed. Reload privacy policies."); }
    private void RequireSupported()
    { if (!IsSupported) throw new PrivacyException("Privacy & Retention requires a direct connection to one database."); }
}
