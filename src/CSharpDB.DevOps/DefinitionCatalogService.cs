using System.Security.Cryptography;
using System.Text;
using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.DevOps;

public static class DefinitionCatalogService
{
    public static async Task<DefinitionCatalogSnapshot> ReadAsync(ICSharpDbClient client,
        IProgress<int>? progress = null, CancellationToken ct = default)
    {
        if (client is not ICSharpDbDefinitionCatalogReader reader)
            throw new NotSupportedException("This connection does not support definition inspection. Upgrade the server.");
        var fragments = new List<DefinitionCatalogRecord>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        string? token = null, version = null;
        DefinitionCatalogPage page;
        do
        {
            ct.ThrowIfCancellationRequested();
            page = await reader.ReadDefinitionCatalogAsync(token, ct: ct);
            version ??= page.CatalogVersion;
            if (version != page.CatalogVersion) throw new InvalidOperationException("Definitions changed during loading. Refresh the catalog.");
            fragments.AddRange(page.Records);
            progress?.Report(fragments.Count);
            token = page.ContinuationToken;
            if (token is not null && !seen.Add(token)) throw new InvalidOperationException("The server repeated a definition catalog page.");
        } while (token is not null);
        var records = new List<DefinitionCatalogRecord>();
        foreach (var group in fragments.GroupBy(r => r.Id, StringComparer.Ordinal))
        {
            ct.ThrowIfCancellationRequested();
            var parts = group.OrderBy(r => r.PartIndex).ToArray();
            var first = parts[0];
            if (parts.Length != first.PartCount || parts.Where((r, i) => r.PartIndex != i || r.PartCount != first.PartCount || r.SourceHash != first.SourceHash
                || r.Kind != first.Kind || r.Name != first.Name || r.OwnerName != first.OwnerName || r.Format != first.Format || r.MetadataJson != first.MetadataJson || r.IsEnabled != first.IsEnabled).Any())
                throw new InvalidOperationException($"Incomplete definition: {first.Name}. Refresh the catalog.");
            string source = string.Concat(parts.Select(r => r.Source));
            string hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(source)));
            if (first.SourceHash.Length > 0 && hash != first.SourceHash) throw new InvalidOperationException($"Definition checksum mismatch: {first.Name}.");
            records.Add(first with { Source = source, SourceHash = hash, PartIndex = 0, PartCount = 1 });
        }
        return new(version!, page.CapturedUtc, records, page.Diagnostics);
    }
}
