using System.Collections.Concurrent;
using CSharpDB.Admin.ImportExport.Models;

namespace CSharpDB.Admin.ImportExport.Services;

public sealed class TableArchiveDownloadStore : ITableArchiveDownloadStore
{
    private readonly ConcurrentDictionary<string, TableArchiveDownload> _downloads = new(StringComparer.Ordinal);

    public TableArchiveDownload Add(string path, string fileName)
    {
        CleanupExpiredDownloads();

        var token = Convert.ToHexString(Guid.NewGuid().ToByteArray()).ToLowerInvariant();
        var download = new TableArchiveDownload
        {
            Token = token,
            Path = path,
            FileName = fileName,
        };

        _downloads[token] = download;
        return download;
    }

    public bool TryTake(string token, out TableArchiveDownload download)
    {
        bool found = _downloads.TryRemove(token, out var value);
        download = value!;
        return found;
    }

    private void CleanupExpiredDownloads()
    {
        DateTimeOffset cutoff = DateTimeOffset.UtcNow.AddHours(-2);
        foreach (var pair in _downloads)
        {
            if (pair.Value.CreatedUtc >= cutoff)
                continue;

            if (_downloads.TryRemove(pair.Key, out var expired))
                TryDelete(expired.Path);
        }
    }

    internal static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch
        {
            // Best-effort cleanup for temporary download packages.
        }
    }
}
