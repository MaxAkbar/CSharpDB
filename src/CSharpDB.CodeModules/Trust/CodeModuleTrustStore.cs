using System.Text.Json;
using System.Text.Json.Serialization;

namespace CSharpDB.CodeModules.Trust;

public interface ICodeModuleTrustStore
{
    Task<CodeModuleTrustState> GetTrustStateAsync(
        string databasePath,
        string moduleSetHash,
        CancellationToken ct = default);

    Task TrustAsync(
        string databasePath,
        string moduleSetHash,
        CancellationToken ct = default);
}

public sealed class FileCodeModuleTrustStore : ICodeModuleTrustStore
{
    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = true,
    };

    private readonly string _storePath;
    private readonly SemaphoreSlim _lock = new(1, 1);

    public FileCodeModuleTrustStore()
        : this(DefaultStorePath())
    {
    }

    public FileCodeModuleTrustStore(string storePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(storePath);
        _storePath = storePath;
    }

    public async Task<CodeModuleTrustState> GetTrustStateAsync(
        string databasePath,
        string moduleSetHash,
        CancellationToken ct = default)
    {
        string normalizedPath = NormalizeDatabasePath(databasePath);
        string normalizedHash = NormalizeHash(moduleSetHash);
        await _lock.WaitAsync(ct);
        try
        {
            TrustStoreDocument document = await ReadAsync(ct);
            string key = BuildKey(normalizedPath, normalizedHash);
            return document.Entries.TryGetValue(key, out TrustEntry? entry)
                ? new CodeModuleTrustState(normalizedPath, normalizedHash, IsTrusted: true, entry.TrustedUtc)
                : new CodeModuleTrustState(normalizedPath, normalizedHash, IsTrusted: false);
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task TrustAsync(
        string databasePath,
        string moduleSetHash,
        CancellationToken ct = default)
    {
        string normalizedPath = NormalizeDatabasePath(databasePath);
        string normalizedHash = NormalizeHash(moduleSetHash);
        await _lock.WaitAsync(ct);
        try
        {
            TrustStoreDocument document = await ReadAsync(ct);
            document.Entries[BuildKey(normalizedPath, normalizedHash)] = new TrustEntry(
                normalizedPath,
                normalizedHash,
                DateTimeOffset.UtcNow);
            await WriteAsync(document, ct);
        }
        finally
        {
            _lock.Release();
        }
    }

    public static string NormalizeDatabasePath(string databasePath)
    {
        if (string.IsNullOrWhiteSpace(databasePath))
            return "<unknown>";

        try
        {
            return Path.GetFullPath(databasePath.Trim()).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar).ToUpperInvariant();
        }
        catch (Exception) when (databasePath.Length > 0)
        {
            return databasePath.Trim().ToUpperInvariant();
        }
    }

    private async Task<TrustStoreDocument> ReadAsync(CancellationToken ct)
    {
        if (!File.Exists(_storePath))
            return new TrustStoreDocument();

        await using FileStream stream = File.OpenRead(_storePath);
        return await JsonSerializer.DeserializeAsync<TrustStoreDocument>(stream, s_jsonOptions, ct)
            ?? new TrustStoreDocument();
    }

    private async Task WriteAsync(TrustStoreDocument document, CancellationToken ct)
    {
        string? directory = Path.GetDirectoryName(_storePath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        await using FileStream stream = File.Create(_storePath);
        await JsonSerializer.SerializeAsync(stream, document, s_jsonOptions, ct);
    }

    private static string BuildKey(string normalizedDatabasePath, string moduleSetHash)
        => $"{normalizedDatabasePath}|{moduleSetHash}";

    private static string NormalizeHash(string moduleSetHash)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(moduleSetHash);
        return moduleSetHash.Trim().ToLowerInvariant();
    }

    private static string DefaultStorePath()
    {
        string root = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (string.IsNullOrWhiteSpace(root))
            root = AppContext.BaseDirectory;

        return Path.Combine(root, "CSharpDB", "code-module-trust.json");
    }

    private sealed class TrustStoreDocument
    {
        public Dictionary<string, TrustEntry> Entries { get; init; } = new(StringComparer.Ordinal);
    }

    private sealed record TrustEntry(string DatabasePath, string ModuleSetHash, DateTimeOffset TrustedUtc);
}

public sealed class InMemoryCodeModuleTrustStore : ICodeModuleTrustStore
{
    private readonly Dictionary<string, DateTimeOffset> _trusted = new(StringComparer.Ordinal);

    public Task<CodeModuleTrustState> GetTrustStateAsync(
        string databasePath,
        string moduleSetHash,
        CancellationToken ct = default)
    {
        string normalizedPath = FileCodeModuleTrustStore.NormalizeDatabasePath(databasePath);
        string normalizedHash = moduleSetHash.Trim().ToLowerInvariant();
        string key = $"{normalizedPath}|{normalizedHash}";
        return Task.FromResult(_trusted.TryGetValue(key, out DateTimeOffset trustedUtc)
            ? new CodeModuleTrustState(normalizedPath, normalizedHash, IsTrusted: true, trustedUtc)
            : new CodeModuleTrustState(normalizedPath, normalizedHash, IsTrusted: false));
    }

    public Task TrustAsync(
        string databasePath,
        string moduleSetHash,
        CancellationToken ct = default)
    {
        string normalizedPath = FileCodeModuleTrustStore.NormalizeDatabasePath(databasePath);
        string normalizedHash = moduleSetHash.Trim().ToLowerInvariant();
        _trusted[$"{normalizedPath}|{normalizedHash}"] = DateTimeOffset.UtcNow;
        return Task.CompletedTask;
    }
}
