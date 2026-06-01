using CSharpDB.Engine;

namespace CSharpDB.Client;

public sealed class CSharpDbRouteContext
{
    public required string Keyspace { get; init; }
    public required string Key { get; init; }
}

public sealed class CSharpDbShardingOptions
{
    public bool Enabled { get; set; }
    public string Keyspace { get; set; } = "default";
    public int MapVersion { get; set; } = 1;
    public int VirtualBucketCount { get; set; } = 4096;
    public CSharpDbShardDefinition[] Shards { get; set; } = [];
    public CSharpDbShardBucketRange[] BucketRanges { get; set; } = [];
    public Dictionary<string, string> ExactKeyPins { get; set; } = new(StringComparer.Ordinal);
    public DatabaseOptions? DirectDatabaseOptions { get; set; }
    public HybridDatabaseOptions? HybridDatabaseOptions { get; set; }
}

public sealed class CSharpDbShardDefinition
{
    public required string ShardId { get; set; }
    public bool Enabled { get; set; } = true;
    public CSharpDbTransport? Transport { get; set; }
    public string? Endpoint { get; set; }
    public string? ConnectionString { get; set; }
    public string? DataSource { get; set; }
    public string? ApiKey { get; set; }
    public string? ApiKeyHeaderName { get; set; }
}

public sealed class CSharpDbShardMapSnapshot
{
    public required string Keyspace { get; init; }
    public int MapVersion { get; init; }
    public int VirtualBucketCount { get; init; }
    public List<CSharpDbShardDefinitionSnapshot> Shards { get; init; } = [];
    public List<CSharpDbShardBucketRange> BucketRanges { get; init; } = [];
    public Dictionary<string, string> ExactKeyPins { get; init; } = new(StringComparer.Ordinal);
    public List<CSharpDbShardDirectoryDefinition> Directories { get; init; } = [];
}

public sealed class CSharpDbShardDefinitionSnapshot
{
    public required string ShardId { get; init; }
    public bool Enabled { get; init; }
    public CSharpDbTransport? Transport { get; init; }
    public string? Endpoint { get; init; }
    public string? DataSource { get; init; }
    public bool HasConnectionString { get; init; }
    public bool HasApiKey { get; init; }
    public string? ApiKeyHeaderName { get; init; }
}

public sealed class CSharpDbShardBucketRange
{
    public int StartBucketInclusive { get; set; }
    public int EndBucketExclusive { get; set; }
    public required string ShardId { get; set; }
}

public sealed class CSharpDbShardResolution
{
    public required string Keyspace { get; init; }
    public required string Key { get; init; }
    public ulong Token { get; init; }
    public int Bucket { get; init; }
    public required string ShardId { get; init; }
    public int MapVersion { get; init; }
}

public sealed class CSharpDbShardStatus
{
    public required string ShardId { get; init; }
    public required string DataSource { get; init; }
    public bool Enabled { get; init; }
    public bool Healthy { get; init; }
    public string? Error { get; init; }
    public Models.DatabaseInfo? Info { get; init; }
}

public sealed class CSharpDbShardSqlExecutionResult
{
    public required string ShardId { get; init; }
    public Models.SqlExecutionResult? Result { get; init; }
    public string? Error { get; init; }
}

public sealed class CSharpDbShardDirectoryDefinition
{
    public required string DirectoryName { get; init; }
    public required string TargetKeyspace { get; init; }
    public string? Description { get; init; }
    public bool ReadOnly { get; init; } = true;
    public int EntryCount { get; init; }
}

public sealed class CSharpDbShardDirectoryEntry
{
    public required string DirectoryName { get; init; }
    public required string LookupKey { get; init; }
    public required string TargetKeyspace { get; init; }
    public required string RouteKey { get; init; }
    public required string ShardId { get; init; }
    public int MapVersion { get; init; }
    public required string State { get; init; }
}

public sealed class CSharpDbShardDirectoryResolution
{
    public required CSharpDbShardDirectoryEntry Entry { get; init; }
    public required CSharpDbShardResolution RouteResolution { get; init; }
}

public interface ICSharpDbShardAdminClient : IAsyncDisposable
{
    string DataSource { get; }

    Task<CSharpDbShardMapSnapshot> GetShardMapAsync(CancellationToken ct = default);
    Task<CSharpDbShardResolution> ResolveRouteAsync(CSharpDbRouteContext routeContext, CancellationToken ct = default);
    Task<IReadOnlyList<CSharpDbShardStatus>> GetShardStatusAsync(CancellationToken ct = default);
    Task<IReadOnlyList<CSharpDbShardSqlExecutionResult>> ExecuteSqlOnAllShardsAsync(string sql, CancellationToken ct = default);
}

public interface ICSharpDbRouteContextAccessor
{
    CSharpDbRouteContext? Current { get; set; }
}

public sealed class CSharpDbRouteContextAccessor : ICSharpDbRouteContextAccessor
{
    private readonly AsyncLocal<CSharpDbRouteContext?> _current = new();

    public CSharpDbRouteContext? Current
    {
        get => _current.Value;
        set => _current.Value = value;
    }
}

public static class CSharpDbRouteHeaderNames
{
    public const string Keyspace = "X-CSharpDB-Keyspace";
    public const string ShardKey = "X-CSharpDB-Shard-Key";

    public static string GrpcKeyspace => Keyspace.ToLowerInvariant();
    public static string GrpcShardKey => ShardKey.ToLowerInvariant();
}
