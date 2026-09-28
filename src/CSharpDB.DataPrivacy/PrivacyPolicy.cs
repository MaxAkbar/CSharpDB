using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CSharpDB.DataPrivacy;

public enum PrivacyConditionKind { All, Any, Compare, IsNull, IsNotNull, OlderThan, WithinLast, Exists, NotExists }
public enum PrivacyComparison { Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual }
public enum PrivacyDateEncoding { Iso8601, UnixSeconds, UnixMilliseconds, ExactFormat }
public enum PrivacyMaskKind { Erase, Constant, Anonymous, Partial, Email }

public sealed class PrivacyPolicy
{
    public int Version { get; set; } = 1;
    public Guid Id { get; set; } = Guid.NewGuid();
    public long Revision { get; set; }
    public string Name { get; set; } = "New privacy policy";
    public string RootTable { get; set; } = "";
    public PrivacyCondition Eligibility { get; set; } = new();
    public List<PrivacyRelationship> Relationships { get; set; } = [];
    public List<PrivacyTarget> Targets { get; set; } = [];
    [JsonIgnore] public string Hash => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(ToJson())));
    public string ToJson() => JsonSerializer.Serialize(this, JsonOptions);
    public static PrivacyPolicy FromJson(string json)
    {
        if (Encoding.UTF8.GetByteCount(json) > 1024 * 1024) throw new PrivacyException("Policy exceeds 1 MiB.");
        try
        {
            var policy = JsonSerializer.Deserialize<PrivacyPolicy>(json, JsonOptions) ?? throw new JsonException();
            if (policy.Version != 1 || policy.Id == Guid.Empty || policy.Revision < 0)
                throw new PrivacyException("Unsupported policy version or identity.");
            if (policy.Name is null || policy.RootTable is null || policy.Eligibility is null || policy.Relationships is null || policy.Targets is null
                || policy.Relationships.Any(r => r is null || string.IsNullOrEmpty(r.Id) || r.SourceTable is null || r.TargetTable is null || r.SourceColumns is null || r.TargetColumns is null || r.SourceColumns.Any(c => c is null) || r.TargetColumns.Any(c => c is null))
                || policy.Targets.Any(t => t is null || t.Table is null || t.RelationshipPath is null || t.RelationshipPath.Any(p => p is null) || t.Columns is null || t.Columns.Any(c => c is null || c.Column is null || c.Replacement is null)))
                throw new PrivacyException("Policy fields and lists cannot be null.");
            void CheckCondition(PrivacyCondition condition)
            {
                if (condition.Children is null || condition.Children.Any(c => c is null) || condition.Column is null || condition.Value is null || condition.RelationshipId is null || condition.DateFormat is null || condition.TimeZoneId is null)
                    throw new PrivacyException("Condition fields and lists cannot be null.");
                foreach (var child in condition.Children) CheckCondition(child);
            }
            CheckCondition(policy.Eligibility);
            return policy;
        }
        catch (JsonException) { throw new PrivacyException("Invalid privacy policy JSON."); }
    }
    public PrivacyPolicy Duplicate()
    {
        var copy = FromJson(ToJson()); copy.Id = Guid.NewGuid(); copy.Revision = 0; copy.Name += " (copy)"; return copy;
    }
    public static JsonSerializerOptions JsonOptions { get; } = new()
    {
        WriteIndented = true, MaxDepth = 32, UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
        Converters = { new JsonStringEnumConverter() }
    };
}

public sealed class PrivacyCondition
{
    public PrivacyConditionKind Kind { get; set; } = PrivacyConditionKind.All;
    public List<PrivacyCondition> Children { get; set; } = [];
    public string Column { get; set; } = "";
    public PrivacyComparison Comparison { get; set; }
    public string Value { get; set; } = "";
    public int Days { get; set; } = 365;
    public PrivacyDateEncoding DateEncoding { get; set; }
    public string DateFormat { get; set; } = "yyyy-MM-dd";
    public string TimeZoneId { get; set; } = "UTC";
    public string RelationshipId { get; set; } = "";
}

/// <summary>A directed mapping; both directions can be configured explicitly.</summary>
public sealed class PrivacyRelationship
{
    public string Id { get; set; } = Guid.NewGuid().ToString("N");
    public string SourceTable { get; set; } = "";
    public List<string> SourceColumns { get; set; } = [];
    public string TargetTable { get; set; } = "";
    public List<string> TargetColumns { get; set; } = [];
}

public sealed class PrivacyTarget
{
    public string Table { get; set; } = "";
    public List<string> RelationshipPath { get; set; } = [];
    public List<PrivacyColumnMask> Columns { get; set; } = [];
}

public sealed class PrivacyColumnMask
{
    public string Column { get; set; } = "";
    public PrivacyMaskKind Kind { get; set; }
    public string Replacement { get; set; } = "Anonymous";
    public int KeepPrefix { get; set; }
    public int KeepSuffix { get; set; }
}

public sealed class PrivacyLimits
{
    public int PreviewRows { get; init; } = 25;
    public int MaxAffectedRows { get; init; } = 10_000;
    public int MaxEvaluatedRows { get; init; } = 100_000;
    public long MaxPreparedBytes { get; init; } = 32 * 1024 * 1024;
    public int TimeoutSeconds { get; init; } = 120;
    public int BatchSize { get; init; } = 100;
    public int MaxStatementBytes { get; init; } = 256 * 1024;
    internal void Validate()
    {
        if (PreviewRows is < 1 or > 100 || MaxAffectedRows < 1 || MaxEvaluatedRows < 1 || MaxPreparedBytes < 1024
            || TimeoutSeconds is < 1 or > 600 || BatchSize is < 1 or > 1000 || MaxStatementBytes < 1024)
            throw new PrivacyException("Invalid privacy execution limits.");
    }
}

public sealed record PrivacySample(IReadOnlyDictionary<string, string> Before, IReadOnlyDictionary<string, string> After);
public sealed record PrivacyTablePreview(string Table, int EligibleRows, int ChangedRows, IReadOnlyList<string> PreservedColumns, IReadOnlyList<PrivacySample> Samples);
public sealed record PrivacyProgress(string Stage, int Processed, int Total);
public sealed record PrivacyValidation(bool IsValid, IReadOnlyList<string> Issues);
public sealed record PrivacyReceipt(Guid RunId, Guid PolicyId, long PolicyRevision, DateTimeOffset ReferenceUtc,
    DateTimeOffset CompletedUtc, string Status, IReadOnlyDictionary<string, int> ChangedRows, string? Message = null);
public sealed class PrivacyException(string message) : Exception(message);

/// <summary>In-memory, non-serializable authorization to apply one reviewed snapshot.</summary>
public sealed class PrivacyPreview : IDisposable
{
    internal PrivacyPreview() { }
    internal PrivacyPolicy Policy { get; init; } = null!;
    internal object ClientIdentity { get; init; } = null!;
    internal string Fingerprint { get; init; } = "";
    internal List<PrivacyChange> Changes { get; } = [];
    internal bool Consumed { get; set; }
    internal bool Uncertain { get; set; }
    public Guid RunId { get; } = Guid.CreateVersion7();
    public string PolicyHash => Policy.Hash;
    public DateTimeOffset ReferenceUtc { get; internal init; }
    public int EligibleRecords { get; internal set; }
    [JsonIgnore] public IReadOnlyList<PrivacyTablePreview> Tables { get; internal set; } = [];
    public IReadOnlyList<string> Warnings { get; internal set; } = [];
    public void Dispose() { Consumed = true; Changes.Clear(); Tables = []; }
}

internal sealed record PrivacyChange(string Table, string Identity, Dictionary<string, object?> Before, Dictionary<string, object?> After);
