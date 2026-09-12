using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Primitives;

namespace CSharpDB.DataGeneration;

public sealed class GenerationProfile
{
    public int Version { get; set; } = 1;
    public string Algorithm { get; set; } = StableRandom.Version;
    public int Seed { get; set; } = 42;
    public DateTime ReferenceUtc { get; set; } = new(2026, 3, 28, 0, 0, 0, DateTimeKind.Utc);
    public string Locale { get; set; } = "en";
    public List<TableGenerationRule> Tables { get; set; } = [];

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true, Converters = { new JsonStringEnumConverter() },
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
    };
    public string ToJson() => JsonSerializer.Serialize(this, JsonOptions);
    [JsonIgnore] public string Hash => Convert.ToHexString(SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(ToJson())));
    public static GenerationProfile FromJson(string json)
    {
        if (json.Length > 1_000_000) throw new InvalidOperationException("Generation profiles are limited to 1 MB.");
        var profile = JsonSerializer.Deserialize<GenerationProfile>(json, JsonOptions)
            ?? throw new InvalidOperationException("The generation profile is empty.");
        profile.ValidateVersion();
        return profile;
    }
    public void ValidateVersion()
    {
        if (Version != 1 || Algorithm != StableRandom.Version)
            throw new InvalidOperationException("This profile uses a different generator version. Recreate it with the current version.");
        if (Locale != "en") throw new InvalidOperationException("This version supports the English provider catalog (en).");
        if (ReferenceUtc.Kind != DateTimeKind.Utc) throw new InvalidOperationException("The reference date must be UTC.");
        if (Tables is null || Tables.Count is < 1 or > 100) throw new InvalidOperationException("Select between 1 and 100 tables.");
        foreach (var table in Tables)
        {
            if (table is null || string.IsNullOrWhiteSpace(table.TableName) || table.Columns is null || table.Relationships is null
                || table.Columns.Any(c => c is null || string.IsNullOrWhiteSpace(c.ColumnName) || c.Constant is null
                    || c.Values is null || c.Values.Any(v => v is null) || c.Weights is null)
                || table.Relationships.Any(r => r is null || string.IsNullOrWhiteSpace(r.Name) || string.IsNullOrWhiteSpace(r.ParentTable)
                    || r.Columns is null || r.ParentColumns is null || r.Columns.Any(string.IsNullOrWhiteSpace) || r.ParentColumns.Any(string.IsNullOrWhiteSpace)))
                throw new InvalidOperationException("The profile contains an incomplete table, field, or relationship configuration.");
        }
    }
}

public sealed class TableGenerationRule
{
    public string TableName { get; set; } = "";
    public Guid SchemaId { get; set; }
    public int Rows { get; set; } = 100;
    public List<ColumnGenerationRule> Columns { get; set; } = [];
    public List<RelationshipGenerationRule> Relationships { get; set; } = [];
}

public enum FieldGenerator
{
    Sequence, Number, Boolean, FirstName, LastName, FullName, Email, Phone, Street,
    City, Region, PostalCode, Country, Company, Product, Text, Uuid, DateTime, Binary,
    Constant, ValueList, Default, RowVersion,
}
public enum FieldDistribution { Uniform, Weighted, Normal, Recent, HotKeys }
public enum ParentKeySource { Generated, Existing }

public sealed class ColumnGenerationRule
{
    public string ColumnName { get; set; } = "";
    public Guid SchemaId { get; set; }
    public FieldGenerator Generator { get; set; } = FieldGenerator.Text;
    public FieldDistribution Distribution { get; set; }
    public decimal Minimum { get; set; } = 1;
    public decimal Maximum { get; set; } = 1000;
    public double Mean { get; set; } = 500;
    public double Deviation { get; set; } = 100;
    public double Probability { get; set; } = 0.8;
    public double NullRate { get; set; }
    public int Length { get; set; } = 32;
    public int Days { get; set; } = 365;
    public int RecentDays { get; set; } = 30;
    public string Constant { get; set; } = "";
    public List<string> Values { get; set; } = [];
    public List<double> Weights { get; set; } = [];
}

public sealed class RelationshipGenerationRule
{
    public string Name { get; set; } = "";
    public List<string> Columns { get; set; } = [];
    public string ParentTable { get; set; } = "";
    public List<string> ParentColumns { get; set; } = [];
    public ParentKeySource Source { get; set; } = ParentKeySource.Generated;
    public FieldDistribution Distribution { get; set; } = FieldDistribution.Uniform;
    public double HotKeyRate { get; set; } = 0.8;
    public double NullRate { get; set; }
}

public sealed class GenerationLimits
{
    public int MaxRows { get; init; } = 10_000;
    public long MaxGeneratedBytes { get; init; } = 32 * 1024 * 1024;
    public long MaxKeyBytes { get; init; } = 32 * 1024 * 1024;
    public int MaxExistingKeyRows { get; init; } = 100_000;
    public int MaxStatementBytes { get; init; } = 256 * 1024;
    public int BatchSize { get; init; } = 100;
    public int PreviewRows { get; init; } = 25;
    public int TimeoutSeconds { get; init; } = 120;
    public void Validate()
    {
        if (MaxRows is < 1 or > 1_000_000 || MaxExistingKeyRows is < 1 or > 1_000_000
            || MaxGeneratedBytes < 1024 || MaxKeyBytes < 1024 || MaxStatementBytes is < 1024 or > 4 * 1024 * 1024
            || BatchSize is < 1 or > 1000 || PreviewRows is < 1 or > 100 || TimeoutSeconds is < 1 or > 600)
            throw new InvalidOperationException("TestDataGeneration limits are outside the supported configuration ranges.");
    }
}

/// <summary>Only existing key columns are retained, never complete existing records.</summary>
public sealed class GenerationTableSnapshot
{
    public required TableSchema Schema { get; init; }
    public IReadOnlyList<IndexSchema> Indexes { get; init; } = [];
    public long RowCount { get; init; }
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> ExistingKeys { get; init; } = [];
    public IReadOnlyList<string> KeyProjection { get; init; } = [];
    public IReadOnlyList<string> InsertTriggers { get; init; } = [];
    public string Fingerprint { get; init; } = "";
}

public sealed record GenerationProgress(string Table, long Processed, long Total, string Stage);
public sealed record GenerationReceipt(string Status, int Seed, string ProfileHash, string Algorithm,
    DateTime ReferenceUtc, string Target, IReadOnlyDictionary<string, int> InsertedRows, TimeSpan Duration, string? Message = null)
{
    public IReadOnlyDictionary<string, int> RequestedRows { get; init; } = new Dictionary<string, int>();
    public IReadOnlyDictionary<string, string> SnapshotFingerprints { get; init; } = new Dictionary<string, string>();
}
