namespace CSharpDB.DataGen;

/// <summary>Generation inputs shared by Studio and the developer CLI. Contains no file destinations.</summary>
public sealed class GenerationOptions
{
    public int Seed { get; init; } = 42;
    public long RowCount { get; init; }
    public string DatasetLabel { get; init; } = "studio";
    public DateTime ReferenceUtc { get; init; } = new(2026, 3, 28, 0, 0, 0, DateTimeKind.Utc);
    public string Locale { get; init; } = "en";
    public int BatchSize { get; init; } = 1000;
    public bool DirectLoad { get; init; }
    public double NullRate { get; init; } = 0.05;
    public double HotKeyRate { get; init; } = 0.20;
    public double RecentRate { get; init; } = 0.80;
    public int AvgDocSizeBytes { get; init; } = 1024;
    public int MaxDirectDocumentSizeBytes { get; init; } = 2048;
    public int TenantCount { get; init; } = 250;
    public int DeviceCount { get; init; } = 100_000;
    public int OrdersPerCustomer { get; init; } = 5;
    public int ItemsPerOrder { get; init; } = 4;
    public IReadOnlyDictionary<string, object?> ExtraOptions { get; init; } = new Dictionary<string, object?>();
}
