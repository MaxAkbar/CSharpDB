namespace CSharpDB.Data;

/// <summary>
/// Named embedded storage-engine presets exposed through the ADO.NET and EF Core providers.
/// </summary>
public enum CSharpDbStoragePreset
{
    DirectLookupOptimized = 0,
    DirectColdFileLookup = 1,
    HybridFileCache = 2,
    WriteOptimized = 3,
    LowLatencyDurableWrite = 4,
}
