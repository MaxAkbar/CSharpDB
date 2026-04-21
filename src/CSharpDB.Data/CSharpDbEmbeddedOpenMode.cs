namespace CSharpDB.Data;

/// <summary>
/// Embedded database open modes exposed through the ADO.NET and EF Core providers.
/// </summary>
public enum CSharpDbEmbeddedOpenMode
{
    Direct = 0,
    HybridIncrementalDurable = 1,
    HybridSnapshot = 2,
}
