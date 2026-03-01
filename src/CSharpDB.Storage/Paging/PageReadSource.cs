namespace CSharpDB.Storage.Paging;

/// <summary>
/// Source used to satisfy a page read.
/// </summary>
public enum PageReadSource
{
    Cache,
    WalSnapshot,
    WalLatest,
    StorageDevice,
}
