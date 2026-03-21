namespace CSharpDB.Storage.Paging;

/// <summary>
/// Internal abstraction for reading a single database page from the main
/// database file. This is the seam used for future mmap-backed read paths.
/// </summary>
internal interface IPageReadProvider
{
    ValueTask<PageReadBuffer> ReadPageAsync(uint pageId, CancellationToken ct = default);

    ValueTask<byte[]> ReadOwnedPageAsync(uint pageId, CancellationToken ct = default);
}
