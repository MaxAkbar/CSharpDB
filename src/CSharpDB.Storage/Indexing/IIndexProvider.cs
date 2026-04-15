namespace CSharpDB.Storage.Indexing;

/// <summary>
/// Factory for index-store instances.
/// </summary>
public interface IIndexProvider
{
    IIndexStore CreateIndexStore(Pager pager, uint rootPageId, string logicalName);
}
