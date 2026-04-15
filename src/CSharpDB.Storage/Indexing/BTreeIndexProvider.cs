namespace CSharpDB.Storage.Indexing;

/// <summary>
/// Default index provider that maps indexes to BTree-backed stores.
/// </summary>
public sealed class BTreeIndexProvider : IIndexProvider
{
    public IIndexStore CreateIndexStore(Pager pager, uint rootPageId, string logicalName) =>
        new BTreeIndexStore(new BTree(pager, rootPageId), logicalName);
}
