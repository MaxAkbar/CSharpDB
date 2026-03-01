namespace CSharpDB.Storage.Indexing;

/// <summary>
/// Optional capability interface for index stores backed by a B+tree.
/// </summary>
public interface IBTreeIndexStore
{
    BTree Tree { get; }
}
