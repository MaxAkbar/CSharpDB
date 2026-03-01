namespace CSharpDB.Storage.Indexing;

/// <summary>
/// IIndexStore adapter backed by BTree.
/// </summary>
public sealed class BTreeIndexStore : IIndexStore
{
    private readonly BTree _tree;

    public BTreeIndexStore(BTree tree)
    {
        _tree = tree;
    }

    public uint RootPageId => _tree.RootPageId;

    public BTree Tree => _tree;

    public ValueTask<byte[]?> FindAsync(long key, CancellationToken ct = default) =>
        _tree.FindAsync(key, ct);

    public ValueTask InsertAsync(long key, ReadOnlyMemory<byte> payload, CancellationToken ct = default) =>
        _tree.InsertAsync(key, payload, ct);

    public ValueTask<bool> DeleteAsync(long key, CancellationToken ct = default) =>
        _tree.DeleteAsync(key, ct);

    public BTreeCursor CreateCursor() => _tree.CreateCursor();
}
