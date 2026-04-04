using CSharpDB.Storage.BTrees;

namespace CSharpDB.Engine;

internal static class BTreeCopyUtility
{
    public static ValueTask<long> CopyAsync(
        BTree sourceTree,
        BTree destinationTree,
        CancellationToken ct = default)
    {
        return CopyAsync(
            sourceTree,
            destinationTree,
            static payload => payload,
            ct);
    }

    public static async ValueTask<long> CopyAsync(
        BTree sourceTree,
        BTree destinationTree,
        Func<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>> payloadTransform,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(sourceTree);
        ArgumentNullException.ThrowIfNull(destinationTree);
        ArgumentNullException.ThrowIfNull(payloadTransform);

        var cursor = sourceTree.CreateCursor();
        long count = 0;
        while (await cursor.MoveNextAsync(ct))
        {
            await destinationTree.InsertAsync(cursor.CurrentKey, payloadTransform(cursor.CurrentValue), ct);
            count++;
        }

        return count;
    }
}
