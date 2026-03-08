using System.Collections.Concurrent;
using System.Runtime.InteropServices;

namespace CSharpDB.Native;

/// <summary>
/// Manages unmanaged blob pointers returned to foreign callers.
/// Blob data for the current row is pinned and freed on the next row advance or result free.
/// </summary>
internal static class BlobCache
{
    private static readonly ConcurrentDictionary<(IntPtr, int), GCHandle> s_pinnedBlobs = new();

    /// <summary>
    /// Pin the blob byte array and return a pointer to its data.
    /// Frees any previously pinned blob for the same (result, column).
    /// </summary>
    public static IntPtr SetCurrentRowBlob(IntPtr resultHandle, int columnIndex, byte[] data)
    {
        var key = (resultHandle, columnIndex);

        // Free previous pinned blob for this slot
        if (s_pinnedBlobs.TryRemove(key, out var oldHandle))
            oldHandle.Free();

        var pinned = GCHandle.Alloc(data, GCHandleType.Pinned);
        s_pinnedBlobs[key] = pinned;
        return pinned.AddrOfPinnedObject();
    }

    /// <summary>
    /// Free all pinned blobs associated with a result handle.
    /// </summary>
    public static void Remove(IntPtr resultHandle)
    {
        foreach (var key in s_pinnedBlobs.Keys)
        {
            if (key.Item1 == resultHandle && s_pinnedBlobs.TryRemove(key, out var handle))
                handle.Free();
        }
    }
}
