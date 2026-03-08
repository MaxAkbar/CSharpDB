using System.Collections.Concurrent;
using System.Runtime.InteropServices;

namespace CSharpDB.Native;

/// <summary>
/// Manages unmanaged UTF-8 string pointers returned to foreign callers.
/// Column name strings live as long as their result handle.
/// Row text values are overwritten on each row advance.
/// </summary>
internal static class StringCache
{
    // Column name pointers: keyed by (resultHandle, columnIndex)
    private static readonly ConcurrentDictionary<(IntPtr, int), IntPtr> s_columnNames = new();

    // Current-row text pointers: keyed by (resultHandle, columnIndex)
    private static readonly ConcurrentDictionary<(IntPtr, int), IntPtr> s_rowTexts = new();

    /// <summary>
    /// Get or allocate a UTF-8 pointer for a column name. Lives until Remove() is called.
    /// </summary>
    public static IntPtr GetOrAdd(IntPtr resultHandle, int columnIndex, string value)
    {
        var key = (resultHandle, columnIndex);
        return s_columnNames.GetOrAdd(key, _ => Utf8StringMemory.Allocate(value));
    }

    /// <summary>
    /// Set the text value for the current row at a given column. Frees the previous value.
    /// </summary>
    public static IntPtr SetCurrentRowText(IntPtr resultHandle, int columnIndex, string value)
    {
        var key = (resultHandle, columnIndex);
        var newPtr = Utf8StringMemory.Allocate(value);

        if (s_rowTexts.TryGetValue(key, out var oldPtr))
        {
            Marshal.FreeHGlobal(oldPtr);
            s_rowTexts[key] = newPtr;
        }
        else
        {
            s_rowTexts[key] = newPtr;
        }

        return newPtr;
    }

    /// <summary>
    /// Free all unmanaged strings associated with a result handle.
    /// </summary>
    public static void Remove(IntPtr resultHandle)
    {
        // Free column name strings
        foreach (var key in s_columnNames.Keys)
        {
            if (key.Item1 == resultHandle && s_columnNames.TryRemove(key, out var ptr))
                Marshal.FreeHGlobal(ptr);
        }

        // Free row text strings
        foreach (var key in s_rowTexts.Keys)
        {
            if (key.Item1 == resultHandle && s_rowTexts.TryRemove(key, out var ptr))
                Marshal.FreeHGlobal(ptr);
        }
    }
}
