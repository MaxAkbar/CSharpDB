using System.Runtime.InteropServices;

namespace CSharpDB.Native;

/// <summary>
/// Maps opaque IntPtr handles to managed objects via GCHandle.
/// This prevents the GC from collecting objects while foreign code holds a reference.
/// </summary>
internal static class HandleTable
{
    /// <summary>
    /// Pin a managed object and return an opaque handle.
    /// </summary>
    public static IntPtr Alloc(object obj)
    {
        var handle = GCHandle.Alloc(obj);
        return GCHandle.ToIntPtr(handle);
    }

    /// <summary>
    /// Retrieve the managed object behind an opaque handle.
    /// </summary>
    public static T Get<T>(IntPtr ptr) where T : class
    {
        var handle = GCHandle.FromIntPtr(ptr);
        return (T)handle.Target!;
    }

    /// <summary>
    /// Free a handle, allowing the managed object to be collected.
    /// </summary>
    public static void Free(IntPtr ptr)
    {
        var handle = GCHandle.FromIntPtr(ptr);
        handle.Free();
    }
}
