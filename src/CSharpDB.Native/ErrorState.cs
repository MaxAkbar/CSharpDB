using System.Runtime.InteropServices;
using CSharpDB.Core;

namespace CSharpDB.Native;

/// <summary>
/// Thread-local error state for the native API.
/// Follows the errno/GetLastError pattern: the last error is stored per-thread
/// and can be retrieved after any API call that returns an error indicator.
/// </summary>
internal static class ErrorState
{
    [ThreadStatic]
    private static string? t_message;

    [ThreadStatic]
    private static int t_code;

    [ThreadStatic]
    private static IntPtr t_messagePtr;

    public static void Set(Exception ex)
    {
        t_message = ex.Message;
        t_code = ex is CSharpDbException cex ? (int)cex.Code : -1;

        // Free previous unmanaged string if any
        if (t_messagePtr != IntPtr.Zero)
        {
            Marshal.FreeHGlobal(t_messagePtr);
            t_messagePtr = IntPtr.Zero;
        }
    }

    public static IntPtr GetMessagePtr()
    {
        if (t_message == null)
            return IntPtr.Zero;

        // Lazily allocate the unmanaged string
        if (t_messagePtr == IntPtr.Zero)
            t_messagePtr = Utf8StringMemory.Allocate(t_message);

        return t_messagePtr;
    }

    public static int GetCode() => t_code;

    public static void Clear()
    {
        t_message = null;
        t_code = 0;

        if (t_messagePtr != IntPtr.Zero)
        {
            Marshal.FreeHGlobal(t_messagePtr);
            t_messagePtr = IntPtr.Zero;
        }
    }
}
