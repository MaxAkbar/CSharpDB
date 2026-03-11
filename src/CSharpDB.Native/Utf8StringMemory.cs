using System.Runtime.InteropServices;
using System.Text;

namespace CSharpDB.Native;

internal static class Utf8StringMemory
{
    public static IntPtr Allocate(string value)
    {
        ArgumentNullException.ThrowIfNull(value);

        byte[] bytes = Encoding.UTF8.GetBytes(value);
        IntPtr buffer = Marshal.AllocHGlobal(bytes.Length + 1);
        Marshal.Copy(bytes, 0, buffer, bytes.Length);
        Marshal.WriteByte(buffer, bytes.Length, 0);
        return buffer;
    }

    public static void Free(IntPtr pointer)
    {
        if (pointer != IntPtr.Zero)
            Marshal.FreeHGlobal(pointer);
    }
}
