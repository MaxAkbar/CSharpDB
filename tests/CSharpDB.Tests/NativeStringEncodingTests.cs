using System.Runtime.InteropServices;
using CSharpDB.Native;

namespace CSharpDB.Tests;

public sealed class NativeStringEncodingTests
{
    [Fact]
    public void StringCache_ReturnsUtf8Pointers()
    {
        IntPtr resultHandle = new(1234);
        const string value = "naïve café";

        try
        {
            IntPtr ptr = StringCache.GetOrAdd(resultHandle, 0, value);
            Assert.Equal(value, Marshal.PtrToStringUTF8(ptr));
        }
        finally
        {
            StringCache.Remove(resultHandle);
        }
    }

    [Fact]
    public void ErrorState_ReturnsUtf8Pointers()
    {
        const string message = "naïve café";

        try
        {
            ErrorState.Set(new InvalidOperationException(message));

            IntPtr ptr = ErrorState.GetMessagePtr();

            Assert.NotEqual(IntPtr.Zero, ptr);
            Assert.Equal(message, Marshal.PtrToStringUTF8(ptr));
        }
        finally
        {
            ErrorState.Clear();
        }
    }
}
