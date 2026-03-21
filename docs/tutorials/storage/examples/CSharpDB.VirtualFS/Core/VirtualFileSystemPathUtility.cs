using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace CSharpDB.VirtualFS;

internal static class VirtualFileSystemPathUtility
{
    public static long MakeChildKey(long parentId, long childId)
    {
        return parentId * 1_000_000_000L + childId;
    }

    public static long ComputePathHash(long parentId, string name)
    {
        Span<byte> input = stackalloc byte[8 + Encoding.UTF8.GetByteCount(name)];
        BinaryPrimitives.WriteInt64LittleEndian(input, parentId);
        Encoding.UTF8.GetBytes(name, input[8..]);

        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(input, hash);

        return BinaryPrimitives.ReadInt64LittleEndian(hash) & 0x7FFF_FFFF_FFFF_FFFFL;
    }

    public static string[] SplitPath(string path)
    {
        return path.Split('/', StringSplitOptions.RemoveEmptyEntries);
    }
}
