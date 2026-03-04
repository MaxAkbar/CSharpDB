using System.Numerics;
using System.Runtime.InteropServices;

namespace CSharpDB.Storage.Integrity;

/// <summary>
/// Default additive checksum used by CSharpDB WAL frames.
/// </summary>
public sealed class AdditiveChecksumProvider : IPageChecksumProvider
{
    public uint Compute(ReadOnlySpan<byte> data)
    {
        uint sum = 0;

        var uints = MemoryMarshal.Cast<byte, uint>(data);
        int uintCount = uints.Length;
        int j = 0;

        if (Vector.IsHardwareAccelerated && uintCount >= Vector<uint>.Count)
        {
            var vsum = Vector<uint>.Zero;
            int limit = uintCount - (uintCount % Vector<uint>.Count);
            for (; j < limit; j += Vector<uint>.Count)
                vsum += new Vector<uint>(uints.Slice(j));
            sum = Vector.Sum(vsum);
        }

        for (; j < uintCount; j++)
            sum += uints[j];

        for (int i = uintCount * 4; i < data.Length; i++)
            sum += data[i];

        return sum;
    }
}
