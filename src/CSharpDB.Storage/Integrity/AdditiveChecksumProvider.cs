namespace CSharpDB.Storage.Integrity;

/// <summary>
/// Default additive checksum used by CSharpDB WAL frames.
/// </summary>
public sealed class AdditiveChecksumProvider : IPageChecksumProvider
{
    public uint Compute(ReadOnlySpan<byte> data)
    {
        uint sum = 0;
        int i = 0;
        for (; i + 3 < data.Length; i += 4)
            sum += BitConverter.ToUInt32(data[i..]);
        for (; i < data.Length; i++)
            sum += data[i];
        return sum;
    }
}
