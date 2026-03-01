using System.Buffers.Binary;
using System.Text;

namespace CSharpDB.Storage.Catalog;

/// <summary>
/// Low-level catalog payload encoding helpers.
/// </summary>
public sealed class CatalogStore : ICatalogStore
{
    public uint ReadRootPage(ReadOnlySpan<byte> data) =>
        BinaryPrimitives.ReadUInt32LittleEndian(data);

    public byte[] WriteRootPayload(uint rootPageId, ReadOnlySpan<byte> metadata)
    {
        var payload = new byte[4 + metadata.Length];
        BitConverter.TryWriteBytes(payload, rootPageId);
        metadata.CopyTo(payload.AsSpan(4));
        return payload;
    }

    public byte[] WriteLengthPrefixedStrings(string s1, string s2)
    {
        var b1 = Encoding.UTF8.GetBytes(s1);
        var b2 = Encoding.UTF8.GetBytes(s2);
        var result = new byte[4 + b1.Length + 4 + b2.Length];
        BitConverter.TryWriteBytes(result.AsSpan(0), b1.Length);
        b1.CopyTo(result.AsSpan(4));
        BitConverter.TryWriteBytes(result.AsSpan(4 + b1.Length), b2.Length);
        b2.CopyTo(result.AsSpan(4 + b1.Length + 4));
        return result;
    }

    public string ReadLengthPrefixedString(ReadOnlySpan<byte> data, int pos, out int newPos)
    {
        int len = BitConverter.ToInt32(data[pos..(pos + 4)]);
        string s = Encoding.UTF8.GetString(data[(pos + 4)..(pos + 4 + len)]);
        newPos = pos + 4 + len;
        return s;
    }
}
