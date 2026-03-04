namespace CSharpDB.Storage.Catalog;

/// <summary>
/// Encodes and decodes low-level catalog payload formats.
/// </summary>
public interface ICatalogStore
{
    uint ReadRootPage(ReadOnlySpan<byte> data);
    byte[] WriteRootPayload(uint rootPageId, ReadOnlySpan<byte> metadata);
    byte[] WriteLengthPrefixedStrings(string s1, string s2);
    string ReadLengthPrefixedString(ReadOnlySpan<byte> data, int pos, out int newPos);
}
