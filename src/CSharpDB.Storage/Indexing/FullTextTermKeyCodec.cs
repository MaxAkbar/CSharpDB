using System.Text;

namespace CSharpDB.Storage.Indexing;

internal static class FullTextTermKeyCodec
{
    public static long ComputeKey(string term)
    {
        ArgumentNullException.ThrowIfNull(term);

        const ulong offsetBasis = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;

        ulong hash = offsetBasis;
        foreach (byte b in Encoding.UTF8.GetBytes(term))
        {
            hash ^= b;
            hash *= prime;
        }

        return unchecked((long)hash);
    }
}
