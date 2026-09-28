using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Tests;

public sealed class SlottedPageCompactionTests
{
    public static IEnumerable<object[]> Layouts()
    {
        foreach (uint pageId in new uint[] { 0, 1 })
        foreach (int count in new[] { 0, 1, 16, 17, 128, 256 })
        foreach (string order in new[] { "ascending", "descending", "random" })
            yield return [pageId, count, order];
    }

    [Theory]
    [MemberData(nameof(Layouts))]
    public void Defragment_PreservesCellsHeadersAndReusableSpace(uint pageId, int count, string order)
    {
        byte[] data = new byte[PageConstants.PageSize];
        data.AsSpan(0, PageConstants.ContentOffset(pageId)).Fill(0xA5);
        var page = new SlottedPage(data, pageId);
        page.Initialize(PageConstants.PageTypeLeaf);
        page.RightChildOrNextLeaf = 42;
        var expected = new List<byte[]>();
        var random = new Random(1729);
        for (int i = 0; i < count; i++)
        {
            byte[] cell = CreateCell(i);
            int index = order == "ascending" ? expected.Count : order == "descending" ? 0 : random.Next(expected.Count + 1);
            Assert.True(page.InsertCell(index, cell));
            expected.Insert(index, cell);
        }

        // Holes force overlapping moves; a page with 17 survivors exercises the
        // comparison-sort boundary, while smaller pages use insertion sort.
        if (count > 17)
        {
            for (int i = expected.Count - 2; i >= 0; i -= 3)
            {
                page.DeleteCell(i);
                expected.RemoveAt(i);
            }
        }

        int oldFreeSpace = page.FreeSpace;
        page.Defragment();

        Assert.Equal(expected.Count, page.CellCount);
        Assert.Equal(PageConstants.PageTypeLeaf, page.PageType);
        Assert.Equal(42u, page.RightChildOrNextLeaf);
        Assert.All(data.Take(PageConstants.ContentOffset(pageId)), value => Assert.Equal(0xA5, value));
        Assert.Equal(PageConstants.PageSize - expected.Sum(cell => cell.Length), page.CellContentStart);
        Assert.True(page.FreeSpace >= oldFreeSpace);
        for (int i = 0; i < expected.Count; i++)
            Assert.Equal(expected[i], page.GetCell(i).ToArray());

        byte[] compacted = data.ToArray();
        page.Defragment();
        Assert.Equal(compacted, data);
        byte[] extraCell = CreateCell(900);
        Assert.True(page.InsertCell(expected.Count, extraCell));
        Assert.Equal(extraCell, page.GetCell(expected.Count).ToArray());
    }

    private static byte[] CreateCell(int value)
    {
        // Include multi-byte length prefixes and overflow-marked cells. The
        // page layer must mask the overflow bit when determining move lengths.
        int payloadLength = value % 31 == 0 ? 130 : 3 + value % 5;
        ulong encodedLength = (ulong)payloadLength;
        if (value % 7 == 0)
            encodedLength |= PageConstants.LeafCellOverflowFlag;
        byte[] cell = new byte[Varint.SizeOf(encodedLength) + payloadLength];
        int headerLength = Varint.Write(cell, encodedLength);
        cell.AsSpan(headerLength).Fill((byte)value);
        return cell;
    }
}
