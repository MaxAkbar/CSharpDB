using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public class SimpleInsertRowBufferTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(16)]
    [InlineData(17)]
    public void LiteralRows_PreserveWidthsAndOwnTheirValues(int width)
    {
        string row = string.Join(",", Enumerable.Range(1, width));
        Assert.True(Parser.TryParseSimpleInsert($"INSERT INTO t VALUES ({row}),({row});", out var parsed));
        Assert.Equal(2, parsed.RowCount);
        Assert.NotSame(parsed.ValueRows[0], parsed.ValueRows[1]);
        foreach (DbValue[] values in parsed.ValueRows)
        {
            Assert.Equal(width, values.Length);
            Assert.Equal(Enumerable.Range(1, width).Select(x => (long)x), values.Select(x => x.AsInteger));
        }

        parsed.ValueRows[0][0] = DbValue.FromInteger(999);
        Assert.Equal(1, parsed.ValueRows[1][0].AsInteger);
        Assert.True(Parser.TryParseSimpleInsert($"INSERT INTO t VALUES ({row});", out var later));
        Assert.Equal(1, later.Values[0].AsInteger);
    }

    [Theory]
    [InlineData("NULL", 1)]
    [InlineData("NULL, 'O''Reilly'", 2)]
    [InlineData("NULL, 'O''Reilly', X'00ff'", 3)]
    [InlineData("NULL, 'O''Reilly', X'00ff', -2.50", 4)]
    [InlineData("NULL, 'O''Reilly', X'00ff', -2.50, 123", 5)]
    public void SmallAndWideRows_PreserveMixedLiterals(string literals, int width)
    {
        Assert.True(Parser.TryParseSimpleInsert($"INSERT INTO t VALUES ({literals});", out var parsed));
        Assert.Equal(width, parsed.Values.Length);
        Assert.True(parsed.Values[0].IsNull);
        if (width > 1) Assert.Equal("O'Reilly", parsed.Values[1].AsText);
        if (width > 2) Assert.Equal(new byte[] { 0, 255 }, parsed.Values[2].AsBlob);
        if (width > 3) Assert.Equal(-2.50m, parsed.Values[3].AsDecimal);
        if (width > 4) Assert.Equal(123, parsed.Values[4].AsInteger);
    }

    [Theory]
    [InlineData("INSERT INTO t VALUES ()")]
    [InlineData("INSERT INTO t VALUES (1,)")]
    [InlineData("INSERT INTO t VALUES (1,2,)")]
    [InlineData("INSERT INTO t VALUES (1,2,3,)")]
    [InlineData("INSERT INTO t VALUES (1,2,3,4,)")]
    [InlineData("INSERT INTO t VALUES (1,2,3,4,5,)")]
    [InlineData("INSERT INTO t VALUES (1,2,3")]
    [InlineData("INSERT INTO t VALUES (1")]
    [InlineData("INSERT INTO t VALUES (1,2")]
    [InlineData("INSERT INTO t VALUES (1,2,3,4")]
    [InlineData("INSERT INTO t VALUES (1,2,3,4,5")]
    [InlineData("INSERT INTO t VALUES (1,2),(3)")]
    [InlineData("INSERT INTO t VALUES (1,2,3,4),(5,6,7,8,9)")]
    [InlineData("INSERT INTO t VALUES (1,'unterminated)")]
    [InlineData("INSERT INTO t VALUES (1,2,3,4,X'0')")]
    [InlineData("INSERT INTO t VALUES (1,2,3) trailing")]
    public void InvalidRows_RejectWithoutPublishingPartialValues(string sql)
    {
        Assert.False(Parser.TryParseSimpleInsert(sql, out var parsed));
        Assert.Equal(default, parsed);
    }

    [Theory]
    [InlineData("INSERT INTO t(a,b,c) VALUES (1,2,3)")]
    [InlineData("INSERT INTO t VALUES (1 + 2,3,4)")]
    [InlineData("INSERT INTO t VALUES (1,2,3,4,5 + 6)")]
    public void NonLiteralStatements_StillReachTheGeneralParser(string sql)
    {
        Assert.False(Parser.TryParseSimpleInsert(sql, out _));
        Assert.IsType<InsertStatement>(Parser.Parse(sql));
    }
}
