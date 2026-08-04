using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class WindowFunctionPhase5ParserTests
{
    [Fact]
    public void Parse_NamedWindow_ResolvesCaseInsensitivelyAfterFinalOrderBy()
    {
        var select = Assert.IsType<SelectStatement>(Parser.Parse(
            """
            SELECT ROW_NUMBER() OVER sales_window AS rn,
                   SUM(amount) OVER SALES_WINDOW AS running_total
            FROM sales
            WINDOW Sales_Window AS (
                PARTITION BY department
                ORDER BY amount DESC, id
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            )
            ORDER BY ROW_NUMBER() OVER SaLeS_WiNdOw DESC
            """));

        NamedWindowDefinition definition = Assert.Single(select.WindowDefinitions);
        Assert.Equal("Sales_Window", definition.Name);
        Assert.Single(definition.Specification.PartitionBy);
        Assert.Equal(2, definition.Specification.OrderBy.Count);
        Assert.True(definition.Specification.OrderBy[0].Descending);
        AssertFrame(
            definition.Specification.Frame,
            WindowFrameBoundKind.Preceding,
            2,
            WindowFrameBoundKind.CurrentRow);

        var first = Assert.IsType<WindowFunctionExpression>(select.Columns[0].Expression);
        var second = Assert.IsType<WindowFunctionExpression>(select.Columns[1].Expression);
        var finalOrder = Assert.IsType<WindowFunctionExpression>(
            Assert.Single(select.OrderBy!).Expression);
        foreach (WindowFunctionExpression window in new[] { first, second, finalOrder })
        {
            Assert.Null(window.Window.ReferenceName);
            Assert.Single(window.Window.PartitionBy);
            Assert.Equal(2, window.Window.OrderBy.Count);
            AssertFrame(
                window.Window.Frame,
                WindowFrameBoundKind.Preceding,
                2,
                WindowFrameBoundKind.CurrentRow);
        }
    }

    [Fact]
    public void Parse_RowsShortForm_DefaultsEndToCurrentRow()
    {
        WindowSpecification specification = ParseSingleWindow(
            "SELECT SUM(value) OVER (ORDER BY id ROWS 3 PRECEDING) FROM samples");

        AssertFrame(
            specification.Frame,
            WindowFrameBoundKind.Preceding,
            3,
            WindowFrameBoundKind.CurrentRow);
    }

    [Theory]
    [InlineData(
        "ROWS UNBOUNDED PRECEDING",
        WindowFrameBoundKind.UnboundedPreceding,
        null,
        WindowFrameBoundKind.CurrentRow,
        null)]
    [InlineData(
        "ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING",
        WindowFrameBoundKind.CurrentRow,
        null,
        WindowFrameBoundKind.Following,
        2L)]
    [InlineData(
        "ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING",
        WindowFrameBoundKind.Preceding,
        1L,
        WindowFrameBoundKind.UnboundedFollowing,
        null)]
    [InlineData(
        "ROWS BETWEEN 0 FOLLOWING AND CURRENT ROW",
        WindowFrameBoundKind.Following,
        0L,
        WindowFrameBoundKind.CurrentRow,
        null)]
    public void Parse_RowsBounds_PreserveKindsAndOffsets(
        string frameSql,
        WindowFrameBoundKind startKind,
        long? startOffset,
        WindowFrameBoundKind endKind,
        long? endOffset)
    {
        WindowSpecification specification = ParseSingleWindow(
            $"SELECT SUM(value) OVER (ORDER BY id {frameSql}) FROM samples");

        AssertFrame(
            specification.Frame,
            startKind,
            startOffset,
            endKind,
            endOffset);
    }

    [Fact]
    public void Parse_EmptyNamedWindow_DoesNotConsumeWindowAsTableAlias()
    {
        var select = Assert.IsType<SelectStatement>(
            Parser.Parse("SELECT id FROM samples WINDOW w AS ()"));

        var table = Assert.IsType<SimpleTableRef>(select.From);
        Assert.Null(table.Alias);
        Assert.Equal("w", Assert.Single(select.WindowDefinitions).Name);
    }

    [Fact]
    public void Parse_TablelessWindowClause_DoesNotConsumeWindowAsSelectAlias()
    {
        var select = Assert.IsType<SelectStatement>(
            Parser.Parse("SELECT 1 WINDOW w AS ()"));

        Assert.Null(Assert.Single(select.Columns).Alias);
        Assert.IsType<SingleRowTableRef>(select.From);
        Assert.Equal("w", Assert.Single(select.WindowDefinitions).Name);
    }

    [Fact]
    public void Parse_QuotedWindow_RemainsAnImplicitSelectAlias()
    {
        var select = Assert.IsType<SelectStatement>(
            Parser.Parse("SELECT 1 \"WINDOW\""));

        Assert.Equal("WINDOW", Assert.Single(select.Columns).Alias);
        Assert.Empty(select.WindowDefinitions);
    }

    [Fact]
    public void Parse_QuotedWindow_RemainsAnImplicitTableAlias()
    {
        var select = Assert.IsType<SelectStatement>(
            Parser.Parse("SELECT id FROM samples \"WINDOW\""));

        var table = Assert.IsType<SimpleTableRef>(select.From);
        Assert.Equal("WINDOW", table.Alias);
        Assert.Empty(select.WindowDefinitions);
    }

    [Theory]
    [InlineData(
        "SELECT ROW_NUMBER() OVER missing_window FROM samples",
        "Undefined window definition")]
    [InlineData(
        "SELECT ROW_NUMBER() OVER w FROM samples WINDOW w AS (), W AS ()",
        "Duplicate window definition")]
    [InlineData(
        "SELECT ROW_NUMBER() OVER (base_window) FROM samples WINDOW base_window AS ()",
        "inheritance")]
    [InlineData(
        "SELECT ROW_NUMBER() OVER child FROM samples WINDOW child AS (base_window ORDER BY id)",
        "inheritance")]
    [InlineData(
        "SELECT SUM(value) OVER (ORDER BY id RANGE UNBOUNDED PRECEDING) FROM samples",
        "RANGE and GROUPS")]
    [InlineData(
        "SELECT SUM(value) OVER (ORDER BY id GROUPS UNBOUNDED PRECEDING) FROM samples",
        "RANGE and GROUPS")]
    [InlineData(
        "SELECT SUM(value) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW) FROM samples",
        "EXCLUDE")]
    [InlineData(
        "SELECT ROW_NUMBER() OVER (ORDER BY id NULLS FIRST) FROM samples",
        "NULLS FIRST/LAST")]
    [InlineData(
        "SELECT ROW_NUMBER() OVER (ORDER BY id DESC NULLS LAST) FROM samples",
        "NULLS FIRST/LAST")]
    public void Parse_UnsupportedNamedOrFrameForms_ReportClearSyntaxErrors(
        string sql,
        string expectedMessage)
    {
        CSharpDbException error = Assert.Throws<CSharpDbException>(() => Parser.Parse(sql));

        Assert.Equal(ErrorCode.SyntaxError, error.Code);
        Assert.Contains(expectedMessage, error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(
        "SELECT 1 WINDOW w AS (ORDER BY ROW_NUMBER() OVER ())")]
    [InlineData(
        "SELECT 1 WINDOW w AS (ORDER BY ROW_NUMBER() OVER w)")]
    [InlineData(
        """
        SELECT 1
        WINDOW left_window AS (ORDER BY ROW_NUMBER() OVER right_window),
               right_window AS (ORDER BY ROW_NUMBER() OVER left_window)
        """)]
    public void Parse_NestedOrCyclicNamedWindowDefinitions_AreRejectedWithoutRecursion(
        string sql)
    {
        CSharpDbException error = Assert.Throws<CSharpDbException>(
            () => Parser.Parse(sql));

        Assert.Equal(ErrorCode.SyntaxError, error.Code);
        Assert.Contains("Nested window functions", error.Message);
    }

    [Theory]
    [InlineData(
        "ROWS UNBOUNDED FOLLOWING",
        "cannot start with UNBOUNDED FOLLOWING")]
    [InlineData(
        "ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING",
        "cannot end with UNBOUNDED PRECEDING")]
    [InlineData(
        "ROWS BETWEEN CURRENT ROW AND 1 PRECEDING",
        "start cannot be after")]
    [InlineData(
        "ROWS BETWEEN 2 PRECEDING AND 3 PRECEDING",
        "start cannot be after")]
    [InlineData(
        "ROWS BETWEEN 3 FOLLOWING AND 2 FOLLOWING",
        "start cannot be after")]
    [InlineData(
        "ROWS -1 PRECEDING",
        "nonnegative integer literal")]
    [InlineData(
        "ROWS 1.5 PRECEDING",
        "nonnegative integer literal")]
    [InlineData(
        "ROWS NULL PRECEDING",
        "nonnegative integer literal")]
    [InlineData(
        "ROWS 1",
        "followed by PRECEDING or FOLLOWING")]
    [InlineData(
        "ROWS BETWEEN CURRENT ROW CURRENT ROW",
        "Expected And")]
    public void Parse_IllegalOrMalformedRowsBounds_ReportClearSyntaxErrors(
        string frameSql,
        string expectedMessage)
    {
        string sql =
            $"SELECT SUM(value) OVER (ORDER BY id {frameSql}) FROM samples";

        CSharpDbException error = Assert.Throws<CSharpDbException>(() => Parser.Parse(sql));

        Assert.Equal(ErrorCode.SyntaxError, error.Code);
        Assert.Contains(expectedMessage, error.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static WindowSpecification ParseSingleWindow(string sql)
    {
        var select = Assert.IsType<SelectStatement>(Parser.Parse(sql));
        var window = Assert.IsType<WindowFunctionExpression>(
            Assert.Single(select.Columns).Expression);
        return window.Window;
    }

    private static void AssertFrame(
        WindowFrame? frame,
        WindowFrameBoundKind startKind,
        long? startOffset,
        WindowFrameBoundKind endKind,
        long? endOffset = null)
    {
        Assert.NotNull(frame);
        Assert.Equal(startKind, frame.Start.Kind);
        Assert.Equal(startOffset, frame.Start.Offset);
        Assert.Equal(endKind, frame.End.Kind);
        Assert.Equal(endOffset, frame.End.Offset);
    }
}
