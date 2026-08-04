using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class PhysicalPlanBoundsTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Formatter_BoundsDeepPlansAndReportsTruncation()
    {
        var root = new PhysicalPlanNode
        {
            OperatorType = PhysicalOperatorType.Query,
        };
        PhysicalPlanNode parent = root;
        for (int i = 0; i < PhysicalPlanResultFormatter.MaxNodeCount + 100; i++)
        {
            var child = new PhysicalPlanNode
            {
                OperatorType = PhysicalOperatorType.Projection,
            };
            parent.AddChild(child);
            parent = child;
        }

        await using QueryResult result =
            PhysicalPlanResultFormatter.Format(new PhysicalPlan(root, analyzesTarget: false));
        List<DbValue[]> rows = await result.ToListAsync(Ct);

        Assert.Equal(PhysicalPlanResultFormatter.MaxNodeCount, rows.Count);
        Assert.Equal("diagnostic", rows[^1][2].AsText);
        Assert.Equal("partial", rows[^1][13].AsText);
        Assert.Equal("plan_truncated", rows[^1][14].AsText);
    }

    [Fact]
    public async Task Formatter_BoundsTextAndCyclesWithoutRecursiveFailure()
    {
        var root = new PhysicalPlanNode
        {
            OperatorType = PhysicalOperatorType.Query,
            Predicate = new string('x', PhysicalPlanResultFormatter.MaxTextLength + 100),
        };
        var child = new PhysicalPlanNode
        {
            OperatorType = PhysicalOperatorType.Filter,
        };
        root.AddChild(child);
        child.AddChild(root);

        await using QueryResult result =
            PhysicalPlanResultFormatter.Format(new PhysicalPlan(root, analyzesTarget: false));
        List<DbValue[]> rows = await result.ToListAsync(Ct);

        Assert.True(rows.Count <= PhysicalPlanResultFormatter.MaxNodeCount);
        Assert.Equal(
            PhysicalPlanResultFormatter.MaxTextLength,
            rows[0][12].AsText.Length);
        Assert.Contains(
            rows,
            static row =>
                row[2].Type == DbType.Text &&
                row[2].AsText == "diagnostic" &&
                row[14].Type == DbType.Text &&
                row[14].AsText == "plan_truncated");
    }
}
