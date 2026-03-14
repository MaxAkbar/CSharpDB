using CSharpDB.Admin.Helpers;
using CSharpDB.Admin.Models;

namespace CSharpDB.Tests;

public class QueryDesignerSqlBuilderTests
{
    [Fact]
    public void Build_NormalizesLegacyFullJoinToInnerJoin()
    {
        var state = new QueryDesignerState
        {
            Tables =
            [
                new DesignerTableNode { TableName = "users" },
                new DesignerTableNode { TableName = "orders" },
            ],
            Joins =
            [
                new DesignerJoin
                {
                    LeftTable = "users",
                    LeftColumn = "id",
                    RightTable = "orders",
                    RightColumn = "user_id",
                    JoinType = DesignerJoinType.Full,
                },
            ],
        };

        string sql = QueryDesignerSqlBuilder.Build(state);

        Assert.Contains("INNER JOIN orders ON users.id = orders.user_id", sql);
        Assert.DoesNotContain("FULL OUTER JOIN", sql);
    }

    [Fact]
    public void Build_ReversesOuterJoinWhenOnlyRightTableIsInChain()
    {
        var state = new QueryDesignerState
        {
            Tables =
            [
                new DesignerTableNode { TableName = "orders" },
                new DesignerTableNode { TableName = "users" },
            ],
            Joins =
            [
                new DesignerJoin
                {
                    LeftTable = "users",
                    LeftColumn = "id",
                    RightTable = "orders",
                    RightColumn = "user_id",
                    JoinType = DesignerJoinType.Left,
                },
            ],
        };

        string sql = QueryDesignerSqlBuilder.Build(state);

        Assert.Contains("FROM orders", sql);
        Assert.Contains("RIGHT JOIN users ON users.id = orders.user_id", sql);
        Assert.DoesNotContain("JOIN orders ON users.id = orders.user_id", sql);
    }

    [Fact]
    public void Build_CrossJoinsDisconnectedTablesBeforeContinuingJoinChain()
    {
        var state = new QueryDesignerState
        {
            Tables =
            [
                new DesignerTableNode { TableName = "users" },
                new DesignerTableNode { TableName = "orders" },
                new DesignerTableNode { TableName = "products" },
            ],
            Joins =
            [
                new DesignerJoin
                {
                    LeftTable = "orders",
                    LeftColumn = "product_id",
                    RightTable = "products",
                    RightColumn = "id",
                    JoinType = DesignerJoinType.Inner,
                },
            ],
        };

        string sql = QueryDesignerSqlBuilder.Build(state);

        Assert.Contains("FROM users", sql);
        Assert.Contains("CROSS JOIN orders", sql);
        Assert.Contains("INNER JOIN products ON orders.product_id = products.id", sql);
    }
}
