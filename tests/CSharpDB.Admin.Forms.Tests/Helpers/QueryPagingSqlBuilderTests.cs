using System.Reflection;

namespace CSharpDB.Admin.Forms.Tests.Helpers;

public sealed class QueryPagingSqlBuilderTests
{
    [Fact]
    public void SelectDateWithoutFrom_SerializesPageAndCountSql()
        => AssertTablelessQuerySerializes(
            "SELECT Date();",
            new[] { "DATE()" },
            "SELECT DATE() LIMIT 50 OFFSET 0",
            "SELECT COUNT(*)");

    [Fact]
    public void TablelessScalarCallback_SerializesPageAndCountSql()
        => AssertTablelessQuerySerializes(
            "SELECT Slugify('Hello World');",
            new[] { "SLUGIFY('Hello World')" },
            "SELECT SLUGIFY('Hello World') LIMIT 50 OFFSET 0",
            "SELECT COUNT(*)");

    [Fact]
    public void UnionAll_PreservesQuantifierInPageAndCountSql()
        => AssertTablelessQuerySerializes(
            "SELECT 1 AS value UNION ALL SELECT 1 AS ignored_name;",
            new[] { "value" },
            "SELECT 1 AS \"value\" UNION ALL SELECT 1 AS \"ignored_name\" LIMIT 50 OFFSET 0",
            "WITH \"__admin_query_results\" AS (SELECT 1 AS \"value\" UNION ALL SELECT 1 AS \"ignored_name\") " +
            "SELECT COUNT(*) FROM \"__admin_query_results\"");

    [Fact]
    public void QuotedIdentifiers_RemainQuotedWhenPagingSqlIsRewritten()
        => AssertTablelessQuerySerializes(
            "SELECT \"order value\" AS \"display name\" FROM \"select table\";",
            new[] { "display name" },
            "SELECT \"order value\" AS \"display name\" FROM \"select table\" LIMIT 50 OFFSET 0",
            "SELECT COUNT(*) FROM \"select table\"");

    [Fact]
    public void NamedWindowAndRowsFrame_ArePreservedWhenPagingSqlIsRewritten()
        => AssertTablelessQuerySerializes(
            """
            SELECT SUM(value) OVER running AS running_total
            FROM samples
            WINDOW running AS (
                PARTITION BY group_id ORDER BY id
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            );
            """,
            new[] { "running_total" },
            "SELECT SUM(\"value\") OVER (PARTITION BY \"group_id\" ORDER BY \"id\" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS \"running_total\" " +
            "FROM \"samples\" WINDOW \"running\" AS (PARTITION BY \"group_id\" ORDER BY \"id\" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) LIMIT 50 OFFSET 0",
            "SELECT COUNT(*) FROM \"samples\"");

    [Theory]
    [InlineData(0, "__q0")]
    [InlineData(1, "__q1")]
    public void WindowQueryFilter_IsAppliedOutsideTheWindowQuery(
        int filterColumn,
        string internalFilterColumn)
    {
        const string sql =
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM scores";
        string[] displayColumns = ["id", "rn"];
        var filters = new Dictionary<int, string>
        {
            [filterColumn] = "2",
        };

        (string pageSql, string countSql) =
            BuildPageAndCountSql(sql, displayColumns, filters);

        const string windowQuery =
            "SELECT \"id\", ROW_NUMBER() OVER (ORDER BY \"id\") AS \"rn\" FROM \"scores\"";
        Assert.Equal(
            $"WITH \"__admin_query_results\"(\"__q0\", \"__q1\") AS ({windowQuery}) " +
            "SELECT * FROM \"__admin_query_results\" " +
            $"WHERE TEXT(\"{internalFilterColumn}\") LIKE '%2%' ESCAPE '!' LIMIT 50 OFFSET 0",
            pageSql);
        Assert.Equal(
            $"WITH \"__admin_query_results\"(\"__q0\", \"__q1\") AS ({windowQuery}) " +
            "SELECT COUNT(*) FROM \"__admin_query_results\" " +
            $"WHERE TEXT(\"{internalFilterColumn}\") LIKE '%2%' ESCAPE '!'",
            countSql);
    }

    private static void AssertTablelessQuerySerializes(
        string sql,
        string[] displayColumns,
        string expectedPageSql,
        string expectedCountSql)
    {
        (string pageSql, string countSql) = BuildPageAndCountSql(
            sql,
            displayColumns,
            new Dictionary<int, string>());

        Assert.Equal(expectedPageSql, pageSql);
        Assert.Equal(expectedCountSql, countSql);
    }

    private static (string PageSql, string CountSql) BuildPageAndCountSql(
        string sql,
        string[] displayColumns,
        IReadOnlyDictionary<int, string> filters)
    {
        Type planType = Type.GetType("CSharpDB.Admin.Helpers.QueryPagingPlan, CSharpDB.Admin", throwOnError: true)!;
        MethodInfo parse = planType.GetMethod("Parse", BindingFlags.Public | BindingFlags.Static)!;
        object plan = parse.Invoke(null, [sql])!;

        MethodInfo buildPageSql = planType.GetMethod(
            "BuildPageSql",
            BindingFlags.Public | BindingFlags.Instance,
            binder: null,
            types:
            [
                typeof(IReadOnlyDictionary<int, string>),
                typeof(int?),
                typeof(bool),
                typeof(int),
                typeof(int),
                typeof(string[]),
            ],
            modifiers: null)!;

        string pageSql = (string)buildPageSql.Invoke(
            plan,
            [filters, null, true, 50, 1, displayColumns])!;

        MethodInfo buildCountPlan = planType.GetMethod(
            "BuildCountPlan",
            BindingFlags.Public | BindingFlags.Instance,
            binder: null,
            types: [typeof(IReadOnlyDictionary<int, string>), typeof(string[])],
            modifiers: null)!;

        object countPlan = buildCountPlan.Invoke(plan, [filters, displayColumns])!;
        string countSql = (string)countPlan.GetType().GetProperty("Sql")!.GetValue(countPlan)!;

        return (pageSql, countSql);
    }
}
