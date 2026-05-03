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

    private static void AssertTablelessQuerySerializes(
        string sql,
        string[] displayColumns,
        string expectedPageSql,
        string expectedCountSql)
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
            [new Dictionary<int, string>(), null, true, 50, 1, displayColumns])!;

        MethodInfo buildCountPlan = planType.GetMethod(
            "BuildCountPlan",
            BindingFlags.Public | BindingFlags.Instance,
            binder: null,
            types: [typeof(IReadOnlyDictionary<int, string>), typeof(string[])],
            modifiers: null)!;

        object countPlan = buildCountPlan.Invoke(plan, [new Dictionary<int, string>(), displayColumns])!;
        string countSql = (string)countPlan.GetType().GetProperty("Sql")!.GetValue(countPlan)!;

        Assert.Equal(expectedPageSql, pageSql);
        Assert.Equal(expectedCountSql, countSql);
    }
}
