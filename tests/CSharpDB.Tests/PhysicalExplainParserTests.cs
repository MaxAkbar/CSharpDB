using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class PhysicalExplainParserTests
{
    [Theory]
    [InlineData("EXPLAIN SELECT 1", false, typeof(SelectStatement))]
    [InlineData("EXPLAIN FOR SELECT 1", false, typeof(SelectStatement))]
    [InlineData("EXPLAIN ANALYZE SELECT 1", true, typeof(SelectStatement))]
    [InlineData(
        "EXPLAIN ANALYZE FOR INSERT INTO items VALUES (1)",
        true,
        typeof(InsertStatement))]
    [InlineData(
        "EXPLAIN UPDATE items SET value = 2 WHERE id = 1",
        false,
        typeof(UpdateStatement))]
    [InlineData(
        "EXPLAIN ANALYZE DELETE FROM items WHERE id = 1",
        true,
        typeof(DeleteStatement))]
    public void Parse_PhysicalExplainForms(
        string sql,
        bool analyze,
        Type targetType)
    {
        ExplainStatement explain = Assert.IsType<ExplainStatement>(
            Parser.Parse(sql));

        Assert.Equal(analyze, explain.Analyze);
        Assert.IsType(targetType, explain.Target);
    }

    [Fact]
    public void Classifier_UsesTargetSemanticsOnlyForAnalyze()
    {
        ExplainStatement plainMutation = Assert.IsType<ExplainStatement>(
            Parser.Parse("EXPLAIN INSERT INTO items VALUES (1)"));
        ExplainStatement analyzedQuery = Assert.IsType<ExplainStatement>(
            Parser.Parse("EXPLAIN ANALYZE SELECT 1"));
        ExplainStatement analyzedMutation = Assert.IsType<ExplainStatement>(
            Parser.Parse("EXPLAIN ANALYZE DELETE FROM items"));

        Assert.True(SqlStatementClassifier.IsReadOnly(plainMutation));
        Assert.True(SqlStatementClassifier.IsReadOnly(analyzedQuery));
        Assert.False(SqlStatementClassifier.IsReadOnly(analyzedMutation));
    }

    [Fact]
    public void Parse_PhysicalExplainRejectsUnsupportedTarget()
    {
        _ = Assert.Throws<CSharpDbException>(
            () => Parser.Parse("EXPLAIN CREATE TABLE items (id INTEGER)"));
    }
}
