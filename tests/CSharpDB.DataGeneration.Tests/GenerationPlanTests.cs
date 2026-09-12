using CSharpDB.DataGeneration;
using CSharpDB.Primitives;

namespace CSharpDB.DataGeneration.Tests;

public sealed class GenerationPlanTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(ParentKeySource.Existing)]
    [InlineData(ParentKeySource.Generated)]
    public void NullableUniqueRelationshipsUseOnlyNonNullRowsAndReplayDeterministically(ParentKeySource source)
    {
        var (profile, snapshots) = Fixture(source, 32);
        var plan = GenerationPlan.Build(profile, snapshots, ct: Ct);
        var rows = plan.Rows("Children", ct: Ct).ToArray();
        var parentKeys = rows.Where(row => row["ParentId"] is not null).Select(row => row["ParentId"]).ToArray();
        Assert.InRange(parentKeys.Length, 2, 31);
        Assert.Contains(rows.Skip(32), row => row["ParentId"] is not null);
        Assert.Equal(parentKeys.Length, parentKeys.Distinct().Count());
        Assert.Contains(rows, row => row["ParentId"] is null);

        // Tighten the pool to exactly the number of non-null children.
        var (exactProfile, exactSnapshots) = Fixture(source, parentKeys.Length);
        var exactPlan = GenerationPlan.Build(exactProfile, exactSnapshots, ct: Ct);
        var exactRows = exactPlan.Rows("Children", ct: Ct).ToArray();
        Assert.Equal(rows.Select(row => row["ParentId"] is null), exactRows.Select(row => row["ParentId"] is null));
        Assert.Equal(Enumerable.Range(1, parentKeys.Length).Select(i => (long)i),
            exactRows.Where(row => row["ParentId"] is not null).Select(row => (long)row["ParentId"]!).Order());
        var repeat = GenerationPlan.Build(GenerationProfile.FromJson(exactProfile.ToJson()), exactSnapshots, ct: Ct);
        Assert.Equal(exactRows.Select(Canonical), repeat.Rows("Children", ct: Ct).Select(Canonical));
        foreach (int offset in new[] { 48, 0, 24, 8 })
            Assert.Equal(exactRows.Skip(offset).Take(8).Select(Canonical), exactPlan.Rows("Children", offset, 8, Ct).Select(Canonical));
        int child = Array.FindLastIndex(exactRows, row => row["ParentId"] is not null);
        Assert.Equal(exactRows[child]["ParentId"], Assert.Single(exactPlan.ReferencedKeys("Children", "Parent", child + 1))["Id"]);

        var (shortProfile, shortSnapshots) = Fixture(source, parentKeys.Length - 1);
        var error = Assert.Throws<InvalidOperationException>(() => GenerationPlan.Build(shortProfile, shortSnapshots, ct: Ct));
        Assert.Contains("insufficient parent keys", error.Message);
    }

    private static string Canonical(IReadOnlyDictionary<string, object?> row)
        => string.Join("|", row.OrderBy(p => p.Key).Select(p => $"{p.Key}={GenerationValues.Display(p.Value)}"));

    private static (GenerationProfile Profile, GenerationTableSnapshot[] Snapshots) Fixture(ParentKeySource source, int parents)
    {
        var parent = new GenerationTableSnapshot
        {
            Schema = new TableSchema { TableName = "Parents", Columns = [new() { Name = "Id", Type = DbType.Integer, IsPrimaryKey = true, Nullable = false }] },
            RowCount = source == ParentKeySource.Existing ? parents : 0,
            ExistingKeys = source == ParentKeySource.Existing
                ? Enumerable.Range(1, parents).Select(i => (IReadOnlyDictionary<string, object?>)new Dictionary<string, object?> { ["Id"] = (long)i }).ToArray() : [],
        };
        var child = new GenerationTableSnapshot
        {
            Schema = new TableSchema
            {
                TableName = "Children", SchemaId = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
                Columns = [new() { Name = "Id", Type = DbType.Integer, IsPrimaryKey = true, Nullable = false }, new() { Name = "ParentId", Type = DbType.Integer }],
                KeyConstraints = [new() { Kind = KeyConstraintKind.Unique, Columns = ["ParentId"] }],
                ForeignKeys = [new() { ConstraintName = "Parent", ColumnName = "ParentId", ReferencedTableName = "Parents", ReferencedColumnName = "Id", SupportingIndexName = "" }],
            },
        };
        var rule = GenerationPlan.Suggest(child);
        rule.Rows = 64;
        rule.Relationships[0].Source = source;
        rule.Relationships[0].NullRate = .75;
        var profile = new GenerationProfile { Tables = [rule] };
        if (source == ParentKeySource.Generated)
        {
            var parentRule = GenerationPlan.Suggest(parent); parentRule.Rows = parents;
            profile.Tables.Add(parentRule);
        }
        return (profile, [parent, child]);
    }
}
