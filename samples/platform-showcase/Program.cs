using CSharpDB.Engine;
using CSharpDB.Sql;

var sampleDirectory = AppContext.BaseDirectory;
var schemaPath = Path.Combine(sampleDirectory, "schema.sql");
var dbPath = Path.Combine(sampleDirectory, "platform-showcase-demo.db");

if (File.Exists(dbPath))
    File.Delete(dbPath);

await using var db = await Database.OpenAsync(dbPath);
await ExecuteSchemaAsync(db, schemaPath);

await db.EnsureFullTextIndexAsync(
    "fts_knowledge_articles",
    "knowledge_articles",
    ["title", "body"]);

var hits = await db.SearchAsync("fts_knowledge_articles", "duplicate renewal");

var filters = await db.GetCollectionAsync<DashboardFilterDocument>("dashboard_filters");
await SeedDashboardFiltersAsync(filters);
await CreateCollectionIndexesAsync(filters);

Console.WriteLine("Atlas Platform Showcase");
Console.WriteLine();

Console.WriteLine("Top customer rollup:");
await using (var result = await db.ExecuteAsync(
    "SELECT customer_name, lifetime_revenue, open_ticket_count FROM customer_360 ORDER BY lifetime_revenue DESC LIMIT 3"))
{
    await foreach (var row in result.GetRowsAsync())
        Console.WriteLine($"  {row[0].AsText} | revenue={row[1].AsReal:F2} | open_tickets={row[2].AsInteger}");
}

Console.WriteLine();
Console.WriteLine("Full-text hits for 'duplicate renewal':");
if (hits.Count == 0)
{
    Console.WriteLine("  (no hits)");
}

foreach (var hit in hits)
{
    await using var article = await db.ExecuteAsync(
        $"SELECT title FROM knowledge_articles WHERE id = {hit.RowId}");
    var rows = await article.ToListAsync();
    var title = rows.Count > 0 ? rows[0][0].AsText : $"Article {hit.RowId}";
    Console.WriteLine($"  row={hit.RowId} | score={hit.Score:F2} | {title}");
}

Console.WriteLine();
Console.WriteLine("Collection lookup by nested path:");
await foreach (var match in filters.FindByPathAsync("Filters.Region", "West"))
{
    Console.WriteLine($"  {match.Key} | owner={match.Value.OwnerEmail} | preset={match.Value.PresetName}");
}

Console.WriteLine();
Console.WriteLine("Collection lookup by tag membership:");
await foreach (var match in filters.FindByPathAsync("$.tags[]", "support"))
{
    Console.WriteLine($"  {match.Key} | owner={match.Value.OwnerEmail} | preset={match.Value.PresetName}");
}

Console.WriteLine();
Console.WriteLine($"Database written to: {dbPath}");

static async Task ExecuteSchemaAsync(Database db, string schemaPath)
{
    string script = await File.ReadAllTextAsync(schemaPath);
    foreach (string statement in SqlScriptSplitter.SplitExecutableStatements(script))
        await db.ExecuteAsync(statement);
}

static async Task SeedDashboardFiltersAsync(Collection<DashboardFilterDocument> filters)
{
    await filters.PutAsync("filter:nora:renewals", new DashboardFilterDocument(
        OwnerEmail: "nora.blake@atlas.local",
        PresetName: "Renewals At Risk",
        Filters: new DashboardFilterSettings("West", "renewal", "high"),
        Tags: ["finance", "support"]));

    await filters.PutAsync("filter:priya:platform", new DashboardFilterDocument(
        OwnerEmail: "priya.raman@atlas.local",
        PresetName: "Platform Escalations",
        Filters: new DashboardFilterSettings("National", "platform", "critical"),
        Tags: ["support", "platform"]));

    await filters.PutAsync("filter:leo:onboarding", new DashboardFilterDocument(
        OwnerEmail: "leo.martinez@atlas.local",
        PresetName: "West Onboarding Rollouts",
        Filters: new DashboardFilterSettings("West", "onboarding", "medium"),
        Tags: ["success", "rollout"]));
}

static async Task CreateCollectionIndexesAsync(Collection<DashboardFilterDocument> filters)
{
    await filters.EnsureIndexAsync(f => f.OwnerEmail);
    await filters.EnsureIndexAsync("Filters.Region");
    await filters.EnsureIndexAsync("$.tags[]");
}

public sealed record DashboardFilterDocument(
    string OwnerEmail,
    string PresetName,
    DashboardFilterSettings Filters,
    string[] Tags);

public sealed record DashboardFilterSettings(
    string Region,
    string Queue,
    string Priority);
