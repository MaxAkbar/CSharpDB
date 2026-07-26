namespace CSharpDB.Cli.Tests;

public sealed class MigrationWebsiteDocumentationTests
{
    [Fact]
    public void
        DatabaseMigrationGuide_CoversSourcesWorkflowRecoveryAndBoundaries()
    {
        string repoRoot = FindRepoRoot();
        string guide = File.ReadAllText(
            Path.Combine(
                repoRoot,
                "www",
                "docs",
                "database-migration.html"));

        Assert.Contains(
            "Move Data from Another Database into CSharpDB",
            guide,
            StringComparison.Ordinal);
        foreach (string source in
                 new[]
                 {
                     "CSV",
                     "JSON",
                     "SQLite",
                     "LiteDB",
                     "Microsoft Access",
                     "SQL Server",
                     "MySQL",
                 })
        {
            Assert.Contains(
                source,
                guide,
                StringComparison.Ordinal);
        }

        foreach (string extension in
                 new[]
                 {
                     ".csdbcsv",
                     ".csdbjson",
                     ".csdbsqlite",
                     ".csdblitedb",
                     ".csdbaccess",
                     ".csdbsqlserver",
                     ".csdbmysql",
                 })
        {
            Assert.Contains(
                extension,
                guide,
                StringComparison.Ordinal);
        }

        foreach (string command in
                 new[]
                 {
                     "csharpdb migrate inspect",
                     "csharpdb migrate plan",
                     "csharpdb migrate preview",
                     "csharpdb migrate apply",
                     "--resume",
                     "csharpdb migrate validate",
                     "--level checksum",
                     "csharpdb migrate snapshot",
                     "csharpdb migrate export",
                     "csharpdb migrate type-map",
                     "csharpdb migrate query-check",
                     "csharpdb migrate ddl-check",
                     "dotnet csharpdb-ef analyze",
                     "CSharpDB.Migration.DualRun",
                 })
        {
            Assert.Contains(
                command,
                guide,
                StringComparison.Ordinal);
        }

        Assert.Contains(
            "The receipts stored in the staged target are the recovery authority.",
            guide,
            StringComparison.Ordinal);
        Assert.Contains(
            "plaintext-sensitive source data",
            guide,
            StringComparison.Ordinal);
        Assert.Contains(
            "independently trusted record",
            guide,
            StringComparison.Ordinal);
        Assert.Contains(
            "Broad live Access, SQL Server, and MySQL",
            guide,
            StringComparison.Ordinal);
        Assert.Contains(
            "disposable Windows VM",
            guide,
            StringComparison.Ordinal);
        Assert.Contains(
            "ALLOW_SNAPSHOT_ISOLATION",
            guide,
            StringComparison.Ordinal);
        Assert.Contains(
            "<code>Initial Catalog</code>",
            guide,
            StringComparison.Ordinal);
        Assert.Contains(
            "<code>Database</code>",
            guide,
            StringComparison.Ordinal);
        Assert.Contains(
            "SQL Server is currently an evaluation capture lane.",
            guide,
            StringComparison.Ordinal);
        Assert.Contains(
            "Do not grant <code>sysadmin</code>",
            guide,
            StringComparison.Ordinal);
        Assert.Contains(
            "currently stop after candidate capture and catalog review",
            guide,
            StringComparison.Ordinal);
        Assert.Contains(
            "dedicated no-write account",
            guide,
            StringComparison.Ordinal);
        Assert.Contains(
            "read the candidate tables",
            guide,
            StringComparison.Ordinal);
        Assert.Contains(
            "view their definitions",
            guide,
            StringComparison.Ordinal);
        Assert.Contains(
            "migration-work, .\\migration-spill",
            guide,
            StringComparison.Ordinal);
        Assert.Contains(
            "mkdir -p",
            guide,
            StringComparison.Ordinal);
    }

    [Fact]
    public void
        DatabaseMigrationGuide_IsDiscoverableAndDistinctFromLibraryRename()
    {
        string repoRoot = FindRepoRoot();
        foreach (string relativePath in
                 new[]
                 {
                     Path.Combine(
                         "www",
                         "docs",
                         "index.html"),
                     Path.Combine(
                         "www",
                         "docs",
                         "cli.html"),
                     Path.Combine(
                         "www",
                         "docs",
                         "migrations.html"),
                     Path.Combine(
                         "www",
                         "downloads.html"),
                     Path.Combine(
                         "www",
                         "sitemap.xml"),
                     "README.md",
                 })
        {
            string content = File.ReadAllText(
                Path.Combine(
                    repoRoot,
                    relativePath));
            Assert.Contains(
                "database-migration.html",
                content,
                StringComparison.Ordinal);
        }

        string renameGuide = File.ReadAllText(
            Path.Combine(
                repoRoot,
                "www",
                "docs",
                "migrations.html"));
        Assert.Contains(
            "CSharpDB.Core",
            renameGuide,
            StringComparison.Ordinal);
        Assert.Contains(
            "Looking for database migration?",
            renameGuide,
            StringComparison.Ordinal);

        string releaseNotes = File.ReadAllText(
            Path.Combine(repoRoot, "RELEASE_NOTES.md"));
        Assert.Contains(
            "`apply --resume`",
            releaseNotes,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "`apply`, `resume`",
            releaseNotes,
            StringComparison.Ordinal);
        Assert.Contains(
            "`MIGRATION-SHA256SUMS.txt`",
            releaseNotes,
            StringComparison.Ordinal);

        string downloads = File.ReadAllText(
            Path.Combine(
                repoRoot,
                "www",
                "downloads.html"));
        Assert.Contains(
            "<code>MIGRATION-SHA256SUMS.txt</code>",
            downloads,
            StringComparison.Ordinal);
        Assert.Contains(
            "CSharpDB.Migration.DualRun",
            downloads,
            StringComparison.Ordinal);

        string blogIndex = File.ReadAllText(
            Path.Combine(
                repoRoot,
                "www",
                "blog",
                "index.html"));
        Assert.Contains(
            "migrating-existing-data-to-csharpdb.html",
            blogIndex,
            StringComparison.Ordinal);
        string migrationArticle = File.ReadAllText(
            Path.Combine(
                repoRoot,
                "www",
                "blog",
                "migrating-existing-data-to-csharpdb.html"));
        Assert.Contains(
            "../docs/database-migration.html",
            migrationArticle,
            StringComparison.Ordinal);
    }

    private static string FindRepoRoot()
    {
        DirectoryInfo? directory =
            new(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(
                    Path.Combine(
                        directory.FullName,
                        "CSharpDB.slnx")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException(
            "Could not locate repository root from test base directory.");
    }
}
