using System.Text.Json;
using System.Text.RegularExpressions;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Engine;

namespace CSharpDB.DevOps.Tests;

public sealed class DatabaseDocumenterTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task GenerationAndExports_ReadOnlySchemaWithCompositeAndSelfRelationships()
    {
        await using var fixture = await Fixture.CreateAsync("""
            CREATE TABLE Accounts (tenant INTEGER, id BIGINT, name VARCHAR(80) COLLATE NOCASE NOT NULL DEFAULT 'Unknown',
                CONSTRAINT pk_accounts PRIMARY KEY (tenant, id), CONSTRAINT uq_accounts UNIQUE (name));
            CREATE TABLE Orders (id BIGINT PRIMARY KEY IDENTITY, tenant INTEGER, account_id BIGINT,
                parent_id BIGINT, amount DECIMAL(18,4) DEFAULT 0 CHECK (amount >= 0), stamp ROWVERSION,
                CONSTRAINT fk_account FOREIGN KEY (tenant, account_id) REFERENCES Accounts (tenant, id) ON DELETE CASCADE ON UPDATE CASCADE,
                CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES Orders (id));
            CREATE INDEX ix_amount ON Orders (amount);
            CREATE VIEW order_amounts AS SELECT id, amount FROM Orders;
            CREATE TRIGGER orders_audit AFTER UPDATE ON Orders BEGIN INSERT INTO Accounts (tenant, id, name) VALUES (NEW.tenant, NEW.account_id, 'Audit'); END;
            """);
        var document = await new DatabaseDocumenterService().GenerateAsync(fixture.Client, "Sales", ct: Ct);
        Assert.True(document.IsComplete, string.Join("\n", document.Diagnostics));
        var orders = document.Entries.Single(e => e.Kind == "Table" && e.Name == "Orders");
        Assert.Equal("DECIMAL(18,4)", orders.Columns.Single(c => c.Link.Label == "Orders.amount").Type);
        Assert.Contains("Identity", orders.Columns.Single(c => c.Link.Label == "Orders.id").Attributes);
        Assert.Contains("Rowversion", orders.Columns.Single(c => c.Link.Label == "Orders.stamp").Attributes);
        Assert.Contains("COLLATE", document.Entries.Single(e => e.Kind == "Table" && e.Name == "Accounts").Definition);
        var relationship = orders.Relationships.Single(r => r.Name == "fk_account");
        Assert.Equal(new[] { "Orders.tenant", "Orders.account_id" }, relationship.Columns.Select(p => p.Child.Label));
        Assert.Equal(new[] { "Accounts.tenant", "Accounts.id" }, relationship.Columns.Select(p => p.Parent.Label));
        Assert.Equal("CASCADE", relationship.OnDelete);
        Assert.Contains(document.Entries, e => e.Kind == "Check");
        Assert.Contains(document.Entries, e => e.Kind == "Key");
        var trigger = document.Entries.Single(e => e.Kind == "Trigger");
        Assert.Contains("AFTER UPDATE", trigger.Definition);
        string html = DatabaseDocumentRenderer.RenderHtml(document), markdown = DatabaseDocumentRenderer.RenderMarkdown(document);
        Assert.Contains("Incoming relationships", html);
        Assert.Contains("Outgoing relationships", markdown);
        var anchors = Regex.Matches(html, "id=\"(object-[a-f0-9]+)\"").Select(m => m.Groups[1].Value).ToHashSet();
        Assert.Equal(document.Entries.Count, anchors.Count);
        Assert.All(Regex.Matches(html, "href=\"#(object-[a-f0-9]+)\""), m => Assert.Contains(m.Groups[1].Value, anchors));
        Assert.All(anchors, anchor => Assert.Contains($"id=\"{anchor}\"", markdown));
        Assert.Equal(html, DatabaseDocumentRenderer.RenderHtml(document));
        await fixture.CloseClientAsync();
        await using var db = await Database.OpenAsync(fixture.Path, Ct);
        Assert.DoesNotContain(db.GetTableNames(), n => n.StartsWith("__", StringComparison.Ordinal));
        await using var count = await db.ExecuteAsync("SELECT COUNT(*) FROM Orders", Ct);
        Assert.True(await count.MoveNextAsync(Ct));
        Assert.Equal(0, count.Current[0].AsInteger);
    }

    [Fact]
    public async Task Descriptions_PersistAcrossRenamesAndReopen_AndDeletedObjectsNeverInheritThem()
    {
        await using var fixture = await Fixture.CreateAsync("CREATE TABLE Items (id INTEGER PRIMARY KEY, name TEXT)");
        var generator = new DatabaseDocumenterService();
        var store = new DatabaseDocumentationStore();
        var original = await generator.GenerateAsync(fixture.Client, ct: Ct);
        var column = original.Entries.Single(e => e.Kind == "Column" && e.Name == "name");
        await store.SaveAsync(fixture.Client, column.Id, column.Fingerprint, 0, "Business name 'quoted' λ", Ct);
        await fixture.Client.RenameTableAsync("Items", "Products", Ct);
        await fixture.Client.RenameColumnAsync("Products", "name", "title", Ct);
        await fixture.ReopenClientAsync();
        var renamed = (await generator.GenerateAsync(fixture.Client, ct: Ct)).Entries.Single(e => e.Id == column.Id);
        Assert.Equal("title", renamed.Name);
        Assert.Equal("Business name 'quoted' λ", renamed.Description);
        Assert.False(renamed.NeedsReview);
        await fixture.Client.DropColumnAsync("Products", "title", Ct);
        await fixture.Client.AddColumnAsync("Products", "title", DbType.Text, false, Ct);
        var recreated = await generator.GenerateAsync(fixture.Client, ct: Ct);
        var replacement = recreated.Entries.Single(e => e.Kind == "Column" && e.Name == "title");
        Assert.NotEqual(column.Id, replacement.Id);
        Assert.Null(replacement.Description);
        Assert.Contains(recreated.Diagnostics, d => d.Contains("retained without reassignment", StringComparison.Ordinal));
    }

    [Fact]
    public async Task NameBoundDescriptions_RequireReview_AndRevisionChecksPreventLostUpdatesAndDeletionAba()
    {
        await using var fixture = await Fixture.CreateAsync("CREATE TABLE Items (id INTEGER); CREATE VIEW visible_items AS SELECT id FROM Items;");
        var generator = new DatabaseDocumenterService();
        var store = new DatabaseDocumentationStore();
        var view = (await generator.GenerateAsync(fixture.Client, ct: Ct)).Entries.Single(e => e.Kind == "View");
        await store.SaveAsync(fixture.Client, view.Id, view.Fingerprint, 0, "The visible items", Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.SaveAsync(fixture.Client, view.Id, view.Fingerprint, 0, "Stale", Ct));
        await fixture.Client.UpdateViewAsync(view.Name, view.Name, "SELECT id FROM Items WHERE id > 0", Ct);
        var changed = (await generator.GenerateAsync(fixture.Client, ct: Ct)).Entries.Single(e => e.Id == view.Id);
        Assert.True(changed.NeedsReview);
        Assert.Null(changed.Description);
        Assert.Equal("The visible items", changed.OverrideDescription);
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.SaveAsync(fixture.Client, view.Id, view.Fingerprint, 1, "Stale definition", Ct));
        await store.SaveAsync(fixture.Client, changed.Id, changed.Fingerprint, changed.Revision, changed.OverrideDescription, Ct);
        var reviewed = (await generator.GenerateAsync(fixture.Client, ct: Ct)).Entries.Single(e => e.Id == view.Id);
        Assert.False(reviewed.NeedsReview);
        Assert.Equal(2, reviewed.Revision);
        await store.SaveAsync(fixture.Client, reviewed.Id, reviewed.Fingerprint, 2, null, Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.SaveAsync(fixture.Client, reviewed.Id, reviewed.Fingerprint, 0, "Old creation", Ct));
        var removed = (await generator.GenerateAsync(fixture.Client, ct: Ct)).Entries.Single(e => e.Id == view.Id);
        Assert.Null(removed.Description);
        Assert.Equal(3, removed.Revision);
    }

    [Fact]
    public async Task NativeProcedureDescriptionsAndParameters_RestoreWhenOverrideRemoved()
    {
        await using var fixture = await Fixture.CreateAsync("CREATE TABLE Items (id INTEGER)");
        await fixture.Client.CreateProcedureAsync(new()
        {
            Name = "find_item", BodySql = "SELECT id FROM Items WHERE id = @id", Description = "Find an item", IsEnabled = false,
            Parameters = [new() { Name = "id", Type = DbType.Integer, Required = true, Description = "Item identifier" }],
        }, Ct);
        var generator = new DatabaseDocumenterService();
        var store = new DatabaseDocumentationStore();
        var entry = (await generator.GenerateAsync(fixture.Client, ct: Ct)).Entries.Single(e => e.Kind == "Procedure");
        Assert.Equal("Find an item", entry.Description);
        Assert.Equal("Item identifier", Assert.Single(entry.Parameters).Description);
        Assert.Contains(entry.Facts, f => f.Name == "Enabled" && f.Value == "No");
        await store.SaveAsync(fixture.Client, entry.Id, entry.Fingerprint, 0, "Business override", Ct);
        entry = (await generator.GenerateAsync(fixture.Client, ct: Ct)).Entries.Single(e => e.Kind == "Procedure");
        Assert.Equal("Business override", entry.Description);
        await store.SaveAsync(fixture.Client, entry.Id, entry.Fingerprint, 1, null, Ct);
        entry = (await generator.GenerateAsync(fixture.Client, ct: Ct)).Entries.Single(e => e.Kind == "Procedure");
        Assert.Equal("Find an item", entry.Description);
    }

    [Fact]
    public async Task EmptyCatalogAndCancelledOrInvalidSave_DoNotInitializeCatalogs()
    {
        await using var fixture = await Fixture.CreateAsync("");
        var generator = new DatabaseDocumenterService();
        var empty = await generator.GenerateAsync(fixture.Client, ct: Ct);
        Assert.Empty(empty.Entries);
        Assert.True(empty.IsComplete);
        Assert.Contains("No database objects", DatabaseDocumentRenderer.RenderHtml(empty));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => generator.GenerateAsync(fixture.Client, ct: new CancellationToken(true)));
        await Assert.ThrowsAsync<ArgumentException>(() => new DatabaseDocumentationStore().SaveAsync(fixture.Client, "missing", "", 0, new string('x', 4001), Ct));
        await fixture.CloseClientAsync();
        await using var db = await Database.OpenAsync(fixture.Path, Ct);
        Assert.Empty(db.GetTableNames());
    }

    [Fact]
    public async Task MalformedProcedureParameters_PreservePartialDictionaryWithCoverageWarning()
    {
        await using var fixture = await Fixture.CreateAsync("CREATE TABLE Items (id INTEGER)");
        Assert.Null((await fixture.Client.ExecuteSqlAsync("INSERT INTO __procedures (name, body_sql, params_json, description, is_enabled, created_utc, updated_utc) VALUES ('broken', 'SELECT 1', 'invalid json', NULL, 1, '2026-01-01', '2026-01-01');", Ct)).Error);
        var document = await new DatabaseDocumenterService().GenerateAsync(fixture.Client, ct: Ct);
        Assert.False(document.IsComplete);
        Assert.Contains(document.Entries, e => e.Kind == "Table" && e.Name == "Items");
        Assert.Contains("Incomplete documentation", DatabaseDocumentRenderer.RenderHtml(document));
    }

    [Fact]
    public void OlderMalformedAndHostileMetadata_ProduceSafeExplicitlyIncompleteExports()
    {
        const string hostile = "<script>alert('x')</script> | [click](https://invalid) & λ\nnext";
        var definitions = new[]
        {
            new DefinitionCatalogRecord { Id = "View:a", Kind = "View", Name = hostile, Source = "SELECT '" + new string((char)96, 5) + "' -- </script>" },
            new DefinitionCatalogRecord { Id = "Table:bad", Kind = "Table", Name = "broken", Source = "not json" },
        };
        var document = new DatabaseDocumenterService().Build(new("1", DateTimeOffset.UnixEpoch, definitions, [new("c:\\private\\secret", "password=secret")]), "Data Source=c:\\private\\db", Ct);
        Assert.False(document.IsComplete);
        Assert.All(document.Entries, e => Assert.False(e.CanEdit));
        Assert.Equal("Database", document.DisplayName);
        string html = DatabaseDocumentRenderer.RenderHtml(document), md = DatabaseDocumentRenderer.RenderMarkdown(document);
        Assert.DoesNotContain("<script>alert", html);
        // SQL remains verbatim inside a sufficiently long fence; prose must encode HTML.
        string markdownProse = Regex.Replace(md, @"(?ms)^([\x60]{3,})sql\n.*?^\1\n", "");
        Assert.DoesNotContain("<script>alert", markdownProse);
        Assert.DoesNotContain("password=secret", html);
        Assert.DoesNotContain("private", md);
        Assert.Contains(new string((char)96, 6) + "sql", md);
        Assert.Contains("Incomplete documentation", html);
        Assert.Contains("could not be documented", md);
        Assert.Single(DatabaseDocumenterService.Search(document, "alert", ct: Ct));
        Assert.Single(DatabaseDocumenterService.Search(document, kind: "Table", missingOnly: true, ct: Ct));
    }

    [Fact]
    public async Task UnusualOwnerAndObjectNames_DoNotShareAnnotationIdentities()
    {
        await using var fixture = await Fixture.CreateAsync("""
            CREATE TABLE "a:b" (id INTEGER);
            CREATE TABLE a (id INTEGER);
            CREATE INDEX c ON "a:b" (id);
            CREATE INDEX "b:c" ON a (id);
            CREATE VIEW "x:y" AS SELECT 1;
            CREATE VIEW "x%3Ay" AS SELECT 2;
            """);
        var generator = new DatabaseDocumenterService();
        var document = await generator.GenerateAsync(fixture.Client, ct: Ct);
        Assert.True(document.IsComplete, string.Join("; ", document.Diagnostics));
        Assert.Contains("\"a:b\"", document.Entries.Single(e => e.Kind == "Table" && e.Name == "a:b").Definition);
        Assert.Equal(document.Entries.Count, document.Entries.Select(e => e.Id).Distinct().Count());
        var first = document.Entries.Single(e => e.Kind == "Index" && e.Name == "c");
        await new DatabaseDocumentationStore().SaveAsync(fixture.Client, first.Id, first.Fingerprint, 0, "Only the first index.", Ct);
        document = await generator.GenerateAsync(fixture.Client, ct: Ct);
        Assert.Equal("Only the first index.", document.Entries.Single(e => e.Id == first.Id).Description);
        Assert.Null(document.Entries.Single(e => e.Kind == "Index" && e.Name == "b:c").Description);
        Assert.Equal(document.Entries.Count, document.Entries.Select(e => DatabaseDocumentRenderer.Anchor(e.Id)).Distinct().Count());
    }

    [Fact]
    public void LargeCatalog_SearchAndAnchorsAreDeterministicAndCancellable()
    {
        var records = Enumerable.Range(0, 2000).Select(i => new DefinitionCatalogRecord { Id = "View:" + i, Name = "view_" + i, Kind = "View", Source = "SELECT " + i }).ToArray();
        var document = new DatabaseDocumenterService().Build(new("large", DateTimeOffset.UnixEpoch, records, []), ct: Ct);
        Assert.Equal(2000, document.Entries.Count);
        Assert.Equal(2000, document.Entries.Select(e => DatabaseDocumentRenderer.Anchor(e.Id)).Distinct().Count());
        Assert.Single(DatabaseDocumenterService.Search(document, "view_1999", ct: Ct));
        Assert.Throws<OperationCanceledException>(() => DatabaseDocumenterService.Search(document, ct: new CancellationToken(true)));
    }

    [Fact]
    public async Task UnicodeDescriptionsAndSources_PaginateByBytesEvenBelowRequestedRecordCount()
    {
        string sourceText = new('漢', 7800), description = new('語', 4000);
        string sql = string.Join("\n", Enumerable.Range(0, 48).Select(i => $"CREATE VIEW v{i} AS SELECT '{sourceText}' AS value;"));
        await using var fixture = await Fixture.CreateAsync(sql);
        var catalog = await DefinitionCatalogService.ReadAsync(fixture.Client, ct: Ct);
        var first = catalog.Definitions.First(d => d.Kind == "View");
        await new DatabaseDocumentationStore().SaveAsync(fixture.Client, first.Id, first.Documentation!.DefinitionFingerprint, 0, description, Ct);
        foreach (var record in catalog.Definitions.Where(d => d.Kind == "View" && d.Id != first.Id))
        {
            var result = await fixture.Client.ExecuteSqlAsync($"""
                INSERT INTO __documentation_annotations (object_id, description, definition_hash, revision, updated_utc)
                VALUES ('{record.Id}', '{description}', '{record.Documentation!.DefinitionFingerprint}', 1, '2026-01-01T00:00:00Z');
                """, Ct);
            Assert.Null(result.Error);
        }
        var reader = Assert.IsAssignableFrom<ICSharpDbDefinitionCatalogReader>(fixture.Client);
        var page = await reader.ReadDefinitionCatalogAsync(pageSize: 64, ct: Ct);
        Assert.NotNull(page.ContinuationToken);
        Assert.InRange(page.Records.Count, 1, 47);
        Assert.True(JsonSerializer.SerializeToUtf8Bytes(page, new JsonSerializerOptions(JsonSerializerDefaults.Web)).Length < 3 * 1024 * 1024);
        var next = await reader.ReadDefinitionCatalogAsync(page.ContinuationToken, 64, Ct);
        Assert.Equal(page.CatalogVersion, next.CatalogVersion);
        var complete = await DefinitionCatalogService.ReadAsync(fixture.Client, ct: Ct);
        Assert.Equal(48, complete.Definitions.Count(d => d.Kind == "View"));
        Assert.All(complete.Definitions.Where(d => d.Kind == "View"), d => Assert.Equal(description, d.Documentation!.Description));
    }

    private sealed class Fixture : IAsyncDisposable
    {
        public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"documenter-{Guid.NewGuid():N}.db");
        public ICSharpDbClient Client { get; private set; } = null!;
        public static async Task<Fixture> CreateAsync(string sql)
        {
            var fixture = new Fixture();
            await using (var db = await Database.OpenAsync(fixture.Path, Ct))
                if (sql.Length > 0)
                    foreach (string statement in CSharpDB.Sql.SqlScriptSplitter.SplitExecutableStatements(sql))
                        await db.ExecuteAsync(statement, Ct);
            fixture.Client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = fixture.Path });
            return fixture;
        }
        public async Task CloseClientAsync() => await Client.DisposeAsync();
        public async Task ReopenClientAsync()
        {
            await Client.DisposeAsync();
            Client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = Path });
        }
        public async ValueTask DisposeAsync()
        {
            await Client.DisposeAsync();
            foreach (string file in new[] { Path, Path + ".wal", Path + ".shm" }) if (File.Exists(file)) File.Delete(file);
        }
    }
}
