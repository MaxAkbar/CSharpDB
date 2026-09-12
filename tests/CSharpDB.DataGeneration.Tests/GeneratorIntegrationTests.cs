using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using CSharpDB.Admin.Configuration;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.DataGen;
using CSharpDB.DataGen.Generators;
using CSharpDB.DataGen.Specs;
using CSharpDB.DataGeneration;
using CSharpDB.Primitives;

namespace CSharpDB.DataGeneration.Tests;

public sealed class GeneratorIntegrationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task PreviewAndAppendPreserveCompositeKeysAndExistingRows()
    {
        await using var scope = await Scope.CreateAsync("""
            CREATE TABLE Customers (TenantId INTEGER NOT NULL, Id INTEGER NOT NULL, FirstName TEXT NOT NULL, LastName TEXT NOT NULL, Email TEXT NOT NULL, PRIMARY KEY (TenantId, Id));
            CREATE TABLE Orders (Id INTEGER PRIMARY KEY IDENTITY, TenantId INTEGER NOT NULL, CustomerId INTEGER NOT NULL, Amount DECIMAL(8,2) NOT NULL CHECK (Amount > 0), FOREIGN KEY (TenantId, CustomerId) REFERENCES Customers(TenantId, Id));
            INSERT INTO Customers VALUES (7, 99, 'Existing', 'Person', 'existing@example.test');
            """);
        GenerationProfile profile = await scope.ProfileAsync(("Customers", 7), ("Orders", 19));
        var preview = await scope.Service.PreviewAsync(profile, ct: Ct);
        var repeat = await scope.Service.PreviewAsync(GenerationProfile.FromJson(profile.ToJson()), ct: Ct);
        Assert.Equal(preview.Plan.Rows("Orders", ct: Ct).Select(Canonical), repeat.Plan.Rows("Orders", ct: Ct).Select(Canonical));
        Assert.Equal(preview.Plan.Rows("Orders", ct: Ct).Skip(4).Take(3).Select(Canonical), preview.Plan.Rows("Orders", 4, 3, Ct).Select(Canonical));
        Assert.Equal(1, await scope.CountAsync("Customers"));
        Assert.Equal(0, await scope.CountAsync("Orders"));
        var receipt = await scope.Service.ExecuteAsync(preview, profile.Hash, ct: Ct);
        Assert.Equal("Committed", receipt.Status);
        Assert.Equal(8, await scope.CountAsync("Customers"));
        Assert.Equal(19, await scope.CountAsync("Orders"));
        var storedParents = await scope.Client.ExecuteSqlAsync("SELECT TenantId, Id FROM Customers;", Ct);
        var storedChildren = await scope.Client.ExecuteSqlAsync("SELECT TenantId, CustomerId FROM Orders;", Ct);
        Assert.All(storedChildren.Rows!, child => Assert.Contains(storedParents.Rows!, parent => Equals(parent[0], child[0]) && Equals(parent[1], child[1])));
        var rows = await scope.Client.ExecuteSqlAsync("SELECT Id, TenantId, CustomerId, Amount FROM Orders ORDER BY Id;", Ct);
        Assert.Null(rows.Error);
        Assert.Equal(preview.Plan.Rows("Orders", ct: Ct).Select(r => Convert.ToDecimal(r["Amount"], CultureInfo.InvariantCulture)), rows.Rows!.Select(r => Convert.ToDecimal(r[3], CultureInfo.InvariantCulture)));
    }

    [Fact]
    public async Task ChildOnlyUsesActualNoncontiguousTextParentKeys()
    {
        await using var scope = await Scope.CreateAsync("""
            CREATE TABLE Parents (Code TEXT PRIMARY KEY);
            CREATE TABLE Children (Id INTEGER PRIMARY KEY, ParentCode TEXT NOT NULL REFERENCES Parents(Code));
            INSERT INTO Parents VALUES ('alpha-7'), ('omega-99');
            """);
        var profile = await scope.ProfileAsync(("Children", 20));
        profile.Tables[0].Relationships[0].Source = ParentKeySource.Existing;
        var preview = await scope.Service.PreviewAsync(profile, ct: Ct);
        Assert.All(preview.Plan.Rows("Children", ct: Ct), row => Assert.Contains(row["ParentCode"], new object[] { "alpha-7", "omega-99" }));
        Assert.Equal("Committed", (await scope.Service.ExecuteAsync(preview, profile.Hash, ct: Ct)).Status);
        Assert.Equal(2, await scope.CountAsync("Parents"));
    }

    [Fact]
    public async Task ExistingKeysOrConfigurationChangingAfterPreviewPreventsWrites()
    {
        await using var scope = await Scope.CreateAsync("CREATE TABLE Items (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);");
        var profile = await scope.ProfileAsync(("Items", 10));
        var preview = await scope.Service.PreviewAsync(profile, ct: Ct);
        await scope.SqlAsync("INSERT INTO Items VALUES (500, 'Other user');");
        var receipt = await scope.Service.ExecuteAsync(preview, profile.Hash, ct: Ct);
        Assert.Equal("Rolled back", receipt.Status);
        Assert.Contains("changed", receipt.Message);
        Assert.Equal(1, await scope.CountAsync("Items"));
        profile.Seed++;
        await Assert.ThrowsAsync<InvalidOperationException>(() => scope.Service.ExecuteAsync(preview, profile.Hash, ct: Ct));
    }

    [Fact]
    public async Task CancellationInLaterTableRollsBackEarlierTable()
    {
        await using var scope = await Scope.CreateAsync("CREATE TABLE A (Id INTEGER PRIMARY KEY); CREATE TABLE B (Id INTEGER PRIMARY KEY, AId INTEGER NOT NULL REFERENCES A(Id));");
        var profile = await scope.ProfileAsync(("A", 120), ("B", 120));
        var preview = await scope.Service.PreviewAsync(profile, ct: Ct);
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var progress = new InlineProgress(p => { if (p.Table == "B" && p.Processed > 120) cancel.Cancel(); });
        var receipt = await scope.Service.ExecuteAsync(preview, profile.Hash, progress, cancel.Token);
        Assert.Equal("Rolled back", receipt.Status);
        Assert.Equal(0, await scope.CountAsync("A"));
        Assert.Equal(0, await scope.CountAsync("B"));
    }

    [Fact]
    public async Task ChecksTypesAndCollationUniquenessAreValidatedWithoutWrites()
    {
        await using var scope = await Scope.CreateAsync("CREATE TABLE Items (Id INTEGER PRIMARY KEY, Code VARCHAR(8) NOT NULL COLLATE NOCASE, Price DECIMAL(6,2) NOT NULL CHECK (Price >= 1 AND Price <= 10), UNIQUE (Code));");
        var profile = await scope.ProfileAsync(("Items", 2));
        var code = profile.Tables[0].Columns.Single(c => c.ColumnName == "Code");
        code.Generator = FieldGenerator.Constant; code.Constant = "repeat";
        var price = profile.Tables[0].Columns.Single(c => c.ColumnName == "Price");
        price.Minimum = 1; price.Maximum = 10;
        await Assert.ThrowsAsync<InvalidOperationException>(() => scope.Service.PreviewAsync(profile, ct: Ct));
        Assert.Equal(0, await scope.CountAsync("Items"));
        code.Generator = FieldGenerator.Sequence;
        price.Minimum = 20; price.Maximum = 30;
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => scope.Service.PreviewAsync(profile, ct: Ct));
        Assert.Contains("CHECK", error.Message);
        price.Minimum = 1; price.Maximum = 10;
        var preview = await scope.Service.PreviewAsync(profile, ct: Ct);
        Assert.Equal("Committed", (await scope.Service.ExecuteAsync(preview, profile.Hash, ct: Ct)).Status);
        profile = await scope.ProfileAsync(("Items", 1));
        code = profile.Tables[0].Columns.Single(c => c.ColumnName == "Code"); code.Generator = FieldGenerator.Constant; code.Constant = "1";
        profile.Tables[0].Columns.Single(c => c.ColumnName == "Price").Maximum = 10;
        await Assert.ThrowsAsync<InvalidOperationException>(() => scope.Service.PreviewAsync(profile, ct: Ct));
    }

    [Fact]
    public async Task MissingParentPoolAndGeneratedCycleAreBlocked()
    {
        await using var scope = await Scope.CreateAsync("CREATE TABLE Tree (Id INTEGER PRIMARY KEY, ParentId INTEGER REFERENCES Tree(Id));");
        var profile = await scope.ProfileAsync(("Tree", 2));
        var relationship = profile.Tables[0].Relationships[0];
        relationship.Source = ParentKeySource.Existing;
        var empty = await Assert.ThrowsAsync<InvalidOperationException>(() => scope.Service.PreviewAsync(profile, ct: Ct));
        Assert.Contains("empty", empty.Message);
        relationship.Source = ParentKeySource.Generated;
        var cycle = await Assert.ThrowsAsync<InvalidOperationException>(() => scope.Service.PreviewAsync(profile, ct: Ct));
        Assert.Contains("cycle", cycle.Message);
        relationship.Source = ParentKeySource.Existing; relationship.NullRate = 1;
        var preview = await scope.Service.PreviewAsync(profile, ct: Ct);
        Assert.All(preview.Plan.Rows("Tree", ct: Ct), r => Assert.Null(r["ParentId"]));
    }

    [Fact]
    public async Task LogicalMappingsAndRowversionAreHandled()
    {
        await using var scope = await Scope.CreateAsync("CREATE TABLE Parent (Id INTEGER PRIMARY KEY); CREATE TABLE Child (Id INTEGER PRIMARY KEY, OwnerId INTEGER NOT NULL, Revision ROWVERSION);");
        var profile = await scope.ProfileAsync(("Parent", 3), ("Child", 8));
        profile.Tables.Single(t => t.TableName == "Child").Relationships.Add(new RelationshipGenerationRule
        { Name = "Owner", Columns = ["OwnerId"], ParentTable = "Parent", ParentColumns = ["Id"] });
        var preview = await scope.Service.PreviewAsync(profile, ct: Ct);
        Assert.All(preview.Plan.Rows("Child", ct: Ct), r => Assert.False(r.ContainsKey("Revision")));
        Assert.Equal("Committed", (await scope.Service.ExecuteAsync(preview, profile.Hash, ct: Ct)).Status);
        Assert.Equal(0, await scope.ScalarAsync("SELECT COUNT(*) FROM Child WHERE Revision IS NULL;"));
        Assert.Equal(0, await scope.ScalarAsync("SELECT COUNT(*) FROM Child c LEFT JOIN Parent p ON c.OwnerId = p.Id WHERE p.Id IS NULL;"));
    }

    [Fact]
    public async Task EveryPersonAndAddressProviderExecutesAndPersonFieldsAgree()
    {
        await using var scope = await Scope.CreateAsync("""
            CREATE TABLE People (Id INTEGER PRIMARY KEY, FirstName TEXT NOT NULL, LastName TEXT NOT NULL,
                FullName TEXT NOT NULL, Email VARCHAR(64) NOT NULL, Phone TEXT NOT NULL, Street TEXT NOT NULL,
                City TEXT NOT NULL, Region TEXT NOT NULL, PostalCode TEXT NOT NULL, Country TEXT NOT NULL,
                Company TEXT NOT NULL, Product TEXT NOT NULL, Notes TEXT NOT NULL);
            """);
        var profile = await scope.ProfileAsync(("People", 80));
        profile.Tables[0].Columns.Single(c => c.ColumnName == "Email").Length = 64;
        profile.Tables[0].Columns.Single(c => c.ColumnName == "FullName").Length = 100;
        var preview = await scope.Service.PreviewAsync(profile, ct: Ct);
        var rows = preview.Plan.Rows("People", ct: Ct).ToArray();
        Assert.All(rows, row =>
        {
            Assert.Equal($"{row["FirstName"]} {row["LastName"]}", row["FullName"]);
            Assert.EndsWith("@example.test", (string)row["Email"]!);
            string emailName = new string(((string)row["FirstName"]!).Where(c => char.IsAsciiLetterOrDigit(c) || c == '.').ToArray()).ToLowerInvariant();
            Assert.StartsWith(emailName, (string)row["Email"]!);
        });
        foreach (var column in profile.Tables[0].Columns.Where(c => c.Generator != FieldGenerator.Sequence))
            Assert.True(rows.Select(r => r[column.ColumnName]).Distinct().Count() > 1, column.ColumnName);
        Assert.Equal("Committed", (await scope.Service.ExecuteAsync(preview, profile.Hash, ct: Ct)).Status);
    }

    [Fact]
    public async Task DistributionsAndLogicalTypesRoundTripWithinBounds()
    {
        await using var scope = await Scope.CreateAsync("""
            CREATE TABLE Samples (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Score DECIMAL(5,2) NOT NULL,
                IsEnabled BOOLEAN NOT NULL, OptionalText TEXT, CreatedUtc DATETIMEOFFSET NOT NULL,
                Day DATE NOT NULL, Clock TIME NOT NULL, Token UUID NOT NULL, Payload VARBINARY(16) NOT NULL,
                Tiny TINYINT NOT NULL, Document JSON NOT NULL, Markup XML NOT NULL);
            """);
        var profile = await scope.ProfileAsync(("Samples", 1200));
        var fields = profile.Tables[0].Columns.ToDictionary(c => c.ColumnName);
        fields["Category"].Distribution = FieldDistribution.Weighted;
        fields["Category"].Values = ["Common", "Rare"]; fields["Category"].Weights = [80, 20];
        fields["Score"].Minimum = 0; fields["Score"].Maximum = 100; fields["Score"].Mean = 50;
        fields["Score"].Deviation = 12; fields["Score"].Distribution = FieldDistribution.Normal;
        fields["OptionalText"].NullRate = .25; fields["IsEnabled"].Probability = .7;
        var preview = await scope.Service.PreviewAsync(profile, ct: Ct);
        var rows = preview.Plan.Rows("Samples", ct: Ct).ToArray();
        Assert.InRange(rows.Count(r => Equals(r["Category"], "Common")), 880, 1040);
        Assert.InRange(rows.Count(r => r["OptionalText"] is null), 240, 360);
        Assert.InRange(rows.Count(r => Equals(r["IsEnabled"], 1L)), 760, 920);
        Assert.InRange(rows.Average(r => (decimal)r["Score"]!), 48, 52);
        Assert.All(rows, row =>
        {
            decimal score = (decimal)row["Score"]!;
            Assert.InRange(score, 0, 100); Assert.Equal(score, decimal.Round(score, 2));
            var date = DateTimeOffset.Parse((string)row["CreatedUtc"]!, CultureInfo.InvariantCulture);
            Assert.InRange(date.UtcDateTime, profile.ReferenceUtc.AddDays(-365), profile.ReferenceUtc);
            Assert.Equal(16, ((byte[])row["Token"]!).Length);
            Assert.Equal(16, ((byte[])row["Payload"]!).Length);
            Assert.InRange((long)row["Tiny"]!, 1, 100);
        });
        Assert.InRange(rows.Count(r => DateTimeOffset.Parse((string)r["CreatedUtc"]!, CultureInfo.InvariantCulture).UtcDateTime >= profile.ReferenceUtc.AddDays(-30)), 900, 1070);
        Assert.Equal("Committed", (await scope.Service.ExecuteAsync(preview, profile.Hash, ct: Ct)).Status);
        var stored = await scope.Client.ExecuteSqlAsync("SELECT Score, CreatedUtc, Token FROM Samples ORDER BY Id;", Ct);
        Assert.Null(stored.Error);
        Assert.Equal(rows.Select(r => GenerationValues.Display(r["Score"])), stored.Rows!.Select(r => GenerationValues.Display(r[0])));
        Assert.Equal(rows.Select(r => GenerationValues.Display(r["CreatedUtc"])), stored.Rows!.Select(r => GenerationValues.Display(r[1])));
        profile.Seed++;
        var changed = await scope.Service.PreviewAsync(profile, ct: Ct);
        Assert.NotEqual(rows[0]["Score"], changed.Plan.Rows("Samples", ct: Ct).First()["Score"]);
    }

    [Fact]
    public async Task OverlappingTenantKeysAndOneToOneCapacityAreValidated()
    {
        await using var scope = await Scope.CreateAsync("""
            CREATE TABLE Customers (TenantId INTEGER NOT NULL, Id INTEGER NOT NULL, PRIMARY KEY (TenantId, Id));
            CREATE TABLE Products (TenantId INTEGER NOT NULL, Id INTEGER NOT NULL, PRIMARY KEY (TenantId, Id));
            CREATE TABLE Orders (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, CustomerId INTEGER NOT NULL, ProductId INTEGER NOT NULL,
                FOREIGN KEY (TenantId, CustomerId) REFERENCES Customers(TenantId, Id), FOREIGN KEY (TenantId, ProductId) REFERENCES Products(TenantId, Id));
            INSERT INTO Customers VALUES (1, 11), (2, 22); INSERT INTO Products VALUES (1, 101), (2, 202);
            CREATE TABLE Badges (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, CustomerId INTEGER NOT NULL,
                UNIQUE (TenantId, CustomerId), FOREIGN KEY (TenantId, CustomerId) REFERENCES Customers(TenantId, Id));
            """);
        var profile = await scope.ProfileAsync(("Orders", 50), ("Badges", 2));
        var preview = await scope.Service.PreviewAsync(profile, ct: Ct);
        Assert.All(preview.Plan.Rows("Orders", ct: Ct), row =>
        {
            Assert.Equal((long)row["TenantId"]! * 11, row["CustomerId"]);
            Assert.Equal((long)row["TenantId"]! * 101, row["ProductId"]);
        });
        Assert.Equal(2, preview.Plan.Rows("Badges", ct: Ct).Select(r => r["CustomerId"]).Distinct().Count());
        profile.Tables.Single(t => t.TableName == "Badges").Rows = 3;
        await Assert.ThrowsAsync<InvalidOperationException>(() => scope.Service.PreviewAsync(profile, ct: Ct));
        profile.Tables.Single(t => t.TableName == "Badges").Rows = 2;
        Assert.Equal("Committed", (await scope.Service.ExecuteAsync(preview, profile.Hash, ct: Ct)).Status);
    }

    [Fact]
    public async Task OneToOneAppendSkipsAlreadyUsedParentKeys()
    {
        await using var scope = await Scope.CreateAsync("""
            CREATE TABLE Parents (Id INTEGER PRIMARY KEY);
            CREATE TABLE Badges (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL REFERENCES Parents(Id), UNIQUE (ParentId));
            INSERT INTO Parents VALUES (11), (22), (33); INSERT INTO Badges VALUES (7, 11);
            """);
        var profile = await scope.ProfileAsync(("Badges", 2));
        var preview = await scope.Service.PreviewAsync(profile, ct: Ct);
        Assert.All(preview.Plan.Rows("Badges", ct: Ct), row => Assert.NotEqual(11L, row["ParentId"]));
        Assert.Equal("Committed", (await scope.Service.ExecuteAsync(preview, profile.Hash, ct: Ct)).Status);
    }

    [Fact]
    public async Task WeightedTypedListsPreserveDecimalPrecisionAndBinaryValues()
    {
        await using var scope = await Scope.CreateAsync("CREATE TABLE ValuesTable (Id INTEGER PRIMARY KEY, Amount DECIMAL(18,2) NOT NULL, Token UUID NOT NULL, Payload VARBINARY(4) NOT NULL);");
        var profile = await scope.ProfileAsync(("ValuesTable", 4));
        var fields = profile.Tables[0].Columns.ToDictionary(c => c.ColumnName);
        foreach (string column in new[] { "Amount", "Token", "Payload" })
        {
            fields[column].Generator = FieldGenerator.ValueList; fields[column].Distribution = FieldDistribution.Weighted;
            fields[column].Weights = [1, 0];
        }
        fields["Amount"].Values = ["1234567890123456.78", "0"];
        fields["Token"].Values = ["12345678-1234-1234-1234-123456789abc", "00000000-0000-0000-0000-000000000000"];
        fields["Payload"].Values = ["01a2ff00", "00"];
        var preview = await scope.Service.PreviewAsync(profile, ct: Ct);
        Assert.All(preview.Plan.Rows("ValuesTable", ct: Ct), row =>
        {
            Assert.Equal(1234567890123456.78m, row["Amount"]);
            Assert.Equal(Guid.Parse(fields["Token"].Values[0]).ToByteArray(), row["Token"]);
            Assert.Equal(new byte[] { 1, 162, 255, 0 }, row["Payload"]);
        });
        Assert.Equal("Committed", (await scope.Service.ExecuteAsync(preview, profile.Hash, ct: Ct)).Status);
        var stored = await scope.Client.ExecuteSqlAsync("SELECT Amount FROM ValuesTable;", Ct);
        Assert.All(stored.Rows!, row => Assert.Equal(1234567890123456.78m, row[0]));
    }

    [Fact]
    public async Task InternalTablesTriggersUnsupportedChecksAndOversizedRowsAreRejected()
    {
        await using var scope = await Scope.CreateAsync("""
            CREATE TABLE __private (Id INTEGER PRIMARY KEY); CREATE TABLE Audit (Id INTEGER);
            CREATE TABLE Triggered (Id INTEGER PRIMARY KEY);
            CREATE TRIGGER audit_insert AFTER INSERT ON Triggered BEGIN INSERT INTO Audit VALUES (NEW.Id); END;
            CREATE TABLE Checked (Id INTEGER PRIMARY KEY, Name TEXT CHECK (Name LIKE 'A%'));
            CREATE TABLE Wide (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """);
        Assert.DoesNotContain(await scope.Service.ReadCatalogAsync(Ct), t => t.Schema.TableName == "__private");
        foreach (string name in new[] { "Triggered", "Checked" })
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await scope.Service.PreviewAsync(await scope.ProfileAsync((name, 1)), ct: Ct));
        Assert.Equal(0, await scope.CountAsync("Audit"));
        var profile = await scope.ProfileAsync(("Wide", 1));
        var field = profile.Tables[0].Columns.Single(c => c.ColumnName == "Name"); field.Generator = FieldGenerator.Constant; field.Constant = new string('x', 2000);
        var limited = new TestDataGenerationAdminService(scope.Holder, new DatabaseChangeService(), new GenerationLimits { MaxStatementBytes = 1024 });
        await Assert.ThrowsAsync<InvalidOperationException>(() => limited.PreviewAsync(profile, ct: Ct));
        Assert.Equal(0, await scope.CountAsync("Wide"));
        field.Constant = "small";
        var preview = await limited.PreviewAsync(profile, ct: Ct);
        Assert.Equal("Committed", (await limited.ExecuteAsync(preview, profile.Hash, ct: Ct)).Status);
    }

    [Fact]
    public async Task QuotedNamesDefaultsAndCaseInsensitiveExistingCollisionsAreHandled()
    {
        await using var scope = await Scope.CreateAsync("""
            CREATE TABLE "Order Details" ("row key" INTEGER PRIMARY KEY, Code TEXT NOT NULL COLLATE NOCASE,
                Amount INTEGER NOT NULL DEFAULT 7, UNIQUE (Code));
            INSERT INTO "Order Details" VALUES (10, 'ALPHA', 7);
            """);
        var profile = await scope.ProfileAsync(("Order Details", 1));
        var fields = profile.Tables[0].Columns.ToDictionary(c => c.ColumnName);
        fields["Amount"].Generator = FieldGenerator.Default;
        fields["Code"].Generator = FieldGenerator.Constant; fields["Code"].Constant = "alpha";
        await Assert.ThrowsAsync<InvalidOperationException>(() => scope.Service.PreviewAsync(profile, ct: Ct));
        fields["Code"].Constant = "O'Brien";
        var preview = await scope.Service.PreviewAsync(profile, ct: Ct);
        Assert.Equal(7L, preview.Plan.Rows("Order Details", ct: Ct).First()["Amount"]);
        Assert.Equal("Committed", (await scope.Service.ExecuteAsync(preview, profile.Hash, ct: Ct)).Status);
    }

    [Theory]
    [InlineData("{\"Version\":2}")]
    [InlineData("{\"Algorithm\":\"old\"}")]
    [InlineData("{\"Tables\":[null]}")]
    [InlineData("{\"Tables\":[{\"TableName\":\"T\",\"Columns\":null}]}")]
    public void IncompleteOrIncompatibleProfilesHaveActionableErrors(string json)
        => Assert.Throws<InvalidOperationException>(() => GenerationProfile.FromJson(json));

    [Fact]
    public async Task CliInferenceHonorsTypesAndRandomizedFieldsVary()
    {
        await using var scope = await Scope.CreateAsync("""
            CREATE TABLE Inferred (Id INTEGER PRIMARY KEY, Email INTEGER NOT NULL, Company TEXT NOT NULL, State TEXT NOT NULL,
                Country TEXT NOT NULL, Website TEXT NOT NULL, Username TEXT NOT NULL, Label VARCHAR(12) NOT NULL,
                Price DECIMAL(4,2) NOT NULL, IsEnabled BOOLEAN NOT NULL, Clock TIME NOT NULL, Token UUID NOT NULL);
            """);
        string databasePath = scope.Client.DataSource;
        await scope.Holder.DisposeAsync();
        var spec = await SchemaInferredSpecBuilder.BuildFromDatabaseAsync(databasePath, Ct);
        var plan = SpecDataGenerator.CreatePlan(new GenerationOptions { RowCount = 30 }, spec);
        var table = Assert.Single(spec.Tables);
        var rows = plan.SqlSources[table.GeneratorKey].CreateRows().ToArray();
        Assert.Equal(30, rows.Length);
        foreach (var row in rows)
        {
            var typed = SqlSpecBuilder.BuildDbValues(table, row);
            Assert.All(typed, value => Assert.False(value.IsNull));
            Assert.InRange(typed[8].AsDecimal, 0, 99.99m);
            Assert.True(typed[7].AsText.Length <= 12);
        }
        foreach (string column in new[] { "Email", "Company", "State", "Country", "Website", "Username" })
            Assert.True(rows.Select(r => r[column]).Distinct().Count() > 1, column);
    }

    [Fact]
    public async Task HybridRunAtDefaultRowLimitCommitsAndRecordsTiming()
    {
        await using var scope = await Scope.CreateAsync("""
            CREATE TABLE Customers (Id INTEGER PRIMARY KEY IDENTITY, FirstName TEXT NOT NULL, LastName TEXT NOT NULL, Email TEXT NOT NULL);
            CREATE TABLE Orders (Id INTEGER PRIMARY KEY IDENTITY, CustomerId INTEGER NOT NULL REFERENCES Customers(Id), Amount DECIMAL(8,2) NOT NULL CHECK (Amount > 0));
            CREATE INDEX ix_customer ON Orders(CustomerId);
            """, hybrid: true);
        var profile = await scope.ProfileAsync(("Customers", 1000), ("Orders", 9000));
        var timer = Stopwatch.StartNew();
        var preview = await scope.Service.PreviewAsync(profile, ct: Ct);
        var previewDuration = timer.Elapsed;
        var receipt = await scope.Service.ExecuteAsync(preview, profile.Hash, ct: Ct);
        Assert.Equal("Committed", receipt.Status);
        Assert.Equal(10000, receipt.InsertedRows.Values.Sum());
        TestContext.Current.TestOutputHelper!.WriteLine($"Hybrid incremental durable: preview={previewDuration.TotalSeconds:F2}s; execute={receipt.Duration.TotalSeconds:F2}s; generated estimate={preview.Plan.GeneratedBytes} bytes; {Environment.OSVersion}; CPUs={Environment.ProcessorCount}.");
        profile.Tables[1].Rows++;
        await Assert.ThrowsAsync<InvalidOperationException>(() => scope.Service.PreviewAsync(profile, ct: Ct));
    }

    [Fact]
    public async Task LateSqlErrorRollsBackAndLostCommitAcknowledgementRemainsUnknown()
    {
        await using var scope = await Scope.CreateAsync("CREATE TABLE A (Id INTEGER PRIMARY KEY); CREATE TABLE B (Id INTEGER PRIMARY KEY);", intercept: true);
        var proxy = (FaultProxy)(object)scope.Client;
        var profile = await scope.ProfileAsync(("A", 3), ("B", 3));
        var preview = await scope.Service.PreviewAsync(profile, ct: Ct);
        proxy.FailLateInsert = true;
        var failed = await scope.Service.ExecuteAsync(preview, profile.Hash, ct: Ct);
        Assert.Equal("Rolled back", failed.Status);
        Assert.Contains("injected", failed.Message);
        Assert.Equal(0, await scope.CountAsync("A")); Assert.Equal(0, await scope.CountAsync("B"));
        proxy.FailLateInsert = false; proxy.LoseCommitAcknowledgement = true;
        var unknown = await scope.Service.ExecuteAsync(preview, profile.Hash, ct: Ct);
        Assert.Equal("Unknown", unknown.Status);
        Assert.Empty(unknown.InsertedRows);
        Assert.Equal(1, proxy.CommitCalls);
        Assert.Equal(3, await scope.CountAsync("A")); Assert.Equal(3, await scope.CountAsync("B"));
    }

    [Fact]
    public async Task DatabaseSwitchDuringRunRollsBackOriginalWithoutWritingToReplacement()
    {
        await using var scope = await Scope.CreateAsync("CREATE TABLE A (Id INTEGER PRIMARY KEY); CREATE TABLE B (Id INTEGER PRIMARY KEY);");
        string original = scope.Client.DataSource;
        string replacement = Path.Combine(Path.GetDirectoryName(original)!, "replacement.db");
        var profile = await scope.ProfileAsync(("A", 120), ("B", 120));
        var preview = await scope.Service.PreviewAsync(profile, ct: Ct);
        using var release = new SemaphoreSlim(0);
        var inserting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var switched = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        scope.Holder.DatabaseChanged += () => switched.TrySetResult();
        int reported = 0;
        var progress = new InlineProgress(p =>
        {
            if (Interlocked.Increment(ref reported) == 1) { inserting.TrySetResult(); release.Wait(Ct); }
        });
        Task<GenerationReceipt> running = Task.Run(() => scope.Service.ExecuteAsync(preview, profile.Hash, progress, Ct), Ct);
        await inserting.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        Task switching = scope.Holder.SwitchAsync(replacement);
        try { await switched.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct); }
        finally { release.Release(); }
        Assert.Equal("Rolled back", (await running).Status);
        await switching;
        Assert.Empty(await scope.Holder.GetTableNamesAsync(Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => scope.Service.ExecuteAsync(preview, profile.Hash, ct: Ct));
        await using var originalClient = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = original });
        var count = await originalClient.ExecuteSqlAsync("SELECT COUNT(*) FROM A;", Ct);
        Assert.Equal(0L, count.Rows![0][0]);
    }

    [Theory]
    [InlineData("relational", "relational.dataset.json")]
    [InlineData("docs", "documents.dataset.json")]
    [InlineData("timeseries", "timeseries.dataset.json")]
    public async Task CliIsRepeatableAcrossFreshProcesses(string dataset, string spec)
    {
        string folder = Path.Combine(Path.GetTempPath(), "CSharpDB-DataGeneration-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(folder);
        try
        {
            foreach (string output in new[] { "a", "b" })
            {
                var start = new ProcessStartInfo("dotnet") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false, CreateNoWindow = true };
                foreach (string argument in new[] { typeof(DataGenOptions).Assembly.Location, dataset, "--rows", "4", "--seed", "1987", "--output-path", Path.Combine(folder, output), "--spec-path", Path.Combine(AppContext.BaseDirectory, "Specs", spec) }) start.ArgumentList.Add(argument);
                using var process = Process.Start(start)!;
                Task<string> stdout = process.StandardOutput.ReadToEndAsync(Ct), stderr = process.StandardError.ReadToEndAsync(Ct);
                await process.WaitForExitAsync(Ct);
                Assert.True(process.ExitCode == 0, await stdout + await stderr);
            }
            var files = Directory.GetFiles(Path.Combine(folder, "a")).Where(f => Path.GetFileName(f) != "summary.json").ToArray();
            Assert.NotEmpty(files);
            foreach (string file in files) Assert.Equal(await File.ReadAllBytesAsync(file, Ct), await File.ReadAllBytesAsync(Path.Combine(folder, "b", Path.GetFileName(file)), Ct));
        }
        finally { Directory.Delete(folder, recursive: true); }
    }

    private static string Canonical(IReadOnlyDictionary<string, object?> row) => string.Join("|", row.OrderBy(p => p.Key).Select(p => $"{p.Key}={GenerationValues.Display(p.Value)}"));
    private sealed class InlineProgress(Action<GenerationProgress> action) : IProgress<GenerationProgress> { public void Report(GenerationProgress value) => action(value); }

    public interface ITestClient : ICSharpDbClient, ICSharpDbTransactionalSnapshotReader;
    public class FaultProxy : DispatchProxy
    {
        public ICSharpDbClient Inner = null!;
        public bool FailLateInsert, LoseCommitAcknowledgement;
        public int CommitCalls;
        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            if (method!.Name == nameof(ICSharpDbClient.ExecuteInTransactionAsync) && FailLateInsert
                && ((string)args![1]!).StartsWith("INSERT INTO \"B\"", StringComparison.Ordinal))
                return Task.FromResult(new CSharpDB.Client.Models.SqlExecutionResult { Error = "injected late SQL failure" });
            if (method.Name == nameof(ICSharpDbClient.CommitTransactionAsync))
            {
                CommitCalls++;
                if (LoseCommitAcknowledgement) return CommitAndLoseResponseAsync((string)args![0]!, (CancellationToken)args[1]!);
            }
            try { return method.Invoke(Inner, args); }
            catch (TargetInvocationException error) when (error.InnerException is not null)
            { System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error.InnerException).Throw(); throw; }
        }
        private async Task CommitAndLoseResponseAsync(string transaction, CancellationToken ct)
        {
            await Inner.CommitTransactionAsync(transaction, ct);
            throw new IOException("injected lost commit acknowledgement");
        }
    }

    private sealed class Scope : IAsyncDisposable
    {
        private readonly string _directory;
        public ICSharpDbClient Client { get; }
        public DatabaseClientHolder Holder { get; }
        public TestDataGenerationAdminService Service { get; }
        private Scope(string directory, ICSharpDbClient client)
        {
            _directory = directory; Client = client;
            Holder = new(client, null, new CSharpDbClientOptions { DataSource = Path.Combine(directory, "test.db") }, new AdminHostDatabaseOptions(), DbFunctionRegistry.Empty);
            Service = new(Holder, new DatabaseChangeService(), new GenerationLimits());
        }
        public static async Task<Scope> CreateAsync(string sql, bool hybrid = false, bool intercept = false)
        {
            string directory = Path.Combine(Path.GetTempPath(), "CSharpDB-GeneratorTests-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            ICSharpDbClient client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = Path.Combine(directory, "test.db"),
                HybridDatabaseOptions = hybrid ? new CSharpDB.Engine.HybridDatabaseOptions() : null });
            if (intercept)
            {
                var proxy = DispatchProxy.Create<ITestClient, FaultProxy>();
                ((FaultProxy)(object)proxy).Inner = client; client = proxy;
            }
            var scope = new Scope(directory, client);
            try { await scope.SqlAsync(sql); return scope; } catch { await scope.DisposeAsync(); throw; }
        }
        public async Task SqlAsync(string sql)
        {
            foreach (string statement in CSharpDB.Sql.SqlScriptSplitter.SplitExecutableStatements(sql))
            {
                var result = await Client.ExecuteSqlAsync(statement, Ct);
                Assert.True(string.IsNullOrEmpty(result.Error), result.Error);
            }
        }
        public async Task<long> ScalarAsync(string sql)
        {
            var result = await Client.ExecuteSqlAsync(sql, Ct); Assert.True(string.IsNullOrEmpty(result.Error), result.Error);
            return Convert.ToInt64(result.Rows![0][0], CultureInfo.InvariantCulture);
        }
        public Task<long> CountAsync(string table) => ScalarAsync($"SELECT COUNT(*) FROM {SqlIdentifierRules.Quote(table)};");
        public async Task<GenerationProfile> ProfileAsync(params (string Name, int Count)[] selected)
        {
            var catalog = await Service.ReadCatalogAsync(Ct);
            var names = selected.Select(t => t.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
            return new GenerationProfile { Tables = selected.Select(table =>
            {
                var rule = GenerationPlan.Suggest(catalog.Single(t => t.Schema.TableName == table.Name), names); rule.Rows = table.Count; return rule;
            }).ToList() };
        }
        public async ValueTask DisposeAsync()
        {
            await Holder.DisposeAsync();
            for (int retry = 0; ; retry++)
                try { Directory.Delete(_directory, true); break; }
                catch (IOException) when (retry < 5) { await Task.Delay(100); }
        }
    }
}
