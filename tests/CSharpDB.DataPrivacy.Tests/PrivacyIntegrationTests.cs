using System.Globalization;
using System.Reflection;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.DataPrivacy;
using CSharpDB.Sql;

namespace CSharpDB.DataPrivacy.Tests;

public sealed class PrivacyIntegrationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static readonly DateTimeOffset Reference = new(2026, 9, 20, 0, 0, 0, TimeSpan.Zero);
    private const string People = "CREATE TABLE People (Id INTEGER PRIMARY KEY, Name TEXT, Email TEXT, LastSeen TEXT, Version ROWVERSION); INSERT INTO People (Id,Name,Email,LastSeen) VALUES (1,'Alice Secret','alice@private.test','2020-01-01'),(2,'Bob Secret','bob@private.test','2026-09-19');";

    [Fact]
    public async Task OneEmailPolicyMasksTwoThousandCustomersWithoutDateRules()
    {
        await using var db = await Scope.CreateAsync("CREATE TABLE Customers (Id INTEGER PRIMARY KEY, Email TEXT);");
        foreach (var batch in Enumerable.Range(1, 2000).Chunk(200))
            await db.Sql("INSERT INTO Customers VALUES " + string.Join(",", batch.Select(id => $"({id},'customer{id}@example.test')")) + ";");
        var policy = await db.Save(new PrivacyPolicy
        {
            Name = "Anonymize customer email", RootTable = "Customers",
            Eligibility = new() { Kind = PrivacyConditionKind.IsNotNull, Column = "Email" },
            Targets = [new() { Table = "Customers", Columns = [new() { Column = "Email", Kind = PrivacyMaskKind.Anonymous }] }]
        });
        using var preview = await db.Preview(policy);
        Assert.Equal(2000, preview.EligibleRecords); Assert.Equal(2000, Assert.Single(preview.Tables).ChangedRows);
        Assert.Equal("Committed", (await db.Service.ApplyAsync(db.Client, preview, policy, ct: Ct)).Status);
        Assert.Equal(2000L, await db.Value("SELECT COUNT(*) FROM Customers WHERE Email LIKE 'anonymous-%';"));
        using var repeated = await db.Preview(policy); Assert.Equal(0, Assert.Single(repeated.Tables).ChangedRows);
    }

    [Fact]
    public async Task EcommerceMasksRelatedCopiesAndPreservesAccounting()
    {
        await using var db = await Scope.CreateAsync("""
            CREATE TABLE Customers (Tenant INTEGER NOT NULL, Id INTEGER NOT NULL, Name TEXT, Email TEXT, LastSeen TEXT, PRIMARY KEY (Tenant,Id));
            CREATE TABLE Orders (Id INTEGER PRIMARY KEY, Tenant INTEGER, CustomerId INTEGER, Address TEXT, OrderedAt TEXT, Amount DECIMAL(8,2), Tax DECIMAL(8,2), FOREIGN KEY (Tenant,CustomerId) REFERENCES Customers(Tenant,Id));
            CREATE TABLE Disputes (Id INTEGER PRIMARY KEY, Tenant INTEGER, CustomerId INTEGER, Status TEXT);
            CREATE TABLE Ledger (Id INTEGER PRIMARY KEY, OrderId INTEGER REFERENCES Orders(Id), Amount DECIMAL(8,2));
            INSERT INTO Customers VALUES (1,1,'Alice Secret','alice@private.test','2020-01-01'),(1,2,'Bob Secret','bob@private.test','2020-01-01'),(1,3,'C Secret','c@private.test','2020-01-01');
            INSERT INTO Orders VALUES (1,1,1,'Secret Street','2020-01-01',99.50,8.50),(2,1,2,'Other Street','2026-09-19',42.50,3.50);
            INSERT INTO Disputes VALUES (1,1,3,'open');
            INSERT INTO Ledger VALUES (1,1,108.00),(2,2,46.00);
            """);
        var policy = Policy("Customers");
        policy.Relationships = [Relation("orders", "Customers", ["Tenant", "Id"], "Orders", ["Tenant", "CustomerId"]), Relation("disputes", "Customers", ["Tenant", "Id"], "Disputes", ["Tenant", "CustomerId"])];
        policy.Eligibility.Children.Add(new() { Kind = PrivacyConditionKind.NotExists, RelationshipId = "orders", Children = [new() { Kind = PrivacyConditionKind.WithinLast, Column = "OrderedAt", Days = 365 }] });
        policy.Eligibility.Children.Add(new() { Kind = PrivacyConditionKind.NotExists, RelationshipId = "disputes", Children = [new() { Kind = PrivacyConditionKind.Compare, Column = "Status", Value = "open" }] });
        policy.Targets.Add(new() { Table = "Orders", RelationshipPath = ["orders"], Columns = [new() { Column = "Address", Kind = PrivacyMaskKind.Erase }] });
        policy.Targets[0].Columns.Add(new() { Column = "Email", Kind = PrivacyMaskKind.Anonymous });
        policy = await db.Save(policy);
        using var preview = await db.Preview(policy);
        Assert.Equal(1, preview.EligibleRecords);
        Assert.Equal("Alice Secret", await db.Value("SELECT Name FROM Customers WHERE Id=1;"));
        Assert.Equal(2, preview.Tables.Sum(t => t.ChangedRows));
        var receipt = await db.Service.ApplyAsync(db.Client, preview, policy, ct: Ct);
        Assert.Equal("Committed", receipt.Status);
        Assert.Equal("Anonymous", await db.Value("SELECT Name FROM Customers WHERE Id=1;"));
        Assert.Equal("Bob Secret", await db.Value("SELECT Name FROM Customers WHERE Id=2;"));
        Assert.Equal("C Secret", await db.Value("SELECT Name FROM Customers WHERE Id=3;"));
        Assert.Null(await db.Value("SELECT Address FROM Orders WHERE Id=1;"));
        Assert.Equal(142m, await db.Value("SELECT SUM(Amount) FROM Orders;"));
        Assert.Equal(12m, await db.Value("SELECT SUM(Tax) FROM Orders;"));
        Assert.Equal(154m, await db.Value("SELECT SUM(Amount) FROM Ledger;"));
        Assert.Equal(3L, await db.Value("SELECT COUNT(*) FROM Customers;"));
        Assert.Equal(2L, await db.Value("SELECT COUNT(*) FROM Ledger l JOIN Orders o ON l.OrderId=o.Id;"));
        Assert.Equal(2L, await db.Value("SELECT COUNT(*) FROM Orders o JOIN Customers c ON o.Tenant=c.Tenant AND o.CustomerId=c.Id;"));
        using var repeat = await db.Preview(policy);
        Assert.Equal(0, repeat.Tables.Sum(t => t.ChangedRows));
        string summary = JsonSerializer.Serialize(receipt);
        Assert.DoesNotContain("Secret", summary); Assert.DoesNotContain("private.test", summary);
        var savedReceipt = await db.Store.FindReceiptAsync(db.Client, receipt.RunId, Ct);
        Assert.Equal("Committed", savedReceipt!.Status);
    }

    [Theory]
    [InlineData(PrivacyMaskKind.Erase, "Alice Secret", null, 0, 0)]
    [InlineData(PrivacyMaskKind.Constant, "Alice Secret", "Anonymous", 0, 0)]
    [InlineData(PrivacyMaskKind.Partial, "A👩‍💻éZ", "A**Z", 1, 1)]
    [InlineData(PrivacyMaskKind.Email, "alice@example.test", "*****@example.test", 0, 0)]
    public async Task MasksHandleUnicodeAndAreIdempotent(PrivacyMaskKind kind, string original, string? expected, int prefix, int suffix)
    {
        await using var db = await Scope.CreateAsync(People);
        await db.Sql($"UPDATE People SET Name='{original.Replace("'", "''")}' WHERE Id=1;");
        var policy = Policy(); policy.Targets[0].Columns[0] = new() { Column = "Name", Kind = kind, KeepPrefix = prefix, KeepSuffix = suffix };
        policy = await db.Save(policy); using var preview = await db.Preview(policy);
        Assert.Equal("Committed", (await db.Service.ApplyAsync(db.Client, preview, policy, ct: Ct)).Status);
        Assert.Equal(expected, await db.Value("SELECT Name FROM People WHERE Id=1;"));
        using var repeat = await db.Preview(policy); Assert.Equal(0, repeat.Tables.Sum(t => t.ChangedRows));
    }

    [Fact]
    public async Task AgeBoundariesOffsetsMissingInvalidAndNestedConditions()
    {
        await using var db = await Scope.CreateAsync("""
            CREATE TABLE People (Id INTEGER PRIMARY KEY, Name TEXT, LastSeen TEXT);
            INSERT INTO People VALUES (1,'before','2025-09-19T23:59:59Z'),(2,'boundary','2025-09-20T00:00:00Z'),(3,'offset','2025-09-20T01:00:00+02:00'),(4,'missing',NULL),(5,'invalid','not-a-date'),(6,'future','2030-01-01');
            """);
        var policy = Policy(); policy.Eligibility.Children.Add(new() { Kind = PrivacyConditionKind.Any, Children = [new() { Kind = PrivacyConditionKind.Compare, Column = "Id", Comparison = PrivacyComparison.Less, Value = "4" }, new() { Kind = PrivacyConditionKind.IsNull, Column = "Name" }] });
        policy = await db.Save(policy); using var preview = await db.Preview(policy);
        Assert.Equal(2, preview.EligibleRecords); Assert.Single(preview.Warnings);
    }

    [Theory]
    [InlineData(PrivacyDateEncoding.UnixSeconds, "1577836800", "", "UTC")]
    [InlineData(PrivacyDateEncoding.UnixMilliseconds, "1577836800000", "", "UTC")]
    [InlineData(PrivacyDateEncoding.ExactFormat, "01/01/2020 12:30", "MM/dd/yyyy HH:mm", "America/Los_Angeles")]
    public async Task ExplicitDateEncodings(PrivacyDateEncoding encoding, string value, string format, string zone)
    {
        await using var db = await Scope.CreateAsync(People); await db.Sql($"UPDATE People SET LastSeen='{value}' WHERE Id=1;");
        var policy = Policy(); var age = policy.Eligibility.Children[0]; age.DateEncoding = encoding; age.DateFormat = format; age.TimeZoneId = zone;
        policy = await db.Save(policy); using var preview = await db.Preview(policy); Assert.Equal(1, preview.EligibleRecords);
    }

    [Theory]
    [InlineData("NULL")]
    [InlineData("'not-a-date'")]
    public async Task UnknownActivityDatesCannotEstablishNoRecentOrders(string date)
    {
        await using var db = await Scope.CreateAsync(People + $" CREATE TABLE Orders (Id INTEGER PRIMARY KEY, Person INTEGER, OrderedAt TEXT); INSERT INTO Orders VALUES (1,1,{date});");
        var policy = Policy(); policy.Relationships = [Relation("orders", "People", ["Id"], "Orders", ["Person"])];
        policy.Eligibility.Children.Add(new() { Kind = PrivacyConditionKind.NotExists, RelationshipId = "orders", Children = [new() { Kind = PrivacyConditionKind.WithinLast, Column = "OrderedAt", Days = 365 }] });
        policy = await db.Save(policy); using var preview = await db.Preview(policy);
        Assert.Equal(0, preview.EligibleRecords);
        Assert.Equal("Alice Secret", await db.Value("SELECT Name FROM People WHERE Id=1;"));
    }

    [Theory]
    [InlineData("11/02/2025 01:30")]
    [InlineData("03/09/2025 02:30")]
    public async Task AmbiguousAndMissingLocalTimesAreExcluded(string date)
    {
        await using var db = await Scope.CreateAsync(People);
        await db.Sql($"UPDATE People SET LastSeen='{date}' WHERE Id=1;");
        var policy = Policy(); var age = policy.Eligibility.Children[0];
        age.DateEncoding = PrivacyDateEncoding.ExactFormat; age.DateFormat = "MM/dd/yyyy HH:mm";
        age.TimeZoneId = "America/Los_Angeles"; age.Days = 1;
        policy = await db.Save(policy); using var preview = await db.Preview(policy);
        Assert.Single(preview.Warnings); Assert.Equal(0, preview.EligibleRecords);
    }

    [Fact]
    public async Task AnotherConnectionAfterFileHandoffInvalidatesPreview()
    {
        await using var db = await Scope.CreateAsync(People); var policy = await db.Save(Policy());
        using var preview = await db.Preview(policy);
        // Direct engine handles are not shared-file coordinators. Release the cached handle
        // before a separate owner opens the file, retaining the original client/preview identity.
        await ((CSharpDbClient)db.Client).ReleaseCachedDatabaseAsync(Ct);
        await using (var other = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = db.Client.DataSource }))
            Assert.Null((await other.ExecuteSqlAsync("UPDATE People SET Name='Changed elsewhere' WHERE Id=1;", Ct)).Error);
        Assert.Equal("Rolled back", (await db.Service.ApplyAsync(db.Client, preview, policy, ct: Ct)).Status);
        Assert.Equal("Changed elsewhere", await db.Value("SELECT Name FROM People WHERE Id=1;"));
    }

    [Fact]
    public async Task ExclusiveSessionQueuesSameClientAndRejectsOtherFileWriters()
    {
        await using var db = await Scope.CreateAsync(People);
        var lease = await ((ICSharpDbExclusiveSessionProvider)db.Client).OpenExclusiveSessionAsync(Ct);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Ct); timeout.CancelAfter(TimeSpan.FromSeconds(10));
        var queued = db.Client.ExecuteSqlAsync("UPDATE People SET Name='Queued' WHERE Id=1;", timeout.Token);
        try
        {
            Assert.False(queued.IsCompleted);
            await using var other = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = db.Client.DataSource });
            await Assert.ThrowsAsync<IOException>(() => other.ExecuteSqlAsync("UPDATE People SET Name='Outside' WHERE Id=1;", timeout.Token));
        }
        finally { await lease.DisposeAsync(); }
        Assert.Null((await queued).Error);
        Assert.Equal("Queued", await db.Value("SELECT Name FROM People WHERE Id=1;"));
    }

    [Fact]
    public async Task PrivacyPreviewBlocksWhileAnotherWriterOwnsTheFile()
    {
        await using var db = await Scope.CreateAsync(People); var policy = await db.Save(Policy());
        await using var other = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = db.Client.DataSource });
        Assert.Null((await other.ExecuteSqlAsync("SELECT Id FROM People;", Ct)).Error);
        Assert.Contains("Exclusive database access", (await Assert.ThrowsAsync<PrivacyException>(() => db.Preview(policy))).Message);
    }

    [Fact]
    public async Task HybridDurableTargetPreservesCommittedAndRolledBackState()
    {
        await using var db = await Scope.CreateAsync(People, hybrid: true);
        var policy = await db.Save(Policy()); using var preview = await db.Preview(policy);
        Assert.Equal("Rolled back", (await db.Service.ApplyAsync(db.Client, preview, policy, targetIsCurrent: () => false, ct: Ct)).Status);
        Assert.Equal("Alice Secret", await db.Value("SELECT Name FROM People WHERE Id=1;"));
        using var fresh = await db.Preview(policy);
        Assert.Equal("Committed", (await db.Service.ApplyAsync(db.Client, fresh, policy, ct: Ct)).Status);
        await db.Reopen();
        Assert.Equal("Anonymous", await db.Value("SELECT Name FROM People WHERE Id=1;"));
        Assert.Single(await db.Store.ListReceiptsAsync(db.Client, Ct));
    }

    [Fact]
    public async Task SharedRelatedRowsAreBlocked()
    {
        await using var db = await Scope.CreateAsync("""
            CREATE TABLE People (Id INTEGER PRIMARY KEY, AddressId INTEGER, Name TEXT, LastSeen TEXT);
            CREATE TABLE Addresses (Id INTEGER PRIMARY KEY, Address TEXT);
            INSERT INTO People VALUES (1,1,'A','2020-01-01'),(2,1,'B','2026-09-19'); INSERT INTO Addresses VALUES (1,'Shared secret');
            """);
        var policy = Policy(); policy.Relationships.Add(Relation("address", "People", ["AddressId"], "Addresses", ["Id"]));
        policy.Targets.Add(new() { Table = "Addresses", RelationshipPath = ["address"], Columns = [new() { Column = "Address", Kind = PrivacyMaskKind.Erase }] });
        policy = await db.Save(policy);
        var error = await Assert.ThrowsAsync<PrivacyException>(() => db.Preview(policy)); Assert.Contains("shared", error.Message);
        Assert.Equal("Shared secret", await db.Value("SELECT Address FROM Addresses;"));
    }

    [Theory]
    [InlineData("Id")]
    [InlineData("Version")]
    public async Task ProtectedColumnsCannotBeMasked(string column)
    {
        await using var db = await Scope.CreateAsync(People); var policy = Policy(); policy.Targets[0].Columns[0].Column = column;
        policy = await db.Save(policy); Assert.Contains("preserved", (await Assert.ThrowsAsync<PrivacyException>(() => db.Preview(policy))).Message);
    }

    [Fact]
    public async Task IncomingRelationshipKeyCannotBeMaskedEvenWhenChildNotSelected()
    {
        await using var db = await Scope.CreateAsync(People + " CREATE UNIQUE INDEX email_key ON People(Email); CREATE TABLE Messages (Id INTEGER PRIMARY KEY, Email TEXT REFERENCES People(Email));");
        var policy = Policy(); policy.Targets[0].Columns[0].Column = "Email"; policy = await db.Save(policy);
        Assert.Contains("relationship", (await Assert.ThrowsAsync<PrivacyException>(() => db.Preview(policy))).Message);
    }

    [Fact]
    public async Task StandaloneUniqueAndTypeConstraintsArePreflighted()
    {
        await using var db = await Scope.CreateAsync("CREATE TABLE People (Id INTEGER PRIMARY KEY, Name VARCHAR(8) COLLATE NOCASE, LastSeen TEXT, UNIQUE(Name)); INSERT INTO People VALUES (1,'Alice','2020-01-01'),(2,'ANON','2026-09-19');");
        var policy = Policy(); policy.Targets[0].Columns[0].Replacement = "anon"; policy = await db.Save(policy);
        Assert.Contains("unique", (await Assert.ThrowsAsync<PrivacyException>(() => db.Preview(policy))).Message);
        policy.Targets[0].Columns[0].Replacement = "too-long-replacement"; policy = await db.Save(policy);
        Assert.Contains("type", (await Assert.ThrowsAsync<PrivacyException>(() => db.Preview(policy))).Message);
    }

    [Theory]
    [InlineData("UPDATE People SET Name='Changed' WHERE Id=1;")]
    [InlineData("UPDATE People SET LastSeen='2026-09-19' WHERE Id=1;")]
    [InlineData("ALTER TABLE People ADD COLUMN Note TEXT;")]
    [InlineData("INSERT INTO People (Id,Name,LastSeen) VALUES (3,'New','2020-01-01');")]
    public async Task ChangesAfterPreviewForceRollback(string mutation)
    {
        await using var db = await Scope.CreateAsync(People); var policy = await db.Save(Policy()); using var preview = await db.Preview(policy);
        await db.Sql(mutation); var receipt = await db.Service.ApplyAsync(db.Client, preview, policy, ct: Ct);
        Assert.Equal("Rolled back", receipt.Status); Assert.Contains("changed", receipt.Message);
        Assert.NotEqual("Anonymous", await db.Value("SELECT Name FROM People WHERE Id=1;"));
    }

    [Fact]
    public async Task SavedRevisionAndConnectionIdentityAreEnforced()
    {
        await using var db = await Scope.CreateAsync(People); var policy = await db.Save(Policy()); using var preview = await db.Preview(policy);
        var changed = PrivacyPolicy.FromJson(policy.ToJson()); changed.Name = "Updated"; await db.Save(changed);
        var result = await db.Service.ApplyAsync(db.Client, preview, policy, ct: Ct); Assert.Equal("Rolled back", result.Status);
        await Assert.ThrowsAsync<PrivacyException>(() => db.Save(policy));
        policy = (await db.Store.ListAsync(db.Client, Ct)).Single(); using var next = await db.Preview(policy);
        await using var other = await Scope.CreateAsync(People);
        await Assert.ThrowsAsync<PrivacyException>(() => db.Service.ApplyAsync(other.Client, next, policy, ct: Ct));
    }

    [Fact]
    public async Task FailureOnSecondUpdateRollsBackFirstAndRedactsError()
    {
        await using var db = await Scope.CreateAsync(People + " UPDATE People SET LastSeen='2020-01-01' WHERE Id=2;", intercept: true);
        var policy = await db.Save(Policy()); using var preview = await db.Preview(policy); db.Proxy!.FailUpdate = 2;
        var result = await db.Service.ApplyAsync(db.Client, preview, policy, ct: Ct);
        Assert.Equal("Rolled back", result.Status); Assert.DoesNotContain("CANARY", JsonSerializer.Serialize(result));
        Assert.Equal("Alice Secret", await db.Value("SELECT Name FROM People WHERE Id=1;"));
        Assert.Empty(await db.Store.ListReceiptsAsync(db.Client, Ct));
    }

    [Fact]
    public async Task CancelAndDatabaseSwitchRollBackPendingUpdates()
    {
        await using var db = await Scope.CreateAsync(People + " UPDATE People SET LastSeen='2020-01-01' WHERE Id=2;");
        var policy = await db.Save(Policy()); var service = new PrivacyService(new() { BatchSize = 1 });
        using var preview = await service.PreviewAsync(db.Client, policy, Reference, ct: Ct);
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var result = await service.ApplyAsync(db.Client, preview, policy, new ImmediateProgress(p => { if (p.Stage.StartsWith("Updating")) cancel.Cancel(); }), ct: cancel.Token);
        Assert.Equal("Rolled back", result.Status); Assert.Equal("Alice Secret", await db.Value("SELECT Name FROM People WHERE Id=1;"));
        using var repeat = await service.PreviewAsync(db.Client, policy, Reference, ct: Ct); bool current = true;
        result = await service.ApplyAsync(db.Client, repeat, policy, new ImmediateProgress(p => { if (p.Stage.StartsWith("Updating")) current = false; }), () => current, Ct);
        Assert.Equal("Rolled back", result.Status); Assert.Equal("Alice Secret", await db.Value("SELECT Name FROM People WHERE Id=1;"));
    }

    [Fact]
    public async Task LostCommitAcknowledgementReconcilesWithoutRepeatingWrites()
    {
        await using var db = await Scope.CreateAsync(People, intercept: true); var policy = await db.Save(Policy());
        using var preview = await db.Preview(policy); db.Proxy!.LoseCommit = true;
        var result = await db.Service.ApplyAsync(db.Client, preview, policy, ct: Ct);
        Assert.Equal("Unknown", result.Status); await Assert.ThrowsAsync<PrivacyException>(() => db.Preview(policy));
        var reconciled = await db.Service.ReconcileAsync(db.Client, result.RunId, Ct); Assert.Equal("Committed", reconciled!.Status);
        using var next = await db.Preview(policy); Assert.Equal(0, next.Tables.Sum(t => t.ChangedRows));
    }

    [Theory]
    [InlineData(1, 100000, 33554432)]
    [InlineData(10000, 1, 33554432)]
    [InlineData(10000, 100000, 1024)]
    public async Task ResourceLimitsBlockWithoutWrites(int affected, int evaluated, long bytes)
    {
        await using var db = await Scope.CreateAsync(People + " UPDATE People SET LastSeen='2020-01-01' WHERE Id=2;");
        var policy = await db.Save(Policy()); var limited = new PrivacyService(new() { MaxAffectedRows = affected, MaxEvaluatedRows = evaluated, MaxPreparedBytes = bytes });
        await Assert.ThrowsAsync<PrivacyException>(() => limited.PreviewAsync(db.Client, policy, Reference, ct: Ct));
        Assert.Equal("Alice Secret", await db.Value("SELECT Name FROM People WHERE Id=1;"));
    }

    [Fact]
    public async Task OperationTimeoutReleasesTransactionWithoutWrites()
    {
        await using var db = await Scope.CreateAsync(People, intercept: true); var policy = await db.Save(Policy());
        db.Proxy!.DelayRead = true;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => new PrivacyService(new() { TimeoutSeconds = 1 }).PreviewAsync(db.Client, policy, Reference, ct: Ct));
        db.Proxy.DelayRead = false;
        using var fresh = await db.Preview(policy);
        Assert.Equal(1, fresh.EligibleRecords);
        Assert.Equal("Alice Secret", await db.Value("SELECT Name FROM People WHERE Id=1;"));
    }

    [Fact]
    public async Task UpdateTriggersBlockAndPreviewCreatesNoRunHistory()
    {
        await using var db = await Scope.CreateAsync(People + " CREATE TRIGGER audit_name AFTER UPDATE ON People BEGIN UPDATE People SET Email='retained' WHERE Id=NEW.Id; END;");
        var policy = await db.Save(Policy()); Assert.Contains("triggers", (await Assert.ThrowsAsync<PrivacyException>(() => db.Preview(policy))).Message);
        Assert.Empty(await db.Store.ListReceiptsAsync(db.Client, Ct));
    }

    [Fact]
    public async Task PolicyAndReceiptSurviveReopenAndOriginalValuesAreNotStored()
    {
        await using var db = await Scope.CreateAsync(People); var policy = await db.Save(Policy()); using var preview = await db.Preview(policy);
        var result = await db.Service.ApplyAsync(db.Client, preview, policy, ct: Ct); Assert.Equal("Committed", result.Status);
        await db.Reopen(); Assert.Equal(policy.Hash, (await db.Store.ListAsync(db.Client, Ct)).Single().Hash);
        Assert.Equal(result.RunId, (await db.Store.ListReceiptsAsync(db.Client, Ct)).Single().RunId);
        Assert.DoesNotContain("Secret", (string)(await db.Value("SELECT receipt_json FROM __privacy_runs;"))!);
        Assert.DoesNotContain("Secret", (string)(await db.Value("SELECT policy_json FROM __privacy_policies;"))!);
        Assert.DoesNotContain("__privacy_policies", await db.Client.GetTableNamesAsync(Ct));
    }

    private static PrivacyRelationship Relation(string id, string source, List<string> columns, string target, List<string> targetColumns)
        => new() { Id = id, SourceTable = source, SourceColumns = columns, TargetTable = target, TargetColumns = targetColumns };

    [Fact]
    public async Task InterruptedCommitIsRecoverableAfterReopening()
    {
        await using var db = await Scope.CreateAsync(People, intercept: true); var policy = await db.Save(Policy());
        using var preview = await db.Preview(policy); db.Proxy!.FailBeforeCommit = true;
        var result = await db.Service.ApplyAsync(db.Client, preview, policy, ct: Ct); Assert.Equal("Unknown", result.Status);
        await db.Reopen();
        Assert.Equal(result.RunId, await db.Store.FindPendingRunAsync(db.Client, Ct));
        Assert.Equal("Rolled back", (await new PrivacyService().ReconcileAsync(db.Client, result.RunId, Ct))!.Status);
        Assert.Equal("Alice Secret", await db.Value("SELECT Name FROM People WHERE Id=1;"));
        using var fresh = await db.Preview(policy); Assert.Equal(1, fresh.Tables.Sum(t => t.ChangedRows));
    }

    [Fact]
    public async Task NullValuesRemainNullAndShortPartialMasksAreRejected()
    {
        await using var db = await Scope.CreateAsync(People); await db.Sql("UPDATE People SET Name=NULL WHERE Id=1;");
        var policy = await db.Save(Policy()); using var preview = await db.Preview(policy); Assert.Equal(0, preview.Tables.Sum(t => t.ChangedRows));
        await db.Sql("UPDATE People SET Name='ab' WHERE Id=1;");
        policy.Targets[0].Columns[0] = new() { Column = "Name", Kind = PrivacyMaskKind.Partial, KeepPrefix = 1, KeepSuffix = 1 };
        policy = await db.Save(policy); Assert.Contains("visible", (await Assert.ThrowsAsync<PrivacyException>(() => db.Preview(policy))).Message);
    }

    [Fact]
    public async Task NumericReplacementAndCheckConstraintsAreValidated()
    {
        await using var db = await Scope.CreateAsync("CREATE TABLE People (Id INTEGER PRIMARY KEY, Name TEXT, LastSeen TEXT, Rating DECIMAL(6,2) CHECK(Rating >= 0)); INSERT INTO People VALUES (1,'A','2020-01-01',10.50);");
        var policy = Policy(); policy.Targets[0].Columns = [new() { Column = "Rating", Kind = PrivacyMaskKind.Constant, Replacement = "-1" }]; policy = await db.Save(policy);
        Assert.Contains("CHECK", (await Assert.ThrowsAsync<PrivacyException>(() => db.Preview(policy))).Message);
        policy.Targets[0].Columns[0].Replacement = "0.25"; policy = await db.Save(policy); using var preview = await db.Preview(policy);
        Assert.Equal("Committed", (await db.Service.ApplyAsync(db.Client, preview, policy, ct: Ct)).Status);
        Assert.Equal(.25m, await db.Value("SELECT Rating FROM People;"));
    }

    [Fact]
    public async Task MultiHopLogicalRelationshipsReachOnlyEligibleRecords()
    {
        await using var db = await Scope.CreateAsync(People + " CREATE TABLE Orders (Id INTEGER PRIMARY KEY, Person INTEGER); CREATE TABLE Shipping (Id INTEGER PRIMARY KEY, OrderId INTEGER, Contact TEXT); INSERT INTO Orders VALUES (1,1),(2,2); INSERT INTO Shipping VALUES (1,1,'Alice contact'),(2,2,'Bob contact');");
        var policy = Policy(); policy.Relationships = [Relation("orders", "People", ["Id"], "Orders", ["Person"]), Relation("shipping", "Orders", ["Id"], "Shipping", ["OrderId"])];
        policy.Targets.Add(new() { Table = "Shipping", RelationshipPath = ["orders", "shipping"], Columns = [new() { Column = "Contact", Kind = PrivacyMaskKind.Erase }] });
        policy = await db.Save(policy); using var preview = await db.Preview(policy);
        Assert.Equal("Committed", (await db.Service.ApplyAsync(db.Client, preview, policy, ct: Ct)).Status);
        Assert.Null(await db.Value("SELECT Contact FROM Shipping WHERE Id=1;")); Assert.Equal("Bob contact", await db.Value("SELECT Contact FROM Shipping WHERE Id=2;"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task HostCallbacksAreBlockedOnlyWhenReachableFromUpdates(bool used)
    {
        string directory = Path.Combine(Path.GetTempPath(), "csharpdb-privacy-tests", Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
        int calls = 0;
        await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
        {
            DataSource = Path.Combine(directory, "host.db"),
            DirectDatabaseOptions = new CSharpDB.Engine.DatabaseOptions
            {
                Functions = CSharpDB.Primitives.DbFunctionRegistry.Create(f => f.AddScalar("Observe", 1,
                    new CSharpDB.Primitives.DbScalarFunctionOptions(CSharpDB.Primitives.DbType.Integer, IsDeterministic: true), (_, _) => { calls++; return CSharpDB.Primitives.DbValue.FromInteger(1); }))
            }
        });
        Assert.Null((await client.ExecuteSqlAsync("CREATE TABLE People (Id INTEGER PRIMARY KEY, Name TEXT, LastSeen TEXT);", Ct)).Error);
        Assert.Null((await client.ExecuteSqlAsync("INSERT INTO People VALUES (1,'Secret','2020-01-01');", Ct)).Error);
        if (used)
        {
            Assert.Null((await client.ExecuteSqlAsync("CREATE TABLE Audit (Value INTEGER);", Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync("CREATE TRIGGER capture AFTER UPDATE ON People BEGIN INSERT INTO Audit VALUES (Observe(NEW.Name)); END;", Ct)).Error);
        }
        var policy = await new PrivacyPolicyStore().SaveAsync(client, Policy(), Ct); calls = 0;
        var service = new PrivacyService();
        if (used) Assert.Contains("callbacks", (await Assert.ThrowsAsync<PrivacyException>(() => service.PreviewAsync(client, policy, Reference, ct: Ct))).Message);
        else { using var preview = await service.PreviewAsync(client, policy, Reference, ct: Ct); Assert.Equal("Committed", (await service.ApplyAsync(client, preview, policy, ct: Ct)).Status); }
        Assert.Equal(0, calls);
        await client.DisposeAsync(); Directory.Delete(directory, true);
    }

    [Fact]
    public async Task PreviewSerializationOmitsPersonalSamples()
    {
        await using var db = await Scope.CreateAsync(People); var policy = await db.Save(Policy()); using var preview = await db.Preview(policy);
        Assert.DoesNotContain("Secret", JsonSerializer.Serialize(preview));
        Assert.DoesNotContain("private.test", JsonSerializer.Serialize(preview));
    }
    private static PrivacyPolicy Policy(string table = "People") => new()
    {
        Name = "Inactive people", RootTable = table,
        Eligibility = new() { Children = [new() { Kind = PrivacyConditionKind.OlderThan, Column = "LastSeen", Days = 365 }] },
        Targets = [new() { Table = table, Columns = [new() { Column = "Name", Kind = PrivacyMaskKind.Constant }] }]
    };
    private sealed class ImmediateProgress(Action<PrivacyProgress> action) : IProgress<PrivacyProgress> { public void Report(PrivacyProgress value) => action(value); }

    public interface IInterceptClient : ICSharpDbClient, ICSharpDbTransactionalSnapshotReader, ICSharpDbExclusiveSessionProvider { }
    public class FaultProxy : DispatchProxy
    {
        public ICSharpDbClient Inner = null!;
        public int FailUpdate, UpdateCount;
        public bool LoseCommit, FailBeforeCommit, DelayRead;
        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            if (method!.Name == nameof(ICSharpDbExclusiveSessionProvider.OpenExclusiveSessionAsync))
                return WrapExclusiveAsync((CancellationToken)args![0]!);
            if (method!.Name == nameof(ICSharpDbClient.ExecuteInTransactionAsync) && DelayRead && ((string)args![1]!).StartsWith("SELECT", StringComparison.Ordinal))
                return DelayedRead((CancellationToken)args[2]!);
            if (method!.Name == nameof(ICSharpDbClient.ExecuteInTransactionAsync) && ((string)args![1]!).StartsWith("UPDATE \"People\"", StringComparison.Ordinal))
                if (++UpdateCount == FailUpdate) return Task.FromResult(new CSharpDB.Client.Models.SqlExecutionResult { Error = "CANARY original sensitive value" });
            if (method.Name == nameof(ICSharpDbClient.CommitTransactionAsync) && LoseCommit && UpdateCount > 0)
            {
                LoseCommit = false;
                return CommitThenThrow((string)args![0]!, (CancellationToken)args[1]!);
            }
            if (method.Name == nameof(ICSharpDbClient.CommitTransactionAsync) && FailBeforeCommit && UpdateCount > 0)
            { FailBeforeCommit = false; return Task.FromException(new IOException("Commit interrupted")); }
            try { return method.Invoke(Inner, args); }
            catch (TargetInvocationException e) { System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(e.InnerException!).Throw(); throw; }
        }
        private async Task CommitThenThrow(string tx, CancellationToken ct) { await Inner.CommitTransactionAsync(tx, ct); throw new IOException("CANARY lost acknowledgement"); }
        private async ValueTask<CSharpDbExclusiveSession> WrapExclusiveAsync(CancellationToken ct)
        {
            var lease = await ((ICSharpDbExclusiveSessionProvider)Inner).OpenExclusiveSessionAsync(ct);
            var wrapped = DispatchProxy.Create<IInterceptClient, FaultProxy>();
            var child = (FaultProxy)(object)wrapped;
            child.Inner = lease.Client;
            child.FailUpdate = FailUpdate; child.LoseCommit = LoseCommit; child.FailBeforeCommit = FailBeforeCommit; child.DelayRead = DelayRead;
            return new(wrapped, lease.DisposeAsync);
        }
        private static async Task<CSharpDB.Client.Models.SqlExecutionResult> DelayedRead(CancellationToken ct)
        { await Task.Delay(Timeout.Infinite, ct); throw new InvalidOperationException(); }
    }
    private sealed class Scope : IAsyncDisposable
    {
        private readonly string _directory;
        private string PathName => Path.Combine(_directory, "test.db");
        public ICSharpDbClient Client { get; private set; }
        public FaultProxy? Proxy { get; private set; }
        public PrivacyService Service { get; } = new();
        public PrivacyPolicyStore Store { get; } = new();
        private Scope(string directory, bool hybrid) { _directory = directory; Client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = PathName, HybridDatabaseOptions = hybrid ? new CSharpDB.Engine.HybridDatabaseOptions() : null }); }
        public static async Task<Scope> CreateAsync(string sql, bool intercept = false, bool hybrid = false)
        {
            string directory = Path.Combine(Path.GetTempPath(), "csharpdb-privacy-tests", Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
            var scope = new Scope(directory, hybrid);
            if (intercept) { var proxy = DispatchProxy.Create<IInterceptClient, FaultProxy>(); scope.Proxy = (FaultProxy)(object)proxy; scope.Proxy.Inner = scope.Client; scope.Client = proxy; }
            try { await scope.Sql(sql); return scope; } catch { await scope.DisposeAsync(); throw; }
        }
        public Task<PrivacyPolicy> Save(PrivacyPolicy policy) => Store.SaveAsync(Client, policy, Ct);
        public Task<PrivacyPreview> Preview(PrivacyPolicy policy) => Service.PreviewAsync(Client, policy, Reference, ct: Ct);
        public async Task Sql(string sql)
        { foreach (string statement in SqlScriptSplitter.SplitExecutableStatements(sql)) { var r = await Client.ExecuteSqlAsync(statement, Ct); Assert.True(string.IsNullOrEmpty(r.Error), r.Error); } }
        public async Task<object?> Value(string sql)
        { var r = await Client.ExecuteSqlAsync(sql, Ct); Assert.True(string.IsNullOrEmpty(r.Error), r.Error); return r.Rows![0][0]; }
        public async Task Reopen() { await Client.DisposeAsync(); Client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = PathName }); }
        public async ValueTask DisposeAsync()
        {
            await Client.DisposeAsync();
            for (int attempt = 0; ; attempt++)
                try { Directory.Delete(_directory, true); break; }
                catch (IOException) when (attempt < 5) { await Task.Delay(100); }
        }
    }
}
