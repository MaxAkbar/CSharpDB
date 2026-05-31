using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;

namespace CSharpDB.Admin.Forms.Tests.Admin;

public sealed class DataHygieneAdminServiceTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void BuildCreateValidationRuleSql_EscapesStringLiteral()
    {
        string sql = DataHygieneAdminService.BuildCreateValidationRuleSql(
            "valid_email",
            "customers",
            "email",
            "email LIKE '%@%'",
            "Email isn't valid");

        Assert.Equal(
            "CREATE VALIDATION RULE valid_email ON customers.email AS email LIKE '%@%' MESSAGE 'Email isn''t valid';",
            sql);
    }

    [Fact]
    public void BuildFindDuplicatesSql_RejectsInvalidIdentifier()
    {
        Assert.Throws<InvalidOperationException>(() =>
            DataHygieneAdminService.BuildFindDuplicatesSql("bad-name", "email"));
    }

    [Fact]
    public void BuildFindDuplicatesSql_RejectsStatementSeparatorInExpression()
    {
        Assert.Throws<InvalidOperationException>(() =>
            DataHygieneAdminService.BuildFindDuplicatesSql("customers", "email; DROP TABLE customers"));
    }

    [Fact]
    public async Task FindDuplicatesAndDedupAsync_MapResultsAndMutateThroughSql()
    {
        await using TestDatabaseScope db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY IDENTITY, email TEXT);");
        await db.ExecuteAsync("INSERT INTO customers (email) VALUES ('a@example.com'), ('a@example.com'), ('b@example.com');");
        var service = new DataHygieneAdminService(db.Client);

        DataHygieneResultSet<DataHygieneDuplicateGroup> preview = await service.FindDuplicatesAsync("customers", "email", Ct);

        DataHygieneDuplicateGroup group = Assert.Single(preview.Rows);
        Assert.Equal(2, group.GroupSize);
        Assert.Equal("['a@example.com']", group.KeyValues);
        Assert.Equal("FIND DUPLICATES IN customers ON email;", preview.Sql);

        DataHygieneResultSet<DataHygieneMutationSummary> dedup = await service.DedupAsync("customers", "email", DataHygieneKeepMode.First, Ct);

        DataHygieneMutationSummary summary = Assert.Single(dedup.Rows);
        Assert.Equal(1, summary.DuplicateGroupCount);
        Assert.Equal(1, summary.RowsDeleted);
        IReadOnlyList<Dictionary<string, object?>> rows = await db.QueryRowsAsync("SELECT COUNT(*) AS remaining FROM customers;");
        Assert.Equal(2L, rows[0]["remaining"]);
    }

    [Fact]
    public async Task ValidationRuleAndValidateAsync_MapRulesAndViolations()
    {
        await using TestDatabaseScope db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY IDENTITY, email TEXT);");
        await db.ExecuteAsync("INSERT INTO customers (email) VALUES ('a@example.com'), ('missing-at');");
        var service = new DataHygieneAdminService(db.Client);

        await service.CreateValidationRuleAsync(
            "valid_email",
            "customers",
            "email",
            "email LIKE '%@%'",
            "Email must contain @",
            Ct);

        DataHygieneResultSet<DataHygieneValidationRuleRow> rules = await service.ListValidationRulesAsync(Ct);
        DataHygieneValidationRuleRow rule = Assert.Single(rules.Rows);
        Assert.Equal("valid_email", rule.RuleName);
        Assert.Equal("customers", rule.TableName);
        Assert.Equal("email", rule.ColumnName);
        Assert.True(rule.IsEnabled);

        DataHygieneResultSet<DataHygieneValidationViolation> validation = await service.ValidateTableAsync("customers", Ct);
        DataHygieneValidationViolation violation = Assert.Single(validation.Rows);
        Assert.Equal("valid_email", violation.RuleName);
        Assert.Equal("Email must contain @", violation.Message);
    }

    [Fact]
    public async Task FindOrphansExplicitAsync_MapsMissingParentRows()
    {
        await using TestDatabaseScope db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync("CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT);");
        await db.ExecuteAsync("CREATE TABLE bookings (id INTEGER PRIMARY KEY, book_id INTEGER);");
        await db.ExecuteAsync("INSERT INTO books (id, title) VALUES (1, 'Existing');");
        await db.ExecuteAsync("INSERT INTO bookings (id, book_id) VALUES (1, 1), (2, 99), (3, NULL);");
        var service = new DataHygieneAdminService(db.Client);

        DataHygieneResultSet<DataHygieneOrphanRow> result = await service.FindOrphansExplicitAsync(
            "bookings",
            "book_id",
            "books",
            "id",
            Ct);

        DataHygieneOrphanRow orphan = Assert.Single(result.Rows);
        Assert.Equal("bookings", orphan.ChildTable);
        Assert.Equal("book_id", orphan.ChildColumn);
        Assert.Equal("99", orphan.ChildValue);
        Assert.Equal("books", orphan.ParentTable);
        Assert.Equal("id", orphan.ParentColumn);
    }
}
