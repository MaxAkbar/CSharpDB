using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class UnsupportedSqlFeatureDiagnosticTests
{
    [Theory]
    [InlineData(
        "SELECT CASE WHEN 1 = 1 THEN 1 ELSE 0 END",
        "CASE expressions are not supported")]
    [InlineData(
        "SELECT TRY_CAST(1 AS TEXT)",
        "TRY_CAST expressions are not supported")]
    [InlineData(
        "INSERT INTO items VALUES (1) RETURNING id",
        "RETURNING clauses are not supported")]
    [InlineData(
        "UPDATE items SET id = 2 RETURNING id",
        "RETURNING clauses are not supported")]
    [InlineData(
        "DELETE FROM items RETURNING id",
        "RETURNING clauses are not supported")]
    [InlineData(
        "UPSERT INTO items VALUES (1)",
        "UPSERT and REPLACE statements are not supported")]
    [InlineData(
        "REPLACE INTO items VALUES (1)",
        "UPSERT and REPLACE statements are not supported")]
    [InlineData(
        "INSERT INTO items VALUES (1) ON CONFLICT DO NOTHING",
        "INSERT ... ON CONFLICT is not supported")]
    [InlineData(
        "INSERT OR REPLACE INTO items VALUES (1)",
        "INSERT OR REPLACE is not supported")]
    [InlineData(
        "SELECT id FROM left_items INTERSECT ALL SELECT id FROM right_items",
        "INTERSECT ALL is not supported; use INTERSECT.")]
    [InlineData(
        "SELECT id FROM left_items EXCEPT ALL SELECT id FROM right_items",
        "EXCEPT ALL is not supported; use EXCEPT.")]
    [InlineData(
        "WITH RECURSIVE values_cte AS (SELECT 1) SELECT * FROM values_cte",
        "Recursive CTE execution is not supported")]
    [InlineData(
        "SELECT LAG(id) IGNORE NULLS OVER (ORDER BY id) FROM items",
        "IGNORE NULLS and RESPECT NULLS window syntax is not supported")]
    [InlineData(
        "SELECT LEAD(id) RESPECT NULLS OVER (ORDER BY id) FROM items",
        "IGNORE NULLS and RESPECT NULLS window syntax is not supported")]
    [InlineData(
        "CREATE PROCEDURE read_items AS SELECT * FROM items",
        "CREATE PROCEDURE SQL statements are not supported")]
    [InlineData(
        "CALL read_items()",
        "Stored-procedure SQL statements are not supported")]
    [InlineData(
        "SELECT * FROM left_items FULL OUTER JOIN right_items ON left_items.id = right_items.id",
        "FULL OUTER JOIN is not supported")]
    [InlineData(
        "SELECT * FROM left_items NATURAL JOIN right_items",
        "NATURAL JOIN is not supported")]
    [InlineData(
        "BEGIN",
        "SQL transaction and savepoint statements are not supported")]
    [InlineData(
        "COMMIT",
        "SQL transaction and savepoint statements are not supported")]
    [InlineData(
        "ROLLBACK",
        "SQL transaction and savepoint statements are not supported")]
    [InlineData(
        "SAVEPOINT before_update",
        "SQL transaction and savepoint statements are not supported")]
    [InlineData(
        "RELEASE SAVEPOINT before_update",
        "SQL transaction and savepoint statements are not supported")]
    [InlineData(
        "SELECT 1; SELECT 2",
        "Multiple statements in one call are not supported")]
    [InlineData(
        "CREATE TABLE child (parent_id INTEGER REFERENCES parent(id) MATCH FULL)",
        "MATCH FULL foreign-key constraints are not supported")]
    [InlineData(
        "CREATE TABLE child (parent_id INTEGER REFERENCES parent(id) MATCH PARTIAL)",
        "MATCH PARTIAL foreign-key constraints are not supported")]
    [InlineData(
        "CREATE TABLE child (parent_id INTEGER REFERENCES parent(id) DEFERRABLE)",
        "DEFERRABLE foreign-key clauses are not supported")]
    [InlineData(
        "CREATE TABLE child (parent_id INTEGER REFERENCES parent(id) INITIALLY DEFERRED)",
        "DEFERRABLE foreign-key clauses are not supported")]
    [InlineData(
        "CREATE TABLE child (parent_id INTEGER REFERENCES parent(id) NOT DEFERRABLE)",
        "DEFERRABLE foreign-key clauses are not supported")]
    public void DocumentedUnsupportedSyntax_HasStableDiagnostic(
        string sql,
        string expectedMessage)
    {
        CSharpDbException error = Assert.Throws<CSharpDbException>(
            () => Parser.Parse(sql));

        Assert.Equal(ErrorCode.SyntaxError, error.Code);
        Assert.Contains(
            expectedMessage,
            error.Message,
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(
        "CREATE TABLE child (parent_id INTEGER REFERENCES parent(id) MATCH SIMPLE)")]
    [InlineData(
        """
        CREATE TABLE child (
            parent_id INTEGER,
            FOREIGN KEY (parent_id)
                REFERENCES parent(id)
                MATCH SIMPLE
                ON DELETE CASCADE
                ON UPDATE SET NULL
        )
        """)]
    public void ExplicitMatchSimple_IsAccepted(string sql)
    {
        Exception? failure = Record.Exception(() => Parser.Parse(sql));

        Assert.Null(failure);
    }

    [Theory]
    [InlineData("STRFTIME('now')", "STRFTIME")]
    [InlineData("CEIL(1.5)", "CEIL")]
    [InlineData("POWER(2, 3)", "POWER")]
    public async Task DocumentedUnregisteredFunctions_HaveStableDiagnostic(
        string expression,
        string functionName)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);

        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            async () =>
            {
                await using QueryResult result = await database.ExecuteAsync(
                    $"SELECT {expression}",
                    ct);
                await result.ToListAsync(ct);
            });

        Assert.Equal(ErrorCode.Unknown, error.Code);
        Assert.Equal($"Unknown scalar function: {functionName}", error.Message);
    }

    [Fact]
    public async Task TriggerWhen_IsRejectedBeforeCatalogPersistence()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        await database.ExecuteAsync(
            "CREATE TABLE trigger_items (id INTEGER PRIMARY KEY, value INTEGER)",
            ct);

        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            async () =>
            {
                await database.ExecuteAsync(
                    """
                    CREATE TRIGGER conditional_update
                    AFTER UPDATE ON trigger_items
                    WHEN (NEW.value > 0)
                    BEGIN
                        UPDATE trigger_items SET value = NEW.value WHERE id = NEW.id;
                    END
                    """,
                    ct);
            });

        Assert.Equal(ErrorCode.SyntaxError, error.Code);
        Assert.Equal(
            "Trigger WHEN conditions are not supported.",
            error.Message);

        await using QueryResult result = await database.ExecuteAsync(
            """
            SELECT COUNT(*)
            FROM sys.triggers
            WHERE trigger_name = 'conditional_update'
            """,
            ct);
        IReadOnlyList<DbValue[]> rows = await result.ToListAsync(ct);

        Assert.Equal(0L, rows[0][0].AsInteger);
    }
}
