using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class TrustedScalarFunctionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void Registry_ValidatesNamesCollisionsAndMetadata()
    {
        var registry = DbFunctionRegistry.Create(functions =>
            functions.AddScalar(
                "Bump",
                1,
                new DbScalarFunctionOptions(DbType.Integer, IsDeterministic: true, NullPropagating: true),
                static (_, args) => DbValue.FromInteger(args[0].AsInteger + 1)));

        Assert.True(registry.TryGetScalar("bump", 1, out var definition));
        Assert.Equal(DbType.Integer, definition.Options.ReturnType);
        Assert.True(definition.Options.IsDeterministic);
        Assert.True(definition.Options.NullPropagating);

        Assert.Throws<ArgumentException>(() => DbFunctionRegistry.Create(functions =>
        {
            functions.AddScalar("Dup", 1, static (_, _) => DbValue.Null);
            functions.AddScalar("dup", 2, static (_, _) => DbValue.Null);
        }));

        Assert.Throws<ArgumentException>(() => DbFunctionRegistry.Create(functions =>
            functions.AddScalar("TEXT", 1, static (_, _) => DbValue.Null)));
    }

    [Fact]
    public void ExpressionCompiler_InvokesRegisteredFunctionAndPropagatesNull()
    {
        var registry = DbFunctionRegistry.Create(functions =>
            functions.AddScalar(
                "DoubleIt",
                1,
                new DbScalarFunctionOptions(DbType.Integer, IsDeterministic: true, NullPropagating: true),
                static (_, args) => DbValue.FromInteger(args[0].AsInteger * 2)));

        var schema = new TableSchema
        {
            TableName = "numbers",
            Columns =
            [
                new ColumnDefinition { Name = "value", Type = DbType.Integer, Nullable = true },
            ],
        };

        var expression = new FunctionCallExpression
        {
            FunctionName = "DoubleIt",
            Arguments = [new ColumnRefExpression { ColumnName = "value" }],
        };

        var evaluator = ExpressionCompiler.CompileSpan(expression, schema, registry);

        Assert.Equal(DbValue.FromInteger(14), evaluator([DbValue.FromInteger(7)]));
        Assert.Equal(DbValue.Null, evaluator([DbValue.Null]));
    }

    [Fact]
    public async Task Sql_UsesRegisteredFunctionsAcrossReadWriteAndTriggerPaths()
    {
        var options = CreateOptions();
        await using var db = await Database.OpenInMemoryAsync(options, Ct);

        await db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, slug TEXT)", Ct);
        await db.ExecuteAsync("CREATE TABLE audit (slug TEXT)", Ct);
        await db.ExecuteAsync("""
            CREATE TRIGGER items_ai AFTER INSERT ON items
            BEGIN
                INSERT INTO audit VALUES (Slugify(NEW.name));
            END
            """, Ct);

        await db.ExecuteAsync("INSERT INTO items VALUES (1, 'Hello World', Slugify('Hello World'))", Ct);
        await db.ExecuteAsync("INSERT INTO items VALUES (2, 'Odd Name', Slugify('Odd Name'))", Ct);
        await db.ExecuteAsync("UPDATE items SET slug = Slugify(name) WHERE IsEven(id) = 1", Ct);

        await using (var result = await db.ExecuteAsync(
            "SELECT Slugify(name) FROM items WHERE IsEven(id) = 1 ORDER BY Slugify(name) DESC",
            Ct))
        {
            var rows = await result.ToListAsync(Ct);
            Assert.Single(rows);
            Assert.Equal("odd-name", rows[0][0].AsText);
        }

        await using (var triggerResult = await db.ExecuteAsync("SELECT slug FROM audit ORDER BY slug", Ct))
        {
            var rows = await triggerResult.ToListAsync(Ct);
            Assert.Equal(["hello-world", "odd-name"], rows.Select(row => row[0].AsText).ToArray());
        }
    }

    [Fact]
    public async Task Sql_MissingAndThrowingFunctionsFailAndStatementRollsBack()
    {
        var options = CreateOptions();
        await using var db = await Database.OpenInMemoryAsync(options, Ct);

        await db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)", Ct);

        var missing = await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await db.ExecuteAsync("INSERT INTO items VALUES (MissingFunc(1), 'bad')", Ct));
        Assert.Contains("Unknown scalar function", missing.Message);

        var thrown = await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await db.ExecuteAsync("INSERT INTO items VALUES (Boom(1), 'bad')", Ct));
        Assert.Contains("Scalar function 'Boom' failed", thrown.Message);

        await using var result = await db.ExecuteAsync("SELECT COUNT(*) FROM items", Ct);
        var rows = await result.ToListAsync(Ct);
        Assert.Equal(0, rows[0][0].AsInteger);
    }

    private static DatabaseOptions CreateOptions()
        => new DatabaseOptions().ConfigureFunctions(functions =>
        {
            functions.AddScalar(
                "Slugify",
                1,
                new DbScalarFunctionOptions(DbType.Text, IsDeterministic: true, NullPropagating: true),
                static (_, args) => DbValue.FromText(args[0].AsText.ToLowerInvariant().Replace(' ', '-')));
            functions.AddScalar(
                "IsEven",
                1,
                new DbScalarFunctionOptions(DbType.Integer, IsDeterministic: true, NullPropagating: true),
                static (_, args) => DbValue.FromInteger(args[0].AsInteger % 2 == 0 ? 1 : 0));
            functions.AddScalar(
                "Boom",
                1,
                static (_, _) => throw new InvalidOperationException("boom"));
        });
}
