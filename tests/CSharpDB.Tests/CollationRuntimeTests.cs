using CSharpDB.Engine;
using CSharpDB.Primitives;
using System.Globalization;

namespace CSharpDB.Tests;

public sealed class CollationRuntimeTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ColumnNoCaseCollation_MatchesCaseInsensitiveEquality_WithoutIndex()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice')", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (2, 'alice')", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (3, 'Bob')", Ct);

        await using var result = await db.ExecuteAsync("SELECT name FROM users WHERE name = 'ALICE' ORDER BY id", Ct);
        var rows = await result.ToListAsync(Ct);

        Assert.Equal(2, rows.Count);
        Assert.Equal("Alice", rows[0][0].AsText);
        Assert.Equal("alice", rows[1][0].AsText);
    }

    [Fact]
    public async Task ColumnNoCaseAiCollation_MatchesCaseAndAccentInsensitiveEquality_WithoutIndex()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE_AI)", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (1, 'José')", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (2, 'JOSE')", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (3, 'Joëlle')", Ct);

        await using var result = await db.ExecuteAsync("SELECT id FROM users WHERE name = 'jose' ORDER BY id", Ct);
        var rows = await result.ToListAsync(Ct);

        Assert.Equal([1L, 2L], rows.Select(static row => row[0].AsInteger).ToArray());
    }

    [Fact]
    public async Task ColumnIcuCollation_UsesLocaleAwareEquality_WithoutIndex()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        const string locale = "en-US";
        string collation = $"ICU:{locale}";
        string search = "resume";
        string[] names = ["resume", "Resume", "résumé", "resumé"];

        await db.ExecuteAsync($"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT COLLATE {collation})", Ct);
        for (int i = 0; i < names.Length; i++)
            await db.ExecuteAsync($"INSERT INTO users VALUES ({i + 1}, '{names[i]}')", Ct);

        await using var result = await db.ExecuteAsync($"SELECT name FROM users WHERE name = '{search}' ORDER BY id", Ct);
        var rows = await result.ToListAsync(Ct);

        var compareInfo = CultureInfo.GetCultureInfo(locale).CompareInfo;
        string[] expected = names.Where(name => compareInfo.Compare(name, search, CompareOptions.None) == 0).ToArray();
        Assert.Equal(expected, rows.Select(static row => row[0].AsText).ToArray());
    }

    [Fact]
    public async Task OrderBy_UsesColumnNoCaseCollation()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)", Ct);
        await db.ExecuteAsync("INSERT INTO items VALUES (1, 'a')", Ct);
        await db.ExecuteAsync("INSERT INTO items VALUES (2, 'B')", Ct);
        await db.ExecuteAsync("INSERT INTO items VALUES (3, 'c')", Ct);

        await using var result = await db.ExecuteAsync("SELECT name FROM items ORDER BY name", Ct);
        var rows = await result.ToListAsync(Ct);

        Assert.Equal(["a", "B", "c"], rows.Select(static row => row[0].AsText).ToArray());
    }

    [Fact]
    public async Task OrderBy_UsesColumnNoCaseAiCollation()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE_AI)", Ct);
        await db.ExecuteAsync("INSERT INTO items VALUES (1, 'Éclair')", Ct);
        await db.ExecuteAsync("INSERT INTO items VALUES (2, 'beta')", Ct);
        await db.ExecuteAsync("INSERT INTO items VALUES (3, 'alpha')", Ct);

        await using var result = await db.ExecuteAsync("SELECT name FROM items ORDER BY name", Ct);
        var rows = await result.ToListAsync(Ct);

        Assert.Equal(["alpha", "beta", "Éclair"], rows.Select(static row => row[0].AsText).ToArray());
    }

    [Fact]
    public async Task OrderBy_UsesColumnIcuCollation()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        const string locale = "sv-SE";
        string collation = $"ICU:{locale}";
        string[] values = ["z", "ä", "å", "a", "ö"];

        await db.ExecuteAsync($"CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT COLLATE {collation})", Ct);
        for (int i = 0; i < values.Length; i++)
            await db.ExecuteAsync($"INSERT INTO items VALUES ({i + 1}, '{values[i]}')", Ct);

        await using var result = await db.ExecuteAsync("SELECT name FROM items ORDER BY name", Ct);
        var rows = await result.ToListAsync(Ct);

        string[] expected = values
            .OrderBy(static value => value, StringComparer.Create(CultureInfo.GetCultureInfo(locale), ignoreCase: false))
            .ToArray();
        Assert.Equal(expected, rows.Select(static row => row[0].AsText).ToArray());
    }

    [Fact]
    public async Task IndexLookup_UsesInheritedNoCaseCollation()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)", Ct);
        await db.ExecuteAsync("CREATE INDEX idx_users_name ON users (name)", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice')", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (2, 'alice')", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (3, 'Bob')", Ct);

        await using var result = await db.ExecuteAsync("SELECT id FROM users WHERE name = 'ALICE' ORDER BY id", Ct);
        var rows = await result.ToListAsync(Ct);

        Assert.Equal([1L, 2L], rows.Select(static row => row[0].AsInteger).ToArray());
    }

    [Fact]
    public async Task ExplicitNoCaseIndex_DoesNotChangeBinaryColumnQuerySemantics()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", Ct);
        await db.ExecuteAsync("CREATE INDEX idx_users_name ON users (name COLLATE NOCASE)", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice')", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (2, 'alice')", Ct);

        await using var result = await db.ExecuteAsync("SELECT id FROM users WHERE name = 'alice' ORDER BY id", Ct);
        var rows = await result.ToListAsync(Ct);

        Assert.Equal([2L], rows.Select(static row => row[0].AsInteger).ToArray());
    }

    [Fact]
    public async Task UniqueNoCaseIndex_RejectsCaseVariants()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", Ct);
        await db.ExecuteAsync("CREATE UNIQUE INDEX idx_users_name ON users (name COLLATE NOCASE)", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice')", Ct);

        var error = await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await db.ExecuteAsync("INSERT INTO users VALUES (2, 'alice')", Ct));

        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
    }

    [Fact]
    public async Task UniqueNoCaseAiIndex_RejectsAccentAndCaseVariants()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", Ct);
        await db.ExecuteAsync("CREATE UNIQUE INDEX idx_users_name_ai ON users (name COLLATE NOCASE_AI)", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (1, 'José')", Ct);

        var error = await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await db.ExecuteAsync("INSERT INTO users VALUES (2, 'JOSE')", Ct));

        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
    }

    [Fact]
    public async Task UniqueIcuIndex_RejectsLocaleEquivalentValues()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        const string locale = "en-US";
        string collation = $"ICU:{locale}";
        await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", Ct);
        await db.ExecuteAsync($"CREATE UNIQUE INDEX idx_users_name_icu ON users (name COLLATE {collation})", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (1, 'résumé')", Ct);

        var compareInfo = CultureInfo.GetCultureInfo(locale).CompareInfo;
        bool equivalent = compareInfo.Compare("résumé", "résumé", CompareOptions.None) == 0;

        if (equivalent)
        {
            var error = await Assert.ThrowsAsync<CSharpDbException>(async () =>
                await db.ExecuteAsync("INSERT INTO users VALUES (2, 'résumé')", Ct));

            Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
        }
        else
        {
            await db.ExecuteAsync("INSERT INTO users VALUES (2, 'résumé')", Ct);
        }
    }

    [Fact]
    public async Task ExplicitNoCaseQueryCollation_OverridesBinaryColumnEquality()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice')", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (2, 'alice')", Ct);

        await using var result = await db.ExecuteAsync(
            "SELECT id FROM users WHERE name COLLATE NOCASE = 'ALICE' ORDER BY id",
            Ct);
        var rows = await result.ToListAsync(Ct);

        Assert.Equal([1L, 2L], rows.Select(static row => row[0].AsInteger).ToArray());
    }

    [Fact]
    public async Task ExplicitRightOperandCollation_OverridesImplicitColumnCollation()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice')", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (2, 'alice')", Ct);

        await using var result = await db.ExecuteAsync(
            "SELECT id FROM users WHERE name = 'alice' COLLATE BINARY ORDER BY id",
            Ct);
        var rows = await result.ToListAsync(Ct);

        Assert.Equal([2L], rows.Select(static row => row[0].AsInteger).ToArray());
    }

    [Fact]
    public async Task ExplicitOrderByCollation_OverridesColumnDefault()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)", Ct);
        await db.ExecuteAsync("INSERT INTO items VALUES (1, 'a')", Ct);
        await db.ExecuteAsync("INSERT INTO items VALUES (2, 'B')", Ct);
        await db.ExecuteAsync("INSERT INTO items VALUES (3, 'c')", Ct);

        await using var result = await db.ExecuteAsync(
            "SELECT name FROM items ORDER BY name COLLATE NOCASE",
            Ct);
        var rows = await result.ToListAsync(Ct);

        Assert.Equal(["a", "B", "c"], rows.Select(static row => row[0].AsText).ToArray());
    }

    [Fact]
    public async Task ExplicitNoCaseQueryCollation_MatchesExplicitNoCaseIndex()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", Ct);
        await db.ExecuteAsync("CREATE INDEX idx_users_name ON users (name COLLATE NOCASE)", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice')", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (2, 'alice')", Ct);
        await db.ExecuteAsync("INSERT INTO users VALUES (3, 'Bob')", Ct);

        await using var result = await db.ExecuteAsync(
            "SELECT id FROM users WHERE name COLLATE NOCASE = 'ALICE' ORDER BY id",
            Ct);
        var rows = await result.ToListAsync(Ct);

        Assert.Equal([1L, 2L], rows.Select(static row => row[0].AsInteger).ToArray());
    }

    [Fact]
    public async Task UnsupportedCollation_Throws()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);

        var error = await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await db.ExecuteAsync("CREATE TABLE users (name TEXT COLLATE TURKISH)", Ct));

        Assert.Equal(ErrorCode.SyntaxError, error.Code);
    }

    [Fact]
    public async Task UnsupportedIcuLocale_Throws()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);

        var error = await Assert.ThrowsAsync<CSharpDbException>(async () =>
            await db.ExecuteAsync("CREATE TABLE users (name TEXT COLLATE ICU:not-a-real-locale)", Ct));

        Assert.Equal(ErrorCode.SyntaxError, error.Code);
    }
}
