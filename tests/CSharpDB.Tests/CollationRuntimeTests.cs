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

    [Theory]
    [InlineData("NOCASE_AI", "rEsUmE_000123")]
    [InlineData("ICU:en-US", "résumé_000123")]
    public async Task OrderedTextSqlIndex_RemainsWritable_AfterSeedAndIndexBuild(
        string collation,
        string probe)
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync(
            $"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT COLLATE {collation} NOT NULL, payload TEXT NOT NULL)",
            Ct);

        for (int i = 0; i < 1024; i++)
        {
            await db.ExecuteAsync(
                $"INSERT INTO users VALUES ({i}, 'résumé_{i:D6}', 'payload_{i:D6}')",
                Ct);
        }

        await db.ExecuteAsync($"CREATE INDEX idx_users_name ON users (name COLLATE {collation})", Ct);

        await db.BeginTransactionAsync(Ct);
        try
        {
            await db.ExecuteAsync(
                "INSERT INTO users VALUES (1000000, 'résumé_1000000', 'payload_1000000')",
                Ct);
            await db.RollbackAsync(Ct);
        }
        catch
        {
            await db.RollbackAsync(Ct);
            throw;
        }

        await using var result = await db.ExecuteAsync(
            $"SELECT id FROM users WHERE name = '{probe}' ORDER BY id",
            Ct);
        var rows = await result.ToListAsync(Ct);

        Assert.Single(rows);
        Assert.Equal(123L, rows[0][0].AsInteger);
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
    public async Task Distinct_UsesDeclaredNoCaseCollation_InHashAndOrderedPaths()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)", Ct);
        await db.ExecuteAsync("INSERT INTO items VALUES (1, 'a')", Ct);
        await db.ExecuteAsync("INSERT INTO items VALUES (2, 'A')", Ct);

        await using (var hashResult = await db.ExecuteAsync("SELECT DISTINCT name FROM items", Ct))
        {
            var rows = await hashResult.ToListAsync(Ct);
            Assert.Single(rows);
            Assert.Equal("a", rows[0][0].AsText, ignoreCase: true);
        }

        await using (var orderedResult = await db.ExecuteAsync("SELECT DISTINCT name FROM items ORDER BY name", Ct))
        {
            var rows = await orderedResult.ToListAsync(Ct);
            Assert.Single(rows);
            Assert.Equal("a", rows[0][0].AsText, ignoreCase: true);
        }
    }

    [Fact]
    public async Task Distinct_UsesExplicitNoCaseCollation()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)", Ct);
        await db.ExecuteAsync("INSERT INTO items VALUES (1, 'a')", Ct);
        await db.ExecuteAsync("INSERT INTO items VALUES (2, 'A')", Ct);

        await using var result = await db.ExecuteAsync(
            "SELECT DISTINCT name COLLATE NOCASE AS normalized_name FROM items",
            Ct);
        var rows = await result.ToListAsync(Ct);

        Assert.Single(rows);
        Assert.Equal("a", rows[0][0].AsText, ignoreCase: true);
    }

    [Fact]
    public async Task GroupBy_UsesDeclaredAndExplicitNoCaseCollations()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE declared_items (id INTEGER PRIMARY KEY, bucket TEXT, name TEXT COLLATE NOCASE)", Ct);
        await db.ExecuteAsync("INSERT INTO declared_items VALUES (1, 'all', 'a')", Ct);
        await db.ExecuteAsync("INSERT INTO declared_items VALUES (2, 'all', 'A')", Ct);

        await using (var declaredResult = await db.ExecuteAsync(
            "SELECT name, COUNT(*) FROM declared_items GROUP BY name",
            Ct))
        {
            var rows = await declaredResult.ToListAsync(Ct);
            Assert.Single(rows);
            Assert.Equal(2L, rows[0][1].AsInteger);
        }

        await using (var distinctAggregateResult = await db.ExecuteAsync(
            "SELECT bucket, COUNT(DISTINCT name) FROM declared_items GROUP BY bucket",
            Ct))
        {
            var rows = await distinctAggregateResult.ToListAsync(Ct);
            Assert.Single(rows);
            Assert.Equal(1L, rows[0][1].AsInteger);
        }

        await db.ExecuteAsync("CREATE TABLE explicit_items (id INTEGER PRIMARY KEY, name TEXT)", Ct);
        await db.ExecuteAsync("INSERT INTO explicit_items VALUES (1, 'a')", Ct);
        await db.ExecuteAsync("INSERT INTO explicit_items VALUES (2, 'A')", Ct);

        await using (var explicitResult = await db.ExecuteAsync(
            "SELECT name COLLATE NOCASE, COUNT(*) FROM explicit_items GROUP BY name COLLATE NOCASE",
            Ct))
        {
            var rows = await explicitResult.ToListAsync(Ct);
            Assert.Single(rows);
            Assert.Equal(2L, rows[0][1].AsInteger);
        }
    }

    [Fact]
    public async Task Join_UsesDeclaredAndExplicitNoCaseCollations()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE declared_left (id INTEGER PRIMARY KEY, code TEXT COLLATE NOCASE)", Ct);
        await db.ExecuteAsync("CREATE TABLE declared_right (id INTEGER PRIMARY KEY, code TEXT COLLATE NOCASE)", Ct);
        await db.ExecuteAsync("INSERT INTO declared_left VALUES (1, 'a')", Ct);
        await db.ExecuteAsync("INSERT INTO declared_right VALUES (2, 'A')", Ct);
        await db.ExecuteAsync("INSERT INTO declared_right VALUES (3, 'b')", Ct);

        await using (var declaredResult = await db.ExecuteAsync(
            "SELECT l.id, r.id FROM declared_left l JOIN declared_right r ON l.code = r.code",
            Ct))
        {
            var rows = await declaredResult.ToListAsync(Ct);
            var row = Assert.Single(rows);
            Assert.Equal(1L, row[0].AsInteger);
            Assert.Equal(2L, row[1].AsInteger);
        }

        await db.ExecuteAsync("CREATE TABLE explicit_left (id INTEGER PRIMARY KEY, code TEXT)", Ct);
        await db.ExecuteAsync("CREATE TABLE explicit_right (id INTEGER PRIMARY KEY, code TEXT)", Ct);
        await db.ExecuteAsync("INSERT INTO explicit_left VALUES (4, 'x')", Ct);
        await db.ExecuteAsync("INSERT INTO explicit_right VALUES (5, 'X')", Ct);
        await db.ExecuteAsync("INSERT INTO explicit_right VALUES (6, 'y')", Ct);

        await using (var explicitResult = await db.ExecuteAsync(
            "SELECT l.id, r.id FROM explicit_left l JOIN explicit_right r ON l.code COLLATE NOCASE = r.code",
            Ct))
        {
            var rows = await explicitResult.ToListAsync(Ct);
            var row = Assert.Single(rows);
            Assert.Equal(4L, row[0].AsInteger);
            Assert.Equal(5L, row[1].AsInteger);
        }
    }

    [Fact]
    public async Task ScalarAggregates_UseNoCaseCollation_InFastAndFilteredPaths()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)", Ct);
        await db.ExecuteAsync("INSERT INTO items VALUES (1, 'Z')", Ct);
        await db.ExecuteAsync("INSERT INTO items VALUES (2, 'a')", Ct);
        await db.ExecuteAsync("INSERT INTO items VALUES (3, 'A')", Ct);

        await using (var distinctResult = await db.ExecuteAsync(
            "SELECT COUNT(DISTINCT name) FROM items",
            Ct))
        {
            Assert.Equal(2L, Assert.Single(await distinctResult.ToListAsync(Ct))[0].AsInteger);
        }

        await using (var filteredDistinctResult = await db.ExecuteAsync(
            "SELECT COUNT(DISTINCT name) FROM items WHERE id > 0",
            Ct))
        {
            Assert.Equal(2L, Assert.Single(await filteredDistinctResult.ToListAsync(Ct))[0].AsInteger);
        }

        await using (var minimumResult = await db.ExecuteAsync("SELECT MIN(name) FROM items", Ct))
        {
            Assert.Equal("a", Assert.Single(await minimumResult.ToListAsync(Ct))[0].AsText, ignoreCase: true);
        }

        await using (var filteredMaximumResult = await db.ExecuteAsync(
            "SELECT MAX(name) FROM items WHERE id > 0",
            Ct))
        {
            Assert.Equal("Z", Assert.Single(await filteredMaximumResult.ToListAsync(Ct))[0].AsText);
        }

        await using (var explicitDistinctResult = await db.ExecuteAsync(
            "SELECT COUNT(DISTINCT name COLLATE NOCASE) FROM items",
            Ct))
        {
            Assert.Equal(2L, Assert.Single(await explicitDistinctResult.ToListAsync(Ct))[0].AsInteger);
        }
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
