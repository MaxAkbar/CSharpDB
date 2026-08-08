using System.Net;
using System.Text.RegularExpressions;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed partial class SqlDocumentationExampleTests
{
    private static readonly IReadOnlyDictionary<string, ExpectedCoverage> SqlDocumentationFiles =
        new Dictionary<string, ExpectedCoverage>(StringComparer.Ordinal)
        {
            [Path.Combine("www", "docs", "sql.html")] =
                new(BlockCount: 11, ExecutedBlockCount: 11, SyntaxBlockCount: 0, GrammarBlockCount: 0),
            [Path.Combine("www", "docs", "sql-reference.html")] =
                new(BlockCount: 37, ExecutedBlockCount: 10, SyntaxBlockCount: 1, GrammarBlockCount: 26),
        };

    private static readonly IReadOnlyDictionary<SqlTypeKind, string[]> ExpectedSqlTypeNames =
        new Dictionary<SqlTypeKind, string[]>
        {
            [SqlTypeKind.Boolean] = ["BOOLEAN", "BOOL", "BIT"],
            [SqlTypeKind.TinyInt] = ["TINYINT"],
            [SqlTypeKind.SmallInt] = ["SMALLINT"],
            [SqlTypeKind.Integer] = ["INTEGER", "INT"],
            [SqlTypeKind.BigInt] = ["BIGINT"],
            [SqlTypeKind.Real] = ["REAL"],
            [SqlTypeKind.Double] = ["DOUBLE PRECISION", "DOUBLE", "FLOAT"],
            [SqlTypeKind.Decimal] = ["DECIMAL", "NUMERIC"],
            [SqlTypeKind.Char] = ["CHAR", "CHARACTER", "NCHAR"],
            [SqlTypeKind.VarChar] = ["VARCHAR", "CHARACTER VARYING", "NVARCHAR"],
            [SqlTypeKind.Text] = ["TEXT", "CLOB"],
            [SqlTypeKind.Binary] = ["BINARY"],
            [SqlTypeKind.VarBinary] = ["VARBINARY"],
            [SqlTypeKind.Blob] = ["BLOB"],
            [SqlTypeKind.Uuid] = ["UUID", "GUID", "UNIQUEIDENTIFIER"],
            [SqlTypeKind.Date] = ["DATE"],
            [SqlTypeKind.Time] = ["TIME"],
            [SqlTypeKind.Timestamp] = ["DATETIME2", "DATETIME"],
            [SqlTypeKind.TimestampWithTimeZone] =
                ["DATETIMEOFFSET", "TIMESTAMP WITH TIME ZONE"],
            [SqlTypeKind.IntervalYearToMonth] = ["INTERVAL YEAR TO MONTH"],
            [SqlTypeKind.IntervalDayToSecond] = ["INTERVAL DAY TO SECOND"],
            [SqlTypeKind.Json] = ["JSON"],
            [SqlTypeKind.Xml] = ["XML"],
            [SqlTypeKind.Bit] = ["BIT"],
            [SqlTypeKind.VarBit] = ["BIT VARYING", "VARBIT"],
        };

    private static readonly string[] ExpectedRowVersionTypeNames = ["ROWVERSION", "TIMESTAMP"];

    private static readonly IReadOnlyDictionary<string, string> FixtureSqlByName =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["employees-table"] =
                """
                CREATE TABLE Employees (
                    Id INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL,
                    DeptId INTEGER,
                    Salary REAL,
                    HireDate TEXT,
                    Photo BLOB
                );
                """,
            ["employees-trigger"] =
                """
                CREATE TABLE Employees (
                    Id INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL,
                    DeptId INTEGER,
                    Salary REAL,
                    HireDate TEXT,
                    Photo BLOB
                );
                CREATE TABLE AuditLog (
                    Action TEXT NOT NULL,
                    EmployeeName TEXT
                );
                """,
            ["employees-alter"] =
                """
                CREATE TABLE Departments (
                    Id INTEGER PRIMARY KEY,
                    DeptName TEXT NOT NULL
                );
                CREATE TABLE Employees (
                    Id INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL,
                    DeptId INTEGER,
                    Salary REAL,
                    HireDate TEXT,
                    Photo BLOB,
                    CONSTRAINT fk_employees_deptid_a1b2c3d4
                        FOREIGN KEY (DeptId) REFERENCES Departments (Id)
                );
                CREATE INDEX idx_emp_dept ON Employees (DeptId);
                CREATE VIEW HighEarners AS
                    SELECT Name, Salary FROM Employees WHERE Salary > 100000;
                """,
            ["employees-profile"] =
                """
                CREATE TABLE Employees (
                    Id INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL,
                    DeptId INTEGER,
                    Salary REAL,
                    HireDate TEXT,
                    Photo BLOB
                );
                INSERT INTO Employees (Id, Name, DeptId, Salary)
                    VALUES (1, 'Alice', 10, 95000.0);
                """,
            ["employees-query"] =
                """
                CREATE TABLE Departments (
                    Id INTEGER PRIMARY KEY,
                    DeptName TEXT NOT NULL,
                    Region TEXT NOT NULL
                );
                CREATE TABLE Employees (
                    Id INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL,
                    DeptId INTEGER,
                    Salary REAL,
                    HireDate TEXT,
                    Photo BLOB
                );
                CREATE TABLE Reviews (
                    Id INTEGER PRIMARY KEY,
                    EmpId INTEGER NOT NULL
                );
                CREATE TABLE Contractors (
                    Id INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL,
                    Active INTEGER NOT NULL
                );
                INSERT INTO Departments VALUES (10, 'Sales', 'West');
                INSERT INTO Departments VALUES (20, 'Engineering', 'East');
                INSERT INTO Employees VALUES (1, 'Alice', 10, 95000.0, '2024-01-10', NULL);
                INSERT INTO Employees VALUES (2, 'Bob', 20, 88000.0, '2024-01-15', NULL);
                INSERT INTO Employees VALUES (3, 'Cara', 10, 105000.0, '2024-02-01', NULL);
                INSERT INTO Employees VALUES (4, 'Dan', 10, 110000.0, '2024-02-10', NULL);
                INSERT INTO Employees VALUES (5, 'Eve', 10, 120000.0, '2024-03-01', NULL);
                INSERT INTO Employees VALUES (6, 'Finn', 10, 125000.0, '2024-03-10', NULL);
                INSERT INTO Reviews VALUES (1, 1);
                INSERT INTO Contractors VALUES (1, 'Grace', 1);
                """,
            ["employees-mutations"] =
                """
                CREATE TABLE Employees (
                    Id INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL,
                    DeptId INTEGER,
                    Salary REAL,
                    HireDate TEXT,
                    Photo BLOB
                );
                INSERT INTO Employees VALUES (1, 'Alice', 10, 95000.0, '2024-01-10', NULL);
                INSERT INTO Employees VALUES (99, 'Retired', 20, 50000.0, '2020-01-01', NULL);
                """,
            ["hygiene"] =
                """
                CREATE TABLE Customers (
                    Id INTEGER PRIMARY KEY,
                    Email TEXT,
                    DisplayName TEXT
                );
                CREATE TABLE Books (
                    Id INTEGER PRIMARY KEY,
                    Title TEXT NOT NULL
                );
                CREATE TABLE Bookings (
                    Id INTEGER PRIMARY KEY,
                    BookId INTEGER,
                    CONSTRAINT fk_bookings_books
                        FOREIGN KEY (BookId) REFERENCES Books (Id)
                );
                INSERT INTO Customers VALUES (1, 'ada@example.com', NULL);
                INSERT INTO Customers VALUES (2, 'ADA@example.com', 'Ada');
                INSERT INTO Books VALUES (1, 'Notes on the Analytical Engine');
                INSERT INTO Bookings VALUES (1, 1);
                """,
            ["reference-subqueries"] =
                """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    age INTEGER NOT NULL
                );
                CREATE TABLE other_table (
                    id INTEGER PRIMARY KEY
                );
                INSERT INTO users VALUES (1, 30);
                INSERT INTO users VALUES (2, 40);
                INSERT INTO other_table VALUES (1);
                """,
            ["reference-duplicates"] =
                """
                CREATE TABLE Customers (
                    Id INTEGER PRIMARY KEY,
                    Email TEXT,
                    DisplayName TEXT
                );
                CREATE TABLE Contacts (
                    Id INTEGER PRIMARY KEY,
                    FirstName TEXT,
                    LastName TEXT,
                    Phone TEXT
                );
                INSERT INTO Customers VALUES (1, 'ada@example.com', NULL);
                INSERT INTO Customers VALUES (2, 'ADA@example.com', 'Ada');
                INSERT INTO Contacts VALUES (1, 'Ada', 'Lovelace', '555-0100');
                INSERT INTO Contacts VALUES (2, 'Ada', 'Lovelace', '555-0100');
                """,
            ["reference-customers"] =
                """
                CREATE TABLE Customers (
                    Id INTEGER PRIMARY KEY,
                    Email TEXT,
                    DisplayName TEXT
                );
                INSERT INTO Customers VALUES (1, 'ada@example.com', NULL);
                INSERT INTO Customers VALUES (2, 'ADA@example.com', 'Ada');
                """,
            ["reference-validation"] =
                """
                CREATE TABLE Customers (
                    Id INTEGER PRIMARY KEY,
                    Email TEXT
                );
                INSERT INTO Customers VALUES (1, 'ada@example.com');
                """,
            ["reference-orphans"] =
                """
                CREATE TABLE Books (
                    Id INTEGER PRIMARY KEY,
                    Title TEXT NOT NULL
                );
                CREATE TABLE Bookings (
                    Id INTEGER PRIMARY KEY,
                    BookId INTEGER,
                    CONSTRAINT fk_bookings_books
                        FOREIGN KEY (BookId) REFERENCES Books (Id)
                );
                INSERT INTO Books VALUES (1, 'Notes on the Analytical Engine');
                INSERT INTO Bookings VALUES (1, 1);
                """,
            ["reference-users"] =
                """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    status TEXT,
                    age INTEGER
                );
                INSERT INTO users VALUES (1, 'active', 30);
                INSERT INTO users VALUES (2, 'pending', 40);
                INSERT INTO users VALUES (3, 'active', NULL);
                """,
            ["reference-window"] =
                """
                CREATE TABLE employees (
                    id INTEGER PRIMARY KEY,
                    department TEXT NOT NULL,
                    salary REAL NOT NULL
                );
                INSERT INTO employees VALUES (1, 'Engineering', 120000.0);
                INSERT INTO employees VALUES (2, 'Engineering', 110000.0);
                INSERT INTO employees VALUES (3, 'Sales', 100000.0);
                """,
        };

    [Fact]
    public async Task PublicSqlBlocks_AreClassifiedAndConcreteExamplesRunFromSource()
    {
        string repositoryRoot = FindRepositoryRoot();
        int executedBlockCount = 0;
        int executedStatementCount = 0;
        int syntaxBlockCount = 0;
        int syntaxStatementCount = 0;
        int grammarBlockCount = 0;

        foreach ((string relativePath, ExpectedCoverage expected) in SqlDocumentationFiles)
        {
            string fullPath = Path.Combine(repositoryRoot, relativePath);
            string html = await File.ReadAllTextAsync(
                fullPath,
                TestContext.Current.CancellationToken);
            MatchCollection blocks = SqlBlockRegex().Matches(html);

            int documentExecutableCount = 0;
            int documentSyntaxCount = 0;
            int documentGrammarCount = 0;
            foreach (Match block in blocks.Cast<Match>())
            {
                int lineNumber = html.AsSpan(0, block.Index).Count('\n') + 1;
                Match classification = ClassificationRegex().Match(
                    block.Groups["opening"].Value);
                Assert.True(
                    classification.Success,
                    $"{relativePath}:{lineNumber} has an unclassified SQL block. " +
                    "Set data-csharpdb-example to execute, syntax, or grammar.");

                string kind = classification.Groups["kind"].Value;
                Assert.True(
                    kind is "execute" or "syntax" or "grammar",
                    $"{relativePath}:{lineNumber} uses invalid SQL block classification '{kind}'.");
                string sql = ExtractSql(block.Groups["sql"].Value);

                if (kind == "grammar")
                {
                    documentGrammarCount++;
                    grammarBlockCount++;
                    Assert.Matches(
                        GrammarNotationRegex(),
                        sql);
                    continue;
                }

                IReadOnlyList<SqlScriptStatement> statements = ParseSql(
                    sql,
                    relativePath,
                    lineNumber);
                Assert.NotEmpty(statements);

                if (kind == "syntax")
                {
                    documentSyntaxCount++;
                    syntaxBlockCount++;
                    syntaxStatementCount += statements.Count;
                    continue;
                }

                documentExecutableCount++;
                executedBlockCount++;
                executedStatementCount += statements.Count;
                string? fixtureName = ReadFixtureName(block.Groups["opening"].Value);
                Exception? failure = await Record.ExceptionAsync(
                    async () =>
                    {
                        await using Database database =
                            await Database.OpenInMemoryAsync(
                                TestContext.Current.CancellationToken);
                        if (fixtureName is not null)
                        {
                            Assert.True(
                                FixtureSqlByName.TryGetValue(fixtureName, out string? fixtureSql),
                                $"{relativePath}:{lineNumber} requests unknown fixture '{fixtureName}'.");
                            await ExecuteScriptAsync(database, fixtureSql!);
                        }

                        await ExecuteStatementsAsync(database, statements);
                    });

                Assert.True(
                    failure is null,
                    $"{relativePath}:{lineNumber} executable SQL failed: {failure}");
            }

            Assert.Equal(expected.BlockCount, blocks.Count);
            Assert.Equal(expected.ExecutedBlockCount, documentExecutableCount);
            Assert.Equal(expected.SyntaxBlockCount, documentSyntaxCount);
            Assert.Equal(expected.GrammarBlockCount, documentGrammarCount);
        }

        Assert.Equal(21, executedBlockCount);
        Assert.Equal(55, executedStatementCount);
        Assert.Equal(1, syntaxBlockCount);
        Assert.Equal(4, syntaxStatementCount);
        Assert.Equal(26, grammarBlockCount);
    }

    [Fact]
    public async Task CompactMcpReference_AgreesWithSupportedSqlAndStableLimitations()
    {
        string repositoryRoot = FindRepositoryRoot();
        string compactReferenceSource = await File.ReadAllTextAsync(
            Path.Combine(
                repositoryRoot,
                "src",
                "CSharpDB.Mcp",
                "Helpers",
                "SqlReference.cs"),
            TestContext.Current.CancellationToken);
        string publicReference = await File.ReadAllTextAsync(
            Path.Combine(repositoryRoot, "www", "docs", "sql-reference.html"),
            TestContext.Current.CancellationToken);
        string[] supportedCoverage =
        [
            "SELECT [DISTINCT]",
            "Scalar, IN/NOT IN, and EXISTS/NOT EXISTS subqueries are supported.",
            "Set operations: UNION, UNION ALL, INTERSECT, EXCEPT.",
            "DEFAULT literal",
            "[CONSTRAINT name] CHECK (expression)",
            "[CONSTRAINT name] UNIQUE (col [, ...])",
            "[CONSTRAINT name] FOREIGN KEY (child_col [, ...])",
            "MATCH SIMPLE",
        ];
        foreach (string expected in supportedCoverage)
            Assert.Contains(expected, compactReferenceSource, StringComparison.Ordinal);

        string[] staleUnsupportedClaims =
        [
            "• SELECT DISTINCT",
            "• Subqueries (nested SELECT in WHERE / FROM)",
            "• UNION / INTERSECT / EXCEPT",
            "• EXISTS / NOT EXISTS",
            "• DEFAULT, CHECK, FOREIGN KEY, UNIQUE column constraints",
            "• String functions (SUBSTR, UPPER, LOWER, TRIM, CONCAT, etc.)",
            "• Math functions (ABS, ROUND, CEIL, FLOOR, MOD, etc.)",
            "• Date functions (DATE, DATETIME, STRFTIME, etc.)",
            "• CAST, COALESCE, NULLIF, IIF, CASE WHEN",
        ];
        foreach (string staleClaim in staleUnsupportedClaims)
            Assert.DoesNotContain(staleClaim, compactReferenceSource, StringComparison.Ordinal);

        string[] stableLimitations =
        [
            "CASE/WHEN and CAST expressions",
            "RETURNING on INSERT/UPDATE/DELETE",
            "UPSERT, REPLACE, INSERT OR REPLACE, and INSERT ... ON CONFLICT",
            "INTERSECT ALL and EXCEPT ALL",
            "WITH RECURSIVE",
            "MATCH FULL, MATCH PARTIAL, and DEFERRABLE foreign keys",
            "Trigger WHEN conditions",
            "SQL CREATE PROCEDURE and CALL",
            "FULL OUTER JOIN / NATURAL JOIN",
            "SQL transaction/savepoint statements",
            "Multiple statements in one call",
        ];
        foreach (string limitation in stableLimitations)
            Assert.Contains(limitation, compactReferenceSource, StringComparison.Ordinal);

        int scalarFunctionsStart = compactReferenceSource.IndexOf(
            "── SCALAR FUNCTIONS ──",
            StringComparison.Ordinal);
        int notSupportedStart = compactReferenceSource.IndexOf(
            "── INTENTIONALLY NOT SUPPORTED ──",
            StringComparison.Ordinal);
        Assert.True(scalarFunctionsStart >= 0);
        Assert.True(notSupportedStart > scalarFunctionsStart);
        string documentedFunctions = compactReferenceSource[
            scalarFunctionsStart..notSupportedStart];
        foreach (DbBuiltInFunctionDescriptor function in DbBuiltInFunctionRegistry.Functions)
        {
            Assert.Matches(
                $@"\b{Regex.Escape(function.Name)}\b",
                documentedFunctions);
            foreach (string alias in function.Aliases)
            {
                Assert.Matches(
                    $@"\b{Regex.Escape(alias)}\b",
                    documentedFunctions);
            }
        }

        Assert.Contains("[MATCH SIMPLE]", publicReference, StringComparison.Ordinal);
        Assert.Contains(
            "<code>MATCH SIMPLE</code> may be written explicitly and is also the default",
            publicReference,
            StringComparison.Ordinal);
        Assert.Contains(
            "<li>Deferred foreign-key constraints and <code>MATCH FULL</code>/<code>MATCH PARTIAL</code></li>",
            publicReference,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task PublicSqlReference_ListsEveryLogicalTypeAndAcceptedBaseNameExactlyOnce()
    {
        string repositoryRoot = FindRepositoryRoot();
        string publicReference = await File.ReadAllTextAsync(
            Path.Combine(repositoryRoot, "www", "docs", "sql-reference.html"),
            TestContext.Current.CancellationToken);
        string localTypeContract = await File.ReadAllTextAsync(
            Path.Combine(repositoryRoot, "docs", "sql-type-semantics-4.5.md"),
            TestContext.Current.CancellationToken);

        Match[] kindMarkers = SqlTypeKindMarkerRegex()
            .Matches(publicReference)
            .Cast<Match>()
            .ToArray();
        Assert.Equal(Enum.GetValues<SqlTypeKind>().Length, kindMarkers.Length);
        Assert.Equal(Enum.GetValues<SqlTypeKind>().Length, ExpectedSqlTypeNames.Count);

        foreach (SqlTypeKind kind in Enum.GetValues<SqlTypeKind>())
        {
            Assert.True(
                ExpectedSqlTypeNames.TryGetValue(kind, out string[]? expectedNames),
                $"The documentation guard does not define accepted names for {kind}.");
            Match marker = Assert.Single(
                kindMarkers,
                match => string.Equals(
                    match.Groups["kind"].Value,
                    kind.ToString(),
                    StringComparison.Ordinal));
            AssertSqlTypeNames(marker.Groups["tag"].Value, expectedNames!, kind.ToString());
        }

        Match rowVersionMarker = Assert.Single(
            SqlTypeSpecialMarkerRegex()
                .Matches(publicReference)
                .Cast<Match>(),
            match => string.Equals(
                match.Groups["special"].Value,
                "RowVersion",
                StringComparison.Ordinal));
        AssertSqlTypeNames(
            rowVersionMarker.Groups["tag"].Value,
            ExpectedRowVersionTypeNames,
            "RowVersion");

        string[] allAcceptedNames = ExpectedSqlTypeNames.Values
            .SelectMany(static names => names)
            .Concat(ExpectedRowVersionTypeNames)
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        foreach (string acceptedName in allAcceptedNames)
        {
            Assert.Contains(
                $"`{acceptedName}`",
                localTypeContract,
                StringComparison.Ordinal);
        }
    }

    private static void AssertSqlTypeNames(
        string markerTag,
        IReadOnlyCollection<string> expectedNames,
        string typeLabel)
    {
        Match namesAttribute = Assert.Single(
            SqlTypeNamesAttributeRegex().Matches(markerTag).Cast<Match>());
        string[] actualNames = namesAttribute.Groups["names"].Value
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

        Assert.True(
            actualNames.Length == actualNames.Distinct(StringComparer.Ordinal).Count(),
            $"{typeLabel} contains a duplicate data-sql-type-names entry.");
        string[] expected = expectedNames.Order(StringComparer.Ordinal).ToArray();
        string[] actual = actualNames.Order(StringComparer.Ordinal).ToArray();
        Assert.True(
            expected.SequenceEqual(actual, StringComparer.Ordinal),
            $"{typeLabel} names differ. Expected [{string.Join(", ", expected)}], " +
            $"actual [{string.Join(", ", actual)}].");
        Assert.DoesNotContain(
            "NULL",
            actualNames,
            StringComparer.Ordinal);
    }

    private static IReadOnlyList<SqlScriptStatement> ParseSql(
        string sql,
        string relativePath,
        int lineNumber)
    {
        try
        {
            return SqlScriptParser.Parse(
                sql,
                cancellationToken: TestContext.Current.CancellationToken);
        }
        catch (Exception ex)
        {
            throw new Xunit.Sdk.XunitException(
                $"{relativePath}:{lineNumber} parser-checked SQL failed: {ex}");
        }
    }

    private static async Task ExecuteScriptAsync(Database database, string sql)
        => await ExecuteStatementsAsync(
            database,
            SqlScriptParser.Parse(
                sql,
                cancellationToken: TestContext.Current.CancellationToken));

    private static async Task ExecuteStatementsAsync(
        Database database,
        IReadOnlyList<SqlScriptStatement> statements)
    {
        foreach (SqlScriptStatement statement in statements)
        {
            await using QueryResult result = await database.ExecuteAsync(
                statement.Statement,
                TestContext.Current.CancellationToken);
            await result.ToListAsync(TestContext.Current.CancellationToken);
        }
    }

    private static string? ReadFixtureName(string openingTag)
    {
        Match fixture = FixtureRegex().Match(openingTag);
        return fixture.Success ? fixture.Groups["fixture"].Value : null;
    }

    private static string ExtractSql(string encodedHtml)
    {
        string withoutMarkup = HtmlTagRegex().Replace(encodedHtml, string.Empty);
        return WebUtility.HtmlDecode(withoutMarkup).Trim();
    }

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "CSharpDB.slnx")))
                return directory.FullName;

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException(
            "Could not locate repository root from test base directory.");
    }

    [GeneratedRegex(
        """(?<opening><div class="code-block"[^>]*data-title="SQL"[^>]*>)\s*<pre><code(?:\s+[^>]*)?>(?<sql>.*?)</code></pre>\s*</div>""",
        RegexOptions.Singleline | RegexOptions.CultureInvariant)]
    private static partial Regex SqlBlockRegex();

    [GeneratedRegex(
        "\\bdata-csharpdb-example=\"(?<kind>[^\"]+)\"",
        RegexOptions.CultureInvariant)]
    private static partial Regex ClassificationRegex();

    [GeneratedRegex(
        "\\bdata-csharpdb-fixture=\"(?<fixture>[^\"]+)\"",
        RegexOptions.CultureInvariant)]
    private static partial Regex FixtureRegex();

    [GeneratedRegex(
        """(?:\[[^\]]+\]|\.\.\.|\{[^}]+\}|\b(?:table_name|column_name|index_name|view_name|trigger_name|constraint_name|rule_name|child_table|parent_table|select_statement|statement1|expression1|value_or_DEFAULT1)\b)""",
        RegexOptions.CultureInvariant)]
    private static partial Regex GrammarNotationRegex();

    [GeneratedRegex("<[^>]+>", RegexOptions.CultureInvariant)]
    private static partial Regex HtmlTagRegex();

    [GeneratedRegex(
        """(?<tag><[A-Za-z][^>]*\bdata-sql-type-kind="(?<kind>[^"]+)"[^>]*>)""",
        RegexOptions.CultureInvariant)]
    private static partial Regex SqlTypeKindMarkerRegex();

    [GeneratedRegex(
        """(?<tag><[A-Za-z][^>]*\bdata-sql-type-special="(?<special>[^"]+)"[^>]*>)""",
        RegexOptions.CultureInvariant)]
    private static partial Regex SqlTypeSpecialMarkerRegex();

    [GeneratedRegex(
        """\bdata-sql-type-names="(?<names>[^"]*)""",
        RegexOptions.CultureInvariant)]
    private static partial Regex SqlTypeNamesAttributeRegex();

    private sealed record ExpectedCoverage(
        int BlockCount,
        int ExecutedBlockCount,
        int SyntaxBlockCount,
        int GrammarBlockCount);
}
