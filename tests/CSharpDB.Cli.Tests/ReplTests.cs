using CSharpDB.Engine;

namespace CSharpDB.Cli.Tests;

[Collection("CliConsole")]
public sealed class ReplTests
{
    [Fact]
    public void SqlScriptParser_SplitAllStatements_KeepsTriggerBodyAsSingleStatement()
    {
        string sql = """
            CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER);
            CREATE TRIGGER tr AFTER INSERT ON t BEGIN
                INSERT INTO t VALUES (NEW.id + 100, NEW.n);
            END;
            INSERT INTO t VALUES (1, 10);
            """;

        var statements = SqlScriptParser.SplitAllStatements(sql);

        Assert.Equal(3, statements.Count);
        Assert.Contains("CREATE TRIGGER tr AFTER INSERT ON t BEGIN", statements[1], StringComparison.OrdinalIgnoreCase);
        Assert.Contains("END;", statements[1], StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Repl_ReadCommand_ExecutesScriptIncludingTrigger()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempFilePath(".db");
        string scriptPath = NewTempFilePath(".sql");

        try
        {
            await File.WriteAllTextAsync(scriptPath, """
                CREATE TABLE s (id INTEGER PRIMARY KEY, n INTEGER);
                CREATE TABLE s_audit (n INTEGER);
                CREATE TRIGGER s_tr AFTER INSERT ON s BEGIN
                    INSERT INTO s_audit VALUES (42);
                END;
                INSERT INTO s VALUES (1, 5);
                """, ct);

            string output = await RunReplAsync(
                dbPath,
                $".read {scriptPath}{Environment.NewLine}.quit{Environment.NewLine}",
                ct);

            Assert.Contains("Script complete: 4 passed, 0 failed.", output, StringComparison.Ordinal);

            await using var db = await Database.OpenAsync(dbPath, ct);
            long auditCount = await QueryCountAsync(db, "SELECT COUNT(*) FROM s_audit;", ct);
            Assert.Equal(1, auditCount);
        }
        finally
        {
            DeleteIfExists(scriptPath);
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task Repl_SnapshotMode_BlocksWritesUntilDisabled()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempFilePath(".db");

        try
        {
            string input = string.Join(Environment.NewLine, new[]
            {
                "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER);",
                ".snapshot on",
                "INSERT INTO t VALUES (1, 10);",
                ".snapshot off",
                "INSERT INTO t VALUES (1, 10);",
                ".quit",
                "",
            });

            string output = await RunReplAsync(dbPath, input, ct);
            Assert.Contains("Snapshot mode is read-only", output, StringComparison.OrdinalIgnoreCase);

            await using var db = await Database.OpenAsync(dbPath, ct);
            long count = await QueryCountAsync(db, "SELECT COUNT(*) FROM t;", ct);
            Assert.Equal(1, count);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task Repl_ExplicitTransactionRollback_DiscardsWrites()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempFilePath(".db");

        try
        {
            string input = string.Join(Environment.NewLine, new[]
            {
                "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER);",
                ".begin",
                "INSERT INTO t VALUES (1, 10);",
                ".rollback",
                ".quit",
                "",
            });

            string output = await RunReplAsync(dbPath, input, ct);
            Assert.Contains("Transaction rolled back.", output, StringComparison.OrdinalIgnoreCase);

            await using var db = await Database.OpenAsync(dbPath, ct);
            long count = await QueryCountAsync(db, "SELECT COUNT(*) FROM t;", ct);
            Assert.Equal(0, count);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task Repl_MultiStatementSingleLine_ExecutesAllStatements()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempFilePath(".db");

        try
        {
            string input = string.Join(Environment.NewLine, new[]
            {
                "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO t VALUES (1, 10); INSERT INTO t VALUES (2, 20);",
                ".quit",
                "",
            });

            await RunReplAsync(dbPath, input, ct);

            await using var db = await Database.OpenAsync(dbPath, ct);
            long count = await QueryCountAsync(db, "SELECT COUNT(*) FROM t;", ct);
            Assert.Equal(2, count);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task Repl_SchemaCommand_ShowsIdentityModifier()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempFilePath(".db");

        try
        {
            string input = string.Join(Environment.NewLine, new[]
            {
                "CREATE TABLE t (id INTEGER PRIMARY KEY IDENTITY, n INTEGER);",
                ".schema t",
                ".quit",
                "",
            });

            string output = await RunReplAsync(dbPath, input, ct);
            string plainOutput = System.Text.RegularExpressions.Regex.Replace(output, @"\x1B\[[0-9;]*m", string.Empty);
            Assert.Contains("PRIMARY KEY IDENTITY", plainOutput, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    private static async Task<long> QueryCountAsync(Database db, string sql, CancellationToken ct)
    {
        await using var result = await db.ExecuteAsync(sql, ct);
        var rows = await result.ToListAsync(ct);
        var row = Assert.Single(rows);
        return row[0].AsInteger;
    }

    private static async Task<string> RunReplAsync(string dbPath, string input, CancellationToken ct)
    {
        await using var db = await Database.OpenAsync(dbPath, ct);

        var commands = BuildCommands();
        var output = new StringWriter();
        using var repl = new Repl(db, dbPath, output, commands);

        var originalIn = Console.In;
        try
        {
            Console.SetIn(new StringReader(input));
            await repl.RunAsync(ct);
        }
        finally
        {
            Console.SetIn(originalIn);
        }

        return output.ToString();
    }

    private static List<IMetaCommand> BuildCommands()
    {
        var commands = new List<IMetaCommand>();
        var help = new HelpCommand(commands);
        commands.Add(help);
        commands.Add(new InfoCommand());
        commands.Add(new TablesCommand());
        commands.Add(new SchemaCommand());
        commands.Add(new IndexesCommand());
        commands.Add(new ViewsCommand());
        commands.Add(new ViewCommand());
        commands.Add(new TriggersCommand());
        commands.Add(new TriggerCommand());
        commands.Add(new CollectionsCommand());
        commands.Add(new BeginTransactionCommand());
        commands.Add(new CommitCommand());
        commands.Add(new RollbackCommand());
        commands.Add(new CheckpointCommand());
        commands.Add(new SnapshotCommand());
        commands.Add(new SyncPointCommand());
        commands.Add(new TimingCommand());
        commands.Add(new ReadCommand());
        return commands;
    }

    private static string NewTempFilePath(string extension)
    {
        return Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_cli_test_{Guid.NewGuid():N}{extension}");
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
