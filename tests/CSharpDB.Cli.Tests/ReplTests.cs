using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using System.Text.Json;

namespace CSharpDB.Cli.Tests;

[Collection("CliConsole")]
public sealed class ReplTests
{
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
    public async Task Repl_MultiLineTriggerDefinition_ExecutesWhenStatementCompletes()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempFilePath(".db");

        try
        {
            string input = string.Join(Environment.NewLine, new[]
            {
                "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER);",
                "CREATE TABLE t_audit (n INTEGER);",
                "CREATE TRIGGER t_tr AFTER INSERT ON t BEGIN",
                "    INSERT INTO t_audit VALUES (42);",
                "END;",
                "INSERT INTO t VALUES (1, 10);",
                ".quit",
                "",
            });

            await RunReplAsync(dbPath, input, ct);

            await using var db = await Database.OpenAsync(dbPath, ct);
            long auditCount = await QueryCountAsync(db, "SELECT COUNT(*) FROM t_audit;", ct);
            Assert.Equal(1, auditCount);
        }
        finally
        {
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

    [Fact]
    public async Task Repl_SchemaCommand_ShowsForeignKeyClause()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempFilePath(".db");

        try
        {
            string input = string.Join(Environment.NewLine, new[]
            {
                "CREATE TABLE parents (id INTEGER PRIMARY KEY);",
                "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id) ON DELETE CASCADE);",
                ".schema children",
                ".quit",
                "",
            });

            string output = await RunReplAsync(dbPath, input, ct);
            string plainOutput = System.Text.RegularExpressions.Regex.Replace(output, @"\x1B\[[0-9;]*m", string.Empty);
            Assert.Contains("REFERENCES parents(id)", plainOutput, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("ON DELETE CASCADE", plainOutput, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task Repl_InfoCommand_DoesNotFailWhenLocalDirectFeaturesAreEnabled()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempFilePath(".db");

        try
        {
            string input = string.Join(Environment.NewLine, new[]
            {
                ".info",
                ".quit",
                "",
            });

            string output = await RunReplAsync(dbPath, input, ct);
            string plainOutput = System.Text.RegularExpressions.Regex.Replace(output, @"\x1B\[[0-9;]*m", string.Empty);

            Assert.Contains("Objects:", plainOutput, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Error:", plainOutput, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task Repl_DotCommandPalette_ShowsHelpWhenInputIsRedirected()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempFilePath(".db");

        try
        {
            string input = string.Join(Environment.NewLine, new[]
            {
                ".",
                ".quit",
                "",
            });

            string output = await RunReplAsync(dbPath, input, ct);
            string plainOutput = System.Text.RegularExpressions.Regex.Replace(output, @"\x1B\[[0-9;]*m", string.Empty);

            Assert.Contains("Available Commands", plainOutput, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(".tables", plainOutput, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public void Repl_MenuLayout_ClampsTopWithinConsoleBuffer()
    {
        Repl.MenuLayout layout = Repl.CalculateMenuLayout(
            requestedTop: 37,
            selectedIndex: 0,
            itemCount: 12,
            bufferHeight: 40);

        Assert.Equal(25, layout.Top);
        Assert.Equal(15, layout.LineCount);
        Assert.Equal(12, layout.VisibleItemCount);
        Assert.Equal(0, layout.WindowStart);
    }

    [Fact]
    public void Repl_MenuLayout_ScrollsVisibleWindowWhenMenuExceedsBuffer()
    {
        Repl.MenuLayout layout = Repl.CalculateMenuLayout(
            requestedTop: 20,
            selectedIndex: 7,
            itemCount: 12,
            bufferHeight: 8);

        Assert.Equal(0, layout.Top);
        Assert.Equal(8, layout.LineCount);
        Assert.Equal(5, layout.VisibleItemCount);
        Assert.Equal(3, layout.WindowStart);
    }

    [Fact]
    public async Task Repl_BackupCommand_WritesSnapshotAndManifest()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempFilePath(".db");
        string backupPath = NewTempFilePath(".backup.db");
        string manifestPath = backupPath + ".manifest.json";

        try
        {
            string input = string.Join(Environment.NewLine, new[]
            {
                "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);",
                "INSERT INTO t VALUES (1, 'alpha');",
                $".backup \"{backupPath}\" --with-manifest",
                ".quit",
                "",
            });

            string output = await RunReplAsync(dbPath, input, ct);
            Assert.Contains("Backup saved to", output, StringComparison.Ordinal);
            Assert.True(File.Exists(backupPath));
            Assert.True(File.Exists(manifestPath));
            Assert.False(File.Exists(backupPath + ".wal"));

            await using var backupDb = await Database.OpenAsync(backupPath, ct);
            long count = await QueryCountAsync(backupDb, "SELECT COUNT(*) FROM t;", ct);
            Assert.Equal(1L, count);

            string manifestJson = await File.ReadAllTextAsync(manifestPath, ct);
            Assert.Contains("backupDatabasePath", manifestJson, StringComparison.Ordinal);
            Assert.Contains("sha256", manifestJson, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
            DeleteIfExists(backupPath);
            DeleteIfExists(backupPath + ".wal");
            DeleteIfExists(manifestPath);
        }
    }

    [Fact]
    public async Task Repl_RestoreCommand_ReplacesCurrentDatabase()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempFilePath(".db");
        string restoreSourcePath = NewTempFilePath(".restore-source.db");

        try
        {
            await using (var sourceDb = await Database.OpenAsync(restoreSourcePath, ct))
            {
                await sourceDb.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);", ct);
                await sourceDb.ExecuteAsync("INSERT INTO t VALUES (1, 'from-restore');", ct);
            }

            string input = string.Join(Environment.NewLine, new[]
            {
                "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);",
                "INSERT INTO t VALUES (1, 'current');",
                $".restore \"{restoreSourcePath}\"",
                ".quit",
                "",
            });

            string output = await RunReplAsync(dbPath, input, ct);
            Assert.Contains("Restore complete from", output, StringComparison.Ordinal);

            await using var restoredDb = await Database.OpenAsync(dbPath, ct);
            await using var result = await restoredDb.ExecuteAsync("SELECT name FROM t WHERE id = 1;", ct);
            var rows = await result.ToListAsync(ct);
            Assert.Single(rows);
            Assert.Equal("from-restore", rows[0][0].AsText);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
            DeleteIfExists(restoreSourcePath);
            DeleteIfExists(restoreSourcePath + ".wal");
        }
    }

    [Fact]
    public async Task Repl_RestoreValidateOnly_DoesNotModifyCurrentDatabase()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempFilePath(".db");
        string restoreSourcePath = NewTempFilePath(".restore-source.db");

        try
        {
            await using (var sourceDb = await Database.OpenAsync(restoreSourcePath, ct))
            {
                await sourceDb.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);", ct);
                await sourceDb.ExecuteAsync("INSERT INTO t VALUES (1, 'source');", ct);
            }

            string input = string.Join(Environment.NewLine, new[]
            {
                "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);",
                "INSERT INTO t VALUES (1, 'current');",
                $".restore \"{restoreSourcePath}\" --validate-only",
                ".quit",
                "",
            });

            string output = await RunReplAsync(dbPath, input, ct);
            Assert.Contains("Restore source is valid", output, StringComparison.Ordinal);

            await using var currentDb = await Database.OpenAsync(dbPath, ct);
            await using var result = await currentDb.ExecuteAsync("SELECT name FROM t WHERE id = 1;", ct);
            var rows = await result.ToListAsync(ct);
            Assert.Single(rows);
            Assert.Equal("current", rows[0][0].AsText);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
            DeleteIfExists(restoreSourcePath);
            DeleteIfExists(restoreSourcePath + ".wal");
        }
    }

    [Fact]
    public async Task Repl_MigrateForeignKeysValidateOnly_ShowsValidationSummary()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempFilePath(".db");
        string specPath = NewTempFilePath(".json");

        try
        {
            await WriteForeignKeyMigrationSpecAsync(
                specPath,
                new ForeignKeyMigrationRequest
                {
                    ValidateOnly = true,
                    Constraints =
                    [
                        new ForeignKeyMigrationConstraintSpec
                        {
                            TableName = "orders",
                            ColumnName = "customer_id",
                            ReferencedTableName = "customers",
                            ReferencedColumnName = "id",
                        },
                    ],
                },
                ct);

            string input = string.Join(Environment.NewLine, new[]
            {
                "CREATE TABLE customers (id INTEGER PRIMARY KEY);",
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER);",
                "INSERT INTO customers VALUES (1);",
                "INSERT INTO orders VALUES (10, 1);",
                $".migrate-fks \"{specPath}\" --validate-only",
                ".quit",
                "",
            });

            string output = await RunReplAsync(dbPath, input, ct);
            Assert.Contains("Foreign key migration validation succeeded.", output, StringComparison.OrdinalIgnoreCase);

            await using var db = await Database.OpenAsync(dbPath, ct);
            await using var result = await db.ExecuteAsync("SELECT COUNT(*) FROM sys.foreign_keys WHERE table_name = 'orders';", ct);
            var rows = await result.ToListAsync(ct);
            Assert.Equal(0L, Assert.Single(rows)[0].AsInteger);
        }
        finally
        {
            DeleteIfExists(specPath);
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task Repl_MigrateForeignKeysApply_PersistsMetadata()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempFilePath(".db");
        string specPath = NewTempFilePath(".json");

        try
        {
            await WriteForeignKeyMigrationSpecAsync(
                specPath,
                new ForeignKeyMigrationRequest
                {
                    Constraints =
                    [
                        new ForeignKeyMigrationConstraintSpec
                        {
                            TableName = "orders",
                            ColumnName = "customer_id",
                            ReferencedTableName = "customers",
                            ReferencedColumnName = "id",
                            OnDelete = ForeignKeyOnDeleteAction.Cascade,
                        },
                    ],
                },
                ct);

            string input = string.Join(Environment.NewLine, new[]
            {
                "CREATE TABLE customers (id INTEGER PRIMARY KEY);",
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER);",
                "INSERT INTO customers VALUES (1);",
                "INSERT INTO orders VALUES (10, 1);",
                $".migrate-fks \"{specPath}\"",
                ".quit",
                "",
            });

            string output = await RunReplAsync(dbPath, input, ct);
            Assert.Contains("Foreign key migration completed.", output, StringComparison.OrdinalIgnoreCase);

            await using var db = await Database.OpenAsync(dbPath, ct);
            await using var result = await db.ExecuteAsync("SELECT COUNT(*) FROM sys.foreign_keys WHERE table_name = 'orders';", ct);
            var rows = await result.ToListAsync(ct);
            Assert.Equal(1L, Assert.Single(rows)[0].AsInteger);
        }
        finally
        {
            DeleteIfExists(specPath);
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
        await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
        {
            DataSource = dbPath,
        });
        Database? db = client is IEngineBackedClient engineBacked
            ? await engineBacked.TryGetDatabaseAsync(ct)
            : null;

        var commands = BuildCommands();
        var output = new StringWriter();
        using var repl = new Repl(client, db, dbPath, output, commands);

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
        commands.Add(new BackupCommand());
        commands.Add(new RestoreCommand());
        commands.Add(new MigrateForeignKeysCommand());
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

    private static async Task WriteForeignKeyMigrationSpecAsync(string path, ForeignKeyMigrationRequest request, CancellationToken ct)
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = true,
        };
        await File.WriteAllTextAsync(path, JsonSerializer.Serialize(request, options), ct);
    }
}
