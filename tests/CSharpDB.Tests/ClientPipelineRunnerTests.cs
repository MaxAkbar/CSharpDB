using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Client.Pipelines;
using CSharpDB.Pipelines.Models;

namespace CSharpDB.Tests;

public sealed class ClientPipelineRunnerTests
{
    [Fact]
    public async Task RunPackageAsync_CopiesRows_FromTableToTable()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_pipeline_test_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
            {
                DataSource = dbPath,
            });

            await client.ExecuteSqlAsync("""
                CREATE TABLE customers_src (id INTEGER PRIMARY KEY, name TEXT, status TEXT);
                CREATE TABLE customers_dest (id INTEGER PRIMARY KEY, full_name TEXT, status TEXT);
                INSERT INTO customers_src VALUES (1, 'Alice', 'active');
                INSERT INTO customers_src VALUES (2, 'Bob', 'inactive');
                """, ct);

            var runner = new CSharpDbPipelineRunner(client);
            PipelineRunResult result = await runner.RunPackageAsync(new PipelinePackageDefinition
            {
                Name = "copy-customers",
                Version = "1.0.0",
                Source = new PipelineSourceDefinition
                {
                    Kind = PipelineSourceKind.CSharpDbTable,
                    TableName = "customers_src",
                },
                Transforms =
                [
                    new PipelineTransformDefinition
                    {
                        Kind = PipelineTransformKind.Rename,
                        RenameMappings =
                        [
                            new PipelineRenameMapping
                            {
                                Source = "name",
                                Target = "full_name",
                            },
                        ],
                    },
                ],
                Destination = new PipelineDestinationDefinition
                {
                    Kind = PipelineDestinationKind.CSharpDbTable,
                    TableName = "customers_dest",
                },
                Options = new PipelineExecutionOptions
                {
                    BatchSize = 1,
                    CheckpointInterval = 1,
                    ErrorMode = PipelineErrorMode.SkipBadRows,
                    MaxRejects = 10,
                },
            }, ct: ct);

            Assert.Equal(PipelineRunStatus.Succeeded, result.Status);
            Assert.Equal(2, result.Metrics.RowsRead);
            Assert.Equal(2, result.Metrics.RowsWritten);

            var query = await client.ExecuteSqlAsync("SELECT id, full_name, status FROM customers_dest ORDER BY id;", ct);
            Assert.True(query.IsQuery);
            Assert.NotNull(query.Rows);
            Assert.Equal(2, query.Rows.Count);
            Assert.Equal("Alice", query.Rows[0][1]);
            Assert.Equal("inactive", query.Rows[1][2]);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task RunPackageAsync_AutoCastsCsvTextValues_ToDestinationSchema()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_pipeline_test_{Guid.NewGuid():N}.db");
        string csvPath = Path.Combine(Path.GetTempPath(), $"csharpdb_pipeline_test_{Guid.NewGuid():N}.csv");

        try
        {
            await File.WriteAllTextAsync(csvPath, "id,name,description\r\n6,Tablets,Touchscreen tablets and drawing tablets\r\n", ct);

            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
            {
                DataSource = dbPath,
            });

            await client.ExecuteSqlAsync("""
                CREATE TABLE categories (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    description TEXT
                );
                """, ct);

            var runner = new CSharpDbPipelineRunner(client);
            PipelineRunResult result = await runner.RunPackageAsync(new PipelinePackageDefinition
            {
                Name = "csv-categories",
                Version = "1.0.0",
                Source = new PipelineSourceDefinition
                {
                    Kind = PipelineSourceKind.CsvFile,
                    Path = csvPath,
                    HasHeaderRow = true,
                },
                Destination = new PipelineDestinationDefinition
                {
                    Kind = PipelineDestinationKind.CSharpDbTable,
                    TableName = "categories",
                },
                Options = new PipelineExecutionOptions
                {
                    BatchSize = 10,
                    CheckpointInterval = 1,
                    ErrorMode = PipelineErrorMode.FailFast,
                    MaxRejects = 0,
                },
            }, ct: ct);

            Assert.Equal(PipelineRunStatus.Succeeded, result.Status);

            var query = await client.ExecuteSqlAsync("SELECT id, name, description FROM categories;", ct);
            Assert.True(query.IsQuery);
            Assert.NotNull(query.Rows);
            Assert.Single(query.Rows);
            Assert.Equal(6L, Convert.ToInt64(query.Rows[0][0]));
            Assert.Equal("Tablets", query.Rows[0][1]);
            Assert.Equal("Touchscreen tablets and drawing tablets", query.Rows[0][2]);
        }
        finally
        {
            DeleteIfExists(csvPath);
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task RunPackageAsync_WritesSqlQueryResults_ToJsonFile()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_pipeline_test_{Guid.NewGuid():N}.db");
        string outputPath = Path.Combine(Path.GetTempPath(), $"csharpdb_pipeline_out_{Guid.NewGuid():N}.json");

        try
        {
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
            {
                DataSource = dbPath,
            });

            await client.ExecuteSqlAsync("""
                CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, status TEXT);
                INSERT INTO customers VALUES (1, 'Alice', 'active');
                INSERT INTO customers VALUES (2, 'Bob', 'inactive');
                """, ct);

            var runner = new CSharpDbPipelineRunner(client);
            PipelineRunResult result = await runner.RunPackageAsync(new PipelinePackageDefinition
            {
                Name = "export-active-customers",
                Version = "1.0.0",
                Source = new PipelineSourceDefinition
                {
                    Kind = PipelineSourceKind.SqlQuery,
                    QueryText = "SELECT id, name FROM customers WHERE status = 'active' ORDER BY id;",
                },
                Destination = new PipelineDestinationDefinition
                {
                    Kind = PipelineDestinationKind.JsonFile,
                    Path = outputPath,
                },
                Options = new PipelineExecutionOptions
                {
                    BatchSize = 50,
                    CheckpointInterval = 1,
                    ErrorMode = PipelineErrorMode.SkipBadRows,
                    MaxRejects = 10,
                },
            }, ct: ct);

            Assert.Equal(PipelineRunStatus.Succeeded, result.Status);
            Assert.True(File.Exists(outputPath));

            using var document = JsonDocument.Parse(await File.ReadAllTextAsync(outputPath, ct));
            Assert.Equal(JsonValueKind.Array, document.RootElement.ValueKind);
            Assert.Single(document.RootElement.EnumerateArray());
            Assert.Equal("Alice", document.RootElement[0].GetProperty("name").GetString());
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
            DeleteIfExists(outputPath);
        }
    }

    [Fact]
    public async Task RunPackageAsync_PersistsRunMetadataAndCheckpoint()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_pipeline_test_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
            {
                DataSource = dbPath,
            });

            await client.ExecuteSqlAsync("""
                CREATE TABLE customers_src (id INTEGER PRIMARY KEY, name TEXT);
                CREATE TABLE customers_dest (id INTEGER PRIMARY KEY, name TEXT);
                INSERT INTO customers_src VALUES (1, 'Alice');
                INSERT INTO customers_src VALUES (2, 'Bob');
                """, ct);

            var runner = new CSharpDbPipelineRunner(client);
            PipelineRunResult result = await runner.RunPackageAsync(new PipelinePackageDefinition
            {
                Name = "persisted-run",
                Version = "1.0.0",
                Source = new PipelineSourceDefinition
                {
                    Kind = PipelineSourceKind.CSharpDbTable,
                    TableName = "customers_src",
                },
                Destination = new PipelineDestinationDefinition
                {
                    Kind = PipelineDestinationKind.CSharpDbTable,
                    TableName = "customers_dest",
                },
                Options = new PipelineExecutionOptions
                {
                    BatchSize = 1,
                    CheckpointInterval = 1,
                    ErrorMode = PipelineErrorMode.SkipBadRows,
                    MaxRejects = 10,
                },
            }, ct: ct);

            var runRow = await client.ExecuteSqlAsync($"SELECT pipeline_name, status, rows_read, rows_written FROM _etl_runs WHERE run_id = '{result.RunId}';", ct);
            Assert.True(runRow.IsQuery);
            Assert.NotNull(runRow.Rows);
            var row = Assert.Single(runRow.Rows);
            Assert.Equal("persisted-run", row[0]);
            Assert.Equal("Succeeded", row[1]);
            Assert.Equal(2L, Convert.ToInt64(row[2]));
            Assert.Equal(2L, Convert.ToInt64(row[3]));

            var checkpointRow = await client.ExecuteSqlAsync($"SELECT batch_number, step_name FROM _etl_checkpoints WHERE run_id = '{result.RunId}';", ct);
            Assert.True(checkpointRow.IsQuery);
            Assert.NotNull(checkpointRow.Rows);
            var checkpoint = Assert.Single(checkpointRow.Rows);
            Assert.Equal(2L, Convert.ToInt64(checkpoint[0]));
            Assert.Equal("destination-write", checkpoint[1]);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task PipelineCatalogClient_CanListGetAndResumeRuns()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_pipeline_test_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
            {
                DataSource = dbPath,
            });

            await client.ExecuteSqlAsync("""
                CREATE TABLE customers_src (id INTEGER PRIMARY KEY, name TEXT);
                CREATE TABLE customers_dest (id INTEGER PRIMARY KEY, name TEXT);
                INSERT INTO customers_src VALUES (1, 'Alice');
                INSERT INTO customers_src VALUES (2, 'Bob');
                """, ct);

            var runner = new CSharpDbPipelineRunner(client);
            PipelineRunResult initial = await runner.RunPackageAsync(new PipelinePackageDefinition
            {
                Name = "resume-test",
                Version = "1.0.0",
                Source = new PipelineSourceDefinition
                {
                    Kind = PipelineSourceKind.CSharpDbTable,
                    TableName = "customers_src",
                },
                Destination = new PipelineDestinationDefinition
                {
                    Kind = PipelineDestinationKind.CSharpDbTable,
                    TableName = "customers_dest",
                },
                Options = new PipelineExecutionOptions
                {
                    BatchSize = 1,
                    CheckpointInterval = 1,
                    ErrorMode = PipelineErrorMode.SkipBadRows,
                    MaxRejects = 10,
                },
            }, ct: ct);

            var catalog = new CSharpDbPipelineCatalogClient(client);
            var runs = await catalog.ListRunsAsync(ct: ct);
            Assert.Contains(runs, run => run.RunId == initial.RunId);

            var loaded = await catalog.GetRunAsync(initial.RunId, ct);
            Assert.NotNull(loaded);
            Assert.Equal("resume-test", loaded!.PipelineName);

            var packageJsonResult = await client.ExecuteSqlAsync($"SELECT package_json FROM _etl_runs WHERE run_id = '{initial.RunId}';", ct);
            Assert.True(packageJsonResult.IsQuery);
            Assert.NotNull(packageJsonResult.Rows);
            Assert.False(string.IsNullOrWhiteSpace(packageJsonResult.Rows[0][0] as string));

            await client.ExecuteSqlAsync("DELETE FROM customers_dest WHERE id = 2;", ct);
            await client.ExecuteSqlAsync(
                $"UPDATE _etl_checkpoints SET batch_number = 1 WHERE run_id = '{initial.RunId}';",
                ct);

            PipelineRunResult resumed = await catalog.ResumeAsync(initial.RunId, ct);
            Assert.Equal(PipelineRunStatus.Succeeded, resumed.Status);

            var count = await client.ExecuteSqlAsync("SELECT COUNT(*) FROM customers_dest;", ct);
            Assert.True(count.IsQuery);
            Assert.NotNull(count.Rows);
            Assert.Equal(2L, Convert.ToInt64(count.Rows[0][0]));
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task PipelineCatalogClient_CanSaveListRunAndDeleteStoredPipelines()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_pipeline_test_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
            {
                DataSource = dbPath,
            });

            await client.ExecuteSqlAsync("""
                CREATE TABLE customers_src (id INTEGER PRIMARY KEY, name TEXT);
                CREATE TABLE customers_dest (id INTEGER PRIMARY KEY, name TEXT);
                INSERT INTO customers_src VALUES (1, 'Alice');
                """, ct);

            var catalog = new CSharpDbPipelineCatalogClient(client);
            var package = new PipelinePackageDefinition
            {
                Name = "stored-customers",
                Version = "1.0.0",
                Description = "Stored package",
                Source = new PipelineSourceDefinition
                {
                    Kind = PipelineSourceKind.CSharpDbTable,
                    TableName = "customers_src",
                },
                Transforms =
                [
                    new PipelineTransformDefinition
                    {
                        Kind = PipelineTransformKind.Select,
                        SelectColumns = ["id", "name", "status", "created_at"],
                    },
                ],
                Destination = new PipelineDestinationDefinition
                {
                    Kind = PipelineDestinationKind.CSharpDbTable,
                    TableName = "customers_dest",
                },
                Options = new PipelineExecutionOptions
                {
                    BatchSize = 10,
                    CheckpointInterval = 1,
                    ErrorMode = PipelineErrorMode.SkipBadRows,
                    MaxRejects = 10,
                },
            };

            var saved = await catalog.SavePipelineAsync(package, ct: ct);
            Assert.Equal("stored-customers", saved.Name);
            Assert.Equal(1, saved.Revision);

            var pipelines = await catalog.ListPipelinesAsync(ct: ct);
            Assert.Contains(pipelines, p => p.Name == "stored-customers");

            var loaded = await catalog.GetPipelineAsync("stored-customers", ct);
            Assert.NotNull(loaded);
            Assert.Equal("Stored package", loaded!.Description);
            Assert.Single(loaded.Transforms);
            Assert.Equal(PipelineTransformKind.Select, loaded.Transforms[0].Kind);
            Assert.Equal(["id", "name", "status", "created_at"], loaded.Transforms[0].SelectColumns);

            PipelineRunResult run = await catalog.RunStoredPipelineAsync("stored-customers", ct: ct);
            Assert.Equal(PipelineRunStatus.Succeeded, run.Status);

            var count = await client.ExecuteSqlAsync("SELECT COUNT(*) FROM customers_dest;", ct);
            Assert.True(count.IsQuery);
            Assert.NotNull(count.Rows);
            Assert.Equal(1L, Convert.ToInt64(count.Rows[0][0]));

            await catalog.DeletePipelineAsync("stored-customers", ct);
            Assert.Null(await catalog.GetPipelineAsync("stored-customers", ct));
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
