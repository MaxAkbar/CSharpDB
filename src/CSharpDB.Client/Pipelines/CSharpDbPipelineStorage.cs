using System.Globalization;
using CSharpDB.Client.Models;
using CSharpDB.Pipelines.Models;
using CSharpDB.Pipelines.Runtime;
using CSharpDB.Pipelines.Serialization;

namespace CSharpDB.Client.Pipelines;

internal sealed class CSharpDbPipelineCatalog
{
    private readonly ICSharpDbClient _client;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private bool _initialized;

    public CSharpDbPipelineCatalog(ICSharpDbClient client)
    {
        _client = client;
    }

    public async Task EnsureInitializedAsync(CancellationToken ct = default)
    {
        if (_initialized)
            return;

        await _initLock.WaitAsync(ct);
        try
        {
            if (_initialized)
                return;

            await ExecuteNonQueryAsync("""
                CREATE TABLE IF NOT EXISTS _etl_runs (
                    run_id TEXT PRIMARY KEY,
                    pipeline_name TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_utc TEXT NOT NULL,
                    completed_utc TEXT,
                    package_json TEXT,
                    rows_read INTEGER NOT NULL,
                    rows_written INTEGER NOT NULL,
                    rows_rejected INTEGER NOT NULL,
                    batches_completed INTEGER NOT NULL,
                    error_summary TEXT
                );
                """, ct);

            await ExecuteNonQueryAsync("""
                CREATE TABLE IF NOT EXISTS _etl_checkpoints (
                    run_id TEXT PRIMARY KEY,
                    step_name TEXT,
                    batch_number INTEGER NOT NULL,
                    offset_token TEXT,
                    updated_utc TEXT NOT NULL
                );
                """, ct);

            await ExecuteNonQueryAsync("""
                CREATE TABLE IF NOT EXISTS _etl_rejects (
                    run_id TEXT NOT NULL,
                    row_number INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    payload_json TEXT
                );
                """, ct);

            await ExecuteNonQueryAsync("""
                CREATE TABLE IF NOT EXISTS _etl_pipelines (
                    name TEXT PRIMARY KEY,
                    package_version TEXT NOT NULL,
                    description TEXT,
                    current_revision INTEGER NOT NULL,
                    updated_utc TEXT NOT NULL
                );
                """, ct);

            await ExecuteNonQueryAsync("""
                CREATE TABLE IF NOT EXISTS _etl_pipeline_versions (
                    name TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    package_version TEXT NOT NULL,
                    description TEXT,
                    package_json TEXT NOT NULL,
                    created_utc TEXT NOT NULL
                );
                """, ct);

            try
            {
                await ExecuteNonQueryAsync("ALTER TABLE _etl_runs ADD COLUMN package_json TEXT;", ct);
            }
            catch
            {
                // Ignore if the column already exists.
            }

            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }

    public async Task ExecuteNonQueryAsync(string sql, CancellationToken ct = default)
    {
        SqlExecutionResult result = await _client.ExecuteSqlAsync(sql, ct);
        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new InvalidOperationException(result.Error);
    }

    public async Task<SqlExecutionResult> ExecuteQueryAsync(string sql, CancellationToken ct = default)
    {
        SqlExecutionResult result = await _client.ExecuteSqlAsync(sql, ct);
        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new InvalidOperationException(result.Error);
        return result;
    }

    public static string Literal(string? value)
    {
        if (value is null)
            return "NULL";

        return $"'{value.Replace("'", "''")}'";
    }

    public static string Literal(DateTimeOffset value)
        => Literal(value.UtcDateTime.ToString("O", CultureInfo.InvariantCulture));

    public static string Literal(long value)
        => value.ToString(CultureInfo.InvariantCulture);
}

internal sealed class CSharpDbPipelineCheckpointStore : IPipelineCheckpointStore
{
    private readonly CSharpDbPipelineCatalog _catalog;

    public CSharpDbPipelineCheckpointStore(ICSharpDbClient client)
    {
        _catalog = new CSharpDbPipelineCatalog(client);
    }

    public async Task<PipelineCheckpointState?> LoadAsync(string runId, CancellationToken ct = default)
    {
        await _catalog.EnsureInitializedAsync(ct);

        SqlExecutionResult result = await _catalog.ExecuteQueryAsync(
            $"SELECT step_name, batch_number, offset_token, updated_utc FROM _etl_checkpoints WHERE run_id = {CSharpDbPipelineCatalog.Literal(runId)};",
            ct);

        if (!result.IsQuery || result.Rows is null || result.Rows.Count == 0)
            return null;

        object?[] row = result.Rows[0];
        return new PipelineCheckpointState
        {
            StepName = row[0] as string,
            BatchNumber = Convert.ToInt64(row[1], CultureInfo.InvariantCulture),
            OffsetToken = row[2] as string,
            UpdatedUtc = DateTimeOffset.Parse(Convert.ToString(row[3], CultureInfo.InvariantCulture)!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
        };
    }

    public async Task SaveAsync(string runId, PipelineCheckpointState checkpoint, CancellationToken ct = default)
    {
        await _catalog.EnsureInitializedAsync(ct);

        SqlExecutionResult exists = await _catalog.ExecuteQueryAsync(
            $"SELECT COUNT(*) FROM _etl_checkpoints WHERE run_id = {CSharpDbPipelineCatalog.Literal(runId)};",
            ct);
        bool hasExisting = exists.Rows is not null
            && exists.Rows.Count > 0
            && Convert.ToInt64(exists.Rows[0][0], CultureInfo.InvariantCulture) > 0;

        string sql = hasExisting
            ? $"""
                UPDATE _etl_checkpoints
                SET step_name = {CSharpDbPipelineCatalog.Literal(checkpoint.StepName)},
                    batch_number = {CSharpDbPipelineCatalog.Literal(checkpoint.BatchNumber)},
                    offset_token = {CSharpDbPipelineCatalog.Literal(checkpoint.OffsetToken)},
                    updated_utc = {CSharpDbPipelineCatalog.Literal(checkpoint.UpdatedUtc)}
                WHERE run_id = {CSharpDbPipelineCatalog.Literal(runId)};
                """
            : $"""
                INSERT INTO _etl_checkpoints (run_id, step_name, batch_number, offset_token, updated_utc)
                VALUES (
                    {CSharpDbPipelineCatalog.Literal(runId)},
                    {CSharpDbPipelineCatalog.Literal(checkpoint.StepName)},
                    {CSharpDbPipelineCatalog.Literal(checkpoint.BatchNumber)},
                    {CSharpDbPipelineCatalog.Literal(checkpoint.OffsetToken)},
                    {CSharpDbPipelineCatalog.Literal(checkpoint.UpdatedUtc)}
                );
                """;

        await _catalog.ExecuteNonQueryAsync(sql, ct);
    }
}

internal sealed class CSharpDbPipelineRunLogger : IPipelineRunLogger
{
    private readonly CSharpDbPipelineCatalog _catalog;

    public CSharpDbPipelineRunLogger(ICSharpDbClient client)
    {
        _catalog = new CSharpDbPipelineCatalog(client);
    }

    public async Task RunStartedAsync(PipelineExecutionContext context, CancellationToken ct = default)
    {
        await _catalog.EnsureInitializedAsync(ct);

        SqlExecutionResult exists = await _catalog.ExecuteQueryAsync(
            $"SELECT COUNT(*) FROM _etl_runs WHERE run_id = {CSharpDbPipelineCatalog.Literal(context.RunId)};",
            ct);
        bool hasExisting = exists.Rows is not null
            && exists.Rows.Count > 0
            && Convert.ToInt64(exists.Rows[0][0], CultureInfo.InvariantCulture) > 0;

        string packageJson = PipelinePackageSerializer.Serialize(context.Package);
        string sql = hasExisting
            ? $"""
                UPDATE _etl_runs
                SET pipeline_name = {CSharpDbPipelineCatalog.Literal(context.Package.Name)},
                    mode = {CSharpDbPipelineCatalog.Literal(context.Mode.ToString())},
                    status = {CSharpDbPipelineCatalog.Literal(PipelineRunStatus.Pending.ToString())},
                    started_utc = {CSharpDbPipelineCatalog.Literal(DateTimeOffset.UtcNow)},
                    completed_utc = NULL,
                    package_json = {CSharpDbPipelineCatalog.Literal(packageJson)},
                    rows_read = 0,
                    rows_written = 0,
                    rows_rejected = 0,
                    batches_completed = 0,
                    error_summary = NULL
                WHERE run_id = {CSharpDbPipelineCatalog.Literal(context.RunId)};
                """
            : $"""
                INSERT INTO _etl_runs (
                    run_id,
                    pipeline_name,
                    mode,
                    status,
                    started_utc,
                    completed_utc,
                    package_json,
                    rows_read,
                    rows_written,
                    rows_rejected,
                    batches_completed,
                    error_summary
                )
                VALUES (
                    {CSharpDbPipelineCatalog.Literal(context.RunId)},
                    {CSharpDbPipelineCatalog.Literal(context.Package.Name)},
                    {CSharpDbPipelineCatalog.Literal(context.Mode.ToString())},
                    {CSharpDbPipelineCatalog.Literal(PipelineRunStatus.Pending.ToString())},
                    {CSharpDbPipelineCatalog.Literal(DateTimeOffset.UtcNow)},
                    NULL,
                    {CSharpDbPipelineCatalog.Literal(packageJson)},
                    0,
                    0,
                    0,
                    0,
                    NULL
                );
                """;

        await _catalog.ExecuteNonQueryAsync(sql, ct);
    }

    public async Task StatusChangedAsync(string runId, PipelineRunStatus status, PipelineRunMetrics metrics, CancellationToken ct = default)
    {
        await _catalog.EnsureInitializedAsync(ct);

        string sql = $"""
            UPDATE _etl_runs
            SET status = {CSharpDbPipelineCatalog.Literal(status.ToString())},
                rows_read = {CSharpDbPipelineCatalog.Literal(metrics.RowsRead)},
                rows_written = {CSharpDbPipelineCatalog.Literal(metrics.RowsWritten)},
                rows_rejected = {CSharpDbPipelineCatalog.Literal(metrics.RowsRejected)},
                batches_completed = {CSharpDbPipelineCatalog.Literal(metrics.BatchesCompleted)}
            WHERE run_id = {CSharpDbPipelineCatalog.Literal(runId)};
            """;

        await _catalog.ExecuteNonQueryAsync(sql, ct);
    }

    public async Task RejectsCapturedAsync(string runId, IReadOnlyList<PipelineRejectRecord> rejects, CancellationToken ct = default)
    {
        await _catalog.EnsureInitializedAsync(ct);

        foreach (var reject in rejects)
        {
            string sql = $"""
                INSERT INTO _etl_rejects (run_id, row_number, reason, payload_json)
                VALUES (
                    {CSharpDbPipelineCatalog.Literal(runId)},
                    {CSharpDbPipelineCatalog.Literal(reject.RowNumber)},
                    {CSharpDbPipelineCatalog.Literal(reject.Reason)},
                    {CSharpDbPipelineCatalog.Literal(reject.PayloadJson)}
                );
                """;

            await _catalog.ExecuteNonQueryAsync(sql, ct);
        }
    }

    public async Task RunCompletedAsync(PipelineRunResult result, CancellationToken ct = default)
    {
        await _catalog.EnsureInitializedAsync(ct);

        string sql = $"""
            UPDATE _etl_runs
            SET status = {CSharpDbPipelineCatalog.Literal(result.Status.ToString())},
                completed_utc = {CSharpDbPipelineCatalog.Literal(result.CompletedUtc ?? DateTimeOffset.UtcNow)},
                rows_read = {CSharpDbPipelineCatalog.Literal(result.Metrics.RowsRead)},
                rows_written = {CSharpDbPipelineCatalog.Literal(result.Metrics.RowsWritten)},
                rows_rejected = {CSharpDbPipelineCatalog.Literal(result.Metrics.RowsRejected)},
                batches_completed = {CSharpDbPipelineCatalog.Literal(result.Metrics.BatchesCompleted)},
                error_summary = {CSharpDbPipelineCatalog.Literal(result.ErrorSummary)}
            WHERE run_id = {CSharpDbPipelineCatalog.Literal(result.RunId)};
            """;

        await _catalog.ExecuteNonQueryAsync(sql, ct);
    }
}

public sealed class CSharpDbPipelineCatalogClient
{
    private readonly ICSharpDbClient _client;
    private readonly CSharpDbPipelineCatalog _catalog;

    public CSharpDbPipelineCatalogClient(ICSharpDbClient client)
    {
        _client = client;
        _catalog = new CSharpDbPipelineCatalog(client);
    }

    public async Task<IReadOnlyList<PipelineRunResult>> ListRunsAsync(int limit = 50, CancellationToken ct = default)
    {
        await _catalog.EnsureInitializedAsync(ct);

        SqlExecutionResult result = await _catalog.ExecuteQueryAsync(
            $"SELECT run_id, pipeline_name, mode, status, started_utc, completed_utc, rows_read, rows_written, rows_rejected, batches_completed, error_summary FROM _etl_runs ORDER BY started_utc DESC LIMIT {limit.ToString(CultureInfo.InvariantCulture)};",
            ct);

        return result.Rows?.Select(MapRun).ToArray() ?? [];
    }

    public async Task<PipelineRunResult?> GetRunAsync(string runId, CancellationToken ct = default)
    {
        await _catalog.EnsureInitializedAsync(ct);

        SqlExecutionResult result = await _catalog.ExecuteQueryAsync(
            $"SELECT run_id, pipeline_name, mode, status, started_utc, completed_utc, rows_read, rows_written, rows_rejected, batches_completed, error_summary FROM _etl_runs WHERE run_id = {CSharpDbPipelineCatalog.Literal(runId)};",
            ct);

        return result.Rows is { Count: > 0 } ? MapRun(result.Rows[0]) : null;
    }

    public async Task<PipelinePackageDefinition?> GetRunPackageAsync(string runId, CancellationToken ct = default)
    {
        await _catalog.EnsureInitializedAsync(ct);

        SqlExecutionResult result = await _catalog.ExecuteQueryAsync(
            $"SELECT package_json FROM _etl_runs WHERE run_id = {CSharpDbPipelineCatalog.Literal(runId)};",
            ct);

        return result.Rows is { Count: > 0 } && result.Rows[0][0] is string packageJson && !string.IsNullOrWhiteSpace(packageJson)
            ? PipelinePackageSerializer.Deserialize(packageJson)
            : null;
    }

    public async Task<IReadOnlyList<PipelineRejectRecord>> GetRejectsAsync(string runId, CancellationToken ct = default)
    {
        await _catalog.EnsureInitializedAsync(ct);

        SqlExecutionResult result = await _catalog.ExecuteQueryAsync(
            $"SELECT row_number, reason, payload_json FROM _etl_rejects WHERE run_id = {CSharpDbPipelineCatalog.Literal(runId)} ORDER BY row_number;",
            ct);

        return result.Rows?.Select(row => new PipelineRejectRecord
        {
            RowNumber = Convert.ToInt64(row[0], CultureInfo.InvariantCulture),
            Reason = Convert.ToString(row[1], CultureInfo.InvariantCulture) ?? string.Empty,
            PayloadJson = row[2] as string,
        }).ToArray() ?? [];
    }

    public async Task<PipelineRunResult> ResumeAsync(string runId, CancellationToken ct = default)
    {
        await _catalog.EnsureInitializedAsync(ct);

        SqlExecutionResult result = await _catalog.ExecuteQueryAsync(
            $"SELECT package_json FROM _etl_runs WHERE run_id = {CSharpDbPipelineCatalog.Literal(runId)};",
            ct);

        if (result.Rows is not { Count: > 0 } || result.Rows[0][0] is not string packageJson || string.IsNullOrWhiteSpace(packageJson))
            throw new InvalidOperationException($"Run '{runId}' does not have a stored package definition.");

        PipelinePackageDefinition package = PipelinePackageSerializer.Deserialize(packageJson);
        var runner = new CSharpDbPipelineRunner(_client);
        return await runner.RunAsync(new PipelineRunRequest
        {
            Package = package,
            Mode = PipelineExecutionMode.Resume,
            ExistingRunId = runId,
        }, ct);
    }

    public async Task<IReadOnlyList<PipelineDefinitionSummary>> ListPipelinesAsync(int limit = 100, CancellationToken ct = default)
    {
        await _catalog.EnsureInitializedAsync(ct);

        SqlExecutionResult result = await _catalog.ExecuteQueryAsync(
            $"SELECT name, package_version, description, current_revision, updated_utc FROM _etl_pipelines ORDER BY updated_utc DESC LIMIT {limit.ToString(CultureInfo.InvariantCulture)};",
            ct);

        return result.Rows?.Select(MapPipelineSummary).ToArray() ?? [];
    }

    public async Task<PipelinePackageDefinition?> GetPipelineAsync(string name, CancellationToken ct = default)
    {
        await _catalog.EnsureInitializedAsync(ct);

        SqlExecutionResult result = await _catalog.ExecuteQueryAsync(
            $"SELECT package_json FROM _etl_pipeline_versions WHERE name = {CSharpDbPipelineCatalog.Literal(name)} ORDER BY revision DESC LIMIT 1;",
            ct);

        return result.Rows is { Count: > 0 } && result.Rows[0][0] is string packageJson
            ? PipelinePackageSerializer.Deserialize(packageJson)
            : null;
    }

    public async Task<IReadOnlyList<PipelineRevisionSummary>> ListPipelineRevisionsAsync(string name, int limit = 25, CancellationToken ct = default)
    {
        await _catalog.EnsureInitializedAsync(ct);

        SqlExecutionResult result = await _catalog.ExecuteQueryAsync(
            $"""
            SELECT name, revision, package_version, description, created_utc
            FROM _etl_pipeline_versions
            WHERE name = {CSharpDbPipelineCatalog.Literal(name)}
            ORDER BY revision DESC
            LIMIT {limit.ToString(CultureInfo.InvariantCulture)};
            """,
            ct);

        return result.Rows?.Select(MapPipelineRevisionSummary).ToArray() ?? [];
    }

    public async Task<PipelinePackageDefinition?> GetPipelineRevisionAsync(string name, int revision, CancellationToken ct = default)
    {
        await _catalog.EnsureInitializedAsync(ct);

        SqlExecutionResult result = await _catalog.ExecuteQueryAsync(
            $"""
            SELECT package_json
            FROM _etl_pipeline_versions
            WHERE name = {CSharpDbPipelineCatalog.Literal(name)}
              AND revision = {revision.ToString(CultureInfo.InvariantCulture)};
            """,
            ct);

        return result.Rows is { Count: > 0 } && result.Rows[0][0] is string packageJson
            ? PipelinePackageSerializer.Deserialize(packageJson)
            : null;
    }

    public async Task<PipelineDefinitionSummary> SavePipelineAsync(PipelinePackageDefinition package, string? nameOverride = null, CancellationToken ct = default)
    {
        await _catalog.EnsureInitializedAsync(ct);

        string name = string.IsNullOrWhiteSpace(nameOverride) ? package.Name : nameOverride;
        if (string.IsNullOrWhiteSpace(name))
            throw new InvalidOperationException("Pipeline name is required.");

        SqlExecutionResult current = await _catalog.ExecuteQueryAsync(
            $"SELECT current_revision FROM _etl_pipelines WHERE name = {CSharpDbPipelineCatalog.Literal(name)};",
            ct);

        int nextRevision = current.Rows is { Count: > 0 }
            ? Convert.ToInt32(current.Rows[0][0], CultureInfo.InvariantCulture) + 1
            : 1;

        var normalized = new PipelinePackageDefinition
        {
            Name = name,
            Version = package.Version,
            Description = package.Description,
            Source = package.Source,
            Transforms = package.Transforms,
            Destination = package.Destination,
            Options = package.Options,
            Incremental = package.Incremental,
            Hooks = package.Hooks,
        };

        string packageJson = PipelinePackageSerializer.Serialize(normalized);
        DateTimeOffset now = DateTimeOffset.UtcNow;

        await _catalog.ExecuteNonQueryAsync($"""
            INSERT INTO _etl_pipeline_versions (name, revision, package_version, description, package_json, created_utc)
            VALUES (
                {CSharpDbPipelineCatalog.Literal(name)},
                {nextRevision.ToString(CultureInfo.InvariantCulture)},
                {CSharpDbPipelineCatalog.Literal(normalized.Version)},
                {CSharpDbPipelineCatalog.Literal(normalized.Description)},
                {CSharpDbPipelineCatalog.Literal(packageJson)},
                {CSharpDbPipelineCatalog.Literal(now)}
            );
            """, ct);

        if (current.Rows is { Count: > 0 })
        {
            await _catalog.ExecuteNonQueryAsync($"""
                UPDATE _etl_pipelines
                SET package_version = {CSharpDbPipelineCatalog.Literal(normalized.Version)},
                    description = {CSharpDbPipelineCatalog.Literal(normalized.Description)},
                    current_revision = {nextRevision.ToString(CultureInfo.InvariantCulture)},
                    updated_utc = {CSharpDbPipelineCatalog.Literal(now)}
                WHERE name = {CSharpDbPipelineCatalog.Literal(name)};
                """, ct);
        }
        else
        {
            await _catalog.ExecuteNonQueryAsync($"""
                INSERT INTO _etl_pipelines (name, package_version, description, current_revision, updated_utc)
                VALUES (
                    {CSharpDbPipelineCatalog.Literal(name)},
                    {CSharpDbPipelineCatalog.Literal(normalized.Version)},
                    {CSharpDbPipelineCatalog.Literal(normalized.Description)},
                    {nextRevision.ToString(CultureInfo.InvariantCulture)},
                    {CSharpDbPipelineCatalog.Literal(now)}
                );
                """, ct);
        }

        return new PipelineDefinitionSummary
        {
            Name = name,
            Version = normalized.Version,
            Description = normalized.Description,
            Revision = nextRevision,
            UpdatedUtc = now,
        };
    }

    public async Task DeletePipelineAsync(string name, CancellationToken ct = default)
    {
        await _catalog.EnsureInitializedAsync(ct);

        await _catalog.ExecuteNonQueryAsync(
            $"DELETE FROM _etl_pipeline_versions WHERE name = {CSharpDbPipelineCatalog.Literal(name)};",
            ct);
        await _catalog.ExecuteNonQueryAsync(
            $"DELETE FROM _etl_pipelines WHERE name = {CSharpDbPipelineCatalog.Literal(name)};",
            ct);
    }

    public async Task<PipelineRunResult> RunStoredPipelineAsync(string name, PipelineExecutionMode mode = PipelineExecutionMode.Run, CancellationToken ct = default)
    {
        PipelinePackageDefinition package = await GetPipelineAsync(name, ct)
            ?? throw new InvalidOperationException($"Pipeline '{name}' was not found.");

        var runner = new CSharpDbPipelineRunner(_client);
        return await runner.RunPackageAsync(package, mode, ct);
    }

    private static PipelineRunResult MapRun(object?[] row)
    {
        Enum.TryParse(Convert.ToString(row[2], CultureInfo.InvariantCulture), ignoreCase: true, out PipelineExecutionMode mode);
        Enum.TryParse(Convert.ToString(row[3], CultureInfo.InvariantCulture), ignoreCase: true, out PipelineRunStatus status);

        return new PipelineRunResult
        {
            RunId = Convert.ToString(row[0], CultureInfo.InvariantCulture) ?? string.Empty,
            PipelineName = Convert.ToString(row[1], CultureInfo.InvariantCulture) ?? string.Empty,
            Mode = mode,
            Status = status,
            StartedUtc = DateTimeOffset.Parse(Convert.ToString(row[4], CultureInfo.InvariantCulture)!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            CompletedUtc = row[5] is string completed && !string.IsNullOrWhiteSpace(completed)
                ? DateTimeOffset.Parse(completed, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)
                : null,
            Metrics = new PipelineRunMetrics
            {
                RowsRead = Convert.ToInt64(row[6], CultureInfo.InvariantCulture),
                RowsWritten = Convert.ToInt64(row[7], CultureInfo.InvariantCulture),
                RowsRejected = Convert.ToInt64(row[8], CultureInfo.InvariantCulture),
                BatchesCompleted = Convert.ToInt32(row[9], CultureInfo.InvariantCulture),
            },
            ErrorSummary = row[10] as string,
        };
    }

    private static PipelineDefinitionSummary MapPipelineSummary(object?[] row) => new()
    {
        Name = Convert.ToString(row[0], CultureInfo.InvariantCulture) ?? string.Empty,
        Version = Convert.ToString(row[1], CultureInfo.InvariantCulture) ?? string.Empty,
        Description = row[2] as string,
        Revision = Convert.ToInt32(row[3], CultureInfo.InvariantCulture),
        UpdatedUtc = DateTimeOffset.Parse(Convert.ToString(row[4], CultureInfo.InvariantCulture)!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
    };

    private static PipelineRevisionSummary MapPipelineRevisionSummary(object?[] row) => new()
    {
        Name = Convert.ToString(row[0], CultureInfo.InvariantCulture) ?? string.Empty,
        Revision = Convert.ToInt32(row[1], CultureInfo.InvariantCulture),
        Version = Convert.ToString(row[2], CultureInfo.InvariantCulture) ?? string.Empty,
        Description = row[3] as string,
        CreatedUtc = DateTimeOffset.Parse(Convert.ToString(row[4], CultureInfo.InvariantCulture)!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
    };
}
