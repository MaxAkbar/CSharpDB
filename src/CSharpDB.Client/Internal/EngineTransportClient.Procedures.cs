using System.Diagnostics;
using System.Globalization;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.Observability;
using CSharpDB.Sql;

namespace CSharpDB.Client.Internal;

internal sealed partial class EngineTransportClient
{
    public async Task<IReadOnlyList<ProcedureDefinition>> GetProceduresAsync(bool includeDisabled = true, CancellationToken ct = default)
    {
        using ClientLockLease clientLock = await AcquireClientLockAsync(ct);
        await EnsureCatalogsInitializedAsync(ct);
        var db = await GetDatabaseAsync(ct);
        string sql = includeDisabled
            ? $"SELECT name, body_sql, params_json, description, is_enabled, created_utc, updated_utc FROM {ProcedureTableName} ORDER BY name;"
            : $"SELECT name, body_sql, params_json, description, is_enabled, created_utc, updated_utc FROM {ProcedureTableName} WHERE is_enabled = 1 ORDER BY name;";

        var result = await ExecuteQueryAsync(db, sql, ct);
        return (result.Rows ?? [])
            .Select(ReadProcedureDefinition)
            .ToArray();
    }

    public async Task<ProcedureDefinition?> GetProcedureAsync(string name, CancellationToken ct = default)
    {
        using ClientLockLease clientLock = await AcquireClientLockAsync(ct);
        await EnsureCatalogsInitializedAsync(ct);
        ValidateIdentifier(name, "procedure name");
        return await GetProcedureInternalAsync(await GetDatabaseAsync(ct), name, ct);
    }

    public async Task CreateProcedureAsync(ProcedureDefinition definition, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(definition);

        using ClientLockLease clientLock = await AcquireClientLockAsync(ct);
        await EnsureCatalogsInitializedAsync(ct);
        var db = await GetDatabaseAsync(ct);
        var normalized = NormalizeProcedureDefinition(definition, DateTime.UtcNow);
        if (await GetProcedureInternalAsync(db, normalized.Name, ct) is not null)
            throw new ArgumentException($"Procedure '{normalized.Name}' already exists.");

        await ExecuteStatementAsync(
            db,
            $"""
            INSERT INTO {ProcedureTableName}
                (name, body_sql, params_json, description, is_enabled, created_utc, updated_utc)
            VALUES
                ({FormatSqlLiteral(normalized.Name)},
                 {FormatSqlLiteral(normalized.BodySql)},
                 {FormatSqlLiteral(SerializeProcedureParameters(normalized.Parameters))},
                 {FormatSqlLiteral(normalized.Description)},
                 {(normalized.IsEnabled ? "1" : "0")},
                 {FormatSqlLiteral(normalized.CreatedUtc.ToString("O", CultureInfo.InvariantCulture))},
                 {FormatSqlLiteral(normalized.UpdatedUtc.ToString("O", CultureInfo.InvariantCulture))});
            """,
            ct);
    }

    public async Task UpdateProcedureAsync(string existingName, ProcedureDefinition definition, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(definition);

        using ClientLockLease clientLock = await AcquireClientLockAsync(ct);
        await EnsureCatalogsInitializedAsync(ct);
        var db = await GetDatabaseAsync(ct);
        ValidateIdentifier(existingName, "existing procedure name");
        var existing = await GetProcedureInternalAsync(db, existingName, ct);
        if (existing is null)
            throw new ArgumentException($"Procedure '{existingName}' not found.");

        var normalized = NormalizeProcedureDefinition(definition, existing.CreatedUtc);
        int affected = await ExecuteNonQueryAsync(
            db,
            $"""
            UPDATE {ProcedureTableName}
            SET name = {FormatSqlLiteral(normalized.Name)},
                body_sql = {FormatSqlLiteral(normalized.BodySql)},
                params_json = {FormatSqlLiteral(SerializeProcedureParameters(normalized.Parameters))},
                description = {FormatSqlLiteral(normalized.Description)},
                is_enabled = {(normalized.IsEnabled ? "1" : "0")},
                created_utc = {FormatSqlLiteral(normalized.CreatedUtc.ToString("O", CultureInfo.InvariantCulture))},
                updated_utc = {FormatSqlLiteral(normalized.UpdatedUtc.ToString("O", CultureInfo.InvariantCulture))}
            WHERE name = {FormatSqlLiteral(existingName)};
            """,
            ct);

        if (affected == 0)
            throw new ArgumentException($"Procedure '{existingName}' not found.");
    }

    public async Task DeleteProcedureAsync(string name, CancellationToken ct = default)
    {
        using ClientLockLease clientLock = await AcquireClientLockAsync(ct);
        await EnsureCatalogsInitializedAsync(ct);
        ValidateIdentifier(name, "procedure name");
        int affected = await ExecuteNonQueryAsync(
            await GetDatabaseAsync(ct),
            $"DELETE FROM {ProcedureTableName} WHERE name = {FormatSqlLiteral(name)};",
            ct);

        if (affected == 0)
            throw new ArgumentException($"Procedure '{name}' not found.");
    }

    public async Task<ProcedureExecutionResult> ExecuteProcedureAsync(string name, IReadOnlyDictionary<string, object?> args, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(args);

        using IDisposable? transportScope = EnterDirectTransportScope();
        CompositeQueryOperation? procedureOperation =
            StartCompositeQueryOperation(CSharpDbOperationClass.Procedure);
        using IDisposable? procedureScope = procedureOperation?.EnterScope();
        ClientLockLease clientLock = default;
        bool lockTaken = false;
        try
        {
            clientLock = await AcquireClientLockAsync(ct);
            lockTaken = true;
            procedureOperation?.MarkDequeued();
            using IDisposable? queueDurationScope = procedureOperation?.EnterQueueDurationScope();

            await EnsureCatalogsInitializedAsync(ct);
            var db = await GetDatabaseAsync(ct);
            ValidateIdentifier(name, "procedure name");

            ProcedureDefinition? procedure;
            using (CSharpDbOperationScope.SuppressDiagnostics())
                procedure = await GetProcedureInternalAsync(db, name, ct);
            if (procedure is null)
            {
                procedureOperation?.Fail(CSharpDB.Primitives.ErrorCode.TableNotFound);
                throw new ArgumentException($"Procedure '{name}' not found.");
            }

            if (!procedure.IsEnabled)
            {
                procedureOperation?.Fail(CSharpDB.Primitives.ErrorCode.ConstraintViolation);
                return new ProcedureExecutionResult
                {
                    ProcedureName = name,
                    Succeeded = false,
                    Error = $"Procedure '{name}' is disabled.",
                    Elapsed = TimeSpan.Zero,
                };
            }

            var stopwatch = Stopwatch.StartNew();
            Dictionary<string, object?> boundArgs;
            try
            {
                boundArgs = BindProcedureArguments(procedure, args);
            }
            catch (ArgumentException ex)
            {
                stopwatch.Stop();
                procedureOperation?.Fail(ex);
                return new ProcedureExecutionResult
                {
                    ProcedureName = name,
                    Succeeded = false,
                    Error = ex.Message,
                    Elapsed = stopwatch.Elapsed,
                };
            }

            IReadOnlyList<string> statements;
            try
            {
                statements = SqlScriptSplitter.SplitExecutableStatements(procedure.BodySql);
            }
            catch (CSharpDB.Primitives.CSharpDbException ex)
            {
                stopwatch.Stop();
                procedureOperation?.Fail(ex.Code);
                return new ProcedureExecutionResult
                {
                    ProcedureName = name,
                    Succeeded = false,
                    Error = ex.Message,
                    Elapsed = stopwatch.Elapsed,
                };
            }

            var results = new List<ProcedureStatementExecutionResult>(statements.Count);
            long totalRowsProduced = 0;
            long totalRowsAffected = 0;
            bool transactionStarted = false;
            try
            {
                using (IDisposable? suppression = _observabilityEnabled
                           ? CSharpDbOperationScope.SuppressDiagnostics()
                           : null)
                {
                    await db.BeginTransactionAsync(ct);
                }
                transactionStarted = true;
                for (int i = 0; i < statements.Count; i++)
                {
                    ProcedureStatementExecutionResult statementResult =
                        await ExecuteSingleStatementWithArgumentsAsync(
                            db,
                            i,
                            statements[i],
                            boundArgs,
                            ct);
                    results.Add(statementResult);
                    if (statementResult.IsQuery)
                    {
                        totalRowsProduced = AddDiagnosticCount(
                            totalRowsProduced,
                            statementResult.Rows?.Count ?? 0);
                    }
                    else
                    {
                        totalRowsAffected = AddDiagnosticCount(
                            totalRowsAffected,
                            statementResult.RowsAffected);
                    }
                }

                using (IDisposable? suppression = _observabilityEnabled
                           ? CSharpDbOperationScope.SuppressDiagnostics()
                           : null)
                {
                    await db.CommitAsync(ct);
                }
                transactionStarted = false;
            }
            catch (OperationCanceledException ex)
            {
                procedureOperation?.Fail(
                    ex,
                    totalRowsProduced,
                    totalRowsAffected);
                if (transactionStarted)
                {
                    using IDisposable? suppression = _observabilityEnabled
                        ? CSharpDbOperationScope.SuppressDiagnostics()
                        : null;
                    await db.RollbackAsync(CancellationToken.None);
                }
                throw;
            }
            catch (Exception ex)
            {
                procedureOperation?.Fail(
                    ex,
                    totalRowsProduced,
                    totalRowsAffected);
                if (transactionStarted)
                {
                    using IDisposable? suppression = _observabilityEnabled
                        ? CSharpDbOperationScope.SuppressDiagnostics()
                        : null;
                    await db.RollbackAsync(CancellationToken.None);
                }
                stopwatch.Stop();
                return new ProcedureExecutionResult
                {
                    ProcedureName = name,
                    Succeeded = false,
                    Statements = results,
                    Error = ex.Message,
                    FailedStatementIndex = results.Count,
                    Elapsed = stopwatch.Elapsed,
                };
            }

            stopwatch.Stop();
            procedureOperation?.Succeed(totalRowsProduced, totalRowsAffected);
            return new ProcedureExecutionResult
            {
                ProcedureName = name,
                Succeeded = true,
                Statements = results,
                Elapsed = stopwatch.Elapsed,
            };
        }
        catch (Exception exception)
        {
            if (!lockTaken)
                procedureOperation?.MarkDequeued();
            procedureOperation?.Fail(exception);
            throw;
        }
        finally
        {
            if (lockTaken)
                clientLock.Dispose();
        }
    }

    private async Task<ProcedureDefinition?> GetProcedureInternalAsync(Database db, string name, CancellationToken ct)
    {
        var result = await ExecuteQueryAsync(
            db,
            $"""
            SELECT name, body_sql, params_json, description, is_enabled, created_utc, updated_utc
            FROM {ProcedureTableName}
            WHERE name = {FormatSqlLiteral(name)};
            """,
            ct);

        return result.Rows is { Count: > 0 } ? ReadProcedureDefinition(result.Rows[0]) : null;
    }

    private static ProcedureDefinition ReadProcedureDefinition(object?[] row)
    {
        return new ProcedureDefinition
        {
            Name = Convert.ToString(row[0], CultureInfo.InvariantCulture) ?? string.Empty,
            BodySql = Convert.ToString(row[1], CultureInfo.InvariantCulture) ?? string.Empty,
            Parameters = DeserializeProcedureParameters(Convert.ToString(row[2], CultureInfo.InvariantCulture) ?? string.Empty),
            Description = row[3] is null ? null : Convert.ToString(row[3], CultureInfo.InvariantCulture),
            IsEnabled = row[4] is not null && Convert.ToInt64(row[4], CultureInfo.InvariantCulture) != 0,
            CreatedUtc = ParseStoredUtc(Convert.ToString(row[5], CultureInfo.InvariantCulture) ?? string.Empty, DateTime.UtcNow),
            UpdatedUtc = ParseStoredUtc(Convert.ToString(row[6], CultureInfo.InvariantCulture) ?? string.Empty, DateTime.UtcNow),
        };
    }

    private ProcedureDefinition NormalizeProcedureDefinition(ProcedureDefinition definition, DateTime defaultCreatedUtc)
    {
        ValidateIdentifier(definition.Name, "procedure name");

        string normalizedBody = NormalizeSqlFragment(definition.BodySql, "procedure body");
        var normalizedParameters = NormalizeProcedureParameters(definition.Parameters);
        ValidateProcedureBodyReferences(normalizedBody, normalizedParameters);

        return new ProcedureDefinition
        {
            Name = definition.Name.Trim(),
            BodySql = normalizedBody,
            Parameters = normalizedParameters,
            Description = string.IsNullOrWhiteSpace(definition.Description) ? null : definition.Description.Trim(),
            IsEnabled = definition.IsEnabled,
            CreatedUtc = defaultCreatedUtc,
            UpdatedUtc = DateTime.UtcNow,
        };
    }
}
