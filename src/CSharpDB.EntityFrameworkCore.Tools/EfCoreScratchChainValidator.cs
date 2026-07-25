using System.Data.Common;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Data;
using CSharpDB.Migration;
using CSharpDB.Migration.Validation;
using CSharpDB.Sql;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace CSharpDB.EntityFrameworkCore.Tools;

internal sealed record EfCoreScratchMigrationInput
{
    internal required int Ordinal { get; init; }

    internal required string MigrationId { get; init; }

    internal required IReadOnlyList<MigrationOperation> UpOperations
    {
        get;
        init;
    }

    internal required IReadOnlyList<MigrationOperation> DownOperations
    {
        get;
        init;
    }

    internal required IModel TargetModel { get; init; }
}

internal sealed record EfCoreScratchChainValidationResult
{
    internal required EfCoreMigrationScratchAnalysisOutcome Outcome
    {
        get;
        init;
    }

    internal required string RuleId { get; init; }

    internal required EfCoreMigrationScratchChainProof Proof { get; init; }
}

internal static class EfCoreScratchChainValidator
{
    internal const int MaxMigrations = 128;
    private const int MaxStatements =
        EfCoreMigrationAnalyzer.MaxCommands * 2;
    private const string ConnectionString =
        "Data Source=:memory:;Pooling=false";
    private const string HistoryTable =
        EfCoreScratchSchemaCanonicalizer.HistoryTableName;
    private const string CreateHistorySql =
        "CREATE TABLE __EFMigrationsHistory (" +
        "Ordinal INTEGER PRIMARY KEY, " +
        "MigrationId TEXT NOT NULL);";

    private static readonly UTF8Encoding StrictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);

    internal static async ValueTask<EfCoreScratchChainValidationResult>
        ValidateAsync(
            IReadOnlyList<EfCoreScratchMigrationInput> migrations,
            IMigrationsSqlGenerator sqlGenerator,
            IMigrationsModelDiffer modelDiffer,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(migrations);
        ArgumentNullException.ThrowIfNull(sqlGenerator);
        ArgumentNullException.ThrowIfNull(modelDiffer);
        cancellationToken.ThrowIfCancellationRequested();

        if (migrations.Count is <= 0 or > MaxMigrations ||
            !InputsAreCoherent(migrations))
        {
            return EmptyFailure(
                migrations.Count,
                EfCoreMigrationScratchAnalysisRules.AnalysisLimit);
        }

        PlanBuildResult planBuild = BuildPlans(
            migrations,
            sqlGenerator,
            cancellationToken);
        if (!planBuild.Succeeded)
        {
            return EmptyFailure(
                migrations.Count,
                planBuild.LimitExceeded
                    ? EfCoreMigrationScratchAnalysisRules.AnalysisLimit
                    : EfCoreMigrationScratchAnalysisRules
                        .ScratchExecutionFailed);
        }

        using var state = new ValidationState(
            migrations.Count,
            migrations
                .Select(static migration => migration.MigrationId)
                .ToArray());
        await using var modelConnection =
            new CSharpDbConnection(ConnectionString);
        await using var chainConnection =
            new CSharpDbConnection(ConnectionString);
        await using var idempotentConnection =
            new CSharpDbConnection(ConnectionString);

        try
        {
            await modelConnection.OpenAsync(cancellationToken);
            await chainConnection.OpenAsync(cancellationToken);
            await idempotentConnection.OpenAsync(
                cancellationToken);

            string? preparationFailure =
                await PrepareExpectedSchemasAsync(
                    migrations,
                    sqlGenerator,
                    modelDiffer,
                    modelConnection,
                    state,
                    cancellationToken);
            if (preparationFailure is not null)
                return state.Failed(preparationFailure);

            if (!await CreateHistoryAsync(
                    chainConnection,
                    cancellationToken) ||
                !await ValidateHistoryAsync(
                    chainConnection,
                    state.MigrationIds,
                    expectedCount: 0,
                    cancellationToken))
            {
                return state.Failed(
                    EfCoreMigrationScratchAnalysisRules
                        .ScratchExecutionFailed);
            }

            string emptySchemaDigest =
                MigrationNormalizedSchemaContract.Create([]).Digest;
            string previousSchemaDigest = emptySchemaDigest;
            for (int index = 0;
                 index < planBuild.Plans.Count;
                 index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ScratchMigrationPlan plan =
                    planBuild.Plans[index];
                string expectedSchemaDigest =
                    state.ExpectedSchemaDigests[index];
                string expectedHistoryDigest =
                    EfCoreScratchEvidenceDigest.History(
                        state.MigrationIds,
                        index + 1);
                string downHistoryDigest =
                    EfCoreScratchEvidenceDigest.History(
                        state.MigrationIds,
                        index);

                if (!await ExecuteDirectionAsync(
                        chainConnection,
                        plan.UpCommands,
                        HistoryMutation.Insert,
                        plan,
                        cancellationToken))
                {
                    return state.Failed(
                        EfCoreMigrationScratchAnalysisRules
                            .ScratchExecutionFailed);
                }
                state.RecordCommittedCommands(
                    plan,
                    "up",
                    replayOrdinal: 0,
                    plan.UpCommands);
                state.AppliedPrefixCount++;

                if (!await ValidateHistoryAsync(
                        chainConnection,
                        state.MigrationIds,
                        index + 1,
                        cancellationToken))
                {
                    return state.Failed(
                        EfCoreMigrationScratchAnalysisRules
                            .ScratchExecutionFailed);
                }
                string? appliedSchema =
                    CaptureDigest(
                        chainConnection,
                        state,
                        cancellationToken);
                if (appliedSchema is null)
                    return state.Failed(state.CaptureFailureRule);
                if (!string.Equals(
                        appliedSchema,
                        expectedSchemaDigest,
                        StringComparison.Ordinal))
                {
                    return state.Failed(
                        EfCoreMigrationScratchAnalysisRules
                            .SchemaDifferent);
                }
                state.SchemaVerifiedPrefixCount++;

                if (!await ExecuteDirectionAsync(
                        chainConnection,
                        plan.DownCommands,
                        HistoryMutation.Delete,
                        plan,
                        cancellationToken))
                {
                    return state.Failed(
                        EfCoreMigrationScratchAnalysisRules
                            .ScratchExecutionFailed);
                }
                state.RecordCommittedCommands(
                    plan,
                    "down",
                    replayOrdinal: 0,
                    plan.DownCommands);
                state.DownPrefixCount++;

                if (!await ValidateHistoryAsync(
                        chainConnection,
                        state.MigrationIds,
                        index,
                        cancellationToken))
                {
                    return state.Failed(
                        EfCoreMigrationScratchAnalysisRules
                            .ScratchExecutionFailed);
                }
                string? downSchema =
                    CaptureDigest(
                        chainConnection,
                        state,
                        cancellationToken);
                if (downSchema is null)
                    return state.Failed(state.CaptureFailureRule);
                if (!string.Equals(
                        downSchema,
                        previousSchemaDigest,
                        StringComparison.Ordinal))
                {
                    return state.Failed(
                        EfCoreMigrationScratchAnalysisRules
                            .RoundTripDifferent);
                }

                if (!await ExecuteDirectionAsync(
                        chainConnection,
                        plan.UpCommands,
                        HistoryMutation.Insert,
                        plan,
                        cancellationToken))
                {
                    return state.Failed(
                        EfCoreMigrationScratchAnalysisRules
                            .ScratchExecutionFailed);
                }
                state.RecordCommittedCommands(
                    plan,
                    "up",
                    replayOrdinal: 1,
                    plan.UpCommands);
                state.ReappliedPrefixCount++;

                if (!await ValidateHistoryAsync(
                        chainConnection,
                        state.MigrationIds,
                        index + 1,
                        cancellationToken))
                {
                    return state.Failed(
                        EfCoreMigrationScratchAnalysisRules
                            .ScratchExecutionFailed);
                }
                string? reappliedSchema =
                    CaptureDigest(
                        chainConnection,
                        state,
                        cancellationToken);
                if (reappliedSchema is null)
                    return state.Failed(state.CaptureFailureRule);
                if (!string.Equals(
                        reappliedSchema,
                        expectedSchemaDigest,
                        StringComparison.Ordinal))
                {
                    return state.Failed(
                        EfCoreMigrationScratchAnalysisRules
                            .RoundTripDifferent);
                }

                state.RoundTripVerifiedPrefixCount++;
                state.Prefixes.Add(
                    new EfCoreMigrationScratchPrefixEvidence
                    {
                        Ordinal = index,
                        MigrationOrdinal = index,
                        Status =
                            MigrationCompatibilityStatus.Compatible,
                        Evidence =
                            MigrationEvidenceLevel.ScratchExecuted,
                        RuleId =
                            EfCoreMigrationScratchAnalysisRules
                                .ScratchPassed,
                        ExpectedSchemaDigest =
                            expectedSchemaDigest,
                        ExpectedHistoryDigest =
                            expectedHistoryDigest,
                        AppliedSchemaDigest = appliedSchema,
                        AppliedHistoryDigest =
                            expectedHistoryDigest,
                        DownSchemaDigest = downSchema,
                        DownHistoryDigest = downHistoryDigest,
                        ReappliedSchemaDigest =
                            reappliedSchema,
                        ReappliedHistoryDigest =
                            expectedHistoryDigest,
                    });
                previousSchemaDigest = expectedSchemaDigest;
            }

            string idempotentScript;
            try
            {
                idempotentScript =
                    BuildIdempotentScript(planBuild.Plans);
            }
            catch (ScratchLimitException)
            {
                return state.Failed(
                    EfCoreMigrationScratchAnalysisRules.AnalysisLimit);
            }
            state.IdempotentSqlDigest =
                EfCoreScratchEvidenceDigest.IdempotentSql(
                    idempotentScript);
            IReadOnlyList<string> idempotentStatements;
            try
            {
                idempotentStatements =
                    SqlScriptSplitter.SplitExecutableStatements(
                        idempotentScript);
            }
            catch (Exception exception)
                when (EfCoreMigrationAnalyzer
                    .IsRecoverable(exception))
            {
                return state.Failed(
                    EfCoreMigrationScratchAnalysisRules
                        .IdempotenceFailed);
            }
            if (idempotentStatements.Count == 0 ||
                idempotentStatements.Count > MaxStatements)
            {
                return state.Failed(
                    EfCoreMigrationScratchAnalysisRules.AnalysisLimit);
            }

            for (int applyOrdinal = 0;
                 applyOrdinal < 2;
                 applyOrdinal++)
            {
                if (!await ExecuteStatementsTransactionallyAsync(
                        idempotentConnection,
                        idempotentStatements,
                        cancellationToken))
                {
                    return state.Failed(
                        EfCoreMigrationScratchAnalysisRules
                            .IdempotenceFailed);
                }
                state.IdempotentApplyCount++;
                state.IdempotentCommandCount = checked(
                    state.IdempotentCommandCount +
                    idempotentStatements.Count);

                if (!await ValidateHistoryAsync(
                        idempotentConnection,
                        state.MigrationIds,
                        state.MigrationIds.Count,
                        cancellationToken))
                {
                    return state.Failed(
                        EfCoreMigrationScratchAnalysisRules
                            .IdempotenceFailed);
                }
                string? schema =
                    CaptureDigest(
                        idempotentConnection,
                        state,
                        cancellationToken);
                if (schema is null ||
                    !string.Equals(
                        schema,
                        state.ExpectedSchemaDigests[^1],
                        StringComparison.Ordinal))
                {
                    return state.Failed(
                        EfCoreMigrationScratchAnalysisRules
                            .IdempotenceFailed);
                }
                string history =
                    EfCoreScratchEvidenceDigest.History(
                        state.MigrationIds,
                        state.MigrationIds.Count);
                if (applyOrdinal == 0)
                {
                    state.FirstIdempotentSchemaDigest = schema;
                    state.FirstIdempotentHistoryDigest = history;
                }
                else
                {
                    state.SecondIdempotentSchemaDigest = schema;
                    state.SecondIdempotentHistoryDigest = history;
                }
            }

            return state.Passed();
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
            when (EfCoreMigrationAnalyzer.IsRecoverable(exception))
        {
            return state.Failed(
                EfCoreMigrationScratchAnalysisRules
                    .ScratchExecutionFailed);
        }
    }

    private static bool InputsAreCoherent(
        IReadOnlyList<EfCoreScratchMigrationInput> migrations)
    {
        string? previousId = null;
        for (int index = 0; index < migrations.Count; index++)
        {
            EfCoreScratchMigrationInput? migration =
                migrations[index];
            if (migration is null ||
                migration.Ordinal != index ||
                string.IsNullOrEmpty(migration.MigrationId) ||
                migration.UpOperations is null ||
                migration.UpOperations.Count == 0 ||
                migration.DownOperations is null ||
                migration.DownOperations.Count == 0 ||
                migration.TargetModel is null ||
                previousId is not null &&
                StringComparer.Ordinal.Compare(
                    previousId,
                    migration.MigrationId) >= 0)
            {
                return false;
            }
            previousId = migration.MigrationId;
        }
        return true;
    }

    private static PlanBuildResult BuildPlans(
        IReadOnlyList<EfCoreScratchMigrationInput> migrations,
        IMigrationsSqlGenerator sqlGenerator,
        CancellationToken cancellationToken)
    {
        var plans = new List<ScratchMigrationPlan>(
            migrations.Count);
        int totalCommands = 0;
        int totalStatements = 0;
        int totalBytes = 0;
        try
        {
            foreach (EfCoreScratchMigrationInput migration in
                     migrations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                IReadOnlyList<ScratchCommand>? up =
                    GenerateDirection(
                        migration.UpOperations,
                        migration.TargetModel,
                        sqlGenerator,
                        ref totalCommands,
                        ref totalStatements,
                        ref totalBytes);
                IModel? previousModel = migration.Ordinal == 0
                    ? null
                    : migrations[migration.Ordinal - 1]
                        .TargetModel;
                IReadOnlyList<ScratchCommand>? down =
                    GenerateDirection(
                        migration.DownOperations,
                        previousModel,
                        sqlGenerator,
                        ref totalCommands,
                        ref totalStatements,
                        ref totalBytes);
                if (up is null || down is null)
                    return PlanBuildResult.Failed(limit: false);
                plans.Add(
                    new ScratchMigrationPlan(
                        migration.Ordinal,
                        migration.MigrationId,
                        up,
                        down));
            }
        }
        catch (ScratchLimitException)
        {
            return PlanBuildResult.Failed(limit: true);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
            when (EfCoreMigrationAnalyzer.IsRecoverable(exception))
        {
            return PlanBuildResult.Failed(limit: false);
        }

        return PlanBuildResult.Success(plans);
    }

    private static IReadOnlyList<ScratchCommand>? GenerateDirection(
        IReadOnlyList<MigrationOperation> operations,
        IModel? model,
        IMigrationsSqlGenerator sqlGenerator,
        ref int totalCommands,
        ref int totalStatements,
        ref int totalBytes)
    {
        IReadOnlyList<MigrationCommand> generated =
            sqlGenerator.Generate(
                operations,
                model,
                MigrationsSqlGenerationOptions.Default);
        if (generated.Count == 0 ||
            generated.Count >
                EfCoreMigrationAnalyzer.MaxCommands -
                totalCommands)
        {
            throw new ScratchLimitException();
        }

        var commands =
            new List<ScratchCommand>(generated.Count);
        foreach (MigrationCommand command in generated)
        {
            if (command.TransactionSuppressed ||
                string.IsNullOrEmpty(command.CommandText))
            {
                return null;
            }
            int bytes =
                StrictUtf8.GetByteCount(command.CommandText);
            if (bytes <= 0 ||
                bytes >
                    EfCoreMigrationAnalyzer
                        .MaxGeneratedSqlUtf8Bytes -
                    totalBytes)
            {
                throw new ScratchLimitException();
            }
            IReadOnlyList<string> statements =
                SqlScriptSplitter.SplitExecutableStatements(
                    command.CommandText);
            if (statements.Count == 0 ||
                statements.Count >
                    MaxStatements - totalStatements)
            {
                throw new ScratchLimitException();
            }
            commands.Add(
                new ScratchCommand(
                    command.CommandText,
                    statements));
            totalBytes = checked(totalBytes + bytes);
            totalStatements = checked(
                totalStatements + statements.Count);
        }
        totalCommands = checked(
            totalCommands + generated.Count);
        return commands;
    }

    private static async ValueTask<string?>
        PrepareExpectedSchemasAsync(
            IReadOnlyList<EfCoreScratchMigrationInput> migrations,
            IMigrationsSqlGenerator sqlGenerator,
            IMigrationsModelDiffer modelDiffer,
            CSharpDbConnection modelConnection,
            ValidationState state,
            CancellationToken cancellationToken)
    {
        IModel? previousModel = null;
        int operationCount = 0;
        int commandCount = 0;
        int statementCount = 0;
        int sqlBytes = 0;
        foreach (EfCoreScratchMigrationInput migration in migrations)
        {
            EfCoreTargetModelMaterializationResult result =
                await EfCoreTargetModelMaterializer.MaterializeAsync(
                    modelDiffer,
                    sqlGenerator,
                    previousModel,
                    migration.TargetModel,
                    modelConnection,
                    cancellationToken);
            if (!result.Succeeded)
            {
                return IsMaterializationLimit(result.Failure)
                    ? EfCoreMigrationScratchAnalysisRules.AnalysisLimit
                    : EfCoreMigrationScratchAnalysisRules
                        .ScratchExecutionFailed;
            }
            operationCount = checked(
                operationCount +
                result.DifferenceOperationCount);
            commandCount = checked(
                commandCount + result.CommandCount);
            statementCount = checked(
                statementCount + result.StatementCount);
            sqlBytes = checked(
                sqlBytes + result.SqlUtf8Bytes);
            if (operationCount >
                    EfCoreMigrationAnalyzer.MaxOperations ||
                commandCount >
                    EfCoreMigrationAnalyzer.MaxCommands ||
                statementCount > MaxStatements ||
                sqlBytes >
                    EfCoreMigrationAnalyzer
                        .MaxGeneratedSqlUtf8Bytes)
            {
                return EfCoreMigrationScratchAnalysisRules.AnalysisLimit;
            }

            string? digest = CaptureDigest(
                modelConnection,
                state,
                cancellationToken);
            if (digest is null)
                return state.CaptureFailureRule;
            state.ExpectedSchemaDigests.Add(digest);
            previousModel = migration.TargetModel;
        }
        return null;
    }

    private static bool IsMaterializationLimit(
        EfCoreTargetModelMaterializationFailure failure) =>
        failure is
            EfCoreTargetModelMaterializationFailure
                .OperationLimitExceeded or
            EfCoreTargetModelMaterializationFailure
                .CommandLimitExceeded or
            EfCoreTargetModelMaterializationFailure
                .SqlUtf8LimitExceeded or
            EfCoreTargetModelMaterializationFailure
                .StatementLimitExceeded;

    private static string? CaptureDigest(
        CSharpDbConnection connection,
        ValidationState state,
        CancellationToken cancellationToken)
    {
        EfCoreScratchSchemaCaptureResult capture =
            EfCoreScratchSchemaCanonicalizer.Capture(
                connection,
                cancellationToken);
        if (!capture.Succeeded)
        {
            state.CaptureFailureRule =
                capture.Failure ==
                    EfCoreScratchSchemaCaptureFailure.LimitExceeded
                ? EfCoreMigrationScratchAnalysisRules.AnalysisLimit
                : EfCoreMigrationScratchAnalysisRules
                    .ScratchExecutionFailed;
            return null;
        }
        return capture.Schema!.Digest;
    }

    private static async ValueTask<bool> CreateHistoryAsync(
        CSharpDbConnection connection,
        CancellationToken cancellationToken) =>
        await ExecuteStatementsTransactionallyAsync(
            connection,
            [CreateHistorySql],
            cancellationToken);

    private static async ValueTask<bool> ExecuteDirectionAsync(
        CSharpDbConnection connection,
        IReadOnlyList<ScratchCommand> commands,
        HistoryMutation mutation,
        ScratchMigrationPlan plan,
        CancellationToken cancellationToken)
    {
        DbTransaction? transaction = null;
        try
        {
            transaction = await connection.BeginTransactionAsync(
                cancellationToken);
            foreach (ScratchCommand commandPayload in commands)
            {
                foreach (string statement in
                         commandPayload.Statements)
                {
                    await using DbCommand command =
                        connection.CreateCommand();
                    command.Transaction = transaction;
                    command.CommandText = statement;
                    _ = await command.ExecuteNonQueryAsync(
                        cancellationToken);
                }
            }

            await using DbCommand history =
                connection.CreateCommand();
            history.Transaction = transaction;
            history.CommandText = mutation == HistoryMutation.Insert
                ? FormattableString.Invariant(
                    $"INSERT INTO {HistoryTable} (Ordinal, MigrationId) VALUES ({plan.Ordinal}, {QuoteLiteral(plan.MigrationId)});")
                : FormattableString.Invariant(
                    $"DELETE FROM {HistoryTable} WHERE Ordinal = {plan.Ordinal} AND MigrationId = {QuoteLiteral(plan.MigrationId)};");
            int affected =
                await history.ExecuteNonQueryAsync(cancellationToken);
            if (affected != 1)
            {
                await transaction.RollbackAsync(
                    CancellationToken.None);
                return false;
            }

            await transaction.CommitAsync(cancellationToken);
            return true;
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            if (transaction is not null)
                await TryRollbackAsync(transaction);
            throw;
        }
        catch (Exception exception)
            when (EfCoreMigrationAnalyzer.IsRecoverable(exception))
        {
            if (transaction is not null)
                await TryRollbackAsync(transaction);
            return false;
        }
        finally
        {
            if (transaction is not null)
                await transaction.DisposeAsync();
        }
    }

    private static async ValueTask<bool>
        ExecuteStatementsTransactionallyAsync(
            CSharpDbConnection connection,
            IReadOnlyList<string> statements,
            CancellationToken cancellationToken)
    {
        DbTransaction? transaction = null;
        try
        {
            transaction = await connection.BeginTransactionAsync(
                cancellationToken);
            foreach (string statement in statements)
            {
                await using DbCommand command =
                    connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = statement;
                _ = await command.ExecuteNonQueryAsync(
                    cancellationToken);
            }
            await transaction.CommitAsync(cancellationToken);
            return true;
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            if (transaction is not null)
                await TryRollbackAsync(transaction);
            throw;
        }
        catch (Exception exception)
            when (EfCoreMigrationAnalyzer.IsRecoverable(exception))
        {
            if (transaction is not null)
                await TryRollbackAsync(transaction);
            return false;
        }
        finally
        {
            if (transaction is not null)
                await transaction.DisposeAsync();
        }
    }

    private static async ValueTask TryRollbackAsync(
        DbTransaction transaction)
    {
        try
        {
            await transaction.RollbackAsync(CancellationToken.None);
        }
        catch (Exception exception)
            when (EfCoreMigrationAnalyzer.IsRecoverable(exception))
        {
            // The private-memory connection is discarded after a failure.
        }
    }

    private static async ValueTask<bool> ValidateHistoryAsync(
        CSharpDbConnection connection,
        IReadOnlyList<string> migrationIds,
        int expectedCount,
        CancellationToken cancellationToken)
    {
        try
        {
            await using DbCommand command =
                connection.CreateCommand();
            command.CommandText =
                $"SELECT Ordinal, MigrationId FROM {HistoryTable} " +
                "ORDER BY Ordinal;";
            await using DbDataReader reader =
                await command.ExecuteReaderAsync(cancellationToken);
            int ordinal = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                if (ordinal >= expectedCount ||
                    reader.GetInt64(0) != ordinal ||
                    !string.Equals(
                        reader.GetString(1),
                        migrationIds[ordinal],
                        StringComparison.Ordinal))
                {
                    return false;
                }
                ordinal++;
            }
            return ordinal == expectedCount;
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
            when (EfCoreMigrationAnalyzer.IsRecoverable(exception))
        {
            return false;
        }
    }

    private static string BuildIdempotentScript(
        IReadOnlyList<ScratchMigrationPlan> plans)
    {
        var builder = new StringBuilder();
        builder.AppendLine(
            "CREATE TABLE IF NOT EXISTS __EFMigrationsHistory (" +
            "Ordinal INTEGER PRIMARY KEY, " +
            "MigrationId TEXT NOT NULL);");
        foreach (ScratchMigrationPlan plan in plans)
        {
            builder.Append(
                "IF NOT EXISTS (SELECT 1 FROM " +
                $"{HistoryTable} WHERE MigrationId = " +
                $"{QuoteLiteral(plan.MigrationId)}) BEGIN");
            builder.AppendLine();
            foreach (ScratchCommand command in plan.UpCommands)
            {
                builder.Append(command.CommandText);
                if (!command.CommandText.EndsWith(
                        '\n'))
                {
                    builder.AppendLine();
                }
            }
            builder.Append(
                FormattableString.Invariant(
                    $"INSERT INTO {HistoryTable} (Ordinal, MigrationId) VALUES ({plan.Ordinal}, {QuoteLiteral(plan.MigrationId)});"));
            builder.AppendLine();
            builder.AppendLine("END;");
        }

        string script = builder.ToString();
        int bytes = StrictUtf8.GetByteCount(script);
        if (bytes >
            EfCoreMigrationAnalyzer.MaxGeneratedSqlUtf8Bytes)
        {
            throw new ScratchLimitException();
        }
        return script;
    }

    private static string QuoteLiteral(string value) =>
        $"'{value.Replace("'", "''", StringComparison.Ordinal)}'";

    private static EfCoreScratchChainValidationResult EmptyFailure(
        int prefixCount,
        string ruleId) =>
        new()
        {
            Outcome = EfCoreMigrationScratchAnalysisOutcome.Failed,
            RuleId = ruleId,
            Proof = new EfCoreMigrationScratchChainProof
            {
                Outcome =
                    EfCoreMigrationScratchAnalysisOutcome.Failed,
                PrefixCount = Math.Max(prefixCount, 0),
                ResourcesDisposed = true,
            },
        };

    private enum HistoryMutation
    {
        Insert,
        Delete,
    }

    private sealed record ScratchCommand(
        string CommandText,
        IReadOnlyList<string> Statements);

    private sealed record ScratchMigrationPlan(
        int Ordinal,
        string MigrationId,
        IReadOnlyList<ScratchCommand> UpCommands,
        IReadOnlyList<ScratchCommand> DownCommands);

    private sealed record PlanBuildResult(
        bool Succeeded,
        bool LimitExceeded,
        IReadOnlyList<ScratchMigrationPlan> Plans)
    {
        internal static PlanBuildResult Success(
            IReadOnlyList<ScratchMigrationPlan> plans) =>
            new(true, false, plans);

        internal static PlanBuildResult Failed(bool limit) =>
            new(false, limit, []);
    }

    private sealed class ValidationState : IDisposable
    {
        private readonly EfCoreScratchEvidenceDigest.Accumulator
            _executedSql =
                EfCoreScratchEvidenceDigest.ExecutedSql();
        private bool _digestFinished;

        internal ValidationState(
            int prefixCount,
            IReadOnlyList<string> migrationIds)
        {
            PrefixCount = prefixCount;
            MigrationIds = migrationIds;
        }

        internal int PrefixCount { get; }

        internal IReadOnlyList<string> MigrationIds { get; }

        internal List<string> ExpectedSchemaDigests { get; } = [];

        internal List<EfCoreMigrationScratchPrefixEvidence> Prefixes
        {
            get;
        } = [];

        internal int AppliedPrefixCount;
        internal int SchemaVerifiedPrefixCount;
        internal int DownPrefixCount;
        internal int ReappliedPrefixCount;
        internal int RoundTripVerifiedPrefixCount;
        internal int IdempotentApplyCount;
        internal int ExecutedCommandCount;
        internal int IdempotentCommandCount;
        internal string? IdempotentSqlDigest;
        internal string? FirstIdempotentSchemaDigest;
        internal string? FirstIdempotentHistoryDigest;
        internal string? SecondIdempotentSchemaDigest;
        internal string? SecondIdempotentHistoryDigest;
        internal string CaptureFailureRule =
            EfCoreMigrationScratchAnalysisRules
                .ScratchExecutionFailed;

        internal void RecordCommittedCommands(
            ScratchMigrationPlan plan,
            string direction,
            int replayOrdinal,
            IReadOnlyList<ScratchCommand> commands)
        {
            for (int commandOrdinal = 0;
                 commandOrdinal < commands.Count;
                 commandOrdinal++)
            {
                _executedSql.AppendInt32(plan.Ordinal);
                _executedSql.AppendString(direction);
                _executedSql.AppendInt32(replayOrdinal);
                _executedSql.AppendInt32(commandOrdinal);
                _executedSql.AppendString(
                    commands[commandOrdinal].CommandText);
                ExecutedCommandCount++;
            }
        }

        internal EfCoreScratchChainValidationResult Failed(
            string ruleId) =>
            Result(
                EfCoreMigrationScratchAnalysisOutcome.Failed,
                ruleId);

        internal EfCoreScratchChainValidationResult Passed() =>
            Result(
                EfCoreMigrationScratchAnalysisOutcome.Passed,
                EfCoreMigrationScratchAnalysisRules.ScratchPassed);

        private EfCoreScratchChainValidationResult Result(
            EfCoreMigrationScratchAnalysisOutcome outcome,
            string ruleId)
        {
            string? executedDigest = null;
            if (ExecutedCommandCount > 0)
            {
                if (_digestFinished)
                    throw new InvalidOperationException();
                executedDigest = _executedSql.Finish();
                _digestFinished = true;
            }

            return new EfCoreScratchChainValidationResult
            {
                Outcome = outcome,
                RuleId = ruleId,
                Proof = new EfCoreMigrationScratchChainProof
                {
                    Outcome = outcome,
                    PrefixCount = PrefixCount,
                    AppliedPrefixCount = AppliedPrefixCount,
                    SchemaVerifiedPrefixCount =
                        SchemaVerifiedPrefixCount,
                    DownPrefixCount = DownPrefixCount,
                    ReappliedPrefixCount =
                        ReappliedPrefixCount,
                    RoundTripVerifiedPrefixCount =
                        RoundTripVerifiedPrefixCount,
                    IdempotentApplyCount =
                        IdempotentApplyCount,
                    ExecutedCommandCount =
                        ExecutedCommandCount,
                    IdempotentCommandCount =
                        IdempotentCommandCount,
                    ExecutedSqlDigest = executedDigest,
                    IdempotentSqlDigest =
                        IdempotentCommandCount == 0
                            ? null
                            : IdempotentSqlDigest,
                    FirstIdempotentSchemaDigest =
                        FirstIdempotentSchemaDigest,
                    FirstIdempotentHistoryDigest =
                        FirstIdempotentHistoryDigest,
                    SecondIdempotentSchemaDigest =
                        SecondIdempotentSchemaDigest,
                    SecondIdempotentHistoryDigest =
                        SecondIdempotentHistoryDigest,
                    ResourcesDisposed = true,
                    Prefixes = Prefixes,
                },
            };
        }

        public void Dispose() => _executedSql.Dispose();
    }

    private sealed class ScratchLimitException : Exception;
}
