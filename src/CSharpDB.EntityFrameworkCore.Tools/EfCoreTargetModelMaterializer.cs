using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Text;
using CSharpDB.Data;
using CSharpDB.Sql;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace CSharpDB.EntityFrameworkCore.Tools;

internal enum EfCoreTargetModelMaterializationFailure
{
    None = 0,
    ConnectionRejected = 1,
    DifferenceFailed = 2,
    OperationLimitExceeded = 3,
    UnsafeOperation = 4,
    GenerationFailed = 5,
    CommandLimitExceeded = 6,
    SqlUtf8LimitExceeded = 7,
    TransactionSuppressed = 8,
    ScriptSplitFailed = 9,
    StatementLimitExceeded = 10,
    ExecutionFailed = 11,
    TransactionFailed = 12,
}

internal readonly record struct EfCoreTargetModelMaterializationLimits(
    int MaxOperations,
    int MaxCommands,
    int MaxStatements,
    int MaxSqlUtf8Bytes)
{
    internal static EfCoreTargetModelMaterializationLimits Default { get; } =
        new(
            EfCoreMigrationAnalyzer.MaxOperations,
            EfCoreMigrationAnalyzer.MaxCommands,
            EfCoreMigrationAnalyzer.MaxCommands,
            EfCoreMigrationAnalyzer.MaxGeneratedSqlUtf8Bytes);

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxOperations, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxCommands, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxStatements, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxSqlUtf8Bytes, 1);
    }
}

internal sealed record EfCoreTargetModelMaterializationResult
{
    internal EfCoreTargetModelMaterializationResult(
        EfCoreTargetModelMaterializationFailure failure,
        int differenceOperationCount,
        int filteredDataOperationCount,
        int structuralOperationCount,
        int commandCount,
        int statementCount,
        int sqlUtf8Bytes)
    {
        Failure = failure;
        DifferenceOperationCount = differenceOperationCount;
        FilteredDataOperationCount = filteredDataOperationCount;
        StructuralOperationCount = structuralOperationCount;
        CommandCount = commandCount;
        StatementCount = statementCount;
        SqlUtf8Bytes = sqlUtf8Bytes;
    }

    internal EfCoreTargetModelMaterializationFailure Failure { get; }

    internal int DifferenceOperationCount { get; }

    internal int FilteredDataOperationCount { get; }

    internal int StructuralOperationCount { get; }

    internal int CommandCount { get; }

    internal int StatementCount { get; }

    internal int SqlUtf8Bytes { get; }

    internal bool Succeeded =>
        Failure == EfCoreTargetModelMaterializationFailure.None;
}

/// <summary>
/// Materializes one initialized EF Core target model into a tool-owned,
/// already-open private-memory CSharpDB connection.
/// </summary>
/// <remarks>
/// The caller owns the supplied services, models, and connection. This type
/// never creates, opens, closes, or disposes a connection and never reads a
/// context's configured connection. Exact seed-data operations are omitted so
/// that the resulting database contains schema evidence only.
/// </remarks>
internal static class EfCoreTargetModelMaterializer
{
    private static readonly UTF8Encoding StrictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    internal static ValueTask<EfCoreTargetModelMaterializationResult>
        MaterializeAsync(
            IMigrationsModelDiffer modelDiffer,
            IMigrationsSqlGenerator sqlGenerator,
            IModel? previousModel,
            IModel currentModel,
            CSharpDbConnection connection,
            CancellationToken cancellationToken = default) =>
        MaterializeAsync(
            modelDiffer,
            sqlGenerator,
            previousModel,
            currentModel,
            connection,
            EfCoreTargetModelMaterializationLimits.Default,
            cancellationToken);

    internal static async ValueTask<
        EfCoreTargetModelMaterializationResult> MaterializeAsync(
            IMigrationsModelDiffer modelDiffer,
            IMigrationsSqlGenerator sqlGenerator,
            IModel? previousModel,
            IModel currentModel,
            CSharpDbConnection connection,
            EfCoreTargetModelMaterializationLimits limits,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(modelDiffer);
        ArgumentNullException.ThrowIfNull(sqlGenerator);
        ArgumentNullException.ThrowIfNull(currentModel);
        ArgumentNullException.ThrowIfNull(connection);
        limits.Validate();
        cancellationToken.ThrowIfCancellationRequested();

        int differenceOperationCount = 0;
        int filteredDataOperationCount = 0;
        int structuralOperationCount = 0;
        int commandCount = 0;
        int statementCount = 0;
        int sqlUtf8Bytes = 0;

        EfCoreTargetModelMaterializationResult Result(
            EfCoreTargetModelMaterializationFailure failure) =>
            new(
                failure,
                differenceOperationCount,
                filteredDataOperationCount,
                structuralOperationCount,
                commandCount,
                statementCount,
                sqlUtf8Bytes);

        if (!IsOpenPrivateMemoryConnection(connection))
        {
            return Result(
                EfCoreTargetModelMaterializationFailure
                    .ConnectionRejected);
        }

        IReadOnlyList<MigrationOperation> differences;
        try
        {
            IRelationalModel? previousRelationalModel =
                previousModel?.GetRelationalModel();
            IRelationalModel currentRelationalModel =
                currentModel.GetRelationalModel();
            differences = modelDiffer.GetDifferences(
                previousRelationalModel,
                currentRelationalModel);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception error) when (IsRecoverable(error))
        {
            return Result(
                EfCoreTargetModelMaterializationFailure.DifferenceFailed);
        }

        if (differences is null)
        {
            return Result(
                EfCoreTargetModelMaterializationFailure.DifferenceFailed);
        }

        differenceOperationCount = differences.Count;
        if (differenceOperationCount > limits.MaxOperations)
        {
            return Result(
                EfCoreTargetModelMaterializationFailure
                    .OperationLimitExceeded);
        }

        var structuralOperations =
            new List<MigrationOperation>(differenceOperationCount);
        foreach (MigrationOperation operation in differences)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (operation is null)
            {
                return Result(
                    EfCoreTargetModelMaterializationFailure
                        .UnsafeOperation);
            }

            Type operationType = operation.GetType();
            if (IsExactDataOperation(operationType))
            {
                filteredDataOperationCount++;
                continue;
            }

            if (!IsExactStructuralOperation(operationType))
            {
                return Result(
                    EfCoreTargetModelMaterializationFailure
                        .UnsafeOperation);
            }

            structuralOperations.Add(operation);
            structuralOperationCount++;
        }

        if (structuralOperationCount == 0)
        {
            return Result(
                EfCoreTargetModelMaterializationFailure.None);
        }

        IReadOnlyList<MigrationCommand> commands;
        try
        {
            commands = sqlGenerator.Generate(
                structuralOperations,
                currentModel,
                MigrationsSqlGenerationOptions.Default);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception error) when (IsRecoverable(error))
        {
            return Result(
                EfCoreTargetModelMaterializationFailure.GenerationFailed);
        }

        if (commands is null)
        {
            return Result(
                EfCoreTargetModelMaterializationFailure.GenerationFailed);
        }

        commandCount = commands.Count;
        if (commandCount == 0)
        {
            return Result(
                EfCoreTargetModelMaterializationFailure.GenerationFailed);
        }
        if (commandCount > limits.MaxCommands)
        {
            return Result(
                EfCoreTargetModelMaterializationFailure
                    .CommandLimitExceeded);
        }

        var statements = new List<string>(
            Math.Min(commandCount, limits.MaxStatements));
        foreach (MigrationCommand command in commands)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (command is null)
            {
                return Result(
                    EfCoreTargetModelMaterializationFailure
                        .GenerationFailed);
            }

            if (command.TransactionSuppressed)
            {
                return Result(
                    EfCoreTargetModelMaterializationFailure
                        .TransactionSuppressed);
            }

            string commandText = command.CommandText;
            int commandUtf8Bytes;
            try
            {
                commandUtf8Bytes =
                    StrictUtf8.GetByteCount(commandText);
            }
            catch (Exception error)
                when (IsRecoverable(error))
            {
                return Result(
                    EfCoreTargetModelMaterializationFailure
                        .GenerationFailed);
            }

            if (commandUtf8Bytes >
                limits.MaxSqlUtf8Bytes - sqlUtf8Bytes)
            {
                return Result(
                    EfCoreTargetModelMaterializationFailure
                        .SqlUtf8LimitExceeded);
            }
            sqlUtf8Bytes += commandUtf8Bytes;

            IReadOnlyList<string> commandStatements;
            try
            {
                commandStatements =
                    SqlScriptSplitter.SplitExecutableStatements(
                        commandText);
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception error) when (IsRecoverable(error))
            {
                return Result(
                    EfCoreTargetModelMaterializationFailure
                        .ScriptSplitFailed);
            }

            if (commandStatements.Count == 0)
            {
                return Result(
                    EfCoreTargetModelMaterializationFailure
                        .ScriptSplitFailed);
            }
            if (commandStatements.Count >
                limits.MaxStatements - statementCount)
            {
                return Result(
                    EfCoreTargetModelMaterializationFailure
                        .StatementLimitExceeded);
            }

            statements.AddRange(commandStatements);
            statementCount += commandStatements.Count;
        }

        if (!IsOpenPrivateMemoryConnection(connection))
        {
            return Result(
                EfCoreTargetModelMaterializationFailure
                    .ConnectionRejected);
        }

        try
        {
            EfCoreTargetModelMaterializationFailure executionFailure =
                await ExecuteTransactionallyAsync(
                    connection,
                    statements,
                    cancellationToken);
            return Result(executionFailure);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception error) when (IsRecoverable(error))
        {
            return Result(
                EfCoreTargetModelMaterializationFailure
                    .TransactionFailed);
        }
    }

    private static async ValueTask<
        EfCoreTargetModelMaterializationFailure>
        ExecuteTransactionallyAsync(
            CSharpDbConnection connection,
            IReadOnlyList<string> statements,
            CancellationToken cancellationToken)
    {
        DbTransaction transaction;
        try
        {
            transaction = await connection.BeginTransactionAsync(
                cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception error) when (IsRecoverable(error))
        {
            return EfCoreTargetModelMaterializationFailure
                .TransactionFailed;
        }

        try
        {
            try
            {
                foreach (string statement in statements)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await using DbCommand command =
                        connection.CreateCommand();
                    command.Transaction = transaction;
                    command.CommandText = statement;
                    _ = await command.ExecuteNonQueryAsync(
                        cancellationToken);
                }
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                _ = await TryRollbackAsync(transaction);
                throw;
            }
            catch (Exception error) when (IsRecoverable(error))
            {
                bool rolledBack =
                    await TryRollbackAsync(transaction);
                return rolledBack
                    ? EfCoreTargetModelMaterializationFailure
                        .ExecutionFailed
                    : EfCoreTargetModelMaterializationFailure
                        .TransactionFailed;
            }

            try
            {
                await transaction.CommitAsync(cancellationToken);
                return EfCoreTargetModelMaterializationFailure.None;
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                _ = await TryRollbackAsync(transaction);
                throw;
            }
            catch (Exception error) when (IsRecoverable(error))
            {
                _ = await TryRollbackAsync(transaction);
                return EfCoreTargetModelMaterializationFailure
                    .TransactionFailed;
            }
        }
        finally
        {
            await transaction.DisposeAsync();
        }
    }

    private static async ValueTask<bool> TryRollbackAsync(
        DbTransaction transaction)
    {
        try
        {
            await transaction.RollbackAsync(CancellationToken.None);
            return true;
        }
        catch (Exception error) when (IsRecoverable(error))
        {
            return false;
        }
    }

    private static bool IsOpenPrivateMemoryConnection(
        CSharpDbConnection connection)
    {
        if (connection.State != ConnectionState.Open)
            return false;

        try
        {
            var builder = new CSharpDbConnectionStringBuilder(
                connection.ConnectionString);
            return string.Equals(
                    builder.DataSource,
                    ":memory:",
                    StringComparison.OrdinalIgnoreCase) &&
                !builder.Pooling &&
                string.IsNullOrWhiteSpace(builder.Endpoint) &&
                string.IsNullOrWhiteSpace(builder.LoadFrom) &&
                string.IsNullOrWhiteSpace(builder.Transport);
        }
        catch (Exception error) when (IsRecoverable(error))
        {
            return false;
        }
    }

    private static bool IsExactDataOperation(Type operationType) =>
        operationType == typeof(InsertDataOperation) ||
        operationType == typeof(UpdateDataOperation) ||
        operationType == typeof(DeleteDataOperation);

    private static bool IsExactStructuralOperation(Type operationType) =>
        operationType == typeof(CreateTableOperation) ||
        operationType == typeof(DropTableOperation) ||
        operationType == typeof(RenameTableOperation) ||
        operationType == typeof(AddColumnOperation) ||
        operationType == typeof(AlterColumnOperation) ||
        operationType == typeof(DropColumnOperation) ||
        operationType == typeof(RenameColumnOperation) ||
        operationType == typeof(CreateIndexOperation) ||
        operationType == typeof(DropIndexOperation) ||
        operationType == typeof(RenameIndexOperation) ||
        operationType == typeof(AddPrimaryKeyOperation) ||
        operationType == typeof(DropPrimaryKeyOperation) ||
        operationType == typeof(AddUniqueConstraintOperation) ||
        operationType == typeof(DropUniqueConstraintOperation) ||
        operationType == typeof(AddForeignKeyOperation) ||
        operationType == typeof(DropForeignKeyOperation) ||
        operationType == typeof(AddCheckConstraintOperation) ||
        operationType == typeof(DropCheckConstraintOperation);

    private static bool IsRecoverable(Exception error)
    {
        if (error is OutOfMemoryException or
            StackOverflowException or
            AccessViolationException)
        {
            return false;
        }

        if (error is AggregateException aggregate)
        {
            return aggregate.InnerExceptions.All(IsRecoverable);
        }

        if (error is TargetInvocationException or
            TypeInitializationException)
        {
            return error.InnerException is null ||
                IsRecoverable(error.InnerException);
        }

        return true;
    }
}
