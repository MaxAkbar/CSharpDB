using CSharpDB.EntityFrameworkCore;
using CSharpDB.Migration;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace CSharpDB.EntityFrameworkCore.Tools.Tests;

public sealed class EfCoreScratchChainValidatorTests
{
    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task ValidateAsync_MapsUnsafeExpectedModelToExecutionFailure()
    {
        using var context = new EmptyContext();
        EfCoreScratchMigrationInput input =
            CreateInput(
                context.GetService<IDesignTimeModel>().Model,
                [CreateTable("UnsafeExpectedModel")]);
        var modelDiffer = new FixedModelDiffer(
            [new SqlOperation { Sql = "CREATE TABLE hidden (id INT)" }]);

        EfCoreScratchChainValidationResult result =
            await EfCoreScratchChainValidator.ValidateAsync(
                [input],
                context.GetService<IMigrationsSqlGenerator>(),
                modelDiffer,
                Ct);

        Assert.Equal(
            EfCoreMigrationScratchAnalysisOutcome.Failed,
            result.Outcome);
        Assert.Equal(
            EfCoreMigrationScratchAnalysisRules
                .ScratchExecutionFailed,
            result.RuleId);
        Assert.Equal(1, result.Proof.PrefixCount);
        Assert.Equal(0, result.Proof.AppliedPrefixCount);
        Assert.Equal(0, result.Proof.ExecutedCommandCount);
        Assert.Null(result.Proof.ExecutedSqlDigest);
        Assert.True(result.Proof.ResourcesDisposed);
        Assert.True(modelDiffer.WasCalled);
    }

    [Fact]
    public async Task ValidateAsync_RollsBackFailedDirectionBeforeEvidence()
    {
        using var context = new EmptyContext();
        IModel targetModel =
            context.GetService<IDesignTimeModel>().Model;
        CreateTableOperation create =
            CreateTable("CreatedThenRolledBack");
        var invalidAlter = new AddColumnOperation
        {
            Name = "MissingColumn",
            Table = "MissingTable",
            ClrType = typeof(long),
            ColumnType = "INTEGER",
            IsNullable = true,
        };
        EfCoreScratchMigrationInput input =
            CreateInput(
                targetModel,
                [create, invalidAlter]);

        EfCoreScratchChainValidationResult result =
            await EfCoreScratchChainValidator.ValidateAsync(
                [input],
                context.GetService<IMigrationsSqlGenerator>(),
                context.GetService<IMigrationsModelDiffer>(),
                Ct);

        Assert.Equal(
            EfCoreMigrationScratchAnalysisOutcome.Failed,
            result.Outcome);
        Assert.Equal(
            EfCoreMigrationScratchAnalysisRules
                .ScratchExecutionFailed,
            result.RuleId);
        Assert.Equal(0, result.Proof.AppliedPrefixCount);
        Assert.Equal(0, result.Proof.ExecutedCommandCount);
        Assert.Null(result.Proof.ExecutedSqlDigest);
        Assert.True(result.Proof.ResourcesDisposed);
    }

    [Fact]
    public async Task ValidateAsync_RejectsScratchChainLimits()
    {
        using var context = new EmptyContext();
        IMigrationsSqlGenerator generator =
            context.GetService<IMigrationsSqlGenerator>();
        IMigrationsModelDiffer differ =
            context.GetService<IMigrationsModelDiffer>();

        EfCoreScratchChainValidationResult empty =
            await EfCoreScratchChainValidator.ValidateAsync(
                [],
                generator,
                differ,
                Ct);
        Assert.Equal(
            EfCoreMigrationScratchAnalysisRules.AnalysisLimit,
            empty.RuleId);

        IModel model =
            context.GetService<IDesignTimeModel>().Model;
        EfCoreScratchMigrationInput[] oversized =
            Enumerable.Range(
                    0,
                    EfCoreScratchChainValidator.MaxMigrations + 1)
                .Select(ordinal =>
                    CreateInput(
                        model,
                        [CreateTable($"Table{ordinal:D4}")],
                        ordinal))
                .ToArray();
        EfCoreScratchChainValidationResult tooMany =
            await EfCoreScratchChainValidator.ValidateAsync(
                oversized,
                generator,
                differ,
                Ct);

        Assert.Equal(
            EfCoreMigrationScratchAnalysisRules.AnalysisLimit,
            tooMany.RuleId);
        Assert.Equal(
            EfCoreScratchChainValidator.MaxMigrations + 1,
            tooMany.Proof.PrefixCount);
        Assert.Equal(0, tooMany.Proof.ExecutedCommandCount);
    }

    [Fact]
    public async Task ValidateAsync_ObservesPreCanceledRequest()
    {
        using var context = new EmptyContext();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            async () =>
                await EfCoreScratchChainValidator.ValidateAsync(
                    [],
                    context.GetService<IMigrationsSqlGenerator>(),
                    context.GetService<IMigrationsModelDiffer>(),
                    cancellation.Token));
    }

    private static EfCoreScratchMigrationInput CreateInput(
        IModel targetModel,
        IReadOnlyList<MigrationOperation> upOperations,
        int ordinal = 0) =>
        new()
        {
            Ordinal = ordinal,
            MigrationId =
                $"20260725{ordinal:D4}_ScratchValidator",
            UpOperations = upOperations,
            DownOperations =
            [
                new DropTableOperation
                {
                    Name = $"Table{ordinal:D4}",
                },
            ],
            TargetModel = targetModel,
        };

    private static CreateTableOperation CreateTable(string table)
    {
        var operation = new CreateTableOperation
        {
            Name = table,
        };
        operation.Columns.Add(
            new AddColumnOperation
            {
                Name = "Id",
                Table = table,
                ClrType = typeof(long),
                ColumnType = "INTEGER",
                IsNullable = false,
            });
        return operation;
    }

    private sealed class EmptyContext : DbContext
    {
        protected override void OnConfiguring(
            DbContextOptionsBuilder optionsBuilder) =>
            optionsBuilder.UseCSharpDb(
                "Data Source=:memory:;Pooling=false");
    }

    private sealed class FixedModelDiffer(
        IReadOnlyList<MigrationOperation> operations) :
        IMigrationsModelDiffer
    {
        internal bool WasCalled { get; private set; }

        public bool HasDifferences(
            IRelationalModel? source,
            IRelationalModel? target) =>
            operations.Count != 0;

        public IReadOnlyList<MigrationOperation> GetDifferences(
            IRelationalModel? source,
            IRelationalModel? target)
        {
            WasCalled = true;
            return operations;
        }
    }
}
