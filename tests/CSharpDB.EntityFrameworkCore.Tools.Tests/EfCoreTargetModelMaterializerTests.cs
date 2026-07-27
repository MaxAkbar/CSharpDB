using System.Data;
using System.Globalization;
using CSharpDB.Data;
using CSharpDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace CSharpDB.EntityFrameworkCore.Tools.Tests;

public sealed class EfCoreTargetModelMaterializerTests
{
    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task MaterializeAsync_AppliesTargetSchemaWithoutSeedRows()
    {
        string selectedContextPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-selected-{Guid.NewGuid():N}.db");
        using var context = new SeededTargetContext(
            $"Data Source={selectedContextPath}");
        IModel currentModel =
            context.GetService<IDesignTimeModel>().Model;

        await using var scratch = await OpenScratchAsync();
        EfCoreTargetModelMaterializationResult result =
            await EfCoreTargetModelMaterializer.MaterializeAsync(
                context.GetService<IMigrationsModelDiffer>(),
                context.GetService<IMigrationsSqlGenerator>(),
                previousModel: null,
                currentModel,
                scratch,
                Ct);

        Assert.True(result.Succeeded);
        Assert.Equal(
            EfCoreTargetModelMaterializationFailure.None,
            result.Failure);
        Assert.Equal(2, result.DifferenceOperationCount);
        Assert.Equal(1, result.FilteredDataOperationCount);
        Assert.Equal(1, result.StructuralOperationCount);
        Assert.Equal(1, result.CommandCount);
        Assert.Equal(1, result.StatementCount);
        Assert.True(result.SqlUtf8Bytes > 0);
        Assert.Equal(ConnectionState.Open, scratch.State);
        Assert.Equal(
            1,
            await CountTablesAsync(scratch, "TargetWidgets"));
        Assert.Equal(
            0,
            await ExecuteScalarIntAsync(
                scratch,
                "SELECT COUNT(*) FROM TargetWidgets"));
        Assert.False(File.Exists(selectedContextPath));
    }

    [Fact]
    public async Task MaterializeAsync_RejectsConnectionBeforeModelDifference()
    {
        using var context = new EmptyTargetContext();
        IModel currentModel =
            context.GetService<IDesignTimeModel>().Model;
        var differ = new FixedModelDiffer([]);
        await using var closedConnection =
            new CSharpDbConnection(
                "Data Source=:memory:;Pooling=false");

        EfCoreTargetModelMaterializationResult result =
            await EfCoreTargetModelMaterializer.MaterializeAsync(
                differ,
                new RejectingSqlGenerator(),
                previousModel: null,
                currentModel,
                closedConnection,
                Ct);

        Assert.Equal(
            EfCoreTargetModelMaterializationFailure
                .ConnectionRejected,
            result.Failure);
        Assert.False(differ.WasCalled);
        Assert.Equal(ConnectionState.Closed, closedConnection.State);
        Assert.Equal(0, result.DifferenceOperationCount);
    }

    [Fact]
    public async Task MaterializeAsync_RejectsRawSqlWithoutGeneratingCommands()
    {
        using var context = new EmptyTargetContext();
        IModel currentModel =
            context.GetService<IDesignTimeModel>().Model;
        var differ = new FixedModelDiffer(
            [new SqlOperation { Sql = "DROP TABLE Anything" }]);
        var generator = new RejectingSqlGenerator();
        await using var scratch = await OpenScratchAsync();

        EfCoreTargetModelMaterializationResult result =
            await EfCoreTargetModelMaterializer.MaterializeAsync(
                differ,
                generator,
                previousModel: null,
                currentModel,
                scratch,
                Ct);

        Assert.Equal(
            EfCoreTargetModelMaterializationFailure.UnsafeOperation,
            result.Failure);
        Assert.Equal(1, result.DifferenceOperationCount);
        Assert.Equal(0, result.FilteredDataOperationCount);
        Assert.Equal(0, result.StructuralOperationCount);
        Assert.False(generator.WasCalled);
    }

    [Fact]
    public async Task MaterializeAsync_DoesNotTreatDerivedSeedOperationAsData()
    {
        using var context = new EmptyTargetContext();
        IModel currentModel =
            context.GetService<IDesignTimeModel>().Model;
        var differ = new FixedModelDiffer(
            [new DerivedInsertDataOperation()]);
        var generator = new RejectingSqlGenerator();
        await using var scratch = await OpenScratchAsync();

        EfCoreTargetModelMaterializationResult result =
            await EfCoreTargetModelMaterializer.MaterializeAsync(
                differ,
                generator,
                previousModel: null,
                currentModel,
                scratch,
                Ct);

        Assert.Equal(
            EfCoreTargetModelMaterializationFailure.UnsafeOperation,
            result.Failure);
        Assert.Equal(0, result.FilteredDataOperationCount);
        Assert.False(generator.WasCalled);
    }

    [Fact]
    public async Task MaterializeAsync_BoundsDifferencesBeforeClassification()
    {
        using var context = new EmptyTargetContext();
        IModel currentModel =
            context.GetService<IDesignTimeModel>().Model;
        var differ = new FixedModelDiffer(
            [
                CreateTable("FirstTable"),
                CreateTable("SecondTable"),
            ]);
        var generator = new RejectingSqlGenerator();
        await using var scratch = await OpenScratchAsync();
        var limits = new EfCoreTargetModelMaterializationLimits(
            MaxOperations: 1,
            MaxCommands: 10,
            MaxStatements: 10,
            MaxSqlUtf8Bytes: 1024);

        EfCoreTargetModelMaterializationResult result =
            await EfCoreTargetModelMaterializer.MaterializeAsync(
                differ,
                generator,
                previousModel: null,
                currentModel,
                scratch,
                limits,
                Ct);

        Assert.Equal(
            EfCoreTargetModelMaterializationFailure
                .OperationLimitExceeded,
            result.Failure);
        Assert.Equal(2, result.DifferenceOperationCount);
        Assert.Equal(0, result.StructuralOperationCount);
        Assert.False(generator.WasCalled);
    }

    [Fact]
    public async Task MaterializeAsync_EnforcesUtf8LimitBeforeExecution()
    {
        using var context = new SeededTargetContext(
            "Data Source=selected-context-must-not-open.db");
        IModel currentModel =
            context.GetService<IDesignTimeModel>().Model;
        await using var scratch = await OpenScratchAsync();
        var limits = new EfCoreTargetModelMaterializationLimits(
            MaxOperations: 10,
            MaxCommands: 10,
            MaxStatements: 10,
            MaxSqlUtf8Bytes: 1);

        EfCoreTargetModelMaterializationResult result =
            await EfCoreTargetModelMaterializer.MaterializeAsync(
                context.GetService<IMigrationsModelDiffer>(),
                context.GetService<IMigrationsSqlGenerator>(),
                previousModel: null,
                currentModel,
                scratch,
                limits,
                Ct);

        Assert.Equal(
            EfCoreTargetModelMaterializationFailure
                .SqlUtf8LimitExceeded,
            result.Failure);
        Assert.Equal(0, result.StatementCount);
        Assert.Equal(
            0,
            await CountTablesAsync(scratch, "TargetWidgets"));
    }

    [Fact]
    public async Task MaterializeAsync_RollsBackStructuralExecutionFailure()
    {
        const string createdTable = "CreatedThenRolledBack";
        using var context = new EmptyTargetContext();
        IModel currentModel =
            context.GetService<IDesignTimeModel>().Model;
        var differ = new FixedModelDiffer(
            [
                CreateTable(createdTable),
                new AddColumnOperation
                {
                    Name = "Extra",
                    Table = "MissingTable",
                    ClrType = typeof(long),
                    ColumnType = "INTEGER",
                    IsNullable = true,
                },
            ]);
        await using var scratch = await OpenScratchAsync();

        EfCoreTargetModelMaterializationResult result =
            await EfCoreTargetModelMaterializer.MaterializeAsync(
                differ,
                context.GetService<IMigrationsSqlGenerator>(),
                previousModel: null,
                currentModel,
                scratch,
                Ct);

        Assert.Equal(
            EfCoreTargetModelMaterializationFailure.ExecutionFailed,
            result.Failure);
        Assert.Equal(2, result.StructuralOperationCount);
        Assert.Equal(2, result.CommandCount);
        Assert.Equal(2, result.StatementCount);
        Assert.Equal(
            0,
            await CountTablesAsync(scratch, createdTable));
        Assert.Equal(ConnectionState.Open, scratch.State);
    }

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

    private static async ValueTask<CSharpDbConnection>
        OpenScratchAsync()
    {
        var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false");
        await connection.OpenAsync(Ct);
        return connection;
    }

    private static ValueTask<int> CountTablesAsync(
        CSharpDbConnection connection,
        string table) =>
        ExecuteScalarIntAsync(
            connection,
            $"SELECT COUNT(*) FROM sys.tables " +
            $"WHERE table_name = '{table}'");

    private static async ValueTask<int> ExecuteScalarIntAsync(
        CSharpDbConnection connection,
        string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        object? value = await command.ExecuteScalarAsync(Ct);
        return Convert.ToInt32(value, CultureInfo.InvariantCulture);
    }

    private sealed class EmptyTargetContext : DbContext
    {
        protected override void OnConfiguring(
            DbContextOptionsBuilder optionsBuilder) =>
            optionsBuilder.UseCSharpDb(
                "Data Source=selected-context-must-not-open.db");
    }

    private sealed class SeededTargetContext(
        string connectionString) : DbContext
    {
        protected override void OnConfiguring(
            DbContextOptionsBuilder optionsBuilder) =>
            optionsBuilder.UseCSharpDb(connectionString);

        protected override void OnModelCreating(
            ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<TargetWidget>(
                entity =>
                {
                    entity.ToTable("TargetWidgets");
                    entity.HasKey(static widget => widget.Id);
                    entity.Property(static widget => widget.Name)
                        .HasMaxLength(100)
                        .IsRequired();
                    entity.HasData(
                        new TargetWidget
                        {
                            Id = 1,
                            Name = "seed-row",
                        });
                });
        }
    }

    private sealed class TargetWidget
    {
        public long Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class DerivedInsertDataOperation :
        InsertDataOperation;

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

    private sealed class RejectingSqlGenerator :
        IMigrationsSqlGenerator
    {
        internal bool WasCalled { get; private set; }

        public IReadOnlyList<MigrationCommand> Generate(
            IReadOnlyList<MigrationOperation> operations,
            IModel? model = null,
            MigrationsSqlGenerationOptions options =
                MigrationsSqlGenerationOptions.Default)
        {
            WasCalled = true;
            throw new InvalidOperationException(
                "The generator must not be called.");
        }
    }
}
