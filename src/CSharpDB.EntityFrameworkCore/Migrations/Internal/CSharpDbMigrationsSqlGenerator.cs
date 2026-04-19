using System.Text;
using CSharpDB.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace CSharpDB.EntityFrameworkCore.Migrations.Internal;

public sealed class CSharpDbMigrationsSqlGenerator : MigrationsSqlGenerator
{
    public CSharpDbMigrationsSqlGenerator(MigrationsSqlGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    protected override void Generate(CreateTableOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
    {
        ValidateNoSchema(operation.Schema, operation.Name);

        if (operation.PrimaryKey?.Columns is { Length: > 1 })
            throw Unsupported("composite primary keys");
        if (operation.UniqueConstraints.Count > 0)
            throw Unsupported("unique constraints");
        if (operation.CheckConstraints.Count > 0)
            throw Unsupported("check constraints");

        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Table name");

        builder.Append("CREATE TABLE ")
            .Append(operation.Name)
            .AppendLine(" (");

        using (builder.Indent())
        {
            for (int i = 0; i < operation.Columns.Count; i++)
            {
                AddColumnOperation column = operation.Columns[i];
                AddForeignKeyOperation? foreignKey = operation.ForeignKeys.SingleOrDefault(fk =>
                    fk.Columns.Length == 1
                    && string.Equals(fk.Columns[0], column.Name, StringComparison.OrdinalIgnoreCase));

                builder.Append(BuildColumnDefinition(operation, column, foreignKey));
                builder.AppendLine(i == operation.Columns.Count - 1 ? string.Empty : ",");
            }
        }

        builder.Append(")");
        EndCommand(builder, terminate);
    }

    protected override void Generate(DropTableOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
    {
        ValidateNoSchema(operation.Schema, operation.Name);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Table name");

        builder.Append("DROP TABLE ")
            .Append(operation.Name);

        EndCommand(builder, terminate);
    }

    protected override void Generate(RenameTableOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        string newTableName = operation.NewName
            ?? throw new InvalidOperationException("RenameTable operations require a new table name.");

        ValidateNoSchema(operation.Schema, operation.Name);
        ValidateNoSchema(operation.NewSchema, newTableName);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Table name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(newTableName, "Table name");

        builder.Append("ALTER TABLE ")
            .Append(operation.Name)
            .Append(" RENAME TO ")
            .Append(newTableName);

        EndCommand(builder, terminate: true);
    }

    protected override void Generate(AddColumnOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
    {
        ValidateColumnOperation(operation);

        builder.Append("ALTER TABLE ")
            .Append(operation.Table)
            .Append(" ADD COLUMN ")
            .Append(BuildColumnDefinition(createTable: null, operation, foreignKey: null));

        EndCommand(builder, terminate);
    }

    protected override void Generate(RenameColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        ValidateNoSchema(operation.Schema, operation.Table);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Table, "Table name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Column name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.NewName, "Column name");

        builder.Append("ALTER TABLE ")
            .Append(operation.Table)
            .Append(" RENAME COLUMN ")
            .Append(operation.Name)
            .Append(" TO ")
            .Append(operation.NewName);

        EndCommand(builder, terminate: true);
    }

    protected override void Generate(DropColumnOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
    {
        ValidateNoSchema(operation.Schema, operation.Table);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Table, "Table name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Column name");

        builder.Append("ALTER TABLE ")
            .Append(operation.Table)
            .Append(" DROP COLUMN ")
            .Append(operation.Name);

        EndCommand(builder, terminate);
    }

    protected override void Generate(CreateIndexOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
    {
        ValidateNoSchema(operation.Schema, operation.Table);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Index name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Table, "Table name");

        if (operation.Columns.Length != 1)
            throw Unsupported("multi-column indexes");
        if (operation.Filter is not null)
            throw Unsupported("filtered indexes");
        if (operation.IsDescending?.Any(static descending => descending) == true)
            throw Unsupported("descending indexes");

        builder.Append("CREATE ");
        if (operation.IsUnique)
            builder.Append("UNIQUE ");

        builder.Append("INDEX ")
            .Append(operation.Name)
            .Append(" ON ")
            .Append(operation.Table)
            .Append(" (")
            .Append(operation.Columns[0])
            .Append(")");

        EndCommand(builder, terminate);
    }

    protected override void Generate(DropIndexOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
    {
        ValidateNoSchema(operation.Schema, operation.Table ?? operation.Name);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Index name");

        builder.Append("DROP INDEX ")
            .Append(operation.Name);

        EndCommand(builder, terminate);
    }

    protected override void Generate(EnsureSchemaOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw Unsupported("schemas");

    protected override void Generate(DropSchemaOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw Unsupported("schemas");

    protected override void Generate(AlterColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw Unsupported("AlterColumn");

    protected override void Generate(AddForeignKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
        => throw Unsupported("standalone foreign key changes");

    protected override void Generate(DropForeignKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
        => throw Unsupported("standalone foreign key changes");

    protected override void Generate(AddPrimaryKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
        => throw Unsupported("standalone primary key changes");

    protected override void Generate(DropPrimaryKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
        => throw Unsupported("standalone primary key changes");

    protected override void Generate(AddUniqueConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw Unsupported("unique constraints");

    protected override void Generate(DropUniqueConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw Unsupported("unique constraints");

    protected override void Generate(AddCheckConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw Unsupported("check constraints");

    protected override void Generate(DropCheckConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw Unsupported("check constraints");

    protected override void Generate(CreateSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw Unsupported("sequences");

    protected override void Generate(AlterSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw Unsupported("sequences");

    protected override void Generate(RenameSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw Unsupported("sequences");

    protected override void Generate(DropSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw Unsupported("sequences");

    protected override void Generate(RestartSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw Unsupported("sequences");

    protected override void Generate(RenameIndexOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw Unsupported("RenameIndex");

    protected override void ColumnDefinition(
        string? schema,
        string table,
        string name,
        ColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
        => builder.Append(BuildColumnDefinition(createTable: null, operation, foreignKey: null));

    private string BuildColumnDefinition(
        CreateTableOperation? createTable,
        ColumnOperation operation,
        AddForeignKeyOperation? foreignKey)
    {
        ValidateColumnOperation(operation);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Column name");

        string storeType = operation.ColumnType ?? GetColumnType(operation.Schema, operation.Table, operation.Name, operation, model: null);
        bool isPrimaryKeyColumn = createTable?.PrimaryKey?.Columns is { Length: 1 } pkColumns
            && string.Equals(pkColumns[0], operation.Name, StringComparison.OrdinalIgnoreCase);
        bool isIdentityColumn = isPrimaryKeyColumn && string.Equals(storeType, "INTEGER", StringComparison.OrdinalIgnoreCase);

        var builder = new StringBuilder();
        builder.Append(operation.Name)
            .Append(' ')
            .Append(storeType);

        if (isPrimaryKeyColumn)
        {
            builder.Append(" PRIMARY KEY");
            if (isIdentityColumn)
                builder.Append(" IDENTITY");
        }
        else if (!operation.IsNullable)
        {
            builder.Append(" NOT NULL");
        }

        if (foreignKey is not null)
        {
            ValidateForeignKey(foreignKey);
            string principalTable = foreignKey.PrincipalTable
                ?? throw new InvalidOperationException("Foreign key operations require a principal table name.");
            string principalColumn = foreignKey.PrincipalColumns![0];

            builder.Append(" REFERENCES ")
                .Append(principalTable)
                .Append(" (")
                .Append(principalColumn)
                .Append(")");

            if (foreignKey.OnDelete == ReferentialAction.Cascade)
                builder.Append(" ON DELETE CASCADE");
        }

        return builder.ToString();
    }

    private static void ValidateForeignKey(AddForeignKeyOperation operation)
    {
        string principalTable = operation.PrincipalTable
            ?? throw new InvalidOperationException("Foreign key operations require a principal table name.");
        string[] principalColumns = operation.PrincipalColumns
            ?? throw new InvalidOperationException("Foreign key operations require principal columns.");

        ValidateNoSchema(operation.Schema, operation.Table);
        ValidateNoSchema(operation.PrincipalSchema, principalTable);

        if (operation.Columns.Length != 1 || principalColumns.Length != 1)
            throw Unsupported("composite foreign keys");
        if (operation.OnUpdate != ReferentialAction.NoAction)
            throw Unsupported("ON UPDATE actions");
        if (operation.OnDelete is not ReferentialAction.NoAction and not ReferentialAction.Restrict and not ReferentialAction.Cascade)
            throw Unsupported($"ON DELETE action '{operation.OnDelete}'");

        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Table, "Table name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Columns[0], "Column name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(principalTable, "Table name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(principalColumns[0], "Column name");
    }

    private static void ValidateColumnOperation(ColumnOperation operation)
    {
        ValidateNoSchema(operation.Schema, operation.Table);

        if (operation.DefaultValue is not null)
            throw Unsupported("default values");
        if (operation.DefaultValueSql is not null)
            throw Unsupported("DefaultValueSql");
        if (operation.ComputedColumnSql is not null)
            throw Unsupported("computed columns");
        if (operation.IsRowVersion)
            throw Unsupported("rowversion columns");

        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Table, "Table name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Column name");
    }

    private static void ValidateNoSchema(string? schema, string objectName)
    {
        if (!string.IsNullOrWhiteSpace(schema))
            throw new NotSupportedException($"Schemas are not supported by the CSharpDB EF Core provider. '{schema}.{objectName}' is not valid.");
    }

    private static NotSupportedException Unsupported(string feature)
        => new($"The CSharpDB EF Core provider does not support {feature} in v1.");

    private static void EndCommand(MigrationCommandListBuilder builder, bool terminate, bool suppressTransaction = false)
    {
        if (!terminate)
            return;

        builder.AppendLine(";");
        builder.EndCommand(suppressTransaction);
    }
}
