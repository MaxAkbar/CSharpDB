using System.Text;
using CSharpDB.EntityFrameworkCore.Infrastructure.Internal;
using CSharpDB.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore;
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

        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Table name");
        int rowVersionColumnCount =
            operation.Columns.Count(static column => column.IsRowVersion);
        if (rowVersionColumnCount > 1)
        {
            throw Unsupported(
                $"more than one rowversion column on table '{operation.Name}'");
        }

        foreach (AddForeignKeyOperation foreignKey in operation.ForeignKeys)
            ValidateForeignKey(foreignKey);

        var definitions = new List<string>(
            operation.Columns.Count
            + operation.CheckConstraints.Count
            + operation.UniqueConstraints.Count
            + operation.ForeignKeys.Count);
        foreach (AddColumnOperation column in operation.Columns)
            definitions.Add(
                BuildColumnDefinition(
                    operation,
                    column,
                    foreignKey: null,
                    model));

        foreach (AddCheckConstraintOperation checkConstraint in operation.CheckConstraints)
            definitions.Add(BuildCheckConstraintDefinition(checkConstraint));

        foreach (AddUniqueConstraintOperation uniqueConstraint in operation.UniqueConstraints)
            definitions.Add(BuildUniqueConstraintDefinition(uniqueConstraint));

        foreach (AddForeignKeyOperation foreignKey in operation.ForeignKeys)
            definitions.Add(BuildForeignKeyDefinition(foreignKey));

        if (operation.PrimaryKey is { } primaryKey)
            definitions.Add(BuildPrimaryKeyDefinition(primaryKey));

        builder.Append("CREATE TABLE ")
            .Append(QuoteIdentifier(operation.Name))
            .AppendLine(" (");

        using (builder.Indent())
        {
            for (int i = 0; i < definitions.Count; i++)
            {
                builder.Append(definitions[i]);
                builder.AppendLine(i == definitions.Count - 1 ? string.Empty : ",");
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
            .Append(QuoteIdentifier(operation.Name));

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
            .Append(QuoteIdentifier(operation.Name))
            .Append(" RENAME TO ")
            .Append(QuoteIdentifier(newTableName));

        EndCommand(builder, terminate: true);
    }

    protected override void Generate(AddColumnOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
    {
        ValidateColumnOperation(operation);

        builder.Append("ALTER TABLE ")
            .Append(QuoteIdentifier(operation.Table))
            .Append(" ADD COLUMN ")
            .Append(
                BuildColumnDefinition(
                    createTable: null,
                    operation,
                    foreignKey: null,
                    model));

        EndCommand(builder, terminate);
    }

    protected override void Generate(RenameColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        ValidateNoSchema(operation.Schema, operation.Table);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Table, "Table name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Column name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.NewName, "Column name");

        builder.Append("ALTER TABLE ")
            .Append(QuoteIdentifier(operation.Table))
            .Append(" RENAME COLUMN ")
            .Append(QuoteIdentifier(operation.Name))
            .Append(" TO ")
            .Append(QuoteIdentifier(operation.NewName));

        EndCommand(builder, terminate: true);
    }

    protected override void Generate(DropColumnOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
    {
        ValidateNoSchema(operation.Schema, operation.Table);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Table, "Table name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Column name");

        builder.Append("ALTER TABLE ")
            .Append(QuoteIdentifier(operation.Table))
            .Append(" DROP COLUMN ")
            .Append(QuoteIdentifier(operation.Name));

        EndCommand(builder, terminate);
    }

    protected override void Generate(CreateIndexOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
    {
        ValidateNoSchema(operation.Schema, operation.Table);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Index name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Table, "Table name");

        if (operation.Columns.Length == 0)
            throw new InvalidOperationException("CreateIndex operations require at least one column.");
        if (operation.Filter is not null)
            throw Unsupported("filtered indexes");
        if (operation.IsDescending?.Any(static descending => descending) == true)
            throw Unsupported("descending indexes");

        builder.Append("CREATE ");
        if (operation.IsUnique)
            builder.Append("UNIQUE ");

        builder.Append("INDEX ")
            .Append(QuoteIdentifier(operation.Name))
            .Append(" ON ")
            .Append(QuoteIdentifier(operation.Table))
            .Append(" (")
            .Append(string.Join(", ", operation.Columns.Select(QuoteIdentifier)))
            .Append(")");

        EndCommand(builder, terminate);
    }

    protected override void Generate(DropIndexOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
    {
        ValidateNoSchema(operation.Schema, operation.Table ?? operation.Name);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Index name");

        builder.Append("DROP INDEX ")
            .Append(QuoteIdentifier(operation.Name));

        EndCommand(builder, terminate);
    }

    protected override void Generate(EnsureSchemaOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw Unsupported("schemas");

    protected override void Generate(DropSchemaOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw Unsupported("schemas");

    protected override void Generate(AlterColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        PopulateOldColumnIdentity(operation);
        ValidateColumnOperation(operation);
        ValidateColumnOperation(operation.OldColumn);

        string storeType = operation.ColumnType
            ?? GetColumnType(operation.Schema, operation.Table, operation.Name, operation, model);
        string oldStoreType = operation.OldColumn.ColumnType
            ?? GetColumnType(
                operation.OldColumn.Schema,
                operation.OldColumn.Table,
                operation.OldColumn.Name,
                operation.OldColumn,
                model: null);

        bool targetIsDecimal =
            TryGetProviderOwnedDecimalFacets(
                operation,
                storeType,
                model,
                out int precision,
                out int scale);
        bool oldIsDecimal =
            TryGetProviderOwnedDecimalFacets(
                operation.OldColumn,
                oldStoreType,
                model: null,
                out int oldPrecision,
                out int oldScale);
        ValidateDecimalColumnOperation(
            operation,
            storeType,
            model);
        ValidateDecimalColumnOperation(
            operation.OldColumn,
            oldStoreType,
            model: null);

        if (targetIsDecimal != oldIsDecimal)
        {
            throw Unsupported(
                $"changing column '{operation.Table}.{operation.Name}' into or out of provider-owned scaled decimal storage without an explicit data rewrite");
        }

        if (targetIsDecimal)
        {
            if (precision != oldPrecision ||
                scale != oldScale)
            {
                throw Unsupported(
                    $"changing CSharpDB decimal precision or scale on column '{operation.Table}.{operation.Name}' from decimal({oldPrecision}, {oldScale}) to decimal({precision}, {scale}) without a scaled-integer data rewrite");
            }
        }

        bool typeChanged = !string.Equals(storeType, oldStoreType, StringComparison.OrdinalIgnoreCase);
        if (typeChanged && !IsSupportedNumericTypeRewrite(oldStoreType, storeType))
        {
            throw Unsupported(
                $"changing column '{operation.Table}.{operation.Name}' from store type '{oldStoreType}' to '{storeType}'. " +
                "The bounded rewrite path supports only exact INTEGER-to-REAL and REAL-to-INTEGER conversions on dependency-free columns");
        }

        bool collationChanged = !CollationsSemanticallyEqual(
            operation.Collation,
            operation.OldColumn.Collation);
        if (collationChanged &&
            (!IsStoreType(storeType, "TEXT") || !IsStoreType(oldStoreType, "TEXT")))
        {
            throw Unsupported(
                $"changing the collation of non-TEXT column '{operation.Table}.{operation.Name}'");
        }

        bool defaultChanged = !Equals(operation.DefaultValue, operation.OldColumn.DefaultValue);
        bool nullabilityChanged = operation.IsNullable != operation.OldColumn.IsNullable;
        if (!typeChanged && !collationChanged && !defaultChanged && !nullabilityChanged)
            return;

        string table = QuoteIdentifier(operation.Table);
        string column = QuoteIdentifier(operation.Name);

        // A REAL-to-INTEGER rewrite cannot validate a REAL default against the
        // target schema. Remove the old default before the rewrite and restore
        // the target default afterwards. EF migrations execute these commands
        // in the surrounding migration transaction.
        if (typeChanged && operation.OldColumn.DefaultValue is not null)
            AppendDropDefault(builder, table, column);

        if (typeChanged)
        {
            builder.Append("ALTER TABLE ")
                .Append(table)
                .Append(" ALTER COLUMN ")
                .Append(column)
                .Append(" TYPE ")
                .Append(storeType);
            EndCommand(builder, terminate: true);
        }

        if (typeChanged)
        {
            if (operation.DefaultValue is not null)
                AppendSetDefault(builder, table, column, operation.DefaultValue);
        }
        else if (defaultChanged)
        {
            if (operation.DefaultValue is null)
                AppendDropDefault(builder, table, column);
            else
                AppendSetDefault(builder, table, column, operation.DefaultValue);
        }

        if (collationChanged)
        {
            builder.Append("ALTER TABLE ")
                .Append(table)
                .Append(" ALTER COLUMN ")
                .Append(column);

            string? targetCollation = NormalizeBinaryCollation(operation.Collation);
            if (targetCollation is null)
                builder.Append(" DROP COLLATION");
            else
                builder.Append(" SET COLLATION ").Append(QuoteIdentifier(targetCollation));

            EndCommand(builder, terminate: true);
        }

        if (nullabilityChanged)
        {
            builder.Append("ALTER TABLE ")
                .Append(table)
                .Append(" ALTER COLUMN ")
                .Append(column)
                .Append(operation.IsNullable ? " DROP NOT NULL" : " SET NOT NULL");
            EndCommand(builder, terminate: true);
        }
    }

    protected override void Generate(AddForeignKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
    {
        builder.Append("ALTER TABLE ")
            .Append(QuoteIdentifier(operation.Table))
            .Append(" ADD ")
            .Append(BuildForeignKeyDefinition(operation));
        EndCommand(builder, terminate);
    }

    protected override void Generate(DropForeignKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
    {
        ValidateNoSchema(operation.Schema, operation.Table);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Table, "Table name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Foreign key name");

        builder.Append("ALTER TABLE ")
            .Append(QuoteIdentifier(operation.Table))
            .Append(" DROP CONSTRAINT ")
            .Append(QuoteIdentifier(operation.Name));
        EndCommand(builder, terminate);
    }

    protected override void Generate(AddPrimaryKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
    {
        if (model is not null)
        {
            ITable? table = model
                .GetRelationalModel()
                .FindTable(
                    operation.Table,
                    operation.Schema);
            if (operation.Columns.Any(column =>
                    table?.FindColumn(column)?
                        .StoreTypeMapping.Converter is
                        CSharpDbDecimalToInt64Converter))
            {
                throw Unsupported(
                    $"provider-owned scaled decimal primary key on table '{operation.Table}'");
            }
        }

        builder.Append("ALTER TABLE ")
            .Append(QuoteIdentifier(operation.Table))
            .Append(" ADD ")
            .Append(BuildPrimaryKeyDefinition(operation));
        EndCommand(builder, terminate);
    }

    protected override void Generate(DropPrimaryKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
    {
        ValidateNoSchema(operation.Schema, operation.Table);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Table, "Table name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Primary key name");

        builder.Append("ALTER TABLE ")
            .Append(QuoteIdentifier(operation.Table))
            .Append(" DROP CONSTRAINT ")
            .Append(QuoteIdentifier(operation.Name));
        EndCommand(builder, terminate);
    }

    protected override void Generate(AddUniqueConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        string definition = BuildUniqueConstraintDefinition(operation);
        builder.Append("ALTER TABLE ")
            .Append(QuoteIdentifier(operation.Table))
            .Append(" ADD ")
            .Append(definition);
        EndCommand(builder, terminate: true);
    }

    protected override void Generate(DropUniqueConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        ValidateNoSchema(operation.Schema, operation.Table);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Table, "Table name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Unique constraint name");

        builder.Append("ALTER TABLE ")
            .Append(QuoteIdentifier(operation.Table))
            .Append(" DROP CONSTRAINT ")
            .Append(QuoteIdentifier(operation.Name));
        EndCommand(builder, terminate: true);
    }

    protected override void Generate(AddCheckConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        string definition = BuildCheckConstraintDefinition(operation);
        builder.Append("ALTER TABLE ")
            .Append(QuoteIdentifier(operation.Table))
            .Append(" ADD ")
            .Append(definition);
        EndCommand(builder, terminate: true);
    }

    protected override void Generate(DropCheckConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        ValidateNoSchema(operation.Schema, operation.Table);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Table, "Table name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Constraint name");

        builder.Append("ALTER TABLE ")
            .Append(QuoteIdentifier(operation.Table))
            .Append(" DROP CONSTRAINT ")
            .Append(QuoteIdentifier(operation.Name));
        EndCommand(builder, terminate: true);
    }

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
    {
        string table = operation.Table
            ?? throw new InvalidOperationException("RenameIndex operations require a table name.");

        ValidateNoSchema(operation.Schema, table);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(table, "Table name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Index name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.NewName, "New index name");

        builder.Append("ALTER TABLE ")
            .Append(QuoteIdentifier(table))
            .Append(" RENAME INDEX ")
            .Append(QuoteIdentifier(operation.Name))
            .Append(" TO ")
            .Append(QuoteIdentifier(operation.NewName));

        EndCommand(builder, terminate: true);
    }

    protected override void ColumnDefinition(
        string? schema,
        string table,
        string name,
        ColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
        => builder.Append(
            BuildColumnDefinition(
                createTable: null,
                operation,
                foreignKey: null,
                model));

    private string BuildColumnDefinition(
        CreateTableOperation? createTable,
        ColumnOperation operation,
        AddForeignKeyOperation? foreignKey,
        IModel? model)
    {
        ValidateColumnOperation(
            operation,
            allowInitialCreateTableRowVersion:
                createTable is not null);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Column name");

        string storeType = operation.ColumnType ?? GetColumnType(operation.Schema, operation.Table, operation.Name, operation, model: null);
        string[]? primaryKeyColumns =
            createTable?.PrimaryKey?.Columns;
        bool isPrimaryKeyColumn =
            primaryKeyColumns?.Contains(
                operation.Name,
                StringComparer.OrdinalIgnoreCase) ==
            true;
        ValidateDecimalColumnOperation(
            operation,
            storeType,
            model);
        if (operation.IsRowVersion)
        {
            ValidateInitialCreateTableRowVersion(
                createTable!,
                operation,
                storeType);
        }

        if (isPrimaryKeyColumn &&
            TryGetProviderOwnedDecimalFacets(
                operation,
                storeType,
                model,
                out _,
                out _))
        {
            throw Unsupported(
                $"provider-owned scaled decimal primary key column '{operation.Table}.{operation.Name}'");
        }

        var builder = new StringBuilder();
        builder.Append(QuoteIdentifier(operation.Name))
            .Append(' ')
            .Append(storeType);

        if (operation.IsRowVersion)
            builder.Append(" ROWVERSION");

        string? collation = NormalizeBinaryCollation(operation.Collation);
        if (collation is not null)
        {
            if (!IsStoreType(storeType, "TEXT"))
            {
                throw Unsupported(
                    $"collation '{collation}' on non-TEXT column '{operation.Table}.{operation.Name}'");
            }

            builder.Append(" COLLATE ")
                .Append(QuoteIdentifier(collation));
        }

        if (!operation.IsNullable)
        {
            builder.Append(" NOT NULL");
        }

        if (operation.DefaultValue is not null)
        {
            builder.Append(" DEFAULT ")
                .Append(GenerateLiteral(operation.DefaultValue));
        }

        if (foreignKey is not null)
        {
            ValidateForeignKey(foreignKey);
            string principalTable = foreignKey.PrincipalTable
                ?? throw new InvalidOperationException("Foreign key operations require a principal table name.");
            string principalColumn = foreignKey.PrincipalColumns![0];

            builder.Append(" REFERENCES ")
                .Append(QuoteIdentifier(principalTable))
                .Append(" (")
                .Append(QuoteIdentifier(principalColumn))
                .Append(")");

            if (foreignKey.OnDelete == ReferentialAction.Cascade)
                builder.Append(" ON DELETE CASCADE");
        }

        return builder.ToString();
    }

    private string BuildCheckConstraintDefinition(AddCheckConstraintOperation operation)
    {
        ValidateNoSchema(operation.Schema, operation.Table);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Table, "Table name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Constraint name");

        if (string.IsNullOrWhiteSpace(operation.Sql))
            throw new InvalidOperationException($"Check constraint '{operation.Name}' requires a SQL expression.");

        return $"CONSTRAINT {QuoteIdentifier(operation.Name)} CHECK ({operation.Sql})";
    }

    private string BuildPrimaryKeyDefinition(AddPrimaryKeyOperation operation)
    {
        ValidateNoSchema(operation.Schema, operation.Table);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Table, "Table name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Primary key name");

        if (operation.Columns.Length == 0)
            throw new InvalidOperationException("Primary-key definitions require at least one column.");

        foreach (string column in operation.Columns)
            CSharpDbProviderValidation.ValidateSimpleIdentifier(column, "Column name");

        return $"CONSTRAINT {QuoteIdentifier(operation.Name)} PRIMARY KEY ({string.Join(", ", operation.Columns.Select(QuoteIdentifier))})";
    }

    private string BuildUniqueConstraintDefinition(AddUniqueConstraintOperation operation)
    {
        ValidateNoSchema(operation.Schema, operation.Table);
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Table, "Table name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Unique constraint name");
        if (operation.Columns.Length == 0)
            throw new InvalidOperationException("Unique constraints require at least one column.");

        foreach (string column in operation.Columns)
            CSharpDbProviderValidation.ValidateSimpleIdentifier(column, "Column name");

        return $"CONSTRAINT {QuoteIdentifier(operation.Name)} UNIQUE ({string.Join(", ", operation.Columns.Select(QuoteIdentifier))})";
    }

    private string BuildForeignKeyDefinition(AddForeignKeyOperation operation)
    {
        ValidateForeignKey(operation);
        string principalTable = operation.PrincipalTable!;

        var builder = new StringBuilder();
        builder.Append("CONSTRAINT ")
            .Append(QuoteIdentifier(operation.Name))
            .Append(" FOREIGN KEY (")
            .Append(string.Join(", ", operation.Columns.Select(QuoteIdentifier)))
            .Append(") REFERENCES ")
            .Append(QuoteIdentifier(principalTable))
            .Append(" (")
            .Append(string.Join(", ", operation.PrincipalColumns!.Select(QuoteIdentifier)))
            .Append(")");

        if (operation.OnDelete == ReferentialAction.Cascade)
            builder.Append(" ON DELETE CASCADE");

        return builder.ToString();
    }

    private string GenerateLiteral(object value)
    {
        var mapping = Dependencies.TypeMappingSource.FindMapping(value.GetType())
            ?? throw new NotSupportedException(
                $"The CSharpDB EF Core provider cannot generate a literal DEFAULT for CLR type '{value.GetType().Name}'.");

        return mapping.GenerateSqlLiteral(value);
    }

    private void AppendDropDefault(
        MigrationCommandListBuilder builder,
        string table,
        string column)
    {
        builder.Append("ALTER TABLE ")
            .Append(table)
            .Append(" ALTER COLUMN ")
            .Append(column)
            .Append(" DROP DEFAULT");
        EndCommand(builder, terminate: true);
    }

    private void AppendSetDefault(
        MigrationCommandListBuilder builder,
        string table,
        string column,
        object value)
    {
        builder.Append("ALTER TABLE ")
            .Append(table)
            .Append(" ALTER COLUMN ")
            .Append(column)
            .Append(" SET DEFAULT ")
            .Append(GenerateLiteral(value));
        EndCommand(builder, terminate: true);
    }

    private static bool IsSupportedNumericTypeRewrite(string oldStoreType, string storeType) =>
        IsStoreType(oldStoreType, "INTEGER") && IsStoreType(storeType, "REAL") ||
        IsStoreType(oldStoreType, "REAL") && IsStoreType(storeType, "INTEGER");

    private static bool IsStoreType(string storeType, string expected) =>
        string.Equals(storeType.Trim(), expected, StringComparison.OrdinalIgnoreCase);

    private static bool TryGetProviderOwnedDecimalFacets(
        ColumnOperation operation,
        string storeType,
        IModel? model,
        out int precision,
        out int scale)
    {
        precision = 0;
        scale = 0;
        if (!IsStoreType(storeType, "INTEGER"))
            return false;

        if (TryParseDecimalStorageAnnotation(
                operation,
                out precision,
                out scale))
        {
            return true;
        }

        IColumn? mappedColumn = model?
            .GetRelationalModel()
            .FindTable(
                operation.Table,
                operation.Schema)?
            .FindColumn(operation.Name);
        if (mappedColumn is not null)
        {
            if (mappedColumn.StoreTypeMapping.Converter is not
                CSharpDbDecimalToInt64Converter converter)
            {
                return false;
            }

            precision = converter.Precision;
            scale = converter.Scale;
            return true;
        }

        Type clrType = Nullable.GetUnderlyingType(
                operation.ClrType) ??
            operation.ClrType;
        if (clrType != typeof(decimal))
            return false;

        (precision, scale) =
            CSharpDbDecimalStorage.ResolveFacets(
                operation.Precision,
                operation.Scale);
        return true;
    }

    private static bool TryParseDecimalStorageAnnotation(
        ColumnOperation operation,
        out int precision,
        out int scale)
    {
        precision = 0;
        scale = 0;
        if (operation.FindAnnotation(
                CSharpDbAnnotationNames.DecimalStorage)?
            .Value is not string encoded)
        {
            return false;
        }

        string[] facets = encoded.Split(',');
        if (facets.Length != 2 ||
            !int.TryParse(
                facets[0],
                System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture,
                out precision) ||
            !int.TryParse(
                facets[1],
                System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture,
                out scale))
        {
            throw new InvalidOperationException(
                $"Invalid CSharpDB decimal storage annotation '{encoded}' on column '{operation.Table}.{operation.Name}'.");
        }

        CSharpDbDecimalStorage.ValidateFacets(
            precision,
            scale);
        return true;
    }

    private static void ValidateDecimalColumnOperation(
        ColumnOperation operation,
        string storeType,
        IModel? model)
    {
        if (!TryGetProviderOwnedDecimalFacets(
                operation,
                storeType,
                model,
                out _,
                out _))
        {
            return;
        }

        if (operation.DefaultValue is not null)
        {
            throw Unsupported(
                $"literal defaults for provider-owned scaled decimal column '{operation.Table}.{operation.Name}'");
        }
    }

    private static bool CollationsSemanticallyEqual(string? left, string? right) =>
        string.Equals(
            NormalizeBinaryCollation(left),
            NormalizeBinaryCollation(right),
            StringComparison.OrdinalIgnoreCase);

    private static string? NormalizeBinaryCollation(string? collation)
    {
        string? normalized = string.IsNullOrWhiteSpace(collation)
            ? null
            : collation.Trim();
        return string.Equals(normalized, "BINARY", StringComparison.OrdinalIgnoreCase)
            ? null
            : normalized;
    }

    private static void ValidateForeignKey(AddForeignKeyOperation operation)
    {
        string principalTable = operation.PrincipalTable
            ?? throw new InvalidOperationException("Foreign key operations require a principal table name.");
        string[] principalColumns = operation.PrincipalColumns
            ?? throw new InvalidOperationException("Foreign key operations require principal columns.");

        ValidateNoSchema(operation.Schema, operation.Table);
        ValidateNoSchema(operation.PrincipalSchema, principalTable);

        if (operation.Columns.Length == 0 || operation.Columns.Length != principalColumns.Length)
            throw new InvalidOperationException("Foreign keys require equal, non-empty child and principal column lists.");
        if (operation.OnUpdate != ReferentialAction.NoAction)
            throw Unsupported("ON UPDATE actions");
        if (operation.OnDelete is not ReferentialAction.NoAction and not ReferentialAction.Restrict and not ReferentialAction.Cascade)
            throw Unsupported($"ON DELETE action '{operation.OnDelete}'");

        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Table, "Table name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Foreign key name");
        foreach (string column in operation.Columns)
            CSharpDbProviderValidation.ValidateSimpleIdentifier(column, "Column name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(principalTable, "Table name");
        foreach (string principalColumn in principalColumns)
            CSharpDbProviderValidation.ValidateSimpleIdentifier(principalColumn, "Column name");
    }

    private static void ValidateColumnOperation(
        ColumnOperation operation,
        bool allowInitialCreateTableRowVersion = false)
    {
        ValidateNoSchema(operation.Schema, operation.Table);

        if (operation.DefaultValueSql is not null)
            throw Unsupported("DefaultValueSql");
        if (operation.ComputedColumnSql is not null)
            throw Unsupported("computed columns");
        if (operation.IsRowVersion &&
            !allowInitialCreateTableRowVersion)
        {
            throw Unsupported(
                $"adding or altering rowversion column '{operation.Table}.{operation.Name}'. Rowversion columns are supported only as part of an initial CREATE TABLE operation");
        }

        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Table, "Table name");
        CSharpDbProviderValidation.ValidateSimpleIdentifier(operation.Name, "Column name");
    }

    private static void ValidateInitialCreateTableRowVersion(
        CreateTableOperation createTable,
        ColumnOperation operation,
        string storeType)
    {
        if (operation.ClrType != typeof(byte[]) ||
            operation.IsNullable ||
            !IsStoreType(
                storeType,
                "BLOB"))
        {
            throw Unsupported(
                $"rowversion column '{operation.Table}.{operation.Name}' outside the required non-nullable byte[]/BLOB shape");
        }

        if (operation.DefaultValue is not null ||
            operation.DefaultValueSql is not null ||
            operation.ComputedColumnSql is not null)
        {
            throw Unsupported(
                $"defaults or computed SQL on rowversion column '{operation.Table}.{operation.Name}'");
        }

        if (createTable.PrimaryKey?.Columns.Contains(
                operation.Name,
                StringComparer.OrdinalIgnoreCase) ==
            true ||
            createTable.UniqueConstraints.Any(constraint =>
                constraint.Columns.Contains(
                    operation.Name,
                    StringComparer.OrdinalIgnoreCase)))
        {
            throw Unsupported(
                $"key or unique-constraint participation by rowversion column '{operation.Table}.{operation.Name}'");
        }

        if (createTable.ForeignKeys.Any(foreignKey =>
                foreignKey.Columns.Contains(
                    operation.Name,
                    StringComparer.OrdinalIgnoreCase)))
        {
            throw Unsupported(
                $"foreign-key participation by rowversion column '{operation.Table}.{operation.Name}'");
        }
    }

    private static void PopulateOldColumnIdentity(
        AlterColumnOperation operation)
    {
        if (string.IsNullOrWhiteSpace(
                operation.OldColumn.Table))
        {
            operation.OldColumn.Table =
                operation.Table;
        }

        if (string.IsNullOrWhiteSpace(
                operation.OldColumn.Name))
        {
            operation.OldColumn.Name =
                operation.Name;
        }

        operation.OldColumn.Schema ??=
            operation.Schema;
    }

    private static void ValidateNoSchema(string? schema, string objectName)
    {
        if (!string.IsNullOrWhiteSpace(schema))
            throw new NotSupportedException($"Schemas are not supported by the CSharpDB EF Core provider. '{schema}.{objectName}' is not valid.");
    }

    private static NotSupportedException Unsupported(string feature)
        => new($"The CSharpDB EF Core provider does not support {feature} in v1.");

    private string QuoteIdentifier(string identifier)
        => Dependencies.SqlGenerationHelper.DelimitIdentifier(identifier);

    private static void EndCommand(MigrationCommandListBuilder builder, bool terminate, bool suppressTransaction = false)
    {
        if (!terminate)
            return;

        builder.AppendLine(";");
        builder.EndCommand(suppressTransaction);
    }
}
