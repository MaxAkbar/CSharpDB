using CSharpDB.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace CSharpDB.EntityFrameworkCore.Infrastructure.Internal;

public sealed class CSharpDbModelValidator : RelationalModelValidator
{
    public CSharpDbModelValidator(
        ModelValidatorDependencies dependencies,
        RelationalModelValidatorDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    public override void Validate(IModel model, IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger)
    {
        ValidateRowVersionModel(model);
        base.Validate(model, logger);

        if (model.GetSequences().Any())
            throw new NotSupportedException("Sequences and HiLo are not supported by the CSharpDB EF Core provider.");

        foreach (IDbFunction dbFunction in
                 model.GetDbFunctions())
        {
            if (IsDecimalType(
                    dbFunction.ReturnType) ||
                dbFunction.Parameters.Any(parameter =>
                    IsDecimalType(
                        parameter.ClrType)))
            {
                throw new NotSupportedException(
                    $"DbFunction '{dbFunction.ModelName}' uses decimal parameters or return values, which are outside CSharpDB's exact scaled-integer foundation.");
            }
        }

        foreach (var entityType in model.GetEntityTypes())
        {
            ValidateEntityType(entityType);
        }
    }

    private static void ValidateRowVersionModel(IModel model)
    {
        foreach (IEntityType entityType in model.GetEntityTypes())
        {
            foreach (IProperty property in entityType.GetProperties().Where(IsRowVersion))
                ValidateRowVersionProperty(entityType, property);
        }

        foreach (var tableGroup in model.GetEntityTypes()
                     .Where(static entityType => entityType.GetTableName() is not null)
                     .GroupBy(
                         static entityType => (
                             Schema: entityType.GetSchema(),
                             Table: entityType.GetTableName()!)))
        {
            int rowVersionPropertyCount =
                tableGroup
                    .SelectMany(static entityType =>
                        entityType.GetProperties())
                    .Where(IsRowVersion)
                    .Distinct()
                    .Count();

            if (rowVersionPropertyCount > 1)
            {
                throw new NotSupportedException(
                    $"Table '{tableGroup.Key.Table}' maps {rowVersionPropertyCount} rowversion properties, but the CSharpDB EF Core provider supports exactly one rowversion property per table.");
            }
        }
    }

    private static void ValidateEntityType(IEntityType entityType)
    {
        if (!string.IsNullOrWhiteSpace(entityType.GetSchema()))
            throw new NotSupportedException($"Entity '{entityType.DisplayName()}' uses schema '{entityType.GetSchema()}', but schemas are not supported by the CSharpDB EF Core provider.");

        string? tableName = entityType.GetTableName();
        if (!string.IsNullOrWhiteSpace(tableName))
            CSharpDbProviderValidation.ValidateSimpleIdentifier(tableName, "Table name");

        if (entityType.GetComplexProperties().Any())
        {
            throw new NotSupportedException(
                $"Entity '{entityType.DisplayName()}' uses complex properties, which are not yet supported by the CSharpDB EF Core provider.");
        }

        foreach (var property in entityType.GetProperties())
        {
            ValidateProperty(entityType, property);
        }

        foreach (var index in entityType.GetIndexes())
        {
            if (index.Properties.Count == 0)
            {
                throw new NotSupportedException(
                    $"Index '{index.Name}' on entity '{entityType.DisplayName()}' must contain at least one column.");
            }

            if (!string.IsNullOrWhiteSpace(index.Name))
                CSharpDbProviderValidation.ValidateSimpleIdentifier(index.Name, "Index name");
        }

        foreach (var foreignKey in entityType.GetForeignKeys())
        {
            ValidateForeignKey(entityType, foreignKey);
        }
    }

    private static void ValidateProperty(IEntityType entityType, IProperty property)
    {
        CSharpDbProviderValidation.ValidateSimpleIdentifier(property.Name, "Column name");

        Type clrType = Nullable.GetUnderlyingType(
                property.ClrType) ??
            property.ClrType;
        if (clrType == typeof(decimal) &&
            property.GetValueConverter() is null)
        {
            (int precision, int scale) =
                CSharpDbDecimalStorage.ResolveFacets(
                    property.GetPrecision(),
                    property.GetScale());

            string? configuredStoreType =
                property.FindAnnotation(
                        RelationalAnnotationNames.ColumnType)?
                    .Value as string;
            if (configuredStoreType is not null &&
                !string.Equals(
                    configuredStoreType.Trim(),
                    "INTEGER",
                    StringComparison.OrdinalIgnoreCase))
            {
                throw new NotSupportedException(
                    $"Property '{entityType.DisplayName()}.{property.Name}' configures store type '{configuredStoreType}', but CSharpDB decimal({precision}, {scale}) uses provider-owned scaled INTEGER storage. Configure precision and scale with HasPrecision instead of HasColumnType.");
            }

            if (entityType.GetKeys().Any(key =>
                    key.Properties.Contains(property)))
            {
                throw new NotSupportedException(
                    $"Property '{entityType.DisplayName()}.{property.Name}' uses CSharpDB decimal({precision}, {scale}) as a key. Provider-owned decimal key mappings are not supported in this bounded release.");
            }

            if (property.FindAnnotation(
                    RelationalAnnotationNames.DefaultValue) is not null ||
                property.GetDefaultValueSql() is not null)
            {
                throw new NotSupportedException(
                    $"Property '{entityType.DisplayName()}.{property.Name}' configures a database default for CSharpDB decimal({precision}, {scale}). Decimal defaults are not supported until scaled-literal migration semantics are implemented.");
            }

            if (property.ValueGenerated != ValueGenerated.Never)
            {
                throw new NotSupportedException(
                    $"Property '{entityType.DisplayName()}.{property.Name}' uses generated-value semantics with CSharpDB decimal({precision}, {scale}), which are not supported in this bounded release.");
            }
        }

        if (property.GetComputedColumnSql() is not null)
            throw new NotSupportedException($"Property '{entityType.DisplayName()}.{property.Name}' uses a computed column, which is not supported by the CSharpDB EF Core provider.");

        if (property.GetDefaultValueSql() is not null)
            throw new NotSupportedException(
                $"Property '{entityType.DisplayName()}.{property.Name}' uses DefaultValueSql, but the CSharpDB EF Core provider currently supports literal DefaultValue metadata only.");

    }

    private static void ValidateRowVersionProperty(
        IEntityType entityType,
        IProperty property)
    {
        string displayName =
            $"{entityType.DisplayName()}.{property.Name}";

        if (property.ClrType != typeof(byte[]))
        {
            throw new NotSupportedException(
                $"Property '{displayName}' uses rowversion semantics with CLR type '{property.ClrType.ShortDisplayName()}'. The CSharpDB EF Core provider supports only non-nullable byte[] rowversion properties.");
        }

        if (property.IsNullable)
        {
            throw new NotSupportedException(
                $"Property '{displayName}' uses nullable rowversion semantics. The CSharpDB EF Core provider supports only non-nullable byte[] rowversion properties.");
        }

        if (property.GetValueConverter() is not null)
        {
            throw new NotSupportedException(
                $"Property '{displayName}' uses a value converter with rowversion semantics, which is not supported by the CSharpDB EF Core provider.");
        }

        string storeType =
            property.GetRelationalTypeMapping().StoreType;
        if (!string.Equals(
                storeType.Trim(),
                "BLOB",
                StringComparison.OrdinalIgnoreCase))
        {
            throw new NotSupportedException(
                $"Property '{displayName}' uses rowversion store type '{storeType}', but the CSharpDB EF Core provider requires BLOB.");
        }

        if (property.FindAnnotation(
                RelationalAnnotationNames.DefaultValue) is not null ||
            property.GetDefaultValueSql() is not null)
        {
            throw new NotSupportedException(
                $"Property '{displayName}' configures a database default with rowversion semantics, which is not supported by the CSharpDB EF Core provider.");
        }

        if (property.GetComputedColumnSql() is not null)
        {
            throw new NotSupportedException(
                $"Property '{displayName}' combines computed-column and rowversion semantics, which is not supported by the CSharpDB EF Core provider.");
        }

        if (property.GetContainingKeys().Any())
        {
            throw new NotSupportedException(
                $"Property '{displayName}' is part of a key and uses rowversion semantics. CSharpDB rowversion columns cannot participate in keys.");
        }

        if (property.GetContainingForeignKeys().Any())
        {
            throw new NotSupportedException(
                $"Property '{displayName}' is part of a foreign key and uses rowversion semantics. CSharpDB rowversion columns cannot participate in foreign keys.");
        }

        if (property.GetContainingIndexes().Any())
        {
            throw new NotSupportedException(
                $"Property '{displayName}' is indexed and uses rowversion semantics. CSharpDB rowversion columns cannot participate in indexes.");
        }
    }

    private static bool IsRowVersion(IProperty property) =>
        property.IsConcurrencyToken &&
        property.ValueGenerated == ValueGenerated.OnAddOrUpdate;

    private static void ValidateForeignKey(IEntityType entityType, IForeignKey foreignKey)
    {
        if (foreignKey.DeleteBehavior == DeleteBehavior.ClientSetNull)
        {
            if (!foreignKey.IsRequired &&
                foreignKey.Properties.Any(static property => property.IsNullable))
                return;

            string properties = string.Join(
                ", ",
                foreignKey.Properties.Select(static property => property.Name));
            throw new NotSupportedException(
                $"Foreign key '{entityType.DisplayName()}.({properties})' uses ClientSetNull, but at least one dependent property must be nullable. CSharpDB maps ClientSetNull to a restrictive database foreign key and relies on EF Core to null tracked dependents before deleting the principal.");
        }

        if (foreignKey.DeleteBehavior is not DeleteBehavior.Cascade
            and not DeleteBehavior.ClientNoAction
            and not DeleteBehavior.NoAction
            and not DeleteBehavior.Restrict)
        {
            throw new NotSupportedException(
                $"Foreign key '{foreignKey.Properties[0].Name}' uses delete behavior '{foreignKey.DeleteBehavior}', which is not supported by the CSharpDB EF Core provider in v1.");
        }
    }

    private static bool IsDecimalType(Type type) =>
        (Nullable.GetUnderlyingType(type) ?? type) ==
        typeof(decimal);
}
