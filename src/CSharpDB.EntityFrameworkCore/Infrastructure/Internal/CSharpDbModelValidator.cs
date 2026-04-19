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
        base.Validate(model, logger);

        if (model.GetSequences().Any())
            throw new NotSupportedException("Sequences and HiLo are not supported by the CSharpDB EF Core provider.");

        foreach (var entityType in model.GetEntityTypes())
        {
            ValidateEntityType(entityType);
        }
    }

    private static void ValidateEntityType(IEntityType entityType)
    {
        if (!string.IsNullOrWhiteSpace(entityType.GetSchema()))
            throw new NotSupportedException($"Entity '{entityType.DisplayName()}' uses schema '{entityType.GetSchema()}', but schemas are not supported by the CSharpDB EF Core provider.");

        if (entityType.GetKeys().Count() > 1)
            throw new NotSupportedException($"Entity '{entityType.DisplayName()}' defines alternate keys, which are not supported by the CSharpDB EF Core provider in v1.");

        if (entityType.GetDeclaredCheckConstraints().Any())
            throw new NotSupportedException($"Entity '{entityType.DisplayName()}' defines check constraints, which are not supported by the CSharpDB EF Core provider.");

        string? tableName = entityType.GetTableName();
        if (!string.IsNullOrWhiteSpace(tableName))
            CSharpDbProviderValidation.ValidateSimpleIdentifier(tableName, "Table name");

        foreach (var property in entityType.GetProperties())
        {
            ValidateProperty(entityType, property);
        }

        foreach (var index in entityType.GetIndexes())
        {
            if (index.Properties.Count != 1)
            {
                throw new NotSupportedException(
                    $"Index '{index.Name}' on entity '{entityType.DisplayName()}' uses multiple columns, which are not supported by the CSharpDB EF Core provider in v1.");
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

        if (property.ClrType == typeof(decimal) && property.GetValueConverter() is null)
        {
            throw new NotSupportedException(
                $"Property '{entityType.DisplayName()}.{property.Name}' uses decimal without an explicit value converter. Map it to TEXT or REAL explicitly for the CSharpDB EF Core provider.");
        }

        if (property.GetComputedColumnSql() is not null)
            throw new NotSupportedException($"Property '{entityType.DisplayName()}.{property.Name}' uses a computed column, which is not supported by the CSharpDB EF Core provider.");

        if (property.FindAnnotation(RelationalAnnotationNames.DefaultValue) is not null)
            throw new NotSupportedException($"Property '{entityType.DisplayName()}.{property.Name}' uses DefaultValue, which is not supported by the CSharpDB EF Core provider.");

        if (property.GetDefaultValueSql() is not null)
            throw new NotSupportedException($"Property '{entityType.DisplayName()}.{property.Name}' uses DefaultValueSql, which is not supported by the CSharpDB EF Core provider.");

        if (property.IsConcurrencyToken && property.ValueGenerated == ValueGenerated.OnAddOrUpdate)
            throw new NotSupportedException($"Property '{entityType.DisplayName()}.{property.Name}' uses rowversion semantics, which are not supported by the CSharpDB EF Core provider.");
    }

    private static void ValidateForeignKey(IEntityType entityType, IForeignKey foreignKey)
    {
        if (foreignKey.Properties.Count != 1 || foreignKey.PrincipalKey.Properties.Count != 1)
        {
            throw new NotSupportedException(
                $"Foreign key '{entityType.DisplayName()}_{string.Join("_", foreignKey.Properties.Select(p => p.Name))}' is composite, which is not supported by the CSharpDB EF Core provider in v1.");
        }

        if (foreignKey.PrincipalEntityType.FindPrimaryKey() != foreignKey.PrincipalKey)
        {
            throw new NotSupportedException(
                $"Foreign key '{foreignKey.Properties[0].Name}' targets an alternate key, which is not supported by the CSharpDB EF Core provider in v1.");
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
}
