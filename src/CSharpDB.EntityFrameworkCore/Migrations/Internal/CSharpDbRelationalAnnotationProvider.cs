using CSharpDB.EntityFrameworkCore.Infrastructure.Internal;
using CSharpDB.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace CSharpDB.EntityFrameworkCore.Migrations.Internal;

public sealed class CSharpDbRelationalAnnotationProvider
    : RelationalAnnotationProvider
{
    public CSharpDbRelationalAnnotationProvider(
        RelationalAnnotationProviderDependencies dependencies)
        : base(dependencies)
    {
    }

    public override IEnumerable<IAnnotation> For(
        IColumn column,
        bool designTime)
    {
        foreach (IAnnotation annotation in
                 base.For(column, designTime))
        {
            yield return annotation;
        }

        if (!designTime ||
            column.StoreTypeMapping.Converter is not
                CSharpDbDecimalToInt64Converter converter ||
            !column.PropertyMappings.Any(static mapping =>
            {
                IProperty property = mapping.Property;
                Type clrType =
                    Nullable.GetUnderlyingType(
                        property.ClrType) ??
                    property.ClrType;
                return clrType == typeof(decimal) &&
                    property.GetValueConverter() is null;
            }))
        {
            yield break;
        }

        yield return new Annotation(
            CSharpDbAnnotationNames.DecimalStorage,
            FormattableString.Invariant(
                $"{converter.Precision},{converter.Scale}"));
    }
}
