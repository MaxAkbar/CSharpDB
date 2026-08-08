using System.Globalization;
using CSharpDB.Primitives;

namespace CSharpDB.Migration;

/// <summary>
/// Canonical migration-catalog transport for CSharpDB logical SQL types. The
/// descriptor is decomposed into bounded facets so migration artifacts remain
/// deterministic and renderers never execute retained source type text.
/// </summary>
internal static class CSharpDbDeclaredTypeContract
{
    internal const string DeclaredTypeFacet = "declaredType";
    internal const string KindFacet = "sqlTypeKind";
    internal const string LengthFacet = "length";
    internal const string PrecisionFacet = "precision";
    internal const string ScaleFacet = "scale";
    internal const string FractionalSecondsPrecisionFacet =
        "fractionalSecondsPrecision";

    internal static void AddFacets(
        ICollection<MigrationCatalogFacet> facets,
        SqlTypeDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(facets);
        ArgumentNullException.ThrowIfNull(descriptor);

        facets.Add(Facet(DeclaredTypeFacet, descriptor.ToSql()));
        facets.Add(Facet(KindFacet, descriptor.Kind.ToString()));
        AddOptional(facets, LengthFacet, descriptor.Length);
        AddOptional(facets, PrecisionFacet, descriptor.Precision);
        AddOptional(facets, ScaleFacet, descriptor.Scale);
        AddOptional(
            facets,
            FractionalSecondsPrecisionFacet,
            descriptor.FractionalSecondsPrecision);
    }

    internal static bool TryRead(
        MigrationCatalogObject source,
        out SqlTypeDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(source);
        descriptor = null!;

        string? declaredType = Facet(source, DeclaredTypeFacet);
        string? kindText = Facet(source, KindFacet);
        if (declaredType is null ||
            kindText is null ||
            !Enum.TryParse(kindText, ignoreCase: false, out SqlTypeKind kind) ||
            !Enum.IsDefined(kind) ||
            !TryOptionalInt(source, LengthFacet, out int? length) ||
            !TryOptionalInt(source, PrecisionFacet, out int? precision) ||
            !TryOptionalInt(source, ScaleFacet, out int? scale) ||
            !TryOptionalInt(
                source,
                FractionalSecondsPrecisionFacet,
                out int? fractionalSecondsPrecision))
        {
            return false;
        }

        try
        {
            descriptor = SqlTypeDescriptor.Create(
                kind,
                length,
                precision,
                scale,
                fractionalSecondsPrecision);
        }
        catch (Exception error) when (error is
            ArgumentException or
            InvalidOperationException)
        {
            descriptor = null!;
            return false;
        }

        return string.Equals(
                descriptor.ToSql(),
                declaredType,
                StringComparison.Ordinal) &&
            string.Equals(
                source.NativeType,
                declaredType,
                StringComparison.Ordinal);
    }

    private static bool TryOptionalInt(
        MigrationCatalogObject source,
        string name,
        out int? value)
    {
        string? text = Facet(source, name);
        if (text is null)
        {
            value = null;
            return true;
        }

        if (int.TryParse(
                text,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out int parsed))
        {
            value = parsed;
            return true;
        }

        value = null;
        return false;
    }

    private static void AddOptional(
        ICollection<MigrationCatalogFacet> facets,
        string name,
        int? value)
    {
        if (value is int number)
        {
            facets.Add(Facet(
                name,
                number.ToString(CultureInfo.InvariantCulture)));
        }
    }

    private static string? Facet(
        MigrationCatalogObject source,
        string name) =>
        source.Facets.SingleOrDefault(facet =>
            string.Equals(facet.Name, name, StringComparison.Ordinal))?.Value;

    private static MigrationCatalogFacet Facet(
        string name,
        string value) => new()
    {
        Name = name,
        Value = value,
    };
}
