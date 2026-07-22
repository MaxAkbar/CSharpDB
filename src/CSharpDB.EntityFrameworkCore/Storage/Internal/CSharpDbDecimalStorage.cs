using CSharpDB.Primitives;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace CSharpDB.EntityFrameworkCore.Storage.Internal;

internal static class CSharpDbDecimalStorage
{
    public const int DefaultPrecision = CSharpDbDecimalCodec.DefaultPrecision;
    public const int DefaultScale = CSharpDbDecimalCodec.DefaultScale;
    public const int MaximumPrecision = CSharpDbDecimalCodec.MaximumPrecision;

    public static (int Precision, int Scale) ResolveFacets(int? precision, int? scale) =>
        CSharpDbDecimalCodec.ResolveFacets(precision, scale);

    public static void ValidateFacets(int precision, int scale) =>
        CSharpDbDecimalCodec.ValidateFacets(precision, scale);

    public static long ToProvider(decimal value, int precision, int scale) =>
        CSharpDbDecimalCodec.ToScaledInt64(value, precision, scale);

    public static decimal FromProvider(long value, int precision, int scale) =>
        CSharpDbDecimalCodec.FromScaledInt64(value, precision, scale);
}

internal sealed class CSharpDbDecimalToInt64Converter
    : ValueConverter<decimal, long>
{
    public int Precision { get; }

    public int Scale { get; }

    public CSharpDbDecimalToInt64Converter(
        int precision,
        int scale)
        : base(
            value => CSharpDbDecimalCodec.ToScaledInt64(value, precision, scale),
            value => CSharpDbDecimalCodec.FromScaledInt64(value, precision, scale))
    {
        Precision = precision;
        Scale = scale;
    }
}
