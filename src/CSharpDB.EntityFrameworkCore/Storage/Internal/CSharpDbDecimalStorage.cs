using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace CSharpDB.EntityFrameworkCore.Storage.Internal;

internal static class CSharpDbDecimalStorage
{
    public const int DefaultPrecision = 18;
    public const int DefaultScale = 2;
    public const int MaximumPrecision = 18;

    private static readonly decimal[] ScaleFactors = CreateScaleFactors();

    public static (int Precision, int Scale) ResolveFacets(
        int? precision,
        int? scale)
    {
        int resolvedPrecision = precision ?? DefaultPrecision;
        int resolvedScale = scale ??
            (precision.HasValue
                ? 0
                : DefaultScale);
        ValidateFacets(resolvedPrecision, resolvedScale);
        return (resolvedPrecision, resolvedScale);
    }

    public static void ValidateFacets(int precision, int scale)
    {
        if (precision is < 1 or > MaximumPrecision)
        {
            throw new NotSupportedException(
                $"CSharpDB's exact decimal mapping supports precision from 1 through {MaximumPrecision}; precision {precision} is outside that range.");
        }

        if (scale < 0 || scale > precision)
        {
            throw new NotSupportedException(
                $"CSharpDB's exact decimal mapping requires scale from 0 through precision; decimal({precision}, {scale}) is invalid.");
        }
    }

    public static long ToProvider(
        decimal value,
        int precision,
        int scale)
    {
        ValidateFacets(precision, scale);

        decimal scaled;
        try
        {
            scaled = checked(value * ScaleFactors[scale]);
        }
        catch (OverflowException error)
        {
            throw new OverflowException(
                $"Decimal value '{value}' exceeds CSharpDB decimal({precision}, {scale}).",
                error);
        }

        if (decimal.Truncate(scaled) != scaled)
        {
            throw new InvalidOperationException(
                $"Decimal value '{value}' has more than {scale} fractional digits and cannot be stored exactly as CSharpDB decimal({precision}, {scale}).");
        }

        decimal exclusiveLimit = ScaleFactors[precision];
        if (scaled <= -exclusiveLimit || scaled >= exclusiveLimit)
        {
            throw new OverflowException(
                $"Decimal value '{value}' exceeds CSharpDB decimal({precision}, {scale}).");
        }

        return decimal.ToInt64(scaled);
    }

    public static decimal FromProvider(
        long value,
        int precision,
        int scale)
    {
        ValidateFacets(precision, scale);

        decimal exclusiveLimit =
            ScaleFactors[precision];
        if (value <= -exclusiveLimit ||
            value >= exclusiveLimit)
        {
            throw new OverflowException(
                $"Stored scaled integer '{value}' exceeds CSharpDB decimal({precision}, {scale}).");
        }

        return value / ScaleFactors[scale];
    }

    private static decimal[] CreateScaleFactors()
    {
        var factors = new decimal[MaximumPrecision + 1];
        factors[0] = 1m;
        for (int index = 1; index < factors.Length; index++)
            factors[index] = factors[index - 1] * 10m;

        return factors;
    }
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
            value => CSharpDbDecimalStorage.ToProvider(
                value,
                precision,
                scale),
            value => CSharpDbDecimalStorage.FromProvider(
                value,
                precision,
                scale))
    {
        Precision = precision;
        Scale = scale;
    }
}
