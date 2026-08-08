using System.Numerics;
using CSharpDB.Primitives;

namespace CSharpDB.Execution;

/// <summary>
/// Defines the exact result contract for aggregates over DECIMAL values.
/// DECIMAL AVG returns DECIMAL(18) and rounds an otherwise non-terminating
/// quotient to at most 18 significant decimal digits using midpoint-to-even.
/// The result scale is therefore magnitude-dependent and never exceeds 18.
/// </summary>
internal static class DecimalAggregateSemantics
{
    public static readonly SqlTypeDescriptor AverageResultType = new(
        SqlTypeKind.Decimal,
        precision: SqlTypeDescriptor.MaximumDecimalPrecision);

    public static DbValue DivideForAverage(DbValue sum, long count)
    {
        if (sum.Type != DbType.Decimal)
            throw new ArgumentException("An exact decimal average requires a DECIMAL sum.", nameof(sum));
        if (count <= 0)
            throw new ArgumentOutOfRangeException(nameof(count), count, "An average requires at least one value.");

        long coefficient = sum.DecimalCoefficient;
        if (coefficient == 0)
            return DbValue.FromDecimalParts(0, 0);

        BigInteger numerator = BigInteger.Abs(new BigInteger(coefficient));
        BigInteger denominator =
            new BigInteger(count) * BigInteger.Pow(10, sum.DecimalScale);
        BigInteger integralPart = numerator / denominator;
        int integralDigits = integralPart.IsZero
            ? 0
            : integralPart.ToString().Length;
        int resultScale = SqlTypeDescriptor.MaximumDecimalPrecision - integralDigits;
        if (resultScale < 0)
        {
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                $"Decimal AVG result exceeds {SqlTypeDescriptor.MaximumDecimalPrecision} significant digits.");
        }

        BigInteger scaledNumerator = numerator * BigInteger.Pow(10, resultScale);
        BigInteger roundedCoefficient = BigInteger.DivRem(
            scaledNumerator,
            denominator,
            out BigInteger remainder);
        int midpointComparison = (remainder * 2).CompareTo(denominator);
        if (midpointComparison > 0 ||
            midpointComparison == 0 && !roundedCoefficient.IsEven)
        {
            roundedCoefficient++;
        }

        if (coefficient < 0)
            roundedCoefficient = -roundedCoefficient;

        try
        {
            return DbValue.FromDecimalParts((long)roundedCoefficient, resultScale);
        }
        catch (Exception error) when (error is OverflowException or ArgumentOutOfRangeException)
        {
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                $"Decimal AVG result exceeds {SqlTypeDescriptor.MaximumDecimalPrecision} significant digits.",
                error);
        }
    }
}
