using System.Numerics;

namespace CSharpDB.Migration.Canonicalization;

/// <summary>
/// A normalized arbitrary-precision decimal represented as
/// <c>coefficient * 10^-scale</c>.
/// </summary>
public sealed class CanonicalDecimal
{
    private readonly byte[] _coefficientBytes;

    public CanonicalDecimal(BigInteger coefficient, uint scale)
    {
        if (coefficient.IsZero)
        {
            Scale = 0;
            _coefficientBytes = [0x00];
            return;
        }

        while (scale > 0)
        {
            BigInteger quotient = BigInteger.DivRem(coefficient, 10, out BigInteger remainder);
            if (!remainder.IsZero)
                break;

            coefficient = quotient;
            scale--;
        }

        Scale = scale;
        _coefficientBytes = coefficient.ToByteArray(isUnsigned: false, isBigEndian: true);
    }

    public CanonicalDecimal(decimal value)
        : this(GetCoefficient(value), GetScale(value))
    {
    }

    public uint Scale { get; }

    public BigInteger Coefficient => new(_coefficientBytes, isUnsigned: false, isBigEndian: true);

    internal ReadOnlySpan<byte> CoefficientBytes => _coefficientBytes;

    private static BigInteger GetCoefficient(decimal value)
    {
        int[] bits = decimal.GetBits(value);
        BigInteger coefficient = (uint)bits[0];
        coefficient |= (BigInteger)(uint)bits[1] << 32;
        coefficient |= (BigInteger)(uint)bits[2] << 64;
        return (bits[3] & int.MinValue) != 0 ? -coefficient : coefficient;
    }

    private static uint GetScale(decimal value) =>
        (uint)((decimal.GetBits(value)[3] >> 16) & 0xFF);
}
