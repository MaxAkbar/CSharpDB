using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

/// <summary>
/// Accumulates SQL numeric values without routing exact decimals through binary
/// floating point. Integer-only and floating-point result behavior remains
/// compatible with the existing aggregate operators.
/// </summary>
internal struct NumericAggregateAccumulator
{
    private DbValue _sum;

    public long Count { get; private set; }

    public bool HasAny => Count != 0;

    public void Reset()
    {
        _sum = DbValue.Null;
        Count = 0;
    }

    public void Add(DbValue value)
    {
        EnsureNumeric(value);
        _sum = Count == 0
            ? value
            : ExpressionEvaluator.EvaluateArithmetic(BinaryOp.Plus, _sum, value);
        Count = checked(Count + 1);
    }

    public void Add(long integerValue) => Add(DbValue.FromInteger(integerValue));

    public void Add(double realValue) => Add(DbValue.FromReal(realValue));

    public void AddRepeatedInteger(long integerValue, long occurrenceCount)
    {
        if (occurrenceCount < 0)
            throw new ArgumentOutOfRangeException(nameof(occurrenceCount));
        if (occurrenceCount == 0)
            return;
        if (Count != 0 && _sum.Type != DbType.Integer)
        {
            throw new InvalidOperationException(
                "Repeated integer accumulation requires an integer-only aggregate.");
        }

        // A run of equal integers is monotonic. Checking its exact Int128 end
        // point therefore has the same overflow behavior as adding every value
        // individually, without making index payload cardinality affect runtime.
        Int128 exactSum = (Count == 0 ? Int128.Zero : _sum.AsInteger) +
                          (Int128)integerValue * occurrenceCount;
        long checkedSum = checked((long)exactSum);
        long checkedCount = checked(Count + occurrenceCount);

        _sum = DbValue.FromInteger(checkedSum);
        Count = checkedCount;
    }

    public void Remove(DbValue value)
    {
        EnsureNumeric(value);
        if (Count <= 0)
            throw new InvalidOperationException("Cannot remove a value from an empty numeric aggregate.");

        if (Count == 1)
        {
            Reset();
            return;
        }

        _sum = ExpressionEvaluator.EvaluateArithmetic(BinaryOp.Minus, _sum, value);
        Count--;
    }

    public DbValue GetSumOrZero() => HasAny ? _sum : DbValue.FromInteger(0);

    public DbValue GetAverageOrNull()
    {
        if (!HasAny)
            return DbValue.Null;

        // Preserve the established REAL result for integer/REAL averages while
        // keeping a DECIMAL-only average in the exact decimal domain.
        if (_sum.Type != DbType.Decimal)
            return DbValue.FromReal(_sum.AsReal / Count);

        return DecimalAggregateSemantics.DivideForAverage(_sum, Count);
    }

    private static void EnsureNumeric(DbValue value)
    {
        if (value.IsNull || value.Type is not (DbType.Integer or DbType.Real or DbType.Decimal))
        {
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                "SUM/AVG aggregate argument must be numeric.");
        }
    }
}
