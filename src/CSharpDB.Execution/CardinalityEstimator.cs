using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

internal static class CardinalityEstimator
{
    public static bool TryEstimateLookupRowCount(
        Func<string, long?>? tableRowCountProvider,
        SchemaCatalog catalog,
        string tableName,
        string columnName,
        out long estimatedRows,
        out long tableRowCount)
    {
        estimatedRows = 0;
        tableRowCount = 0;

        if (!TryGetTableRowCount(tableRowCountProvider, catalog, tableName, out tableRowCount) ||
            tableRowCount <= 0 ||
            !catalog.TryGetFreshColumnStatistics(tableName, columnName, out var stats) ||
            stats.DistinctCount <= 0)
        {
            return false;
        }

        estimatedRows = Math.Max(1, DivideRoundUp(tableRowCount, stats.DistinctCount));
        return true;
    }

    public static bool TryEstimateEqualityJoinRowCount(
        SchemaCatalog catalog,
        JoinType joinType,
        TableSchema leftSchema,
        TableSchema rightSchema,
        ReadOnlySpan<int> leftKeyIndices,
        ReadOnlySpan<int> rightKeyIndices,
        long leftRows,
        long rightRows,
        out long estimatedRows)
    {
        estimatedRows = 0;

        if (joinType == JoinType.Cross ||
            leftRows <= 0 ||
            rightRows <= 0 ||
            leftKeyIndices.Length == 0 ||
            leftKeyIndices.Length != rightKeyIndices.Length ||
            string.Equals(leftSchema.TableName, "joined", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(rightSchema.TableName, "joined", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        long maxDistinctProduct = 1;
        double nonNullFactor = 1d;
        long maxJoinDomain = Math.Max(Math.Max(leftRows, rightRows), 1);

        for (int i = 0; i < leftKeyIndices.Length; i++)
        {
            int leftKeyIndex = leftKeyIndices[i];
            int rightKeyIndex = rightKeyIndices[i];
            if (leftKeyIndex < 0 || leftKeyIndex >= leftSchema.Columns.Count ||
                rightKeyIndex < 0 || rightKeyIndex >= rightSchema.Columns.Count)
            {
                return false;
            }

            string leftColumnName = leftSchema.Columns[leftKeyIndex].Name;
            string rightColumnName = rightSchema.Columns[rightKeyIndex].Name;
            if (!catalog.TryGetFreshColumnStatistics(leftSchema.TableName, leftColumnName, out var leftStats) ||
                !catalog.TryGetFreshColumnStatistics(rightSchema.TableName, rightColumnName, out var rightStats) ||
                leftStats.DistinctCount <= 0 ||
                rightStats.DistinctCount <= 0)
            {
                return false;
            }

            long pairDomain = Math.Max(leftStats.DistinctCount, rightStats.DistinctCount);
            pairDomain = Math.Clamp(pairDomain, 1, maxJoinDomain);
            maxDistinctProduct = Math.Min(SafeMultiply(maxDistinctProduct, pairDomain), maxJoinDomain);

            double leftNonNullFraction = Math.Clamp((double)leftStats.NonNullCount / Math.Max(leftRows, 1), 0d, 1d);
            double rightNonNullFraction = Math.Clamp((double)rightStats.NonNullCount / Math.Max(rightRows, 1), 0d, 1d);
            nonNullFactor *= leftNonNullFraction * rightNonNullFraction;
        }

        long crossRows = SafeMultiply(leftRows, rightRows);
        long baseInnerEstimate = DivideRoundUp(crossRows, maxDistinctProduct);
        long adjustedInnerEstimate = Math.Max(1, (long)Math.Ceiling(baseInnerEstimate * Math.Clamp(nonNullFactor, 0d, 1d)));
        adjustedInnerEstimate = Math.Min(adjustedInnerEstimate, crossRows);

        estimatedRows = joinType switch
        {
            JoinType.Inner => adjustedInnerEstimate,
            JoinType.LeftOuter => Math.Max(leftRows, adjustedInnerEstimate),
            JoinType.RightOuter => Math.Max(rightRows, adjustedInnerEstimate),
            _ => adjustedInnerEstimate,
        };

        return true;
    }

    public static long EstimateFallbackJoinRowCount(
        JoinType joinType,
        bool hasLeftEstimate,
        long leftRows,
        bool hasRightEstimate,
        long rightRows)
    {
        if (!hasLeftEstimate && !hasRightEstimate)
            return 0;

        return joinType switch
        {
            JoinType.Cross => hasLeftEstimate && hasRightEstimate
                ? SafeMultiply(leftRows, rightRows)
                : hasLeftEstimate
                    ? leftRows
                    : rightRows,
            JoinType.LeftOuter => hasLeftEstimate
                ? leftRows
                : rightRows,
            JoinType.RightOuter => hasRightEstimate
                ? rightRows
                : leftRows,
            JoinType.Inner => hasLeftEstimate && hasRightEstimate
                ? Math.Max(leftRows, rightRows)
                : hasLeftEstimate
                    ? leftRows
                    : rightRows,
            _ => hasLeftEstimate && hasRightEstimate
                ? Math.Max(leftRows, rightRows)
                : hasLeftEstimate
                    ? leftRows
                    : rightRows,
        };
    }

    private static bool TryGetTableRowCount(
        Func<string, long?>? tableRowCountProvider,
        SchemaCatalog catalog,
        string tableName,
        out long rowCount)
    {
        if (tableRowCountProvider is not null)
        {
            long? provided = tableRowCountProvider(tableName);
            if (provided.HasValue)
            {
                rowCount = provided.Value;
                return true;
            }
        }

        return catalog.TryGetTableRowCount(tableName, out rowCount);
    }

    private static long DivideRoundUp(long dividend, long divisor)
    {
        if (divisor <= 0)
            return dividend;

        long quotient = dividend / divisor;
        return dividend % divisor == 0
            ? quotient
            : quotient + 1;
    }

    private static long SafeMultiply(long a, long b)
    {
        if (a <= 0 || b <= 0)
            return 0;

        if (a > long.MaxValue / b)
            return long.MaxValue;

        return a * b;
    }
}
