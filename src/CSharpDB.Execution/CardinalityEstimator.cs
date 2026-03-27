using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

internal static class CardinalityEstimator
{
    private enum NullFilterKind
    {
        None,
        IsNull,
        IsNotNull,
    }

    private sealed class ColumnConstraint
    {
        public HashSet<DbValue>? AllowedValues { get; set; }
        public HashSet<DbValue>? ExcludedValues { get; set; }
        public List<ColumnConstraint>? Alternatives { get; set; }
        public DbValue? LowerBound { get; set; }
        public bool LowerInclusive { get; set; }
        public DbValue? UpperBound { get; set; }
        public bool UpperInclusive { get; set; }
        public NullFilterKind NullFilter { get; set; }
        public bool IsImpossible { get; set; }
    }

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

    public static bool TryEstimateFilteredRowCount(
        SchemaCatalog catalog,
        TableSchema schema,
        long tableRowCount,
        IReadOnlyList<Expression> predicates,
        out long estimatedRows)
    {
        estimatedRows = 0;
        if (tableRowCount <= 0 || predicates.Count == 0)
            return false;

        var constraintsByColumn = new Dictionary<int, ColumnConstraint>();
        for (int i = 0; i < predicates.Count; i++)
        {
            TryAccumulatePredicateConstraint(schema, predicates[i], constraintsByColumn);
        }

        if (constraintsByColumn.Count == 0)
            return false;

        double combinedSelectivity = 1d;
        bool usedStats = false;
        foreach (var (columnIndex, constraint) in constraintsByColumn)
        {
            if (!TryEstimateColumnConstraintSelectivity(catalog, schema, tableRowCount, columnIndex, constraint, out double selectivity))
                continue;

            combinedSelectivity *= Math.Clamp(selectivity, 0d, 1d);
            usedStats = true;
        }

        if (!usedStats)
            return false;

        estimatedRows = Math.Clamp(
            (long)Math.Ceiling(tableRowCount * Math.Clamp(combinedSelectivity, 0d, 1d)),
            1,
            tableRowCount);
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

    private static void TryAccumulatePredicateConstraint(
        TableSchema schema,
        Expression predicate,
        Dictionary<int, ColumnConstraint> constraintsByColumn)
    {
        if (TryParseColumnConstraint(schema, predicate, out int columnIndex, out var parsedConstraint))
        {
            if (!constraintsByColumn.TryGetValue(columnIndex, out var existing))
            {
                constraintsByColumn[columnIndex] = parsedConstraint;
                return;
            }

            MergeColumnConstraint(existing, parsedConstraint);
        }
    }

    private static bool TryEstimateColumnConstraintSelectivity(
        SchemaCatalog catalog,
        TableSchema schema,
        long tableRowCount,
        int columnIndex,
        ColumnConstraint constraint,
        out double selectivity)
    {
        selectivity = 0d;

        if (columnIndex < 0 || columnIndex >= schema.Columns.Count)
            return false;

        if (constraint.IsImpossible)
        {
            selectivity = MinimumSelectivity(tableRowCount);
            return true;
        }

        if (constraint.Alternatives is { Count: > 0 } alternatives)
            return TryEstimateAlternativeConstraintSelectivity(catalog, schema, tableRowCount, columnIndex, alternatives, out selectivity);

        bool isPrimaryKey = columnIndex == schema.PrimaryKeyColumnIndex;
        string columnName = schema.Columns[columnIndex].Name;
        bool hasFreshStats = catalog.TryGetFreshColumnStatistics(schema.TableName, columnName, out var stats);
        double minimumSelectivity = MinimumSelectivity(tableRowCount);

        if (constraint.NullFilter == NullFilterKind.IsNull)
        {
            if (isPrimaryKey)
            {
                selectivity = minimumSelectivity;
                return true;
            }

            if (!hasFreshStats)
                return false;

            double nullFraction = Math.Clamp(1d - ((double)stats.NonNullCount / Math.Max(tableRowCount, 1)), 0d, 1d);
            selectivity = Math.Max(nullFraction, minimumSelectivity);
            return true;
        }

        double nonNullFraction = isPrimaryKey
            ? 1d
            : hasFreshStats
                ? Math.Clamp((double)stats.NonNullCount / Math.Max(tableRowCount, 1), 0d, 1d)
                : 0d;

        if (constraint.AllowedValues is { } allowedValues)
        {
            int matchingValueCount = CountAllowedValues(constraint, allowedValues);
            if (matchingValueCount <= 0)
            {
                selectivity = minimumSelectivity;
                return true;
            }

            if (isPrimaryKey)
            {
                selectivity = Math.Clamp((double)matchingValueCount / Math.Max(tableRowCount, 1), minimumSelectivity, 1d);
                return true;
            }

            if (!hasFreshStats)
                return false;

            if (stats.NonNullCount <= 0 || stats.DistinctCount <= 0)
            {
                selectivity = minimumSelectivity;
                return true;
            }

            double distinctFraction = (double)matchingValueCount / Math.Max(stats.DistinctCount, 1);
            selectivity = Math.Clamp(Math.Max(distinctFraction * nonNullFraction, minimumSelectivity), minimumSelectivity, 1d);
            return true;
        }

        bool hasRangeConstraint = constraint.LowerBound is DbValue || constraint.UpperBound is DbValue;
        double baseSelectivity = 1d;
        bool usedStats = false;

        if (constraint.NullFilter == NullFilterKind.IsNotNull)
        {
            baseSelectivity = nonNullFraction;
            usedStats = !isPrimaryKey;
        }

        if (hasRangeConstraint)
        {
            if (!hasFreshStats || !TryEstimateRangeSelectivity(stats, constraint, out double rangeSelectivity))
                return false;

            baseSelectivity = Math.Max(0d, rangeSelectivity) * nonNullFraction;
            usedStats = true;
        }

        if (constraint.ExcludedValues is { Count: > 0 } excludedValues)
        {
            double exclusionFraction;
            if (isPrimaryKey)
            {
                exclusionFraction = Math.Clamp(1d - (excludedValues.Count / (double)Math.Max(tableRowCount, 1)), 0d, 1d);
            }
            else
            {
                if (!hasFreshStats || stats.DistinctCount <= 0)
                    return false;

                exclusionFraction = Math.Clamp(1d - (excludedValues.Count / (double)Math.Max(stats.DistinctCount, 1)), 0d, 1d);
                usedStats = true;
                if (constraint.NullFilter == NullFilterKind.None && !hasRangeConstraint)
                    baseSelectivity = nonNullFraction;
            }

            selectivity = Math.Clamp(Math.Max(baseSelectivity * exclusionFraction, minimumSelectivity), minimumSelectivity, 1d);
            return true;
        }

        if (!usedStats && !isPrimaryKey)
            return false;

        selectivity = Math.Clamp(Math.Max(baseSelectivity, minimumSelectivity), minimumSelectivity, 1d);
        return true;
    }

    private static bool TryCreateDbValue(LiteralExpression literalExpression, out DbValue value)
    {
        value = literalExpression.Value switch
        {
            null => DbValue.Null,
            long intValue => DbValue.FromInteger(intValue),
            int intValue => DbValue.FromInteger(intValue),
            double realValue => DbValue.FromReal(realValue),
            float realValue => DbValue.FromReal(realValue),
            string text => DbValue.FromText(text),
            byte[] blob => DbValue.FromBlob(blob),
            _ => DbValue.Null,
        };

        return !value.IsNull || literalExpression.Value is null;
    }

    private static bool TryParseColumnConstraint(
        TableSchema schema,
        Expression predicate,
        out int columnIndex,
        out ColumnConstraint constraint)
    {
        columnIndex = -1;
        constraint = new ColumnConstraint();

        if (TryParseDisjunctionConstraint(schema, predicate, out columnIndex, out constraint))
            return true;

        if (predicate is BetweenExpression between &&
            !between.Negated &&
            between.Operand is ColumnRefExpression betweenColumn &&
            between.Low is LiteralExpression lowLiteral &&
            between.High is LiteralExpression highLiteral &&
            TryCreateDbValue(lowLiteral, out var lowValue) &&
            TryCreateDbValue(highLiteral, out var highValue))
        {
            columnIndex = ResolveColumnIndex(schema, betweenColumn);
            if (columnIndex < 0 || lowValue.IsNull || highValue.IsNull)
                return false;

            constraint.LowerBound = lowValue;
            constraint.LowerInclusive = true;
            constraint.UpperBound = highValue;
            constraint.UpperInclusive = true;
            return true;
        }

        if (predicate is InExpression inExpression &&
            inExpression.Operand is ColumnRefExpression inColumn)
        {
            columnIndex = ResolveColumnIndex(schema, inColumn);
            if (columnIndex < 0)
                return false;

            var values = new HashSet<DbValue>();
            for (int i = 0; i < inExpression.Values.Count; i++)
            {
                if (inExpression.Values[i] is not LiteralExpression literal ||
                    !TryCreateDbValue(literal, out var value))
                {
                    return false;
                }

                // NULL in IN / NOT IN changes three-valued SQL semantics enough that the simple
                // selectivity model should decline to estimate the predicate.
                if (value.IsNull)
                    return false;

                values.Add(value);
            }

            if (inExpression.Negated)
                constraint.ExcludedValues = values;
            else
                constraint.AllowedValues = values;

            NormalizeConstraint(constraint);
            return true;
        }

        if (predicate is IsNullExpression isNull &&
            isNull.Operand is ColumnRefExpression nullColumn)
        {
            columnIndex = ResolveColumnIndex(schema, nullColumn);
            if (columnIndex < 0)
                return false;

            constraint.NullFilter = isNull.Negated ? NullFilterKind.IsNotNull : NullFilterKind.IsNull;
            NormalizeConstraint(constraint);
            return true;
        }

        if (predicate is not BinaryExpression binary)
            return false;

        if (TryGetColumnAndLiteral(binary.Left, binary.Right, out var leftColumn, out var leftLiteral))
        {
            columnIndex = ResolveColumnIndex(schema, leftColumn);
            if (columnIndex < 0)
                return false;
            bool applied = TryApplyBinaryConstraint(binary.Op, leftLiteral, reverseOperator: false, constraint);
            if (applied)
                NormalizeConstraint(constraint);
            return applied;
        }

        if (TryGetColumnAndLiteral(binary.Right, binary.Left, out var rightColumn, out var rightLiteral))
        {
            columnIndex = ResolveColumnIndex(schema, rightColumn);
            if (columnIndex < 0)
                return false;
            bool applied = TryApplyBinaryConstraint(binary.Op, rightLiteral, reverseOperator: true, constraint);
            if (applied)
                NormalizeConstraint(constraint);
            return applied;
        }

        return false;
    }

    private static bool TryParseDisjunctionConstraint(
        TableSchema schema,
        Expression predicate,
        out int columnIndex,
        out ColumnConstraint constraint)
    {
        columnIndex = -1;
        constraint = new ColumnConstraint();

        if (predicate is not BinaryExpression { Op: BinaryOp.Or })
            return false;

        var disjuncts = new List<Expression>();
        CollectOrDisjuncts(predicate, disjuncts);
        if (disjuncts.Count < 2)
            return false;

        HashSet<DbValue>? unionValues = null;
        List<ColumnConstraint>? alternatives = null;
        bool allDiscrete = true;
        for (int i = 0; i < disjuncts.Count; i++)
        {
            if (!TryParseColumnConstraint(schema, disjuncts[i], out int disjunctColumnIndex, out var disjunctConstraint))
                return false;

            if (!IsUnionCompatibleConstraint(disjunctConstraint))
                return false;

            if (columnIndex < 0)
            {
                columnIndex = disjunctColumnIndex;
            }
            else if (columnIndex != disjunctColumnIndex)
            {
                return false;
            }

            if (TryExtractDiscreteAllowedValues(disjunctConstraint, out var disjunctValues))
            {
                unionValues ??= [];
                unionValues.UnionWith(disjunctValues);
            }
            else
            {
                allDiscrete = false;
                alternatives ??= [];
                alternatives.Add(CloneConstraint(disjunctConstraint));
            }
        }

        if (columnIndex < 0)
            return false;

        if (allDiscrete)
        {
            if (unionValues is not { Count: > 0 })
                return false;

            constraint.AllowedValues = unionValues;
        }
        else
        {
            if (unionValues is { Count: > 0 })
            {
                alternatives ??= [];
                alternatives.Add(new ColumnConstraint { AllowedValues = unionValues });
            }

            if (alternatives is not { Count: > 0 })
                return false;

            constraint.Alternatives = alternatives;
        }

        NormalizeConstraint(constraint);
        return !constraint.IsImpossible;
    }

    private static bool TryApplyBinaryConstraint(
        BinaryOp op,
        DbValue literal,
        bool reverseOperator,
        ColumnConstraint constraint)
    {
        if (literal.IsNull)
            return false;

        BinaryOp normalized = reverseOperator ? ReverseComparisonOperator(op) : op;
        switch (normalized)
        {
            case BinaryOp.Equals:
                constraint.AllowedValues = [literal];
                return true;
            case BinaryOp.NotEquals:
                constraint.ExcludedValues = [literal];
                return true;
            case BinaryOp.GreaterThan:
                constraint.LowerBound = literal;
                constraint.LowerInclusive = false;
                return true;
            case BinaryOp.GreaterOrEqual:
                constraint.LowerBound = literal;
                constraint.LowerInclusive = true;
                return true;
            case BinaryOp.LessThan:
                constraint.UpperBound = literal;
                constraint.UpperInclusive = false;
                return true;
            case BinaryOp.LessOrEqual:
                constraint.UpperBound = literal;
                constraint.UpperInclusive = true;
                return true;
            default:
                return false;
        }
    }

    private static BinaryOp ReverseComparisonOperator(BinaryOp op)
    {
        return op switch
        {
            BinaryOp.LessThan => BinaryOp.GreaterThan,
            BinaryOp.LessOrEqual => BinaryOp.GreaterOrEqual,
            BinaryOp.GreaterThan => BinaryOp.LessThan,
            BinaryOp.GreaterOrEqual => BinaryOp.LessOrEqual,
            _ => op,
        };
    }

    private static void MergeColumnConstraint(ColumnConstraint target, ColumnConstraint incoming)
    {
        if (target.IsImpossible)
            return;

        if (incoming.IsImpossible)
        {
            target.IsImpossible = true;
            return;
        }

        if (target.Alternatives is { Count: > 0 } || incoming.Alternatives is { Count: > 0 })
        {
            MergeAlternativeConstraints(target, incoming);
            return;
        }

        MergeSimpleConstraint(target, incoming);
        NormalizeConstraint(target);
    }

    private static void MergeAlternativeConstraints(ColumnConstraint target, ColumnConstraint incoming)
    {
        var leftAlternatives = ExpandAlternatives(target);
        var rightAlternatives = ExpandAlternatives(incoming);
        var merged = new List<ColumnConstraint>();

        for (int i = 0; i < leftAlternatives.Count; i++)
        {
            for (int j = 0; j < rightAlternatives.Count; j++)
            {
                var candidate = CloneConstraint(leftAlternatives[i]);
                MergeSimpleConstraint(candidate, rightAlternatives[j]);
                NormalizeConstraint(candidate);
                if (!candidate.IsImpossible)
                    merged.Add(candidate);
            }
        }

        if (merged.Count == 0)
        {
            target.IsImpossible = true;
            target.Alternatives = null;
            target.AllowedValues = null;
            target.ExcludedValues = null;
            return;
        }

        if (merged.Count == 1)
        {
            CopyConstraint(target, merged[0]);
            return;
        }

        target.AllowedValues = null;
        target.ExcludedValues = null;
        target.Alternatives = merged;
        target.LowerBound = null;
        target.UpperBound = null;
        target.NullFilter = NullFilterKind.None;
        target.IsImpossible = false;
    }

    private static void MergeSimpleConstraint(ColumnConstraint target, ColumnConstraint incoming)
    {
        if (incoming.NullFilter != NullFilterKind.None)
        {
            if (target.NullFilter == NullFilterKind.None)
            {
                target.NullFilter = incoming.NullFilter;
            }
            else if (target.NullFilter != incoming.NullFilter)
            {
                target.IsImpossible = true;
                return;
            }
        }

        if (incoming.AllowedValues is { } incomingAllowed)
        {
            if (target.AllowedValues is null)
            {
                target.AllowedValues = new HashSet<DbValue>(incomingAllowed);
            }
            else
            {
                target.AllowedValues.IntersectWith(incomingAllowed);
            }
        }

        if (incoming.ExcludedValues is { } incomingExcluded)
        {
            target.ExcludedValues ??= [];
            target.ExcludedValues.UnionWith(incomingExcluded);
        }

        if (incoming.LowerBound is DbValue lower)
            SetLowerBound(target, lower, incoming.LowerInclusive);

        if (incoming.UpperBound is DbValue upper)
            SetUpperBound(target, upper, incoming.UpperInclusive);
    }

    private static void SetLowerBound(ColumnConstraint target, DbValue lower, bool inclusive)
    {
        if (target.LowerBound is not DbValue existingLower)
        {
            target.LowerBound = lower;
            target.LowerInclusive = inclusive;
            return;
        }

        int cmp = DbValue.Compare(lower, existingLower);
        if (cmp > 0 || (cmp == 0 && !inclusive && target.LowerInclusive))
        {
            target.LowerBound = lower;
            target.LowerInclusive = inclusive;
        }
    }

    private static void SetUpperBound(ColumnConstraint target, DbValue upper, bool inclusive)
    {
        if (target.UpperBound is not DbValue existingUpper)
        {
            target.UpperBound = upper;
            target.UpperInclusive = inclusive;
            return;
        }

        int cmp = DbValue.Compare(upper, existingUpper);
        if (cmp < 0 || (cmp == 0 && !inclusive && target.UpperInclusive))
        {
            target.UpperBound = upper;
            target.UpperInclusive = inclusive;
        }
    }

    private static bool ConstraintAllowsEquality(ColumnConstraint constraint, DbValue equality)
    {
        if (constraint.Alternatives is { Count: > 0 } alternatives)
            return alternatives.Any(alt => ConstraintAllowsEquality(alt, equality));

        if (constraint.NullFilter == NullFilterKind.IsNull)
            return false;

        if (constraint.ExcludedValues?.Contains(equality) == true)
            return false;

        if (constraint.LowerBound is DbValue lower)
        {
            int cmp = DbValue.Compare(equality, lower);
            if (cmp < 0 || (cmp == 0 && !constraint.LowerInclusive))
                return false;
        }

        if (constraint.UpperBound is DbValue upper)
        {
            int cmp = DbValue.Compare(equality, upper);
            if (cmp > 0 || (cmp == 0 && !constraint.UpperInclusive))
                return false;
        }

        return true;
    }

    private static int CountAllowedValues(ColumnConstraint constraint, HashSet<DbValue> allowedValues)
    {
        int count = 0;
        foreach (DbValue value in allowedValues)
        {
            if (ConstraintAllowsEquality(constraint, value))
                count++;
        }

        return count;
    }

    private static bool TryEstimateAlternativeConstraintSelectivity(
        SchemaCatalog catalog,
        TableSchema schema,
        long tableRowCount,
        int columnIndex,
        IReadOnlyList<ColumnConstraint> alternatives,
        out double selectivity)
    {
        selectivity = 0d;

        if (alternatives.Count == 0)
            return false;

        if (TryEstimateAlternativeRangeUnionSelectivity(catalog, schema, tableRowCount, columnIndex, alternatives, out selectivity))
            return true;

        double combined = 0d;
        bool used = false;
        for (int i = 0; i < alternatives.Count; i++)
        {
            if (!TryEstimateColumnConstraintSelectivity(catalog, schema, tableRowCount, columnIndex, alternatives[i], out double alternativeSelectivity))
                continue;

            combined += alternativeSelectivity;
            used = true;
        }

        if (!used)
            return false;

        selectivity = Math.Clamp(combined, MinimumSelectivity(tableRowCount), 1d);
        return true;
    }

    private static bool TryEstimateAlternativeRangeUnionSelectivity(
        SchemaCatalog catalog,
        TableSchema schema,
        long tableRowCount,
        int columnIndex,
        IReadOnlyList<ColumnConstraint> alternatives,
        out double selectivity)
    {
        selectivity = 0d;

        if (columnIndex < 0 || columnIndex >= schema.Columns.Count)
            return false;

        string columnName = schema.Columns[columnIndex].Name;
        if (!catalog.TryGetFreshColumnStatistics(schema.TableName, columnName, out var stats) ||
            stats.MinValue.IsNull ||
            stats.MaxValue.IsNull)
        {
            return false;
        }

        return stats.MinValue.Type switch
        {
            DbType.Integer when stats.MaxValue.Type == DbType.Integer => TryEstimateIntegerAlternativeRangeUnionSelectivity(stats, alternatives, out selectivity),
            DbType.Real or DbType.Integer when stats.MaxValue.Type is DbType.Real or DbType.Integer => TryEstimateRealAlternativeRangeUnionSelectivity(stats, alternatives, out selectivity),
            _ => false,
        };
    }

    private static bool TryEstimateIntegerAlternativeRangeUnionSelectivity(
        ColumnStatistics stats,
        IReadOnlyList<ColumnConstraint> alternatives,
        out double selectivity)
    {
        selectivity = 0d;
        long min = stats.MinValue.AsInteger;
        long max = stats.MaxValue.AsInteger;
        if (max < min)
            return false;

        var segments = new List<(long Start, long End)>(alternatives.Count);
        for (int i = 0; i < alternatives.Count; i++)
        {
            if (!TryConvertConstraintToIntegerSegment(alternatives[i], min, max, out var segment))
                return false;

            segments.Add(segment);
        }

        long covered = MergeIntegerSegmentsAndMeasure(segments);
        double totalSpan = (double)max - min + 1d;
        selectivity = Math.Clamp(covered / Math.Max(totalSpan, 1d), 0d, 1d);
        return true;
    }

    private static bool TryEstimateRealAlternativeRangeUnionSelectivity(
        ColumnStatistics stats,
        IReadOnlyList<ColumnConstraint> alternatives,
        out double selectivity)
    {
        selectivity = 0d;
        double min = stats.MinValue.AsReal;
        double max = stats.MaxValue.AsReal;
        if (max < min)
            return false;

        var segments = new List<(double Start, double End)>(alternatives.Count);
        for (int i = 0; i < alternatives.Count; i++)
        {
            if (!TryConvertConstraintToRealSegment(alternatives[i], min, max, out var segment))
                return false;

            segments.Add(segment);
        }

        double covered = MergeRealSegmentsAndMeasure(segments);
        double totalSpan = max - min;
        if (totalSpan <= 0d)
        {
            selectivity = 1d;
            return true;
        }

        selectivity = Math.Clamp(covered / totalSpan, 0d, 1d);
        return true;
    }

    private static bool TryEstimateRangeSelectivity(
        ColumnStatistics stats,
        ColumnConstraint constraint,
        out double selectivity)
    {
        selectivity = 0d;
        if (stats.MinValue.IsNull || stats.MaxValue.IsNull)
            return false;

        return stats.MinValue.Type switch
        {
            DbType.Integer when stats.MaxValue.Type == DbType.Integer => TryEstimateIntegerRangeSelectivity(stats, constraint, out selectivity),
            DbType.Real or DbType.Integer when stats.MaxValue.Type is DbType.Real or DbType.Integer => TryEstimateRealRangeSelectivity(stats, constraint, out selectivity),
            _ => false,
        };
    }

    private static bool TryConvertConstraintToIntegerSegment(
        ColumnConstraint constraint,
        long min,
        long max,
        out (long Start, long End) segment)
    {
        segment = default;

        if (!IsUnionCompatibleConstraint(constraint))
            return false;

        if (constraint.AllowedValues is { Count: > 0 } allowedValues)
        {
            if (allowedValues.Count != 1)
                return false;

            DbValue onlyValue = allowedValues.First();
            if (onlyValue.Type != DbType.Integer)
                return false;

            long point = Math.Clamp(onlyValue.AsInteger, min, max);
            segment = (point, point);
            return true;
        }

        long lower = min;
        long upper = max;

        if (constraint.LowerBound is DbValue lowerBound)
        {
            if (lowerBound.Type != DbType.Integer)
                return false;

            long value = lowerBound.AsInteger;
            if (!constraint.LowerInclusive && value < long.MaxValue)
                value++;
            lower = Math.Max(lower, value);
        }

        if (constraint.UpperBound is DbValue upperBound)
        {
            if (upperBound.Type != DbType.Integer)
                return false;

            long value = upperBound.AsInteger;
            if (!constraint.UpperInclusive && value > long.MinValue)
                value--;
            upper = Math.Min(upper, value);
        }

        if (upper < lower)
            return false;

        segment = (lower, upper);
        return true;
    }

    private static bool TryConvertConstraintToRealSegment(
        ColumnConstraint constraint,
        double min,
        double max,
        out (double Start, double End) segment)
    {
        segment = default;

        if (!IsUnionCompatibleConstraint(constraint))
            return false;

        if (constraint.AllowedValues is { Count: > 0 } allowedValues)
        {
            if (allowedValues.Count != 1)
                return false;

            DbValue onlyValue = allowedValues.First();
            if (onlyValue.Type is not (DbType.Integer or DbType.Real))
                return false;

            double point = Math.Clamp(onlyValue.AsReal, min, max);
            segment = (point, point);
            return true;
        }

        double lower = min;
        double upper = max;

        if (constraint.LowerBound is DbValue lowerBound)
        {
            if (lowerBound.Type is not (DbType.Integer or DbType.Real))
                return false;

            lower = Math.Max(lower, lowerBound.AsReal);
        }

        if (constraint.UpperBound is DbValue upperBound)
        {
            if (upperBound.Type is not (DbType.Integer or DbType.Real))
                return false;

            upper = Math.Min(upper, upperBound.AsReal);
        }

        if (upper < lower)
            return false;

        segment = (lower, upper);
        return true;
    }

    private static long MergeIntegerSegmentsAndMeasure(List<(long Start, long End)> segments)
    {
        if (segments.Count == 0)
            return 0;

        segments.Sort(static (a, b) => a.Start.CompareTo(b.Start));
        long covered = 0;
        long currentStart = segments[0].Start;
        long currentEnd = segments[0].End;

        for (int i = 1; i < segments.Count; i++)
        {
            var segment = segments[i];
            if (segment.Start <= currentEnd + 1)
            {
                currentEnd = Math.Max(currentEnd, segment.End);
                continue;
            }

            covered += currentEnd - currentStart + 1;
            currentStart = segment.Start;
            currentEnd = segment.End;
        }

        covered += currentEnd - currentStart + 1;
        return covered;
    }

    private static double MergeRealSegmentsAndMeasure(List<(double Start, double End)> segments)
    {
        if (segments.Count == 0)
            return 0d;

        segments.Sort(static (a, b) => a.Start.CompareTo(b.Start));
        double covered = 0d;
        double currentStart = segments[0].Start;
        double currentEnd = segments[0].End;

        for (int i = 1; i < segments.Count; i++)
        {
            var segment = segments[i];
            if (segment.Start <= currentEnd)
            {
                currentEnd = Math.Max(currentEnd, segment.End);
                continue;
            }

            covered += currentEnd - currentStart;
            currentStart = segment.Start;
            currentEnd = segment.End;
        }

        covered += currentEnd - currentStart;
        return covered;
    }

    private static bool TryEstimateIntegerRangeSelectivity(
        ColumnStatistics stats,
        ColumnConstraint constraint,
        out double selectivity)
    {
        selectivity = 0d;
        long min = stats.MinValue.AsInteger;
        long max = stats.MaxValue.AsInteger;
        if (max < min)
            return false;

        long effectiveLower = min;
        long effectiveUpper = max;

        if (constraint.LowerBound is DbValue lower)
        {
            long value = lower.AsInteger;
            if (!constraint.LowerInclusive && value < long.MaxValue)
                value++;
            effectiveLower = Math.Max(effectiveLower, value);
        }

        if (constraint.UpperBound is DbValue upper)
        {
            long value = upper.AsInteger;
            if (!constraint.UpperInclusive && value > long.MinValue)
                value--;
            effectiveUpper = Math.Min(effectiveUpper, value);
        }

        if (effectiveUpper < effectiveLower)
        {
            selectivity = 0d;
            return true;
        }

        double totalSpan = (double)max - min + 1d;
        double selectedSpan = (double)effectiveUpper - effectiveLower + 1d;
        selectivity = Math.Clamp(selectedSpan / Math.Max(totalSpan, 1d), 0d, 1d);
        return true;
    }

    private static bool TryEstimateRealRangeSelectivity(
        ColumnStatistics stats,
        ColumnConstraint constraint,
        out double selectivity)
    {
        selectivity = 0d;
        double min = stats.MinValue.AsReal;
        double max = stats.MaxValue.AsReal;
        if (max < min)
            return false;

        double effectiveLower = min;
        double effectiveUpper = max;

        if (constraint.LowerBound is DbValue lower)
            effectiveLower = Math.Max(effectiveLower, lower.AsReal);

        if (constraint.UpperBound is DbValue upper)
            effectiveUpper = Math.Min(effectiveUpper, upper.AsReal);

        if (effectiveUpper < effectiveLower)
        {
            selectivity = 0d;
            return true;
        }

        double totalSpan = max - min;
        if (totalSpan <= 0d)
        {
            selectivity = 1d;
            return true;
        }

        double selectedSpan = effectiveUpper - effectiveLower;
        selectivity = Math.Clamp(selectedSpan / totalSpan, 0d, 1d);
        return true;
    }

    private static int ResolveColumnIndex(TableSchema schema, ColumnRefExpression columnRef)
    {
        return columnRef.TableAlias is { Length: > 0 }
            ? schema.GetQualifiedColumnIndex(columnRef.TableAlias, columnRef.ColumnName)
            : schema.GetColumnIndex(columnRef.ColumnName);
    }

    private static bool TryGetColumnAndLiteral(Expression left, Expression right, out ColumnRefExpression columnRef, out DbValue literal)
    {
        columnRef = null!;
        literal = DbValue.Null;

        if (left is not ColumnRefExpression column || right is not LiteralExpression literalExpression)
            return false;

        if (!TryCreateDbValue(literalExpression, out literal))
            return false;

        columnRef = column;
        return true;
    }

    private static bool TryExtractDiscreteAllowedValues(ColumnConstraint constraint, out HashSet<DbValue> values)
    {
        values = [];

        if (constraint.IsImpossible ||
            constraint.Alternatives is { Count: > 0 } ||
            constraint.NullFilter != NullFilterKind.None ||
            constraint.ExcludedValues is { Count: > 0 } ||
            constraint.LowerBound is DbValue ||
            constraint.UpperBound is DbValue ||
            constraint.AllowedValues is not { Count: > 0 } allowedValues)
        {
            return false;
        }

        values = new HashSet<DbValue>(allowedValues);
        return true;
    }

    private static bool IsUnionCompatibleConstraint(ColumnConstraint constraint)
    {
        return !constraint.IsImpossible &&
               constraint.Alternatives is not { Count: > 0 } &&
               constraint.NullFilter == NullFilterKind.None &&
               constraint.ExcludedValues is not { Count: > 0 };
    }

    private static void CollectOrDisjuncts(Expression expression, List<Expression> output)
    {
        if (expression is BinaryExpression { Op: BinaryOp.Or } orExpr)
        {
            CollectOrDisjuncts(orExpr.Left, output);
            CollectOrDisjuncts(orExpr.Right, output);
            return;
        }

        output.Add(expression);
    }

    private static void NormalizeConstraint(ColumnConstraint constraint)
    {
        if (constraint.IsImpossible)
            return;

        if (constraint.Alternatives is { Count: > 0 } alternatives)
        {
            for (int i = alternatives.Count - 1; i >= 0; i--)
            {
                NormalizeConstraint(alternatives[i]);
                if (alternatives[i].IsImpossible)
                    alternatives.RemoveAt(i);
            }

            if (alternatives.Count == 0)
            {
                constraint.IsImpossible = true;
                return;
            }

            if (alternatives.Count == 1)
            {
                CopyConstraint(constraint, alternatives[0]);
                return;
            }
        }

        if (constraint.LowerBound is DbValue lower && constraint.UpperBound is DbValue upper)
        {
            int cmp = DbValue.Compare(lower, upper);
            if (cmp > 0 || (cmp == 0 && (!constraint.LowerInclusive || !constraint.UpperInclusive)))
            {
                constraint.IsImpossible = true;
                return;
            }
        }

        if (constraint.NullFilter == NullFilterKind.IsNull)
        {
            if (constraint.AllowedValues != null ||
                constraint.ExcludedValues != null ||
                constraint.LowerBound is DbValue ||
                constraint.UpperBound is DbValue)
            {
                constraint.IsImpossible = true;
            }

            return;
        }

        if (constraint.AllowedValues is { } allowedValues)
        {
            allowedValues.RemoveWhere(static value => value.IsNull);
            if (constraint.ExcludedValues is { Count: > 0 } excludedValues)
                allowedValues.ExceptWith(excludedValues);

            allowedValues.RemoveWhere(value => !ConstraintAllowsEquality(constraint, value));
            if (allowedValues.Count == 0)
            {
                constraint.IsImpossible = true;
                return;
            }
        }
    }

    private static List<ColumnConstraint> ExpandAlternatives(ColumnConstraint constraint)
    {
        if (constraint.Alternatives is { Count: > 0 } alternatives)
            return alternatives.Select(CloneConstraint).ToList();

        return [CloneConstraint(constraint)];
    }

    private static ColumnConstraint CloneConstraint(ColumnConstraint source)
    {
        return new ColumnConstraint
        {
            AllowedValues = source.AllowedValues != null ? new HashSet<DbValue>(source.AllowedValues) : null,
            ExcludedValues = source.ExcludedValues != null ? new HashSet<DbValue>(source.ExcludedValues) : null,
            Alternatives = source.Alternatives != null ? source.Alternatives.Select(CloneConstraint).ToList() : null,
            LowerBound = source.LowerBound,
            LowerInclusive = source.LowerInclusive,
            UpperBound = source.UpperBound,
            UpperInclusive = source.UpperInclusive,
            NullFilter = source.NullFilter,
            IsImpossible = source.IsImpossible,
        };
    }

    private static void CopyConstraint(ColumnConstraint target, ColumnConstraint source)
    {
        target.AllowedValues = source.AllowedValues != null ? new HashSet<DbValue>(source.AllowedValues) : null;
        target.ExcludedValues = source.ExcludedValues != null ? new HashSet<DbValue>(source.ExcludedValues) : null;
        target.Alternatives = source.Alternatives != null ? source.Alternatives.Select(CloneConstraint).ToList() : null;
        target.LowerBound = source.LowerBound;
        target.LowerInclusive = source.LowerInclusive;
        target.UpperBound = source.UpperBound;
        target.UpperInclusive = source.UpperInclusive;
        target.NullFilter = source.NullFilter;
        target.IsImpossible = source.IsImpossible;
    }

    private static double MinimumSelectivity(long tableRowCount)
        => 1d / Math.Max(tableRowCount, 1);

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
