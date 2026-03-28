using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

internal static class CollationSupport
{
    public const string BinaryCollation = "BINARY";
    public const string NoCaseCollation = "NOCASE";

    public static string? NormalizeMetadataName(string? collation)
    {
        if (string.IsNullOrWhiteSpace(collation))
            return null;

        return collation.Trim().ToUpperInvariant();
    }

    public static bool IsSupported(string? collation)
    {
        string? normalized = NormalizeMetadataName(collation);
        return normalized is null or BinaryCollation or NoCaseCollation;
    }

    public static bool IsNoCase(string? collation) =>
        string.Equals(NormalizeMetadataName(collation), NoCaseCollation, StringComparison.Ordinal);

    public static bool IsBinaryOrDefault(string? collation)
    {
        string? normalized = NormalizeMetadataName(collation);
        return normalized is null or BinaryCollation;
    }

    public static bool SemanticallyEquals(string? left, string? right) =>
        string.Equals(Canonicalize(left), Canonicalize(right), StringComparison.Ordinal);

    public static int Compare(DbValue left, DbValue right, string? collation)
    {
        if (!left.IsNull &&
            !right.IsNull &&
            left.Type == DbType.Text &&
            right.Type == DbType.Text &&
            IsNoCase(collation))
        {
            return CompareText(left.AsText, right.AsText, collation);
        }

        return DbValue.Compare(left, right);
    }

    public static int CompareText(string left, string right, string? collation)
    {
        if (!IsNoCase(collation))
            return string.Compare(left, right, StringComparison.Ordinal);

        return string.Compare(
            NormalizeText(left, collation),
            NormalizeText(right, collation),
            StringComparison.Ordinal);
    }

    public static string NormalizeText(string text, string? collation) =>
        IsNoCase(collation) ? text.ToUpperInvariant() : text;

    public static DbValue NormalizeIndexValue(DbValue value, string? collation)
    {
        if (value.Type == DbType.Text && IsNoCase(collation))
            return DbValue.FromText(NormalizeText(value.AsText, collation));

        return value;
    }

    public static Expression StripCollation(Expression expression)
    {
        while (expression is CollateExpression collate)
            expression = collate.Operand;

        return expression;
    }

    public static string? TryGetExplicitExpressionCollation(Expression expression) =>
        expression is CollateExpression collate
            ? NormalizeMetadataName(collate.Collation)
            : null;

    public static string? ResolveExpressionCollation(Expression expression, TableSchema schema)
    {
        string? explicitCollation = TryGetExplicitExpressionCollation(expression);
        if (explicitCollation != null)
            return explicitCollation;

        expression = StripCollation(expression);
        if (expression is not ColumnRefExpression columnRef)
            return null;

        int columnIndex = ResolveColumnIndex(columnRef, schema);
        if (columnIndex < 0 || columnIndex >= schema.Columns.Count)
            return null;

        return schema.Columns[columnIndex].Type == DbType.Text
            ? NormalizeMetadataName(schema.Columns[columnIndex].Collation)
            : null;
    }

    public static string? ResolveComparisonCollation(Expression left, Expression right, TableSchema schema)
    {
        string? leftExplicit = TryGetExplicitExpressionCollation(left);
        if (leftExplicit != null)
            return leftExplicit;

        string? rightExplicit = TryGetExplicitExpressionCollation(right);
        if (rightExplicit != null)
            return rightExplicit;

        string? leftImplicit = ResolveExpressionCollation(StripCollation(left), schema);
        if (leftImplicit != null)
            return leftImplicit;

        return ResolveExpressionCollation(StripCollation(right), schema);
    }

    public static int ResolveColumnIndex(ColumnRefExpression columnRef, TableSchema schema) =>
        columnRef.TableAlias != null
            ? schema.GetQualifiedColumnIndex(columnRef.TableAlias, columnRef.ColumnName)
            : schema.GetColumnIndex(columnRef.ColumnName);

    public static string? GetEffectiveIndexColumnCollation(
        IndexSchema index,
        TableSchema schema,
        int indexColumnPosition,
        int schemaColumnIndex)
    {
        string? explicitCollation = indexColumnPosition < index.ColumnCollations.Count
            ? NormalizeMetadataName(index.ColumnCollations[indexColumnPosition])
            : null;

        return explicitCollation ?? NormalizeMetadataName(schema.Columns[schemaColumnIndex].Collation);
    }

    public static string?[] GetEffectiveIndexColumnCollations(
        IndexSchema index,
        TableSchema schema,
        ReadOnlySpan<int> schemaColumnIndices)
    {
        var collations = new string?[schemaColumnIndices.Length];
        for (int i = 0; i < schemaColumnIndices.Length; i++)
            collations[i] = GetEffectiveIndexColumnCollation(index, schema, i, schemaColumnIndices[i]);

        return collations;
    }

    public static bool CanUseIndexForLookup(
        IndexSchema index,
        TableSchema schema,
        ReadOnlySpan<int> schemaColumnIndices,
        ReadOnlySpan<string?> queryCollations = default)
    {
        for (int i = 0; i < schemaColumnIndices.Length; i++)
        {
            int columnIndex = schemaColumnIndices[i];
            if (columnIndex < 0 || columnIndex >= schema.Columns.Count)
                return false;

            if (schema.Columns[columnIndex].Type != DbType.Text)
                continue;

            string? queryCollation = queryCollations.IsEmpty || i >= queryCollations.Length
                ? NormalizeMetadataName(schema.Columns[columnIndex].Collation)
                : NormalizeMetadataName(queryCollations[i]);
            string? indexCollation = GetEffectiveIndexColumnCollation(index, schema, i, columnIndex);
            if (!SemanticallyEquals(queryCollation, indexCollation))
                return false;
        }

        return true;
    }

    private static string Canonicalize(string? collation) =>
        NormalizeMetadataName(collation) ?? BinaryCollation;
}
