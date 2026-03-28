using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

internal static class CollationSupport
{
    public const string BinaryCollation = "BINARY";
    public const string NoCaseCollation = "NOCASE";
    public const string NoCaseAccentInsensitiveCollation = "NOCASE_AI";
    public const string IcuCollationPrefix = "ICU:";

    private static readonly RegisteredCollation BinaryDefinition = new BinaryCollationDefinition();
    private static readonly RegisteredCollation NoCaseDefinition = new NoCaseCollationDefinition();
    private static readonly RegisteredCollation NoCaseAccentInsensitiveDefinition = new NoCaseAccentInsensitiveCollationDefinition();

    private static readonly string[] SupportedCollationNames =
    [
        BinaryCollation,
        NoCaseCollation,
        NoCaseAccentInsensitiveCollation,
        "ICU:<locale>",
    ];

    private static readonly ConcurrentDictionary<string, RegisteredCollation> Registry =
        new(StringComparer.Ordinal)
        {
            [BinaryCollation] = BinaryDefinition,
            [NoCaseCollation] = NoCaseDefinition,
            [NoCaseAccentInsensitiveCollation] = NoCaseAccentInsensitiveDefinition,
        };

    public static string DescribeSupportedCollations() => string.Join(", ", SupportedCollationNames);

    public static string? NormalizeMetadataName(string? collation)
    {
        if (string.IsNullOrWhiteSpace(collation))
            return null;

        string normalized = collation.Trim().ToUpperInvariant();
        return TryResolveRegistered(normalized, out var definition)
            ? definition.Name
            : normalized;
    }

    public static bool IsSupported(string? collation)
    {
        string? normalized = NormalizeMetadataName(collation);
        return normalized is null || TryResolveRegistered(normalized, out _);
    }

    public static bool IsNoCase(string? collation) =>
        string.Equals(NormalizeMetadataName(collation), NoCaseCollation, StringComparison.Ordinal);

    public static bool IsBinaryOrDefault(string? collation) =>
        ResolveOrDefault(collation).IsBinaryLike;

    public static bool SemanticallyEquals(string? left, string? right) =>
        string.Equals(Canonicalize(left), Canonicalize(right), StringComparison.Ordinal);

    public static int Compare(DbValue left, DbValue right, string? collation)
    {
        if (!left.IsNull &&
            !right.IsNull &&
            left.Type == DbType.Text &&
            right.Type == DbType.Text)
        {
            return CompareText(left.AsText, right.AsText, collation);
        }

        return DbValue.Compare(left, right);
    }

    public static int CompareText(string left, string right, string? collation) =>
        ResolveOrDefault(collation).CompareText(left, right);

    public static string NormalizeText(string text, string? collation) =>
        ResolveOrDefault(collation).NormalizeText(text);

    public static DbValue NormalizeIndexValue(DbValue value, string? collation)
    {
        if (value.Type == DbType.Text)
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

    private static string Canonicalize(string? collation)
    {
        string? normalized = NormalizeMetadataName(collation);
        if (normalized == null)
            return BinaryCollation;

        return TryResolveRegistered(normalized, out var definition)
            ? definition.Name
            : normalized;
    }

    private static RegisteredCollation ResolveOrDefault(string? collation)
    {
        string? normalized = NormalizeMetadataName(collation);
        if (normalized != null && TryResolveRegistered(normalized, out var definition))
            return definition;

        return BinaryDefinition;
    }

    private static bool TryResolveRegistered(string normalized, out RegisteredCollation definition) =>
        Registry.TryGetValue(normalized, out definition!) || TryResolveIcuCollation(normalized, out definition);

    private static bool TryResolveIcuCollation(string normalized, out RegisteredCollation definition)
    {
        definition = null!;
        if (!normalized.StartsWith(IcuCollationPrefix, StringComparison.Ordinal))
            return false;

        string locale = normalized[IcuCollationPrefix.Length..];
        if (string.IsNullOrWhiteSpace(locale))
            return false;

        CultureInfo culture;
        try
        {
            culture = CultureInfo.GetCultureInfo(locale);
        }
        catch (CultureNotFoundException)
        {
            return false;
        }

        string canonicalName = $"{IcuCollationPrefix}{culture.Name}";
        definition = Registry.GetOrAdd(canonicalName, static (_, state) => new IcuCollationDefinition(state), culture);
        return true;
    }

    private abstract class RegisteredCollation
    {
        protected RegisteredCollation(string name, bool isBinaryLike)
        {
            Name = name;
            IsBinaryLike = isBinaryLike;
        }

        public string Name { get; }

        public bool IsBinaryLike { get; }

        public virtual int CompareText(string left, string right) =>
            string.Compare(left, right, StringComparison.Ordinal);

        public virtual string NormalizeText(string text) => text;
    }

    private sealed class BinaryCollationDefinition : RegisteredCollation
    {
        public BinaryCollationDefinition()
            : base(BinaryCollation, isBinaryLike: true)
        {
        }
    }

    private sealed class NoCaseCollationDefinition : RegisteredCollation
    {
        public NoCaseCollationDefinition()
            : base(NoCaseCollation, isBinaryLike: false)
        {
        }

        public override int CompareText(string left, string right) =>
            string.Compare(NormalizeText(left), NormalizeText(right), StringComparison.Ordinal);

        public override string NormalizeText(string text) => text.ToUpperInvariant();
    }

    private sealed class NoCaseAccentInsensitiveCollationDefinition : RegisteredCollation
    {
        public NoCaseAccentInsensitiveCollationDefinition()
            : base(NoCaseAccentInsensitiveCollation, isBinaryLike: false)
        {
        }

        public override int CompareText(string left, string right) =>
            string.Compare(NormalizeText(left), NormalizeText(right), StringComparison.Ordinal);

        public override string NormalizeText(string text)
        {
            string decomposed = text.Normalize(NormalizationForm.FormD);
            var builder = new StringBuilder(decomposed.Length);

            for (int i = 0; i < decomposed.Length; i++)
            {
                char ch = decomposed[i];
                UnicodeCategory category = CharUnicodeInfo.GetUnicodeCategory(ch);
                if (category is UnicodeCategory.NonSpacingMark or UnicodeCategory.SpacingCombiningMark or UnicodeCategory.EnclosingMark)
                    continue;

                builder.Append(char.ToUpperInvariant(ch));
            }

            return builder.ToString().Normalize(NormalizationForm.FormC);
        }
    }

    private sealed class IcuCollationDefinition : RegisteredCollation
    {
        private readonly CompareInfo _compareInfo;

        public IcuCollationDefinition(CultureInfo culture)
            : base($"{IcuCollationPrefix}{culture.Name}", isBinaryLike: false)
        {
            _compareInfo = culture.CompareInfo;
        }

        public override int CompareText(string left, string right) =>
            string.CompareOrdinal(NormalizeText(left), NormalizeText(right));

        public override string NormalizeText(string text) =>
            Convert.ToHexString(_compareInfo.GetSortKey(text, CompareOptions.None).KeyData);
    }
}
