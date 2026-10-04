using CSharpDB.Sql;
using CSharpDB.Storage.Indexing;

namespace CSharpDB.Execution;

/// <summary>
/// Value-independent syntax for the deliberately small prepared integer-index path.
/// Unsupported SQL and parameter types continue through ordinary literal binding.
/// </summary>
internal sealed class PreparedSelectTemplate
{
    internal SelectStatement Statement { get; }
    internal ColumnRefExpression KeyColumn { get; }
    internal string[] ParameterNames { get; }
    internal bool HasEquality { get; }
    private readonly Term[] _terms;
    private readonly List<(long[] Values, SelectStatement Statement)> _fallbackStatements = new();
    private readonly object _fallbackGate = new();

    private PreparedSelectTemplate(SelectStatement statement, ColumnRefExpression keyColumn,
        string[] parameterNames, Term[] terms)
    {
        Statement = statement;
        KeyColumn = keyColumn;
        ParameterNames = parameterNames;
        _terms = terms;
        HasEquality = terms.Any(static term => term.Op == BinaryOp.Equals);
    }

    internal static PreparedSelectTemplate? TryCreate(Statement statement, string[] parameterNames)
    {
        if (parameterNames.Length == 0 || statement is not SelectStatement select ||
            select.From is not SimpleTableRef || select.Where is null || select.IsDistinct ||
            select.GroupBy is not null || select.Having is not null || select.WindowDefinitions.Count != 0 ||
            select.OrderBy is { Count: > 1 } ||
            select.OrderBy is { Count: 1 } order && (order[0].Descending || order[0].Expression is not ColumnRefExpression) ||
            !(select.Columns.Count == 1 && select.Columns[0].IsStar) &&
                select.Columns.Any(c => c.IsStar || c.Expression is not ColumnRefExpression))
            return null;

        // ORDER BY may resolve an output alias before a table column with the same name.
        if (select.OrderBy is { Count: 1 } && select.OrderBy[0].Expression is ColumnRefExpression { TableAlias: null } ordered &&
            select.Columns.Any(c => string.Equals(c.Alias, ordered.ColumnName, StringComparison.OrdinalIgnoreCase)))
            return null;

        var terms = new List<Term>();
        ColumnRefExpression? keyColumn = null;
        bool Collect(Expression expression)
        {
            if (expression is not BinaryExpression binary) return false;
            if (binary.Op == BinaryOp.And) return Collect(binary.Left) && Collect(binary.Right);
            if (binary.Op is not (BinaryOp.Equals or BinaryOp.LessThan or BinaryOp.LessOrEqual or
                BinaryOp.GreaterThan or BinaryOp.GreaterOrEqual)) return false;
            var column = binary.Left as ColumnRefExpression ?? binary.Right as ColumnRefExpression;
            if (column is null) return false;
            if (keyColumn is not null &&
                (!string.Equals(keyColumn.ColumnName, column.ColumnName, StringComparison.OrdinalIgnoreCase) ||
                 !string.Equals(keyColumn.TableAlias, column.TableAlias, StringComparison.OrdinalIgnoreCase))) return false;
            keyColumn = column;
            bool reversed = !ReferenceEquals(binary.Left, column);
            Expression value = reversed ? binary.Left : binary.Right;
            int slot = -1;
            long constant = 0;
            if (value is ParameterExpression parameter)
            {
                slot = Array.FindIndex(parameterNames, name => string.Equals(name, parameter.Name, StringComparison.OrdinalIgnoreCase));
                if (slot < 0) return false;
            }
            else if (value is LiteralExpression { LiteralType: TokenType.IntegerLiteral, Value: long integer })
                constant = integer;
            else return false;
            BinaryOp op = reversed ? Reverse(binary.Op) : binary.Op;
            terms.Add(new Term(op, slot, constant));
            return true;
        }

        return Collect(select.Where) && keyColumn is not null
            ? new PreparedSelectTemplate(select, keyColumn, parameterNames, terms.ToArray()) : null;
    }

    internal PreparedSelectStatement Bind(ReadOnlySpan<long> values)
    {
        if (values.Length != ParameterNames.Length) throw new ArgumentException("Parameter count mismatch.", nameof(values));
        return new PreparedSelectStatement(this, values.ToArray());
    }

    internal IndexScanRange GetRange(ReadOnlySpan<long> values)
    {
        long? lower = null, upper = null;
        bool lowerInclusive = true, upperInclusive = true;
        foreach (Term term in _terms)
        {
            long value = term.Slot < 0 ? term.Constant : values[term.Slot];
            if (term.Op is BinaryOp.Equals or BinaryOp.GreaterThan or BinaryOp.GreaterOrEqual)
            {
                bool inclusive = term.Op != BinaryOp.GreaterThan;
                if (lower is null || value > lower) { lower = value; lowerInclusive = inclusive; }
                else if (value == lower) lowerInclusive &= inclusive;
            }
            if (term.Op is BinaryOp.Equals or BinaryOp.LessThan or BinaryOp.LessOrEqual)
            {
                bool inclusive = term.Op != BinaryOp.LessThan;
                if (upper is null || value < upper) { upper = value; upperInclusive = inclusive; }
                else if (value == upper) upperInclusive &= inclusive;
            }
        }
        return new IndexScanRange(lower, lowerInclusive, upper, upperInclusive);
    }

    internal SelectStatement BindLiterals(long[] values)
    {
        // Preserve ordinary statement-identity reuse for unsupported catalogs (e.g. a missing index).
        // These immutable frames belong to executions, never to a mutable ADO parameter collection.
        lock (_fallbackGate)
        {
            for (int i = 0; i < _fallbackStatements.Count; i++)
            {
                var entry = _fallbackStatements[i];
                if (!values.AsSpan().SequenceEqual(entry.Values)) continue;
                if (i != 0) { _fallbackStatements.RemoveAt(i); _fallbackStatements.Insert(0, entry); }
                return entry.Statement;
            }
            SelectStatement statement = CreateBoundStatement(values);
            _fallbackStatements.Insert(0, (values, statement));
            if (_fallbackStatements.Count > 64) _fallbackStatements.RemoveAt(64);
            return statement;
        }
    }

    private SelectStatement CreateBoundStatement(ReadOnlySpan<long> values)
    {
        // Only used when the current catalog cannot use the prepared index path.
        Expression? where = null;
        foreach (Term term in _terms)
        {
            var comparison = new BinaryExpression
            {
                Left = KeyColumn, Op = term.Op,
                Right = new LiteralExpression { LiteralType = TokenType.IntegerLiteral,
                    Value = term.Slot < 0 ? term.Constant : values[term.Slot] },
            };
            where = where is null ? comparison : new BinaryExpression { Left = where, Op = BinaryOp.And, Right = comparison };
        }
        return new SelectStatement
        {
            Columns = Statement.Columns, From = Statement.From, Where = where,
            OrderBy = Statement.OrderBy, Limit = Statement.Limit, Offset = Statement.Offset,
        };
    }

    private static BinaryOp Reverse(BinaryOp op) => op switch
    {
        BinaryOp.LessThan => BinaryOp.GreaterThan, BinaryOp.LessOrEqual => BinaryOp.GreaterOrEqual,
        BinaryOp.GreaterThan => BinaryOp.LessThan, BinaryOp.GreaterOrEqual => BinaryOp.LessOrEqual, _ => op,
    };

    private readonly record struct Term(BinaryOp Op, int Slot, long Constant);
}

/// <summary>Owns this execution's values; neither the command nor a cached plan can change them.</summary>
internal sealed class PreparedSelectStatement(PreparedSelectTemplate template, long[] values) : QueryStatement
{
    internal PreparedSelectTemplate Template { get; } = template;
    internal IndexScanRange GetRange() => Template.GetRange(values);
    internal SelectStatement BindLiterals() => Template.BindLiterals(values);
}
