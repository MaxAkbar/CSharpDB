namespace CSharpDB.Sql;

public sealed record SqlDependencyRelation(string Name, IReadOnlyList<string> Columns);
public sealed record SqlDependencyReference(string Relation, string? Column, string? OutputColumn,
    string Usage, SqlSourceSpan Span, bool Resolved = true);
public sealed record SqlDependencyDiagnostic(string Message, SqlSourceSpan Span);
public sealed record SqlDependencyAnalysis(IReadOnlyList<SqlDependencyReference> References,
    IReadOnlyList<string> OutputColumns, IReadOnlyList<SqlDependencyDiagnostic> Diagnostics);

/// <summary>Inspects syntax and supplied catalog names only; never invokes the query planner or a SQL function.</summary>
public sealed class SqlDependencyAnalyzer
{
    public SqlDependencyAnalysis Analyze(string sql, Func<string, SqlDependencyRelation?> resolve,
        string? ownerTable = null, bool expressionOnly = false, CancellationToken ct = default)
    {
        var visitor = new Visitor(resolve, ownerTable, ct);
        try
        {
            if (expressionOnly)
            {
                visitor.Span = new(0, sql.Length, 1, 1);
                visitor.Expression(Parser.ParseExpressionSql(sql), visitor.OwnerScope(), null, "Expression");
            }
            else
                foreach (var statement in SqlScriptParser.Parse(sql, cancellationToken: ct))
                {
                    visitor.Span = statement.Span;
                    visitor.Statement(statement.Statement, visitor.OwnerScope());
                }
        }
        catch (OperationCanceledException) { throw; }
        catch (CSharpDB.Primitives.CSharpDbException ex)
        { visitor.Diagnostics.Add(new(ex.Message, ex is SqlScriptParseException script ? script.Span : visitor.Span)); }
        return new(visitor.References.Distinct().ToArray(), visitor.Outputs, visitor.Diagnostics);
    }

    private sealed class Binding(string name, IReadOnlyList<string> columns,
        IReadOnlyList<SqlDependencyReference>? expansion = null)
    {
        public string Name { get; } = name;
        public IReadOnlyList<string> Columns { get; } = columns;
        public IReadOnlyList<SqlDependencyReference>? Expansion { get; } = expansion;
    }

    private sealed class Scope(Scope? parent = null)
    {
        public Scope? Parent { get; } = parent;
        public Dictionary<string, Binding> Bindings { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, Binding> Ctes { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Binding? Cte(string name) => Ctes.GetValueOrDefault(name) ?? Parent?.Cte(name);
        public Binding? Qualifier(string name) => Bindings.GetValueOrDefault(name) ?? Parent?.Qualifier(name);
    }

    private sealed class Visitor(Func<string, SqlDependencyRelation?> resolve, string? owner, CancellationToken ct)
    {
        public readonly List<SqlDependencyReference> References = [];
        public readonly List<SqlDependencyDiagnostic> Diagnostics = [];
        public IReadOnlyList<string> Outputs = [];
        public SqlSourceSpan Span = new(0, 0, 1, 1);
        private static bool Same(string? a, string? b) => string.Equals(a, b, StringComparison.OrdinalIgnoreCase);

        public Scope OwnerScope()
        {
            var scope = new Scope();
            if (owner is not null)
            {
                var relation = resolve(owner);
                var binding = new Binding(relation?.Name ?? owner, relation?.Columns ?? []);
                scope.Bindings[owner] = binding;
                scope.Bindings["OLD"] = binding;
                scope.Bindings["NEW"] = binding;
            }
            return scope;
        }

        private void Add(Binding binding, string? column, string? output, string usage)
        {
            ct.ThrowIfCancellationRequested();
            if (binding.Expansion is { } expansion)
            {
                References.AddRange(expansion.Where(r => column is null || r.OutputColumn is null || Same(r.OutputColumn, column))
                    .Select(r => r with { OutputColumn = output, Span = Span }));
                return;
            }
            bool resolved = resolve(binding.Name) is not null && (column is null || binding.Columns.Any(c => Same(c, column)));
            References.Add(new(binding.Name, column, output, usage, Span, resolved));
            if (!resolved) Diagnostics.Add(new($"Unresolved reference: {binding.Name}{(column is null ? "" : "." + column)}.", Span));
        }

        private Binding Table(string name, Scope scope, string? alias = null)
        {
            Binding binding = scope.Cte(name) ?? (resolve(name) is { } relation
                ? new Binding(relation.Name, relation.Columns) : new Binding(name, []));
            scope.Bindings[alias ?? name] = binding;
            if (binding.Expansion is null) Add(binding, null, null, "Table");
            else References.AddRange(binding.Expansion.Where(r => r.OutputColumn is null));
            return binding;
        }

        private void From(TableRef table, Scope scope)
        {
            switch (table)
            {
                case SimpleTableRef simple: Table(simple.TableName, scope, simple.Alias); break;
                case JoinTableRef join:
                    From(join.Left, scope); From(join.Right, scope);
                    Expression(join.Condition, scope, null, "Predicate"); break;
                case SingleRowTableRef: break;
                default: Diagnostics.Add(new($"Unsupported table source: {table.GetType().Name}.", Span)); break;
            }
        }

        public void Statement(Statement statement, Scope scope)
        {
            ct.ThrowIfCancellationRequested();
            switch (statement)
            {
                case WithStatement with:
                {
                    var local = new Scope(scope);
                    foreach (var cte in with.Ctes)
                    {
                        int start = References.Count;
                        var columns = Query(cte.Query, local);
                        var expansion = References.Skip(start).ToArray();
                        References.RemoveRange(start, References.Count - start);
                        if (cte.ColumnNames is { } names)
                        {
                            expansion = expansion.Select(r => r with
                            {
                                OutputColumn = r.OutputColumn is null ? null : names.ElementAtOrDefault(columns.ToList().FindIndex(c => Same(c, r.OutputColumn))) ?? r.OutputColumn,
                            }).ToArray();
                            columns = names;
                        }
                        local.Ctes[cte.Name] = new Binding(cte.Name, columns, expansion);
                    }
                    Outputs = Query(with.MainQuery, local); break;
                }
                case QueryStatement query: Outputs = Query(query, scope); break;
                case InsertStatement insert:
                {
                    var local = new Scope(scope);
                    var binding = Table(insert.TableName, local);
                    if (insert.ColumnNames is null && !insert.IsDefaultValues) Add(binding, null, null, "Positional insert");
                    foreach (string column in insert.ColumnNames ?? []) Add(binding, column, null, "Write");
                    foreach (var row in insert.ValueRows) foreach (var value in row) Expression(value, scope, null, "Read");
                    break;
                }
                case UpdateStatement update:
                {
                    var local = new Scope(scope); var binding = Table(update.TableName, local);
                    foreach (var set in update.SetClauses) { Add(binding, set.ColumnName, null, "Write"); Expression(set.Value, local, null, "Read"); }
                    Expression(update.Where, local, null, "Predicate"); break;
                }
                case DeleteStatement delete:
                {
                    var local = new Scope(scope); Table(delete.TableName, local); Expression(delete.Where, local, null, "Predicate"); break;
                }
                case ConditionalStatement conditional:
                    Query(conditional.ExistsQuery, scope);
                    foreach (var child in conditional.Body) Statement(child, scope); break;
                case CreateViewStatement view: Outputs = Query(view.Query, scope); break;
                case CreateTriggerStatement trigger:
                    var triggerScope = new Scope(scope); var target = Table(trigger.TableName, triggerScope);
                    triggerScope.Bindings["OLD"] = target; triggerScope.Bindings["NEW"] = target;
                    Expression(trigger.WhenCondition, triggerScope, null, "Predicate");
                    foreach (var child in trigger.Body) Statement(child, triggerScope); break;
                case ExplainStatement explain: Statement(explain.Target, scope); break;
                case ExplainEstimateStatement explain: Statement(explain.Target, scope); break;
                case CreateIndexStatement index:
                    var indexed = Table(index.TableName, new Scope(scope));
                    foreach (var column in index.Columns) Add(indexed, column, null, "Index"); break;
                case CreateValidationRuleStatement rule:
                    var ruleScope = new Scope(scope); Table(rule.TableName, ruleScope);
                    Expression(rule.Expression, ruleScope, null, "Validation"); break;
                default: Diagnostics.Add(new($"Dependency analysis does not support {statement.GetType().Name}; review this statement.", Span)); break;
            }
        }

        private IReadOnlyList<string> Query(QueryStatement query, Scope parent)
        {
            ct.ThrowIfCancellationRequested();
            if (query is CompoundSelectStatement compound)
            {
                var left = Query(compound.Left, parent);
                int start = References.Count;
                var right = Query(compound.Right, parent);
                for (int i = start; i < References.Count; i++)
                    if (References[i].OutputColumn is { } output)
                        References[i] = References[i] with { OutputColumn = left.ElementAtOrDefault(right.ToList().FindIndex(c => Same(c, output))) ?? output };
                foreach (var order in compound.OrderBy ?? [])
                    if (order.Expression is not ColumnRefExpression column || !left.Any(c => Same(c, column.ColumnName)))
                        Expression(order.Expression, parent, null, "Order");
                return left;
            }
            if (query is not SelectStatement select)
            {
                Diagnostics.Add(new($"Dependency analysis does not support {query.GetType().Name}.", Span)); return [];
            }
            var local = new Scope(parent);
            From(select.From, local);
            var outputs = new List<string>();
            foreach (var column in select.Columns)
            {
                if (column.IsStar)
                {
                    foreach (var binding in local.Bindings.Values.Distinct())
                    {
                        Add(binding, null, null, "Wildcard");
                        foreach (string name in binding.Columns) { outputs.Add(name); Add(binding, name, name, "Projection"); }
                    }
                }
                else
                {
                    string name = column.Alias ?? (column.Expression as ColumnRefExpression)?.ColumnName ?? $"expression{outputs.Count + 1}";
                    outputs.Add(name);
                    Expression(column.Expression, local, name, "Projection");
                }
            }
            Expression(select.Where, local, null, "Predicate");
            Expression(select.Having, local, null, "Predicate");
            foreach (var group in select.GroupBy ?? []) Expression(group, local, null, "Grouping");
            foreach (var order in select.OrderBy ?? [])
            {
                if (order.Expression is ColumnRefExpression { TableAlias: null } reference && outputs.Any(c => Same(c, reference.ColumnName))) continue;
                Expression(order.Expression, local, null, "Order");
            }
            foreach (var window in select.WindowDefinitions) Window(window.Specification, local, null);
            return outputs;
        }

        public void Expression(Expression? expression, Scope scope, string? output, string usage)
        {
            ct.ThrowIfCancellationRequested();
            switch (expression)
            {
                case null or LiteralExpression or ParameterExpression or DefaultExpression: return;
                case ColumnRefExpression column:
                {
                    if (column.TableAlias is { } qualifier)
                    {
                        var binding = scope.Qualifier(qualifier);
                        if (binding is not null) Add(binding, column.ColumnName, output, usage);
                        else { References.Add(new(qualifier, column.ColumnName, output, usage, Span, false)); Diagnostics.Add(new($"Unresolved qualifier '{qualifier}'.", Span)); }
                        return;
                    }
                    for (Scope? cursor = scope; cursor is not null; cursor = cursor.Parent)
                    {
                        var matches = cursor.Bindings.Values.Distinct().Where(b => b.Columns.Any(c => Same(c, column.ColumnName))).ToArray();
                        if (matches.Length == 1) { Add(matches[0], column.ColumnName, output, usage); return; }
                        if (matches.Length > 1) { Diagnostics.Add(new($"Ambiguous column '{column.ColumnName}'.", Span)); return; }
                    }
                    Diagnostics.Add(new($"Unresolved column '{column.ColumnName}'.", Span)); return;
                }
                case BinaryExpression binary: Expression(binary.Left, scope, output, usage); Expression(binary.Right, scope, output, usage); break;
                case UnaryExpression unary: Expression(unary.Operand, scope, output, usage); break;
                case CastExpression cast: Expression(cast.Operand, scope, output, usage); break;
                case CollateExpression collate: Expression(collate.Operand, scope, output, usage); break;
                case LikeExpression like: Expression(like.Operand, scope, output, usage); Expression(like.Pattern, scope, output, usage); Expression(like.EscapeChar, scope, output, usage); break;
                case InExpression inside: Expression(inside.Operand, scope, output, usage); foreach (var item in inside.Values) Expression(item, scope, output, usage); break;
                case BetweenExpression between: Expression(between.Operand, scope, output, usage); Expression(between.Low, scope, output, usage); Expression(between.High, scope, output, usage); break;
                case IsNullExpression isNull: Expression(isNull.Operand, scope, output, usage); break;
                case FunctionCallExpression function: foreach (var arg in function.Arguments) Expression(arg, scope, output, usage); break;
                case WindowFunctionExpression window: Expression(window.Function, scope, output, usage); Window(window.Window, scope, output); break;
                case ScalarSubqueryExpression scalar: Subquery(scalar.Query, scope, output); break;
                case ExistsExpression exists: Subquery(exists.Query, scope, output); break;
                case InSubqueryExpression subquery: Expression(subquery.Operand, scope, output, usage); Subquery(subquery.Query, scope, output); break;
                default: Diagnostics.Add(new($"Unsupported expression {expression.GetType().Name}.", Span)); break;
            }
        }

        private void Subquery(QueryStatement query, Scope scope, string? output)
        {
            int start = References.Count; Query(query, scope);
            for (int i = start; i < References.Count; i++) References[i] = References[i] with { OutputColumn = output };
        }
        private void Window(WindowSpecification window, Scope scope, string? output)
        {
            foreach (var partition in window.PartitionBy) Expression(partition, scope, output, "Window");
            foreach (var order in window.OrderBy) Expression(order.Expression, scope, output, "Window");
        }
    }
}
