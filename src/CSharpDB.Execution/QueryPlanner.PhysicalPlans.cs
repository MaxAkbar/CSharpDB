using System.Diagnostics;
using System.Collections;
using System.Text;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

public sealed partial class QueryPlanner
{
    private const string PhysicalProfileDiagnosticsDataKey =
        "CSharpDB.PhysicalProfileDiagnostics";
    private const int MaxFailureDiagnosticNodes = 32;
    private const int MaxFailureDiagnosticCharacters = 2048;

    private async ValueTask<QueryResult> ExecutePhysicalExplainAsync(
        ExplainStatement statement,
        CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        PhysicalPlan plan = statement.Analyze
            ? await ExecuteAndCapturePhysicalPlanAsync(statement.Target, ct)
            : await CapturePhysicalPlanWithoutExecutionAsync(statement.Target, ct);

        // End capture before creating the materialized plan rowset so the
        // formatter's own operator is never included in the target plan.
        return PhysicalPlanResultFormatter.Format(plan);
    }

    private async ValueTask<PhysicalPlan> ExecuteAndCapturePhysicalPlanAsync(
        Statement target,
        CancellationToken ct)
    {
        using PhysicalPlanCaptureScope capture = PhysicalPlanCapture.Begin(collectActuals: true);
        long started = Stopwatch.GetTimestamp();
        long observedRows = 0;
        QueryResult? targetResult = null;
        try
        {
            targetResult = await ExecuteCoreAsync(target, ct);
            if (targetResult.IsQuery)
            {
                while (await targetResult.MoveNextAsync(ct))
                    observedRows++;
            }
            else
            {
                observedRows = targetResult.RowsAffected;
            }

            ct.ThrowIfCancellationRequested();
            await targetResult.DisposeAsync();
            targetResult = null;

            return capture.Context.CreatePlan(
                GetStatementOperatorType(target),
                analyzesTarget: true,
                GetStatementObjectName(target),
                observedRows,
                Stopwatch.GetElapsedTime(started),
                GetRedactedStatementPredicate(target));
        }
        catch (OperationCanceledException ex)
        {
            QueryResult? failedResult = targetResult;
            targetResult = null;
            await DisposeAfterProfileFailureAsync(failedResult);

            string diagnostics = TryBuildFailureDiagnostics(
                capture.Context,
                target,
                observedRows,
                Stopwatch.GetElapsedTime(started),
                cancelled: true);
            throw EnrichProfileCancellation(ex, diagnostics);
        }
        catch (CSharpDbException ex)
        {
            QueryResult? failedResult = targetResult;
            targetResult = null;
            await DisposeAfterProfileFailureAsync(failedResult);

            string diagnostics = TryBuildFailureDiagnostics(
                capture.Context,
                target,
                observedRows,
                Stopwatch.GetElapsedTime(started),
                cancelled: false);
            throw EnrichProfileError(ex, diagnostics);
        }
        catch (Exception ex)
        {
            QueryResult? failedResult = targetResult;
            targetResult = null;
            await DisposeAfterProfileFailureAsync(failedResult);

            string diagnostics = TryBuildFailureDiagnostics(
                capture.Context,
                target,
                observedRows,
                Stopwatch.GetElapsedTime(started),
                cancelled: false);
            TryAttachProfileDiagnostics(ex, diagnostics);
            throw;
        }
        finally
        {
            if (targetResult is not null)
                await targetResult.DisposeAsync();
        }
    }

    private static async ValueTask DisposeAfterProfileFailureAsync(
        QueryResult? result)
    {
        if (result is null)
            return;

        try
        {
            await result.DisposeAsync();
        }
        catch
        {
            // Preserve the execution or cancellation failure that initiated
            // cleanup. The partial operator states are still safe to report.
        }
    }

    private static string BuildFailureDiagnostics(
        PhysicalPlanCaptureContext capture,
        Statement target,
        long observedRows,
        TimeSpan elapsed,
        bool cancelled)
    {
        PhysicalPlan plan = capture.CreatePlan(
            GetStatementOperatorType(target),
            analyzesTarget: true);
        plan.Root.SetActuals(observedRows, loops: 1, elapsed);
        if (cancelled)
            plan.Root.MarkCancelled();
        else
            plan.Root.MarkError("execution_error");

        var summary = new StringBuilder(
            "Physical profile diagnostics (partial): ");
        var pending = new Stack<PhysicalPlanNode>();
        var visited = new HashSet<PhysicalPlanNode>(
            ReferenceEqualityComparer.Instance);
        pending.Push(plan.Root);

        int nodesShown = 0;
        bool truncated = false;
        while (pending.Count > 0)
        {
            PhysicalPlanNode node = pending.Pop();
            if (!visited.Add(node))
            {
                truncated = true;
                continue;
            }

            if (nodesShown >= MaxFailureDiagnosticNodes ||
                summary.Length >= MaxFailureDiagnosticCharacters)
            {
                truncated = true;
                break;
            }

            if (nodesShown > 0)
                summary.Append("; ");

            summary
                .Append(PhysicalPlanResultFormatter.ToStableOperatorName(
                    node.OperatorType))
                .Append("/status=")
                .Append(node.Status.ToString().ToLowerInvariant())
                .Append("/rows=")
                .Append(node.ActualRows?.ToString() ?? "unknown")
                .Append("/loops=")
                .Append(node.ActualLoops?.ToString() ?? "unknown");
            nodesShown++;

            for (int i = node.Children.Count - 1; i >= 0; i--)
                pending.Push(node.Children[i]);
        }

        if (pending.Count > 0)
            truncated = true;
        if (truncated)
            summary.Append("; truncated=true");

        if (summary.Length > MaxFailureDiagnosticCharacters)
            summary.Length = MaxFailureDiagnosticCharacters;

        return summary.ToString();
    }

    private static string TryBuildFailureDiagnostics(
        PhysicalPlanCaptureContext capture,
        Statement target,
        long observedRows,
        TimeSpan elapsed,
        bool cancelled)
    {
        try
        {
            return BuildFailureDiagnostics(
                capture,
                target,
                observedRows,
                elapsed,
                cancelled);
        }
        catch
        {
            // Diagnostics are secondary to the original failure. Return only
            // bounded, non-user-authored fields if the captured topology
            // cannot be summarized safely.
            return "Physical profile diagnostics (partial): " +
                $"{GetStatementOperatorType(target)}/status=" +
                $"{(cancelled ? "cancelled" : "error")}/rows={observedRows}/loops=1; " +
                "truncated=true";
        }
    }

    private static OperationCanceledException EnrichProfileCancellation(
        OperationCanceledException original,
        string diagnostics)
    {
        string message = $"{original.Message} {diagnostics}";
        OperationCanceledException enriched = original is TaskCanceledException
            ? new TaskCanceledException(
                message,
                original,
                original.CancellationToken)
            : new OperationCanceledException(
                message,
                original,
                original.CancellationToken);
        CopyExceptionData(original, enriched);
        enriched.Data[PhysicalProfileDiagnosticsDataKey] = diagnostics;
        return enriched;
    }

    private static CSharpDbException EnrichProfileError(
        CSharpDbException original,
        string diagnostics)
    {
        string message = $"{original.Message} {diagnostics}";
        CSharpDbException enriched = original switch
        {
            CSharpDbConflictException when original.InnerException is not null =>
                new CSharpDbConflictException(
                    message,
                    original.InnerException),
            CSharpDbConflictException =>
                new CSharpDbConflictException(message),
            _ when original.InnerException is not null =>
                new CSharpDbException(
                    original.Code,
                    message,
                    original.InnerException),
            _ => new CSharpDbException(original.Code, message),
        };
        CopyExceptionData(original, enriched);
        enriched.Data[PhysicalProfileDiagnosticsDataKey] = diagnostics;
        return enriched;
    }

    private static void CopyExceptionData(Exception source, Exception destination)
    {
        foreach (DictionaryEntry entry in source.Data)
            destination.Data[entry.Key] = entry.Value;
    }

    private static void TryAttachProfileDiagnostics(
        Exception exception,
        string diagnostics)
    {
        try
        {
            exception.Data[PhysicalProfileDiagnosticsDataKey] = diagnostics;
        }
        catch
        {
            // Diagnostics must never replace the original execution failure.
        }
    }

    private async ValueTask<PhysicalPlan> CapturePhysicalPlanWithoutExecutionAsync(
        Statement target,
        CancellationToken ct)
    {
        using PhysicalPlanCaptureScope capture = PhysicalPlanCapture.Begin(collectActuals: false);

        switch (target)
        {
            case QueryStatement query:
            {
                EnsurePlainExplainCanPlanWithoutExecution(query);
                await using QueryResult planned = await ExecuteQueryAsync(query, ct);
                break;
            }
            case WithStatement:
                throw PlainExplainRequiresAnalyze(
                    "WITH queries currently materialize common table expressions during planning");
            case InsertStatement insert:
                ValidatePlainInsertExplain(insert);
                break;
            case UpdateStatement update:
                await CapturePlainMutationTargetAsync(
                    update.TableName,
                    update.Where,
                    update.SetClauses,
                    ct);
                break;
            case DeleteStatement delete:
                await CapturePlainMutationTargetAsync(
                    delete.TableName,
                    delete.Where,
                    setClauses: null,
                    ct);
                break;
            default:
                throw new CSharpDbException(
                    ErrorCode.SyntaxError,
                    $"Physical EXPLAIN does not support {target.GetType().Name}.");
        }

        ct.ThrowIfCancellationRequested();
        return capture.Context.CreatePlan(
            GetStatementOperatorType(target),
            analyzesTarget: false,
            GetStatementObjectName(target),
            predicate: GetRedactedStatementPredicate(target));
    }

    private void EnsurePlainExplainCanPlanWithoutExecution(QueryStatement query)
    {
        if (ContainsSubqueries(query))
        {
            throw PlainExplainRequiresAnalyze(
                "subqueries can perform eager probe or materialization work during planning");
        }

        switch (query)
        {
            case SelectStatement select:
                if (TableRefContainsView(select.From))
                {
                    throw PlainExplainRequiresAnalyze(
                        "views can perform eager materialization work during planning");
                }
                break;
            case CompoundSelectStatement compound:
                if (compound.Operation != SetOperationKind.Union ||
                    compound.Quantifier != SetQuantifier.All)
                {
                    throw PlainExplainRequiresAnalyze(
                        "duplicate-eliminating compound queries materialize their inputs during planning");
                }

                EnsurePlainExplainCanPlanWithoutExecution(compound.Left);
                EnsurePlainExplainCanPlanWithoutExecution(compound.Right);
                break;
        }
    }

    private bool TableRefContainsView(TableRef tableRef)
        => tableRef switch
        {
            SimpleTableRef simple => _catalog.IsView(simple.TableName),
            JoinTableRef join =>
                TableRefContainsView(join.Left) ||
                TableRefContainsView(join.Right),
            _ => false,
        };

    private void ValidatePlainInsertExplain(InsertStatement statement)
    {
        ThrowIfExternalTableTarget(statement.TableName, "profiled with INSERT");
        TableSchema schema = GetSchema(statement.TableName);
        IReadOnlyList<string> insertColumns =
            (IReadOnlyList<string>?)statement.ColumnNames ?? Array.Empty<string>();
        var seenColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < insertColumns.Count; i++)
        {
            string columnName = insertColumns[i];
            if (schema.GetColumnIndex(columnName) < 0)
            {
                throw new CSharpDbException(
                    ErrorCode.ColumnNotFound,
                    $"Column '{columnName}' not found.");
            }

            if (!seenColumns.Add(columnName))
            {
                throw new CSharpDbException(
                    ErrorCode.SyntaxError,
                    $"Column '{columnName}' is specified more than once.");
            }
        }

        if (!statement.IsDefaultValues)
        {
            int expectedValueCount = insertColumns.Count > 0
                ? insertColumns.Count
                : schema.Columns.Count;
            for (int rowIndex = 0; rowIndex < statement.ValueRows.Count; rowIndex++)
            {
                if (statement.ValueRows[rowIndex].Count != expectedValueCount)
                {
                    throw new CSharpDbException(
                        ErrorCode.SyntaxError,
                        "INSERT column count does not match value count.");
                }
            }
        }

        if (ContainsSubqueries(statement))
        {
            throw PlainExplainRequiresAnalyze(
                "INSERT subqueries can execute while values are rewritten");
        }

        DbFunctionRegistry validationFunctions =
            CreateNonExecutingFunctionRegistry();
        for (int rowIndex = 0; rowIndex < statement.ValueRows.Count; rowIndex++)
        {
            IReadOnlyList<Expression> values = statement.ValueRows[rowIndex];
            for (int valueIndex = 0; valueIndex < values.Count; valueIndex++)
            {
                if (values[valueIndex] is DefaultExpression)
                    continue;

                _ = ExpressionEvaluator.Evaluate(
                    values[valueIndex],
                    Array.Empty<DbValue>(),
                    schema,
                    validationFunctions);
            }
        }
    }

    private DbFunctionRegistry CreateNonExecutingFunctionRegistry()
        => DbFunctionRegistry.Create(builder =>
        {
            foreach (DbScalarFunctionDefinition definition in
                     _functions.ScalarFunctions)
            {
                builder.AddScalar(
                    definition.Name,
                    definition.Arity,
                    definition.Options,
                    static (_, _) => DbValue.Null);
            }
        });

    private async ValueTask CapturePlainMutationTargetAsync(
        string tableName,
        Expression? where,
        IReadOnlyList<SetClause>? setClauses,
        CancellationToken ct)
    {
        ThrowIfExternalTableTarget(tableName, "profiled with a mutation");
        if ((where is not null && ContainsSubqueries(where)) ||
            (setClauses is not null &&
             setClauses.Any(static clause => ContainsSubqueries(clause.Value))))
        {
            throw PlainExplainRequiresAnalyze(
                "mutation subqueries can execute while the target is resolved");
        }

        TableSchema schema = GetSchema(tableName);
        if (setClauses is not null)
        {
            for (int i = 0; i < setClauses.Count; i++)
            {
                SetClause set = setClauses[i];
                if (schema.GetColumnIndex(set.ColumnName) < 0)
                {
                    throw new CSharpDbException(
                        ErrorCode.ColumnNotFound,
                        $"Column '{set.ColumnName}' not found.");
                }

                _ = GetOrCompileSpanExpression(set.Value, schema);
            }
        }

        IOperator source;
        Expression? remainingWhere = where;
        if (HasTemporaryTable(tableName))
        {
            BTree temporaryTree = RequireTemporaryTables().GetTableTree(tableName);
            source = new TableScanOperator(
                temporaryTree,
                schema,
                GetReadSerializer(schema),
                TryGetCachedTreeRowCountCapacityHint(temporaryTree));
        }
        else
        {
            BTree tree = _catalog.GetTableTree(tableName, _pager);
            source = where is not null
                ? TryBuildIndexScan(tableName, where, schema, out remainingWhere)
                  ?? TryBuildIntegerIndexRangeScan(tableName, where, schema, out remainingWhere)
                  ?? TryBuildOrderedTextIndexRangeScan(tableName, where, schema, out remainingWhere)
                  ?? new TableScanOperator(
                      tree,
                      schema,
                      GetReadSerializer(schema),
                      TryGetCachedTreeRowCountCapacityHint(tree))
                : new TableScanOperator(
                    tree,
                    schema,
                    GetReadSerializer(schema),
                    TryGetCachedTreeRowCountCapacityHint(tree));
        }

        if (remainingWhere is not null)
            _ = GetOrCompileSpanExpression(remainingWhere, schema);

        source = PhysicalPlanCapture.WrapRootIfActive(source);
        await source.DisposeAsync();
        ct.ThrowIfCancellationRequested();
    }

    private static CSharpDbException PlainExplainRequiresAnalyze(string reason)
        => new(
            ErrorCode.SyntaxError,
            $"Physical EXPLAIN cannot safely plan this statement without executing it because {reason}. " +
            "Use EXPLAIN ANALYZE to execute and profile it.");

    private static PhysicalOperatorType GetStatementOperatorType(Statement statement)
        => statement switch
        {
            InsertStatement => PhysicalOperatorType.Insert,
            UpdateStatement => PhysicalOperatorType.Update,
            DeleteStatement => PhysicalOperatorType.Delete,
            QueryStatement or WithStatement => PhysicalOperatorType.Query,
            _ => PhysicalOperatorType.Unknown,
        };

    private static string? GetStatementObjectName(Statement statement)
        => statement switch
        {
            InsertStatement insert => insert.TableName,
            UpdateStatement update => update.TableName,
            DeleteStatement delete => delete.TableName,
            _ => null,
        };

    private static string? GetRedactedStatementPredicate(Statement statement)
        => statement switch
        {
            QueryStatement query =>
                GetRedactedQueryPredicate(query),
            WithStatement with =>
                GetRedactedQueryPredicate(with.MainQuery),
            UpdateStatement { Where: not null } update =>
                RedactExpression(update.Where),
            DeleteStatement { Where: not null } delete =>
                RedactExpression(delete.Where),
            _ => null,
        };

    private static IOperator AnnotatePhysicalPredicate(
        IOperator source,
        Expression? predicate)
        => predicate is null
            ? source
            : PhysicalPlanCapture.AnnotatePredicateIfActive(
                source,
                RedactExpression(predicate));

    private static string? GetRedactedQueryPredicate(QueryStatement query)
    {
        var predicates = new List<string>();
        switch (query)
        {
            case SelectStatement select:
                CollectRedactedJoinPredicates(select.From, predicates);
                if (select.Where is not null)
                    predicates.Add($"WHERE {RedactExpression(select.Where)}");
                break;
            case CompoundSelectStatement compound:
                AddIfPresent(GetRedactedQueryPredicate(compound.Left), predicates);
                AddIfPresent(GetRedactedQueryPredicate(compound.Right), predicates);
                break;
        }

        return predicates.Count == 0
            ? null
            : string.Join(" AND ", predicates);
    }

    private static void CollectRedactedJoinPredicates(
        TableRef tableRef,
        List<string> predicates)
    {
        if (tableRef is not JoinTableRef join)
            return;

        CollectRedactedJoinPredicates(join.Left, predicates);
        CollectRedactedJoinPredicates(join.Right, predicates);
        if (join.Condition is not null)
            predicates.Add($"JOIN ON {RedactExpression(join.Condition)}");
    }

    private static void AddIfPresent(string? value, List<string> destination)
    {
        if (!string.IsNullOrEmpty(value))
            destination.Add(value);
    }

    private static string RedactExpression(Expression expression)
        => expression switch
        {
            LiteralExpression or ParameterExpression => "?",
            ColumnRefExpression column => column.TableAlias is null
                ? SqlIdentifierRules.Quote(column.ColumnName)
                : $"{SqlIdentifierRules.Quote(column.TableAlias)}.{SqlIdentifierRules.Quote(column.ColumnName)}",
            BinaryExpression binary =>
                $"({RedactExpression(binary.Left)} {BinaryOpToSql(binary.Op)} {RedactExpression(binary.Right)})",
            UnaryExpression unary => unary.Op == TokenType.Not
                ? $"NOT {RedactExpression(unary.Operand)}"
                : $"-{RedactExpression(unary.Operand)}",
            CollateExpression collate =>
                $"{RedactExpression(collate.Operand)} COLLATE {SqlIdentifierRules.Quote(collate.Collation)}",
            CastExpression cast =>
                $"CAST({RedactExpression(cast.Operand)} AS {cast.TargetType.ToSql()})",
            FunctionCallExpression function =>
                $"{function.FunctionName.ToUpperInvariant()}(" +
                (function.IsStarArg
                    ? "*"
                    : string.Join(", ", function.Arguments.Select(RedactExpression))) +
                ")",
            WindowFunctionExpression window =>
                $"{RedactExpression(window.Function)} OVER (…)",
            LikeExpression like =>
                $"{RedactExpression(like.Operand)}{(like.Negated ? " NOT" : "")} LIKE {RedactExpression(like.Pattern)}" +
                (like.EscapeChar is null
                    ? string.Empty
                    : $" ESCAPE {RedactExpression(like.EscapeChar)}"),
            InExpression inExpression =>
                $"{RedactExpression(inExpression.Operand)}{(inExpression.Negated ? " NOT" : "")} " +
                $"IN ({string.Join(", ", inExpression.Values.Select(RedactExpression))})",
            InSubqueryExpression inSubquery =>
                $"{RedactExpression(inSubquery.Operand)}{(inSubquery.Negated ? " NOT" : "")} IN (subquery)",
            ScalarSubqueryExpression => "(subquery)",
            ExistsExpression => "EXISTS (subquery)",
            BetweenExpression between =>
                $"{RedactExpression(between.Operand)}{(between.Negated ? " NOT" : "")} " +
                $"BETWEEN {RedactExpression(between.Low)} AND {RedactExpression(between.High)}",
            IsNullExpression isNull =>
                $"{RedactExpression(isNull.Operand)} IS{(isNull.Negated ? " NOT" : "")} NULL",
            _ => "predicate",
        };
}
