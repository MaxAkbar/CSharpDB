using System.Collections;
using System.Globalization;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Primitives;
using CSharpDB.Sql;
#if CSHARPDB_SQLSERVER_QUERY_ANALYZER
using CSharpDB.Migration.Compatibility;
using Microsoft.SqlServer.TransactSql.ScriptDom;
#endif
using CSharpDbSelectStatement = CSharpDB.Sql.SelectStatement;
#if CSHARPDB_SQLSERVER_QUERY_ANALYZER
using TSqlSelectStatement = Microsoft.SqlServer.TransactSql.ScriptDom.SelectStatement;
#endif

#if CSHARPDB_SQLSERVER_QUERY_ANALYZER
namespace CSharpDB.Migration.SqlServer;
#else
namespace CSharpDB.Migration.Compatibility;
#endif

/// <summary>
/// A bounded, read-only static checker. A conditional result means that both
/// source and candidate target syntax parsed, not that names, parameter types,
/// or semantics were proven.
/// </summary>
#if CSHARPDB_SQLSERVER_QUERY_ANALYZER
public sealed class SqlServerQueryCompatibilityAnalyzer
#else
public sealed class QueryCompatibilityAnalyzer
#endif
{
#if CSHARPDB_SQLSERVER_QUERY_ANALYZER
    private const string TopToLimitRewriteId = "tsql-top-integer-to-csharpdb-limit/v1";
#endif

    public QueryCompatibilityReport Analyze(
        QueryCompatibilityRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ValidateRequest(request);

        CSharpDbCapabilityCatalog capabilityCatalog =
            CSharpDbCapabilityCatalogLoader.LoadEmbedded(
                request.TargetCSharpDbVersion);

        long totalBytes = 0;
        var queryIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (QueryCompatibilityInput query in request.Queries)
        {
            ArgumentNullException.ThrowIfNull(query);
            ValidateInput(query);
            if (!queryIds.Add(query.QueryId))
            {
                throw new ArgumentException(
                    $"Query pack contains duplicate query id '{query.QueryId}'.",
                    nameof(request));
            }

            int queryBytes = Utf8ByteCount(query.Sql);
            if (queryBytes > request.Limits.MaxQueryBytes)
            {
                throw new ArgumentException(
                    $"Query '{query.QueryId}' exceeds the per-query byte limit.",
                    nameof(request));
            }

            if (totalBytes > request.Limits.MaxTotalQueryBytes - queryBytes)
            {
                throw new ArgumentException(
                    "Query pack exceeds the aggregate byte limit.",
                    nameof(request));
            }
            totalBytes += queryBytes;
        }

        QueryCompatibilityResult[] results = request.Queries
            .OrderBy(static item => item.QueryId, StringComparer.Ordinal)
            .Select(item =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                return AnalyzeOne(item, request, cancellationToken);
            })
            .ToArray();

        return new QueryCompatibilityReport
        {
            TargetCSharpDbVersion = request.TargetCSharpDbVersion,
            CapabilityDigest = capabilityCatalog.Digest,
            Summary = Summarize(results),
            Results = results,
        };
    }

    private static QueryCompatibilityResult AnalyzeOne(
        QueryCompatibilityInput input,
        QueryCompatibilityRequest request,
        CancellationToken cancellationToken) =>
        input.SourceDialect switch
        {
            QuerySourceDialect.CSharpDb => AnalyzeCSharpDb(
                input,
                request.Limits,
                cancellationToken),
            QuerySourceDialect.SqlServerTsql =>
#if CSHARPDB_SQLSERVER_QUERY_ANALYZER
                AnalyzeTsql(
                input,
                request,
                cancellationToken),
#else
                UnknownDialect(input),
#endif
            QuerySourceDialect.MySql or
                QuerySourceDialect.Sqlite or
                QuerySourceDialect.Access => UnknownDialect(input),
            _ => throw new ArgumentOutOfRangeException(
                nameof(input),
                input.SourceDialect,
                "Unknown query source dialect."),
        };

    private static QueryCompatibilityResult AnalyzeCSharpDb(
        QueryCompatibilityInput input,
        QueryCompatibilityLimits limits,
        CancellationToken cancellationToken)
    {
        string sourceDigest = Digest(input.Sql);
        var diagnostics = new List<MigrationDiagnostic>();
        ParseCSharpDbResult parsed = TryParseCSharpDb(
            input.Sql,
            limits,
            cancellationToken);
        if (parsed.LimitExceeded)
        {
            diagnostics.Add(Diagnostic(
                input.QueryId,
                QueryCompatibilityRuleIds.InputLimitExceeded,
                0,
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "The query exceeded a static-analysis safety limit.",
                "No compatibility conclusion was made because bounded token or nesting analysis stopped before parsing.",
                "Reduce the query size or split the query pack entry."));
            return Result(
                input,
                sourceDigest,
                MigrationCompatibilityStatus.Unknown,
                evidence: null,
                sourceParsed: false,
                targetParsed: false,
                isReadOnly: null,
                diagnostics);
        }

        if (parsed.Statement is null)
        {
            diagnostics.Add(Diagnostic(
                input.QueryId,
                QueryCompatibilityRuleIds.SourceParseFailed,
                0,
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "The query did not parse as CSharpDB SQL.",
                "The declared source dialect parser rejected the query. Parser exception text is intentionally not retained in the report.",
                "Correct the source dialect or query syntax."));
            return Result(
                input,
                sourceDigest,
                MigrationCompatibilityStatus.Unknown,
                evidence: null,
                sourceParsed: false,
                targetParsed: false,
                isReadOnly: null,
                diagnostics);
        }

        bool readOnly = SqlStatementClassifier.IsReadOnly(parsed.Statement);
        if (!readOnly)
        {
            diagnostics.Add(Diagnostic(
                input.QueryId,
                QueryCompatibilityRuleIds.NotReadOnly,
                0,
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "Only read-only query statements are accepted.",
                "The statement parsed, but the query checker does not analyze writes, DDL, or other state-changing operations.",
                "Provide a read-only SELECT or equivalent CSharpDB query."));
            return Result(
                input,
                sourceDigest,
                MigrationCompatibilityStatus.Unsupported,
                MigrationEvidenceLevel.Parsed,
                sourceParsed: true,
                targetParsed: true,
                isReadOnly: false,
                diagnostics);
        }

        AddCSharpDbFeatureDiagnostics(input.QueryId, parsed.Statement, diagnostics);
        AddUnboundDiagnostic(input.QueryId, diagnostics);
        return Result(
            input,
            sourceDigest,
            StaticResultStatus(diagnostics),
            MigrationEvidenceLevel.Parsed,
            sourceParsed: true,
            targetParsed: true,
            isReadOnly: true,
            diagnostics);
    }

#if CSHARPDB_SQLSERVER_QUERY_ANALYZER
    private static QueryCompatibilityResult AnalyzeTsql(
        QueryCompatibilityInput input,
        QueryCompatibilityRequest request,
        CancellationToken cancellationToken)
    {
        string sourceDigest = Digest(input.Sql);
        var diagnostics = new List<MigrationDiagnostic>();
        TSqlParser? parser = SelectTsqlParser(
            request.SqlServerCompatibilityLevel,
            request.SqlServerQuotedIdentifiers);
        if (parser is null)
        {
            diagnostics.Add(Diagnostic(
                input.QueryId,
                QueryCompatibilityRuleIds.DialectUnqualified,
                0,
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "The SQL Server grammar is not qualified.",
                "Only SQL Server compatibility levels 150, 160, and 170 have a selected ScriptDom grammar.",
                "Select a qualified compatibility level or add a separately tested dialect adapter."));
            return Result(
                input,
                sourceDigest,
                MigrationCompatibilityStatus.Unknown,
                evidence: null,
                sourceParsed: false,
                targetParsed: false,
                isReadOnly: null,
                diagnostics);
        }

        TSqlParseResult source = TryParseTsql(
            input.Sql,
            parser,
            request.Limits,
            cancellationToken);
        if (source.LimitExceeded)
        {
            diagnostics.Add(Diagnostic(
                input.QueryId,
                QueryCompatibilityRuleIds.InputLimitExceeded,
                0,
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "The query exceeded a static-analysis safety limit.",
                "No compatibility conclusion was made because bounded token, AST, error, or nesting analysis stopped.",
                "Reduce the query size or split the query pack entry."));
            return Result(
                input,
                sourceDigest,
                MigrationCompatibilityStatus.Unknown,
                evidence: null,
                sourceParsed: false,
                targetParsed: false,
                isReadOnly: null,
                diagnostics);
        }

        if (source.Fragment is null || source.Errors.Count != 0)
        {
            ParseError? first = source.Errors.FirstOrDefault();
            diagnostics.Add(Diagnostic(
                input.QueryId,
                QueryCompatibilityRuleIds.SourceParseFailed,
                0,
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "The query did not parse as qualified SQL Server T-SQL.",
                first is null
                    ? "ScriptDom rejected the query. Parser exception text is intentionally not retained in the report."
                    : $"ScriptDom reported parser error {first.Number}. Parser message text is intentionally not retained in the report.",
                "Correct the source dialect or query syntax.",
                first is null
                    ? null
                    : new MigrationSourceSpan
                    {
                        Start = first.Offset,
                        Line = first.Line,
                        Column = first.Column,
                    }));
            return Result(
                input,
                sourceDigest,
                MigrationCompatibilityStatus.Unknown,
                evidence: null,
                sourceParsed: false,
                targetParsed: false,
                isReadOnly: null,
                diagnostics);
        }

        IReadOnlyList<TSqlStatement> statements = GetStatements(source.Fragment);
        if (statements.Count != 1)
        {
            diagnostics.Add(Diagnostic(
                input.QueryId,
                QueryCompatibilityRuleIds.MultipleStatements,
                0,
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "A query-pack entry must contain exactly one statement.",
                "Multi-statement batches are not analyzed because their session state and side effects require execution-level evidence.",
                "Split the batch into one independently identified read-only query per entry."));
            return Result(
                input,
                sourceDigest,
                MigrationCompatibilityStatus.Unsupported,
                MigrationEvidenceLevel.Parsed,
                sourceParsed: true,
                targetParsed: false,
                isReadOnly: false,
                diagnostics);
        }

        TSqlFeatureSnapshot features = InspectTsql(
            statements[0],
            request.Limits,
            cancellationToken);
        if (features.LimitExceeded)
        {
            diagnostics.Add(Diagnostic(
                input.QueryId,
                QueryCompatibilityRuleIds.InputLimitExceeded,
                0,
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "The query exceeded an AST analysis safety limit.",
                "The source parsed, but bounded feature inspection did not complete.",
                "Reduce the query complexity."));
            return Result(
                input,
                sourceDigest,
                MigrationCompatibilityStatus.Unknown,
                MigrationEvidenceLevel.Parsed,
                sourceParsed: true,
                targetParsed: false,
                isReadOnly: null,
                diagnostics);
        }

        AddTsqlFeatureDiagnostics(input.QueryId, features, diagnostics);
        bool readOnly = statements[0] is TSqlSelectStatement &&
            !features.HasSelectInto &&
            !features.HasSelectAssignment &&
            !features.HasSequenceMutation;
        if (!readOnly)
        {
            diagnostics.Add(Diagnostic(
                input.QueryId,
                QueryCompatibilityRuleIds.NotReadOnly,
                0,
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "Only read-only SQL Server SELECT statements are accepted.",
                "The source statement contains a write, SELECT INTO, variable assignment, sequence mutation, or another state-changing operation.",
                "Provide a side-effect-free SELECT query."));
            return Result(
                input,
                sourceDigest,
                MigrationCompatibilityStatus.Unsupported,
                MigrationEvidenceLevel.Parsed,
                sourceParsed: true,
                targetParsed: false,
                isReadOnly: false,
                diagnostics);
        }

        string candidate = input.Sql;
        QueryCompatibilityRewrite? rewrite = null;
        ParseCSharpDbResult target;
        if (features.TopFilters.Count != 0 &&
            TryRewriteRootTopToLimit(
                input.Sql,
                statements[0],
                features,
                out string? rewritten))
        {
            candidate = rewritten;
            target = TryParseCSharpDb(
                candidate,
                request.Limits,
                cancellationToken);
            if (target.Statement is not null &&
                SqlStatementClassifier.IsReadOnly(target.Statement))
            {
                rewrite = new QueryCompatibilityRewrite
                {
                    RewriteId = TopToLimitRewriteId,
                    CandidateCSharpDbSql = candidate,
                    CandidateDigest = Digest(candidate),
                };
                diagnostics.Add(Diagnostic(
                    input.QueryId,
                    QueryCompatibilityRuleIds.TopToLimitRewrite,
                    0,
                    MigrationDiagnosticSeverity.Information,
                    MigrationCompatibilityStatus.CompatibleWithRewrite,
                    "A bounded TOP-to-LIMIT candidate was generated.",
                    "One root TOP with a non-negative integer literal was moved to a CSharpDB LIMIT clause, and the candidate parsed as a read-only CSharpDB query.",
                    "Review the candidate; it is not automatically applied."));
            }
        }
        else if (features.TopFilters.Count != 0)
        {
            // A CSharpDB parse of raw TOP text is not accepted as portability
            // evidence: the target grammar may interpret the vendor token as
            // an identifier or alias with different semantics.
            target = new ParseCSharpDbResult(
                Statement: null,
                LimitExceeded: false);
        }
        else
        {
            target = TryParseCSharpDb(
                candidate,
                request.Limits,
                cancellationToken);
        }

        if (target.Statement is null ||
            !SqlStatementClassifier.IsReadOnly(target.Statement))
        {
            diagnostics.Add(Diagnostic(
                input.QueryId,
                QueryCompatibilityRuleIds.TargetParseFailed,
                0,
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "No CSharpDB-parsable candidate was established.",
                "The source T-SQL parsed, but the unchanged query and any bounded rewrite candidate did not parse as a read-only CSharpDB query.",
                "Rewrite the vendor-specific syntax explicitly and run the checker again."));
            return Result(
                input,
                sourceDigest,
                MigrationCompatibilityStatus.Unknown,
                MigrationEvidenceLevel.Parsed,
                sourceParsed: true,
                targetParsed: false,
                isReadOnly: true,
                diagnostics);
        }

        AddCSharpDbFeatureDiagnostics(input.QueryId, target.Statement, diagnostics);
        AddUnboundDiagnostic(input.QueryId, diagnostics);
        return Result(
            input,
            sourceDigest,
            StaticResultStatus(diagnostics),
            MigrationEvidenceLevel.Parsed,
            sourceParsed: true,
            targetParsed: true,
            isReadOnly: true,
            diagnostics,
            rewrite);
    }

#endif

    private static QueryCompatibilityResult UnknownDialect(
        QueryCompatibilityInput input)
    {
        MigrationDiagnostic diagnostic = Diagnostic(
            input.QueryId,
            QueryCompatibilityRuleIds.DialectUnqualified,
            0,
            MigrationDiagnosticSeverity.Error,
            MigrationCompatibilityStatus.Unknown,
            $"The {input.SourceDialect} query grammar is not implemented.",
            "The checker does not claim source parsing by borrowing the CSharpDB or T-SQL parser for a different dialect.",
            "Add and qualify a dialect-specific parser adapter.");
        return Result(
            input,
            Digest(input.Sql),
            MigrationCompatibilityStatus.Unknown,
            evidence: null,
            sourceParsed: false,
            targetParsed: false,
            isReadOnly: null,
            [diagnostic]);
    }

    private static ParseCSharpDbResult TryParseCSharpDb(
        string sql,
        QueryCompatibilityLimits limits,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            List<Token> tokens = new Tokenizer(sql).Tokenize();
            if (tokens.Count > limits.MaxTokensPerQuery ||
                ParenthesisNesting(tokens) > limits.MaxNestingPerQuery)
            {
                return new(null, LimitExceeded: true);
            }

            cancellationToken.ThrowIfCancellationRequested();
            Statement statement = Parser.Parse(sql);
            return new(statement, LimitExceeded: false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception) when (IsRecoverableParseFailure(exception))
        {
            return new(null, LimitExceeded: false);
        }
    }

#if CSHARPDB_SQLSERVER_QUERY_ANALYZER
    private static TSqlParseResult TryParseTsql(
        string sql,
        TSqlParser parser,
        QueryCompatibilityLimits limits,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            using var tokenReader = new CancellationCheckingTextReader(
                sql,
                cancellationToken);
            IList<ParseError>? lexerErrors;
            IList<TSqlParserToken> tokens =
                parser.GetTokenStream(tokenReader, out lexerErrors);
            lexerErrors ??= [];
            if (tokens.Count > limits.MaxTokensPerQuery ||
                lexerErrors.Count > limits.MaxParseErrorsPerQuery)
            {
                return new(null, lexerErrors.ToArray(), LimitExceeded: true);
            }

            TSqlFragment fragment = parser.Parse(tokens, out IList<ParseError>? errors);
            errors ??= [];
            if (errors.Count > limits.MaxParseErrorsPerQuery)
                return new(null, errors.ToArray(), LimitExceeded: true);
            return new(fragment, lexerErrors.Concat(errors).ToArray(), LimitExceeded: false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception) when (IsRecoverableParseFailure(exception))
        {
            return new(null, [], LimitExceeded: false);
        }
    }

    private static TSqlFeatureSnapshot InspectTsql(
        TSqlStatement statement,
        QueryCompatibilityLimits limits,
        CancellationToken cancellationToken)
    {
        var seen = new HashSet<TSqlFragment>(ReferenceEqualityComparer.Instance);
        var pending = new Stack<(TSqlFragment Fragment, int Depth)>();
        var tops = new List<TopRowFilter>();
        var nondeterministicFunctions = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var temporaryObjects = new HashSet<string>(StringComparer.Ordinal);
        bool hasSelectInto =
            statement is TSqlSelectStatement { Into: not null };
        bool hasSelectAssignment = false;
        bool hasSequenceMutation = false;
        bool hasSessionState = false;
        int nodes = 0;
        int maxDepth = 0;
        pending.Push((statement, 1));
        while (pending.TryPop(out (TSqlFragment Fragment, int Depth) item))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!seen.Add(item.Fragment))
                continue;

            nodes++;
            maxDepth = Math.Max(maxDepth, item.Depth);
            if (nodes > limits.MaxAstNodesPerQuery ||
                maxDepth > limits.MaxNestingPerQuery)
            {
                return TSqlFeatureSnapshot.Limited;
            }

            switch (item.Fragment)
            {
                case QuerySpecification query:
                    if (query.TopRowFilter is not null)
                        tops.Add(query.TopRowFilter);
                    if (query.SelectElements.Any(static element =>
                            element is SelectSetVariable))
                    {
                        hasSelectAssignment = true;
                    }
                    break;
                case FunctionCall function:
                    string functionName = function.FunctionName.Value ?? string.Empty;
                    if (IsNondeterministicTsqlFunction(functionName))
                        nondeterministicFunctions.Add(functionName);
                    break;
                case GlobalVariableExpression:
                    hasSessionState = true;
                    break;
                case NextValueForExpression:
                    hasSequenceMutation = true;
                    break;
                case NamedTableReference table:
                    string? tableName = table.SchemaObject?.BaseIdentifier?.Value;
                    if (tableName?.StartsWith('#') == true)
                        temporaryObjects.Add(tableName);
                    break;
            }

            foreach (TSqlFragment child in TSqlChildren(item.Fragment))
                pending.Push((child, checked(item.Depth + 1)));
        }

        bool hasRootOrderBy =
            statement is TSqlSelectStatement select &&
            select.QueryExpression.OrderByClause is not null;
        return new TSqlFeatureSnapshot(
            LimitExceeded: false,
            tops,
            RootTop: statement is TSqlSelectStatement
            {
                QueryExpression: QuerySpecification { TopRowFilter: not null } root
            }
                ? root.TopRowFilter
                : null,
            HasRootOrderBy: hasRootOrderBy,
            HasSelectInto: hasSelectInto,
            HasSelectAssignment: hasSelectAssignment,
            HasSequenceMutation: hasSequenceMutation,
            HasSessionState: hasSessionState,
            NondeterministicFunctions: nondeterministicFunctions
                .OrderBy(static value => value, StringComparer.OrdinalIgnoreCase)
                .ToArray(),
            TemporaryObjects: temporaryObjects
                .OrderBy(static value => value, StringComparer.Ordinal)
                .ToArray());
    }

    private static void AddTsqlFeatureDiagnostics(
        string queryId,
        TSqlFeatureSnapshot features,
        ICollection<MigrationDiagnostic> diagnostics)
    {
        int occurrence = 0;
        foreach (string function in features.NondeterministicFunctions)
        {
            diagnostics.Add(Diagnostic(
                queryId,
                QueryCompatibilityRuleIds.NondeterministicFunction,
                occurrence++,
                MigrationDiagnosticSeverity.Warning,
                MigrationCompatibilityStatus.Conditional,
                $"Function '{function}' is nondeterministic or session-dependent.",
                "Static syntax evidence cannot establish equivalent values across source and target executions.",
                "Replace the function with a bound parameter or validate it through a coherent dual run."));
        }

        occurrence = 0;
        foreach (string table in features.TemporaryObjects)
        {
            diagnostics.Add(Diagnostic(
                queryId,
                QueryCompatibilityRuleIds.TemporaryObject,
                occurrence++,
                MigrationDiagnosticSeverity.Warning,
                MigrationCompatibilityStatus.Conditional,
                $"Temporary object '{table}' depends on source session state.",
                "The static checker has no source-session catalog for temporary objects.",
                "Materialize the input as a migration table or provide an explicit query-pack setup contract."));
        }

        if (features.HasSessionState)
        {
            diagnostics.Add(Diagnostic(
                queryId,
                QueryCompatibilityRuleIds.SessionState,
                0,
                MigrationDiagnosticSeverity.Warning,
                MigrationCompatibilityStatus.Conditional,
                "The query reads SQL Server session or global state.",
                "Session state is not represented in the static compatibility contract.",
                "Replace session-dependent values with typed parameters."));
        }

        if (features.RootTop is not null && !features.HasRootOrderBy)
        {
            diagnostics.Add(Diagnostic(
                queryId,
                QueryCompatibilityRuleIds.NondeterministicLimit,
                0,
                MigrationDiagnosticSeverity.Warning,
                MigrationCompatibilityStatus.Conditional,
                "TOP without ORDER BY does not select a deterministic row set.",
                "A syntactic TOP-to-LIMIT rewrite cannot prove that the source and target choose the same rows.",
                "Add a deterministic ORDER BY covering a unique key."));
        }
    }

#endif

    private static void AddCSharpDbFeatureDiagnostics(
        string queryId,
        Statement statement,
        ICollection<MigrationDiagnostic> diagnostics)
    {
        CSharpDbFeatureSnapshot features = InspectCSharpDb(statement);
        int occurrence = 0;
        foreach (string function in features.NondeterministicFunctions)
        {
            diagnostics.Add(Diagnostic(
                queryId,
                QueryCompatibilityRuleIds.NondeterministicFunction,
                occurrence++,
                MigrationDiagnosticSeverity.Warning,
                MigrationCompatibilityStatus.Conditional,
                $"Function '{function}' is not immutable.",
                "Static parsing cannot establish equivalent values for statement-stable or volatile functions.",
                "Replace the function with a bound parameter or validate it through a coherent dual run."));
        }

        occurrence = 0;
        foreach (string function in features.UnboundFunctions)
        {
            diagnostics.Add(Diagnostic(
                queryId,
                QueryCompatibilityRuleIds.UnboundFunction,
                occurrence++,
                MigrationDiagnosticSeverity.Warning,
                MigrationCompatibilityStatus.Unknown,
                $"Function '{function}' requires runtime binding.",
                "The CSharpDB parser accepts function-call syntax, but the static checker cannot prove that a matching registered function exists.",
                "Register and describe the function or replace it with a qualified built-in."));
        }

        if (features.HasLimitWithoutOrder)
        {
            diagnostics.Add(Diagnostic(
                queryId,
                QueryCompatibilityRuleIds.NondeterministicLimit,
                0,
                MigrationDiagnosticSeverity.Warning,
                MigrationCompatibilityStatus.Conditional,
                "LIMIT without ORDER BY does not select a deterministic row set.",
                "Static parsing cannot prove equivalent row selection without an ordering contract.",
                "Add a deterministic ORDER BY covering a unique key."));
        }

        int temporaryOccurrence = 0;
        foreach (string table in features.TemporaryObjects)
        {
            diagnostics.Add(Diagnostic(
                queryId,
                QueryCompatibilityRuleIds.TemporaryObject,
                temporaryOccurrence++,
                MigrationDiagnosticSeverity.Warning,
                MigrationCompatibilityStatus.Conditional,
                $"Temporary object '{table}' depends on session setup.",
                "The static checker has no setup phase or temporary-object catalog.",
                "Use a persistent migrated table or provide a future query-pack setup contract."));
        }
    }

    private static CSharpDbFeatureSnapshot InspectCSharpDb(Statement statement)
    {
        var seen = new HashSet<object>(ReferenceEqualityComparer.Instance);
        var pending = new Stack<object>();
        var nondeterministicFunctions = new HashSet<string>(
            StringComparer.OrdinalIgnoreCase);
        var unboundFunctions = new HashSet<string>(
            StringComparer.OrdinalIgnoreCase);
        var temporaryObjects = new HashSet<string>(StringComparer.Ordinal);
        bool limitWithoutOrder = false;
        pending.Push(statement);
        while (pending.TryPop(out object? item))
        {
            if (!seen.Add(item))
                continue;

            switch (item)
            {
                case CSharpDbSelectStatement select:
                    if (select.Limit is not null && (select.OrderBy?.Count ?? 0) == 0)
                        limitWithoutOrder = true;
                    break;
                case CompoundSelectStatement compound:
                    if (compound.Limit is not null &&
                        (compound.OrderBy?.Count ?? 0) == 0)
                    {
                        limitWithoutOrder = true;
                    }
                    break;
                case FunctionCallExpression function:
                    if (DbBuiltInFunctionRegistry.TryGet(
                            function.FunctionName,
                            out DbBuiltInFunctionDescriptor descriptor))
                    {
                        if (!descriptor.IsDeterministic)
                            nondeterministicFunctions.Add(descriptor.Name);
                    }
                    else
                    {
                        unboundFunctions.Add(function.FunctionName);
                    }
                    break;
                case SimpleTableRef table
                    when table.TableName.StartsWith(
                        "#",
                        StringComparison.Ordinal):
                    temporaryObjects.Add(table.TableName);
                    break;
            }

            foreach (object child in CSharpDbChildren(item))
                pending.Push(child);
        }

        return new CSharpDbFeatureSnapshot(
            limitWithoutOrder,
            nondeterministicFunctions
                .OrderBy(static value => value, StringComparer.OrdinalIgnoreCase)
                .ToArray(),
            unboundFunctions
                .OrderBy(static value => value, StringComparer.OrdinalIgnoreCase)
                .ToArray(),
            temporaryObjects
                .OrderBy(static value => value, StringComparer.Ordinal)
                .ToArray());
    }

    private static IEnumerable<object> CSharpDbChildren(object node)
    {
        foreach (PropertyInfo property in node.GetType()
                     .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                     .Where(static property =>
                         property.CanRead &&
                         property.GetIndexParameters().Length == 0)
                     .OrderBy(static property => property.Name, StringComparer.Ordinal))
        {
            object? value = property.GetValue(node);
            if (value is null ||
                value is string ||
                value is byte[] ||
                value.GetType().IsValueType)
            {
                continue;
            }

            if (value is IEnumerable values)
            {
                foreach (object? child in values)
                {
                    if (child is not null &&
                        child is not string &&
                        !child.GetType().IsValueType)
                    {
                        yield return child;
                    }
                }
            }
            else if (value.GetType().Namespace?.StartsWith(
                         "CSharpDB.Sql",
                         StringComparison.Ordinal) == true)
            {
                yield return value;
            }
        }
    }

#if CSHARPDB_SQLSERVER_QUERY_ANALYZER
    private static IEnumerable<TSqlFragment> TSqlChildren(TSqlFragment fragment)
    {
        foreach (PropertyInfo property in fragment.GetType()
                     .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                     .Where(static property =>
                         property.CanRead &&
                         property.GetIndexParameters().Length == 0 &&
                         property.Name != nameof(TSqlFragment.ScriptTokenStream) &&
                         (typeof(TSqlFragment).IsAssignableFrom(property.PropertyType) ||
                          typeof(IEnumerable).IsAssignableFrom(property.PropertyType)))
                     .OrderBy(static property => property.Name, StringComparer.Ordinal))
        {
            object? value = property.GetValue(fragment);
            if (value is TSqlFragment child)
            {
                yield return child;
            }
            else if (value is IEnumerable values && value is not string)
            {
                foreach (object? candidate in values)
                {
                    if (candidate is TSqlFragment listedChild)
                        yield return listedChild;
                }
            }
        }
    }

    private static bool TryRewriteRootTopToLimit(
        string sql,
        TSqlStatement statement,
        TSqlFeatureSnapshot features,
        out string rewritten)
    {
        rewritten = string.Empty;
        if (statement is not TSqlSelectStatement ||
            features.RootTop is not TopRowFilter top ||
            features.TopFilters.Count != 1 ||
            top.Percent ||
            top.WithTies ||
            !TryReadTopLimit(top.Expression, out int limit) ||
            top.StartOffset < 0 ||
            top.FragmentLength <= 0 ||
            top.StartOffset > sql.Length - top.FragmentLength)
        {
            return false;
        }

        string withoutTop = sql.Remove(top.StartOffset, top.FragmentLength);
        int insertion = withoutTop.Length;
        while (insertion > 0 && char.IsWhiteSpace(withoutTop[insertion - 1]))
            insertion--;
        if (insertion > 0 && withoutTop[insertion - 1] == ';')
            insertion--;

        rewritten = withoutTop.Insert(
            insertion,
            $" LIMIT {limit.ToString(CultureInfo.InvariantCulture)}");
        return true;
    }

    private static bool TryReadTopLimit(
        ScalarExpression expression,
        out int limit)
    {
        limit = 0;
        while (expression is ParenthesisExpression parenthesized)
            expression = parenthesized.Expression;

        return expression is IntegerLiteral literal &&
            int.TryParse(
                literal.Value,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out limit) &&
            limit >= 0;
    }

#endif

    private static void AddUnboundDiagnostic(
        string queryId,
        ICollection<MigrationDiagnostic> diagnostics) =>
        diagnostics.Add(Diagnostic(
            queryId,
            QueryCompatibilityRuleIds.BindingNotPerformed,
            0,
            MigrationDiagnosticSeverity.Warning,
            MigrationCompatibilityStatus.Conditional,
            "Schema and typed-parameter binding were not performed.",
            "Source and target syntax parsed, but the current public surface does not provide a parse/bind/plan-only proof against the migration catalog.",
            "Bind the query against a schema-faithful scratch target before treating it as executable."));

    private static MigrationCompatibilityStatus StaticResultStatus(
        IEnumerable<MigrationDiagnostic> diagnostics) =>
        diagnostics.Any(static item =>
            item.Status == MigrationCompatibilityStatus.Unknown)
            ? MigrationCompatibilityStatus.Unknown
            : MigrationCompatibilityStatus.Conditional;

    private static QueryCompatibilityResult Result(
        QueryCompatibilityInput input,
        string sourceDigest,
        MigrationCompatibilityStatus status,
        MigrationEvidenceLevel? evidence,
        bool sourceParsed,
        bool targetParsed,
        bool? isReadOnly,
        IEnumerable<MigrationDiagnostic> diagnostics,
        QueryCompatibilityRewrite? rewrite = null) =>
        new()
        {
            QueryId = input.QueryId,
            SourceDialect = input.SourceDialect,
            SourceDigest = sourceDigest,
            Status = status,
            Evidence = evidence,
            SourceParsed = sourceParsed,
            TargetParsed = targetParsed,
            IsReadOnly = isReadOnly,
            Rewrite = rewrite,
            Diagnostics = diagnostics
                .OrderBy(static item => item.DiagnosticId, StringComparer.Ordinal)
                .ToArray(),
        };

    private static MigrationDiagnostic Diagnostic(
        string queryId,
        string ruleId,
        int occurrence,
        MigrationDiagnosticSeverity severity,
        MigrationCompatibilityStatus status,
        string summary,
        string explanation,
        string remediation,
        MigrationSourceSpan? span = null)
    {
        string stableInput = string.Join(
            '\0',
            ruleId,
            queryId,
            occurrence.ToString(CultureInfo.InvariantCulture),
            summary,
            span?.Start?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
            span?.Line?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
            span?.Column?.ToString(CultureInfo.InvariantCulture) ?? string.Empty);
        return new MigrationDiagnostic
        {
            DiagnosticId = $"query:{Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(stableInput)))
                .ToLowerInvariant()[..24]}",
            RuleId = ruleId,
            Severity = severity,
            Status = status,
            Evidence = MigrationEvidenceLevel.Parsed,
            Summary = summary,
            Explanation = explanation,
            ObjectId = queryId,
            SourceSpan = span,
            Remediation = remediation,
            CanOverride = false,
        };
    }

    private static QueryCompatibilityReportSummary Summarize(
        IReadOnlyList<QueryCompatibilityResult> results) =>
        new()
        {
            Total = results.Count,
            Compatible = results.Count(static item =>
                item.Status == MigrationCompatibilityStatus.Compatible),
            CompatibleWithRewrite = results.Count(static item =>
                item.Status ==
                MigrationCompatibilityStatus.CompatibleWithRewrite),
            Conditional = results.Count(static item =>
                item.Status == MigrationCompatibilityStatus.Conditional),
            Unsupported = results.Count(static item =>
                item.Status == MigrationCompatibilityStatus.Unsupported),
            Unknown = results.Count(static item =>
                item.Status == MigrationCompatibilityStatus.Unknown),
        };

#if CSHARPDB_SQLSERVER_QUERY_ANALYZER
    private static IReadOnlyList<TSqlStatement> GetStatements(
        TSqlFragment fragment) =>
        fragment switch
        {
            TSqlScript script => script.Batches
                .SelectMany(static batch => batch.Statements)
                .ToArray(),
            TSqlBatch batch => batch.Statements.ToArray(),
            TSqlStatement statement => [statement],
            _ => [],
        };

    private static TSqlParser? SelectTsqlParser(
        int compatibilityLevel,
        bool quotedIdentifiers) =>
        compatibilityLevel switch
        {
            150 => new TSql150Parser(
                quotedIdentifiers,
                SqlEngineType.Standalone),
            160 => new TSql160Parser(
                quotedIdentifiers,
                SqlEngineType.Standalone),
            170 => new TSql170Parser(
                quotedIdentifiers,
                SqlEngineType.Standalone),
            _ => null,
        };

#endif

    private static int ParenthesisNesting(IReadOnlyList<Token> tokens)
    {
        int depth = 0;
        int maximum = 0;
        foreach (Token token in tokens)
        {
            if (token.Type == TokenType.LeftParen)
            {
                depth++;
                maximum = Math.Max(maximum, depth);
            }
            else if (token.Type == TokenType.RightParen && depth > 0)
            {
                depth--;
            }
        }
        return maximum;
    }

#if CSHARPDB_SQLSERVER_QUERY_ANALYZER
    private static bool IsNondeterministicTsqlFunction(string name) =>
        name.ToUpperInvariant() is
            "CURRENT_TIMESTAMP" or
            "GETDATE" or
            "GETUTCDATE" or
            "SYSDATETIME" or
            "SYSUTCDATETIME" or
            "SYSDATETIMEOFFSET" or
            "NEWID" or
            "NEWSEQUENTIALID" or
            "RAND";

#endif

    private static bool IsRecoverableParseFailure(Exception exception) =>
        exception is CSharpDbException or
            ArgumentException or
            InvalidOperationException or
            IOException;

    private static string Digest(string value) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value)))
            .ToLowerInvariant();

    private static int Utf8ByteCount(string value)
    {
        try
        {
            return Encoding.UTF8.GetByteCount(value);
        }
        catch (EncoderFallbackException exception)
        {
            throw new ArgumentException(
                "Query text is not valid UTF-16 input.",
                nameof(value),
                exception);
        }
    }

    private static void ValidateRequest(QueryCompatibilityRequest request)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            request.TargetCSharpDbVersion);
        ArgumentNullException.ThrowIfNull(request.Limits);
        ArgumentNullException.ThrowIfNull(request.Queries);
        QueryCompatibilityLimits limits = request.Limits;
        if (limits.MaxQueries <= 0 ||
            limits.MaxQueryBytes <= 0 ||
            limits.MaxTotalQueryBytes <= 0 ||
            limits.MaxTokensPerQuery <= 0 ||
            limits.MaxAstNodesPerQuery <= 0 ||
            limits.MaxNestingPerQuery <= 0 ||
            limits.MaxParseErrorsPerQuery <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "All query compatibility limits must be positive.");
        }

        if (request.Queries.Count > limits.MaxQueries)
        {
            throw new ArgumentException(
                "Query pack exceeds the maximum query count.",
                nameof(request));
        }
    }

    private static void ValidateInput(QueryCompatibilityInput input)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(input.QueryId);
        if (input.QueryId.Length > 256 ||
            input.QueryId.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Query ids must contain 1-256 non-control characters.",
                nameof(input));
        }
        ArgumentException.ThrowIfNullOrWhiteSpace(input.Sql);
        if (!Enum.IsDefined(input.SourceDialect))
        {
            throw new ArgumentOutOfRangeException(
                nameof(input),
                input.SourceDialect,
                "Unknown query source dialect.");
        }
    }

#if CSHARPDB_SQLSERVER_QUERY_ANALYZER
    private sealed class CancellationCheckingTextReader : TextReader
    {
        private readonly StringReader _inner;
        private readonly CancellationToken _cancellationToken;

        public CancellationCheckingTextReader(
            string source,
            CancellationToken cancellationToken)
        {
            _inner = new StringReader(source);
            _cancellationToken = cancellationToken;
        }

        public override int Peek()
        {
            _cancellationToken.ThrowIfCancellationRequested();
            return _inner.Peek();
        }

        public override int Read()
        {
            _cancellationToken.ThrowIfCancellationRequested();
            return _inner.Read();
        }

        public override int Read(char[] buffer, int index, int count)
        {
            _cancellationToken.ThrowIfCancellationRequested();
            return _inner.Read(buffer, index, count);
        }

        public override int Read(Span<char> buffer)
        {
            _cancellationToken.ThrowIfCancellationRequested();
            return _inner.Read(buffer);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _inner.Dispose();
            base.Dispose(disposing);
        }
    }

#endif

    private sealed record ParseCSharpDbResult(
        Statement? Statement,
        bool LimitExceeded);

#if CSHARPDB_SQLSERVER_QUERY_ANALYZER
    private sealed record TSqlParseResult(
        TSqlFragment? Fragment,
        IReadOnlyList<ParseError> Errors,
        bool LimitExceeded);

    private sealed record TSqlFeatureSnapshot(
        bool LimitExceeded,
        IReadOnlyList<TopRowFilter> TopFilters,
        TopRowFilter? RootTop,
        bool HasRootOrderBy,
        bool HasSelectInto,
        bool HasSelectAssignment,
        bool HasSequenceMutation,
        bool HasSessionState,
        IReadOnlyList<string> NondeterministicFunctions,
        IReadOnlyList<string> TemporaryObjects)
    {
        public static TSqlFeatureSnapshot Limited { get; } = new(
            LimitExceeded: true,
            TopFilters: [],
            RootTop: null,
            HasRootOrderBy: false,
            HasSelectInto: false,
            HasSelectAssignment: false,
            HasSequenceMutation: false,
            HasSessionState: false,
            NondeterministicFunctions: [],
            TemporaryObjects: []);
    }

#endif

    private sealed record CSharpDbFeatureSnapshot(
        bool HasLimitWithoutOrder,
        IReadOnlyList<string> NondeterministicFunctions,
        IReadOnlyList<string> UnboundFunctions,
        IReadOnlyList<string> TemporaryObjects);
}
