using System.Collections;
using System.Collections.Concurrent;
using System.Globalization;
using System.Reflection;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace CSharpDB.Migration.SqlServer;

internal static class SqlServerScriptDomAnalyzer
{
    private const string UnqualifiedGrammar = "unqualified";

    public static SqlServerScriptDomAnalysisSnapshot Analyze(
        SqlServerCatalogSnapshot snapshot,
        SqlServerInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(limits);
        limits.Validate();

        IReadOnlyList<DefinitionInput> inputs = GetInputs(snapshot);
        var budget = new AnalysisBudget(limits);
        var analyses = new List<SqlServerScriptDomDefinitionAnalysis>(inputs.Count);
        foreach (DefinitionInput input in inputs)
        {
            cancellationToken.ThrowIfCancellationRequested();
            analyses.Add(AnalyzeOne(
                input,
                snapshot.Instance.ProductMajorVersion,
                snapshot.Database.CompatibilityLevel,
                limits,
                budget,
                cancellationToken));
        }

        return new SqlServerScriptDomAnalysisSnapshot(analyses);
    }

    private static SqlServerScriptDomDefinitionAnalysis AnalyzeOne(
        DefinitionInput input,
        int productMajorVersion,
        short compatibilityLevel,
        SqlServerInspectionLimits limits,
        AnalysisBudget budget,
        CancellationToken cancellationToken)
    {
        string sourceDigest = DigestSource(input);
        SqlServerScriptDomRootKind expectedRoot = ExpectedRoot(input);
        ParserSelection? selection = SelectParser(
            productMajorVersion,
            compatibilityLevel,
            input.QuotedIdentifiers);
        if (selection is null)
        {
            return Result(
                input,
                SqlServerScriptDomStatus.DialectUnqualified,
                UnqualifiedGrammar,
                expectedRoot,
                sourceDigest);
        }

        int sourceBytes;
        try
        {
            sourceBytes = Encoding.UTF8.GetByteCount(input.Source);
        }
        catch (EncoderFallbackException)
        {
            return Result(
                input,
                SqlServerScriptDomStatus.AnalyzerFailure,
                selection.Grammar,
                expectedRoot,
                sourceDigest);
        }

        budget.ReserveInput(sourceBytes);
        if (sourceBytes > limits.MaxExpressionBytes)
        {
            return Result(
                input,
                SqlServerScriptDomStatus.InputLimitExceeded,
                selection.Grammar,
                expectedRoot,
                sourceDigest);
        }

        int tokenCount = 0;
        int nestingDepth = 0;
        IList<ParseError>? lexerErrors = null;
        IList<TSqlParserToken>? tokens = null;
        try
        {
            using var reader = new CancellationCheckingTextReader(
                input.Source,
                cancellationToken);
            tokens = selection.Parser.GetTokenStream(reader, out lexerErrors);
            tokenCount = tokens.Count;
            nestingDepth = ParenthesisNesting(tokens, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception) when (IsRecoverable(exception))
        {
            return Result(
                input,
                SqlServerScriptDomStatus.AnalyzerFailure,
                selection.Grammar,
                expectedRoot,
                sourceDigest);
        }

        budget.ReserveTokens(tokenCount);
        if (tokenCount > limits.MaxScriptDomTokensPerDefinition)
        {
            return Result(
                input,
                SqlServerScriptDomStatus.TokenLimitExceeded,
                selection.Grammar,
                expectedRoot,
                sourceDigest,
                tokenCount: tokenCount,
                nestingDepth: nestingDepth);
        }

        if (nestingDepth > limits.MaxScriptDomNestingPerDefinition)
        {
            return Result(
                input,
                SqlServerScriptDomStatus.NestingLimitExceeded,
                selection.Grammar,
                expectedRoot,
                sourceDigest,
                tokenCount: tokenCount,
                nestingDepth: nestingDepth);
        }

        lexerErrors ??= [];
        budget.ReserveParseErrors(lexerErrors.Count);
        if (lexerErrors.Count > limits.MaxScriptDomParseErrorsPerDefinition)
        {
            return Result(
                input,
                SqlServerScriptDomStatus.ParseErrorLimitExceeded,
                selection.Grammar,
                expectedRoot,
                sourceDigest,
                tokenCount: tokenCount,
                nestingDepth: nestingDepth,
                errors: lexerErrors);
        }

        if (lexerErrors.Count > 0)
        {
            return Result(
                input,
                SqlServerScriptDomStatus.LexerError,
                selection.Grammar,
                expectedRoot,
                sourceDigest,
                tokenCount: tokenCount,
                nestingDepth: nestingDepth,
                errors: lexerErrors);
        }

        TSqlFragment fragment;
        IList<ParseError>? parserErrors;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (input.Key.Kind == SqlServerScriptDomDefinitionKind.Module)
            {
                fragment = selection.Parser.Parse(tokens, out parserErrors);
            }
            else
            {
                using var reader = new CancellationCheckingTextReader(
                    input.Source,
                    cancellationToken);
                fragment = ParseExpression(
                    selection.Parser,
                    reader,
                    input.Key.Kind is
                        SqlServerScriptDomDefinitionKind.CheckPredicate or
                        SqlServerScriptDomDefinitionKind.IndexFilterPredicate,
                    out parserErrors);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception) when (IsRecoverable(exception))
        {
            return Result(
                input,
                SqlServerScriptDomStatus.AnalyzerFailure,
                selection.Grammar,
                expectedRoot,
                sourceDigest,
                tokenCount: tokenCount,
                nestingDepth: nestingDepth);
        }

        cancellationToken.ThrowIfCancellationRequested();
        parserErrors ??= [];
        budget.ReserveParseErrors(parserErrors.Count);
        if (parserErrors.Count > limits.MaxScriptDomParseErrorsPerDefinition)
        {
            return Result(
                input,
                SqlServerScriptDomStatus.ParseErrorLimitExceeded,
                selection.Grammar,
                expectedRoot,
                sourceDigest,
                tokenCount: tokenCount,
                nestingDepth: nestingDepth,
                errors: parserErrors);
        }

        FragmentCounts counts;
        try
        {
            counts = CancellationCheckingFragmentVisitor.Count(
                fragment,
                limits.MaxScriptDomNodesPerDefinition,
                limits.MaxScriptDomNestingPerDefinition,
                limits.MaxScriptDomStatementsPerDefinition,
                cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception) when (IsRecoverable(exception))
        {
            return Result(
                input,
                SqlServerScriptDomStatus.AnalyzerFailure,
                selection.Grammar,
                expectedRoot,
                sourceDigest,
                tokenCount: tokenCount,
                nestingDepth: nestingDepth,
                errors: parserErrors);
        }

        budget.ReserveNodes(counts.NodeCount);
        budget.ReserveStatements(counts.StatementCount);
        nestingDepth = Math.Max(nestingDepth, counts.NestingDepth);

        SqlServerScriptDomRootKind rootKind = SafeRootKind(input, fragment);
        SqlServerScriptDomStatus status;
        if (counts.NodeCount > limits.MaxScriptDomNodesPerDefinition)
            status = SqlServerScriptDomStatus.NodeLimitExceeded;
        else if (nestingDepth > limits.MaxScriptDomNestingPerDefinition)
            status = SqlServerScriptDomStatus.NestingLimitExceeded;
        else if (counts.StatementCount > limits.MaxScriptDomStatementsPerDefinition)
            status = SqlServerScriptDomStatus.StatementLimitExceeded;
        else if (parserErrors.Count > 0)
            status = SqlServerScriptDomStatus.ParserError;
        else if (!RootMatches(input, fragment, snapshotCaseSensitive: input.CaseSensitive))
            status = SqlServerScriptDomStatus.RootMismatch;
        else
            status = SqlServerScriptDomStatus.Parsed;

        return Result(
            input,
            status,
            selection.Grammar,
            expectedRoot,
            sourceDigest,
            tokenCount,
            counts.NodeCount,
            nestingDepth,
            counts.StatementCount,
            rootKind,
            parserErrors);
    }

    private static TSqlFragment ParseExpression(
        TSqlParser parser,
        TextReader reader,
        bool booleanExpression,
        out IList<ParseError> errors) =>
        parser switch
        {
            TSql150Parser selected when booleanExpression =>
                selected.ParseBooleanExpression(reader, out errors, 0, 1, 1),
            TSql150Parser selected =>
                selected.ParseExpression(reader, out errors, 0, 1, 1),
            TSql160Parser selected when booleanExpression =>
                selected.ParseBooleanExpression(reader, out errors, 0, 1, 1),
            TSql160Parser selected =>
                selected.ParseExpression(reader, out errors, 0, 1, 1),
            TSql170Parser selected when booleanExpression =>
                selected.ParseBooleanExpression(reader, out errors, 0, 1, 1),
            TSql170Parser selected =>
                selected.ParseExpression(reader, out errors, 0, 1, 1),
            _ => throw new InvalidOperationException(
                "The selected ScriptDom grammar is not qualified.")
        };

    private static int ParenthesisNesting(
        IEnumerable<TSqlParserToken> tokens,
        CancellationToken cancellationToken)
    {
        int current = 0;
        int maximum = 0;
        foreach (TSqlParserToken token in tokens)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (token.TokenType == TSqlTokenType.LeftParenthesis)
            {
                current++;
                maximum = Math.Max(maximum, current);
            }
            else if (token.TokenType == TSqlTokenType.RightParenthesis)
            {
                current = Math.Max(0, current - 1);
            }
        }

        return maximum;
    }

    private static ParserSelection? SelectParser(
        int productMajorVersion,
        short compatibilityLevel,
        bool quotedIdentifiers) =>
        (productMajorVersion, compatibilityLevel) switch
        {
            (15, 150) => new(
                new TSql150Parser(
                    quotedIdentifiers,
                    SqlEngineType.Standalone),
                "tsql150"),
            (16, 160) => new(
                new TSql160Parser(
                    quotedIdentifiers,
                    SqlEngineType.Standalone),
                "tsql160"),
            (17, 170) => new(
                new TSql170Parser(
                    quotedIdentifiers,
                    SqlEngineType.Standalone),
                "tsql170"),
            _ => null
        };

    private static IReadOnlyList<DefinitionInput> GetInputs(
        SqlServerCatalogSnapshot snapshot)
    {
        bool databaseQuotedIdentifiers = snapshot.Database.IsQuotedIdentifierOn;
        bool caseSensitive = IsCaseSensitive(snapshot.Database.Collation);
        var schemas = snapshot.Schemas.ToDictionary(
            static item => item.SchemaId,
            static item => item.Name);
        var inputs = new List<DefinitionInput>();

        foreach (SqlServerModuleMetadata module in snapshot.Modules)
        {
            if (module.Definition is null)
                continue;
            schemas.TryGetValue(module.SchemaId, out string? schemaName);
            inputs.Add(new(
                new(
                    SqlServerScriptDomDefinitionKind.Module,
                    module.ObjectId,
                    0),
                module.Definition,
                module.UsesQuotedIdentifier,
                module.ObjectType,
                schemaName,
                module.Name,
                caseSensitive));
        }

        foreach (SqlServerColumnMetadata column in snapshot.Columns)
        {
            if (column.DefaultDefinition is not null)
            {
                inputs.Add(new(
                    new(
                        SqlServerScriptDomDefinitionKind.DefaultExpression,
                        column.ObjectId,
                        column.ColumnId),
                    column.DefaultDefinition,
                    databaseQuotedIdentifiers,
                    null,
                    null,
                    null,
                    caseSensitive));
            }

            if (column.ComputedDefinition is not null)
            {
                inputs.Add(new(
                    new(
                        SqlServerScriptDomDefinitionKind.ComputedExpression,
                        column.ObjectId,
                        column.ColumnId),
                    column.ComputedDefinition,
                    databaseQuotedIdentifiers,
                    null,
                    null,
                    null,
                    caseSensitive));
            }
        }

        foreach (SqlServerCheckMetadata check in snapshot.Checks)
        {
            if (check.Definition is null)
                continue;
            inputs.Add(new(
                new(
                    SqlServerScriptDomDefinitionKind.CheckPredicate,
                    check.ObjectId,
                    0),
                check.Definition,
                databaseQuotedIdentifiers,
                null,
                null,
                null,
                caseSensitive));
        }

        foreach (SqlServerIndexMetadata index in snapshot.Indexes)
        {
            if (index.FilterDefinition is null)
                continue;
            inputs.Add(new(
                new(
                    SqlServerScriptDomDefinitionKind.IndexFilterPredicate,
                    index.ObjectId,
                    index.IndexId),
                index.FilterDefinition,
                databaseQuotedIdentifiers,
                null,
                null,
                null,
                caseSensitive));
        }

        return inputs
            .OrderBy(static item => item.Key.Kind)
            .ThenBy(static item => item.Key.ObjectId)
            .ThenBy(static item => item.Key.SubObjectId)
            .ToArray();
    }

    private static bool RootMatches(
        DefinitionInput input,
        TSqlFragment fragment,
        bool snapshotCaseSensitive)
    {
        if (input.Key.Kind != SqlServerScriptDomDefinitionKind.Module)
        {
            return input.Key.Kind is
                SqlServerScriptDomDefinitionKind.CheckPredicate or
                SqlServerScriptDomDefinitionKind.IndexFilterPredicate
                ? fragment is BooleanExpression
                : fragment is ScalarExpression;
        }

        if (!TryGetSingleStatement(fragment, out TSqlStatement? statement))
            return false;

        SchemaObjectName? name = input.ObjectType switch
        {
            "V" when statement is
                CreateViewStatement or
                AlterViewStatement or
                CreateOrAlterViewStatement =>
                ((ViewStatementBody)statement).SchemaObjectName,
            "TR" when statement is
                CreateTriggerStatement or
                AlterTriggerStatement or
                CreateOrAlterTriggerStatement =>
                ((TriggerStatementBody)statement).Name,
            "P" or "RF" when statement is
                CreateProcedureStatement or
                AlterProcedureStatement or
                CreateOrAlterProcedureStatement =>
                ((ProcedureStatementBody)statement).ProcedureReference.Name,
            "FN" or "IF" or "TF" when statement is
                CreateFunctionStatement or
                AlterFunctionStatement or
                CreateOrAlterFunctionStatement =>
                FunctionTypeMatches(
                    input.ObjectType,
                    (FunctionStatementBody)statement)
                    ? ((FunctionStatementBody)statement).Name
                    : null,
            "R" when statement is CreateRuleStatement rule => rule.Name,
            "D" when statement is CreateDefaultStatement defaultStatement =>
                defaultStatement.Name,
            _ => null
        };

        return name is not null &&
               NameMatches(
                   name,
                   input.SchemaName,
                   input.ObjectName,
                   snapshotCaseSensitive);
    }

    private static bool NameMatches(
        SchemaObjectName name,
        string? expectedSchema,
        string? expectedName,
        bool caseSensitive)
    {
        if (expectedName is null || name.Identifiers.Count is < 1 or > 2)
            return false;

        StringComparison comparison = caseSensitive
            ? StringComparison.Ordinal
            : StringComparison.OrdinalIgnoreCase;
        string actualName = name.Identifiers[^1].Value;
        if (!string.Equals(actualName, expectedName, comparison))
            return false;

        if (name.Identifiers.Count == 1)
            return true;
        return expectedSchema is not null &&
               string.Equals(
                   name.Identifiers[0].Value,
                   expectedSchema,
                   comparison);
    }

    private static bool FunctionTypeMatches(
        string objectType,
        FunctionStatementBody statement) =>
        objectType switch
        {
            "FN" => statement.ReturnType is ScalarFunctionReturnType,
            "IF" => statement.ReturnType is SelectFunctionReturnType,
            "TF" => statement.ReturnType is TableValuedFunctionReturnType,
            _ => false
        };

    private static SqlServerScriptDomRootKind SafeRootKind(
        DefinitionInput input,
        TSqlFragment fragment)
    {
        if (input.Key.Kind != SqlServerScriptDomDefinitionKind.Module)
        {
            return fragment switch
            {
                BooleanExpression => SqlServerScriptDomRootKind.BooleanExpression,
                ScalarExpression => SqlServerScriptDomRootKind.ScalarExpression,
                _ => SqlServerScriptDomRootKind.None
            };
        }

        if (!TryGetSingleStatement(fragment, out TSqlStatement? statement))
            return SqlServerScriptDomRootKind.None;
        return statement switch
        {
            ViewStatementBody => SqlServerScriptDomRootKind.View,
            TriggerStatementBody => SqlServerScriptDomRootKind.Trigger,
            ProcedureStatementBody => SqlServerScriptDomRootKind.Procedure,
            FunctionStatementBody function => function.ReturnType switch
            {
                ScalarFunctionReturnType =>
                    SqlServerScriptDomRootKind.ScalarFunction,
                SelectFunctionReturnType or TableValuedFunctionReturnType =>
                    SqlServerScriptDomRootKind.TableValuedFunction,
                _ => SqlServerScriptDomRootKind.None
            },
            CreateRuleStatement => SqlServerScriptDomRootKind.StandaloneRule,
            CreateDefaultStatement => SqlServerScriptDomRootKind.StandaloneDefault,
            _ => SqlServerScriptDomRootKind.None
        };
    }

    private static bool TryGetSingleStatement(
        TSqlFragment fragment,
        out TSqlStatement? statement)
    {
        statement = null;
        if (fragment is not TSqlScript script)
            return false;
        foreach (TSqlBatch batch in script.Batches)
        {
            foreach (TSqlStatement candidate in batch.Statements)
            {
                if (statement is not null)
                    return false;
                statement = candidate;
            }
        }

        return statement is not null;
    }

    private static SqlServerScriptDomRootKind ExpectedRoot(
        DefinitionInput input) =>
        input.Key.Kind switch
        {
            SqlServerScriptDomDefinitionKind.DefaultExpression or
            SqlServerScriptDomDefinitionKind.ComputedExpression =>
                SqlServerScriptDomRootKind.ScalarExpression,
            SqlServerScriptDomDefinitionKind.CheckPredicate or
            SqlServerScriptDomDefinitionKind.IndexFilterPredicate =>
                SqlServerScriptDomRootKind.BooleanExpression,
            _ => input.ObjectType switch
            {
                "V" => SqlServerScriptDomRootKind.View,
                "TR" => SqlServerScriptDomRootKind.Trigger,
                "P" or "RF" => SqlServerScriptDomRootKind.Procedure,
                "FN" => SqlServerScriptDomRootKind.ScalarFunction,
                "IF" or "TF" => SqlServerScriptDomRootKind.TableValuedFunction,
                "R" => SqlServerScriptDomRootKind.StandaloneRule,
                "D" => SqlServerScriptDomRootKind.StandaloneDefault,
                _ => SqlServerScriptDomRootKind.None
            }
        };

    private static SqlServerScriptDomDefinitionAnalysis Result(
        DefinitionInput input,
        SqlServerScriptDomStatus status,
        string grammar,
        SqlServerScriptDomRootKind expectedRoot,
        string sourceDigest,
        int tokenCount = 0,
        int nodeCount = 0,
        int nestingDepth = 0,
        int statementCount = 0,
        SqlServerScriptDomRootKind rootKind = SqlServerScriptDomRootKind.None,
        IList<ParseError>? errors = null)
    {
        ParseError? first = errors?.FirstOrDefault();
        int errorCount = errors?.Count ?? 0;
        string analysisDigest = "sha256:" + SqlServerStableDigest.Text(
            "csharpdb-sqlserver-scriptdom-analysis/v1",
            Invariant((int)input.Key.Kind),
            Invariant(input.Key.ObjectId),
            Invariant(input.Key.SubObjectId),
            status.ToString(),
            grammar,
            input.QuotedIdentifiers ? "true" : "false",
            Invariant(tokenCount),
            Invariant(nodeCount),
            Invariant(nestingDepth),
            Invariant(statementCount),
            Invariant(errorCount),
            expectedRoot.ToString(),
            rootKind.ToString(),
            first is null ? null : Invariant(first.Number),
            first is null ? null : Invariant(first.Offset),
            first is null ? null : Invariant(first.Line),
            first is null ? null : Invariant(first.Column),
            sourceDigest);

        return new(
            input.Key,
            status,
            grammar,
            input.QuotedIdentifiers,
            tokenCount,
            nodeCount,
            nestingDepth,
            statementCount,
            errorCount,
            expectedRoot,
            rootKind,
            first?.Number,
            first?.Offset,
            first?.Line,
            first?.Column,
            sourceDigest,
            analysisDigest);
    }

    private static string DigestSource(DefinitionInput input) =>
        "sha256:" + SqlServerStableDigest.Text(
            "csharpdb-sqlserver-scriptdom-source/v1",
            Invariant((int)input.Key.Kind),
            Invariant(input.Key.ObjectId),
            Invariant(input.Key.SubObjectId),
            input.Source);

    private static string Invariant(int value) =>
        value.ToString(CultureInfo.InvariantCulture);

    private static bool IsCaseSensitive(string? collation) =>
        collation is not null &&
        (collation.Contains("_CS_", StringComparison.OrdinalIgnoreCase) ||
         collation.Contains("_BIN", StringComparison.OrdinalIgnoreCase));

    private static bool IsRecoverable(Exception exception) =>
        exception is not OutOfMemoryException and
        not StackOverflowException and
        not AccessViolationException;

    private sealed record DefinitionInput(
        SqlServerScriptDomDefinitionKey Key,
        string Source,
        bool QuotedIdentifiers,
        string? ObjectType,
        string? SchemaName,
        string? ObjectName,
        bool CaseSensitive);

    private sealed record ParserSelection(TSqlParser Parser, string Grammar);

    private readonly record struct FragmentCounts(
        int NodeCount,
        int NestingDepth,
        int StatementCount);

    /// <summary>
    /// ScriptDom has no cancellation-aware whole-tree visitor. This bounded
    /// visitor follows only public fragment-valued child properties, never
    /// token/source properties, and checks cancellation at every node.
    /// </summary>
    private static class CancellationCheckingFragmentVisitor
    {
        private static readonly ConcurrentDictionary<Type, PropertyInfo[]>
            s_childProperties = new();

        public static FragmentCounts Count(
            TSqlFragment root,
            int maximumNodes,
            int maximumNesting,
            int maximumStatements,
            CancellationToken cancellationToken)
        {
            var seen = new HashSet<TSqlFragment>(
                ReferenceEqualityComparer.Instance);
            var pending = new Stack<(TSqlFragment Fragment, int Depth)>();
            pending.Push((root, 1));
            int nodeCount = 0;
            int nestingDepth = 0;
            int statementCount = 0;

            while (pending.TryPop(out (TSqlFragment Fragment, int Depth) item))
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!seen.Add(item.Fragment))
                    continue;
                nodeCount++;
                nestingDepth = Math.Max(nestingDepth, item.Depth);
                if (item.Fragment is TSqlStatement)
                    statementCount++;
                if (nodeCount > maximumNodes ||
                    nestingDepth > maximumNesting ||
                    statementCount > maximumStatements)
                {
                    break;
                }

                foreach (TSqlFragment child in Children(item.Fragment))
                    pending.Push((child, checked(item.Depth + 1)));
            }

            return new(nodeCount, nestingDepth, statementCount);
        }

        private static IEnumerable<TSqlFragment> Children(TSqlFragment fragment)
        {
            PropertyInfo[] properties = s_childProperties.GetOrAdd(
                fragment.GetType(),
                static type => type
                    .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(static property =>
                        property.CanRead &&
                        property.GetIndexParameters().Length == 0 &&
                        property.Name != nameof(TSqlFragment.ScriptTokenStream) &&
                        (typeof(TSqlFragment).IsAssignableFrom(property.PropertyType) ||
                         typeof(IEnumerable).IsAssignableFrom(
                             property.PropertyType)))
                    .ToArray());
            foreach (PropertyInfo property in properties)
            {
                object? value = property.GetValue(fragment);
                if (value is TSqlFragment child)
                {
                    yield return child;
                    continue;
                }

                if (value is not IEnumerable values || value is string)
                    continue;
                foreach (object? candidate in values)
                {
                    if (candidate is TSqlFragment listedChild)
                        yield return listedChild;
                }
            }
        }
    }

    private sealed class CancellationCheckingTextReader : TextReader
    {
        private readonly StringReader inner;
        private readonly CancellationToken cancellationToken;

        public CancellationCheckingTextReader(
            string source,
            CancellationToken cancellationToken)
        {
            inner = new StringReader(source);
            this.cancellationToken = cancellationToken;
        }

        public override int Peek()
        {
            cancellationToken.ThrowIfCancellationRequested();
            return inner.Peek();
        }

        public override int Read()
        {
            cancellationToken.ThrowIfCancellationRequested();
            return inner.Read();
        }

        public override int Read(char[] buffer, int index, int count)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return inner.Read(buffer, index, count);
        }

        public override int Read(Span<char> buffer)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return inner.Read(buffer);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                inner.Dispose();
            base.Dispose(disposing);
        }
    }

    private sealed class AnalysisBudget
    {
        private readonly SqlServerInspectionLimits limits;
        private long inputBytes;
        private long tokens;
        private long nodes;
        private long parseErrors;
        private long statements;

        public AnalysisBudget(SqlServerInspectionLimits limits) =>
            this.limits = limits;

        public void ReserveInput(int count) =>
            Reserve(
                ref inputBytes,
                count,
                limits.MaxExpressionBytesTotal,
                "ScriptDom input-byte");

        public void ReserveTokens(int count) =>
            Reserve(
                ref tokens,
                count,
                limits.MaxScriptDomTokensTotal,
                "ScriptDom token");

        public void ReserveNodes(int count) =>
            Reserve(
                ref nodes,
                count,
                limits.MaxScriptDomNodesTotal,
                "ScriptDom AST-node");

        public void ReserveParseErrors(int count) =>
            Reserve(
                ref parseErrors,
                count,
                limits.MaxScriptDomParseErrorsTotal,
                "ScriptDom parse-error");

        public void ReserveStatements(int count) =>
            Reserve(
                ref statements,
                count,
                limits.MaxScriptDomStatementsTotal,
                "ScriptDom statement");

        private static void Reserve(
            ref long total,
            int count,
            long maximum,
            string category)
        {
            if (count < 0 || total > maximum - count)
            {
                throw new SqlServerMigrationException(
                    $"The aggregate {category} inspection limit was exceeded.");
            }
            total += count;
        }
    }
}
