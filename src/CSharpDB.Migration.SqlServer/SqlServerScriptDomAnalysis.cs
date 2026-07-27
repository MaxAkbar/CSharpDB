using System.Collections.ObjectModel;

namespace CSharpDB.Migration.SqlServer;

internal enum SqlServerScriptDomDefinitionKind
{
    Module = 0,
    DefaultExpression = 1,
    ComputedExpression = 2,
    CheckPredicate = 3,
    IndexFilterPredicate = 4
}

internal enum SqlServerScriptDomStatus
{
    Parsed = 0,
    RootMismatch = 1,
    DialectUnqualified = 2,
    LexerError = 3,
    ParserError = 4,
    TokenLimitExceeded = 5,
    NestingLimitExceeded = 6,
    NodeLimitExceeded = 7,
    ParseErrorLimitExceeded = 8,
    StatementLimitExceeded = 9,
    InputLimitExceeded = 10,
    AnalyzerFailure = 11
}

internal enum SqlServerScriptDomRootKind
{
    None = 0,
    ScalarExpression = 1,
    BooleanExpression = 2,
    View = 3,
    Trigger = 4,
    Procedure = 5,
    ScalarFunction = 6,
    TableValuedFunction = 7,
    StandaloneRule = 8,
    StandaloneDefault = 9
}

internal readonly record struct SqlServerScriptDomDefinitionKey(
    SqlServerScriptDomDefinitionKind Kind,
    int ObjectId,
    int SubObjectId);

/// <summary>
/// Sanitized, value-only evidence from one bounded ScriptDom analysis. No
/// source, token, AST, identifier, literal, parser message, or ScriptDom type
/// crosses this boundary.
/// </summary>
internal sealed record SqlServerScriptDomDefinitionAnalysis(
    SqlServerScriptDomDefinitionKey Key,
    SqlServerScriptDomStatus Status,
    string Grammar,
    bool QuotedIdentifiers,
    int TokenCount,
    int NodeCount,
    int NestingDepth,
    int StatementCount,
    int ParseErrorCount,
    SqlServerScriptDomRootKind ExpectedRootKind,
    SqlServerScriptDomRootKind RootKind,
    int? FirstErrorNumber,
    int? FirstErrorOffset,
    int? FirstErrorLine,
    int? FirstErrorColumn,
    string SourceDigest,
    string AnalysisDigest);

internal sealed class SqlServerScriptDomAnalysisSnapshot
{
    private readonly IReadOnlyDictionary<
        SqlServerScriptDomDefinitionKey,
        SqlServerScriptDomDefinitionAnalysis> byKey;

    public SqlServerScriptDomAnalysisSnapshot(
        IEnumerable<SqlServerScriptDomDefinitionAnalysis> definitions)
    {
        ArgumentNullException.ThrowIfNull(definitions);

        SqlServerScriptDomDefinitionAnalysis[] ordered = definitions
            .OrderBy(static item => item.Key.Kind)
            .ThenBy(static item => item.Key.ObjectId)
            .ThenBy(static item => item.Key.SubObjectId)
            .ToArray();

        var entries = new Dictionary<
            SqlServerScriptDomDefinitionKey,
            SqlServerScriptDomDefinitionAnalysis>(ordered.Length);
        foreach (SqlServerScriptDomDefinitionAnalysis definition in ordered)
        {
            if (!entries.TryAdd(definition.Key, definition))
            {
                throw new SqlServerMigrationException(
                    "ScriptDom analysis contains a duplicate catalog definition key.");
            }
        }

        Definitions = new ReadOnlyCollection<
            SqlServerScriptDomDefinitionAnalysis>(ordered);
        byKey = new ReadOnlyDictionary<
            SqlServerScriptDomDefinitionKey,
            SqlServerScriptDomDefinitionAnalysis>(entries);
    }

    public IReadOnlyList<SqlServerScriptDomDefinitionAnalysis> Definitions { get; }

    public bool TryGet(
        SqlServerScriptDomDefinitionKey key,
        out SqlServerScriptDomDefinitionAnalysis? analysis) =>
        byKey.TryGetValue(key, out analysis);
}
