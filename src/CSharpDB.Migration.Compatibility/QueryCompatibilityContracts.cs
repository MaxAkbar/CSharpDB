namespace CSharpDB.Migration.Compatibility;

public static class QueryCompatibilityReportFormats
{
    public const string V1 = "csharpdb-query-compatibility-report/v1";
}

public enum QuerySourceDialect
{
    CSharpDb,
    SqlServerTsql,
    MySql,
    Sqlite,
    Access,
}

public sealed record QueryCompatibilityLimits
{
    public int MaxQueries { get; init; } = 1_000;

    public int MaxQueryBytes { get; init; } = 1024 * 1024;

    public long MaxTotalQueryBytes { get; init; } = 16L * 1024 * 1024;

    public int MaxTokensPerQuery { get; init; } = 100_000;

    public int MaxAstNodesPerQuery { get; init; } = 100_000;

    public int MaxNestingPerQuery { get; init; } = 256;

    public int MaxParseErrorsPerQuery { get; init; } = 16;
}

public sealed record QueryCompatibilityInput
{
    public required string QueryId { get; init; }

    public required QuerySourceDialect SourceDialect { get; init; }

    public required string Sql { get; init; }
}

public sealed record QueryCompatibilityRequest
{
    public string TargetCSharpDbVersion { get; init; } =
        CSharpDbCapabilityCatalogLoader.CurrentTargetVersion;

    /// <summary>
    /// Qualified values are 150, 160, and 170, corresponding to SQL Server
    /// 2019, 2022, and 2025. Other values fail closed as an unknown dialect.
    /// </summary>
    public int SqlServerCompatibilityLevel { get; init; } = 160;

    public bool SqlServerQuotedIdentifiers { get; init; } = true;

    public QueryCompatibilityLimits Limits { get; init; } = new();

    public IReadOnlyList<QueryCompatibilityInput> Queries { get; init; } = [];
}

public sealed record QueryCompatibilityReport
{
    public string Format { get; init; } = QueryCompatibilityReportFormats.V1;

    public required string TargetCSharpDbVersion { get; init; }

    public required string CapabilityDigest { get; init; }

    public required QueryCompatibilityReportSummary Summary { get; init; }

    public IReadOnlyList<QueryCompatibilityResult> Results { get; init; } = [];
}

public sealed record QueryCompatibilityReportSummary
{
    public int Total { get; init; }

    public int Compatible { get; init; }

    public int CompatibleWithRewrite { get; init; }

    public int Conditional { get; init; }

    public int Unsupported { get; init; }

    public int Unknown { get; init; }
}

public sealed record QueryCompatibilityResult
{
    public required string QueryId { get; init; }

    public required QuerySourceDialect SourceDialect { get; init; }

    public required string SourceDigest { get; init; }

    public required MigrationCompatibilityStatus Status { get; init; }

    public MigrationEvidenceLevel? Evidence { get; init; }

    public bool SourceParsed { get; init; }

    public bool TargetParsed { get; init; }

    public bool? IsReadOnly { get; init; }

    public QueryCompatibilityRewrite? Rewrite { get; init; }

    public IReadOnlyList<MigrationDiagnostic> Diagnostics { get; init; } = [];
}

public sealed record QueryCompatibilityRewrite
{
    public required string RewriteId { get; init; }

    public required string CandidateCSharpDbSql { get; init; }

    public required string CandidateDigest { get; init; }
}

public static class QueryCompatibilityRuleIds
{
    public const string DialectUnqualified = "MIG-QUERY-DIALECT-001";
    public const string InputLimitExceeded = "MIG-QUERY-LIMIT-001";
    public const string SourceParseFailed = "MIG-QUERY-SOURCE-PARSE-001";
    public const string MultipleStatements = "MIG-QUERY-MULTI-STATEMENT-001";
    public const string NotReadOnly = "MIG-QUERY-READ-ONLY-001";
    public const string TargetParseFailed = "MIG-QUERY-TARGET-PARSE-001";
    public const string BindingNotPerformed = "MIG-QUERY-UNBOUND-001";
    public const string NondeterministicFunction = "MIG-QUERY-NONDETERMINISTIC-FUNCTION-001";
    public const string UnboundFunction = "MIG-QUERY-FUNCTION-UNBOUND-001";
    public const string NondeterministicLimit = "MIG-QUERY-NONDETERMINISTIC-LIMIT-001";
    public const string TemporaryObject = "MIG-QUERY-TEMPORARY-OBJECT-001";
    public const string SessionState = "MIG-QUERY-SESSION-STATE-001";
    public const string TopToLimitRewrite = "MIG-QUERY-REWRITE-TOP-001";
}
