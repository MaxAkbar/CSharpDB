namespace CSharpDB.Migration;

/// <summary>
/// The direction in which a compiled EF Core migration operation was
/// produced.
/// </summary>
public enum EfCoreMigrationDirection
{
    Up,
    Down,
}

/// <summary>
/// Fixed, provider-independent names for the EF Core migration operation
/// types understood by the generation-only analyzer.
/// </summary>
public enum EfCoreMigrationOperationKind
{
    CreateTable,
    DropTable,
    RenameTable,
    AddColumn,
    AlterColumn,
    DropColumn,
    RenameColumn,
    CreateIndex,
    DropIndex,
    RenameIndex,
    AddPrimaryKey,
    DropPrimaryKey,
    AddUniqueConstraint,
    DropUniqueConstraint,
    AddForeignKey,
    DropForeignKey,
    AddCheckConstraint,
    DropCheckConstraint,
    RawSql,
    EnsureSchema,
    DropSchema,
    CreateSequence,
    AlterSequence,
    RenameSequence,
    DropSequence,
    RestartSequence,
    InsertData,
    UpdateData,
    DeleteData,
    AlterDatabase,
    AlterTable,
    Unknown,
}

/// <summary>
/// The complete rule-id catalog for the v1 EF Core migration analysis
/// contract.
/// </summary>
public static class EfCoreMigrationAnalysisRules
{
    public const string GenerationBound =
        "csharpdb.ef.generation.bound";
    public const string SchemaUnsupported =
        "csharpdb.ef.operation.schema.unsupported";
    public const string SequenceUnsupported =
        "csharpdb.ef.operation.sequence.unsupported";
    public const string DataUnknown =
        "csharpdb.ef.operation.data.unknown";
    public const string OperationUnknown =
        "csharpdb.ef.operation.type.unknown";
    public const string RawSqlBound =
        "csharpdb.ef.raw-sql.bound";
    public const string RawSqlUnsupported =
        "csharpdb.ef.raw-sql.unsupported";
    public const string RawSqlUnknown =
        "csharpdb.ef.raw-sql.unknown";
    public const string GenerationUnsupported =
        "csharpdb.ef.generation.unsupported";
    public const string GenerationFailed =
        "csharpdb.ef.generation.failed";
    public const string TransactionSuppressed =
        "csharpdb.ef.command.transaction-suppressed";
    public const string AnalysisLimit =
        "csharpdb.ef.analysis.limit";
    public const string EmptyMigration =
        "csharpdb.ef.migration.empty";
    public const string EmptyDownMigration =
        "csharpdb.ef.migration.down-empty";
}

/// <summary>
/// Domain separators for generated-SQL digests in the v1 analysis contract.
/// Each digest starts with its UTF-8 domain as a four-byte big-endian
/// length-prefixed frame. Every generated command then contributes frames for
/// the big-endian Int32 migration ordinal, UTF-8 direction token
/// (<c>up</c>/<c>down</c>), big-endian Int32 direction ordinal, big-endian
/// Int32 command ordinal, and the raw SHA-256 digest of the command's strict
/// UTF-8 text. Every value, including each four-byte integer, is prefixed by
/// its own four-byte big-endian length.
/// </summary>
public static class EfCoreMigrationAnalysisDigestDomains
{
    public const string Operation =
        "csharpdb-ef-operation-sql/v1";
    public const string Migration =
        "csharpdb-ef-migration-sql/v1";
    public const string Chain =
        "csharpdb-ef-chain-sql/v1";
}

/// <summary>
/// Input to the in-process compiled migration analyzer.
/// </summary>
public sealed record EfCoreMigrationAnalysisRequest
{
    public const string CurrentFormat =
        "csharpdb-ef-migration-analysis-request/v1";

    public string Format { get; init; } = CurrentFormat;

    /// <summary>
    /// A rooted path used only to load the trusted application assembly. It is
    /// never copied into an analysis report or diagnostic.
    /// </summary>
    public required string AssemblyPath { get; init; }

    public required string AssemblyDigest { get; init; }

    /// <summary>
    /// An exact fully qualified context name or a unique simple context name.
    /// When omitted, the assembly must contain exactly one concrete context.
    /// </summary>
    public string? Context { get; init; }
}

/// <summary>
/// One sanitized operation finding. SQL, identifiers, annotation names, and
/// exception text are deliberately absent.
/// </summary>
public sealed record EfCoreMigrationOperationFinding
{
    public required int Ordinal { get; init; }

    public required EfCoreMigrationDirection Direction { get; init; }

    public required int DirectionOrdinal { get; init; }

    public required EfCoreMigrationOperationKind Kind { get; init; }

    public required MigrationCompatibilityStatus Status { get; init; }

    public required MigrationEvidenceLevel Evidence { get; init; }

    public required string RuleId { get; init; }

    public bool IsDestructive { get; init; }

    public int AnnotationCount { get; init; }

    public int CommandCount { get; init; }

    public int GeneratedSqlUtf8Bytes { get; init; }

    /// <summary>
    /// A domain-separated digest of the ordered generated commands. Required
    /// exactly when <see cref="CommandCount"/> is non-zero.
    /// </summary>
    public string? GeneratedSqlDigest { get; init; }
}

/// <summary>
/// Ordered findings for one compiled EF Core migration. Up operations precede
/// Down operations in <see cref="Operations"/>.
/// </summary>
public sealed record EfCoreMigrationAnalysisMigration
{
    public required int Ordinal { get; init; }

    public required string MigrationId { get; init; }

    public required MigrationCompatibilityStatus Status { get; init; }

    public required MigrationEvidenceLevel HighestEvidence { get; init; }

    public required string RuleId { get; init; }

    public int UpOperationCount { get; init; }

    public int DownOperationCount { get; init; }

    public int OperationCount { get; init; }

    public int DestructiveOperationCount { get; init; }

    public int CommandCount { get; init; }

    /// <summary>
    /// A domain-separated digest of this migration's generated commands.
    /// Required exactly when <see cref="CommandCount"/> is non-zero.
    /// </summary>
    public string? GeneratedSqlDigest { get; init; }

    public IReadOnlyList<EfCoreMigrationOperationFinding> Operations
    {
        get;
        init;
    } = [];
}

/// <summary>
/// A deterministic diagnostic which contains only fixed analyzer prose.
/// </summary>
public sealed record EfCoreMigrationAnalysisDiagnostic
{
    public required int Ordinal { get; init; }

    public required string DiagnosticId { get; init; }

    public required string RuleId { get; init; }

    public required MigrationDiagnosticSeverity Severity { get; init; }

    public required MigrationCompatibilityStatus Status { get; init; }

    public required MigrationEvidenceLevel Evidence { get; init; }

    public int? MigrationOrdinal { get; init; }

    public int? OperationOrdinal { get; init; }

    public required string Summary { get; init; }

    public string? Remediation { get; init; }
}

/// <summary>
/// Bounded, deterministic evidence from generation of a compiled EF Core
/// migration chain. This v1 contract never claims full compatibility because
/// it does not execute the chain against an isolated scratch database.
/// </summary>
public sealed record EfCoreMigrationAnalysisReport
{
    public const string CurrentFormat =
        "csharpdb-ef-migration-analysis/v1";

    public const string CSharpDbProvider =
        "CSharpDB.EntityFrameworkCore";

    public string Format { get; init; } = CurrentFormat;

    public string Provider { get; init; } = CSharpDbProvider;

    public required string TargetCSharpDbVersion { get; init; }

    public required string CapabilityDigest { get; init; }

    public required string AssemblyDigest { get; init; }

    public required string QualifiedEfCoreVersion { get; init; }

    public required string Context { get; init; }

    public required MigrationCompatibilityStatus Status { get; init; }

    public required MigrationEvidenceLevel HighestEvidence { get; init; }

    public required string RuleId { get; init; }

    public int MigrationCount { get; init; }

    public int OperationCount { get; init; }

    public int DestructiveOperationCount { get; init; }

    public int CommandCount { get; init; }

    /// <summary>
    /// A domain-separated digest of every generated command in migration,
    /// direction, operation, and command order. Required exactly when
    /// <see cref="CommandCount"/> is non-zero.
    /// </summary>
    public string? GeneratedSqlDigest { get; init; }

    public IReadOnlyList<EfCoreMigrationAnalysisMigration> Migrations
    {
        get;
        init;
    } = [];

    public IReadOnlyList<EfCoreMigrationAnalysisDiagnostic> Diagnostics
    {
        get;
        init;
    } = [];
}
