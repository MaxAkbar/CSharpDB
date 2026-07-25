using System.Security.Cryptography;
using System.Text;
using System.Buffers.Binary;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Migration.CSharpDb;

/// <summary>
/// Production bounds used while parsing and proving one CSharpDB DDL script.
/// Callers may reduce these limits, but the underlying parser and preview
/// ceilings cannot be exceeded.
/// </summary>
public sealed record CSharpDbDdlCompatibilityOptions
{
    public static CSharpDbDdlCompatibilityOptions Default { get; } = new();

    public SqlScriptParserOptions ParserOptions { get; init; } =
        SqlScriptParserOptions.Default;

    public CSharpDbDdlPreviewBuildOptions PreviewOptions { get; init; } =
        CSharpDbDdlPreviewBuildOptions.Default;

    public CSharpDbDdlScratchValidationOptions ScratchOptions { get; init; } =
        CSharpDbDdlScratchValidationOptions.Default;
}

/// <summary>
/// Sanitized result for one whole source statement. Child AST nodes do not
/// expose source spans, so a statement span is the smallest claimed location.
/// </summary>
public sealed record CSharpDbDdlCompatibilityStatement
{
    public required int Index { get; init; }

    public required string Kind { get; init; }

    public required MigrationSourceSpan Span { get; init; }

    public required MigrationCompatibilityStatus Status { get; init; }

    public required MigrationEvidenceLevel Evidence { get; init; }

    public required string RuleId { get; init; }
}

/// <summary>
/// A deterministic diagnostic that deliberately omits SQL, identifiers, paths,
/// ASTs, and parser or engine exception messages.
/// </summary>
public sealed record CSharpDbDdlCompatibilityDiagnostic
{
    public required int Ordinal { get; init; }

    public required string DiagnosticId { get; init; }

    public required string RuleId { get; init; }

    public required MigrationDiagnosticSeverity Severity { get; init; }

    public required MigrationCompatibilityStatus Status { get; init; }

    public required MigrationEvidenceLevel? Evidence { get; init; }

    public int? StatementIndex { get; init; }

    public MigrationSourceSpan? SourceSpan { get; init; }

    public required string Summary { get; init; }

    public string? Remediation { get; init; }
}

/// <summary>
/// Deterministic, sanitized compatibility evidence for one bounded script.
/// A compatible result requires isolated scratch execution and normalized
/// schema equality; parsing or capability matching alone never passes.
/// </summary>
public sealed record CSharpDbDdlCompatibilityReport
{
    public const string CurrentFormat =
        "csharpdb-ddl-compatibility/v1";

    public string Format { get; init; } = CurrentFormat;

    public string Dialect { get; init; } = "csharpdb";

    public required string TargetCSharpDbVersion { get; init; }

    public required string CapabilityDigest { get; init; }

    public required string ScriptDigest { get; init; }

    public required MigrationCompatibilityStatus Status { get; init; }

    public required MigrationEvidenceLevel? HighestEvidence { get; init; }

    public required string RuleId { get; init; }

    public int StatementCount { get; init; }

    public int ProvenStatementCount { get; init; }

    public int CandidateActionCount { get; init; }

    public string? CatalogDigest { get; init; }

    public string? PlanContractDigest { get; init; }

    public string? GeneratedDdlDigest { get; init; }

    public string? ExpectedSchemaDigest { get; init; }

    public string? ActualSchemaDigest { get; init; }

    public IReadOnlyList<CSharpDbDdlCompatibilityStatement> Statements
    {
        get;
        init;
    } = [];

    public IReadOnlyList<CSharpDbDdlCompatibilityDiagnostic> Diagnostics
    {
        get;
        init;
    } = [];

    public IReadOnlyList<CSharpDbDdlScratchValidationDifference> Differences
    {
        get;
        init;
    } = [];
}

/// <summary>
/// Proves the supported additive subset of a CSharpDB DDL script without
/// opening an existing database. Every statement is allowlisted and lowered
/// before the isolated scratch database can be opened.
/// </summary>
public static class CSharpDbDdlCompatibilityAnalyzer
{
    public const string ScratchEqualRuleId =
        "csharpdb.ddl.scratch.schema-equal";
    public const string ScratchRejectedRuleId =
        "csharpdb.ddl.scratch.rejected";
    public const string ScratchDifferentRuleId =
        "csharpdb.ddl.scratch.schema-different";
    public const string EmptyScriptRuleId =
        "csharpdb.ddl.script.empty";
    public const string ParseRuleId =
        "csharpdb.ddl.script.parse";
    public const string ParseLimitRuleId =
        "csharpdb.ddl.script.limit";
    public const string RenderLimitRuleId =
        "csharpdb.ddl.candidate.limit";
    public const string UnsupportedStatementRuleId =
        "csharpdb.ddl.statement.unsupported";
    public const string UnsupportedFeatureRuleId =
        "csharpdb.ddl.feature.unsupported";
    public const string InvalidReferenceRuleId =
        "csharpdb.ddl.reference.invalid";
    public const string DuplicateObjectRuleId =
        "csharpdb.ddl.object.duplicate";
    public const string CapabilityRuleId =
        "csharpdb.ddl.capability.unproven";
    public const string InternalRuleId =
        "csharpdb.ddl.proof.unavailable";
    public const string RewriteRuleId =
        "csharpdb.ddl.canonical-rewrite";

    public static async ValueTask<CSharpDbDdlCompatibilityReport>
        AnalyzeAsync(
            string script,
            CSharpDbDdlCompatibilityOptions? options = null,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(script);
        options ??= CSharpDbDdlCompatibilityOptions.Default;
        ArgumentNullException.ThrowIfNull(options.ParserOptions);
        ArgumentNullException.ThrowIfNull(options.PreviewOptions);
        ArgumentNullException.ThrowIfNull(options.ScratchOptions);
        cancellationToken.ThrowIfCancellationRequested();

        CSharpDbCapabilityCatalog capabilities =
            CSharpDbCapabilityCatalogLoader.LoadEmbedded();

        IReadOnlyList<SqlScriptStatement> parsed;
        try
        {
            parsed = SqlScriptParser.Parse(
                script,
                options.ParserOptions,
                cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (SqlScriptParseException error)
        {
            string failureScriptDigest = error.Rule ==
                    "script.max-characters"
                ? LimitDigest(script.Length)
                : error.Rule == "script.valid-utf16"
                    ? InvalidUtf16Digest(
                        script,
                        cancellationToken)
                : Digest(
                    "csharpdb-ddl-input/v1",
                    script,
                    cancellationToken);
            bool limit =
                error.Category == SqlScriptParseErrorCategory.Limit;
            MigrationCompatibilityStatus status = limit
                ? MigrationCompatibilityStatus.Unknown
                : MigrationCompatibilityStatus.Unsupported;
            string ruleId = limit ? ParseLimitRuleId : ParseRuleId;
            MigrationSourceSpan span = SourceSpan(error.Span);
            return Failure(
                capabilities,
                failureScriptDigest,
                status,
                highestEvidence: null,
                ruleId,
                statementCount: 0,
                statements: [],
                diagnostics:
                [
                    Diagnostic(
                        ordinal: 0,
                        ruleId,
                        status,
                        evidence: null,
                        statementIndex: null,
                        span,
                        limit
                            ? "The DDL script exceeded a production parsing limit."
                            : "The DDL script could not be parsed completely.",
                        limit
                            ? "Reduce the script or split it into independently reviewed bounded scripts."
                            : "Correct the statement at the reported whole-script location."),
                ]);
        }
        catch (Exception error) when (IsRecoverable(error))
        {
            string failureScriptDigest = script.Length >
                options.ParserOptions.MaxScriptCharacters
                    ? LimitDigest(script.Length)
                    : Digest(
                        "csharpdb-ddl-input/v1",
                        script,
                        cancellationToken);
            return Failure(
                capabilities,
                failureScriptDigest,
                MigrationCompatibilityStatus.Unknown,
                highestEvidence: null,
                InternalRuleId,
                statementCount: 0,
                statements: [],
                diagnostics:
                [
                    Diagnostic(
                        ordinal: 0,
                        InternalRuleId,
                        MigrationCompatibilityStatus.Unknown,
                        evidence: null,
                        statementIndex: null,
                        span: null,
                        "The DDL proof could not be produced safely.",
                        "Review the bounded input and retry with a supported script."),
                ]);
        }

        cancellationToken.ThrowIfCancellationRequested();
        string scriptDigest = Digest(
            "csharpdb-ddl-input/v1",
            script,
            cancellationToken);
        if (parsed.Count == 0)
        {
            return Failure(
                capabilities,
                scriptDigest,
                MigrationCompatibilityStatus.Unsupported,
                MigrationEvidenceLevel.Parsed,
                EmptyScriptRuleId,
                statementCount: 0,
                statements: [],
                diagnostics:
                [
                    Diagnostic(
                        ordinal: 0,
                        EmptyScriptRuleId,
                        MigrationCompatibilityStatus.Unsupported,
                        MigrationEvidenceLevel.Parsed,
                        statementIndex: null,
                        span: null,
                        "The DDL script contains no schema statements.",
                        "Provide at least one supported schema statement."),
                ]);
        }

        LoweringResult lowering;
        try
        {
            lowering = DdlLowerer.Lower(
                parsed,
                capabilities,
                scriptDigest,
                cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception error) when (IsRecoverable(error))
        {
            return Failure(
                capabilities,
                scriptDigest,
                MigrationCompatibilityStatus.Unknown,
                MigrationEvidenceLevel.Parsed,
                InternalRuleId,
                parsed.Count,
                parsed.Select(item =>
                    new CSharpDbDdlCompatibilityStatement
                    {
                        Index = item.Index,
                        Kind = "unproven",
                        Span = SourceSpan(item.Span),
                        Status =
                            MigrationCompatibilityStatus.Unknown,
                        Evidence = MigrationEvidenceLevel.Parsed,
                        RuleId = InternalRuleId,
                    }).ToArray(),
                diagnostics:
                [
                    Diagnostic(
                        ordinal: 0,
                        InternalRuleId,
                        MigrationCompatibilityStatus.Unknown,
                        MigrationEvidenceLevel.Parsed,
                        statementIndex: null,
                        span: null,
                        "The parsed DDL could not be lowered safely.",
                        "Treat the script as unproven and review the supported subset."),
                ]);
        }
        if (lowering.Catalog is null)
        {
            return Failure(
                capabilities,
                scriptDigest,
                lowering.Status,
                MigrationEvidenceLevel.Parsed,
                lowering.RuleId,
                parsed.Count,
                lowering.Statements,
                lowering.Diagnostics);
        }

        MigrationPlan plan;
        CSharpDbDdlPreview preview;
        try
        {
            plan = new MigrationPlanner(capabilities).CreatePlan(
                lowering.Catalog);
            cancellationToken.ThrowIfCancellationRequested();
            if (plan.Objects.Any(item => !item.Included))
            {
                return Failure(
                    capabilities,
                    scriptDigest,
                    MigrationCompatibilityStatus.Unsupported,
                    MigrationEvidenceLevel.CapabilityMatched,
                    CapabilityRuleId,
                    parsed.Count,
                    SetStatementOutcome(
                        lowering.Statements,
                        MigrationCompatibilityStatus.Unsupported,
                        MigrationEvidenceLevel.CapabilityMatched,
                        CapabilityRuleId),
                    AppendDiagnostic(
                        lowering.Diagnostics,
                        CapabilityRuleId,
                        MigrationCompatibilityStatus.Unsupported,
                        MigrationEvidenceLevel.CapabilityMatched,
                        "At least one schema object is not proven by the target capability catalog.",
                        "Remove or rewrite the unproven schema feature."));
            }

            preview = CSharpDbDdlPreviewBuilder.BuildBounded(
                plan,
                lowering.Catalog,
                options.PreviewOptions,
                cancellationToken: cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (CSharpDbDdlPreviewLimitException)
        {
            return Failure(
                capabilities,
                scriptDigest,
                MigrationCompatibilityStatus.Unknown,
                MigrationEvidenceLevel.CapabilityMatched,
                RenderLimitRuleId,
                parsed.Count,
                SetStatementOutcome(
                    lowering.Statements,
                    MigrationCompatibilityStatus.Unknown,
                    MigrationEvidenceLevel.CapabilityMatched,
                    RenderLimitRuleId),
                AppendDiagnostic(
                    lowering.Diagnostics,
                    RenderLimitRuleId,
                    MigrationCompatibilityStatus.Unknown,
                    MigrationEvidenceLevel.CapabilityMatched,
                    "The candidate DDL exceeded a production rendering limit.",
                    "Reduce the script or split it into independently reviewed bounded scripts."),
                catalogDigest:
                    MigrationArtifactSerializer.ComputeCatalogDigest(
                        lowering.Catalog));
        }
        catch (Exception error) when (IsRecoverable(error))
        {
            return Failure(
                capabilities,
                scriptDigest,
                MigrationCompatibilityStatus.Unknown,
                MigrationEvidenceLevel.Parsed,
                InternalRuleId,
                parsed.Count,
                SetStatementOutcome(
                    lowering.Statements,
                    MigrationCompatibilityStatus.Unknown,
                    MigrationEvidenceLevel.Parsed,
                    InternalRuleId),
                AppendDiagnostic(
                    lowering.Diagnostics,
                    InternalRuleId,
                    MigrationCompatibilityStatus.Unknown,
                    MigrationEvidenceLevel.Parsed,
                    "The candidate schema could not be bound safely.",
                    "Review the reported supported subset and retry."));
        }

        cancellationToken.ThrowIfCancellationRequested();
        IReadOnlyDictionary<string, MigrationCatalogObject>
            sourceObjects = lowering.Catalog.Objects.ToDictionary(
                item => item.ObjectId,
                StringComparer.Ordinal);
        bool renamed = false;
        foreach (MigrationPlanObject item in plan.Objects)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!item.Included)
                continue;
            MigrationCatalogObject source =
                sourceObjects[item.SourceObjectId];
            if (!string.Equals(
                    source.SourceName,
                    item.TargetName,
                    StringComparison.Ordinal))
            {
                renamed = true;
                break;
            }
        }
        bool rewritten = lowering.RequiresRewrite || renamed;

        CSharpDbDdlScratchValidationReport scratch;
        try
        {
            scratch = await CSharpDbDdlScratchValidator.ValidateAsync(
                plan,
                lowering.Catalog,
                preview,
                options.ScratchOptions,
                cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception error) when (IsRecoverable(error))
        {
            return Failure(
                capabilities,
                scriptDigest,
                MigrationCompatibilityStatus.Unknown,
                MigrationEvidenceLevel.Bound,
                ScratchRejectedRuleId,
                parsed.Count,
                SetStatementOutcome(
                    lowering.Statements,
                    MigrationCompatibilityStatus.Unknown,
                    MigrationEvidenceLevel.Bound,
                    ScratchRejectedRuleId),
                AppendDiagnostic(
                    lowering.Diagnostics,
                    ScratchRejectedRuleId,
                    MigrationCompatibilityStatus.Unknown,
                    MigrationEvidenceLevel.Bound,
                    "The isolated scratch proof could not be completed safely.",
                    "Review the candidate schema and retry."),
                preview);
        }

        cancellationToken.ThrowIfCancellationRequested();
        if (scratch.Status != CSharpDbDdlScratchValidationStatus.Passed)
        {
            bool different =
                scratch.Status ==
                CSharpDbDdlScratchValidationStatus.Different;
            string ruleId = different
                ? ScratchDifferentRuleId
                : ScratchRejectedRuleId;
            return Failure(
                capabilities,
                scriptDigest,
                MigrationCompatibilityStatus.Unknown,
                scratch.HighestEvidence ?? MigrationEvidenceLevel.Bound,
                ruleId,
                parsed.Count,
                SetStatementOutcome(
                    lowering.Statements,
                    MigrationCompatibilityStatus.Unknown,
                    scratch.HighestEvidence ??
                    MigrationEvidenceLevel.Bound,
                    ruleId),
                AppendDiagnostic(
                    lowering.Diagnostics,
                    ruleId,
                    MigrationCompatibilityStatus.Unknown,
                    scratch.HighestEvidence ??
                    MigrationEvidenceLevel.Bound,
                    different
                        ? "The isolated scratch schema differs from the intended normalized schema."
                        : "The isolated scratch proof rejected the candidate schema.",
                    "Treat the script as unproven and review the reported evidence."),
                preview,
                scratch);
        }

        MigrationCompatibilityStatus finalStatus = rewritten
            ? MigrationCompatibilityStatus.CompatibleWithRewrite
            : MigrationCompatibilityStatus.Compatible;
        string finalRule = rewritten ? RewriteRuleId : ScratchEqualRuleId;
        IReadOnlyList<CSharpDbDdlCompatibilityDiagnostic> diagnostics =
            rewritten
                ? AppendDiagnostic(
                    lowering.Diagnostics,
                    RewriteRuleId,
                    finalStatus,
                    MigrationEvidenceLevel.ScratchExecuted,
                    "The proven candidate requires a deterministic canonical rewrite.",
                    "Review the generated migration plan before any apply workflow.",
                    MigrationDiagnosticSeverity.Warning)
                : lowering.Diagnostics;
        int actionCount = preview.Stages.Sum(stage => stage.Actions.Count);
        return new CSharpDbDdlCompatibilityReport
        {
            TargetCSharpDbVersion = capabilities.TargetCSharpDbVersion,
            CapabilityDigest = capabilities.Digest,
            ScriptDigest = scriptDigest,
            Status = finalStatus,
            HighestEvidence = MigrationEvidenceLevel.ScratchExecuted,
            RuleId = finalRule,
            StatementCount = parsed.Count,
            ProvenStatementCount = parsed.Count,
            CandidateActionCount = actionCount,
            CatalogDigest = preview.CatalogDigest,
            PlanContractDigest = preview.PlanContractDigest,
            GeneratedDdlDigest = preview.GeneratedDdlDigest,
            ExpectedSchemaDigest = scratch.ExpectedSchemaDigest,
            ActualSchemaDigest = scratch.ActualSchemaDigest,
            Statements = SetStatementOutcome(
                lowering.Statements,
                finalStatus,
                MigrationEvidenceLevel.ScratchExecuted,
                finalRule),
            Diagnostics = diagnostics,
            Differences = scratch.Differences,
        };
    }

    private static CSharpDbDdlCompatibilityReport Failure(
        CSharpDbCapabilityCatalog capabilities,
        string scriptDigest,
        MigrationCompatibilityStatus status,
        MigrationEvidenceLevel? highestEvidence,
        string ruleId,
        int statementCount,
        IReadOnlyList<CSharpDbDdlCompatibilityStatement> statements,
        IReadOnlyList<CSharpDbDdlCompatibilityDiagnostic> diagnostics,
        CSharpDbDdlPreview? preview = null,
        CSharpDbDdlScratchValidationReport? scratch = null,
        string? catalogDigest = null) =>
        new()
        {
            TargetCSharpDbVersion = capabilities.TargetCSharpDbVersion,
            CapabilityDigest = capabilities.Digest,
            ScriptDigest = scriptDigest,
            Status = status,
            HighestEvidence = highestEvidence,
            RuleId = ruleId,
            StatementCount = statementCount,
            ProvenStatementCount = 0,
            CandidateActionCount =
                preview?.Stages.Sum(stage => stage.Actions.Count) ?? 0,
            CatalogDigest = preview?.CatalogDigest ?? catalogDigest,
            PlanContractDigest = preview?.PlanContractDigest,
            GeneratedDdlDigest = preview?.GeneratedDdlDigest,
            ExpectedSchemaDigest = scratch?.ExpectedSchemaDigest,
            ActualSchemaDigest = scratch?.ActualSchemaDigest,
            Statements = statements,
            Diagnostics = diagnostics,
            Differences = scratch?.Differences ?? [],
        };

    private static IReadOnlyList<CSharpDbDdlCompatibilityStatement>
        SetStatementOutcome(
            IReadOnlyList<CSharpDbDdlCompatibilityStatement> statements,
            MigrationCompatibilityStatus status,
            MigrationEvidenceLevel evidence,
            string ruleId) =>
        statements
            .Select(item => item with
            {
                Status = status,
                Evidence = evidence,
                RuleId = ruleId,
            })
            .ToArray();

    private static IReadOnlyList<CSharpDbDdlCompatibilityDiagnostic>
        AppendDiagnostic(
            IReadOnlyList<CSharpDbDdlCompatibilityDiagnostic> diagnostics,
            string ruleId,
            MigrationCompatibilityStatus status,
            MigrationEvidenceLevel evidence,
            string summary,
            string remediation,
            MigrationDiagnosticSeverity severity =
                MigrationDiagnosticSeverity.Error)
    {
        var result =
            new CSharpDbDdlCompatibilityDiagnostic[diagnostics.Count + 1];
        for (int index = 0; index < diagnostics.Count; index++)
            result[index] = diagnostics[index];
        result[^1] = Diagnostic(
            diagnostics.Count,
            ruleId,
            status,
            evidence,
            statementIndex: null,
            span: null,
            summary,
            remediation,
            severity);
        return result;
    }

    internal static CSharpDbDdlCompatibilityDiagnostic Diagnostic(
        int ordinal,
        string ruleId,
        MigrationCompatibilityStatus status,
        MigrationEvidenceLevel? evidence,
        int? statementIndex,
        MigrationSourceSpan? span,
        string summary,
        string remediation,
        MigrationDiagnosticSeverity severity =
            MigrationDiagnosticSeverity.Error) =>
        new()
        {
            Ordinal = ordinal,
            DiagnosticId = string.Concat(
                "csharpdb-ddl/",
                ordinal.ToString("D6", System.Globalization.CultureInfo.InvariantCulture),
                "/",
                ruleId),
            RuleId = ruleId,
            Severity = severity,
            Status = status,
            Evidence = evidence,
            StatementIndex = statementIndex,
            SourceSpan = span,
            Summary = summary,
            Remediation = remediation,
        };

    internal static MigrationSourceSpan SourceSpan(SqlSourceSpan span) =>
        new()
        {
            SourceId = "input",
            Start = span.Start,
            Length = span.Length,
            Line = span.Line,
            Column = span.Column,
        };

    internal static string Digest(
        string domain,
        string value,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using IncrementalHash hash =
            IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        byte[] domainBytes = Encoding.UTF8.GetBytes(domain);
        hash.AppendData(domainBytes);
        hash.AppendData([0]);
        byte[] valueBytes = Encoding.UTF8.GetBytes(value);
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            hash.AppendData(valueBytes);
            cancellationToken.ThrowIfCancellationRequested();
            return Convert.ToHexString(hash.GetHashAndReset())
                .ToLowerInvariant();
        }
        finally
        {
            CryptographicOperations.ZeroMemory(domainBytes);
            CryptographicOperations.ZeroMemory(valueBytes);
        }
    }

    private static string LimitDigest(int characterCount) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(
            string.Concat(
                "csharpdb-ddl-over-character-limit/v1",
                "\0",
                characterCount.ToString(
                    System.Globalization.CultureInfo.InvariantCulture)))))
            .ToLowerInvariant();

    private static string InvalidUtf16Digest(
        string value,
        CancellationToken cancellationToken)
    {
        using IncrementalHash hash =
            IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        byte[] domain = Encoding.UTF8.GetBytes(
            "csharpdb-ddl-invalid-utf16/v1");
        try
        {
            hash.AppendData(domain);
            hash.AppendData([0]);
            Span<byte> buffer = stackalloc byte[4096];
            const int charactersPerChunk = 2048;
            for (int start = 0;
                 start < value.Length;
                 start += charactersPerChunk)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int count = Math.Min(
                    charactersPerChunk,
                    value.Length - start);
                for (int index = 0; index < count; index++)
                {
                    BinaryPrimitives.WriteUInt16LittleEndian(
                        buffer.Slice(index * 2, 2),
                        value[start + index]);
                }

                hash.AppendData(buffer[..(count * 2)]);
                CryptographicOperations.ZeroMemory(
                    buffer[..(count * 2)]);
            }

            return Convert.ToHexString(hash.GetHashAndReset())
                .ToLowerInvariant();
        }
        finally
        {
            CryptographicOperations.ZeroMemory(domain);
        }
    }

    internal static bool IsRecoverable(Exception error) =>
        error is not OutOfMemoryException and
        not StackOverflowException and
        not AccessViolationException;

    internal sealed record LoweringResult(
        MigrationCatalog? Catalog,
        MigrationCompatibilityStatus Status,
        string RuleId,
        bool RequiresRewrite,
        IReadOnlyList<CSharpDbDdlCompatibilityStatement> Statements,
        IReadOnlyList<CSharpDbDdlCompatibilityDiagnostic> Diagnostics);
}
