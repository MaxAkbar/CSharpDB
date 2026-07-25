using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Engine;
using CSharpDB.Migration.Validation;
using CSharpDB.Sql;

namespace CSharpDB.Migration.CSharpDb;

public enum CSharpDbDdlScratchValidationStatus
{
    Passed = 0,
    Different = 1,
    Rejected = 2,
}

/// <summary>
/// One sanitized normalized-schema difference. The object identity is exposed
/// only as a domain-separated digest; names and definitions are omitted.
/// </summary>
public sealed record CSharpDbDdlScratchValidationDifference
{
    public required int Ordinal { get; init; }

    public required string ObjectIdentityDigest { get; init; }

    public required MigrationObjectKind Kind { get; init; }

    public string? ExpectedDefinitionDigest { get; init; }

    public string? ActualDefinitionDigest { get; init; }
}

/// <summary>
/// Deterministic evidence from parsing and applying a reviewed DDL preview to
/// an in-memory CSharpDB database. A passed result establishes only that the
/// preview executed and produced the expected normalized catalog. It is not
/// source semantic equivalence proof and does not claim that view or trigger
/// bodies were bound.
/// </summary>
public sealed record CSharpDbDdlScratchValidationReport
{
    public const string CurrentFormat =
        "csharpdb-ddl-scratch-validation/v1";

    public string Format { get; init; } = CurrentFormat;

    public required CSharpDbDdlScratchValidationStatus Status { get; init; }

    /// <summary>
    /// Highest evidence actually established, or <see langword="null"/> when
    /// the request was rejected before the preview binding was verified.
    /// </summary>
    public required MigrationEvidenceLevel? HighestEvidence { get; init; }

    public required string TargetCSharpDbVersion { get; init; }

    public required string CatalogDigest { get; init; }

    public required string PlanContractDigest { get; init; }

    public required string GeneratedDdlDigest { get; init; }

    public string? AttachedPlanDigest { get; init; }

    /// <summary>
    /// Readiness from the verified preview, or <see langword="null"/> when the
    /// preview binding could not be established.
    /// </summary>
    public MigrationPlanReadinessStatus? ReadinessStatus { get; init; }

    public string? ExpectedSchemaDigest { get; init; }

    public string? ActualSchemaDigest { get; init; }

    public required string RuleId { get; init; }

    public string? StageId { get; init; }

    public string? ActionId { get; init; }

    public int ParsedActionCount { get; init; }

    public int ExecutedActionCount { get; init; }

    public IReadOnlyList<CSharpDbDdlScratchValidationDifference> Differences
    {
        get;
        init;
    } = [];
}

public sealed record CSharpDbDdlScratchValidationOptions
{
    public const int HardMaxActionCount = 4096;
    public const long HardMaxSqlUtf8Bytes = 16L * 1024 * 1024;

    public static CSharpDbDdlScratchValidationOptions Default { get; } = new();

    public int MaxActionCount { get; init; } = HardMaxActionCount;

    public long MaxSqlUtf8Bytes { get; init; } = HardMaxSqlUtf8Bytes;

    public SqlScriptParserOptions ParserOptions { get; init; } = new()
    {
        MaxScriptCharacters = 1024 * 1024,
        MaxScriptUtf8Bytes = 4 * 1024 * 1024,
        MaxStatementCount = 2,
        MaxStatementCharacters = 1024 * 1024,
        MaxTokenCount = 100_000,
        MaxNestingDepth = 128,
    };
}

/// <summary>
/// Validates reviewed CSharpDB target DDL in an isolated in-memory database.
/// No existing target is opened and no supplied plan or catalog is changed.
/// </summary>
public static class CSharpDbDdlScratchValidator
{
    public const string PreviewBindingRuleId =
        "csharpdb.scratch.preview-binding";
    public const string ActionCountLimitRuleId =
        "csharpdb.scratch.limit.action-count";
    public const string SqlByteLimitRuleId =
        "csharpdb.scratch.limit.sql-bytes";
    public const string ActionKindRuleId =
        "csharpdb.scratch.action-kind";
    public const string SqlParseRuleId =
        "csharpdb.scratch.sql.parse";
    public const string SqlStatementCountRuleId =
        "csharpdb.scratch.sql.statement-count";
    public const string SqlStageKindRuleId =
        "csharpdb.scratch.sql.stage-kind";
    public const string SqlExecuteRuleId =
        "csharpdb.scratch.sql.execute";
    public const string SchemaCaptureRuleId =
        "csharpdb.scratch.schema.capture";
    public const string SchemaDifferentRuleId =
        "csharpdb.scratch.schema.different";
    public const string SchemaEqualRuleId =
        "csharpdb.scratch.schema.equal";
    public const string TransactionRuleId =
        "csharpdb.scratch.transaction";

    public static async ValueTask<CSharpDbDdlScratchValidationReport>
        ValidateAsync(
            MigrationPlan plan,
            MigrationCatalog catalog,
            CSharpDbDdlPreview preview,
            CSharpDbDdlScratchValidationOptions? options = null,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(preview);
        options ??= CSharpDbDdlScratchValidationOptions.Default;
        ValidateOptions(options);
        cancellationToken.ThrowIfCancellationRequested();

        ReportBinding reportBinding = CreateUnboundReportBinding(preview);
        int actionCount = 0;
        long sqlUtf8Bytes = 0;
        try
        {
            if (preview.Stages is null || preview.Stages.Count != 5)
            {
                return Rejected(
                    reportBinding,
                    PreviewBindingRuleId,
                    highestEvidence: null);
            }

            foreach (CSharpDbDdlPreviewStage? stage in preview.Stages)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (stage?.Actions is null)
                {
                    return Rejected(
                        reportBinding,
                        PreviewBindingRuleId,
                        highestEvidence: null);
                }

                actionCount = checked(actionCount + stage.Actions.Count);
                if (actionCount > options.MaxActionCount)
                {
                    return Rejected(
                        reportBinding,
                        ActionCountLimitRuleId,
                        highestEvidence: null);
                }

                foreach (CSharpDbDdlPreviewAction? action in stage.Actions)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (action is null)
                    {
                        return Rejected(
                            reportBinding,
                            PreviewBindingRuleId,
                            highestEvidence: null);
                    }

                    if (action.Kind != CSharpDbDdlPreviewActionKind.Sql)
                    {
                        if (action.Kind !=
                                CSharpDbDdlPreviewActionKind
                                    .EnsureJsonDocumentCollection ||
                            action.Sql is not null ||
                            action.TargetName is null ||
                            action.TargetName.Length >
                                MigrationDocumentCollectionContract
                                    .MaximumLogicalCollectionNameLength ||
                            string.IsNullOrWhiteSpace(action.TargetName))
                        {
                            return Rejected(
                                reportBinding,
                                PreviewBindingRuleId,
                                highestEvidence: null);
                        }
                        continue;
                    }
                    if (action.Sql is null || action.TargetName is not null)
                    {
                        return Rejected(
                            reportBinding,
                            PreviewBindingRuleId,
                            highestEvidence: null);
                    }
                    if (action.Sql.Length >
                        options.ParserOptions.MaxScriptCharacters)
                    {
                        return Rejected(
                            reportBinding,
                            SqlParseRuleId,
                            highestEvidence: null);
                    }

                    int actionUtf8Bytes =
                        Encoding.UTF8.GetByteCount(action.Sql);
                    if (actionUtf8Bytes >
                        options.ParserOptions.MaxScriptUtf8Bytes)
                    {
                        return Rejected(
                            reportBinding,
                            SqlParseRuleId,
                            highestEvidence: null);
                    }

                    sqlUtf8Bytes = checked(
                        sqlUtf8Bytes + actionUtf8Bytes);
                    if (sqlUtf8Bytes > options.MaxSqlUtf8Bytes)
                    {
                        return Rejected(
                            reportBinding,
                            SqlByteLimitRuleId,
                            highestEvidence: null);
                    }
                }
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (OverflowException)
        {
            return Rejected(
                reportBinding,
                ActionCountLimitRuleId,
                highestEvidence: null);
        }
        catch (Exception error) when (IsRecoverable(error))
        {
            return Rejected(
                reportBinding,
                PreviewBindingRuleId,
                highestEvidence: null);
        }

        ValidationBinding binding;
        try
        {
            MigrationPlan attached =
                CSharpDbDdlPreviewBuilder
                    .AttachGeneratedDdlDigestBounded(
                        plan,
                        catalog,
                        preview,
                        new CSharpDbDdlRenderLimits(
                            options.MaxActionCount,
                            options.ParserOptions.MaxScriptCharacters,
                            options.ParserOptions.MaxScriptUtf8Bytes,
                            options.MaxSqlUtf8Bytes),
                        cancellationToken: cancellationToken);
            reportBinding = CreateBoundReportBinding(
                preview,
                MigrationArtifactSerializer.ComputePlanDigest(attached));

            MigrationNormalizedSchema expected =
                MigrationNormalizedSchemaContract.CreateExpected(
                    attached,
                    catalog);
            IReadOnlyDictionary<string, CSharpDbCollectionMigrationBinding>
                collectionBindings =
                    CSharpDbCollectionMigrationBinding.CreateAll(
                        attached,
                        catalog);
            binding = new ValidationBinding(
                attached,
                expected,
                collectionBindings);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (CSharpDbDdlRenderLimitException error)
        {
            return Rejected(
                reportBinding,
                error.Kind switch
                {
                    CSharpDbDdlRenderLimitKind.ActionCount =>
                        ActionCountLimitRuleId,
                    CSharpDbDdlRenderLimitKind.SqlParse =>
                        SqlParseRuleId,
                    CSharpDbDdlRenderLimitKind.AggregateSqlUtf8Bytes =>
                        SqlByteLimitRuleId,
                    _ => throw new InvalidOperationException(
                        "Unknown CSharpDB DDL render limit kind."),
                },
                highestEvidence: null);
        }
        catch (Exception error) when (IsRecoverable(error))
        {
            return Rejected(
                reportBinding,
                PreviewBindingRuleId,
                highestEvidence: null);
        }

        cancellationToken.ThrowIfCancellationRequested();
        var prepared = new List<PreparedAction>(actionCount);
        int parsedActionCount = 0;
        foreach (CSharpDbDdlPreviewStage stage in preview.Stages)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string stageId = StageId(stage.Stage);
            foreach (CSharpDbDdlPreviewAction action in stage.Actions)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string actionId = ActionId(stageId, action.Ordinal);
                if (action.Kind ==
                    CSharpDbDdlPreviewActionKind
                        .EnsureJsonDocumentCollection)
                {
                    if (stage.Stage != MigrationSchemaStage.LoadEssential)
                    {
                        return Rejected(
                            reportBinding,
                            ActionKindRuleId,
                            MigrationEvidenceLevel.CapabilityMatched,
                            stageId,
                            actionId,
                            parsedActionCount);
                    }

                    prepared.Add(new PreparedAction(
                        stageId,
                        actionId,
                        Statement: null,
                        action.TargetName));
                    continue;
                }

                if (action.Kind != CSharpDbDdlPreviewActionKind.Sql)
                {
                    return Rejected(
                        reportBinding,
                        ActionKindRuleId,
                        MigrationEvidenceLevel.CapabilityMatched,
                        stageId,
                        actionId,
                        parsedActionCount);
                }

                IReadOnlyList<SqlScriptStatement> statements;
                try
                {
                    statements = SqlScriptParser.Parse(
                        action.Sql!,
                        options.ParserOptions,
                        cancellationToken);
                }
                catch (OperationCanceledException)
                    when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception error) when (IsRecoverable(error))
                {
                    return Rejected(
                        reportBinding,
                        SqlParseRuleId,
                        MigrationEvidenceLevel.CapabilityMatched,
                        stageId,
                        actionId,
                        parsedActionCount);
                }

                if (statements.Count != 1)
                {
                    return Rejected(
                        reportBinding,
                        SqlStatementCountRuleId,
                        MigrationEvidenceLevel.CapabilityMatched,
                        stageId,
                        actionId,
                        parsedActionCount);
                }

                Statement statement = statements[0].Statement;
                if (!IsAllowedStatement(stage.Stage, statement))
                {
                    return Rejected(
                        reportBinding,
                        SqlStageKindRuleId,
                        MigrationEvidenceLevel.CapabilityMatched,
                        stageId,
                        actionId,
                        parsedActionCount);
                }

                parsedActionCount++;
                prepared.Add(new PreparedAction(
                    stageId,
                    actionId,
                    statement,
                    TargetName: null));
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        ScratchExecutionOutcome outcome = await ExecuteAsync(
            binding.Plan,
            catalog,
            binding,
            prepared,
            cancellationToken).ConfigureAwait(false);
        if (outcome.Cancellation is not null)
            outcome.Cancellation.Throw();
        if (outcome.RuleId is not null)
        {
            return Rejected(
                reportBinding,
                outcome.RuleId,
                outcome.HighestEvidence,
                outcome.StageId,
                outcome.ActionId,
                parsedActionCount,
                outcome.ExecutedActionCount,
                binding.Expected.Digest,
                outcome.Actual?.Digest);
        }

        MigrationNormalizedSchema actual = outcome.Actual ??
            throw new InvalidOperationException(
                "Scratch validation completed without a captured schema.");
        IReadOnlyList<MigrationNormalizedSchemaDifference> differences =
            MigrationNormalizedSchemaContract.Compare(
                binding.Expected,
                actual);
        CSharpDbDdlScratchValidationDifference[] sanitizedDifferences =
            differences.Select((difference, ordinal) =>
                new CSharpDbDdlScratchValidationDifference
                {
                    Ordinal = ordinal,
                    ObjectIdentityDigest = ObjectIdentityDigest(
                        difference.ObjectId),
                    Kind = difference.Kind,
                    ExpectedDefinitionDigest =
                        difference.SourceDefinitionDigest,
                    ActualDefinitionDigest =
                        difference.TargetDefinitionDigest,
                })
                .ToArray();
        bool equal = sanitizedDifferences.Length == 0 &&
            FixedTimeDigestEquals(
                binding.Expected.Digest,
                actual.Digest);
        return new CSharpDbDdlScratchValidationReport
        {
            Status = equal
                ? CSharpDbDdlScratchValidationStatus.Passed
                : CSharpDbDdlScratchValidationStatus.Different,
            HighestEvidence = MigrationEvidenceLevel.ScratchExecuted,
            TargetCSharpDbVersion =
                reportBinding.TargetCSharpDbVersion,
            CatalogDigest = reportBinding.CatalogDigest,
            PlanContractDigest = reportBinding.PlanContractDigest,
            GeneratedDdlDigest = reportBinding.GeneratedDdlDigest,
            AttachedPlanDigest = reportBinding.AttachedPlanDigest,
            ReadinessStatus = reportBinding.ReadinessStatus,
            ExpectedSchemaDigest = binding.Expected.Digest,
            ActualSchemaDigest = actual.Digest,
            RuleId = equal ? SchemaEqualRuleId : SchemaDifferentRuleId,
            ParsedActionCount = parsedActionCount,
            ExecutedActionCount = outcome.ExecutedActionCount,
            Differences = sanitizedDifferences,
        };
    }

    private static async ValueTask<ScratchExecutionOutcome> ExecuteAsync(
        MigrationPlan plan,
        MigrationCatalog catalog,
        ValidationBinding binding,
        IReadOnlyList<PreparedAction> actions,
        CancellationToken cancellationToken)
    {
        Database? database = null;
        bool transactionStarted = false;
        ExceptionDispatchInfo? cancellation = null;
        string? ruleId = null;
        string? stageId = null;
        string? actionId = null;
        int executedActionCount = 0;
        MigrationNormalizedSchema? actual = null;
        MigrationEvidenceLevel highestEvidence =
            MigrationEvidenceLevel.Parsed;

        try
        {
            database = await Database.OpenInMemoryAsync(cancellationToken)
                .ConfigureAwait(false);
            await database.BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);
            transactionStarted = true;

            foreach (PreparedAction action in actions)
            {
                cancellationToken.ThrowIfCancellationRequested();
                stageId = action.StageId;
                actionId = action.ActionId;
                try
                {
                    if (action.Statement is not null)
                    {
                        await using var result =
                            await database.ExecuteAsync(
                                action.Statement,
                                cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        await database.EnsureJsonDocumentCollectionAsync(
                            action.TargetName!,
                            cancellationToken).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException)
                    when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception error) when (IsRecoverable(error))
                {
                    ruleId = SqlExecuteRuleId;
                    break;
                }

                executedActionCount++;
            }

            if (ruleId is null)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    await database.CommitAsync(cancellationToken)
                        .ConfigureAwait(false);
                    transactionStarted = false;
                }
                catch (OperationCanceledException)
                    when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception error) when (IsRecoverable(error))
                {
                    ruleId = TransactionRuleId;
                    stageId = null;
                    actionId = null;
                }
            }

            if (ruleId is null)
            {
                cancellationToken.ThrowIfCancellationRequested();
                highestEvidence = MigrationEvidenceLevel.ScratchExecuted;
                stageId = null;
                actionId = null;
                try
                {
                    actual = CSharpDbActualSchemaReader.Capture(
                        database,
                        plan,
                        catalog,
                        binding.CollectionBindings,
                        excludeUnexpectedTable: null,
                        cancellationToken);
                }
                catch (OperationCanceledException)
                    when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception error) when (IsRecoverable(error))
                {
                    ruleId = SchemaCaptureRuleId;
                }
            }
        }
        catch (OperationCanceledException error)
            when (cancellationToken.IsCancellationRequested)
        {
            cancellation = ExceptionDispatchInfo.Capture(error);
        }
        catch (Exception error) when (IsRecoverable(error))
        {
            ruleId ??= TransactionRuleId;
            stageId = null;
            actionId = null;
        }
        finally
        {
            if (transactionStarted && database is not null)
            {
                try
                {
                    await database.RollbackAsync(CancellationToken.None)
                        .ConfigureAwait(false);
                }
                catch (Exception error) when (IsRecoverable(error))
                {
                    if (cancellation is null)
                    {
                        ruleId = TransactionRuleId;
                        stageId = null;
                        actionId = null;
                        actual = null;
                    }
                }
            }

            if (database is not null)
            {
                try
                {
                    await database.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception error) when (IsRecoverable(error))
                {
                    if (cancellation is null)
                    {
                        ruleId = TransactionRuleId;
                        stageId = null;
                        actionId = null;
                        actual = null;
                    }
                }
            }
        }

        return new ScratchExecutionOutcome(
            ruleId,
            stageId,
            actionId,
            highestEvidence,
            executedActionCount,
            actual,
            cancellation);
    }

    private static CSharpDbDdlScratchValidationReport Rejected(
        ReportBinding binding,
        string ruleId,
        MigrationEvidenceLevel? highestEvidence,
        string? stageId = null,
        string? actionId = null,
        int parsedActionCount = 0,
        int executedActionCount = 0,
        string? expectedSchemaDigest = null,
        string? actualSchemaDigest = null) =>
        new()
        {
            Status = CSharpDbDdlScratchValidationStatus.Rejected,
            HighestEvidence = highestEvidence,
            TargetCSharpDbVersion = binding.TargetCSharpDbVersion,
            CatalogDigest = binding.CatalogDigest,
            PlanContractDigest = binding.PlanContractDigest,
            GeneratedDdlDigest = binding.GeneratedDdlDigest,
            AttachedPlanDigest = binding.AttachedPlanDigest,
            ReadinessStatus = binding.ReadinessStatus,
            ExpectedSchemaDigest = SafeNullableDigest(
                expectedSchemaDigest),
            ActualSchemaDigest = SafeNullableDigest(actualSchemaDigest),
            RuleId = ruleId,
            StageId = stageId,
            ActionId = actionId,
            ParsedActionCount = parsedActionCount,
            ExecutedActionCount = executedActionCount,
        };

    private static ReportBinding CreateUnboundReportBinding(
        CSharpDbDdlPreview preview) =>
        new(
            TargetCSharpDbVersion: string.Empty,
            SafeDigest(preview.CatalogDigest),
            SafeDigest(preview.PlanContractDigest),
            SafeDigest(preview.GeneratedDdlDigest),
            AttachedPlanDigest: null,
            ReadinessStatus: null);

    private static ReportBinding CreateBoundReportBinding(
        CSharpDbDdlPreview preview,
        string attachedPlanDigest) =>
        new(
            preview.TargetCSharpDbVersion,
            preview.CatalogDigest,
            preview.PlanContractDigest,
            preview.GeneratedDdlDigest,
            attachedPlanDigest,
            preview.Readiness.Status);

    private static bool IsAllowedStatement(
        MigrationSchemaStage stage,
        Statement statement) =>
        stage switch
        {
            MigrationSchemaStage.LoadEssential =>
                statement is CreateTableStatement,
            MigrationSchemaStage.SecondaryIndexes =>
                statement is CreateIndexStatement,
            MigrationSchemaStage.Constraints =>
                statement is AlterTableStatement
                {
                    Action: AddKeyConstraintAction or
                        AddForeignKeyConstraintAction or
                        AddCheckConstraintAction,
                },
            MigrationSchemaStage.Views =>
                statement is CreateViewStatement,
            MigrationSchemaStage.Triggers =>
                statement is CreateTriggerStatement,
            _ => false,
        };

    private static string StageId(MigrationSchemaStage stage) =>
        stage switch
        {
            MigrationSchemaStage.LoadEssential => "load-essential",
            MigrationSchemaStage.SecondaryIndexes => "secondary-indexes",
            MigrationSchemaStage.Constraints => "constraints",
            MigrationSchemaStage.Views => "views",
            MigrationSchemaStage.Triggers => "triggers",
            _ => throw new InvalidDataException(
                "The CSharpDB DDL preview contains an unknown schema stage."),
        };

    private static string ActionId(string stageId, int actionOrdinal) =>
        string.Concat(
            stageId,
            "/action/",
            actionOrdinal.ToString(
                System.Globalization.CultureInfo.InvariantCulture));

    private static void ValidateOptions(
        CSharpDbDdlScratchValidationOptions options)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(
            options.MaxActionCount,
            1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            options.MaxActionCount,
            CSharpDbDdlScratchValidationOptions.HardMaxActionCount);
        ArgumentOutOfRangeException.ThrowIfLessThan(
            options.MaxSqlUtf8Bytes,
            1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            options.MaxSqlUtf8Bytes,
            CSharpDbDdlScratchValidationOptions.HardMaxSqlUtf8Bytes);
        ArgumentNullException.ThrowIfNull(options.ParserOptions);
        ValidateParserLimit(
            options.ParserOptions.MaxScriptCharacters,
            SqlScriptParserOptions.HardMaxScriptCharacters,
            nameof(options.ParserOptions.MaxScriptCharacters));
        ValidateParserLimit(
            options.ParserOptions.MaxScriptUtf8Bytes,
            SqlScriptParserOptions.HardMaxScriptUtf8Bytes,
            nameof(options.ParserOptions.MaxScriptUtf8Bytes));
        ValidateParserLimit(
            options.ParserOptions.MaxStatementCount,
            SqlScriptParserOptions.HardMaxStatementCount,
            nameof(options.ParserOptions.MaxStatementCount));
        ValidateParserLimit(
            options.ParserOptions.MaxStatementCharacters,
            SqlScriptParserOptions.HardMaxStatementCharacters,
            nameof(options.ParserOptions.MaxStatementCharacters));
        ValidateParserLimit(
            options.ParserOptions.MaxTokenCount,
            SqlScriptParserOptions.HardMaxTokenCount,
            nameof(options.ParserOptions.MaxTokenCount));
        ValidateParserLimit(
            options.ParserOptions.MaxNestingDepth,
            SqlScriptParserOptions.HardMaxNestingDepth,
            nameof(options.ParserOptions.MaxNestingDepth));
    }

    private static void ValidateParserLimit(
        int value,
        int maximum,
        string name)
    {
        if (value <= 0 || value > maximum)
        {
            throw new ArgumentOutOfRangeException(
                name,
                value,
                "Scratch SQL parser limits must be positive and within production ceilings.");
        }
    }

    private static string SafeDigest(string? value) =>
        IsLowerSha256(value) ? value! : string.Empty;

    private static string? SafeNullableDigest(string? value) =>
        value is null ? null : SafeDigest(value);

    private static bool IsLowerSha256(string? value) =>
        value is { Length: 64 } &&
        value.All(static character =>
            character is >= '0' and <= '9' or >= 'a' and <= 'f');

    private static bool FixedTimeDigestEquals(
        string left,
        string right) =>
        IsLowerSha256(left) &&
        IsLowerSha256(right) &&
        CryptographicOperations.FixedTimeEquals(
            Convert.FromHexString(left),
            Convert.FromHexString(right));

    private static bool IsRecoverable(Exception error) =>
        error is not OutOfMemoryException and
        not StackOverflowException and
        not AccessViolationException;

    private static string ObjectIdentityDigest(string objectId) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(
            string.Concat(
                "csharpdb-ddl-scratch-object-identity/v1",
                "\0",
                objectId))))
            .ToLowerInvariant();

    private sealed record ValidationBinding(
        MigrationPlan Plan,
        MigrationNormalizedSchema Expected,
        IReadOnlyDictionary<string, CSharpDbCollectionMigrationBinding>
            CollectionBindings);

    private sealed record ReportBinding(
        string TargetCSharpDbVersion,
        string CatalogDigest,
        string PlanContractDigest,
        string GeneratedDdlDigest,
        string? AttachedPlanDigest,
        MigrationPlanReadinessStatus? ReadinessStatus);

    private sealed record PreparedAction(
        string StageId,
        string ActionId,
        Statement? Statement,
        string? TargetName);

    private sealed record ScratchExecutionOutcome(
        string? RuleId,
        string? StageId,
        string? ActionId,
        MigrationEvidenceLevel HighestEvidence,
        int ExecutedActionCount,
        MigrationNormalizedSchema? Actual,
        ExceptionDispatchInfo? Cancellation);
}
