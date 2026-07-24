using System.Buffers;
using System.Security.Cryptography;
using System.Text.Json;

namespace CSharpDB.Migration.CSharpDb;

public enum CSharpDbDdlPreviewActionKind
{
    Sql = 0,
    EnsureJsonDocumentCollection = 1,
}

public sealed record CSharpDbDdlPreviewAction
{
    public required int Ordinal { get; init; }

    public required CSharpDbDdlPreviewActionKind Kind { get; init; }

    public string? Sql { get; init; }

    public string? TargetName { get; init; }
}

public sealed record CSharpDbDdlPreviewStage
{
    public required int Ordinal { get; init; }

    public required MigrationSchemaStage Stage { get; init; }

    public IReadOnlyList<CSharpDbDdlPreviewAction> Actions { get; init; } = [];
}

/// <summary>
/// A transient, no-write rendering of the target schema actions selected by a
/// migration plan. The plan can retain
/// <see cref="MigrationPlan.GeneratedDdlDigest"/>, but does not retain the
/// rendered SQL or collection actions.
/// </summary>
public sealed record CSharpDbDdlPreview
{
    public const string CurrentFormat = "csharpdb-ddl-preview/v1";
    public const string GeneratedDdlDigestFormat =
        "csharpdb-schema-actions/v1";

    public string Format { get; init; } = CurrentFormat;

    public required string TargetCSharpDbVersion { get; init; }

    public required string CatalogDigest { get; init; }

    /// <summary>
    /// Digest of the normalized plan with GeneratedDdlDigest cleared. This
    /// remains stable before and after the preview digest is attached.
    /// </summary>
    public required string PlanContractDigest { get; init; }

    public required MigrationPlanReadiness Readiness { get; init; }

    public IReadOnlyList<CSharpDbDdlPreviewStage> Stages { get; init; } = [];

    public required string GeneratedDdlDigest { get; init; }
}

/// <summary>
/// Renders the exact schema-stage actions used by the CSharpDB staged target
/// without opening or writing a target database.
/// </summary>
public static class CSharpDbDdlPreviewBuilder
{
    private static readonly MigrationSchemaStage[] s_stageOrder =
    [
        MigrationSchemaStage.LoadEssential,
        MigrationSchemaStage.SecondaryIndexes,
        MigrationSchemaStage.Constraints,
        MigrationSchemaStage.Views,
        MigrationSchemaStage.Triggers,
    ];

    public static CSharpDbDdlPreview Build(
        MigrationPlan plan,
        MigrationCatalog catalog,
        IDataTypeMappingProvider? mappingPolicy = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(catalog);
        cancellationToken.ThrowIfCancellationRequested();

        MigrationPlanReadiness readiness =
            MigrationPlanReadinessValidator.Evaluate(
                plan,
                catalog,
                mappingPolicy);
        cancellationToken.ThrowIfCancellationRequested();
        string planContractDigest = MigrationArtifactSerializer.ComputePlanDigest(
            plan with { GeneratedDdlDigest = null });
        cancellationToken.ThrowIfCancellationRequested();

        var stages = new CSharpDbDdlPreviewStage[s_stageOrder.Length];
        for (int stageOrdinal = 0; stageOrdinal < s_stageOrder.Length; stageOrdinal++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            MigrationSchemaStage stage = s_stageOrder[stageOrdinal];
            IReadOnlyList<string> rendered =
                CSharpDbMigrationSql.BuildStageActions(plan, catalog, stage);
            cancellationToken.ThrowIfCancellationRequested();
            var actions = new CSharpDbDdlPreviewAction[rendered.Count];
            for (int actionOrdinal = 0; actionOrdinal < rendered.Count; actionOrdinal++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string action = rendered[actionOrdinal];
                if (string.IsNullOrWhiteSpace(action))
                {
                    throw new InvalidDataException(
                        $"CSharpDB schema stage '{stage}' rendered an empty action.");
                }

                actions[actionOrdinal] =
                    CSharpDbMigrationSql.TryParseCollectionAction(
                        action,
                        out string targetName)
                        ? new CSharpDbDdlPreviewAction
                        {
                            Ordinal = actionOrdinal,
                            Kind =
                                CSharpDbDdlPreviewActionKind
                                    .EnsureJsonDocumentCollection,
                            TargetName = targetName,
                        }
                        : new CSharpDbDdlPreviewAction
                        {
                            Ordinal = actionOrdinal,
                            Kind = CSharpDbDdlPreviewActionKind.Sql,
                            Sql = action,
                        };
            }

            stages[stageOrdinal] = new CSharpDbDdlPreviewStage
            {
                Ordinal = stageOrdinal,
                Stage = stage,
                Actions = actions,
            };
        }

        string generatedDdlDigest = ComputeGeneratedDdlDigest(
            stages,
            cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        return new CSharpDbDdlPreview
        {
            TargetCSharpDbVersion = plan.TargetCSharpDbVersion,
            CatalogDigest = plan.CatalogDigest.ToLowerInvariant(),
            PlanContractDigest = planContractDigest,
            Readiness = readiness,
            Stages = stages,
            GeneratedDdlDigest = generatedDdlDigest,
        };
    }

    /// <summary>
    /// Re-renders and verifies a preview before attaching its digest to the
    /// plan. Attaching changes the plan digest and therefore must happen before
    /// creating or resuming a staged target.
    /// </summary>
    public static MigrationPlan AttachGeneratedDdlDigest(
        MigrationPlan plan,
        MigrationCatalog catalog,
        CSharpDbDdlPreview preview,
        IDataTypeMappingProvider? mappingPolicy = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(preview);
        cancellationToken.ThrowIfCancellationRequested();

        ValidatePreview(preview, cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        CSharpDbDdlPreview actual = Build(
            plan,
            catalog,
            mappingPolicy,
            cancellationToken);
        if (!FixedTimeDigestEquals(
                preview.PlanContractDigest,
                actual.PlanContractDigest) ||
            !FixedTimeDigestEquals(
                preview.CatalogDigest,
                actual.CatalogDigest) ||
            !string.Equals(
                preview.TargetCSharpDbVersion,
                actual.TargetCSharpDbVersion,
                StringComparison.Ordinal) ||
            !ReadinessEquals(preview.Readiness, actual.Readiness) ||
            !FixedTimeDigestEquals(
                preview.GeneratedDdlDigest,
                actual.GeneratedDdlDigest))
        {
            throw new InvalidDataException(
                "The CSharpDB DDL preview does not match the supplied plan and catalog.");
        }

        if (plan.GeneratedDdlDigest is not null &&
            !FixedTimeDigestEquals(
                plan.GeneratedDdlDigest,
                actual.GeneratedDdlDigest))
        {
            throw new InvalidDataException(
                "The migration plan already contains a different generated DDL digest.");
        }

        return plan with
        {
            GeneratedDdlDigest = actual.GeneratedDdlDigest,
        };
    }

    internal static void ValidateAttachedGeneratedDdlDigest(
        MigrationPlan plan,
        MigrationCatalog catalog)
    {
        CSharpDbDdlPreview actual = Build(plan, catalog);
        if (plan.GeneratedDdlDigest is not null &&
            !FixedTimeDigestEquals(
                plan.GeneratedDdlDigest,
                actual.GeneratedDdlDigest))
        {
            throw new InvalidDataException(
                "The migration plan generated DDL digest does not match the CSharpDB schema renderer.");
        }
    }

    private static string ComputeGeneratedDdlDigest(
        IReadOnlyList<CSharpDbDdlPreviewStage> stages,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var payload = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(payload))
        {
            writer.WriteStartObject();
            writer.WriteString(
                "Format",
                CSharpDbDdlPreview.GeneratedDdlDigestFormat);
            writer.WriteNumber("StageCount", stages.Count);
            writer.WriteStartArray("Stages");
            foreach (CSharpDbDdlPreviewStage stage in stages)
            {
                cancellationToken.ThrowIfCancellationRequested();
                writer.WriteStartObject();
                writer.WriteNumber("Ordinal", stage.Ordinal);
                writer.WriteString("StageContract", StageContract(stage.Stage));
                writer.WriteNumber("ActionCount", stage.Actions.Count);
                writer.WriteStartArray("Actions");
                foreach (CSharpDbDdlPreviewAction action in stage.Actions)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    writer.WriteStartObject();
                    writer.WriteNumber("Ordinal", action.Ordinal);
                    writer.WriteNumber("KindCode", (int)action.Kind);
                    writer.WritePropertyName("Payload");
                    writer.WriteStringValue(action.Kind switch
                    {
                        CSharpDbDdlPreviewActionKind.Sql => action.Sql,
                        CSharpDbDdlPreviewActionKind
                            .EnsureJsonDocumentCollection => action.TargetName,
                        _ => throw new InvalidDataException(
                            "The CSharpDB DDL preview contains an unknown action kind."),
                    });
                    writer.WriteEndObject();
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
            writer.WriteEndObject();
            writer.Flush();
        }
        cancellationToken.ThrowIfCancellationRequested();
        return Convert.ToHexString(SHA256.HashData(payload.WrittenSpan))
            .ToLowerInvariant();
    }

    private static void ValidatePreview(
        CSharpDbDdlPreview preview,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!string.Equals(
                preview.Format,
                CSharpDbDdlPreview.CurrentFormat,
                StringComparison.Ordinal) ||
            string.IsNullOrWhiteSpace(preview.TargetCSharpDbVersion) ||
            preview.Readiness is null ||
            !Enum.IsDefined(preview.Readiness.Status) ||
            !IsSortedIdList(preview.Readiness.PendingDiagnosticIds) ||
            !IsSortedIdList(preview.Readiness.PendingExclusionObjectIds) ||
            !IsSortedIdList(preview.Readiness.BlockingDiagnosticIds) ||
            !IsSortedIdList(preview.Readiness.ExcludedObjectIds) ||
            !IsLowercaseSha256(preview.CatalogDigest) ||
            !IsLowercaseSha256(preview.PlanContractDigest) ||
            !IsLowercaseSha256(preview.GeneratedDdlDigest) ||
            preview.Stages is null ||
            preview.Stages.Count != s_stageOrder.Length)
        {
            throw new InvalidDataException(
                "The CSharpDB DDL preview contract is invalid.");
        }

        for (int stageOrdinal = 0; stageOrdinal < s_stageOrder.Length; stageOrdinal++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CSharpDbDdlPreviewStage? stage = preview.Stages[stageOrdinal];
            if (stage is null ||
                stage.Ordinal != stageOrdinal ||
                stage.Stage != s_stageOrder[stageOrdinal] ||
                stage.Actions is null)
            {
                throw new InvalidDataException(
                    "The CSharpDB DDL preview stage contract is invalid.");
            }

            for (int actionOrdinal = 0;
                 actionOrdinal < stage.Actions.Count;
                 actionOrdinal++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                CSharpDbDdlPreviewAction? action =
                    stage.Actions[actionOrdinal];
                bool validPayload = action?.Kind switch
                {
                    CSharpDbDdlPreviewActionKind.Sql =>
                        !string.IsNullOrWhiteSpace(action.Sql) &&
                        action.TargetName is null,
                    CSharpDbDdlPreviewActionKind
                        .EnsureJsonDocumentCollection =>
                        action.Sql is null &&
                        !string.IsNullOrWhiteSpace(action.TargetName),
                    _ => false,
                };
                if (action is null ||
                    action.Ordinal != actionOrdinal ||
                    !validPayload)
                {
                    throw new InvalidDataException(
                        "The CSharpDB DDL preview action contract is invalid.");
                }
            }
        }

        string expectedDigest = ComputeGeneratedDdlDigest(
            preview.Stages,
            cancellationToken);
        if (!FixedTimeDigestEquals(
                preview.GeneratedDdlDigest,
                expectedDigest))
        {
            throw new InvalidDataException(
                "The CSharpDB DDL preview digest does not match its rendered actions.");
        }
    }

    private static bool IsSha256(string? value) =>
        value is { Length: 64 } &&
        value.All(Uri.IsHexDigit);

    private static bool IsLowercaseSha256(string? value) =>
        IsSha256(value) &&
        !value!.Any(static character => character is >= 'A' and <= 'F');

    private static string StageContract(MigrationSchemaStage stage) =>
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

    private static bool IsSortedIdList(IReadOnlyList<string>? values)
    {
        if (values is null)
            return false;
        string? previous = null;
        foreach (string? value in values)
        {
            if (string.IsNullOrWhiteSpace(value) ||
                previous is not null &&
                StringComparer.Ordinal.Compare(previous, value) >= 0)
            {
                return false;
            }
            previous = value;
        }
        return true;
    }

    private static bool ReadinessEquals(
        MigrationPlanReadiness left,
        MigrationPlanReadiness right) =>
        left.Status == right.Status &&
        left.PendingDiagnosticIds.SequenceEqual(
            right.PendingDiagnosticIds,
            StringComparer.Ordinal) &&
        left.PendingExclusionObjectIds.SequenceEqual(
            right.PendingExclusionObjectIds,
            StringComparer.Ordinal) &&
        left.BlockingDiagnosticIds.SequenceEqual(
            right.BlockingDiagnosticIds,
            StringComparer.Ordinal) &&
        left.ExcludedObjectIds.SequenceEqual(
            right.ExcludedObjectIds,
            StringComparer.Ordinal);

    private static bool FixedTimeDigestEquals(string left, string right)
    {
        if (!IsSha256(left) || !IsSha256(right))
            return false;
        return CryptographicOperations.FixedTimeEquals(
            Convert.FromHexString(left),
            Convert.FromHexString(right));
    }
}
