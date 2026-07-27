using System.Collections.Frozen;
using System.Collections.ObjectModel;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Json;

public static class JsonTypedTableSchemaRules
{
    public const string ColumnMismatch =
        "MIG-JSON-TYPED-SCHEMA-COLUMN-001";
}

public sealed class JsonTypedTableSchemaException : Exception
{
    internal JsonTypedTableSchemaException(
        string ruleId,
        string message)
        : base(message)
    {
        RuleId = ruleId;
    }

    internal JsonTypedTableSchemaException(
        string ruleId,
        string message,
        Exception innerException)
        : base(message, innerException)
    {
        RuleId = ruleId;
    }

    public string RuleId { get; }
}

/// <summary>
/// One structurally discovered JSON table column together with its optional
/// typed interpretation and effective provider-neutral source kind.
/// </summary>
public sealed class JsonTypedTableColumnSchema
{
    internal JsonTypedTableColumnSchema(
        JsonTableColumnSchema representationSchema,
        JsonTypedColumnIntent? intent,
        bool representationRequiresFullStreamValidation)
    {
        ArgumentNullException.ThrowIfNull(representationSchema);
        RepresentationSchema = representationSchema;
        Intent = intent is null
            ? null
            : JsonTypedIntentSidecar.CloneIntent(intent);
        SourceValueKind = intent is null
            ? UntypedSourceKind(
                representationSchema.LogicalType)
            : TypedSourceKind(intent.Codec);
        RequiresFullStreamValidation =
            intent is not null ||
            representationRequiresFullStreamValidation;
    }

    public JsonTableColumnSchema RepresentationSchema { get; }

    public JsonTypedColumnIntent? Intent { get; }

    public MigrationSourceValueKind SourceValueKind { get; }

    public bool RequiresFullStreamValidation { get; }

    public int ColumnIndex =>
        RepresentationSchema.ColumnIndex;

    public string SourceName =>
        RepresentationSchema.SourceName;

    public string OriginalPropertyName =>
        RepresentationSchema.OriginalPropertyName;

    public bool Nullable =>
        RepresentationSchema.Nullable;

    public JsonMissingPropertyPolicy MissingPolicy =>
        RepresentationSchema.MissingPolicy;

    private static MigrationSourceValueKind TypedSourceKind(
        JsonTypedValueCodec codec) =>
        codec switch
        {
            JsonTypedValueCodec.BinaryBase64 =>
                MigrationSourceValueKind.Binary,
            JsonTypedValueCodec.DecimalString or
            JsonTypedValueCodec.DecimalNumber =>
                MigrationSourceValueKind.Decimal,
            JsonTypedValueCodec.GuidD =>
                MigrationSourceValueKind.Guid,
            JsonTypedValueCodec.DateCSharpDbText =>
                MigrationSourceValueKind.Date,
            JsonTypedValueCodec.TimeCSharpDbText =>
                MigrationSourceValueKind.Time,
            JsonTypedValueCodec.DateTimeCSharpDbText =>
                MigrationSourceValueKind.DateTime,
            JsonTypedValueCodec.DateTimeOffsetCSharpDbText =>
                MigrationSourceValueKind.DateTimeOffset,
            JsonTypedValueCodec.Int64String =>
                MigrationSourceValueKind.SignedInteger,
            JsonTypedValueCodec.UInt64String =>
                MigrationSourceValueKind.UnsignedInteger,
            _ => throw new ArgumentOutOfRangeException(
                nameof(codec)),
        };

    private static MigrationSourceValueKind UntypedSourceKind(
        JsonTableColumnLogicalType logicalType) =>
        logicalType switch
        {
            JsonTableColumnLogicalType.Text or
            JsonTableColumnLogicalType.Json =>
                MigrationSourceValueKind.Text,
            JsonTableColumnLogicalType.Boolean =>
                MigrationSourceValueKind.Boolean,
            JsonTableColumnLogicalType.SignedInteger =>
                MigrationSourceValueKind.SignedInteger,
            JsonTableColumnLogicalType.UnsignedInteger =>
                MigrationSourceValueKind.UnsignedInteger,
            JsonTableColumnLogicalType.Decimal =>
                MigrationSourceValueKind.Decimal,
            _ => throw new ArgumentOutOfRangeException(
                nameof(logicalType)),
        };
}

/// <summary>
/// A typed schema is deliberately distinct from the untyped v1 result so it
/// cannot be passed to a v1 retained-package API or silently lose its intent.
/// </summary>
public sealed class JsonTypedTableSchemaInferenceResult
{
    internal JsonTypedTableSchemaInferenceResult(
        JsonTableSchemaInferenceResult representationSchema,
        JsonTypedIntentManifest intentManifest)
    {
        ArgumentNullException.ThrowIfNull(
            representationSchema);
        ArgumentNullException.ThrowIfNull(intentManifest);

        RepresentationSchema = representationSchema;
        IntentManifest = intentManifest;
        FrozenDictionary<int, JsonTypedColumnIntent>
            intentsByColumn = intentManifest.Columns
                .ToFrozenDictionary(
                    intent => intent.ColumnIndex,
                    JsonTypedIntentSidecar.CloneIntent);
        IntentsByColumn = intentsByColumn;
        FrozenSet<string> typedColumnObjectIds =
            intentsByColumn.Keys
                .Select(JsonMigrationObjectIds.Column)
                .ToFrozenSet(StringComparer.Ordinal);
        Columns = Array.AsReadOnly(
            representationSchema.Columns
                .Select(
                    column =>
                        new JsonTypedTableColumnSchema(
                            column,
                            intentsByColumn.GetValueOrDefault(
                                column.ColumnIndex),
                            representationSchema
                                .TypeProfileCoverage
                                .RequiresFullStreamValidation))
                .ToArray());
        Diagnostics = Array.AsReadOnly(
            representationSchema.Diagnostics
                .Where(
                    diagnostic =>
                        !string.Equals(
                            diagnostic.RuleId,
                            JsonTableSchemaDiagnosticRules
                                .OverrideMismatch,
                            StringComparison.Ordinal) ||
                        diagnostic.ObjectId is null ||
                        !typedColumnObjectIds.Contains(
                            diagnostic.ObjectId))
                .ToArray());
    }

    public const string AlgorithmId =
        "csharpdb-json-typed-table-schema-v1";

    public const string ScalarPolicyId =
        "csharpdb-json-typed-table-scalar-v1";

    public MigrationSourceIdentity Source =>
        RepresentationSchema.Source;

    public string SnapshotIdentity =>
        RepresentationSchema.SnapshotIdentity;

    public string ContentDigest =>
        RepresentationSchema.ContentDigest;

    public long ContentLength =>
        RepresentationSchema.ContentLength;

    public string TableName =>
        RepresentationSchema.TableName;

    public long TotalRecords =>
        RepresentationSchema.TotalRecords;

    public long EligibleObjectRecords =>
        RepresentationSchema.EligibleObjectRecords;

    public long IneligibleRecords =>
        RepresentationSchema.IneligibleRecords;

    public long TotalColumnNameBytes =>
        RepresentationSchema.TotalColumnNameBytes;

    public long ProfileRecordsExamined =>
        RepresentationSchema.ProfileRecordsExamined;

    public long ProfileBytesExamined =>
        RepresentationSchema.ProfileBytesExamined;

    public bool ProfileRecordLimitReached =>
        RepresentationSchema.ProfileRecordLimitReached;

    public bool ProfileByteLimitReached =>
        RepresentationSchema.ProfileByteLimitReached;

    public MigrationProfileCoverage StructuralCoverage =>
        RepresentationSchema.StructuralCoverage;

    public MigrationProfileCoverage TypeProfileCoverage =>
        RepresentationSchema.TypeProfileCoverage;

    public JsonTypedIntentManifest IntentManifest { get; }

    public ReadOnlyCollection<JsonTypedTableColumnSchema>
        Columns { get; }

    public ReadOnlyCollection<MigrationDiagnostic>
        Diagnostics { get; }

    public MigrationCatalog CreateCatalog(
        string targetCSharpDbVersion) =>
        JsonTypedMigrationCatalogBuilder.Build(
            this,
            targetCSharpDbVersion);

    internal JsonTableSchemaInferenceResult
        RepresentationSchema { get; }

    internal FrozenDictionary<int, JsonTypedColumnIntent>
        IntentsByColumn { get; }
}
