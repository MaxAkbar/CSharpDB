using System.Text;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Adapts a caller-owned immutable JSON snapshot and exact reader binding into
/// the shared migration inspection contract. Structural discovery always
/// scans the complete snapshot; optional profiling controls only type
/// evidence.
/// </summary>
public sealed class JsonMigrationSourceInspector :
    IMigrationSourceInspector
{
    private static readonly UTF8Encoding s_strictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);

    private readonly JsonSourceBinding binding;
    private readonly JsonSourceSnapshot snapshot;
    private readonly JsonTableSchemaInferenceOptions options;

    public JsonMigrationSourceInspector(
        JsonSourceBinding binding,
        JsonSourceSnapshot snapshot,
        JsonTableSchemaInferenceOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(binding);
        ArgumentNullException.ThrowIfNull(snapshot);
        if (!string.Equals(
                binding.SnapshotIdentity,
                snapshot.SnapshotIdentity,
                StringComparison.Ordinal) ||
            !string.Equals(
                binding.ContentDigest,
                snapshot.ContentDigest,
                StringComparison.Ordinal) ||
            binding.ContentLength != snapshot.ContentLength)
        {
            throw new ArgumentException(
                "The JSON source inspector binding belongs to a different snapshot.",
                nameof(snapshot));
        }

        this.binding = binding;
        this.snapshot = snapshot;
        this.options = ValidateAndFreezeOptions(
            options ?? new JsonTableSchemaInferenceOptions(),
            binding.ReaderOptions.MaxPropertyNameBytes);
    }

    public MigrationSourceKind SourceKind => MigrationSourceKind.Json;

    public async ValueTask<MigrationCatalog> InspectAsync(
        MigrationInspectionRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();
        if (!string.Equals(
                request.TargetCSharpDbVersion,
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                StringComparison.Ordinal))
        {
            throw new NotSupportedException(
                $"The JSON adapter is qualified for CSharpDB {CSharpDbCapabilityCatalogLoader.CurrentTargetVersion}.");
        }
        if (request.ProfileSampleSize <= 0 ||
            request.ProfileSampleSize >
                JsonTableSchemaInferenceOptions
                    .MaximumSupportedProfileRecords)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                $"Profile sample size must be between 1 and {JsonTableSchemaInferenceOptions.MaximumSupportedProfileRecords}.");
        }

        JsonTableSchemaInferenceResult result =
            request.IncludeProfile
                ? await JsonTableSchemaInferer.InferAsync(
                        binding,
                        snapshot,
                        request.ProfileSampleSize,
                        options,
                        cancellationToken)
                    .ConfigureAwait(false)
                : await JsonTableSchemaInferer.DiscoverAsync(
                        binding,
                        snapshot,
                        options,
                        cancellationToken)
                    .ConfigureAwait(false);
        return JsonMigrationCatalogBuilder.Build(
            result,
            request.TargetCSharpDbVersion);
    }

    private static JsonTableSchemaInferenceOptions ValidateAndFreezeOptions(
        JsonTableSchemaInferenceOptions options,
        int maximumPropertyNameBytes)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(options.TableName) ||
            options.TableName.Length > 1024)
        {
            throw new ArgumentException(
                "The JSON table name must be nonblank and at most 1024 characters.",
                nameof(options));
        }
        try
        {
            _ = s_strictUtf8.GetByteCount(options.TableName);
        }
        catch (EncoderFallbackException exception)
        {
            throw new ArgumentException(
                "The JSON table name contains invalid Unicode.",
                nameof(options),
                exception);
        }
        if (options.ColumnOverrides is null)
        {
            throw new ArgumentException(
                "JSON table column overrides cannot be null.",
                nameof(options));
        }
        if (options.MaxColumns is < 1 or >
            JsonTableSchemaInferenceOptions.MaximumSupportedColumns)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                $"The JSON table column limit must be between 1 and {JsonTableSchemaInferenceOptions.MaximumSupportedColumns}.");
        }
        if (options.MaxTotalColumnNameBytes is < 1 or >
            JsonTableSchemaInferenceOptions
                .MaximumSupportedTotalColumnNameBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                $"The cumulative JSON column-name byte limit must be between 1 and {JsonTableSchemaInferenceOptions.MaximumSupportedTotalColumnNameBytes}.");
        }
        if (options.MaxProfileBytes is < 1 or >
            JsonTableSchemaInferenceOptions.MaximumSupportedProfileBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                $"The JSON type-profile byte limit must be between 1 and {JsonTableSchemaInferenceOptions.MaximumSupportedProfileBytes}.");
        }
        if (options.ColumnOverrides.Count > options.MaxColumns)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "The JSON override count cannot exceed the configured column limit.");
        }

        var seen = new HashSet<int>();
        var overrides =
            new JsonTableColumnSchemaOverride[
                options.ColumnOverrides.Count];
        for (int index = 0; index < overrides.Length; index++)
        {
            JsonTableColumnSchemaOverride item =
                options.ColumnOverrides[index] ??
                throw new ArgumentException(
                    "JSON table column overrides cannot contain null values.",
                    nameof(options));
            if (item.ColumnIndex < 0 ||
                item.ColumnIndex >= options.MaxColumns)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(options),
                    "A JSON column override index is outside the configured column limit.");
            }
            if (!seen.Add(item.ColumnIndex))
            {
                throw new ArgumentException(
                    $"JSON column {item.ColumnIndex} has more than one schema override.",
                    nameof(options));
            }
            if (item.ExpectedPropertyName is null)
            {
                throw new ArgumentException(
                    "A JSON column override must include an exact property-name guard.",
                    nameof(options));
            }
            try
            {
                if (s_strictUtf8.GetByteCount(
                        item.ExpectedPropertyName) >
                    maximumPropertyNameBytes)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(options),
                        "A JSON override property name exceeds the bound reader limit.");
                }
            }
            catch (EncoderFallbackException exception)
            {
                throw new ArgumentException(
                    "A JSON override property name contains invalid Unicode.",
                    nameof(options),
                    exception);
            }
            if (!Enum.IsDefined(item.LogicalType) ||
                !Enum.IsDefined(item.MissingPolicy))
            {
                throw new ArgumentOutOfRangeException(
                    nameof(options),
                    "A JSON column override policy is invalid.");
            }
            if (item.MissingPolicy ==
                    JsonMissingPropertyPolicy.AsNull &&
                item.Nullable == false)
            {
                throw new ArgumentException(
                    "A JSON MissingAsNull override cannot declare the column non-nullable.",
                    nameof(options));
            }

            overrides[index] = new JsonTableColumnSchemaOverride
            {
                ColumnIndex = item.ColumnIndex,
                ExpectedPropertyName = item.ExpectedPropertyName,
                LogicalType = item.LogicalType,
                Nullable = item.Nullable,
                MissingPolicy = item.MissingPolicy,
            };
        }

        return options with
        {
            ColumnOverrides = Array.AsReadOnly(overrides),
        };
    }
}
