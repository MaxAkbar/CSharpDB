using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Adapts a caller-owned immutable CSV snapshot and its exact format binding
/// into the shared migration inspection contract. This inspector never owns
/// or disposes the snapshot.
/// </summary>
public sealed class CsvMigrationSourceInspector : IMigrationSourceInspector
{
    private readonly CsvSourceBinding binding;
    private readonly CsvSourceSnapshot snapshot;
    private readonly CsvSchemaInferenceOptions options;

    public CsvMigrationSourceInspector(
        CsvSourceBinding binding,
        CsvSourceSnapshot snapshot,
        CsvSchemaInferenceOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(binding);
        ArgumentNullException.ThrowIfNull(snapshot);
        if (!string.Equals(binding.SnapshotIdentity, snapshot.SnapshotIdentity, StringComparison.Ordinal) ||
            !string.Equals(binding.ContentDigest, snapshot.ContentDigest, StringComparison.Ordinal) ||
            binding.ContentLength != snapshot.ContentLength)
        {
            throw new ArgumentException(
                "The CSV source inspector binding belongs to a different snapshot.",
                nameof(snapshot));
        }

        CsvSchemaInferenceOptions sourceOptions = options ?? new CsvSchemaInferenceOptions();
        if (sourceOptions.ColumnOverrides is null)
            throw new ArgumentException("CSV column overrides cannot be null.", nameof(options));

        this.binding = binding;
        this.snapshot = snapshot;
        this.options = sourceOptions with
        {
            ColumnOverrides = sourceOptions.ColumnOverrides.ToArray(),
        };
    }

    public MigrationSourceKind SourceKind => MigrationSourceKind.Csv;

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
                $"The CSV adapter is qualified for CSharpDB {CSharpDbCapabilityCatalogLoader.CurrentTargetVersion}.");
        }
        if (request.ProfileSampleSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(request), "Profile sample size must be positive.");

        CsvSchemaInferenceResult result = request.IncludeProfile
            ? await CsvSchemaInferer.InferAsync(
                    binding,
                    snapshot,
                    request.ProfileSampleSize,
                    options,
                    cancellationToken)
                .ConfigureAwait(false)
            : await CsvSchemaInferer.DiscoverAsync(
                    binding,
                    snapshot,
                    options,
                    cancellationToken)
                .ConfigureAwait(false);
        return result.CreateCatalog(request.TargetCSharpDbVersion);
    }
}
