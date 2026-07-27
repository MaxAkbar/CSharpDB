namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Complete publication request for one already data-complete prepared CSV.
/// The expected digest must come from the source-qualified resumable writer,
/// rather than from an untrusted final sidecar.
/// </summary>
public sealed record CsvExportPublicationRequest
{
    /// <summary>The exact final CSV path used by the prepared writer.</summary>
    public required string DestinationPath { get; init; }

    /// <summary>
    /// Exact final sidecar path. It must be a distinct sibling of the CSV.
    /// </summary>
    public required string ManifestPath { get; init; }

    /// <summary>
    /// Independently retained lowercase SHA-256 digest of the canonical
    /// manifest represented by the data-complete checkpoint.
    /// </summary>
    public required string ExpectedManifestDigest { get; init; }
}

/// <summary>Final exact pair returned by manifest-last publication.</summary>
public sealed record CsvExportPublicationResult
{
    public required string DestinationPath { get; init; }

    public required string ManifestPath { get; init; }

    public required CsvExportManifest Manifest { get; init; }

    public required byte[] CanonicalManifestBytes { get; init; }

    public required string ManifestDigest { get; init; }

    /// <summary>True when an exact final CSV already existed.</summary>
    public required bool ReusedData { get; init; }

    /// <summary>True when an exact canonical sidecar already existed.</summary>
    public required bool ReusedManifest { get; init; }
}

/// <summary>
/// Publishes a qualified private prepared CSV as an atomic no-overwrite data
/// file followed by its canonical manifest. A manifest is never created before
/// the exact final data file exists and is held stable.
/// </summary>
public sealed class CsvExportPreparedOutputPublisher
{
    private readonly ICsvExportPublicationFaultInjector? faultInjector;

    public CsvExportPreparedOutputPublisher()
    {
    }

    internal CsvExportPreparedOutputPublisher(
        ICsvExportPublicationFaultInjector faultInjector)
    {
        this.faultInjector = faultInjector
            ?? throw new ArgumentNullException(nameof(faultInjector));
    }

    /// <summary>
    /// Validates the normalized, sibling, non-aliasing publication paths
    /// without opening the prepared source or creating any file.
    /// </summary>
    public static void ValidatePaths(
        string destinationPath,
        string manifestPath) =>
        CsvExportPreparedOutputFileSystem
            .ValidatePublicationPathsForPreflight(
                destinationPath,
                manifestPath);

    public async ValueTask<CsvExportPublicationResult> PublishCompletedAsync(
        CsvExportPublicationRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();
        ValidatePaths(
            request.DestinationPath,
            request.ManifestPath);

        CsvExportPreparedOutputLease? lease = null;
        bool committedPair = false;
        try
        {
            lease =
                await CsvExportPreparedOutputLease.OpenForPublicationAsync(
                        request.DestinationPath,
                        request.ExpectedManifestDigest,
                        cancellationToken)
                    .ConfigureAwait(false);
            CsvExportCheckpoint checkpoint = lease.CurrentCheckpoint
                ?? throw new InvalidOperationException(
                    "The data-complete CSV export checkpoint is unavailable.");
            CsvExportManifest manifest =
                CsvExportCheckpointSerializer.CreateCompletedManifest(
                    checkpoint);
            byte[] canonicalManifestBytes =
                CsvExportManifestSerializer.Serialize(manifest);
            string manifestDigest =
                CsvExportManifestSerializer.ComputeManifestDigest(manifest);
            if (!string.Equals(
                    manifestDigest,
                    request.ExpectedManifestDigest,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    "The reconstructed CSV export manifest does not match the expected digest.");
            }

            CsvExportFilePublicationResult files =
                await lease.PublishCompletedAsync(
                        request.ManifestPath,
                        canonicalManifestBytes,
                        faultInjector,
                        cancellationToken)
                    .ConfigureAwait(false);

            var result = new CsvExportPublicationResult
            {
                DestinationPath = lease.DestinationPath,
                ManifestPath = Path.GetFullPath(request.ManifestPath),
                Manifest = manifest,
                CanonicalManifestBytes = canonicalManifestBytes,
                ManifestDigest = manifestDigest,
                ReusedData = files.ReusedData,
                ReusedManifest = files.ReusedManifest,
            };
            committedPair = true;
            return result;
        }
        finally
        {
            if (lease is not null)
            {
                try
                {
                    await lease.DisposeAsync().ConfigureAwait(false);
                }
                catch when (committedPair)
                {
                    // A qualified final pair is the commit. Releasing private
                    // recovery handles cannot retroactively turn it into a
                    // reported publication failure.
                }
            }
        }
    }
}

internal enum CsvExportPublicationFaultPoint
{
    BeforeDataNamespaceCommit,
    AfterDataNamespaceCommitBeforeManifest,
    BeforeManifestNamespaceCommit,
    AfterManifestNamespaceCommitBeforeResult,
}

internal interface ICsvExportPublicationFaultInjector
{
    ValueTask InjectAsync(
        CsvExportPublicationFaultPoint point,
        CancellationToken cancellationToken);
}

internal sealed record CsvExportFilePublicationResult(
    bool ReusedData,
    bool ReusedManifest);
