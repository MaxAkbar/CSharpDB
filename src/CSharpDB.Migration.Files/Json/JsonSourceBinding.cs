using System.Globalization;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Binds an immutable raw-byte snapshot to exact JSON framing, logical-value,
/// and resource-limit semantics. Changing bytes or any reader policy other
/// than stream ownership changes the source fingerprint.
/// </summary>
public sealed class JsonSourceBinding
{
    internal const string AdapterProviderVersion =
        "csharpdb-json-adapter-v1";
    internal const string SourceFingerprintAlgorithm =
        "csharpdb-json-source-v1";
    internal const string OptionsAlgorithm =
        "csharpdb-json-options-v1";
    internal const string SnapshotConsistencyDescription =
        "Private immutable raw-byte snapshot bound to strict JSON reader semantics.";

    private const string ContentIdentityPrefix = "json-content:";
    private const string LogicalIdentityPrefix = "json-logical:";
    private const string LogicalIdentityAlgorithm =
        "csharpdb-json-logical-id-v1";

    private readonly JsonStreamingReaderOptions readerOptions;

    private JsonSourceBinding(
        MigrationSourceIdentity source,
        string snapshotIdentity,
        string contentDigest,
        long contentLength,
        string optionsDigest,
        JsonStreamingReaderOptions readerOptions)
    {
        Source = source;
        SnapshotIdentity = snapshotIdentity;
        ContentDigest = contentDigest;
        ContentLength = contentLength;
        OptionsDigest = optionsDigest;
        this.readerOptions = readerOptions;
    }

    public MigrationSourceIdentity Source { get; }

    public string SnapshotIdentity { get; }

    public string ContentDigest { get; }

    public long ContentLength { get; }

    public string OptionsDigest { get; }

    public JsonInputFraming Framing => readerOptions.Framing;

    /// <summary>
    /// Returns a normalized defensive copy for adapter-owned replay. Reader
    /// ownership is always forced to false because each replay owns its lease.
    /// </summary>
    internal JsonStreamingReaderOptions ReaderOptions =>
        FreezeReaderOptions(readerOptions);

    public static async ValueTask<JsonSourceBinding> CreateAsync(
        JsonSourceSnapshot snapshot,
        JsonStreamingReaderOptions? readerOptions = null,
        string? logicalSourceIdentity = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        // Freeze and validate every option before hashing snapshot bytes.
        JsonStreamingReaderOptions normalizedOptions =
            FreezeReaderOptions(
                readerOptions ?? new JsonStreamingReaderOptions());
        if (logicalSourceIdentity is not null &&
            (string.IsNullOrWhiteSpace(logicalSourceIdentity) ||
             logicalSourceIdentity.Length > 4096))
        {
            throw new ArgumentException(
                "The logical JSON source identity must be nonblank and at most 4096 characters.",
                nameof(logicalSourceIdentity));
        }

        cancellationToken.ThrowIfCancellationRequested();
        await snapshot
            .VerifyIntegrityAsync(cancellationToken)
            .ConfigureAwait(false);

        string optionsDigest = DigestOptions(normalizedOptions);
        string sourceFingerprint = ComputeSourceFingerprint(
            snapshot.ContentDigest,
            snapshot.ContentLength,
            optionsDigest);
        string safeIdentity = logicalSourceIdentity is null
            ? ContentIdentityPrefix + snapshot.ContentDigest
            : LogicalIdentityPrefix + JsonStableDigest.Compute(
                LogicalIdentityAlgorithm,
                logicalSourceIdentity);
        var source = new MigrationSourceIdentity
        {
            Kind = MigrationSourceKind.Json,
            Identity = safeIdentity,
            Fingerprint = sourceFingerprint,
            ProviderVersion = AdapterProviderVersion,
            SourceVersion = null,
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Snapshot,
                Description = SnapshotConsistencyDescription,
                Watermark = null,
            },
        };

        return new JsonSourceBinding(
            source,
            snapshot.SnapshotIdentity,
            snapshot.ContentDigest,
            snapshot.ContentLength,
            optionsDigest,
            normalizedOptions);
    }

    /// <summary>
    /// Opens a fresh strict reader over the exact snapshot bound to this
    /// source. The returned reader owns and releases its snapshot lease.
    /// </summary>
    public async ValueTask<JsonStreamingReader> OpenReaderAsync(
        JsonSourceSnapshot snapshot,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        if (!string.Equals(
                snapshot.SnapshotIdentity,
                SnapshotIdentity,
                StringComparison.Ordinal) ||
            !string.Equals(
                snapshot.ContentDigest,
                ContentDigest,
                StringComparison.Ordinal) ||
            snapshot.ContentLength != ContentLength)
        {
            throw new ArgumentException(
                "The JSON source binding cannot be used with a different snapshot.",
                nameof(snapshot));
        }

        cancellationToken.ThrowIfCancellationRequested();
        Stream stream = snapshot.OpenRead();
        try
        {
            return await JsonStreamingReader
                .OpenAsync(stream, readerOptions, cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private static JsonStreamingReaderOptions FreezeReaderOptions(
        JsonStreamingReaderOptions options)
    {
        JsonStreamingReaderSettings settings =
            JsonStreamingReaderSettings.Create(options);
        return new JsonStreamingReaderOptions
        {
            Framing = settings.Framing,
            MaxValueBytes = settings.MaxValueBytes,
            MaxDepth = settings.MaxDepth,
            MaxPropertiesPerObject =
                settings.MaxPropertiesPerObject,
            MaxArrayElements = settings.MaxArrayElements,
            MaxTotalNodes = settings.MaxTotalNodes,
            MaxPropertyNameBytes = settings.MaxPropertyNameBytes,
            MaxStringBytes = settings.MaxStringBytes,
            MaxNumberBytes = settings.MaxNumberBytes,
            LeaveOpen = false,
        };
    }

    private static string DigestOptions(
        JsonStreamingReaderOptions options) =>
        JsonStableDigest.Compute(
            OptionsAlgorithm,
            JsonInputContracts.EncodingName,
            JsonInputContracts.EncodingPolicy,
            JsonInputContracts.AcceptsLeadingUtf8Bom
                ? "leading-utf8-bom-accepted"
                : "leading-utf8-bom-rejected",
            JsonInputContracts.DecodedPropertyNameComparison,
            JsonInputContracts.PropertyOrderPolicy,
            JsonInputContracts.NumberLexemePolicy,
            JsonInputContracts.DuplicatePropertyPolicy,
            JsonInputContracts.CanonicalNestedJsonVersion,
            FramingName(options.Framing),
            options.MaxValueBytes.ToString(CultureInfo.InvariantCulture),
            options.MaxDepth.ToString(CultureInfo.InvariantCulture),
            options.MaxPropertiesPerObject.ToString(
                CultureInfo.InvariantCulture),
            options.MaxArrayElements.ToString(
                CultureInfo.InvariantCulture),
            options.MaxTotalNodes.ToString(
                CultureInfo.InvariantCulture),
            options.MaxPropertyNameBytes.ToString(
                CultureInfo.InvariantCulture),
            options.MaxStringBytes.ToString(
                CultureInfo.InvariantCulture),
            options.MaxNumberBytes.ToString(
                CultureInfo.InvariantCulture));

    private static string ComputeSourceFingerprint(
        string contentDigest,
        long contentLength,
        string optionsDigest) =>
        JsonStableDigest.Compute(
            SourceFingerprintAlgorithm,
            contentDigest,
            contentLength.ToString(CultureInfo.InvariantCulture),
            optionsDigest);

    private static string FramingName(JsonInputFraming framing) =>
        framing switch
        {
            JsonInputFraming.RootArray => "root-array",
            JsonInputFraming.MultipleValues => "multiple-values",
            _ => throw new ArgumentOutOfRangeException(nameof(framing)),
        };
}
