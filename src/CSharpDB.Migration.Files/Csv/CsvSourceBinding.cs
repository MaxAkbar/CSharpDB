using CSharpDB.Migration;
using System.Globalization;
using System.Security.Cryptography;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Binds an immutable raw-byte snapshot to normalized CSV semantics. Changing
/// either the bytes or a semantic format option changes the source fingerprint.
/// </summary>
public sealed class CsvSourceBinding
{
    internal const string AdapterProviderVersion = "csharpdb-csv-adapter-v1";
    private const string ContentIdentityPrefix = "csv-content:";
    private const string LogicalIdentityPrefix = "csv-logical:";
    internal const string SourceFingerprintAlgorithm = "csharpdb-csv-source-v1";
    internal const string FormatAlgorithm = "csharpdb-csv-format-v1";
    private const string LogicalIdentityAlgorithm = "csharpdb-csv-logical-id-v1";
    internal const string SnapshotConsistencyDescription =
        "Private immutable raw-byte snapshot bound to normalized CSV format semantics.";

    private readonly CsvReaderOptions readerOptions;

    private CsvSourceBinding(
        MigrationSourceIdentity source,
        string snapshotIdentity,
        string contentDigest,
        long contentLength,
        CsvResolvedFormat format,
        string optionsDigest,
        CsvReaderOptions readerOptions)
    {
        Source = source;
        SnapshotIdentity = snapshotIdentity;
        ContentDigest = contentDigest;
        ContentLength = contentLength;
        Format = format;
        OptionsDigest = optionsDigest;
        this.readerOptions = readerOptions;
    }

    public MigrationSourceIdentity Source { get; }

    public string SnapshotIdentity { get; }

    public string ContentDigest { get; }

    public long ContentLength { get; }

    public CsvResolvedFormat Format { get; }

    public string OptionsDigest { get; }

    internal CultureInfo Culture => readerOptions.Culture;

    /// <summary>
    /// Returns a normalized defensive copy suitable for durable serialization.
    /// The nested culture and encoding instances cannot mutate this binding.
    /// </summary>
    internal CsvReaderOptions ReaderOptions => FreezeReaderOptions(readerOptions);

    public static async ValueTask<CsvSourceBinding> CreateAsync(
        CsvSourceSnapshot snapshot,
        CsvFormatInspection inspection,
        string? logicalSourceIdentity = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(inspection);
        if (inspection.Delimiter.Resolution != CsvInspectionResolution.Resolved ||
            inspection.Format is null ||
            inspection.ResolvedReaderOptions is null)
        {
            throw new InvalidOperationException(
                "A CSV source binding requires a resolved delimiter and normalized format.");
        }
        if (!string.Equals(
                snapshot.SnapshotIdentity,
                inspection.SnapshotIdentity,
                StringComparison.Ordinal) ||
            !string.Equals(snapshot.ContentDigest, inspection.ContentDigest, StringComparison.Ordinal) ||
            snapshot.ContentLength != inspection.ContentLength)
        {
            throw new ArgumentException(
                "The CSV inspection belongs to a different source snapshot.",
                nameof(inspection));
        }
        if (logicalSourceIdentity is not null)
        {
            if (string.IsNullOrWhiteSpace(logicalSourceIdentity) || logicalSourceIdentity.Length > 4096)
            {
                throw new ArgumentException(
                    "The logical CSV source identity must be nonblank and at most 4096 characters.",
                    nameof(logicalSourceIdentity));
            }
        }

        await snapshot.VerifyIntegrityAsync(cancellationToken).ConfigureAwait(false);

        string optionsDigest = DigestFormat(inspection.Format);
        string sourceFingerprint = ComputeSourceFingerprint(
            inspection.ContentDigest,
            inspection.ContentLength,
            optionsDigest);
        string safeIdentity = logicalSourceIdentity is null
            ? ContentIdentityPrefix + inspection.ContentDigest
            : LogicalIdentityPrefix + DigestComponents(LogicalIdentityAlgorithm, logicalSourceIdentity);
        var source = new MigrationSourceIdentity
        {
            Kind = MigrationSourceKind.Csv,
            Identity = safeIdentity,
            Fingerprint = sourceFingerprint,
            ProviderVersion = AdapterProviderVersion,
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Snapshot,
                Description = SnapshotConsistencyDescription,
            },
        };

        CsvReaderSettings settings = CsvReaderSettings.Create(inspection.ResolvedReaderOptions);
        return new CsvSourceBinding(
            source,
            inspection.SnapshotIdentity,
            inspection.ContentDigest,
            inspection.ContentLength,
            inspection.Format,
            optionsDigest,
            settings.ToOptions(leaveOpen: false));
    }

    /// <summary>
    /// Restores a durable CSV binding only after re-deriving every value that
    /// depends on the snapshot bytes or normalized reader semantics.
    /// </summary>
    internal static async ValueTask<CsvSourceBinding> RestoreAsync(
        CsvSourceSnapshot snapshot,
        MigrationSourceIdentity source,
        CsvResolvedFormat format,
        string expectedOptionsDigest,
        CsvReaderOptions readerOptions,
        CancellationToken cancellationToken = default) =>
        await RestoreCoreAsync(
                snapshot,
                source,
                format,
                expectedOptionsDigest,
                readerOptions,
                verifySnapshotIntegrity: true,
                cancellationToken)
            .ConfigureAwait(false);

    /// <summary>
    /// Restores a binding after the immediate caller has already hashed and
    /// matched a freshly created private snapshot.
    /// </summary>
    internal static async ValueTask<CsvSourceBinding> RestoreFromVerifiedSnapshotAsync(
        CsvSourceSnapshot snapshot,
        MigrationSourceIdentity source,
        CsvResolvedFormat format,
        string expectedOptionsDigest,
        CsvReaderOptions readerOptions,
        CancellationToken cancellationToken = default) =>
        await RestoreCoreAsync(
                snapshot,
                source,
                format,
                expectedOptionsDigest,
                readerOptions,
                verifySnapshotIntegrity: false,
                cancellationToken)
            .ConfigureAwait(false);

    private static async ValueTask<CsvSourceBinding> RestoreCoreAsync(
        CsvSourceSnapshot snapshot,
        MigrationSourceIdentity source,
        CsvResolvedFormat format,
        string expectedOptionsDigest,
        CsvReaderOptions readerOptions,
        bool verifySnapshotIntegrity,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(format);
        ArgumentNullException.ThrowIfNull(readerOptions);

        if (!IsCanonicalDigest(expectedOptionsDigest))
        {
            throw new InvalidDataException(
                "The restored CSV options digest is not canonical SHA-256 text.");
        }

        CsvReaderSettings settings;
        try
        {
            settings = CsvReaderSettings.Create(readerOptions);
        }
        catch (Exception exception) when (exception is ArgumentException or NotSupportedException)
        {
            throw new InvalidDataException(
                "The restored CSV reader options are invalid.",
                exception);
        }

        CsvReaderOptions normalizedOptions = settings.ToOptions(leaveOpen: false);
        ValidateFormatSettings(format, settings);

        if (verifySnapshotIntegrity)
            await snapshot.VerifyIntegrityAsync(cancellationToken).ConfigureAwait(false);
        CsvEncodingResolution resolvedEncoding = await ResolveSnapshotEncodingAsync(
                snapshot,
                settings,
                cancellationToken)
            .ConfigureAwait(false);
        ValidateResolvedEncoding(format, resolvedEncoding);

        string optionsDigest = DigestFormat(format);
        if (!string.Equals(optionsDigest, expectedOptionsDigest, StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The restored CSV options digest does not match the normalized format.");
        }

        string sourceFingerprint = ComputeSourceFingerprint(
            snapshot.ContentDigest,
            snapshot.ContentLength,
            optionsDigest);
        MigrationSourceIdentity canonicalSource = RestoreCanonicalSource(
            source,
            snapshot.ContentDigest,
            sourceFingerprint);

        return new CsvSourceBinding(
            canonicalSource,
            snapshot.SnapshotIdentity,
            snapshot.ContentDigest,
            snapshot.ContentLength,
            format,
            optionsDigest,
            normalizedOptions);
    }

    public async ValueTask<CsvStreamingReader> OpenReaderAsync(
        CsvSourceSnapshot snapshot,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        if (!string.Equals(snapshot.SnapshotIdentity, SnapshotIdentity, StringComparison.Ordinal) ||
            !string.Equals(snapshot.ContentDigest, ContentDigest, StringComparison.Ordinal) ||
            snapshot.ContentLength != ContentLength)
        {
            throw new ArgumentException(
                "The CSV source binding cannot be used with a different snapshot.",
                nameof(snapshot));
        }

        Stream stream = snapshot.OpenRead();
        try
        {
            return await CsvStreamingReader.OpenAsync(stream, readerOptions, cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private static string DigestFormat(CsvResolvedFormat format) =>
        DigestComponents(
            FormatAlgorithm,
            format.Delimiter,
            ((int)format.Quote).ToString(CultureInfo.InvariantCulture),
            format.HasHeaderRecord ? "true" : "false",
            format.EncodingName,
            format.EncodingCodePage.ToString(CultureInfo.InvariantCulture),
            format.HasByteOrderMark ? "bom-consumed" : "no-bom-consumed",
            format.CultureName,
            format.CulturePolicyDigest,
            format.NullToken,
            format.NullTokenMatchesQuotedFields ? "quoted-null" : "unquoted-null-only",
            format.ExpectedFieldCount?.ToString(CultureInfo.InvariantCulture),
            format.NewlinePolicy);

    private static string ComputeSourceFingerprint(
        string contentDigest,
        long contentLength,
        string optionsDigest) =>
        DigestComponents(
            SourceFingerprintAlgorithm,
            contentDigest,
            contentLength.ToString(CultureInfo.InvariantCulture),
            optionsDigest);

    private static CsvReaderOptions FreezeReaderOptions(CsvReaderOptions options) =>
        CsvReaderSettings.Create(options).ToOptions(leaveOpen: false);

    private static void ValidateFormatSettings(
        CsvResolvedFormat format,
        CsvReaderSettings settings)
    {
        string culturePolicyDigest = CsvCulturePolicy.ComputeDigest(settings.Culture);
        if (!string.Equals(format.Delimiter, settings.Delimiter.ToString(), StringComparison.Ordinal) ||
            format.Quote != settings.Quote ||
            format.HasHeaderRecord != settings.HasHeaderRecord ||
            !string.Equals(format.CultureName, settings.Culture.Name, StringComparison.Ordinal) ||
            !string.Equals(
                format.CulturePolicyDigest,
                culturePolicyDigest,
                StringComparison.Ordinal) ||
            !string.Equals(format.NullToken, settings.NullToken, StringComparison.Ordinal) ||
            format.NullTokenMatchesQuotedFields != settings.NullTokenMatchesQuotedFields ||
            format.ExpectedFieldCount != settings.ExpectedFieldCount)
        {
            throw new InvalidDataException(
                "The restored CSV format does not match the normalized reader options.");
        }
    }

    private static async ValueTask<CsvEncodingResolution> ResolveSnapshotEncodingAsync(
        CsvSourceSnapshot snapshot,
        CsvReaderSettings settings,
        CancellationToken cancellationToken)
    {
        int prefixLength = (int)Math.Min(snapshot.ContentLength, 4L);
        byte[] prefix = new byte[prefixLength];
        try
        {
            await using Stream stream = snapshot.OpenRead();
            if (prefix.Length > 0)
            {
                await stream.ReadExactlyAsync(prefix, cancellationToken).ConfigureAwait(false);
            }

            return CsvEncodingResolver.Resolve(
                prefix,
                settings.Encoding,
                settings.DetectEncodingFromByteOrderMarks);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(prefix);
        }
    }

    private static void ValidateResolvedEncoding(
        CsvResolvedFormat format,
        CsvEncodingResolution resolvedEncoding)
    {
        if (!string.Equals(
                format.EncodingName,
                resolvedEncoding.Encoding.WebName,
                StringComparison.Ordinal) ||
            format.EncodingCodePage != resolvedEncoding.Encoding.CodePage ||
            format.HasByteOrderMark != resolvedEncoding.HasByteOrderMark)
        {
            throw new InvalidDataException(
                "The restored CSV encoding or byte-order mark does not match the snapshot bytes.");
        }
    }

    private static MigrationSourceIdentity RestoreCanonicalSource(
        MigrationSourceIdentity source,
        string contentDigest,
        string sourceFingerprint)
    {
        MigrationConsistencyStrategy? consistency = source.Consistency;
        if (source.Kind != MigrationSourceKind.Csv ||
            !string.Equals(
                source.ProviderVersion,
                AdapterProviderVersion,
                StringComparison.Ordinal) ||
            source.SourceVersion is not null ||
            consistency is null ||
            consistency.Kind != MigrationConsistencyKind.Snapshot ||
            !string.Equals(
                consistency.Description,
                SnapshotConsistencyDescription,
                StringComparison.Ordinal) ||
            consistency.Watermark is not null)
        {
            throw new InvalidDataException(
                "The restored migration source is not canonical CSV snapshot metadata.");
        }

        if (!IsSafeSourceIdentity(source.Identity, contentDigest))
        {
            throw new InvalidDataException(
                "The restored CSV source identity is not a canonical safe identity.");
        }

        if (!string.Equals(source.Fingerprint, sourceFingerprint, StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The restored CSV source fingerprint does not match the snapshot and format.");
        }

        return new MigrationSourceIdentity
        {
            Kind = MigrationSourceKind.Csv,
            Identity = source.Identity,
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
    }

    private static bool IsSafeSourceIdentity(string? identity, string contentDigest)
    {
        if (string.Equals(
                identity,
                ContentIdentityPrefix + contentDigest,
                StringComparison.Ordinal))
        {
            return true;
        }

        return identity is not null &&
            identity.StartsWith(LogicalIdentityPrefix, StringComparison.Ordinal) &&
            IsCanonicalDigest(identity[LogicalIdentityPrefix.Length..]);
    }

    private static bool IsCanonicalDigest(string? digest)
    {
        if (digest is null ||
            digest.Length != 71 ||
            !digest.StartsWith("sha256:", StringComparison.Ordinal))
        {
            return false;
        }

        foreach (char character in digest.AsSpan(7))
        {
            if (character is not (>= '0' and <= '9') and
                not (>= 'a' and <= 'f'))
            {
                return false;
            }
        }

        return true;
    }

    private static string DigestComponents(params string?[] components) =>
        CsvStableDigest.Compute(components);
}
