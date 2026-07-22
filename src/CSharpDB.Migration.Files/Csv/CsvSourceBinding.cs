using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Binds an immutable raw-byte snapshot to normalized CSV semantics. Changing
/// either the bytes or a semantic format option changes the source fingerprint.
/// </summary>
public sealed class CsvSourceBinding
{
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
        string sourceFingerprint = DigestComponents(
            "csharpdb-csv-source-v1",
            inspection.ContentDigest,
            inspection.ContentLength.ToString(System.Globalization.CultureInfo.InvariantCulture),
            optionsDigest);
        string safeIdentity = logicalSourceIdentity is null
            ? $"csv-content:{inspection.ContentDigest}"
            : $"csv-logical:{DigestComponents("csharpdb-csv-logical-id-v1", logicalSourceIdentity)}";
        var source = new MigrationSourceIdentity
        {
            Kind = MigrationSourceKind.Csv,
            Identity = safeIdentity,
            Fingerprint = sourceFingerprint,
            ProviderVersion = "csharpdb-csv-adapter-v1",
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Snapshot,
                Description = "Private immutable raw-byte snapshot bound to normalized CSV format semantics.",
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
            "csharpdb-csv-format-v1",
            format.Delimiter,
            ((int)format.Quote).ToString(System.Globalization.CultureInfo.InvariantCulture),
            format.HasHeaderRecord ? "true" : "false",
            format.EncodingName,
            format.EncodingCodePage.ToString(System.Globalization.CultureInfo.InvariantCulture),
            format.HasByteOrderMark ? "bom-consumed" : "no-bom-consumed",
            format.CultureName,
            format.CulturePolicyDigest,
            format.NullToken,
            format.NullTokenMatchesQuotedFields ? "quoted-null" : "unquoted-null-only",
            format.ExpectedFieldCount?.ToString(System.Globalization.CultureInfo.InvariantCulture),
            format.NewlinePolicy);

    private static string DigestComponents(params string?[] components) =>
        CsvStableDigest.Compute(components);
}
