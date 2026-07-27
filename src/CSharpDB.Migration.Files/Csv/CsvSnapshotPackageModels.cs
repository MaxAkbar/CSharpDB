using System.Globalization;
using System.Runtime.ExceptionServices;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>Stable rules raised while publishing or reopening a retained CSV package.</summary>
public static class CsvSnapshotPackageRules
{
    public const string InvalidFormat = "MIG-CSV-PACKAGE-FORMAT-001";
    public const string IntegrityMismatch = "MIG-CSV-PACKAGE-INTEGRITY-001";
    public const string PolicyMismatch = "MIG-CSV-PACKAGE-POLICY-001";
    public const string SizeLimitExceeded = "MIG-CSV-PACKAGE-LIMIT-001";
    public const string UnsafePath = "MIG-CSV-PACKAGE-PATH-001";
}

public sealed class CsvSnapshotPackageException : IOException
{
    internal CsvSnapshotPackageException(string ruleId, string message)
        : base(message)
    {
        RuleId = ruleId;
    }

    internal CsvSnapshotPackageException(
        string ruleId,
        string message,
        Exception innerException)
        : base(message, innerException)
    {
        RuleId = ruleId;
    }

    public string RuleId { get; }
}

/// <summary>Bounds and environment policy for reopening a retained CSV package.</summary>
public sealed record CsvSnapshotPackageOpenOptions
{
    /// <summary>
    /// Caller-controlled parent directory for the new private, ephemeral
    /// snapshot. It must remain inaccessible to untrusted writers and stable
    /// until the returned package session is disposed. The retained package is
    /// never used as a live migration reader.
    /// </summary>
    public string? WorkspacePath { get; init; }

    public long MaxSourceBytes { get; init; } = 1024L * 1024 * 1024 * 1024;

    public int CopyBufferBytes { get; init; } = 128 * 1024;

    /// <summary>
    /// Optional trusted SHA-256 returned by the package writer. When present,
    /// it is checked against the fixed header before manifest allocation or
    /// raw snapshot copying.
    /// </summary>
    public string? ExpectedManifestDigest { get; init; }

    /// <summary>
    /// Optional culture policy supplied when the current platform can no
    /// longer reproduce the package's named culture. Its complete policy
    /// digest must still match the retained manifest.
    /// </summary>
    public CultureInfo? CultureOverride { get; init; }
}

/// <summary>Safe, value-free identity facts retained by a CSV package.</summary>
public sealed class CsvSnapshotPackageManifest
{
    internal CsvSnapshotPackageManifest(
        string manifestDigest,
        string snapshotIdentity,
        string contentDigest,
        long contentLength,
        MigrationSourceIdentity source,
        string optionsDigest,
        string targetCSharpDbVersion,
        string catalogDigest)
    {
        ManifestDigest = manifestDigest;
        SnapshotIdentity = snapshotIdentity;
        ContentDigest = contentDigest;
        ContentLength = contentLength;
        Source = source;
        OptionsDigest = optionsDigest;
        TargetCSharpDbVersion = targetCSharpDbVersion;
        CatalogDigest = catalogDigest;
    }

    /// <summary>SHA-256 of the exact canonical manifest envelope bytes.</summary>
    public string ManifestDigest { get; }

    public string SnapshotIdentity { get; }

    public string ContentDigest { get; }

    public long ContentLength { get; }

    public MigrationSourceIdentity Source { get; }

    public string OptionsDigest { get; }

    public string TargetCSharpDbVersion { get; }

    public string CatalogDigest { get; }
}

/// <summary>
/// Owns the verified private snapshot and migration source reconstructed from
/// one retained package. Disposing a session never removes the package.
/// </summary>
public sealed class CsvSnapshotPackageSession : IAsyncDisposable
{
    private readonly object gate = new();
    private readonly CsvSourceSnapshot snapshot;
    private Task? disposeTask;

    internal CsvSnapshotPackageSession(
        CsvSnapshotPackageManifest manifest,
        CsvSourceSnapshot snapshot,
        CsvSchemaInferenceResult schema,
        MigrationCatalog catalog,
        CsvMigrationDataSource dataSource)
    {
        Manifest = manifest;
        this.snapshot = snapshot;
        Schema = schema;
        Catalog = catalog;
        DataSource = dataSource;
    }

    public CsvSnapshotPackageManifest Manifest { get; }

    public CsvSchemaInferenceResult Schema { get; }

    public MigrationCatalog Catalog { get; }

    public CsvMigrationDataSource DataSource { get; }

    public ValueTask DisposeAsync()
    {
        lock (gate)
        {
            disposeTask ??= DisposeCoreAsync();
            return new ValueTask(disposeTask);
        }
    }

    private async Task DisposeCoreAsync()
    {
        Exception? sourceFailure = null;
        try
        {
            await DataSource.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            sourceFailure = exception;
        }

        try
        {
            await snapshot.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception snapshotFailure) when (sourceFailure is not null)
        {
            throw new AggregateException(sourceFailure, snapshotFailure);
        }

        if (sourceFailure is not null)
            ExceptionDispatchInfo.Capture(sourceFailure).Throw();

        GC.SuppressFinalize(this);
    }
}
