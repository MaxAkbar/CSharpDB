using System.Runtime.ExceptionServices;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Json;

/// <summary>Stable rules raised while publishing or reopening a retained JSON package.</summary>
public static class JsonSnapshotPackageRules
{
    public const string InvalidFormat = "MIG-JSON-PACKAGE-FORMAT-001";
    public const string IntegrityMismatch = "MIG-JSON-PACKAGE-INTEGRITY-001";
    public const string PolicyMismatch = "MIG-JSON-PACKAGE-POLICY-001";
    public const string SizeLimitExceeded = "MIG-JSON-PACKAGE-LIMIT-001";
    public const string UnsafePath = "MIG-JSON-PACKAGE-PATH-001";
}

public sealed class JsonSnapshotPackageException : IOException
{
    internal JsonSnapshotPackageException(string ruleId, string message)
        : base(message)
    {
        RuleId = ruleId;
    }

    internal JsonSnapshotPackageException(
        string ruleId,
        string message,
        Exception innerException)
        : base(message, innerException)
    {
        RuleId = ruleId;
    }

    public string RuleId { get; }
}

/// <summary>Bounds and environment policy for reopening a retained JSON package.</summary>
public sealed record JsonSnapshotPackageOpenOptions
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
}

/// <summary>Safe identity facts retained by a JSON package.</summary>
public sealed class JsonSnapshotPackageManifest
{
    internal JsonSnapshotPackageManifest(
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
public sealed class JsonSnapshotPackageSession : IAsyncDisposable
{
    private readonly object gate = new();
    private readonly JsonSourceSnapshot snapshot;
    private Task? disposeTask;

    internal JsonSnapshotPackageSession(
        JsonSnapshotPackageManifest manifest,
        JsonSourceSnapshot snapshot,
        JsonTableSchemaInferenceResult schema,
        MigrationCatalog catalog,
        JsonMigrationDataSource dataSource)
    {
        Manifest = manifest;
        this.snapshot = snapshot;
        Schema = schema;
        Catalog = catalog;
        DataSource = dataSource;
    }

    public JsonSnapshotPackageManifest Manifest { get; }

    public JsonTableSchemaInferenceResult Schema { get; }

    public MigrationCatalog Catalog { get; }

    public JsonMigrationDataSource DataSource { get; }

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
