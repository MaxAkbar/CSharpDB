using System.Runtime.ExceptionServices;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Safe identity facts retained by a typed JSON snapshot package.
/// </summary>
public sealed class JsonTypedSnapshotPackageManifest
{
    internal JsonTypedSnapshotPackageManifest(
        string manifestDigest,
        string intentManifestDigest,
        string snapshotIdentity,
        string contentDigest,
        long contentLength,
        MigrationSourceIdentity source,
        string optionsDigest,
        string targetCSharpDbVersion,
        string catalogDigest)
    {
        ManifestDigest = manifestDigest;
        IntentManifestDigest = intentManifestDigest;
        SnapshotIdentity = snapshotIdentity;
        ContentDigest = contentDigest;
        ContentLength = contentLength;
        Source = source;
        OptionsDigest = optionsDigest;
        TargetCSharpDbVersion = targetCSharpDbVersion;
        CatalogDigest = catalogDigest;
    }

    /// <summary>
    /// SHA-256 of the exact canonical package-manifest envelope bytes.
    /// This is the independently retainable package pin.
    /// </summary>
    public string ManifestDigest { get; }

    /// <summary>
    /// SHA-256 of the exact embedded canonical typed-intent bytes.
    /// </summary>
    public string IntentManifestDigest { get; }

    public string SnapshotIdentity { get; }

    public string ContentDigest { get; }

    public long ContentLength { get; }

    public MigrationSourceIdentity Source { get; }

    public string OptionsDigest { get; }

    public string TargetCSharpDbVersion { get; }

    public string CatalogDigest { get; }
}

/// <summary>
/// Owns the verified private snapshot and typed migration source reconstructed
/// from one v2 package. It can never expose an untyped v1 schema result.
/// </summary>
public sealed class JsonTypedSnapshotPackageSession :
    IAsyncDisposable
{
    private readonly object gate = new();
    private readonly JsonSourceSnapshot snapshot;
    private Task? disposeTask;

    internal JsonTypedSnapshotPackageSession(
        JsonTypedSnapshotPackageManifest manifest,
        JsonTypedIntentManifest intentManifest,
        JsonSourceSnapshot snapshot,
        JsonTypedTableSchemaInferenceResult schema,
        MigrationCatalog catalog,
        JsonMigrationDataSource dataSource)
    {
        Manifest = manifest;
        IntentManifest = intentManifest;
        this.snapshot = snapshot;
        Schema = schema;
        Catalog = catalog;
        DataSource = dataSource;
    }

    public JsonTypedSnapshotPackageManifest Manifest { get; }

    public JsonTypedIntentManifest IntentManifest { get; }

    public JsonTypedTableSchemaInferenceResult Schema { get; }

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
        catch (Exception snapshotFailure)
            when (sourceFailure is not null)
        {
            throw new AggregateException(
                sourceFailure,
                snapshotFailure);
        }

        if (sourceFailure is not null)
        {
            ExceptionDispatchInfo
                .Capture(sourceFailure)
                .Throw();
        }

        GC.SuppressFinalize(this);
    }
}
