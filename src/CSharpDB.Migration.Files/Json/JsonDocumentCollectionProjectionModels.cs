using System.Collections.ObjectModel;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Controls the explicit projection of complete top-level JSON values into
/// one document collection.
/// </summary>
public sealed record JsonDocumentCollectionProjectionOptions
{
    public const int MaximumCollectionNameCharacters =
        MigrationDocumentCollectionContract
            .MaximumLogicalCollectionNameLength;

    /// <summary>Gets the logical source collection name.</summary>
    public string CollectionName { get; init; } = "json_documents";
}

/// <summary>
/// Fully scanned, snapshot-bound metadata for one JSON document collection.
/// Counts cover every top-level value; no property name or value is retained.
/// </summary>
public sealed class JsonDocumentCollectionProjectionResult
{
    internal JsonDocumentCollectionProjectionResult(
        JsonSourceBinding binding,
        string collectionName,
        long totalRecords,
        long nullRecords,
        long booleanRecords,
        long stringRecords,
        long numberRecords,
        long objectRecords,
        long arrayRecords,
        long maxCanonicalDocumentBytes)
    {
        Binding = binding;
        Source = binding.Source;
        SnapshotIdentity = binding.SnapshotIdentity;
        ContentDigest = binding.ContentDigest;
        ContentLength = binding.ContentLength;
        CollectionName = collectionName;
        TotalRecords = totalRecords;
        NullRecords = nullRecords;
        BooleanRecords = booleanRecords;
        StringRecords = stringRecords;
        NumberRecords = numberRecords;
        ObjectRecords = objectRecords;
        ArrayRecords = arrayRecords;
        MaxCanonicalDocumentBytes = maxCanonicalDocumentBytes;
        Diagnostics = Array.AsReadOnly(
            Array.Empty<MigrationDiagnostic>());
    }

    public MigrationSourceIdentity Source { get; }

    public string SnapshotIdentity { get; }

    public string ContentDigest { get; }

    public long ContentLength { get; }

    public string CollectionName { get; }

    public long TotalRecords { get; }

    public long NullRecords { get; }

    public long BooleanRecords { get; }

    public long StringRecords { get; }

    public long NumberRecords { get; }

    public long ObjectRecords { get; }

    public long ArrayRecords { get; }

    public long MaxCanonicalDocumentBytes { get; }

    public ReadOnlyCollection<MigrationDiagnostic> Diagnostics { get; }

    public MigrationCatalog CreateCatalog(
        string targetCSharpDbVersion) =>
        JsonDocumentCollectionCatalogBuilder.Build(
            this,
            targetCSharpDbVersion);

    internal JsonSourceBinding Binding { get; }
}
