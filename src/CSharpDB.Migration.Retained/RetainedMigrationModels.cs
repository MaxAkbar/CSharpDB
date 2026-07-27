using System.Collections.ObjectModel;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Retained;

/// <summary>
/// Stable provider-neutral retained-row package contract.
/// </summary>
public static class RetainedMigrationPackageContract
{
    public const string Format = "csharpdb-retained-migration/v1";

    public const string ContentDigestAlgorithm =
        "csharpdb-retained-content/v1";

    public const string TableSectionDigestAlgorithm =
        "csharpdb-retained-table-section/v1";
}

/// <summary>
/// The exact source projection and ordering contract represented by one
/// retained table section.
/// </summary>
public sealed record RetainedMigrationTableDescriptor
{
    public required string SourceObjectId { get; init; }

    public IReadOnlyList<string> ColumnObjectIds { get; init; } = [];

    public IReadOnlyList<string> OrderingKeyColumnObjectIds { get; init; } = [];
}

/// <summary>
/// A single-use, deterministically ordered row stream for one table.
/// </summary>
public sealed record RetainedMigrationTableWrite
{
    public required RetainedMigrationTableDescriptor Descriptor { get; init; }

    public required IAsyncEnumerable<MigrationDataRow> Rows { get; init; }
}

/// <summary>
/// Catalog and immutable snapshot identity selected after retained content has
/// been summarized.
/// </summary>
public sealed record RetainedMigrationCatalogBinding
{
    public required MigrationCatalog Catalog { get; init; }

    public required string SnapshotIdentity { get; init; }
}

public sealed record RetainedMigrationContentTableSummary
{
    public required RetainedMigrationTableDescriptor Descriptor { get; init; }

    public long RowCount { get; init; }

    public required string SectionDigest { get; init; }
}

/// <summary>
/// A deterministic summary of row content and ordering descriptors. Providers
/// may use <see cref="ContentDigest"/> to finalize a source fingerprint without
/// creating a circular dependency on the completed package digest.
/// </summary>
public sealed record RetainedMigrationContentSummary
{
    public required string DigestAlgorithm { get; init; }

    public required string ContentDigest { get; init; }

    public IReadOnlyList<RetainedMigrationContentTableSummary> Tables { get; init; } =
        [];
}

public sealed record RetainedMigrationPackageTableManifest
{
    public required RetainedMigrationTableDescriptor Descriptor { get; init; }

    public long RowCount { get; init; }

    public long SectionLength { get; init; }

    public required string SectionDigest { get; init; }
}

/// <summary>
/// Embedded, non-secret package binding. The whole-package digest is
/// deliberately external to avoid a self-referential manifest.
/// </summary>
public sealed record RetainedMigrationPackageManifest
{
    public required string Format { get; init; }

    public required string CatalogDigest { get; init; }

    public required MigrationSourceKind SourceKind { get; init; }

    public required string SourceIdentity { get; init; }

    public required string SourceFingerprint { get; init; }

    public required string SnapshotIdentity { get; init; }

    public required string ContentDigest { get; init; }

    public IReadOnlyList<RetainedMigrationPackageTableManifest> Tables { get; init; } =
        [];
}

public sealed record RetainedMigrationPackageWriteResult
{
    public required RetainedMigrationPackageManifest Manifest { get; init; }

    public required string PackageDigest { get; init; }

    public required RetainedMigrationContentSummary ContentSummary { get; init; }

    public IReadOnlyDictionary<string, long> RowCounts { get; init; } =
        new ReadOnlyDictionary<string, long>(
            new Dictionary<string, long>(StringComparer.Ordinal));
}

public sealed record RetainedMigrationPackageWriteOptions
{
    public long MaxPackageBytes { get; init; } = 256L * 1024 * 1024 * 1024;

    public int MaxCatalogBytes { get; init; } = 256 * 1024 * 1024;

    public int MaxManifestBytes { get; init; } = 512 * 1024 * 1024;

    public int MaxTables { get; init; } = 100_000;

    public int MaxColumnsPerTable { get; init; } = 65_536;

    public long MaxRowsPerTable { get; init; } = 10_000_000_000;

    public int MaxValueBytes { get; init; } = 64 * 1024 * 1024;

    public int MaxRowBytes { get; init; } = 256 * 1024 * 1024;

    public int MaxStableKeyBytes { get; init; } = 4 * 1024 * 1024;

    public int CopyBufferBytes { get; init; } = 1024 * 1024;
}

/// <summary>
/// Writes a package from an already-finalized catalog.
/// </summary>
public sealed record RetainedMigrationPackageWriteRequest
{
    public required string OutputPath { get; init; }

    public required MigrationCatalog Catalog { get; init; }

    public required string SnapshotIdentity { get; init; }

    public IReadOnlyList<RetainedMigrationTableWrite> Tables { get; init; } = [];

    public RetainedMigrationPackageWriteOptions Options { get; init; } = new();
}

/// <summary>
/// Writes a package while allowing the provider to finalize its catalog and
/// snapshot identity after all retained row sections have stable digests.
/// </summary>
public sealed record RetainedMigrationPackageCaptureRequest
{
    public required string OutputPath { get; init; }

    public IReadOnlyList<RetainedMigrationTableWrite> Tables { get; init; } = [];

    public required Func<
        RetainedMigrationContentSummary,
        CancellationToken,
        ValueTask<RetainedMigrationCatalogBinding>>
        CatalogFactory
    { get; init; }

    public RetainedMigrationPackageWriteOptions Options { get; init; } = new();
}

public sealed record RetainedMigrationPackageOpenOptions
{
    public required string ExpectedPackageDigest { get; init; }

    public string? WorkspacePath { get; init; }

    public long MaxPackageBytes { get; init; } = 256L * 1024 * 1024 * 1024;

    public int MaxCatalogBytes { get; init; } = 256 * 1024 * 1024;

    public int MaxManifestBytes { get; init; } = 512 * 1024 * 1024;

    public int MaxTables { get; init; } = 100_000;

    public int MaxColumnsPerTable { get; init; } = 65_536;

    public long MaxRowsPerTable { get; init; } = 10_000_000_000;

    public int MaxValueBytes { get; init; } = 64 * 1024 * 1024;

    public int MaxRowBytes { get; init; } = 256 * 1024 * 1024;

    public int MaxStableKeyBytes { get; init; } = 4 * 1024 * 1024;

    public int CopyBufferBytes { get; init; } = 1024 * 1024;
}

public class RetainedMigrationPackageException : IOException
{
    public RetainedMigrationPackageException(string message)
        : base(message)
    {
    }

    public RetainedMigrationPackageException(
        string message,
        Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// Identifies a retained-package operation that exceeded an explicit
/// caller-configured resource bound.
/// </summary>
public sealed class RetainedMigrationPackageLimitException
    : RetainedMigrationPackageException
{
    public RetainedMigrationPackageLimitException(string message)
        : base(message)
    {
    }

    public RetainedMigrationPackageLimitException(
        string message,
        Exception innerException)
        : base(message, innerException)
    {
    }
}
