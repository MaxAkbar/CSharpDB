using System.Collections.ObjectModel;
using System.Data.OleDb;
using System.Runtime.Versioning;
using CSharpDB.Migration;
using CSharpDB.Migration.Retained;

namespace CSharpDB.Migration.Access;

/// <summary>
/// Captures a bounded local-table subset from an unencrypted .mdb or .accdb
/// into the provider-neutral retained migration package.
/// </summary>
public static class AccessRetainedCapture
{
    public static async ValueTask<
        RetainedMigrationPackageWriteResult>
        CaptureAsync(
        string sourceFilePath,
        string outputPath,
        AccessRetainedCaptureOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            sourceFilePath);
        ArgumentException.ThrowIfNullOrWhiteSpace(
            outputPath);
        AccessRetainedCaptureOptions effective =
            options ??
            new AccessRetainedCaptureOptions();
        effective.Validate();
        cancellationToken.ThrowIfCancellationRequested();
        if (!OperatingSystem.IsWindows())
        {
            throw new AccessMigrationException(
                AccessMigrationErrorCode.UnsupportedPlatform,
                "Microsoft Access retained capture requires Windows.");
        }

        try
        {
            AccessLiveRetainedCaptureSource source =
                await AccessLiveRetainedCaptureSource
                    .OpenAsync(
                        sourceFilePath,
                        effective.Source,
                        cancellationToken)
                    .ConfigureAwait(false);
            return await CaptureAsync(
                    source,
                    outputPath,
                    effective,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (AccessMigrationException)
        {
            throw;
        }
        catch (RetainedMigrationPackageLimitException)
        {
            throw new AccessRetainedCaptureLimitException(
                "Microsoft Access retained capture exceeded a package safety bound.");
        }
        catch (RetainedMigrationPackageException)
        {
            throw;
        }
        catch (Exception exception) when (
            exception is OleDbException or
                InvalidOperationException or
                InvalidDataException or
                IOException or
                UnauthorizedAccessException)
        {
            throw new AccessMigrationException(
                AccessMigrationErrorCode.CaptureFailed,
                "Microsoft Access retained capture could not be completed.");
        }
    }

    [SupportedOSPlatform("windows")]
    internal static async ValueTask<
        RetainedMigrationPackageWriteResult>
        CaptureAsync(
        IAccessRetainedCaptureSource source,
        string outputPath,
        AccessRetainedCaptureOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentException.ThrowIfNullOrWhiteSpace(
            outputPath);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        await using IAccessRetainedCaptureSource
            ownedSource = source;
        AccessCatalogSnapshot snapshot =
            await ownedSource.ReadCatalogAsync(
                    cancellationToken)
                .ConfigureAwait(false);
        AccessCatalogBinding binding =
            AccessCatalogBuilder.Build(
                snapshot,
                new MigrationInspectionRequest
                {
                    TargetCSharpDbVersion =
                        CSharpDbCapabilityCatalogLoader
                            .CurrentTargetVersion,
                    IncludeProfile = false,
                });
        if (binding.Tables.Count >
            options.MaxTables)
        {
            throw new AccessRetainedCaptureLimitException(
                "Microsoft Access retained capture exceeds its table-count bound.");
        }
        if (binding.Tables.Any(table =>
                table.Columns.Count >
                options.MaxColumnsPerTable))
        {
            throw new AccessRetainedCaptureLimitException(
                "A Microsoft Access table exceeds the retained column-count bound.");
        }

        var budget =
            new AccessRetainedCaptureBudget(
                options.MaxRowsTotal);
        RetainedMigrationTableWrite[] writes =
            binding.AvailableTables
                .Select(table =>
                    new RetainedMigrationTableWrite
                    {
                        Descriptor =
                            CreateDescriptor(table),
                        Rows =
                            ownedSource.ReadRowsAsync(
                                table,
                                options,
                                budget,
                                cancellationToken),
                    })
                .ToArray();

        return await RetainedMigrationPackageWriter
            .WriteAsync(
                new RetainedMigrationPackageCaptureRequest
                {
                    OutputPath = outputPath,
                    Tables =
                        Array.AsReadOnly(writes),
                    CatalogFactory =
                        (summary,
                         factoryCancellationToken) =>
                        {
                            factoryCancellationToken
                                .ThrowIfCancellationRequested();
                            return ValueTask.FromResult(
                                AccessRetainedCatalog
                                    .Create(
                                        binding,
                                        summary));
                        },
                    Options =
                        options.ToRetainedOptions(),
                },
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static RetainedMigrationTableDescriptor
        CreateDescriptor(
        AccessTableBinding table) =>
        new()
        {
            SourceObjectId =
                table.CatalogObject.ObjectId,
            ColumnObjectIds =
                Array.AsReadOnly(
                    table.Columns
                        .Select(static column =>
                            column.CatalogObject
                                .ObjectId)
                        .ToArray()),
            OrderingKeyColumnObjectIds =
                Array.AsReadOnly(
                    table.PrimaryKeyColumns
                        .Select(static column =>
                            column.CatalogObject
                                .ObjectId)
                        .ToArray()),
        };
}

internal static class AccessRetainedCatalog
{
    internal static RetainedMigrationCatalogBinding
        Create(
        AccessCatalogBinding binding,
        RetainedMigrationContentSummary summary)
    {
        ArgumentNullException.ThrowIfNull(binding);
        ArgumentNullException.ThrowIfNull(summary);
        IReadOnlyDictionary<string,
                AccessTableBinding>
            tables =
                binding.Tables.ToDictionary(
                    static table =>
                        table.CatalogObject.ObjectId,
                    StringComparer.Ordinal);
        IReadOnlyDictionary<string,
                AccessColumnBinding>
            columns =
                binding.Tables
                    .SelectMany(static table =>
                        table.Columns)
                    .ToDictionary(
                        static column =>
                            column.CatalogObject
                                .ObjectId,
                        StringComparer.Ordinal);
        IReadOnlyDictionary<string,
                RetainedMigrationContentTableSummary>
            summaries =
                summary.Tables.ToDictionary(
                    static table =>
                        table.Descriptor
                            .SourceObjectId,
                    StringComparer.Ordinal);
        string snapshotDigest =
            AccessStableDigest.Text(
                "csharpdb-access-retained-snapshot/v1",
                binding.Catalog.Source.Fingerprint,
                summary.ContentDigest);
        string snapshotIdentity =
            AccessRetainedDataContract
                .SnapshotIdentityPrefix +
            snapshotDigest[
                "sha256:".Length..];

        MigrationCatalogObject[] objects =
            binding.Catalog.Objects
                .Select(item =>
                    TransformObject(
                        item,
                        binding.Database.ObjectId,
                        tables,
                        columns,
                        summaries,
                        summary.ContentDigest,
                        snapshotIdentity))
                .OrderBy(
                    static item =>
                        item.ObjectId,
                    StringComparer.Ordinal)
                .ToArray();
        MigrationCatalog catalog =
            binding.Catalog with
            {
                Source =
                    binding.Catalog.Source with
                    {
                        Consistency =
                            new MigrationConsistencyStrategy
                            {
                                Kind =
                                    MigrationConsistencyKind
                                        .Snapshot,
                                Description =
                                    "Catalog facts and rows were captured while a write/delete-denying lease pinned one Access file; retained rows are immutable and provider-neutral.",
                            },
                    },
                Objects =
                    Array.AsReadOnly(objects),
            };
        MigrationContractValidator.ValidateCatalog(
            catalog);
        return new RetainedMigrationCatalogBinding
        {
            Catalog = catalog,
            SnapshotIdentity =
                snapshotIdentity,
        };
    }

    private static MigrationCatalogObject
        TransformObject(
        MigrationCatalogObject item,
        string databaseObjectId,
        IReadOnlyDictionary<string,
            AccessTableBinding> tables,
        IReadOnlyDictionary<string,
            AccessColumnBinding> columns,
        IReadOnlyDictionary<string,
            RetainedMigrationContentTableSummary>
            summaries,
        string contentDigest,
        string snapshotIdentity)
    {
        var additions =
            new List<MigrationCatalogFacet>();
        if (string.Equals(
                item.ObjectId,
                databaseObjectId,
                StringComparison.Ordinal))
        {
            additions.Add(
                Facet(
                    "accessRetainedDataContract",
                    AccessRetainedDataContract
                        .DataContract));
            additions.Add(
                Facet(
                    "accessRetainedContentDigest",
                    contentDigest));
            additions.Add(
                Facet(
                    "accessRetainedSnapshotIdentity",
                    snapshotIdentity));
        }
        if (tables.TryGetValue(
                item.ObjectId,
                out AccessTableBinding? table) &&
            table.IsDataAvailable)
        {
            RetainedMigrationContentTableSummary
                tableSummary =
                summaries[item.ObjectId];
            additions.Add(
                Facet(
                    "accessRowOrderContract",
                    AccessRetainedDataContract
                        .RowOrderContract));
            additions.Add(
                Facet(
                    "accessRetainedRowCount",
                    tableSummary.RowCount.ToString(
                        System.Globalization
                            .CultureInfo
                            .InvariantCulture)));
            additions.Add(
                Facet(
                    "accessRetainedSectionDigest",
                    tableSummary.SectionDigest));
        }
        if (columns.TryGetValue(
                item.ObjectId,
                out AccessColumnBinding? column) &&
            column.Codec is AccessScalarCodecKind codec)
        {
            additions.Add(
                Facet(
                    "accessScalarCodecContract",
                    AccessRetainedDataContract
                        .ScalarCodecContract));
            additions.Add(
                Facet(
                    "accessScalarCodec",
                    codec.ToString()));
        }
        if (additions.Count == 0)
            return item;
        return item with
        {
            Facets =
                new ReadOnlyCollection<
                    MigrationCatalogFacet>(
                    item.Facets.Concat(
                            additions)
                        .OrderBy(
                            static facet =>
                                facet.Name,
                            StringComparer.Ordinal)
                        .ToArray()),
        };
    }

    private static MigrationCatalogFacet Facet(
        string name,
        string? value) =>
        new()
        {
            Name = name,
            Value = value,
        };
}
