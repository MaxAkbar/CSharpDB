using System.Reflection;
using System.Runtime.CompilerServices;
using CSharpDB.Engine;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.CSharpDb;

/// <summary>
/// Complete retained CSharpDB source and resource policy for one
/// table-to-JSON or table-to-NDJSON export.
/// </summary>
public sealed record CSharpDbRetainedJsonExportRequest
{
    public required string SnapshotPath { get; init; }

    public required RetainedDatabaseSnapshotIdentity SnapshotIdentity
    { get; init; }

    public required string TableName { get; init; }

    public required string DestinationPath { get; init; }

    public JsonExportProfile Profile { get; init; } =
        JsonExportProfile.LosslessV1;

    public JsonExportFraming Framing { get; init; } =
        JsonExportFraming.RootArray;

    public long MaxDataBytes { get; init; } = 1L << 40;

    public int MaximumDecodedBlobBytes { get; init; } =
        JsonExportContracts.MaximumSupportedDecodedBlobBytes;

    public long CheckpointRowInterval { get; init; } = 10_000;
}

/// <summary>
/// Binds JSON export to one independently verified retained CSharpDB snapshot.
/// Resumable operations open and own one default-configured read-only session
/// across schema capture, replay, continuation, and publication. Snapshots
/// that require custom storage, catalog, checksum, index, or serializer
/// providers are outside this adapter's default-reader contract.
/// </summary>
public sealed class CSharpDbJsonExportAdapter
{
    private const string Sha256Prefix =
        JsonExportHashManifest.Sha256Algorithm + ":";

    /// <summary>
    /// Writes or resumes one table's private prepared JSON or NDJSON output.
    /// Source identity, persisted schema, replay, and continuation are all
    /// derived from the retained snapshot named by
    /// <paramref name="request"/>.
    /// </summary>
    public async ValueTask<JsonStreamingExportResult>
        WriteResumableTableAsync(
        CSharpDbRetainedJsonExportRequest request,
        CancellationToken cancellationToken = default)
    {
        ValidateRequest(request);
        JsonExportPublisher.ValidateSourcePath(
            request.SnapshotPath);
        ValidateSnapshotOutputPaths(
            request,
            manifestPath: null);

        cancellationToken.ThrowIfCancellationRequested();
        await using RetainedDatabaseSnapshotSession snapshot =
            await RetainedDatabaseSnapshot.OpenAsync(
                    request.SnapshotPath,
                    request.SnapshotIdentity,
                    databaseOptions: null,
                    options: null,
                    cancellationToken)
                .ConfigureAwait(false);
        JsonResumableExportRequest resumable =
            await CreateResumableRequestAsync(
                    request,
                    snapshot,
                    cancellationToken)
                .ConfigureAwait(false);
        return await new JsonStreamingExporter()
            .WriteResumableAsync(
                resumable,
                cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Writes or resumes, source-requalifies, and publishes the final JSON or
    /// NDJSON data before the explicit canonical manifest path. Exact
    /// data-only and exact-pair states are recoverable across a fresh process.
    /// </summary>
    public async ValueTask<JsonExportPublicationResult>
        WriteResumableAndPublishTableAsync(
        CSharpDbRetainedJsonExportRequest request,
        string manifestPath,
        CancellationToken cancellationToken = default)
    {
        ValidateRequest(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(
            manifestPath);
        JsonExportPublisher
            .ValidatePreparedPublicationPaths(
            request.DestinationPath,
            manifestPath);
        JsonExportPublisher.ValidateSourcePath(
            request.SnapshotPath);
        ValidateSnapshotOutputPaths(
            request,
            manifestPath);

        cancellationToken.ThrowIfCancellationRequested();
        RetainedDatabaseSnapshotSession? snapshot = null;
        bool committedPair = false;
        try
        {
            snapshot =
                await RetainedDatabaseSnapshot.OpenAsync(
                        request.SnapshotPath,
                        request.SnapshotIdentity,
                        databaseOptions: null,
                        options: null,
                        cancellationToken)
                    .ConfigureAwait(false);
            JsonResumableExportRequest resumable =
                await CreateResumableRequestAsync(
                        request,
                        snapshot,
                        cancellationToken)
                    .ConfigureAwait(false);
            JsonExportPublicationResult result =
                await new JsonStreamingExporter()
                    .WriteResumableAndPublishAsync(
                        resumable,
                        manifestPath,
                        cancellationToken)
                    .ConfigureAwait(false);
            committedPair = true;
            return result;
        }
        finally
        {
            if (snapshot is not null)
            {
                try
                {
                    await snapshot
                        .DisposeAsync()
                        .ConfigureAwait(false);
                }
                catch when (committedPair)
                {
                    // Publication is irreversible once the exact
                    // manifest-last pair exists. Snapshot cleanup cannot
                    // downgrade success.
                }
            }
        }
    }

    /// <summary>
    /// Requalifies one retained table through EOF and publishes its exact data
    /// file before the canonical manifest. Exact data-only and exact-pair
    /// states are recoverable by rerunning the same request.
    /// </summary>
    public async ValueTask<JsonExportPublicationResult>
        WriteAndPublishTableAsync(
            CSharpDbRetainedJsonExportRequest request,
            string manifestPath,
            CancellationToken cancellationToken = default)
    {
        ValidateRequest(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(manifestPath);
        JsonExportPublisher.ValidatePaths(
            request.DestinationPath,
            manifestPath);
        JsonExportPublisher.ValidateSourcePath(
            request.SnapshotPath);
        ValidateSnapshotOutputPaths(
            request,
            manifestPath);

        cancellationToken.ThrowIfCancellationRequested();
        RetainedDatabaseSnapshotSession? snapshot = null;
        bool committedPair = false;
        try
        {
            snapshot = await RetainedDatabaseSnapshot.OpenAsync(
                        request.SnapshotPath,
                        request.SnapshotIdentity,
                        databaseOptions: null,
                        options: null,
                        cancellationToken)
                    .ConfigureAwait(false);
            PreparedTable prepared = await PrepareTableAsync(
                    snapshot,
                    request.TableName,
                    cancellationToken)
                .ConfigureAwait(false);
            JsonStreamingExportRequest export = CreateExportRequest(
                request,
                snapshot,
                prepared.Schema);

            JsonExportPublicationResult result =
                await new JsonExportPublisher()
                    .PublishAsync(
                        new JsonExportPublicationRequest
                        {
                            DestinationPath =
                                request.DestinationPath,
                            ManifestPath = manifestPath,
                            Export = export,
                        },
                        cancellationToken)
                    .ConfigureAwait(false);
            committedPair = true;
            return result;
        }
        finally
        {
            if (snapshot is not null)
            {
                try
                {
                    await snapshot.DisposeAsync().ConfigureAwait(false);
                }
                catch when (committedPair)
                {
                    // The exact manifest-last pair is already committed.
                    // Cleanup cannot retroactively downgrade publication.
                }
            }
        }
    }

    private static void ValidateRequest(
        CSharpDbRetainedJsonExportRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(
            request.SnapshotPath);
        ArgumentNullException.ThrowIfNull(
            request.SnapshotIdentity);
        ArgumentException.ThrowIfNullOrWhiteSpace(
            request.TableName);
        ArgumentException.ThrowIfNullOrWhiteSpace(
            request.DestinationPath);
        if (!Enum.IsDefined(request.Profile) ||
            request.Profile != JsonExportProfile.LosslessV1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request));
        }
        if (!Enum.IsDefined(request.Framing))
        {
            throw new ArgumentOutOfRangeException(
                nameof(request));
        }
        if (request.MaxDataBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request));
        }
        if (request.MaximumDecodedBlobBytes is < 1 or >
            JsonExportContracts.MaximumSupportedDecodedBlobBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request));
        }
        if (request.CheckpointRowInterval <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request));
        }
    }

    private static void ValidateSnapshotOutputPaths(
        CSharpDbRetainedJsonExportRequest request,
        string? manifestPath)
    {
        string snapshot =
            NormalizePath(request.SnapshotPath);
        string destination =
            NormalizePath(request.DestinationPath);
        if (string.Equals(
                snapshot,
                destination,
                StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                "The retained CSharpDB snapshot and JSON export destination must use different files.",
                nameof(request));
        }

        if (manifestPath is not null)
        {
            string manifest =
                NormalizePath(manifestPath);
            if (string.Equals(
                    snapshot,
                    manifest,
                    StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException(
                    "The retained CSharpDB snapshot and JSON export manifest must use different files.",
                    nameof(manifestPath));
            }
        }
    }

    private static string NormalizePath(
        string path) =>
        Path.TrimEndingDirectorySeparator(
            Path.GetFullPath(path));

    private static async ValueTask<JsonResumableExportRequest>
        CreateResumableRequestAsync(
        CSharpDbRetainedJsonExportRequest request,
        RetainedDatabaseSnapshotSession snapshot,
        CancellationToken cancellationToken)
    {
        PreparedTable prepared =
            await PrepareTableAsync(
                    snapshot,
                    request.TableName,
                    cancellationToken)
                .ConfigureAwait(false);
        JsonExportSourceManifest source =
            CreateSource(snapshot.Identity);

        return new JsonResumableExportRequest
        {
            DestinationPath =
                request.DestinationPath,
            Profile = request.Profile,
            Framing = request.Framing,
            Source = source,
            SourceSnapshotIdentity =
                snapshot.Identity.SnapshotIdentity,
            Table = prepared.Schema,
            OpenRows =
                (
                    afterRowIdExclusive,
                    sourceCancellationToken
                ) => ReadRowsAsync(
                    snapshot,
                    prepared.Schema,
                    afterRowIdExclusive,
                    sourceCancellationToken),
            MaxDataBytes = request.MaxDataBytes,
            MaximumDecodedBlobBytes =
                request.MaximumDecodedBlobBytes,
            CheckpointRowInterval =
                request.CheckpointRowInterval,
        };
    }

    private static JsonStreamingExportRequest CreateExportRequest(
        CSharpDbRetainedJsonExportRequest request,
        RetainedDatabaseSnapshotSession snapshot,
        TableSchema schema) => new()
        {
            Profile = request.Profile,
            Framing = request.Framing,
            Source = CreateSource(snapshot.Identity),
            Table = schema,
            Rows = ReadRowsAsync(
                snapshot,
                schema,
                afterRowIdExclusive: null,
                cancellationToken:
                    CancellationToken.None),
            MaxDataBytes = request.MaxDataBytes,
            MaximumDecodedBlobBytes =
                request.MaximumDecodedBlobBytes,
        };

    private static async ValueTask<PreparedTable> PrepareTableAsync(
        RetainedDatabaseSnapshotSession snapshot,
        string tableName,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await using RetainedDatabaseSnapshotTableReader reader =
            snapshot.OpenTableReader(tableName);
        var schema = new TableSchema
        {
            TableName = reader.TableName,
            Columns = reader.Columns.Select(CopyColumn).ToArray(),
        };
        return new PreparedTable(schema);
    }

    private static async IAsyncEnumerable<JsonExportRow> ReadRowsAsync(
        RetainedDatabaseSnapshotSession snapshot,
        TableSchema expectedSchema,
        long? afterRowIdExclusive,
        [EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        await using RetainedDatabaseSnapshotTableReader reader =
            snapshot.OpenTableReader(
                expectedSchema.TableName,
                afterRowIdExclusive);
        RequireMatchingSchema(reader, expectedSchema);

        while (await reader.MoveNextAsync(cancellationToken)
                   .ConfigureAwait(false))
        {
            yield return new JsonExportRow(
                reader.CurrentRowId,
                reader.Current);
        }
    }

    private static JsonExportSourceManifest CreateSource(
        RetainedDatabaseSnapshotIdentity identity)
    {
        string sha256 = identity.Sha256;
        if (!sha256.StartsWith(
                Sha256Prefix,
                StringComparison.Ordinal) ||
            sha256.Length != Sha256Prefix.Length + 64)
        {
            throw new InvalidDataException(
                "The retained CSharpDB snapshot digest is not canonical SHA-256 evidence.");
        }

        return new JsonExportSourceManifest
        {
            Kind = JsonExportContracts.SourceKind,
            Version = GetReaderVersion(),
            SnapshotByteLength = identity.ByteLength,
            SnapshotDigest = new JsonExportHashManifest
            {
                Algorithm =
                    JsonExportHashManifest.Sha256Algorithm,
                Value = sha256[Sha256Prefix.Length..],
            },
        };
    }

    private static string GetReaderVersion()
    {
        Assembly assembly =
            typeof(RetainedDatabaseSnapshotSession).Assembly;
        string? informational = assembly
            .GetCustomAttribute<
                AssemblyInformationalVersionAttribute>()?
            .InformationalVersion;
        string? version = informational?.Split('+', 2)[0];
        if (string.IsNullOrWhiteSpace(version))
        {
            version = assembly.GetName().Version?.ToString();
        }
        if (string.IsNullOrWhiteSpace(version))
        {
            throw new InvalidOperationException(
                "The retained CSharpDB snapshot reader version is unavailable.");
        }
        return version;
    }

    private static void RequireMatchingSchema(
        RetainedDatabaseSnapshotTableReader reader,
        TableSchema expected)
    {
        if (!string.Equals(
                reader.TableName,
                expected.TableName,
                StringComparison.Ordinal) ||
            reader.Columns.Count != expected.Columns.Count)
        {
            throw new InvalidDataException(
                "The retained CSharpDB table schema changed during JSON export.");
        }

        for (int index = 0;
             index < reader.Columns.Count;
             index++)
        {
            ColumnDefinition actual = reader.Columns[index];
            ColumnDefinition captured = expected.Columns[index];
            if (!string.Equals(
                    actual.Name,
                    captured.Name,
                    StringComparison.Ordinal) ||
                actual.Type != captured.Type ||
                actual.Nullable != captured.Nullable)
            {
                throw new InvalidDataException(
                    "The retained CSharpDB table schema changed during JSON export.");
            }
        }
    }

    private static ColumnDefinition CopyColumn(
        ColumnDefinition column) => new()
        {
            Name = column.Name,
            Type = column.Type,
            Nullable = column.Nullable,
            IsPrimaryKey = column.IsPrimaryKey,
            IsIdentity = column.IsIdentity,
            IsRowVersion = column.IsRowVersion,
            Collation = column.Collation,
            DefaultSql = column.DefaultSql,
        };

    private sealed record PreparedTable(TableSchema Schema);
}
