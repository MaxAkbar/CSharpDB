using System.Reflection;
using System.Runtime.CompilerServices;
using CSharpDB.Engine;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.CSharpDb;

/// <summary>
/// Complete retained CSharpDB source and resource policy for one restart-only
/// table-to-JSON or table-to-NDJSON publication.
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
}

/// <summary>
/// Binds restart-only JSON publication to one independently verified retained
/// CSharpDB snapshot. A rerun reopens and replays the snapshot from row zero;
/// this adapter does not retain a mid-stream resume cursor. Snapshots that
/// require custom storage, catalog, checksum, index, or serializer providers
/// are outside this adapter's default-reader contract.
/// </summary>
public sealed class CSharpDbJsonExportAdapter
{
    private const string Sha256Prefix =
        JsonExportHashManifest.Sha256Algorithm + ":";

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
        [EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        await using RetainedDatabaseSnapshotTableReader reader =
            snapshot.OpenTableReader(expectedSchema.TableName);
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
