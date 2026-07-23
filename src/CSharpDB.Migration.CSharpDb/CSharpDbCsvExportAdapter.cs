using System.Reflection;
using System.Runtime.CompilerServices;
using CSharpDB.Engine;
using CSharpDB.Migration.Files.Csv;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.CSharpDb;

/// <summary>
/// Complete retained CSharpDB source and resource policy for one resumable
/// table-to-CSV prepared output.
/// </summary>
public sealed record CSharpDbRetainedCsvExportRequest
{
    public required string SnapshotPath { get; init; }

    public required RetainedDatabaseSnapshotIdentity SnapshotIdentity
    { get; init; }

    public required string TableName { get; init; }

    public required string DestinationPath { get; init; }

    public CsvExportProfile Profile { get; init; } =
        CsvExportProfile.LosslessV1;

    public long MaxDataBytes { get; init; } = 1L << 40;

    public int MaximumDecodedBlobBytes { get; init; } =
        CsvExportContracts.MaximumSupportedDecodedBlobBytes;

    public long CheckpointRowInterval { get; init; } = 10_000;
}

/// <summary>
/// Binds resumable CSV export to one independently verified retained CSharpDB
/// snapshot. The adapter opens and owns one default-configured read-only
/// session across schema capture, replay, and continuation. Snapshots that
/// require custom storage, catalog, checksum, index, or serializer providers
/// are outside this adapter's contract.
/// </summary>
public sealed class CSharpDbCsvExportAdapter
{
    private const string Sha256Prefix =
        CsvExportHashManifest.Sha256Algorithm + ":";

    /// <summary>
    /// Writes or resumes one table's private prepared CSV output. Source
    /// identity, persisted schema, replay, and continuation are all derived
    /// from the retained snapshot named by <paramref name="request"/>.
    /// </summary>
    public async ValueTask<CsvStreamingExportResult> WriteResumableTableAsync(
        CSharpDbRetainedCsvExportRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.SnapshotPath);
        ArgumentNullException.ThrowIfNull(request.SnapshotIdentity);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.TableName);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.DestinationPath);
        if (!Enum.IsDefined(request.Profile))
            throw new ArgumentOutOfRangeException(nameof(request));
        if (request.MaxDataBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(request));
        if (request.MaximumDecodedBlobBytes is < 1 or >
            CsvExportContracts.MaximumSupportedDecodedBlobBytes)
        {
            throw new ArgumentOutOfRangeException(nameof(request));
        }
        if (request.CheckpointRowInterval <= 0)
            throw new ArgumentOutOfRangeException(nameof(request));

        cancellationToken.ThrowIfCancellationRequested();
        await using RetainedDatabaseSnapshotSession snapshot =
            await RetainedDatabaseSnapshot.OpenAsync(
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
        CsvExportSourceManifest source = CreateSource(snapshot.Identity);

        var resumable = new CsvResumableExportRequest
        {
            DestinationPath = request.DestinationPath,
            Profile = request.Profile,
            Source = source,
            SourceSnapshotIdentity = snapshot.Identity.SnapshotIdentity,
            Table = prepared.Schema,
            OpenRows = (afterRowIdExclusive, sourceCancellationToken) =>
                ReadRowsAsync(
                    snapshot,
                    prepared.Schema,
                    afterRowIdExclusive,
                    sourceCancellationToken),
            MaxDataBytes = request.MaxDataBytes,
            MaximumDecodedBlobBytes =
                request.MaximumDecodedBlobBytes,
            CheckpointRowInterval = request.CheckpointRowInterval,
        };
        return await new CsvStreamingExporter()
            .WriteResumableAsync(resumable, cancellationToken)
            .ConfigureAwait(false);
    }

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

    private static async IAsyncEnumerable<CsvExportRow> ReadRowsAsync(
        RetainedDatabaseSnapshotSession snapshot,
        TableSchema expectedSchema,
        long? afterRowIdExclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await using RetainedDatabaseSnapshotTableReader reader =
            snapshot.OpenTableReader(
                expectedSchema.TableName,
                afterRowIdExclusive);
        RequireMatchingSchema(reader, expectedSchema);

        while (await reader.MoveNextAsync(cancellationToken)
                   .ConfigureAwait(false))
        {
            yield return new CsvExportRow(
                reader.CurrentRowId,
                reader.Current);
        }
    }

    private static CsvExportSourceManifest CreateSource(
        RetainedDatabaseSnapshotIdentity identity)
    {
        string sha256 = identity.Sha256;
        if (!sha256.StartsWith(Sha256Prefix, StringComparison.Ordinal) ||
            sha256.Length != Sha256Prefix.Length + 64)
        {
            throw new InvalidDataException(
                "The retained CSharpDB snapshot digest is not canonical SHA-256 evidence.");
        }

        return new CsvExportSourceManifest
        {
            Kind = CsvExportContracts.SourceKind,
            Version = GetReaderVersion(),
            SnapshotByteLength = identity.ByteLength,
            SnapshotDigest = new CsvExportHashManifest
            {
                Algorithm = CsvExportHashManifest.Sha256Algorithm,
                Value = sha256[Sha256Prefix.Length..],
            },
        };
    }

    private static string GetReaderVersion()
    {
        Assembly assembly = typeof(RetainedDatabaseSnapshotSession).Assembly;
        string? informational = assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
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
                "The retained CSharpDB table schema changed during CSV export.");
        }

        for (int index = 0; index < reader.Columns.Count; index++)
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
                    "The retained CSharpDB table schema changed during CSV export.");
            }
        }
    }

    private static ColumnDefinition CopyColumn(ColumnDefinition column) => new()
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
