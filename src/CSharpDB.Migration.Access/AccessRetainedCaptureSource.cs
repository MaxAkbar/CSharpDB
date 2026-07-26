using System.Collections.ObjectModel;
using System.Data;
using System.Data.OleDb;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.Versioning;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Access;

internal interface IAccessRetainedCaptureSource
    : IAsyncDisposable
{
    ValueTask<AccessCatalogSnapshot>
        ReadCatalogAsync(
            CancellationToken cancellationToken);

    IAsyncEnumerable<MigrationDataRow>
        ReadRowsAsync(
            AccessTableBinding table,
            AccessRetainedCaptureOptions options,
            AccessRetainedCaptureBudget budget,
            CancellationToken cancellationToken);
}

internal sealed class AccessRetainedCaptureBudget
{
    private readonly long maximumRows;
    private long rows;

    internal AccessRetainedCaptureBudget(
        long maximumRows)
    {
        this.maximumRows = maximumRows > 0
            ? maximumRows
            : throw new ArgumentOutOfRangeException(
                nameof(maximumRows));
    }

    internal void AddRow()
    {
        if (Interlocked.Increment(ref rows) >
            maximumRows)
        {
            throw new AccessRetainedCaptureLimitException(
                "Microsoft Access retained capture exceeds its total row-count bound.");
        }
    }
}

[SupportedOSPlatform("windows")]
internal sealed class AccessLiveRetainedCaptureSource
    : IAccessRetainedCaptureSource
{
    private readonly AccessSourceSession session;
    private int catalogRead;
    private int disposed;

    private AccessLiveRetainedCaptureSource(
        AccessSourceSession session)
    {
        this.session = session;
    }

    internal static async ValueTask<
        AccessLiveRetainedCaptureSource>
        OpenAsync(
        string sourceFilePath,
        AccessSourceOptions options,
        CancellationToken cancellationToken)
    {
        AccessSourceSession session =
            await AccessSourceSession.OpenAsync(
                    sourceFilePath,
                    options,
                    cancellationToken)
                .ConfigureAwait(false);
        return new AccessLiveRetainedCaptureSource(
            session);
    }

    public ValueTask<AccessCatalogSnapshot>
        ReadCatalogAsync(
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (Interlocked.Exchange(
                ref catalogRead,
                1) != 0)
        {
            throw new InvalidOperationException(
                "The Access retained catalog is single-use.");
        }
        return AccessCatalogReader.ReadAsync(
            session,
            AccessInspectionLimits.Default,
            cancellationToken);
    }

    public async IAsyncEnumerable<MigrationDataRow>
        ReadRowsAsync(
        AccessTableBinding table,
        AccessRetainedCaptureOptions options,
        AccessRetainedCaptureBudget budget,
        [EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(budget);
        ThrowIfDisposed();
        if (!table.IsDataAvailable)
        {
            throw new ArgumentException(
                "Only data-available Access tables can be retained.",
                nameof(table));
        }

        await using OleDbCommand command =
            session.Connection.CreateCommand();
        command.CommandType = CommandType.Text;
        command.CommandTimeout =
            session.CommandTimeoutSeconds;
        command.CommandText =
            AccessRetainedReadSql.Build(table);
        await using OleDbDataReader reader =
            await command.ExecuteReaderAsync(
                    CommandBehavior.SequentialAccess |
                    CommandBehavior.SingleResult,
                    cancellationToken)
                .ConfigureAwait(false) as OleDbDataReader ??
            throw new AccessMigrationException(
                AccessMigrationErrorCode.CaptureFailed,
                "ACE returned no Access row reader.");

        long tableRows = 0;
        while (await reader.ReadAsync(
                       cancellationToken)
                   .ConfigureAwait(false))
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            if (tableRows >=
                options.MaxRowsPerTable)
            {
                throw new AccessRetainedCaptureLimitException(
                    "A Microsoft Access table exceeds its configured row-count bound.");
            }
            budget.AddRow();

            var values =
                new MigrationSourceValue[
                    table.Columns.Count];
            long rowBytes = 0;
            for (int ordinal = 0;
                 ordinal < table.Columns.Count;
                 ordinal++)
            {
                AccessProjectedScalar projected =
                    AccessScalarCodec.Read(
                        reader,
                        ordinal,
                        table.Columns[ordinal],
                        options.MaxValueBytes);
                rowBytes = checked(
                    rowBytes +
                    1L +
                    projected.PayloadBytes);
                if (rowBytes >
                    options.MaxRowBytes)
                {
                    throw new AccessRetainedCaptureLimitException(
                        "A Microsoft Access row exceeds its configured byte bound.");
                }
                values[ordinal] =
                    projected.Value;
            }

            string stableKey =
                CreateStableKey(
                    table,
                    values);
            tableRows++;
            yield return new MigrationDataRow
            {
                StableKey = stableKey,
                Values =
                    Array.AsReadOnly(values),
            };
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(
                ref disposed,
                1) != 0)
        {
            return;
        }
        await session.DisposeAsync()
            .ConfigureAwait(false);
    }

    private static string CreateStableKey(
        AccessTableBinding table,
        IReadOnlyList<MigrationSourceValue> values)
    {
        IReadOnlyDictionary<string, int> ordinals =
            table.Columns.Select(
                    (column, ordinal) =>
                        (column.CatalogObject.ObjectId,
                         ordinal))
                .ToDictionary(
                    static item =>
                        item.ObjectId,
                    static item =>
                        item.ordinal,
                    StringComparer.Ordinal);
        string[] keyComponents =
            table.PrimaryKeyColumns
                .Select(column =>
                {
                    MigrationSourceValue value =
                        values[
                            ordinals[
                                column.CatalogObject
                                    .ObjectId]];
                    if (value.Kind ==
                        MigrationSourceValueKind.Null)
                    {
                        throw new AccessMigrationException(
                            AccessMigrationErrorCode
                                .CaptureFailed,
                            "ACE returned NULL for an Access primary-key column.");
                    }
                    return value.Kind ==
                        MigrationSourceValueKind.Binary
                        ? Convert.ToHexString(
                                value.BinaryValue.Span)
                            .ToLowerInvariant()
                        : value.CanonicalText ??
                          throw new AccessMigrationException(
                              AccessMigrationErrorCode
                                  .CaptureFailed,
                              "An Access primary-key value has no canonical representation.");
                })
                .ToArray();
        string digest = AccessStableDigest.Text(
            "csharpdb-access-stable-key/v1",
            keyComponents);
        return "access-key:" +
            digest["sha256:".Length..];
    }

    private void ThrowIfDisposed() =>
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref disposed) != 0,
            this);
}

internal static class AccessRetainedReadSql
{
    internal static string Build(
        AccessTableBinding table)
    {
        ArgumentNullException.ThrowIfNull(table);
        if (!table.IsDataAvailable)
        {
            throw new ArgumentException(
                "An Access retained read requires supported columns and a primary key.",
                nameof(table));
        }
        string columns = string.Join(
            ", ",
            table.Columns.Select(column =>
                QuoteIdentifier(
                    column.Metadata.Name)));
        string order = string.Join(
            ", ",
            table.PrimaryKeyColumns.Select(
                column =>
                    QuoteIdentifier(
                        column.Metadata.Name) +
                    " ASC"));
        return string.Concat(
            "SELECT ",
            columns,
            " FROM ",
            QuoteIdentifier(
                table.Metadata.Name),
            " ORDER BY ",
            order,
            ";");
    }

    internal static string QuoteIdentifier(
        string identifier)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            identifier);
        if (identifier.Length > 64 ||
            identifier.IndexOf('\0') >= 0)
        {
            throw new AccessMigrationException(
                AccessMigrationErrorCode.InvalidSource,
                "An Access identifier exceeds the verified source-name bound.");
        }
        return "[" +
            identifier.Replace(
                "]",
                "]]",
                StringComparison.Ordinal) +
            "]";
    }
}
