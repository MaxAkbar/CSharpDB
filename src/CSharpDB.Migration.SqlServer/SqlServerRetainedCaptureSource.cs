using System.Data;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using CSharpDB.Migration;
using Microsoft.Data.SqlClient;

namespace CSharpDB.Migration.SqlServer;

internal interface ISqlServerRetainedCaptureSource : IAsyncDisposable
{
    ValueTask<MigrationCatalog> ReadCatalogAsync(
        CancellationToken cancellationToken);

    IAsyncEnumerable<MigrationDataRow> ReadRowsAsync(
        SqlServerRetainedTableBinding table,
        SqlServerRetainedCaptureOptions options,
        SqlServerRetainedCaptureBudget budget,
        CancellationToken cancellationToken);
}

internal sealed class SqlServerRetainedCaptureBudget
{
    private readonly long maximumRows;
    private long rows;

    internal SqlServerRetainedCaptureBudget(long maximumRowsTotal)
    {
        if (maximumRowsTotal <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maximumRowsTotal));
        }
        maximumRows = maximumRowsTotal;
    }

    internal void AddRow()
    {
        long current = Interlocked.Increment(ref rows);
        if (current > maximumRows)
        {
            throw new SqlServerRetainedCaptureLimitException(
                "The SQL Server retained capture exceeds its total row-count bound.");
        }
    }
}

internal sealed class SqlServerLiveRetainedCaptureSource :
    ISqlServerRetainedCaptureSource
{
    internal const string SnapshotIsolationStateQuery =
        """
        SELECT d.snapshot_isolation_state_desc
        FROM sys.databases AS d
        WHERE d.database_id = DB_ID();
        """;

    private readonly SqlServerCatalogReader catalogReader;
    private readonly SqlServerInspectionLimits inspectionLimits;
    private readonly SqlConnection connection;
    private readonly SqlTransaction transaction;
    private int catalogRead;
    private int disposed;

    private SqlServerLiveRetainedCaptureSource(
        SqlServerCatalogReader catalogReader,
        SqlServerInspectionLimits inspectionLimits,
        SqlConnection connection,
        SqlTransaction transaction)
    {
        this.catalogReader = catalogReader;
        this.inspectionLimits = inspectionLimits;
        this.connection = connection;
        this.transaction = transaction;
    }

    internal static async ValueTask<
        SqlServerLiveRetainedCaptureSource> OpenAsync(
        string connectionString,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        cancellationToken.ThrowIfCancellationRequested();
        var reader = new SqlServerCatalogReader(connectionString);
        SqlConnection connection = reader.CreateConnection();
        try
        {
            await connection.OpenAsync(cancellationToken)
                .ConfigureAwait(false);
            await using (var stateCommand = new SqlCommand(
                             SnapshotIsolationStateQuery,
                             connection)
            {
                CommandType = CommandType.Text,
                CommandTimeout = 30,
            })
            {
                object? state = await stateCommand.ExecuteScalarAsync(
                        cancellationToken)
                    .ConfigureAwait(false);
                if (!string.Equals(
                        state as string,
                        "ON",
                        StringComparison.Ordinal))
                {
                    throw new SqlServerMigrationException(
                        "SQL Server retained capture requires snapshot_isolation_state ON.");
                }
            }

            SqlTransaction transaction = (SqlTransaction)
                await connection.BeginTransactionAsync(
                        IsolationLevel.Snapshot,
                        cancellationToken)
                    .ConfigureAwait(false);
            return new SqlServerLiveRetainedCaptureSource(
                reader,
                SqlServerInspectionLimits.Default,
                connection,
                transaction);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    public async ValueTask<MigrationCatalog> ReadCatalogAsync(
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (Interlocked.Exchange(ref catalogRead, 1) != 0)
        {
            throw new InvalidOperationException(
                "The retained SQL Server catalog is single-use.");
        }

        SqlServerCatalogSnapshot snapshot =
            await catalogReader.ReadAsync(
                    connection,
                    transaction,
                    inspectionLimits,
                    cancellationToken)
                .ConfigureAwait(false);
        if (!string.Equals(
                snapshot.Database.SnapshotIsolationState,
                "ON",
                StringComparison.Ordinal))
        {
            throw new SqlServerMigrationException(
                "SQL Server snapshot isolation changed before retained catalog capture.");
        }
        return SqlServerCatalogBuilder.Build(
            snapshot,
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion =
                    CSharpDbCapabilityCatalogLoader
                        .CurrentTargetVersion,
                IncludeProfile = false,
            },
            inspectionLimits,
            cancellationToken);
    }

    public async IAsyncEnumerable<MigrationDataRow>
        ReadRowsAsync(
        SqlServerRetainedTableBinding table,
        SqlServerRetainedCaptureOptions options,
        SqlServerRetainedCaptureBudget budget,
        [EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(budget);
        ThrowIfDisposed();
        if (!table.IsAvailable || table.Order is null)
        {
            throw new ArgumentException(
                "Only data-available SQL Server tables can be retained.",
                nameof(table));
        }

        SqlServerRetainedReadCommand definition =
            SqlServerRetainedReadSql.CreateCommand(
                table,
                options);
        await using var command = new SqlCommand(
            definition.CommandText,
            connection,
            transaction)
        {
            CommandType = CommandType.Text,
            CommandTimeout =
                definition.CommandTimeoutSeconds,
        };
        command.Parameters.Add(
            new SqlParameter(
                "@qualifiedName",
                SqlDbType.NVarChar,
                517)
            {
                Value = definition.QualifiedName,
            });
        command.Parameters.Add(
            new SqlParameter(
                "@expectedObjectId",
                SqlDbType.Int)
            {
                Value = definition.ExpectedObjectId,
            });

        await using SqlDataReader reader =
            await command.ExecuteReaderAsync(
                    CommandBehavior.SequentialAccess |
                    CommandBehavior.SingleResult,
                    cancellationToken)
                .ConfigureAwait(false);
        long tableRows = 0;
        while (await reader.ReadAsync(cancellationToken)
                   .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (tableRows >= options.MaxRowsPerTable)
            {
                throw new SqlServerRetainedCaptureLimitException(
                    "A SQL Server retained table exceeds its row-count bound.");
            }
            budget.AddRow();

            var values =
                new MigrationSourceValue[table.Columns.Count];
            long rowBytes = 0;
            for (int index = 0;
                 index < table.Columns.Count;
                 index++)
            {
                int lengthOrdinal = checked(index * 2);
                int valueOrdinal =
                    checked(lengthOrdinal + 1);
                SqlServerProjectedScalar projected =
                    SqlServerScalarCodec.Read(
                        reader,
                        lengthOrdinal,
                        valueOrdinal,
                        table.Columns[index],
                        options.MaxValueBytes);
                rowBytes = checked(
                    rowBytes + 1L +
                    projected.PayloadBytes);
                if (rowBytes > options.MaxRowBytes)
                {
                    throw new SqlServerRetainedCaptureLimitException(
                        "A SQL Server retained row exceeds its byte bound.");
                }
                values[index] = projected.Value;
            }

            tableRows++;
            yield return new MigrationDataRow
            {
                StableKey = null,
                Values = Array.AsReadOnly(values),
            };
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
            return;
        await SqlServerRetainedCleanup.DisposeAsync(
                () => new ValueTask(
                    transaction.RollbackAsync(
                        CancellationToken.None)),
                transaction.DisposeAsync,
                connection.DisposeAsync)
            .ConfigureAwait(false);
    }

    private void ThrowIfDisposed() =>
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref disposed) != 0,
        this);
}

internal static class SqlServerRetainedCleanup
{
    internal static async ValueTask DisposeAsync(
        Func<ValueTask> rollbackAsync,
        Func<ValueTask> disposeTransactionAsync,
        Func<ValueTask> disposeConnectionAsync)
    {
        ArgumentNullException.ThrowIfNull(rollbackAsync);
        ArgumentNullException.ThrowIfNull(
            disposeTransactionAsync);
        ArgumentNullException.ThrowIfNull(
            disposeConnectionAsync);

        await IgnoreProviderFailureAsync(
                rollbackAsync)
            .ConfigureAwait(false);
        await IgnoreProviderFailureAsync(
                disposeTransactionAsync)
            .ConfigureAwait(false);
        await IgnoreProviderFailureAsync(
                disposeConnectionAsync)
            .ConfigureAwait(false);
    }

    private static async ValueTask IgnoreProviderFailureAsync(
        Func<ValueTask> action)
    {
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (Exception exception) when (
            exception is SqlException or
                InvalidOperationException)
        {
            // A package may already have been published, and cleanup can also
            // run while another capture failure is unwinding. Provider cleanup
            // text must not turn either outcome into an ambiguous new failure.
        }
    }
}

internal readonly record struct SqlServerRetainedReadCommand(
    string CommandText,
    int CommandTimeoutSeconds,
    string QualifiedName,
    int ExpectedObjectId);

internal static class SqlServerRetainedReadSql
{
    private static readonly UTF8Encoding StrictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);

    internal static string Build(
        SqlServerRetainedTableBinding table)
    {
        ArgumentNullException.ThrowIfNull(table);
        if (!table.IsAvailable ||
            table.Order is null ||
            table.Columns.Count == 0 ||
            table.Order.Columns.Count == 0)
        {
            throw new ArgumentException(
                "A retained SQL Server read requires columns and a stable order.",
                nameof(table));
        }

        var sql = new StringBuilder(
            """
            IF ISNULL(OBJECT_ID(@qualifiedName, N'U'), -1) <> @expectedObjectId
                THROW 51000, N'The retained SQL Server table identity changed.', 1;
            SELECT
            """);
        sql.AppendLine();
        for (int index = 0;
             index < table.Columns.Count;
             index++)
        {
            if (index > 0)
                sql.Append(", ");
            string column = QuoteIdentifier(
                table.Columns[index].CatalogObject.SourceName);
            sql.Append("CONVERT(bigint, DATALENGTH(");
            sql.Append(column);
            sql.Append(")), ");
            sql.Append(column);
        }
        sql.Append(" FROM ");
        sql.Append(QualifiedName(table));
        sql.Append(" ORDER BY ");
        for (int index = 0;
             index < table.Order.Columns.Count;
             index++)
        {
            if (index > 0)
                sql.Append(", ");
            sql.Append(QuoteIdentifier(
                table.Order.Columns[index]
                    .CatalogObject.SourceName));
            sql.Append(" ASC");
        }
        sql.Append(';');
        return sql.ToString();
    }

    internal static SqlServerRetainedReadCommand CreateCommand(
        SqlServerRetainedTableBinding table,
        SqlServerRetainedCaptureOptions options)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        return new SqlServerRetainedReadCommand(
            Build(table),
            options.RowCommandTimeoutSeconds,
            QualifiedName(table),
            table.SqlServerObjectId);
    }

    internal static string QualifiedName(
        SqlServerRetainedTableBinding table)
    {
        string schema =
            table.CatalogObject.SourceNamespace ??
            throw new SqlServerMigrationException(
                "A retained SQL Server table is missing its schema.");
        return string.Concat(
            QuoteIdentifier(schema),
            ".",
            QuoteIdentifier(table.CatalogObject.SourceName));
    }

    internal static string QuoteIdentifier(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (value.Length > 128 ||
            value.IndexOf('\0') >= 0)
        {
            throw new SqlServerMigrationException(
                "A SQL Server identifier is outside the verified source-name bounds.");
        }
        try
        {
            _ = StrictUtf8.GetByteCount(value);
        }
        catch (EncoderFallbackException)
        {
            throw new SqlServerMigrationException(
                "A SQL Server identifier is not valid Unicode.");
        }
        return string.Concat(
            "[",
            value.Replace("]", "]]", StringComparison.Ordinal),
            "]");
    }
}
