using System.Data;
using System.Runtime.CompilerServices;
using System.Text;
using MySqlConnector;

namespace CSharpDB.Migration.MySql;

internal interface IMySqlRetainedCaptureSource : IAsyncDisposable
{
    ValueTask<MigrationCatalog> ReadCatalogAsync(
        CancellationToken cancellationToken);

    IAsyncEnumerable<MigrationDataRow> ReadRowsAsync(
        MySqlRetainedTableBinding table,
        MySqlRetainedCaptureOptions options,
        MySqlRetainedCaptureBudget budget,
        CancellationToken cancellationToken);
}

internal sealed class MySqlRetainedCaptureBudget
{
    private readonly long maximumRows;
    private long rows;

    internal MySqlRetainedCaptureBudget(long maximumRowsTotal)
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
            throw new MySqlRetainedCaptureLimitException(
                "The MySQL retained capture exceeds its total row-count bound.");
        }
    }

    internal void EnsureCanAdd(long additionalRows)
    {
        if (additionalRows < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(additionalRows));
        }
        long current = Volatile.Read(ref rows);
        if (additionalRows > maximumRows - current)
        {
            throw new MySqlRetainedCaptureLimitException(
                "The MySQL retained capture exceeds its total row-count bound.");
        }
    }
}

internal sealed class MySqlLiveRetainedCaptureSource :
    IMySqlRetainedCaptureSource
{
    private readonly MySqlCatalogReader catalogReader;
    private readonly MySqlInspectionLimits inspectionLimits;
    private readonly MySqlConnection connection;
    private readonly MySqlTransaction transaction;
    private int catalogReads;
    private int disposed;

    private MySqlLiveRetainedCaptureSource(
        MySqlCatalogReader catalogReader,
        MySqlInspectionLimits inspectionLimits,
        MySqlConnection connection,
        MySqlTransaction transaction)
    {
        this.catalogReader = catalogReader;
        this.inspectionLimits = inspectionLimits;
        this.connection = connection;
        this.transaction = transaction;
    }

    internal static async ValueTask<
        MySqlLiveRetainedCaptureSource> OpenAsync(
        string connectionString,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            connectionString);
        cancellationToken.ThrowIfCancellationRequested();
        var reader = new MySqlCatalogReader(connectionString);
        MySqlConnection connection = reader.CreateConnection();
        try
        {
            await connection.OpenAsync(cancellationToken)
                .ConfigureAwait(false);
            MySqlTransaction transaction =
                await connection.BeginTransactionAsync(
                        IsolationLevel.Snapshot,
                        isReadOnly: true,
                        cancellationToken)
                    .ConfigureAwait(false);
            return new MySqlLiveRetainedCaptureSource(
                reader,
                MySqlInspectionLimits.Default,
                connection,
                transaction);
        }
        catch
        {
            await connection.DisposeAsync()
                .ConfigureAwait(false);
            throw;
        }
    }

    public async ValueTask<MigrationCatalog> ReadCatalogAsync(
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        int read = Interlocked.Increment(ref catalogReads);
        if (read > 2)
        {
            throw new InvalidOperationException(
                "The retained MySQL catalog can be read only before and after row capture.");
        }

        MySqlCatalogSnapshot snapshot =
            await catalogReader.ReadAsync(
                    connection,
                    transaction,
                    inspectionLimits,
                    cancellationToken)
                .ConfigureAwait(false);
        return MySqlCatalogBuilder.Build(
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
        MySqlRetainedTableBinding table,
        MySqlRetainedCaptureOptions options,
        MySqlRetainedCaptureBudget budget,
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
                "Only data-available MySQL tables can be retained.",
                nameof(table));
        }

        MySqlRetainedReadCommand definition =
            MySqlRetainedReadSql.CreateCommand(
                table,
                options);
        await VerifyTableIdentityAsync(
                definition,
                cancellationToken)
            .ConfigureAwait(false);
        long preflightRows = await PreflightRowsAsync(
                table,
                definition,
                options,
                budget,
                cancellationToken)
            .ConfigureAwait(false);

        await using var command = new MySqlCommand(
            definition.CommandText,
            connection,
            transaction)
        {
            CommandType = CommandType.Text,
            CommandTimeout =
                definition.CommandTimeoutSeconds,
        };
        command.Parameters.Add(
            new MySqlParameter(
                "@max_value_bytes",
                MySqlDbType.Int32)
            {
                Value = options.MaxValueBytes,
            });
        await command.PrepareAsync(cancellationToken)
            .ConfigureAwait(false);
        await using MySqlDataReader reader =
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
                throw new MySqlRetainedCaptureLimitException(
                    "A MySQL retained table exceeds its row-count bound.");
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
                MySqlProjectedScalar projected =
                    MySqlScalarCodec.Read(
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
                    throw new MySqlRetainedCaptureLimitException(
                        "A MySQL retained row exceeds its byte bound.");
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
        if (tableRows != preflightRows)
        {
            throw new MySqlMigrationException(
                "The retained MySQL row preflight no longer matches its row stream.");
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
            return;
        await MySqlRetainedCleanup.DisposeAsync(
                () => new ValueTask(
                    transaction.RollbackAsync(
                        CancellationToken.None)),
                transaction.DisposeAsync,
                connection.DisposeAsync)
            .ConfigureAwait(false);
    }

    private async ValueTask VerifyTableIdentityAsync(
        MySqlRetainedReadCommand definition,
        CancellationToken cancellationToken)
    {
        await using var command = new MySqlCommand(
            MySqlRetainedReadSql.IdentityQuery,
            connection,
            transaction)
        {
            CommandType = CommandType.Text,
            CommandTimeout =
                definition.CommandTimeoutSeconds,
        };
        command.Parameters.Add(
            new MySqlParameter(
                "@database_name",
                MySqlDbType.VarChar)
            {
                Value = definition.DatabaseName,
            });
        command.Parameters.Add(
            new MySqlParameter(
                "@table_name",
                MySqlDbType.VarChar)
            {
                Value = definition.TableName,
            });
        await using MySqlDataReader reader =
            await command.ExecuteReaderAsync(
                    CommandBehavior.SingleRow |
                    CommandBehavior.SingleResult,
                    cancellationToken)
                .ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken)
                .ConfigureAwait(false) ||
            !reader.GetBoolean(0) ||
            reader.GetInt64(1) != 1 ||
            reader.GetInt64(2) != 0)
        {
            throw new MySqlMigrationException(
                "The retained MySQL table identity changed.");
        }
    }

    private async ValueTask<long> PreflightRowsAsync(
        MySqlRetainedTableBinding table,
        MySqlRetainedReadCommand definition,
        MySqlRetainedCaptureOptions options,
        MySqlRetainedCaptureBudget budget,
        CancellationToken cancellationToken)
    {
        await using var command = new MySqlCommand(
            definition.PreflightCommandText,
            connection,
            transaction)
        {
            CommandType = CommandType.Text,
            CommandTimeout =
                definition.CommandTimeoutSeconds,
        };
        await command.PrepareAsync(cancellationToken)
            .ConfigureAwait(false);
        await using MySqlDataReader reader =
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
                throw new MySqlRetainedCaptureLimitException(
                    "A MySQL retained table exceeds its row-count bound.");
            }
            budget.EnsureCanAdd(
                checked(tableRows + 1));

            long rowBytes = 0;
            for (int index = 0;
                 index < table.Columns.Count;
                 index++)
            {
                if (reader.IsDBNull(index))
                {
                    if (!table.Columns[index].Nullable)
                    {
                        throw new MySqlMigrationException(
                            "MySQL returned NULL for a nonnullable retained column.");
                    }
                    rowBytes = checked(rowBytes + 1);
                    continue;
                }

                long valueBytes = reader.GetInt64(index);
                if (valueBytes < 0 ||
                    valueBytes > options.MaxValueBytes)
                {
                    throw new MySqlRetainedCaptureLimitException(
                        "A MySQL scalar exceeds the retained value bound.");
                }
                rowBytes = checked(
                    rowBytes + 1L + valueBytes);
                if (rowBytes > options.MaxRowBytes)
                {
                    throw new MySqlRetainedCaptureLimitException(
                        "A MySQL retained row exceeds its byte bound.");
                }
            }
            tableRows++;
        }
        return tableRows;
    }

    private void ThrowIfDisposed() =>
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref disposed) != 0,
            this);
}

internal static class MySqlRetainedCleanup
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

        await IgnoreProviderFailureAsync(rollbackAsync)
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
            exception is MySqlException or
                InvalidOperationException)
        {
            // Cleanup must not replace either a published result or the
            // original bounded capture failure with provider message text.
        }
    }
}

internal readonly record struct MySqlRetainedReadCommand(
    string CommandText,
    string PreflightCommandText,
    int CommandTimeoutSeconds,
    string DatabaseName,
    string TableName);

internal static class MySqlRetainedReadSql
{
    internal const string IdentityQuery =
        """
        SELECT
            BINARY DATABASE() = BINARY @database_name,
            (
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES AS t
                WHERE BINARY t.TABLE_SCHEMA = BINARY @database_name
                  AND BINARY t.TABLE_NAME = BINARY @table_name
                  AND t.TABLE_TYPE = 'BASE TABLE'
                  AND t.ENGINE = 'InnoDB'
            ),
            (
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.PARTITIONS AS p
                WHERE BINARY p.TABLE_SCHEMA = BINARY @database_name
                  AND BINARY p.TABLE_NAME = BINARY @table_name
                  AND p.PARTITION_NAME IS NOT NULL
            );
        """;

    private static readonly UTF8Encoding StrictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);

    internal static string Build(
        MySqlRetainedTableBinding table)
    {
        ArgumentNullException.ThrowIfNull(table);
        if (!table.IsAvailable ||
            table.Order is null ||
            table.Columns.Count == 0 ||
            table.Order.Columns.Count == 0)
        {
            throw new ArgumentException(
                "A retained MySQL read requires columns and a stable order.",
                nameof(table));
        }

        var sql = new StringBuilder("SELECT ");
        for (int index = 0;
             index < table.Columns.Count;
             index++)
        {
            if (index > 0)
                sql.Append(", ");
            MySqlRetainedColumnBinding binding =
                table.Columns[index];
            string column = QuoteIdentifier(
                binding.CatalogObject.SourceName);
            AppendProjectedLength(
                sql,
                binding,
                column);
            sql.Append(", CASE WHEN ");
            AppendProjectedLength(
                sql,
                binding,
                column);
            sql.Append(" <= @max_value_bytes THEN ");
            sql.Append(column);
            sql.Append(" ELSE NULL END");
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

    internal static string BuildLengthPreflight(
        MySqlRetainedTableBinding table)
    {
        ArgumentNullException.ThrowIfNull(table);
        if (!table.IsAvailable ||
            table.Order is null ||
            table.Columns.Count == 0 ||
            table.Order.Columns.Count == 0)
        {
            throw new ArgumentException(
                "A retained MySQL preflight requires columns and a stable order.",
                nameof(table));
        }

        var sql = new StringBuilder("SELECT ");
        for (int index = 0;
             index < table.Columns.Count;
             index++)
        {
            if (index > 0)
                sql.Append(", ");
            MySqlRetainedColumnBinding binding =
                table.Columns[index];
            AppendProjectedLength(
                sql,
                binding,
                QuoteIdentifier(
                    binding.CatalogObject.SourceName));
        }
        sql.Append(" FROM ");
        sql.Append(QualifiedName(table));
        sql.Append(" ORDER BY ");
        AppendOrder(sql, table);
        sql.Append(';');
        return sql.ToString();
    }

    internal static MySqlRetainedReadCommand CreateCommand(
        MySqlRetainedTableBinding table,
        MySqlRetainedCaptureOptions options)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        string database =
            table.CatalogObject.SourceNamespace ??
            throw new MySqlMigrationException(
                "A retained MySQL table is missing its database.");
        return new MySqlRetainedReadCommand(
            Build(table),
            BuildLengthPreflight(table),
            options.RowCommandTimeoutSeconds,
            database,
            table.CatalogObject.SourceName);
    }

    internal static string QualifiedName(
        MySqlRetainedTableBinding table)
    {
        string database =
            table.CatalogObject.SourceNamespace ??
            throw new MySqlMigrationException(
                "A retained MySQL table is missing its database.");
        return string.Concat(
            QuoteIdentifier(database),
            ".",
            QuoteIdentifier(
                table.CatalogObject.SourceName));
    }

    internal static string QuoteIdentifier(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (value.Length > 64 ||
            value.IndexOf('\0') >= 0)
        {
            throw new MySqlMigrationException(
                "A MySQL identifier is outside the verified source-name bounds.");
        }
        try
        {
            _ = StrictUtf8.GetByteCount(value);
        }
        catch (EncoderFallbackException)
        {
            throw new MySqlMigrationException(
                "A MySQL identifier is not valid Unicode.");
        }
        return string.Concat(
            "`",
            value.Replace(
                "`",
                "``",
                StringComparison.Ordinal),
            "`");
    }

    private static void AppendOrder(
        StringBuilder sql,
        MySqlRetainedTableBinding table)
    {
        MySqlRetainedOrderBinding order =
            table.Order ??
            throw new ArgumentException(
                "A retained MySQL read requires a stable order.",
                nameof(table));
        for (int index = 0;
             index < order.Columns.Count;
             index++)
        {
            if (index > 0)
                sql.Append(", ");
            sql.Append(QuoteIdentifier(
                order.Columns[index]
                    .CatalogObject.SourceName));
            sql.Append(" ASC");
        }
    }

    private static void AppendProjectedLength(
        StringBuilder sql,
        MySqlRetainedColumnBinding column,
        string quotedIdentifier)
    {
        sql.Append("OCTET_LENGTH(");
        if (column.Codec ==
            MySqlScalarCodecKind.Text)
        {
            // MySqlConnector always negotiates utf8mb4 for result text.
            // Measure the bytes the provider can buffer rather than the
            // source column's storage-charset bytes.
            sql.Append("CONVERT(");
            sql.Append(quotedIdentifier);
            sql.Append(" USING utf8mb4)");
        }
        else
        {
            sql.Append(quotedIdentifier);
        }
        sql.Append(')');
    }
}
