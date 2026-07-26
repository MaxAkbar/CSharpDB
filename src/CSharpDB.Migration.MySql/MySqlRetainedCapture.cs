using CSharpDB.Migration.Retained;
using MySqlConnector;

namespace CSharpDB.Migration.MySql;

/// <summary>
/// Captures the bounded MySQL subset that can be represented by the
/// provider-neutral retained migration package contract.
/// </summary>
public static class MySqlRetainedCapture
{
    public static async ValueTask<
        RetainedMigrationPackageWriteResult> CaptureAsync(
        string connectionString,
        string outputPath,
        MySqlRetainedCaptureOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            connectionString);
        MySqlRetainedCaptureOptions effective =
            options ?? new MySqlRetainedCaptureOptions();
        effective.Validate();
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            MySqlLiveRetainedCaptureSource source =
                await MySqlLiveRetainedCaptureSource.OpenAsync(
                        connectionString,
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
        catch (MySqlMigrationException exception)
            when (IsInspectionLimit(exception))
        {
            throw new MySqlRetainedCaptureLimitException(
                "The retained MySQL capture exceeded a fixed inspection safety bound.");
        }
        catch (MySqlMigrationException)
        {
            throw;
        }
        catch (RetainedMigrationPackageLimitException)
        {
            throw new MySqlRetainedCaptureLimitException(
                "The retained MySQL capture exceeded a configured package safety bound.");
        }
        catch (RetainedMigrationPackageException)
        {
            throw;
        }
        catch (Exception exception) when (
            exception is MySqlException or
                InvalidOperationException or
                IOException or
                UnauthorizedAccessException)
        {
            throw new MySqlMigrationException(
                "The retained MySQL capture could not be completed.");
        }
    }

    internal static async ValueTask<
        RetainedMigrationPackageWriteResult> CaptureAsync(
        IMySqlRetainedCaptureSource source,
        string outputPath,
        MySqlRetainedCaptureOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        cancellationToken.ThrowIfCancellationRequested();

        await using IMySqlRetainedCaptureSource ownedSource = source;
        MigrationCatalog analyzerCatalog =
            await ownedSource.ReadCatalogAsync(cancellationToken)
                .ConfigureAwait(false);
        string analyzerDigest =
            MigrationArtifactSerializer.ComputeCatalogDigest(
                analyzerCatalog);
        MySqlRetainedSourceBinding binding =
            MySqlRetainedBinding.Create(
                analyzerCatalog,
                options);
        var budget =
            new MySqlRetainedCaptureBudget(
                options.MaxRowsTotal);
        RetainedMigrationTableWrite[] writes =
            binding.AvailableTables
                .Select(table =>
                    new RetainedMigrationTableWrite
                    {
                        Descriptor =
                            CreateDescriptor(table),
                        Rows = ownedSource.ReadRowsAsync(
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
                    Tables = Array.AsReadOnly(writes),
                    CatalogFactory =
                        async (
                            summary,
                            factoryCancellationToken) =>
                        {
                            MigrationCatalog finalCatalog =
                                await ownedSource.ReadCatalogAsync(
                                        factoryCancellationToken)
                                    .ConfigureAwait(false);
                            string finalDigest =
                                MigrationArtifactSerializer
                                    .ComputeCatalogDigest(
                                        finalCatalog);
                            if (!string.Equals(
                                    analyzerDigest,
                                    finalDigest,
                                    StringComparison.Ordinal))
                            {
                                throw new MySqlMigrationException(
                                    "The MySQL catalog changed during retained capture.");
                            }

                            return MySqlRetainedCatalog.Create(
                                binding,
                                summary);
                        },
                    Options = options.ToRetainedOptions(),
                },
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static RetainedMigrationTableDescriptor
        CreateDescriptor(
        MySqlRetainedTableBinding table)
    {
        MySqlRetainedOrderBinding order =
            table.Order ??
            throw new InvalidOperationException(
                "A data-available MySQL table is missing its ordering binding.");
        return new RetainedMigrationTableDescriptor
        {
            SourceObjectId =
                table.CatalogObject.ObjectId,
            ColumnObjectIds =
                Array.AsReadOnly(
                    table.Columns
                        .Select(static column =>
                            column.CatalogObject.ObjectId)
                        .ToArray()),
            OrderingKeyColumnObjectIds =
                Array.AsReadOnly(
                    order.Columns
                        .Select(static column =>
                            column.CatalogObject.ObjectId)
                        .ToArray()),
        };
    }

    private static bool IsInspectionLimit(
        MySqlMigrationException exception) =>
        exception.GetType() ==
            typeof(MySqlMigrationException) &&
        exception.ErrorCode ==
            MySqlMigrationErrorCode.InspectionLimit;
}
