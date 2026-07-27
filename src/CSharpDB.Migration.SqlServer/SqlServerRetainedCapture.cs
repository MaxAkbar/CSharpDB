using CSharpDB.Migration.Retained;
using Microsoft.Data.SqlClient;

namespace CSharpDB.Migration.SqlServer;

/// <summary>
/// Captures the bounded SQL Server subset that can be represented by the
/// provider-neutral retained migration package contract.
/// </summary>
public static class SqlServerRetainedCapture
{
    public static async ValueTask<
        RetainedMigrationPackageWriteResult> CaptureAsync(
        string connectionString,
        string outputPath,
        SqlServerRetainedCaptureOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            connectionString);
        SqlServerRetainedCaptureOptions effective =
            options ?? new SqlServerRetainedCaptureOptions();
        effective.Validate();
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            SqlServerLiveRetainedCaptureSource source =
                await SqlServerLiveRetainedCaptureSource
                    .OpenAsync(
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
        catch (SqlServerMigrationException exception)
            when (IsInspectionLimit(exception))
        {
            throw new SqlServerRetainedCaptureLimitException(
                "The retained SQL Server capture exceeded a fixed inspection safety bound.");
        }
        catch (SqlServerMigrationException)
        {
            throw;
        }
        catch (RetainedMigrationPackageLimitException)
        {
            throw new SqlServerRetainedCaptureLimitException(
                "The retained SQL Server capture exceeded a configured package safety bound.");
        }
        catch (RetainedMigrationPackageException)
        {
            throw;
        }
        catch (Exception exception) when (
            exception is SqlException or
                InvalidOperationException or
                IOException or
                UnauthorizedAccessException)
        {
            throw new SqlServerMigrationException(
                "The retained SQL Server capture could not be completed.");
        }
    }

    internal static async ValueTask<
        RetainedMigrationPackageWriteResult> CaptureAsync(
        ISqlServerRetainedCaptureSource source,
        string outputPath,
        SqlServerRetainedCaptureOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        cancellationToken.ThrowIfCancellationRequested();

        await using ISqlServerRetainedCaptureSource
            ownedSource = source;
        MigrationCatalog analyzerCatalog =
            await ownedSource.ReadCatalogAsync(
                    cancellationToken)
                .ConfigureAwait(false);
        SqlServerRetainedSourceBinding binding =
            SqlServerRetainedBinding.Create(
                analyzerCatalog,
                options);
        var budget =
            new SqlServerRetainedCaptureBudget(
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
                    Tables =
                        Array.AsReadOnly(writes),
                    CatalogFactory =
                        (summary, factoryCancellationToken) =>
                        {
                            factoryCancellationToken
                                .ThrowIfCancellationRequested();
                            return ValueTask.FromResult(
                                SqlServerRetainedCatalog
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
        SqlServerRetainedTableBinding table)
    {
        SqlServerRetainedOrderBinding order =
            table.Order ??
            throw new InvalidOperationException(
                "A data-available SQL Server table is missing its ordering binding.");
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
        SqlServerMigrationException exception) =>
        exception.GetType() ==
            typeof(SqlServerMigrationException) &&
        exception.ErrorCode ==
            SqlServerMigrationErrorCode
                .InspectionLimit;
}
