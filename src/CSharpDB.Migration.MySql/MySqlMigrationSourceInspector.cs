using CSharpDB.Migration;
using MySqlConnector;

namespace CSharpDB.Migration.MySql;

internal interface IMySqlCatalogReader
{
    ValueTask<MySqlCatalogSnapshot> ReadAsync(
        MySqlInspectionLimits limits,
        CancellationToken cancellationToken);
}

/// <summary>
/// Builds a deterministic, schema-only MySQL catalog from bounded metadata.
/// </summary>
public sealed class MySqlMigrationSourceInspector : IMigrationSourceInspector
{
    private readonly IMySqlCatalogReader reader;
    private readonly MySqlInspectionLimits limits;

    public MySqlMigrationSourceInspector(string connectionString)
        : this(
            new MySqlCatalogReader(connectionString),
            MySqlInspectionLimits.Default)
    {
    }

    internal MySqlMigrationSourceInspector(
        IMySqlCatalogReader reader,
        MySqlInspectionLimits limits)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(limits);
        limits.Validate();
        this.reader = reader;
        this.limits = limits;
    }

    public MigrationSourceKind SourceKind => MigrationSourceKind.MySql;

    public async ValueTask<MigrationCatalog> InspectAsync(
        MigrationInspectionRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();
        ValidateRequest(request);

        try
        {
            MySqlCatalogSnapshot snapshot = await reader.ReadAsync(
                    limits,
                    cancellationToken)
                .ConfigureAwait(false);
            return MySqlCatalogBuilder.Build(
                snapshot,
                request,
                limits,
                cancellationToken);
        }
        catch (MySqlMigrationException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception) when (
            exception is MySqlException or InvalidOperationException or IOException or
                FormatException or InvalidCastException or OverflowException)
        {
            throw new MySqlMigrationException(
                "The MySQL schema could not be inspected safely.");
        }
    }

    private static void ValidateRequest(MigrationInspectionRequest request)
    {
        if (!string.Equals(
                request.TargetCSharpDbVersion,
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                StringComparison.Ordinal))
        {
            throw new NotSupportedException(
                $"The MySQL analyzer targets CSharpDB {CSharpDbCapabilityCatalogLoader.CurrentTargetVersion}.");
        }
        if (request.ProfileSampleSize <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "Profile sample size must be positive.");
        }
        if (request.IncludeProfile)
        {
            throw new NotSupportedException(
                "The Phase 7B MySQL checkpoint performs schema analysis only; data profiling is not supported.");
        }
    }
}
