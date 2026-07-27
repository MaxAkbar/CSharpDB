using CSharpDB.Migration;
using Microsoft.Data.SqlClient;

namespace CSharpDB.Migration.SqlServer;

/// <summary>
/// Builds a deterministic SQL Server schema-readiness catalog using only
/// static SELECT statements over SERVERPROPERTY and sys catalog views.
/// </summary>
public sealed class SqlServerMigrationSourceInspector : IMigrationSourceInspector
{
    private readonly ISqlServerCatalogReader reader;
    private readonly SqlServerInspectionLimits limits;

    public SqlServerMigrationSourceInspector(string connectionString)
        : this(
            new SqlServerCatalogReader(connectionString),
            SqlServerInspectionLimits.Default)
    {
    }

    internal SqlServerMigrationSourceInspector(
        ISqlServerCatalogReader reader,
        SqlServerInspectionLimits limits)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(limits);
        limits.Validate();
        this.reader = reader;
        this.limits = limits;
    }

    public MigrationSourceKind SourceKind => MigrationSourceKind.SqlServer;

    public async ValueTask<MigrationCatalog> InspectAsync(
        MigrationInspectionRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();
        if (!string.Equals(
                request.TargetCSharpDbVersion,
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                StringComparison.Ordinal))
        {
            throw new NotSupportedException(
                $"The SQL Server analyzer is qualified for CSharpDB {CSharpDbCapabilityCatalogLoader.CurrentTargetVersion}.");
        }
        if (request.ProfileSampleSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(request), "Profile sample size must be positive.");
        if (request.IncludeProfile)
        {
            throw new NotSupportedException(
                "The Phase 7A SQL Server checkpoint performs schema analysis only; data profiling is not supported.");
        }

        try
        {
            SqlServerCatalogSnapshot snapshot = await reader.ReadAsync(
                    limits,
                    cancellationToken)
                .ConfigureAwait(false);
            return SqlServerCatalogBuilder.Build(
                snapshot,
                request,
                limits,
                cancellationToken);
        }
        catch (SqlServerMigrationException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception) when (
            exception is SqlException or InvalidOperationException or IOException or
                FormatException or InvalidCastException or OverflowException)
        {
            throw new SqlServerMigrationException(
                "The SQL Server schema could not be inspected safely.");
        }
    }
}
