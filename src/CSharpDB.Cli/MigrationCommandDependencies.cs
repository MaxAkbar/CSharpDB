using CSharpDB.Migration;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Migration.SqlServer;

namespace CSharpDB.Cli;

internal sealed record MigrationCommandDependencies
{
    internal static MigrationCommandDependencies Default { get; } = new();

    internal Func<string, string?> ReadEnvironmentVariable { get; init; } =
        Environment.GetEnvironmentVariable;

    internal Func<string, IMigrationSourceInspector> CreateSqlServerInspector
    {
        get;
        init;
    } = connectionString =>
        new SqlServerMigrationSourceInspector(connectionString);

    internal Func<
        MigrationPlan,
        MigrationCatalog,
        CancellationToken,
        CSharpDbDdlPreview> BuildCSharpDbDdlPreview { get; init; } =
        static (plan, catalog, cancellationToken) =>
            CSharpDbDdlPreviewBuilder.BuildBounded(
                plan,
                catalog,
                cancellationToken: cancellationToken);

    internal Func<
        MigrationPlan,
        MigrationCatalog,
        CancellationToken,
        MigrationPlan> SealCSharpDbMigrationPlan { get; init; } =
        static (plan, catalog, cancellationToken) =>
            CSharpDbDdlPreviewBuilder.BuildAndAttachGeneratedDdlDigestBounded(
                plan,
                catalog,
                cancellationToken: cancellationToken);

    internal Func<MigrationPlan, MigrationCatalog, string>
        SerializeMigrationPlan { get; init; } =
        static (plan, catalog) =>
            MigrationArtifactSerializer.SerializePlan(
                plan,
                catalog);
}
