using CSharpDB.Migration;
using CSharpDB.Migration.CSharpDb;

namespace CSharpDB.Cli;

internal sealed record MigrationCommandDependencies
{
    internal static MigrationCommandDependencies Default { get; } = new();

    internal Func<
        string,
        string,
        CancellationToken,
        ValueTask<SqlServerWorkerResult>>
    InspectSqlServerAsync
    { get; init; } =
        SqlServerWorkerClient.InspectAsync;

    internal Func<
        string,
        string,
        string,
        long,
        int,
        CancellationToken,
        ValueTask<SqlServerCaptureWorkerResult>>
    CaptureSqlServerAsync
    { get; init; } =
        SqlServerWorkerClient.CaptureAsync;

    internal Func<
        string,
        string,
        CancellationToken,
        ValueTask<MySqlWorkerResult>>
    InspectMySqlAsync
    { get; init; } =
        MySqlWorkerClient.InspectAsync;

    internal Func<
        MigrationPlan,
        MigrationCatalog,
        CancellationToken,
        CSharpDbDdlPreview>
    BuildCSharpDbDdlPreview
    { get; init; } =
        static (plan, catalog, cancellationToken) =>
            CSharpDbDdlPreviewBuilder.BuildBounded(
                plan,
                catalog,
                cancellationToken: cancellationToken);

    internal Func<
        string,
        CancellationToken,
        ValueTask<CSharpDbDdlCompatibilityReport>>
    AnalyzeCSharpDbDdlAsync
    { get; init; } =
        static (script, cancellationToken) =>
            CSharpDbDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken: cancellationToken);

    internal Func<
        string,
        string,
        CancellationToken,
        ValueTask<SqlServerDdlWorkerResult>>
    AnalyzeTsqlDdlAsync
    { get; init; } =
        SqlServerWorkerClient.AnalyzeDdlAsync;

    internal Func<
        MigrationPlan,
        MigrationCatalog,
        CancellationToken,
        MigrationPlan>
    SealCSharpDbMigrationPlan
    { get; init; } =
        static (plan, catalog, cancellationToken) =>
            CSharpDbDdlPreviewBuilder.BuildAndAttachGeneratedDdlDigestBounded(
                plan,
                catalog,
                cancellationToken: cancellationToken);

    internal Func<MigrationPlan, MigrationCatalog, string>
    SerializeMigrationPlan
    { get; init; } =
        static (plan, catalog) =>
            MigrationArtifactSerializer.SerializePlan(
                plan,
                catalog);
}
