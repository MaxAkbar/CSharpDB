using CSharpDB.Migration;
using CSharpDB.Migration.Retained;
using CSharpDB.Migration.SqlServer;
using Microsoft.Data.SqlClient;

namespace CSharpDB.Migration.SqlServer.Tests;

public sealed class SqlServerLeastPrivilegeLiveQualificationTests
{
    private const string LiveAdminConnectionEnvironmentVariable =
        "CSHARPDB_SQLSERVER_LIVE_ADMIN_CONNECTION";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task RestrictedAccountProvesCompleteMetadataAndDetectsObjectDeny()
    {
        string? adminConnectionString =
            Environment.GetEnvironmentVariable(
                LiveAdminConnectionEnvironmentVariable);
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(adminConnectionString),
            $"Set {LiveAdminConnectionEnvironmentVariable} to run the SQL Server least-privilege live qualification.");

        var adminSettings = new SqlConnectionStringBuilder(adminConnectionString)
        {
            InitialCatalog = "master",
            Pooling = false,
            ConnectRetryCount = 0,
        };
        string suffix = Guid.NewGuid().ToString("N")[..12];
        string databaseName = $"CSharpDbMigrationQualification_{suffix}";
        string loginName = $"csharpdb_migration_reader_{suffix}";
        string password = string.Concat(
            Guid.NewGuid().ToString("N"),
            "!Aa7");
        string workspace = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-sqlserver-live-{suffix}");
        Directory.CreateDirectory(workspace);

        await using var admin = new SqlConnection(adminSettings.ConnectionString);
        await admin.OpenAsync(Ct);
        try
        {
            await ExecuteAsync(
                admin,
                $"""
                CREATE LOGIN [{loginName}]
                    WITH PASSWORD = N'{password}', CHECK_POLICY = OFF;
                """);
            await ExecuteAsync(
                admin,
                $"CREATE DATABASE [{databaseName}];");
            await ExecuteAsync(
                admin,
                $"ALTER DATABASE [{databaseName}] SET ALLOW_SNAPSHOT_ISOLATION ON;");

            var databaseSettings =
                new SqlConnectionStringBuilder(adminSettings.ConnectionString)
                {
                    InitialCatalog = databaseName,
                };
            await using (var database =
                         new SqlConnection(databaseSettings.ConnectionString))
            {
                await database.OpenAsync(Ct);
                await ExecuteAsync(
                    database,
                    $"""
                    CREATE USER [{loginName}] FOR LOGIN [{loginName}];
                    CREATE TABLE dbo.Orders
                    (
                        Id int NOT NULL CONSTRAINT PK_Orders PRIMARY KEY,
                        Name nvarchar(100) NOT NULL
                    );
                    CREATE TABLE dbo.Hidden
                    (
                        Id int NOT NULL CONSTRAINT PK_Hidden PRIMARY KEY
                    );
                    INSERT dbo.Orders(Id, Name)
                    VALUES (1, N'one'), (2, N'two');
                    GRANT CONNECT TO [{loginName}];
                    GRANT VIEW DEFINITION TO [{loginName}];
                    GRANT SELECT ON SCHEMA::dbo TO [{loginName}];
                    GRANT SELECT
                        ON OBJECT::sys.sql_expression_dependencies
                        TO [{loginName}];
                    """);
                await ExecuteAsync(
                    database,
                    """
                    CREATE VIEW dbo.OrderView
                    AS
                        SELECT Id, Name
                        FROM dbo.Orders;
                    """);
                await ExecuteAsync(
                    database,
                    """
                    CREATE PROCEDURE dbo.ReadOrders
                    AS
                    BEGIN
                        SET NOCOUNT ON;
                        SELECT Id, Name
                        FROM dbo.Orders;
                    END;
                    """);
                await ExecuteAsync(
                    database,
                    """
                    CREATE TRIGGER dbo.OrdersTrigger
                    ON dbo.Orders
                    AFTER INSERT
                    AS
                    BEGIN
                        SET NOCOUNT ON;
                    END;
                    """);
            }

            var restrictedSettings =
                new SqlConnectionStringBuilder(databaseSettings.ConnectionString)
                {
                    IntegratedSecurity = false,
                    UserID = loginName,
                    Password = password,
                };
            var inspector = new SqlServerMigrationSourceInspector(
                restrictedSettings.ConnectionString);

            MigrationCatalog complete = await inspector.InspectAsync(
                Request(),
                Ct);

            MigrationCatalogObject completeDatabase = Assert.Single(
                complete.Objects,
                static item => item.Kind == MigrationObjectKind.Database);
            Assert.Equal(
                "complete",
                Facet(
                    completeDatabase,
                    "sqlServerMetadataVisibility"));
            Assert.DoesNotContain(
                complete.Diagnostics,
                static item =>
                    item.RuleId ==
                    "MIG-SQLSERVER-METADATA-VISIBILITY-001");

            string packagePath =
                Path.Combine(
                    workspace,
                    "source.csdbsqlserver");
            RetainedMigrationPackageWriteResult retained =
                await SqlServerRetainedCapture.CaptureAsync(
                    restrictedSettings.ConnectionString,
                    packagePath,
                    new SqlServerRetainedCaptureOptions
                    {
                        MaxPackageBytes = 16 * 1024 * 1024,
                    },
                    Ct);
            Assert.Equal(2, retained.RowCounts.Count);
            Assert.Equal(2, retained.RowCounts.Values.Sum());

            await using (RetainedMigrationPackageSession session =
                         await RetainedMigrationPackageSession.OpenAsync(
                             packagePath,
                             new RetainedMigrationPackageOpenOptions
                             {
                                 ExpectedPackageDigest =
                                     retained.PackageDigest,
                                 WorkspacePath =
                                     workspace,
                                 MaxPackageBytes =
                                     16 * 1024 * 1024,
                             },
                             Ct))
            {
                Assert.DoesNotContain(
                    session.Catalog.Diagnostics,
                    static item =>
                        item.RuleId ==
                        "MIG-SQLSERVER-METADATA-VISIBILITY-001");
                MigrationPlan plan =
                    new MigrationPlanner().CreatePlan(
                        session.Catalog,
                        new MigrationPlanningOptions
                        {
                            AcceptAllExclusions = true,
                        });
                MigrationPlanReadiness readiness =
                    MigrationPlanReadinessValidator.Evaluate(
                        plan,
                        session.Catalog);
                Assert.DoesNotContain(
                    readiness.BlockingDiagnosticIds,
                    diagnosticId =>
                        diagnosticId.Contains(
                            "mig-sqlserver-metadata-visibility-001",
                            StringComparison.Ordinal));
            }

            await using (var database =
                         new SqlConnection(databaseSettings.ConnectionString))
            {
                await database.OpenAsync(Ct);
                await ExecuteAsync(
                    database,
                    $"DENY VIEW DEFINITION ON OBJECT::dbo.Hidden TO [{loginName}];");
            }

            MigrationCatalog denied = await inspector.InspectAsync(
                Request(),
                Ct);

            MigrationCatalogObject deniedDatabase = Assert.Single(
                denied.Objects,
                static item => item.Kind == MigrationObjectKind.Database);
            Assert.Equal(
                "incomplete",
                Facet(
                    deniedDatabase,
                    "sqlServerMetadataVisibility"));
            Assert.Contains(
                denied.Diagnostics,
                static item =>
                    item.RuleId ==
                        "MIG-SQLSERVER-METADATA-VISIBILITY-001" &&
                    !item.CanOverride);
            Assert.Contains(
                denied.Diagnostics,
                static item =>
                    item.RuleId ==
                        "MIG-SQLSERVER-PERMISSION-DENY-001" &&
                    !item.CanOverride);
        }
        finally
        {
            await ExecuteAsync(
                admin,
                $"""
                IF DB_ID(N'{databaseName}') IS NOT NULL
                BEGIN
                    ALTER DATABASE [{databaseName}]
                        SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{databaseName}];
                END;
                IF SUSER_ID(N'{loginName}') IS NOT NULL
                    DROP LOGIN [{loginName}];
                """);
            if (Directory.Exists(workspace))
                Directory.Delete(workspace, recursive: true);
        }
    }

    private static MigrationInspectionRequest Request() =>
        new()
        {
            TargetCSharpDbVersion =
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
        };

    private static async Task ExecuteAsync(
        SqlConnection connection,
        string commandText)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = commandText;
        command.CommandTimeout = 30;
        await command.ExecuteNonQueryAsync(Ct);
    }

    private static string Facet(
        MigrationCatalogObject item,
        string name) =>
        Assert.Single(
            item.Facets,
            facet => string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal)).Value ??
        throw new InvalidOperationException(
            $"Facet '{name}' has no value.");
}
