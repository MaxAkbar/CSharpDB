using CSharpDB.Migration;
using CSharpDB.Migration.MySql;
using CSharpDB.Migration.Retained;
using MySqlConnector;

namespace CSharpDB.Migration.MySql.Tests;

public sealed class MySqlLeastPrivilegeLiveQualificationTests
{
    private const string LiveAdminConnectionEnvironmentVariable =
        "CSHARPDB_MYSQL_LIVE_ADMIN_CONNECTION";

    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task
        DirectSchemaGrantsProveBoundedMetadataWhileRoleOnlyFailsClosed()
    {
        string? adminConnectionString =
            Environment.GetEnvironmentVariable(
                LiveAdminConnectionEnvironmentVariable);
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(adminConnectionString),
            $"Set {LiveAdminConnectionEnvironmentVariable} to run the MySQL least-privilege live qualification.");

        var adminSettings =
            new MySqlConnectionStringBuilder(
                adminConnectionString)
            {
                Database = string.Empty,
                Pooling = false,
            };
        string suffix =
            Guid.NewGuid().ToString("N")[..12];
        string databaseName =
            $"CSharpDbMigrationQualification_{suffix}";
        string directUser =
            $"csdb_rd_{suffix}";
        string roleUser =
            $"csdb_rr_{suffix}";
        string roleName =
            $"csdb_role_{suffix}";
        string directPassword =
            Password();
        string rolePassword =
            Password();
        string directAccount =
            Account(directUser);
        string roleAccount =
            Account(roleUser);
        string role =
            Account(roleName);
        string database =
            Identifier(databaseName);
        string workspace = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-mysql-live-{suffix}");
        Directory.CreateDirectory(workspace);

        await using var admin =
            new MySqlConnection(
                adminSettings.ConnectionString);
        await admin.OpenAsync(Ct);
        try
        {
            await ExecuteAsync(
                admin,
                $"CREATE DATABASE {database};");
            await ExecuteAsync(
                admin,
                $"CREATE USER {directAccount} IDENTIFIED BY '{directPassword}';");
            await ExecuteAsync(
                admin,
                $"CREATE USER {roleAccount} IDENTIFIED BY '{rolePassword}';");
            await ExecuteAsync(
                admin,
                $"CREATE ROLE {role};");

            var databaseSettings =
                new MySqlConnectionStringBuilder(
                    adminSettings.ConnectionString)
                {
                    Database = databaseName,
                };
            await using (var fixture =
                         new MySqlConnection(
                             databaseSettings.ConnectionString))
            {
                await fixture.OpenAsync(Ct);
                await ExecuteAsync(
                    fixture,
                    """
                    CREATE TABLE `Orders`
                    (
                        `Id` BIGINT NOT NULL,
                        `Name` VARCHAR(100) NOT NULL,
                        CONSTRAINT `PK_Orders`
                            PRIMARY KEY (`Id`)
                    ) ENGINE = InnoDB;
                    """);
                await ExecuteAsync(
                    fixture,
                    """
                    INSERT INTO `Orders` (`Id`, `Name`)
                    VALUES (1, 'one'), (2, 'two');
                    """);
                await ExecuteAsync(
                    fixture,
                    """
                    CREATE VIEW `OrderView`
                    AS
                        SELECT `Id`, `Name`
                        FROM `Orders`;
                    """);
                await ExecuteAsync(
                    fixture,
                    """
                    CREATE TRIGGER `OrdersBeforeInsert`
                    BEFORE INSERT ON `Orders`
                    FOR EACH ROW
                        SET NEW.`Name` = NEW.`Name`;
                    """);
                await ExecuteAsync(
                    fixture,
                    """
                    CREATE PROCEDURE `ReadOrders`()
                    SQL SECURITY INVOKER
                        SELECT `Id`, `Name`
                        FROM `Orders`;
                    """);
                await ExecuteAsync(
                    fixture,
                    """
                    CREATE FUNCTION `IdentityValue`(
                        `value` BIGINT)
                    RETURNS BIGINT
                    DETERMINISTIC
                    NO SQL
                        RETURN `value`;
                    """);
            }

            string directPrivileges =
                $"SELECT, SHOW VIEW, TRIGGER, EXECUTE ON {database}.*";
            await ExecuteAsync(
                admin,
                $"GRANT {directPrivileges} TO {directAccount};");
            await ExecuteAsync(
                admin,
                $"GRANT {directPrivileges} TO {role};");
            await ExecuteAsync(
                admin,
                $"GRANT {role} TO {roleAccount};");
            await ExecuteAsync(
                admin,
                $"SET DEFAULT ROLE {role} TO {roleAccount};");

            MySqlConnectionStringBuilder directSettings =
                RestrictedSettings(
                    adminSettings,
                    databaseName,
                    directUser,
                    directPassword);
            var directInspector =
                new MySqlMigrationSourceInspector(
                    directSettings.ConnectionString);
            MigrationCatalog directCatalog =
                await directInspector.InspectAsync(
                    Request(),
                    Ct);

            MigrationCatalogObject directDatabase =
                Assert.Single(
                    directCatalog.Objects,
                    static item =>
                        item.Kind ==
                        MigrationObjectKind.Database);
            foreach (string facet in new[]
                     {
                         "mysqlMetadataVisibilityProofAttempted",
                         "mysqlMetadataVisibilityAccountFormatSupported",
                         "mysqlMetadataVisibilityGranteeMatched",
                         "mysqlDirectSchemaSelect",
                         "mysqlDirectSchemaShowView",
                         "mysqlDirectSchemaTrigger",
                         "mysqlDirectSchemaExecute",
                         "mysqlMetadataVisibilityComplete",
                     })
            {
                Assert.Equal(
                    "true",
                    Facet(directDatabase, facet));
            }
            Assert.DoesNotContain(
                directCatalog.Diagnostics,
                static item =>
                    item.RuleId ==
                    "MIG-MYSQL-METADATA-COMPLETENESS-UNKNOWN-001");
            Assert.DoesNotContain(
                directCatalog.Diagnostics,
                static item =>
                    item.RuleId is
                        "MIG-MYSQL-SERVER-VARIANT-UNQUALIFIED-001" or
                        "MIG-MYSQL-VERSION-UNQUALIFIED-001");
            Assert.Contains(
                directCatalog.Objects,
                static item =>
                    item.Kind ==
                        MigrationObjectKind.Table &&
                    item.SourceName == "Orders");
            Assert.Contains(
                directCatalog.Objects,
                static item =>
                    item.Kind ==
                        MigrationObjectKind.View &&
                    item.SourceName == "OrderView");
            Assert.Contains(
                directCatalog.Objects,
                static item =>
                    item.Kind ==
                        MigrationObjectKind.Trigger &&
                    item.SourceName ==
                        "OrdersBeforeInsert");
            Assert.Equal(
                2,
                directCatalog.Objects.Count(
                    static item =>
                        item.Kind ==
                        MigrationObjectKind.Routine));

            MigrationDiagnostic livePending =
                Assert.Single(
                    directCatalog.Diagnostics,
                    static item =>
                        item.RuleId ==
                        "MIG-MYSQL-LIVE-QUALIFICATION-PENDING-001");
            Assert.False(livePending.CanOverride);
            MigrationPlan directPlan =
                new MigrationPlanner().CreatePlan(
                    directCatalog,
                    new MigrationPlanningOptions
                    {
                        AcceptAllExclusions = true,
                    });
            MigrationPlanReadiness directReadiness =
                MigrationPlanReadinessValidator.Evaluate(
                    directPlan,
                    directCatalog);
            Assert.Equal(
                MigrationPlanReadinessStatus.Blocked,
                directReadiness.Status);
            Assert.Contains(
                livePending.DiagnosticId,
                directReadiness
                    .BlockingDiagnosticIds);

            string packagePath = Path.Combine(
                workspace,
                "direct.csdbmysql");
            RetainedMigrationPackageWriteResult retained =
                await MySqlRetainedCapture.CaptureAsync(
                    directSettings.ConnectionString,
                    packagePath,
                    new MySqlRetainedCaptureOptions
                    {
                        MaxPackageBytes =
                            16 * 1024 * 1024,
                    },
                    Ct);
            Assert.Equal(
                2,
                Assert.Single(
                    retained.RowCounts).Value);

            await using (
                RetainedMigrationPackageSession session =
                    await RetainedMigrationPackageSession
                        .OpenAsync(
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
                        "MIG-MYSQL-METADATA-COMPLETENESS-UNKNOWN-001");
                Assert.Contains(
                    session.Catalog.Diagnostics,
                    static item =>
                        item.RuleId ==
                            "MIG-MYSQL-RETAINED-LIVE-QUALIFICATION-DEFERRED-001" &&
                        !item.CanOverride);
            }

            MySqlConnectionStringBuilder roleSettings =
                RestrictedSettings(
                    adminSettings,
                    databaseName,
                    roleUser,
                    rolePassword);
            var roleInspector =
                new MySqlMigrationSourceInspector(
                    roleSettings.ConnectionString);
            MigrationCatalog roleCatalog =
                await roleInspector.InspectAsync(
                    Request(),
                    Ct);
            MigrationCatalogObject roleDatabase =
                Assert.Single(
                    roleCatalog.Objects,
                    static item =>
                        item.Kind ==
                        MigrationObjectKind.Database);
            Assert.Equal(
                "true",
                Facet(
                    roleDatabase,
                    "mysqlMetadataVisibilityProofAttempted"));
            Assert.Equal(
                "false",
                Facet(
                    roleDatabase,
                    "mysqlMetadataVisibilityGranteeMatched"));
            Assert.Equal(
                "false",
                Facet(
                    roleDatabase,
                    "mysqlDirectSchemaSelect"));
            Assert.Equal(
                "false",
                Facet(
                    roleDatabase,
                    "mysqlMetadataVisibilityComplete"));
            MigrationDiagnostic roleBlocker =
                Assert.Single(
                    roleCatalog.Diagnostics,
                    static item =>
                        item.RuleId ==
                        "MIG-MYSQL-METADATA-COMPLETENESS-UNKNOWN-001");
            Assert.False(roleBlocker.CanOverride);

            string rolePackagePath = Path.Combine(
                workspace,
                "role-only.csdbmysql");
            MySqlMigrationException roleFailure =
                await Assert.ThrowsAsync<
                    MySqlMigrationException>(
                    async () =>
                        await MySqlRetainedCapture
                            .CaptureAsync(
                                roleSettings.ConnectionString,
                                rolePackagePath,
                                new MySqlRetainedCaptureOptions
                                {
                                    MaxPackageBytes =
                                        16 * 1024 * 1024,
                                },
                                Ct));
            Assert.Equal(
                "Retained MySQL capture requires a direct schema-level SELECT grant for the authenticated account.",
                roleFailure.Message);
            Assert.False(
                File.Exists(rolePackagePath));
        }
        finally
        {
            await ExecuteAsync(
                admin,
                $"DROP DATABASE IF EXISTS {database};");
            await ExecuteAsync(
                admin,
                $"DROP USER IF EXISTS {directAccount};");
            await ExecuteAsync(
                admin,
                $"DROP USER IF EXISTS {roleAccount};");
            await ExecuteAsync(
                admin,
                $"DROP ROLE IF EXISTS {role};");
            if (Directory.Exists(workspace))
            {
                Directory.Delete(
                    workspace,
                    recursive: true);
            }
        }
    }

    private static MySqlConnectionStringBuilder
        RestrictedSettings(
        MySqlConnectionStringBuilder adminSettings,
        string database,
        string user,
        string password) =>
        new(adminSettings.ConnectionString)
        {
            Database = database,
            UserID = user,
            Password = password,
            Pooling = false,
        };

    private static MigrationInspectionRequest Request() =>
        new()
        {
            TargetCSharpDbVersion =
                CSharpDbCapabilityCatalogLoader
                    .CurrentTargetVersion,
        };

    private static string Password() =>
        string.Concat(
            Guid.NewGuid().ToString("N"),
            "!Aa7");

    private static string Identifier(string value)
    {
        if (value.Length > 64 ||
            value.Any(static character =>
                !char.IsAsciiLetterOrDigit(
                    character) &&
                character != '_'))
        {
            throw new InvalidOperationException(
                "The generated MySQL qualification identifier is invalid.");
        }
        return string.Concat(
            "`",
            value,
            "`");
    }

    private static string Account(string user) =>
        string.Concat(
            "'",
            Identifier(user)[1..^1],
            "'@'%'");

    private static async Task ExecuteAsync(
        MySqlConnection connection,
        string commandText)
    {
        await using MySqlCommand command =
            connection.CreateCommand();
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
