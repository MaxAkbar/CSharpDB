using CSharpDB.Migration;
using CSharpDB.Migration.MySql;

namespace CSharpDB.Migration.MySql.Tests;

public sealed class MySqlCatalogBuilderTests
{
    private const string GoldenCatalogDigest =
        "7d337296bbc697c802caf7e18c20034740590b1b1a51c6f6d06594f70304da9c";
    private const string GoldenSourceFingerprint =
        "sha256:3a72d66f35a58024055025204f7b507c11575bbe37643ff04f0d58c2c45c7a3f";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void BuildProducesDeterministicValidSecretFreePartialCatalog()
    {
        MySqlCatalogSnapshot snapshot = MySqlTestSnapshot.Create();

        MigrationCatalog first = Build(snapshot);
        MigrationCatalog second = Build(snapshot);

        MigrationContractValidator.ValidateCatalog(first);
        Assert.Equal(
            MigrationArtifactSerializer.SerializeCatalog(first),
            MigrationArtifactSerializer.SerializeCatalog(second));
        Assert.Equal(
            MigrationArtifactSerializer.ComputeCatalogDigest(first),
            MigrationArtifactSerializer.ComputeCatalogDigest(second));
        Assert.Equal(
            GoldenCatalogDigest,
            MigrationArtifactSerializer.ComputeCatalogDigest(first));
        Assert.Equal(MigrationSourceKind.MySql, first.Source.Kind);
        Assert.Equal(
            MigrationConsistencyKind.BestEffort,
            first.Source.Consistency.Kind);
        Assert.Equal("8.0.42", first.Source.SourceVersion);
        Assert.Equal("2.6.1", first.Source.ProviderVersion);
        Assert.StartsWith(
            "mysql-database:",
            first.Source.Identity,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            snapshot.EndpointDigest,
            first.Source.Identity,
            StringComparison.Ordinal);
        Assert.StartsWith(
            "sha256:",
            first.Source.Fingerprint,
            StringComparison.Ordinal);
        Assert.Equal(GoldenSourceFingerprint, first.Source.Fingerprint);

        string serialized = MigrationArtifactSerializer.SerializeCatalog(first);
        Assert.DoesNotContain(
            MySqlTestSnapshot.SecretGenerationExpression,
            serialized,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "NeverPersistThisMySqlExpression",
            serialized,
            StringComparison.Ordinal);

        MigrationCatalogObject database = Assert.Single(
            first.Objects,
            static item => item.Kind == MigrationObjectKind.Database);
        Assert.Equal(
            MySqlCatalogBuilder.CatalogContract,
            Facet(database, "mysqlCatalogContract"));
        Assert.Equal("8.0.42", Facet(database, "mysqlServerVersion"));
        Assert.Equal("0", Facet(database, "mysqlLowerCaseTableNames"));
        Assert.Equal(
            "true",
            Facet(database, "mysqlShowGeneratedInvisiblePrimaryKey"));
        Assert.Equal("2", Facet(database, "mysqlBaseTableCount"));
        Assert.Equal("12", Facet(database, "mysqlColumnCount"));

        string[] rules = first.Diagnostics
            .Select(static item => item.RuleId)
            .ToArray();
        Assert.Contains("MIG-MYSQL-INVENTORY-PARTIAL-001", rules);
        Assert.Contains(
            "MIG-MYSQL-METADATA-COMPLETENESS-UNKNOWN-001",
            rules);
        Assert.Contains(
            "MIG-MYSQL-LIVE-QUALIFICATION-PENDING-001",
            rules);
        Assert.DoesNotContain(
            "MIG-MYSQL-SERVER-VARIANT-UNQUALIFIED-001",
            rules);
        Assert.DoesNotContain(
            "MIG-MYSQL-VERSION-UNQUALIFIED-001",
            rules);

        MigrationPlan plan = new MigrationPlanner().CreatePlan(first);
        MigrationPlanReadiness readiness =
            MigrationPlanReadinessValidator.Evaluate(plan, first);
        Assert.Equal(MigrationPlanReadinessStatus.Blocked, readiness.Status);
        Assert.Contains(
            first.Diagnostics.Single(static item =>
                item.RuleId == "MIG-MYSQL-INVENTORY-PARTIAL-001").DiagnosticId,
            readiness.BlockingDiagnosticIds);
    }

    [Fact]
    public void BuildIsIndependentOfProviderRowOrder()
    {
        MySqlCatalogSnapshot ordered = MySqlTestSnapshot.Create();
        var reversed = new MySqlCatalogSnapshot(
            ordered.EndpointDigest,
            ordered.ProviderVersion,
            ordered.Server,
            ordered.Session,
            ordered.Database,
            ordered.Tables.Reverse(),
            ordered.Columns.Reverse());

        Assert.Equal(
            MigrationArtifactSerializer.SerializeCatalog(Build(ordered)),
            MigrationArtifactSerializer.SerializeCatalog(Build(reversed)));
    }

    [Fact]
    public void BuildMapsFoundationScalarTypesWithoutOverstatingNativeTypes()
    {
        MigrationCatalog catalog = Build(
            MySqlTestSnapshot.Create(
                columns:
                [
                    .. MySqlTestSnapshot.Columns(),
                    MySqlTestSnapshot.Column(
                        "Orders",
                        12,
                        "Flags",
                        "bit",
                        nullable: false,
                        columnType: "bit(8)"),
                    MySqlTestSnapshot.Column(
                        "Orders",
                        13,
                        "Duration",
                        "time",
                        nullable: false,
                        dateTimePrecision: 6),
                ]));

        AssertColumn(catalog, "Id", "signedInteger", "bigint");
        AssertColumn(
            catalog,
            "UnsignedId",
            "unsignedInteger",
            "bigint unsigned");
        MigrationCatalogObject amount =
            AssertColumn(catalog, "Amount", "decimal", "decimal(18,2)");
        Assert.Equal("18", Facet(amount, "precision"));
        Assert.Equal("2", Facet(amount, "scale"));
        MigrationCatalogObject enabled =
            AssertColumn(catalog, "Enabled", "boolean", "tinyint(1)");
        Assert.Equal("true", Facet(enabled, "mysqlTinyIntOne"));
        MigrationCatalogObject createdAt =
            AssertColumn(catalog, "CreatedAt", "dateTime", "datetime(6)");
        Assert.Equal("6", Facet(createdAt, "fractionalSeconds"));
        MigrationCatalogObject customer =
            AssertColumn(catalog, "Customer", "text", "varchar(100)");
        Assert.Equal("100", Facet(customer, "maxLength"));
        AssertColumn(catalog, "Payload", "binary", "varbinary(64)");
        AssertColumn(catalog, "Document", "json", "json");
        AssertColumn(catalog, "Location", "native", "geometry");
        MigrationCatalogObject flags =
            AssertColumn(catalog, "Flags", "native", "bit");
        MigrationCatalogObject duration =
            AssertColumn(catalog, "Duration", "native", "time(6)");

        Assert.Contains(
            catalog.Diagnostics,
            item =>
                item.ObjectId == enabled.ObjectId &&
                item.RuleId ==
                    "MIG-MYSQL-TINYINT-BOOLEAN-SEMANTICS-001" &&
                !item.CanOverride);
        foreach (string name in new[] { "Document", "Location" })
        {
            MigrationCatalogObject column = FindColumn(catalog, name);
            Assert.Contains(
                catalog.Diagnostics,
                item =>
                    item.ObjectId == column.ObjectId &&
                    item.RuleId == "MIG-MYSQL-TYPE-UNSUPPORTED-001" &&
                    !item.CanOverride);
        }
        Assert.Contains(
            catalog.Diagnostics,
            item =>
                item.ObjectId == flags.ObjectId &&
                item.RuleId == "MIG-MYSQL-BIT-SEMANTICS-001" &&
                item.Status == MigrationCompatibilityStatus.Unsupported &&
                !item.CanOverride);
        Assert.Contains(
            catalog.Diagnostics,
            item =>
                item.ObjectId == duration.ObjectId &&
                item.RuleId == "MIG-MYSQL-TIME-SEMANTICS-001" &&
                item.Status == MigrationCompatibilityStatus.Unsupported &&
                !item.CanOverride);
    }

    [Theory]
    [InlineData("8.0.42", "MySQL Community Server - GPL")]
    [InlineData("8.4.5", "MySQL Enterprise Server - Commercial")]
    public void OracleMySqlCandidateLanesAvoidVariantAndVersionDiagnostics(
        string version,
        string versionComment)
    {
        MySqlServerMetadata server = MySqlTestSnapshot.Server() with
        {
            Version = version,
            VersionComment = versionComment,
        };

        MigrationCatalog catalog = Build(
            MySqlTestSnapshot.Create(server: server));

        Assert.DoesNotContain(
            catalog.Diagnostics,
            static item =>
                item.RuleId is
                    "MIG-MYSQL-SERVER-VARIANT-UNQUALIFIED-001" or
                    "MIG-MYSQL-VERSION-UNQUALIFIED-001");
        Assert.Contains(
            catalog.Diagnostics,
            static item =>
                item.RuleId ==
                    "MIG-MYSQL-LIVE-QUALIFICATION-PENDING-001" &&
                !item.CanOverride);
    }

    [Theory]
    [InlineData("10.11.8-MariaDB", "mariadb.org binary distribution")]
    [InlineData("8.0.mysql_aurora.3.08.0", "Amazon Aurora MySQL")]
    [InlineData("8.0.42-33", "Percona Server (GPL)")]
    [InlineData("8.0.33-TiDB", "TiDB Server")]
    [InlineData("8.0.30", "Vitess")]
    [InlineData("8.4.5", "MySQL HeatWave Service")]
    [InlineData("8.0.42", "Compatible SQL Server")]
    public void CompatibleServerVariantsAreExplicitNonOverrideableBlockers(
        string version,
        string versionComment)
    {
        MySqlServerMetadata server = MySqlTestSnapshot.Server() with
        {
            Version = version,
            VersionComment = versionComment,
        };

        MigrationCatalog catalog = Build(
            MySqlTestSnapshot.Create(server: server));

        Assert.Contains(
            catalog.Diagnostics,
            static item =>
                item.RuleId ==
                    "MIG-MYSQL-SERVER-VARIANT-UNQUALIFIED-001" &&
                item.Status == MigrationCompatibilityStatus.Unsupported &&
                !item.CanOverride);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(null)]
    public void MissingGeneratedInvisiblePrimaryKeyVisibilityIsBlocked(
        bool? visibility)
    {
        MySqlServerMetadata server = MySqlTestSnapshot.Server() with
        {
            ShowGeneratedInvisiblePrimaryKey = visibility,
        };

        MigrationCatalog catalog = Build(
            MySqlTestSnapshot.Create(server: server));

        Assert.Contains(
            catalog.Diagnostics,
            static item =>
                item.RuleId == "MIG-MYSQL-GIPK-VISIBILITY-001" &&
                item.Status == MigrationCompatibilityStatus.Unknown &&
                !item.CanOverride);
    }

    [Theory]
    [InlineData("5.7.44")]
    [InlineData("8.1.0")]
    [InlineData("8.2.0")]
    [InlineData("9.0.1")]
    [InlineData("not-a-version")]
    public void UnsupportedOracleVersionFamiliesAreExplicitBlockers(
        string version)
    {
        MySqlServerMetadata server = MySqlTestSnapshot.Server() with
        {
            Version = version,
        };

        MigrationCatalog catalog = Build(
            MySqlTestSnapshot.Create(server: server));

        Assert.Contains(
            catalog.Diagnostics,
            static item =>
                item.RuleId == "MIG-MYSQL-VERSION-UNQUALIFIED-001" &&
                item.Status == MigrationCompatibilityStatus.Unsupported &&
                !item.CanOverride);
    }

    [Fact]
    public void DeferredTableAndColumnFeaturesRemainExplicitBlockers()
    {
        MySqlTableMetadata table = MySqlTestSnapshot.Table(
            "Orders",
            engine: "MyISAM",
            partitioned: true);
        MySqlDatabaseMetadata database = MySqlTestSnapshot.Database() with
        {
            ViewCount = 1,
        };
        MySqlServerMetadata server = MySqlTestSnapshot.Server() with
        {
            LowerCaseTableNames = 1,
        };
        MySqlColumnMetadata generated = MySqlTestSnapshot.Column(
            "Orders",
            1,
            "GeneratedValue",
            "int",
            generated: true,
            generationKind: "VIRTUAL GENERATED",
            generationExpression: "`SourceValue` + 1");
        MySqlColumnMetadata invisible = MySqlTestSnapshot.Column(
            "Orders",
            2,
            "InvisibleValue",
            "varchar",
            characterMaximumLength: 20,
            characterSetName: "utf8mb4",
            collationName: "utf8mb4_0900_ai_ci",
            invisible: true);

        MigrationCatalog catalog = Build(MySqlTestSnapshot.Create(
            server: server,
            database: database,
            tables: [table],
            columns: [generated, invisible]));
        string[] rules = catalog.Diagnostics
            .Select(static item => item.RuleId)
            .ToArray();

        Assert.Contains("MIG-MYSQL-IDENTIFIER-CASE-SEMANTICS-001", rules);
        Assert.Contains("MIG-MYSQL-VIEW-INVENTORY-DEFERRED-001", rules);
        Assert.Contains("MIG-MYSQL-STORAGE-ENGINE-UNQUALIFIED-001", rules);
        Assert.Contains("MIG-MYSQL-PARTITIONING-DEFERRED-001", rules);
        Assert.Contains("MIG-MYSQL-GENERATED-COLUMN-DEFERRED-001", rules);
        Assert.Contains("MIG-MYSQL-INVISIBLE-COLUMN-DEFERRED-001", rules);
        Assert.Contains("MIG-MYSQL-COLLATION-UNANALYZED-001", rules);
        Assert.All(
            catalog.Diagnostics.Where(static item =>
                item.RuleId is
                    "MIG-MYSQL-IDENTIFIER-CASE-SEMANTICS-001" or
                    "MIG-MYSQL-VIEW-INVENTORY-DEFERRED-001" or
                    "MIG-MYSQL-STORAGE-ENGINE-UNQUALIFIED-001" or
                    "MIG-MYSQL-PARTITIONING-DEFERRED-001" or
                    "MIG-MYSQL-GENERATED-COLUMN-DEFERRED-001" or
                    "MIG-MYSQL-INVISIBLE-COLUMN-DEFERRED-001" or
                    "MIG-MYSQL-COLLATION-UNANALYZED-001"),
            static item => Assert.False(item.CanOverride));

        MigrationCatalogObject generatedObject =
            FindColumn(catalog, "GeneratedValue");
        Assert.Equal(
            "VIRTUAL GENERATED",
            Facet(generatedObject, "mysqlGenerationKind"));
        Assert.Equal(
            "17",
            Facet(generatedObject, "mysqlGenerationExpressionBytes"));
        Assert.StartsWith(
            "sha256:",
            Facet(generatedObject, "mysqlGenerationExpressionDigest"),
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "`SourceValue` + 1",
            MigrationArtifactSerializer.SerializeCatalog(catalog),
            StringComparison.Ordinal);
    }

    [Fact]
    public void BuildFailsClosedForRequestCancellationAndEveryBound()
    {
        MySqlCatalogSnapshot snapshot = MySqlTestSnapshot.Create();

        Assert.Throws<NotSupportedException>(
            () => Build(
                snapshot,
                request: Request() with { IncludeProfile = true }));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => Build(
                snapshot,
                request: Request() with { ProfileSampleSize = 0 }));
        Assert.Throws<NotSupportedException>(
            () => Build(
                snapshot,
                request: Request() with
                {
                    TargetCSharpDbVersion = "999.0.0",
                }));

        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        Assert.ThrowsAny<OperationCanceledException>(
            () => MySqlCatalogBuilder.Build(
                snapshot,
                Request(),
                MySqlInspectionLimits.Default,
                canceled.Token));

        Assert.Throws<MySqlMigrationException>(
            () => Build(
                snapshot,
                limits: MySqlInspectionLimits.Default with { MaxTables = 1 }));
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                snapshot,
                limits: MySqlInspectionLimits.Default with { MaxColumns = 1 }));
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                MySqlTestSnapshot.Create(
                    database: MySqlTestSnapshot.Database() with
                    {
                        ViewCount = 2,
                    }),
                limits: MySqlInspectionLimits.Default with { MaxViews = 1 }));
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                snapshot,
                limits: MySqlInspectionLimits.Default with
                {
                    MaxNameBytes = 4,
                }));
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                snapshot,
                limits: MySqlInspectionLimits.Default with
                {
                    MaxColumnTypeBytes = 4,
                }));
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                snapshot,
                limits: MySqlInspectionLimits.Default with
                {
                    MaxExpressionBytes = 8,
                }));
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                snapshot,
                limits: MySqlInspectionLimits.Default with
                {
                    MaxExpressionBytesTotal = 8,
                }));
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                snapshot,
                limits: MySqlInspectionLimits.Default with
                {
                    MaxMetadataBytes = 8,
                }));
    }

    [Theory]
    [MemberData(nameof(InvalidSnapshots))]
    public void BuildRejectsInvalidOwnershipAndProviderShapes(object value)
    {
        MySqlCatalogSnapshot snapshot =
            Assert.IsType<MySqlCatalogSnapshot>(value);

        Assert.Throws<MySqlMigrationException>(() => Build(snapshot));
    }

    public static IEnumerable<object[]> InvalidSnapshots()
    {
        MySqlCatalogSnapshot baseline = MySqlTestSnapshot.Create();
        MySqlColumnMetadata firstColumn = baseline.Columns[0];
        yield return
        [
            MySqlTestSnapshot.Create(
                tables:
                [
                    baseline.Tables[0] with { SchemaName = "OtherDb" },
                    baseline.Tables[1],
                ]),
        ];
        yield return
        [
            MySqlTestSnapshot.Create(
                columns:
                [
                    firstColumn with { TableName = "MissingTable" },
                    .. baseline.Columns.Skip(1),
                ]),
        ];
        yield return
        [
            MySqlTestSnapshot.Create(
                columns:
                [
                    firstColumn with
                    {
                        ColumnTypeBytes = firstColumn.ColumnTypeBytes + 1,
                    },
                    .. baseline.Columns.Skip(1),
                ]),
        ];
        yield return
        [
            MySqlTestSnapshot.Create(
                columns:
                [
                    firstColumn,
                    firstColumn with { Name = "DuplicateOrdinal" },
                    .. baseline.Columns.Skip(1),
                ]),
        ];
        yield return
        [
            MySqlTestSnapshot.Create(
                columns:
                [
                    firstColumn with
                    {
                        NumericPrecision = 2,
                        NumericScale = 3,
                    },
                    .. baseline.Columns.Skip(1),
                ]),
        ];
        yield return
        [
            MySqlTestSnapshot.Create(
                columns:
                [
                    firstColumn with
                    {
                        IsGenerated = true,
                        GenerationKind = "NEVER",
                    },
                    .. baseline.Columns.Skip(1),
                ]),
        ];
        yield return
        [
            new MySqlCatalogSnapshot(
                "not-a-digest",
                baseline.ProviderVersion,
                baseline.Server,
                baseline.Session,
                baseline.Database,
                baseline.Tables,
                baseline.Columns),
        ];
        yield return
        [
            MySqlTestSnapshot.Create(
                server: baseline.Server with { LowerCaseTableNames = 3 }),
        ];
    }

    [Fact]
    public void SourceFingerprintBindsRetainedMetadataButNotProviderRowOrder()
    {
        MySqlCatalogSnapshot baseline = MySqlTestSnapshot.Create();
        string expected = Build(baseline).Source.Fingerprint;
        MySqlColumnMetadata amount = baseline.Columns.Single(static item =>
            item.Name == "Amount");
        MySqlCatalogSnapshot changed = MySqlTestSnapshot.Create(
            columns:
            [
                .. baseline.Columns.Where(static item => item.Name != "Amount"),
                amount with { NumericScale = 3 },
            ]);
        MySqlCatalogSnapshot reversed = new(
            baseline.EndpointDigest,
            baseline.ProviderVersion,
            baseline.Server,
            baseline.Session,
            baseline.Database,
            baseline.Tables.Reverse(),
            baseline.Columns.Reverse());

        Assert.NotEqual(expected, Build(changed).Source.Fingerprint);
        Assert.Equal(expected, Build(reversed).Source.Fingerprint);
    }

    [Fact]
    public void SameLengthEnumDeclarationMutationChangesCatalogAndFingerprint()
    {
        const string firstDeclaration = "enum('red','blue')";
        const string secondDeclaration = "enum('red','cyan')";
        Assert.Equal(
            System.Text.Encoding.UTF8.GetByteCount(firstDeclaration),
            System.Text.Encoding.UTF8.GetByteCount(secondDeclaration));
        MySqlColumnMetadata firstEnum = MySqlTestSnapshot.Column(
            "Orders",
            1,
            "Status",
            "enum",
            nullable: false,
            columnType: firstDeclaration);
        MySqlColumnMetadata secondEnum = firstEnum with
        {
            ColumnType = secondDeclaration,
        };
        MigrationCatalog first = Build(
            MySqlTestSnapshot.Create(
                tables: [MySqlTestSnapshot.Table("Orders")],
                columns: [firstEnum]));
        MigrationCatalog second = Build(
            MySqlTestSnapshot.Create(
                tables: [MySqlTestSnapshot.Table("Orders")],
                columns: [secondEnum]));
        MigrationCatalogObject firstColumn = FindColumn(first, "Status");
        MigrationCatalogObject secondColumn = FindColumn(second, "Status");

        Assert.Equal(
            Facet(firstColumn, "mysqlColumnTypeBytes"),
            Facet(secondColumn, "mysqlColumnTypeBytes"));
        Assert.NotEqual(
            Facet(firstColumn, "mysqlColumnTypeDigest"),
            Facet(secondColumn, "mysqlColumnTypeDigest"));
        Assert.NotEqual(
            first.Source.Fingerprint,
            second.Source.Fingerprint);
        Assert.NotEqual(
            MigrationArtifactSerializer.ComputeCatalogDigest(first),
            MigrationArtifactSerializer.ComputeCatalogDigest(second));
        string serialized =
            MigrationArtifactSerializer.SerializeCatalog(first) +
            MigrationArtifactSerializer.SerializeCatalog(second);
        Assert.DoesNotContain(
            firstDeclaration,
            serialized,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            secondDeclaration,
            serialized,
            StringComparison.Ordinal);
    }

    private static MigrationCatalog Build(
        MySqlCatalogSnapshot snapshot,
        MigrationInspectionRequest? request = null,
        MySqlInspectionLimits? limits = null) =>
        MySqlCatalogBuilder.Build(
            snapshot,
            request ?? Request(),
            limits ?? MySqlInspectionLimits.Default,
            Ct);

    private static MigrationInspectionRequest Request() =>
        new()
        {
            TargetCSharpDbVersion =
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
        };

    private static MigrationCatalogObject AssertColumn(
        MigrationCatalog catalog,
        string name,
        string logicalType,
        string nativeType)
    {
        MigrationCatalogObject column = FindColumn(catalog, name);
        Assert.Equal(logicalType, Facet(column, "logicalType"));
        Assert.Equal(nativeType, column.NativeType);
        return column;
    }

    private static MigrationCatalogObject FindColumn(
        MigrationCatalog catalog,
        string name) =>
        Assert.Single(
            catalog.Objects,
            item =>
                item.Kind == MigrationObjectKind.Column &&
                item.SourceName == name);

    private static string? Facet(
        MigrationCatalogObject item,
        string name) =>
        item.Facets.SingleOrDefault(facet =>
            string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal))?.Value;
}
