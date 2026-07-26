using CSharpDB.Migration;
using CSharpDB.Migration.MySql;

namespace CSharpDB.Migration.MySql.Tests;

public sealed class MySqlCatalogBuilderTests
{
    private const string GoldenCatalogDigest =
        "ba6abab6a480bf4f7fa1d043477bd67d690b9b772b59c7034ea4576d98b5e200";
    private const string GoldenSourceFingerprint =
        "sha256:f109bb449e192312533704d8e2bde3a18dc23700e420c4c3ab4cbedfe9aa0266";

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
        string goldenDigest = MigrationArtifactSerializer.ComputeCatalogDigest(first);
        Assert.True(
            string.Equals(GoldenCatalogDigest, goldenDigest, StringComparison.Ordinal),
            $"MySQL catalog golden digest changed. Actual value: {goldenDigest}");
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
            ordered.Columns.Reverse(),
            ordered.TableDefinitions.Reverse(),
            ordered.Keys.Reverse(),
            ordered.KeyColumns.Reverse(),
            ordered.ForeignKeys.Reverse(),
            ordered.ForeignKeyColumns.Reverse(),
            ordered.Checks.Reverse(),
            ordered.Indexes.Reverse(),
            ordered.IndexParts.Reverse());

        Assert.Equal(
            MigrationArtifactSerializer.SerializeCatalog(Build(ordered)),
            MigrationArtifactSerializer.SerializeCatalog(Build(reversed)));
    }

    [Fact]
    public void BuildInventoriesRelationalObjectsWithoutPersistingRawSql()
    {
        MigrationCatalog catalog = Build(MySqlTestSnapshot.Create());

        MigrationCatalogObject database = Assert.Single(
            catalog.Objects,
            static item => item.Kind == MigrationObjectKind.Database);
        Assert.Equal("true", Facet(database, "mysqlSqlQuoteShowCreate"));
        Assert.Equal("2", Facet(database, "mysqlTableDefinitionCount"));
        Assert.Equal("3", Facet(database, "mysqlKeyCount"));
        Assert.Equal("4", Facet(database, "mysqlKeyColumnCount"));
        Assert.Equal("1", Facet(database, "mysqlForeignKeyCount"));
        Assert.Equal("1", Facet(database, "mysqlForeignKeyColumnCount"));
        Assert.Equal("1", Facet(database, "mysqlCheckCount"));
        Assert.Equal("4", Facet(database, "mysqlIndexCount"));
        Assert.Equal("6", Facet(database, "mysqlIndexPartCount"));

        MigrationCatalogObject uniqueKey = FindObject(
            catalog,
            MigrationObjectKind.Key,
            "UQ_Orders_Amount_Customer");
        Assert.Equal(
            ["Amount", "Customer"],
            MemberNames(
                catalog,
                uniqueKey,
                MigrationObjectReferenceRoles.Column));
        Assert.Equal(
            [0, 1],
            uniqueKey.Members
                .Where(static item =>
                    item.Role == MigrationObjectReferenceRoles.Column)
                .Select(static item => item.Ordinal));

        MigrationCatalogObject ordinaryIndex = FindObject(
            catalog,
            MigrationObjectKind.Index,
            "IX_Orders_Amount_Customer");
        Assert.Equal("standard", Facet(ordinaryIndex, "kind"));
        Assert.Equal(
            ["Amount", "Customer"],
            MemberNames(
                catalog,
                ordinaryIndex,
                MigrationObjectReferenceRoles.Column));
        Assert.Equal(
            [0, 1],
            ordinaryIndex.Members.Select(static item => item.Ordinal));

        Assert.DoesNotContain(
            catalog.Objects,
            static item =>
                item.Kind == MigrationObjectKind.Index &&
                item.SourceName is "PRIMARY" or
                    "UQ_Orders_Amount_Customer");

        MigrationCatalogObject foreignKey = FindObject(
            catalog,
            MigrationObjectKind.ForeignKey,
            "FK_Archive_Orders");
        Assert.Equal(
            ["ArchiveId"],
            MemberNames(
                catalog,
                foreignKey,
                MigrationObjectReferenceRoles.SourceColumn));
        MigrationObjectReference referencedKey = Assert.Single(
            foreignKey.Members,
            static item =>
                item.Role == MigrationObjectReferenceRoles.ReferencedKey);
        MigrationCatalogObject referencedObject = Assert.Single(
            catalog.Objects,
            item => item.ObjectId == referencedKey.ObjectId);
        Assert.Equal(MigrationObjectKind.Key, referencedObject.Kind);
        Assert.Equal("PRIMARY", referencedObject.SourceName);
        MigrationCatalogObject referencedTable = Assert.Single(
            catalog.Objects,
            item => item.ObjectId == referencedObject.ParentObjectId);
        Assert.Equal("Orders", referencedTable.SourceName);
        Assert.Equal("cascade", Facet(foreignKey, "onDelete"));

        MigrationCatalogObject check = FindObject(
            catalog,
            MigrationObjectKind.CheckConstraint,
            "CK_Orders_Amount");
        Assert.StartsWith(
            "sha256:",
            Facet(check, "mysqlCheckClauseDigest"),
            StringComparison.Ordinal);
        Assert.Equal(
            System.Text.Encoding.UTF8.GetByteCount(
                MySqlTestSnapshot.SecretCheckClause).ToString(
                    System.Globalization.CultureInfo.InvariantCulture),
            Facet(check, "mysqlCheckClauseSourceBytes"));
        Assert.Contains(
            catalog.Diagnostics,
            item =>
                item.ObjectId == check.ObjectId &&
                item.RuleId == "MIG-MYSQL-CHECK-INVENTORY-ONLY-001" &&
                item.Status == MigrationCompatibilityStatus.Unknown &&
                !item.CanOverride);

        MigrationCatalogObject orders = FindObject(
            catalog,
            MigrationObjectKind.Table,
            "Orders");
        Assert.StartsWith(
            "sha256:",
            Facet(orders, "mysqlShowCreateDigest"),
            StringComparison.Ordinal);
        Assert.NotNull(Facet(orders, "mysqlShowCreateSourceBytes"));

        string serialized =
            MigrationArtifactSerializer.SerializeCatalog(catalog);
        Assert.DoesNotContain(
            MySqlTestSnapshot.SecretCheckClause,
            serialized,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            MySqlTestSnapshot.SecretFunctionalIndexExpression,
            serialized,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            MySqlTestSnapshot.SecretShowCreateMarker,
            serialized,
            StringComparison.Ordinal);
    }

    [Fact]
    public void CompositeForeignKeyMembersPreserveProviderOrderAndExactBinding()
    {
        MySqlCatalogSnapshot baseline = MySqlTestSnapshot.Create();
        MySqlForeignKeyMetadata composite =
            MySqlTestSnapshot.ForeignKey(
                tableName: "Orders",
                name: "FK_Orders_Amount_Customer",
                referencedTableName: "Orders",
                uniqueConstraintName: "UQ_Orders_Amount_Customer");
        MySqlForeignKeyColumnMetadata[] compositeColumns =
        [
            MySqlTestSnapshot.ForeignKeyColumn(
                tableName: "Orders",
                constraintName: composite.Name,
                ordinal: 1,
                columnName: "Amount",
                referencedTableName: "Orders",
                referencedColumnName: "Amount",
                positionInUniqueConstraint: 1),
            MySqlTestSnapshot.ForeignKeyColumn(
                tableName: "Orders",
                constraintName: composite.Name,
                ordinal: 2,
                columnName: "Customer",
                referencedTableName: "Orders",
                referencedColumnName: "Customer",
                positionInUniqueConstraint: 2),
        ];

        MigrationCatalog catalog = Build(MySqlTestSnapshot.Create(
            foreignKeys: [.. baseline.ForeignKeys, composite],
            foreignKeyColumns:
            [
                .. baseline.ForeignKeyColumns,
                .. compositeColumns,
            ]));
        MigrationCatalogObject foreignKey = FindObject(
            catalog,
            MigrationObjectKind.ForeignKey,
            composite.Name);

        Assert.Equal(
            ["Amount", "Customer"],
            MemberNames(
                catalog,
                foreignKey,
                MigrationObjectReferenceRoles.SourceColumn));
        Assert.Equal(
            [0, 1],
            foreignKey.Members
                .Where(static item =>
                    item.Role == MigrationObjectReferenceRoles.SourceColumn)
                .Select(static item => item.Ordinal));
        MigrationObjectReference referenced = Assert.Single(
            foreignKey.Members,
            static item =>
                item.Role == MigrationObjectReferenceRoles.ReferencedKey);
        MigrationCatalogObject referencedKey = Assert.Single(
            catalog.Objects,
            item => item.ObjectId == referenced.ObjectId);
        Assert.Equal(
            "UQ_Orders_Amount_Customer",
            referencedKey.SourceName);
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

    [Fact]
    public void IndexVariantsAndStandaloneUniqueIndexesAreExplicitBlockers()
    {
        MySqlIndexMetadata[] variants =
        [
            MySqlTestSnapshot.Index("Orders", "IX_Orders_Functional"),
            MySqlTestSnapshot.Index("Orders", "IX_Orders_Prefix"),
            MySqlTestSnapshot.Index("Orders", "IX_Orders_Descending"),
            MySqlTestSnapshot.Index(
                "Orders",
                "IX_Orders_Invisible",
                visible: false),
            MySqlTestSnapshot.Index(
                "Orders",
                "FT_Orders_Customer",
                indexType: "FULLTEXT"),
            MySqlTestSnapshot.Index(
                "Orders",
                "UX_Orders_Amount",
                unique: true),
        ];
        MySqlIndexPartMetadata[] variantParts =
        [
            MySqlTestSnapshot.IndexPart(
                "Orders",
                "IX_Orders_Functional",
                1,
                expression:
                    MySqlTestSnapshot.SecretFunctionalIndexExpression),
            MySqlTestSnapshot.IndexPart(
                "Orders",
                "IX_Orders_Prefix",
                1,
                columnName: "Customer",
                prefixLength: 12),
            MySqlTestSnapshot.IndexPart(
                "Orders",
                "IX_Orders_Descending",
                1,
                columnName: "Amount",
                sortDirection: "D"),
            MySqlTestSnapshot.IndexPart(
                "Orders",
                "IX_Orders_Invisible",
                1,
                columnName: "Amount"),
            MySqlTestSnapshot.IndexPart(
                "Orders",
                "FT_Orders_Customer",
                1,
                columnName: "Customer"),
            MySqlTestSnapshot.IndexPart(
                "Orders",
                "UX_Orders_Amount",
                1,
                columnName: "Amount"),
        ];
        MigrationCatalog catalog = Build(MySqlTestSnapshot.Create(
            indexes: [.. MySqlTestSnapshot.Indexes(), .. variants],
            indexParts: [.. MySqlTestSnapshot.IndexParts(), .. variantParts]));

        foreach (MySqlIndexMetadata variant in variants)
        {
            MigrationCatalogObject index = FindObject(
                catalog,
                MigrationObjectKind.Index,
                variant.Name);
            Assert.Contains(
                catalog.Diagnostics,
                item =>
                    item.ObjectId == index.ObjectId &&
                    item.RuleId ==
                        "MIG-MYSQL-INDEX-SHAPE-UNSUPPORTED-001" &&
                    item.Status ==
                        MigrationCompatibilityStatus.Unsupported &&
                    !item.CanOverride);
        }

        MigrationCatalogObject functional = FindObject(
            catalog,
            MigrationObjectKind.Index,
            "IX_Orders_Functional");
        Assert.Empty(functional.Members);
        Assert.StartsWith(
            "sha256:",
            Facet(functional, "mysqlIndexExpressionDigest"),
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            MySqlTestSnapshot.SecretFunctionalIndexExpression,
            MigrationArtifactSerializer.SerializeCatalog(catalog),
            StringComparison.Ordinal);
    }

    [Fact]
    public void UnsupportedKeyAndForeignKeySupportShapesFailClosed()
    {
        IReadOnlyList<MySqlIndexMetadata> indexes =
        [
            .. MySqlTestSnapshot.Indexes()
                .Select(static item =>
                    item.Name == "UQ_Orders_Amount_Customer"
                        ? item with { IsVisible = false }
                        : item),
            MySqlTestSnapshot.Index("Archive", "IX_Archive_OrderSupport"),
        ];
        IReadOnlyList<MySqlIndexPartMetadata> indexParts =
        [
            .. MySqlTestSnapshot.IndexParts(),
            MySqlTestSnapshot.IndexPart(
                "Archive",
                "IX_Archive_OrderSupport",
                1,
                columnName: "ArchiveId"),
        ];

        MigrationCatalog catalog = Build(MySqlTestSnapshot.Create(
            indexes: indexes,
            indexParts: indexParts));

        Assert.Contains(
            catalog.Diagnostics,
            static item =>
                item.RuleId ==
                    "MIG-MYSQL-KEY-BACKING-INDEX-UNSUPPORTED-001" &&
                !item.CanOverride);
        Assert.Contains(
            catalog.Diagnostics,
            static item =>
                item.RuleId ==
                    "MIG-MYSQL-FK-SUPPORT-AMBIGUOUS-001" &&
                !item.CanOverride);
    }

    [Fact]
    public void UnenforcedChecksRemainDistinctUnsupportedEvidence()
    {
        MySqlCheckMetadata check =
            Assert.Single(MySqlTestSnapshot.Checks()) with
            {
                IsEnforced = false,
            };

        MigrationCatalog catalog = Build(
            MySqlTestSnapshot.Create(checks: [check]));
        MigrationCatalogObject checkObject = FindObject(
            catalog,
            MigrationObjectKind.CheckConstraint,
            check.Name);

        Assert.Contains(
            catalog.Diagnostics,
            item =>
                item.ObjectId == checkObject.ObjectId &&
                item.RuleId ==
                    "MIG-MYSQL-CHECK-NOT-ENFORCED-001" &&
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
    [InlineData(false)]
    [InlineData(null)]
    public void MissingShowCreateQuotingProofIsBlocked(bool? enabled)
    {
        MySqlSessionMetadata session = MySqlTestSnapshot.Session() with
        {
            SqlQuoteShowCreate = enabled,
        };

        MigrationCatalog catalog = Build(
            MySqlTestSnapshot.Create(session: session));

        Assert.Contains(
            catalog.Diagnostics,
            static item =>
                item.RuleId ==
                    "MIG-MYSQL-SHOW-CREATE-QUOTING-UNPROVEN-001" &&
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
            ViewCount = 0,
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
        Assert.Contains("MIG-MYSQL-STORAGE-ENGINE-UNQUALIFIED-001", rules);
        Assert.Contains("MIG-MYSQL-PARTITIONING-DEFERRED-001", rules);
        Assert.Contains("MIG-MYSQL-GENERATED-COLUMN-DEFERRED-001", rules);
        Assert.Contains("MIG-MYSQL-INVISIBLE-COLUMN-DEFERRED-001", rules);
        Assert.Contains("MIG-MYSQL-COLLATION-UNANALYZED-001", rules);
        Assert.All(
            catalog.Diagnostics.Where(static item =>
                item.RuleId is
                    "MIG-MYSQL-IDENTIFIER-CASE-SEMANTICS-001" or
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
                snapshot,
                limits: MySqlInspectionLimits.Default with
                {
                    MaxTableDefinitions = 1,
                }));
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                snapshot,
                limits: MySqlInspectionLimits.Default with { MaxKeys = 1 }));
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                snapshot,
                limits: MySqlInspectionLimits.Default with
                {
                    MaxKeyColumns = 1,
                }));
        MySqlForeignKeyMetadata secondForeignKey =
            MySqlTestSnapshot.ForeignKey(
                tableName: "Archive",
                name: "FK_Archive_Orders_Second",
                referencedTableName: "Orders",
                uniqueConstraintName: "PRIMARY");
        MySqlForeignKeyColumnMetadata secondForeignKeyColumn =
            MySqlTestSnapshot.ForeignKeyColumn(
                tableName: "Archive",
                constraintName: secondForeignKey.Name,
                ordinal: 1,
                columnName: "ArchiveId",
                referencedTableName: "Orders",
                referencedColumnName: "Id",
                positionInUniqueConstraint: 1);
        MySqlCatalogSnapshot expandedForeignKeys = MySqlTestSnapshot.Create(
            foreignKeys:
            [
                .. snapshot.ForeignKeys,
                secondForeignKey,
            ],
            foreignKeyColumns:
            [
                .. snapshot.ForeignKeyColumns,
                secondForeignKeyColumn,
            ]);
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                expandedForeignKeys,
                limits: MySqlInspectionLimits.Default with
                {
                    MaxForeignKeys = 1,
                }));
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                expandedForeignKeys,
                limits: MySqlInspectionLimits.Default with
                {
                    MaxForeignKeyColumns = 1,
                }));
        MySqlCheckMetadata secondCheck = MySqlTestSnapshot.Check(
            "Orders",
            "CK_Orders_Amount_Second",
            "(`Amount` >= 0)");
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                MySqlTestSnapshot.Create(
                    checks:
                    [
                        .. snapshot.Checks,
                        secondCheck,
                    ]),
                limits: MySqlInspectionLimits.Default with
                {
                    MaxChecks = 1,
                }));
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                snapshot,
                limits: MySqlInspectionLimits.Default with
                {
                    MaxIndexes = 1,
                }));
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                snapshot,
                limits: MySqlInspectionLimits.Default with
                {
                    MaxIndexParts = 1,
                }));
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                snapshot,
                limits: MySqlInspectionLimits.Default with
                {
                    MaxStructuralRowsTotal = 1,
                }));
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
                    MaxDefinitionBytes = 8,
                }));
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                snapshot,
                limits: MySqlInspectionLimits.Default with
                {
                    MaxDefinitionBytesTotal = 8,
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
                baseline.Columns,
                baseline.TableDefinitions,
                baseline.Keys,
                baseline.KeyColumns,
                baseline.ForeignKeys,
                baseline.ForeignKeyColumns,
                baseline.Checks,
                baseline.Indexes,
                baseline.IndexParts),
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
            baseline.Columns.Reverse(),
            baseline.TableDefinitions.Reverse(),
            baseline.Keys.Reverse(),
            baseline.KeyColumns.Reverse(),
            baseline.ForeignKeys.Reverse(),
            baseline.ForeignKeyColumns.Reverse(),
            baseline.Checks.Reverse(),
            baseline.Indexes.Reverse(),
            baseline.IndexParts.Reverse());

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

    [Fact]
    public void SameLengthRawRelationalSqlMutationsChangeCatalogAndFingerprint()
    {
        const string firstCheckClause =
            "(`Amount` > 0 /* NeverPersistAlpha */)";
        const string secondCheckClause =
            "(`Amount` > 0 /* NeverPersistBravo */)";
        AssertSameUtf8Length(firstCheckClause, secondCheckClause);
        MySqlCheckMetadata check =
            Assert.Single(MySqlTestSnapshot.Checks());
        AssertRawMutationChangesCatalog(
            MySqlTestSnapshot.Create(
                checks:
                [
                    check with
                    {
                        ClauseBytes =
                            System.Text.Encoding.UTF8.GetByteCount(
                                firstCheckClause),
                        Clause = firstCheckClause,
                    },
                ]),
            MySqlTestSnapshot.Create(
                checks:
                [
                    check with
                    {
                        ClauseBytes =
                            System.Text.Encoding.UTF8.GetByteCount(
                                secondCheckClause),
                        Clause = secondCheckClause,
                    },
                ]),
            firstCheckClause,
            secondCheckClause);

        const string firstShowCreate =
            "CREATE TABLE `Orders` (`Id` bigint) /* NeverPersistAlpha */";
        const string secondShowCreate =
            "CREATE TABLE `Orders` (`Id` bigint) /* NeverPersistBravo */";
        AssertSameUtf8Length(firstShowCreate, secondShowCreate);
        MySqlCatalogSnapshot baseline = MySqlTestSnapshot.Create();
        MySqlTableDefinitionMetadata ordersDefinition =
            baseline.TableDefinitions.Single(static item =>
                item.TableName == "Orders");
        AssertRawMutationChangesCatalog(
            MySqlTestSnapshot.Create(
                tableDefinitions:
                [
                    ordersDefinition with
                    {
                        DefinitionBytes =
                            System.Text.Encoding.UTF8.GetByteCount(
                                firstShowCreate),
                        Definition = firstShowCreate,
                    },
                    .. baseline.TableDefinitions.Where(static item =>
                        item.TableName != "Orders"),
                ]),
            MySqlTestSnapshot.Create(
                tableDefinitions:
                [
                    ordersDefinition with
                    {
                        DefinitionBytes =
                            System.Text.Encoding.UTF8.GetByteCount(
                                secondShowCreate),
                        Definition = secondShowCreate,
                    },
                    .. baseline.TableDefinitions.Where(static item =>
                        item.TableName != "Orders"),
                ]),
            firstShowCreate,
            secondShowCreate);

        const string firstIndexExpression =
            "(lower(`Customer`) /* NeverPersistAlpha */)";
        const string secondIndexExpression =
            "(lower(`Customer`) /* NeverPersistBravo */)";
        AssertSameUtf8Length(firstIndexExpression, secondIndexExpression);
        MySqlIndexMetadata functional =
            MySqlTestSnapshot.Index("Orders", "IX_Orders_Functional");
        MySqlIndexPartMetadata firstPart = MySqlTestSnapshot.IndexPart(
            "Orders",
            functional.Name,
            1,
            expression: firstIndexExpression);
        MySqlIndexPartMetadata secondPart = firstPart with
        {
            ExpressionBytes =
                System.Text.Encoding.UTF8.GetByteCount(secondIndexExpression),
            Expression = secondIndexExpression,
        };
        AssertRawMutationChangesCatalog(
            MySqlTestSnapshot.Create(
                indexes: [.. baseline.Indexes, functional],
                indexParts: [.. baseline.IndexParts, firstPart]),
            MySqlTestSnapshot.Create(
                indexes: [.. baseline.Indexes, functional],
                indexParts: [.. baseline.IndexParts, secondPart]),
            firstIndexExpression,
            secondIndexExpression);
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

    private static MigrationCatalogObject FindObject(
        MigrationCatalog catalog,
        MigrationObjectKind kind,
        string sourceName) =>
        Assert.Single(
            catalog.Objects,
            item =>
                item.Kind == kind &&
                item.SourceName == sourceName);

    private static string[] MemberNames(
        MigrationCatalog catalog,
        MigrationCatalogObject item,
        string role)
    {
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById =
            catalog.Objects.ToDictionary(
                static candidate => candidate.ObjectId,
                StringComparer.Ordinal);
        return item.Members
            .Where(member =>
                string.Equals(member.Role, role, StringComparison.Ordinal))
            .OrderBy(static member => member.Ordinal)
            .Select(member => objectsById[member.ObjectId].SourceName)
            .ToArray();
    }

    private static void AssertRawMutationChangesCatalog(
        MySqlCatalogSnapshot firstSnapshot,
        MySqlCatalogSnapshot secondSnapshot,
        string firstRaw,
        string secondRaw)
    {
        MigrationCatalog first = Build(firstSnapshot);
        MigrationCatalog second = Build(secondSnapshot);

        Assert.NotEqual(first.Source.Fingerprint, second.Source.Fingerprint);
        Assert.NotEqual(
            MigrationArtifactSerializer.ComputeCatalogDigest(first),
            MigrationArtifactSerializer.ComputeCatalogDigest(second));
        string serialized =
            MigrationArtifactSerializer.SerializeCatalog(first) +
            MigrationArtifactSerializer.SerializeCatalog(second);
        Assert.DoesNotContain(firstRaw, serialized, StringComparison.Ordinal);
        Assert.DoesNotContain(secondRaw, serialized, StringComparison.Ordinal);
    }

    private static void AssertSameUtf8Length(string first, string second) =>
        Assert.Equal(
            System.Text.Encoding.UTF8.GetByteCount(first),
            System.Text.Encoding.UTF8.GetByteCount(second));

    private static string? Facet(
        MigrationCatalogObject item,
        string name) =>
        item.Facets.SingleOrDefault(facet =>
            string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal))?.Value;
}
