using CSharpDB.Migration;
using CSharpDB.Migration.SqlServer;

namespace CSharpDB.Migration.SqlServer.Tests;

public sealed class SqlServerCatalogBuilderTests
{
    private const string GoldenCatalogDigest =
        "f61fb1bce66aebba4935b20095bb5ea64e3e851440de1b02f25737941bd6ffda";
    private const string GoldenSourceFingerprint =
        "sha256:04a6f7f9a7cdc31243e246ac5c98c05a2f1535af03e448ec3f684456517797ca";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void BuildProducesDeterministicValidSafePartialCatalog()
    {
        SqlServerCatalogSnapshot snapshot = SqlServerTestSnapshot.Create();

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
        Assert.Equal(GoldenCatalogDigest, goldenDigest);
        Assert.Equal(MigrationSourceKind.SqlServer, first.Source.Kind);
        Assert.Equal(MigrationConsistencyKind.BestEffort, first.Source.Consistency.Kind);
        Assert.Equal("16.0.4175.1", first.Source.SourceVersion);
        Assert.Equal("7.0.2", first.Source.ProviderVersion);
        Assert.StartsWith("sqlserver-database:", first.Source.Identity, StringComparison.Ordinal);
        Assert.DoesNotContain(snapshot.EndpointDigest, first.Source.Identity, StringComparison.Ordinal);
        Assert.StartsWith("sha256:", first.Source.Fingerprint, StringComparison.Ordinal);
        Assert.Equal(GoldenSourceFingerprint, first.Source.Fingerprint);

        string serialized = MigrationArtifactSerializer.SerializeCatalog(first);
        Assert.DoesNotContain(
            SqlServerTestSnapshot.SecretDefaultDefinition,
            serialized,
            StringComparison.Ordinal);
        Assert.DoesNotContain("NeverPersistThis", serialized, StringComparison.Ordinal);

        MigrationCatalogObject database = Assert.Single(
            first.Objects,
            static item => item.Kind == MigrationObjectKind.Database);
        Assert.Equal("complete", Facet(database, "sqlServerMetadataVisibility"));
        Assert.Equal("160", Facet(database, "sqlServerCompatibilityLevel"));

        MigrationCatalogObject amount = FindColumn(first, "Amount");
        Assert.Equal("decimal", Facet(amount, "logicalType"));
        Assert.Equal("18", Facet(amount, "precision"));
        Assert.Equal("2", Facet(amount, "scale"));
        Assert.Equal("source-expression", Facet(amount, "defaultKind"));
        Assert.StartsWith(
            "sha256:",
            Facet(amount, "sqlServerDefaultDefinitionDigest"),
            StringComparison.Ordinal);

        MigrationCatalogObject customer = FindColumn(first, "Customer");
        Assert.Equal("100", Facet(customer, "maxLength"));
        Assert.Equal(
            "Latin1_General_100_CI_AS_SC_UTF8",
            Facet(customer, "sqlServerCollation"));
        Assert.Null(Facet(customer, "collation"));

        Assert.Equal("native", Facet(FindColumn(first, "ComputedAmount"), "logicalType"));
        Assert.Equal("native", Facet(FindColumn(first, "Version"), "logicalType"));
        MigrationCatalogObject aliasCode = FindColumn(first, "AliasCode");
        Assert.Equal("native", Facet(aliasCode, "logicalType"));
        Assert.Equal("dbo.CustomerCode", aliasCode.NativeType);
        Assert.Equal("native", Facet(FindColumn(first, "XmlPayload"), "logicalType"));
        Assert.Equal("true", Facet(aliasCode, "sqlServerUserDefinedType"));

        string[] rules = first.Diagnostics.Select(static item => item.RuleId).ToArray();
        Assert.Contains("MIG-SQLSERVER-INVENTORY-PARTIAL-001", rules);
        Assert.Contains("MIG-SQLSERVER-LIVE-QUALIFICATION-PENDING-001", rules);
        Assert.Contains("MIG-SQLSERVER-DEFAULT-UNANALYZED-001", rules);
        Assert.Contains("MIG-SQLSERVER-COLLATION-UNANALYZED-001", rules);
        Assert.Contains("MIG-SQLSERVER-TYPE-OR-GENERATION-UNSUPPORTED-001", rules);

        IReadOnlyDictionary<string, string> targetNames =
            DeterministicMigrationNameMapper.Map(first);
        MigrationCatalogObject archive = first.Objects.Single(
            static item =>
                item.Kind == MigrationObjectKind.Table &&
                item.SourceName == "Archive");
        Assert.Equal("Sales__Archive", targetNames[archive.ObjectId]);

        MigrationPlan plan = new MigrationPlanner().CreatePlan(first);
        MigrationPlanReadiness readiness =
            MigrationPlanReadinessValidator.Evaluate(plan, first);
        Assert.Equal(MigrationPlanReadinessStatus.Blocked, readiness.Status);
        Assert.Contains(
            first.Diagnostics.Single(static item =>
                item.RuleId == "MIG-SQLSERVER-INVENTORY-PARTIAL-001").DiagnosticId,
            readiness.BlockingDiagnosticIds);
    }

    [Fact]
    public void BuildIsIndependentOfProviderRowOrder()
    {
        SqlServerCatalogSnapshot ordered = SqlServerTestSnapshot.Create();
        var reversed = new SqlServerCatalogSnapshot(
            ordered.EndpointDigest,
            ordered.ProviderVersion,
            ordered.Instance,
            ordered.Database,
            ordered.Schemas.Reverse(),
            ordered.Tables.Reverse(),
            ordered.Columns.Reverse());

        Assert.Equal(
            MigrationArtifactSerializer.SerializeCatalog(Build(ordered)),
            MigrationArtifactSerializer.SerializeCatalog(Build(reversed)));
    }

    [Fact]
    public void BuildDetectsIncompletePermissionsAndUnqualifiedVariants()
    {
        SqlServerDatabaseMetadata database = SqlServerTestSnapshot.Database() with
        {
            CompatibilityLevel = 140,
            IsSysAdmin = false,
            IsDbOwner = false,
            HasControl = false,
            HasViewDefinition = false,
        };
        SqlServerInstanceMetadata instance = SqlServerTestSnapshot.Instance() with
        {
            ProductVersion = "14.0.1000.169",
            ProductMajorVersion = 14,
            Edition = "SQL Azure",
            EngineEdition = 5,
        };

        MigrationCatalog catalog = Build(
            SqlServerTestSnapshot.Create(instance: instance, database: database));
        MigrationCatalogObject databaseObject = Assert.Single(
            catalog.Objects,
            static item => item.Kind == MigrationObjectKind.Database);
        Assert.Equal("incomplete", Facet(databaseObject, "sqlServerMetadataVisibility"));
        string[] rules = catalog.Diagnostics.Select(static item => item.RuleId).ToArray();
        Assert.Contains("MIG-SQLSERVER-METADATA-VISIBILITY-001", rules);
        Assert.Contains("MIG-SQLSERVER-ENGINE-VARIANT-UNQUALIFIED-001", rules);
        Assert.Contains("MIG-SQLSERVER-VERSION-UNQUALIFIED-001", rules);
    }

    [Theory]
    [InlineData(true, false, false)]
    [InlineData(false, true, false)]
    [InlineData(false, false, true)]
    public void BuildTreatsDatabaseLevelPermissionEvidenceAsUnknown(
        bool isDbOwner,
        bool hasControl,
        bool hasViewDefinition)
    {
        SqlServerDatabaseMetadata database = SqlServerTestSnapshot.Database() with
        {
            IsSysAdmin = false,
            IsDbOwner = isDbOwner,
            HasControl = hasControl,
            HasViewDefinition = hasViewDefinition,
        };

        MigrationCatalog catalog = Build(
            SqlServerTestSnapshot.Create(database: database));
        MigrationCatalogObject databaseObject = Assert.Single(
            catalog.Objects,
            static item => item.Kind == MigrationObjectKind.Database);

        Assert.Equal("unknown", Facet(databaseObject, "sqlServerMetadataVisibility"));
        Assert.Contains(
            catalog.Diagnostics,
            static item =>
                item.RuleId == "MIG-SQLSERVER-METADATA-VISIBILITY-001" &&
                item.Status == MigrationCompatibilityStatus.Unknown &&
                !item.CanOverride);
    }

    [Fact]
    public void BuildUsesCaseSensitiveCanonicalSchemaAndTypeNames()
    {
        SqlServerColumnMetadata upperSysAlias = SqlServerTestSnapshot.Column(
            100,
            1,
            "UpperSysAlias",
            "IntAlias",
            "int",
            4,
            10,
            typeSchema: "SYS");
        SqlServerColumnMetadata sysname = SqlServerTestSnapshot.Column(
            100,
            2,
            "SystemName",
            "sysname",
            "nvarchar",
            256,
            0);
        MigrationCatalog catalog = Build(SqlServerTestSnapshot.Create(
            schemas:
            [
                new SqlServerSchemaMetadata(1, "dbo"),
                new SqlServerSchemaMetadata(2, "DBO"),
            ],
            tables: [SqlServerTestSnapshot.OrdinaryTable(100, 1, "Orders")],
            columns: [upperSysAlias, sysname]));

        MigrationCatalogObject lowerDbo = Assert.Single(
            catalog.Objects,
            static item =>
                item.Kind == MigrationObjectKind.Namespace &&
                item.SourceName == "dbo");
        MigrationCatalogObject upperDbo = Assert.Single(
            catalog.Objects,
            static item =>
                item.Kind == MigrationObjectKind.Namespace &&
                item.SourceName == "DBO");
        Assert.Equal("true", Facet(lowerDbo, "isDefault"));
        Assert.Equal("false", Facet(upperDbo, "isDefault"));

        MigrationCatalogObject alias = FindColumn(catalog, "UpperSysAlias");
        Assert.Equal("native", Facet(alias, "logicalType"));
        Assert.Equal("true", Facet(alias, "sqlServerUserDefinedType"));
        Assert.Equal("SYS.IntAlias", alias.NativeType);

        MigrationCatalogObject builtInAlias = FindColumn(catalog, "SystemName");
        Assert.Equal("text", Facet(builtInAlias, "logicalType"));
        Assert.Equal("false", Facet(builtInAlias, "sqlServerUserDefinedType"));
        Assert.Equal("sys.sysname", builtInAlias.NativeType);
    }

    [Fact]
    public void BuildDetectsCompatibilityMismatchWithinIntendedLane()
    {
        SqlServerDatabaseMetadata database = SqlServerTestSnapshot.Database() with
        {
            CompatibilityLevel = 150,
        };

        MigrationCatalog catalog = Build(
            SqlServerTestSnapshot.Create(database: database));

        Assert.Contains(
            catalog.Diagnostics,
            static item =>
                item.RuleId == "MIG-SQLSERVER-COMPATIBILITY-UNQUALIFIED-001" &&
                item.Status == MigrationCompatibilityStatus.Unsupported &&
                !item.CanOverride);
    }

    [Fact]
    public void BuildFailsClosedForProfileCancellationAndBounds()
    {
        SqlServerCatalogSnapshot snapshot = SqlServerTestSnapshot.Create();
        var profile = Request() with { IncludeProfile = true };
        Assert.Throws<NotSupportedException>(
            () => SqlServerCatalogBuilder.Build(
                snapshot,
                profile,
                SqlServerInspectionLimits.Default,
                Ct));

        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        Assert.ThrowsAny<OperationCanceledException>(
            () => SqlServerCatalogBuilder.Build(
                snapshot,
                Request(),
                SqlServerInspectionLimits.Default,
                canceled.Token));

        Assert.Throws<SqlServerMigrationException>(
            () => SqlServerCatalogBuilder.Build(
                snapshot,
                Request(),
                new SqlServerInspectionLimits { MaxColumns = 1 },
                Ct));
        Assert.Throws<SqlServerMigrationException>(
            () => SqlServerCatalogBuilder.Build(
                snapshot,
                Request(),
                new SqlServerInspectionLimits { MaxExpressionBytes = 8 },
                Ct));
    }

    [Theory]
    [MemberData(nameof(InvalidColumnShapes))]
    public void BuildRejectsInvalidColumnMetadata(object value)
    {
        SqlServerColumnMetadata invalid =
            Assert.IsType<SqlServerColumnMetadata>(value);
        SqlServerCatalogSnapshot snapshot = SqlServerTestSnapshot.Create(
            tables: [SqlServerTestSnapshot.OrdinaryTable(100, 1, "Orders")],
            columns: [invalid]);

        Assert.Throws<SqlServerMigrationException>(() => Build(snapshot));
    }

    public static IEnumerable<object[]> InvalidColumnShapes()
    {
        SqlServerColumnMetadata baseline = SqlServerTestSnapshot.Column(
            100,
            1,
            "Value",
            "int",
            "int",
            4,
            10);
        return
        [
            new object[] { baseline with { IsIdentity = true, IdentitySeed = "1", IdentityIncrement = null } },
            new object[]
            {
                baseline with
                {
                    IsIdentity = false,
                    IdentitySeed = "1",
                    IdentityIncrement = "1",
                },
            },
            new object[] { baseline with
            {
                TypeName = "nvarchar",
                SystemTypeName = "nvarchar",
                MaxLength = 3,
            } },
            new object[] { baseline with
            {
                TypeName = "decimal",
                SystemTypeName = "decimal",
                Precision = 2,
                Scale = 3,
            } },
            new object[] { baseline with
            {
                TypeName = "datetime2",
                SystemTypeName = "datetime2",
                Scale = 8,
            } },
            new object[]
            {
                baseline with
                {
                    IsComputed = false,
                    ComputedDefinitionBytes = 22,
                    ComputedDefinition = "([Value]+1)",
                },
            },
            new object[]
            {
                baseline with
                {
                    HasDefault = false,
                    DefaultConstraintName = "DF_Value",
                },
            },
            new object[]
            {
                baseline with
                {
                    HasDefault = true,
                    DefaultDefinitionBytes = -1,
                },
            },
            new object[]
            {
                baseline with
                {
                    HasDefault = true,
                    DefaultDefinitionBytes = 4,
                    DefaultDefinition = "x",
                },
            },
            new object[] { baseline with { IsComputed = false, IsPersisted = true } },
            new object[] { baseline with { XmlCollectionId = -1 } },
        ];
    }

    [Fact]
    public void ProviderSpecificColumnFeaturesRemainExplicitBlockers()
    {
        SqlServerColumnMetadata special = SqlServerTestSnapshot.Column(
            100,
            1,
            "ProtectedValue",
            "nvarchar",
            "nvarchar",
            40,
            0,
            isFileStream: true,
            isMasked: true,
            encryptionType: "DETERMINISTIC",
            xmlCollectionId: 7);
        MigrationCatalog catalog = Build(SqlServerTestSnapshot.Create(
            tables: [SqlServerTestSnapshot.OrdinaryTable(100, 1, "Orders")],
            columns: [special]));

        MigrationCatalogObject column = FindColumn(catalog, "ProtectedValue");
        Assert.Equal("true", Facet(column, "sqlServerFileStream"));
        Assert.Equal("true", Facet(column, "sqlServerMasked"));
        Assert.Equal("DETERMINISTIC", Facet(column, "sqlServerEncryptionType"));
        Assert.Equal("7", Facet(column, "sqlServerXmlCollectionId"));
        Assert.Contains(
            catalog.Diagnostics,
            item =>
                item.ObjectId == column.ObjectId &&
                item.RuleId == "MIG-SQLSERVER-COLUMN-FEATURE-UNSUPPORTED-001" &&
                !item.CanOverride);
    }

    [Fact]
    public void HiddenDefaultIdentityAndComputedDetailsCannotMasqueradeAsOrdinaryColumns()
    {
        SqlServerColumnMetadata hidden = SqlServerTestSnapshot.Column(
            100,
            1,
            "HiddenMetadata",
            "int",
            "int",
            4,
            10,
            hasDefault: true,
            defaultConstraintName: "DF_HiddenMetadata",
            isComputed: true,
            isIdentity: true);
        MigrationCatalog catalog = Build(SqlServerTestSnapshot.Create(
            tables: [SqlServerTestSnapshot.OrdinaryTable(100, 1, "Orders")],
            columns: [hidden]));

        MigrationCatalogObject column = FindColumn(catalog, "HiddenMetadata");
        Assert.Equal("true", Facet(column, "hasDefault"));
        Assert.Equal("source-expression", Facet(column, "defaultKind"));
        Assert.Equal("unknown", Facet(column, "sqlServerDefaultDefinitionSourceBytes"));
        Assert.Equal("true", Facet(column, "identity"));
        Assert.Equal("native", Facet(column, "logicalType"));
        Assert.Null(Facet(column, "sqlServerDefaultDefinitionDigest"));
        Assert.Null(Facet(column, "sqlServerComputedDefinitionDigest"));
        string[] rules = catalog.Diagnostics
            .Where(item => item.ObjectId == column.ObjectId)
            .Select(static item => item.RuleId)
            .ToArray();
        Assert.Contains("MIG-SQLSERVER-DEFAULT-UNANALYZED-001", rules);
        Assert.Contains("MIG-SQLSERVER-IDENTITY-DETAILS-UNKNOWN-001", rules);
        Assert.Contains("MIG-SQLSERVER-TYPE-OR-GENERATION-UNSUPPORTED-001", rules);
    }

    [Fact]
    public void SnapshotFingerprintBindsExplicitDefaultComputedAndIdentityPresence()
    {
        SqlServerCatalogSnapshot baseline = SqlServerTestSnapshot.Create(
            tables: [SqlServerTestSnapshot.OrdinaryTable(100, 1, "Orders")],
            columns:
            [
                SqlServerTestSnapshot.Column(
                    100,
                    1,
                    "Value",
                    "int",
                    "int",
                    4,
                    10),
            ]);
        SqlServerColumnMetadata column = Assert.Single(baseline.Columns);
        string expected = SqlServerCatalogBuilder.ComputeSnapshotDigest(baseline);

        SqlServerCatalogSnapshot Mutated(SqlServerColumnMetadata changed) =>
            new(
                baseline.EndpointDigest,
                baseline.ProviderVersion,
                baseline.Instance,
                baseline.Database,
                baseline.Schemas,
                baseline.Tables,
                [changed]);

        Assert.NotEqual(
            expected,
            SqlServerCatalogBuilder.ComputeSnapshotDigest(
                Mutated(column with { HasDefault = true })));
        Assert.NotEqual(
            expected,
            SqlServerCatalogBuilder.ComputeSnapshotDigest(
                Mutated(column with { DefaultDefinitionBytes = 2 })));
        Assert.NotEqual(
            expected,
            SqlServerCatalogBuilder.ComputeSnapshotDigest(
                Mutated(column with { ComputedDefinitionBytes = 2 })));
        Assert.NotEqual(
            expected,
            SqlServerCatalogBuilder.ComputeSnapshotDigest(
                Mutated(column with { IsIdentity = true })));
    }

    private static MigrationCatalog Build(SqlServerCatalogSnapshot snapshot) =>
        SqlServerCatalogBuilder.Build(
            snapshot,
            Request(),
            SqlServerInspectionLimits.Default,
            Ct);

    private static MigrationInspectionRequest Request() =>
        new()
        {
            TargetCSharpDbVersion =
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            IncludeProfile = false,
        };

    private static MigrationCatalogObject FindColumn(
        MigrationCatalog catalog,
        string name) =>
        Assert.Single(
            catalog.Objects,
            item =>
                item.Kind == MigrationObjectKind.Column &&
                item.SourceName == name);

    private static string? Facet(MigrationCatalogObject item, string name) =>
        item.Facets.SingleOrDefault(
            facet => string.Equals(facet.Name, name, StringComparison.Ordinal))?.Value;
}
