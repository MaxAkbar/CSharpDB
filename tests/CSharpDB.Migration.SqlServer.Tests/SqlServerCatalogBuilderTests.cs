using CSharpDB.Migration;
using CSharpDB.Migration.SqlServer;

namespace CSharpDB.Migration.SqlServer.Tests;

public sealed class SqlServerCatalogBuilderTests
{
    private const string GoldenCatalogDigest =
        "debf3a3ac8e87e3e19ab3c71837607dc4d55b35c610af5ac3c698ec2127b4af5";
    private const string GoldenSourceFingerprint =
        "sha256:97405c123b11d0f1b574344400bfb1d6b9087c598dfad0609685809fda9f692a";

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
        Assert.DoesNotContain(
            SqlServerTestSnapshot.SecretCheckDefinition,
            serialized,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            SqlServerTestSnapshot.SecretFilterDefinition,
            serialized,
            StringComparison.Ordinal);
        Assert.DoesNotContain("NeverPersistThis", serialized, StringComparison.Ordinal);

        MigrationCatalogObject database = Assert.Single(
            first.Objects,
            static item => item.Kind == MigrationObjectKind.Database);
        Assert.Equal("complete", Facet(database, "sqlServerMetadataVisibility"));
        Assert.Equal("160", Facet(database, "sqlServerCompatibilityLevel"));
        Assert.Equal(SqlServerCatalogBuilder.CatalogContract, Facet(
            database,
            "sqlServerCatalogContract"));
        Assert.Equal("true", Facet(database, "sqlServerPermissionAuditAttempted"));
        Assert.Equal("true", Facet(database, "sqlServerPermissionAuditStable"));
        Assert.Equal("2", Facet(database, "sqlServerPermissionTokenCount"));
        Assert.Equal("0", Facet(database, "sqlServerPermissionDenyCount"));

        MigrationCatalogObject amount = FindColumn(first, "Amount");
        Assert.Equal("decimal", Facet(amount, "logicalType"));
        Assert.Equal("18", Facet(amount, "precision"));
        Assert.Equal("2", Facet(amount, "scale"));
        Assert.Equal("source-expression", Facet(amount, "defaultKind"));
        Assert.StartsWith(
            "sha256:",
            Facet(amount, "sqlServerDefaultDefinitionDigest"),
            StringComparison.Ordinal);

        MigrationCatalogObject customer = FindColumn(first, "Customer", "dbo");
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
            ordered.Columns.Reverse(),
            ordered.Keys.Reverse(),
            ordered.Indexes.Reverse(),
            ordered.IndexColumns.Reverse(),
            ordered.ForeignKeys.Reverse(),
            ordered.ForeignKeyColumns.Reverse(),
            ordered.Checks.Reverse(),
            ordered.Sequences.Reverse(),
            Reverse(ordered.PermissionAuditBefore),
            Reverse(ordered.PermissionAuditAfter));

        Assert.Equal(
            MigrationArtifactSerializer.SerializeCatalog(Build(ordered)),
            MigrationArtifactSerializer.SerializeCatalog(Build(reversed)));
    }

    [Fact]
    public void BuildInventoriesRelationalObjectsWithoutOverstatingCompatibility()
    {
        MigrationCatalog catalog = Build(SqlServerTestSnapshot.Create());

        MigrationCatalogObject primaryKey =
            FindObject(catalog, MigrationObjectKind.Key, "PK_Orders");
        Assert.Equal("primary", Facet(primaryKey, "kind"));
        Assert.Equal(
            ["Id", "Customer"],
            MemberNames(catalog, primaryKey, MigrationObjectReferenceRoles.Column));
        Assert.Equal([0, 1], primaryKey.Members.Select(static item => item.Ordinal));

        MigrationCatalogObject nullableUnique =
            FindObject(catalog, MigrationObjectKind.Key, "UQ_Orders_OptionalCode");
        Assert.Equal("sqlserver-null-sensitive-unique", Facet(nullableUnique, "kind"));
        Assert.Equal(
            ["OptionalCode"],
            MemberNames(catalog, nullableUnique, MigrationObjectReferenceRoles.Column));

        Assert.DoesNotContain(
            catalog.Objects,
            static item =>
                item.Kind == MigrationObjectKind.Index &&
                item.SourceName is "PK_Orders" or "UQ_Orders_OptionalCode");

        MigrationCatalogObject ordinaryIndex =
            FindObject(catalog, MigrationObjectKind.Index, "IX_Orders_Customer");
        Assert.Equal("standard", Facet(ordinaryIndex, "kind"));
        Assert.Equal(
            ["Customer"],
            MemberNames(catalog, ordinaryIndex, MigrationObjectReferenceRoles.Column));

        MigrationCatalogObject shapedIndex = FindObject(
            catalog,
            MigrationObjectKind.Index,
            "IX_Orders_Amount_Filtered");
        Assert.Equal("true", Facet(shapedIndex, "partial"));
        Assert.Equal("true", Facet(shapedIndex, "includedColumns"));
        Assert.Equal("descending", Facet(shapedIndex, "sortDirections"));
        Assert.Equal(
            ["Amount"],
            MemberNames(catalog, shapedIndex, MigrationObjectReferenceRoles.Column));
        Assert.Equal(
            ["Amount", "Customer"],
            DependencyNames(catalog, shapedIndex));
        Assert.StartsWith(
            "sha256:",
            Facet(shapedIndex, "sqlServerFilterDefinitionDigest"),
            StringComparison.Ordinal);

        MigrationCatalogObject clusteredIndex = FindObject(
            catalog,
            MigrationObjectKind.Index,
            "CX_Archive_ArchiveId");
        Assert.Equal("clustered", Facet(clusteredIndex, "kind"));

        MigrationCatalogObject foreignKey = FindObject(
            catalog,
            MigrationObjectKind.ForeignKey,
            "FK_Archive_Orders");
        Assert.Equal(
            ["OrderId", "Customer"],
            MemberNames(
                catalog,
                foreignKey,
                MigrationObjectReferenceRoles.SourceColumn));
        MigrationObjectReference referencedKey = Assert.Single(
            foreignKey.Members,
            static item =>
                item.Role == MigrationObjectReferenceRoles.ReferencedKey);
        Assert.Equal(primaryKey.ObjectId, referencedKey.ObjectId);
        Assert.Equal("cascade", Facet(foreignKey, "onDelete"));
        Assert.Null(Facet(foreignKey, "onUpdate"));

        MigrationCatalogObject unsupportedForeignKey = FindObject(
            catalog,
            MigrationObjectKind.ForeignKey,
            "FK_Archive_OptionalCode");
        Assert.Equal("sqlserver-disabled", Facet(unsupportedForeignKey, "timing"));
        Assert.Equal("cascade", Facet(unsupportedForeignKey, "onUpdate"));

        MigrationCatalogObject unresolvedForeignKey = FindObject(
            catalog,
            MigrationObjectKind.Other,
            "FK_Archive_Customer_UX");
        Assert.Equal(
            "sqlserver-unresolved-foreign-key",
            Facet(unresolvedForeignKey, "kind"));
        Assert.Empty(unresolvedForeignKey.Members);
        MigrationCatalogObject standaloneUnique = FindObject(
            catalog,
            MigrationObjectKind.Index,
            "UX_Orders_Customer");
        Assert.Contains(standaloneUnique.ObjectId, unresolvedForeignKey.DependsOn);

        MigrationCatalogObject check = FindObject(
            catalog,
            MigrationObjectKind.CheckConstraint,
            "CK_Orders_Amount");
        Assert.StartsWith(
            "sha256:",
            Facet(check, "sqlServerCheckDefinitionDigest"),
            StringComparison.Ordinal);
        Assert.Null(Facet(check, "deterministic"));
        Assert.Null(Facet(check, "rowLocal"));
        Assert.Equal(["Amount"], DependencyNames(catalog, check));

        MigrationCatalogObject sequence = FindObject(
            catalog,
            MigrationObjectKind.Sequence,
            "OrderSequence");
        Assert.Null(sequence.NativeType);
        Assert.Equal("5", Facet(sequence, "sqlServerIncrement"));

        string[] rules = catalog.Diagnostics
            .Select(static item => item.RuleId)
            .ToArray();
        Assert.Contains("MIG-SQLSERVER-NULLABLE-UNIQUE-SEMANTICS-001", rules);
        Assert.Contains("MIG-SQLSERVER-INDEX-SHAPE-UNSUPPORTED-001", rules);
        Assert.Contains("MIG-SQLSERVER-FK-SHAPE-UNSUPPORTED-001", rules);
        Assert.Contains(
            "MIG-SQLSERVER-FK-UNIQUE-INDEX-TARGET-UNSUPPORTED-001",
            rules);
        Assert.Contains("MIG-SQLSERVER-CHECK-EXPRESSION-UNANALYZED-001", rules);
        Assert.Contains("MIG-SQLSERVER-SEQUENCE-UNSUPPORTED-001", rules);

        string serialized = MigrationArtifactSerializer.SerializeCatalog(catalog);
        Assert.DoesNotContain(
            SqlServerTestSnapshot.SecretCheckDefinition,
            serialized,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            SqlServerTestSnapshot.SecretFilterDefinition,
            serialized,
            StringComparison.Ordinal);
    }

    [Fact]
    public void PlannerIncludesOnlyTheProvenRelationalSubset()
    {
        MigrationCatalog catalog =
            Build(SqlServerTestSnapshot.CreateSupportedRelational());

        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);

        Assert.True(PlanObject(plan, catalog, MigrationObjectKind.Key, "PK_Orders").Included);
        MigrationCatalogObject heapIndex = FindObject(
            catalog,
            MigrationObjectKind.Index,
            "IX_Archive_OrderId");
        Assert.Equal("standard", Facet(heapIndex, "kind"));
        Assert.Equal("true", Facet(heapIndex, "sqlServerHeapRid"));
        Assert.Equal(
            ["OrderId"],
            MemberNames(catalog, heapIndex, MigrationObjectReferenceRoles.Column));
        Assert.Equal(["OrderId"], DependencyNames(catalog, heapIndex));
        Assert.True(PlanObject(
            plan,
            catalog,
            MigrationObjectKind.Index,
            "IX_Archive_OrderId").Included);
        Assert.True(PlanObject(
            plan,
            catalog,
            MigrationObjectKind.ForeignKey,
            "FK_Archive_Orders").Included);

        MigrationPlanReadiness readiness =
            MigrationPlanReadinessValidator.Evaluate(plan, catalog);
        Assert.Equal(MigrationPlanReadinessStatus.Blocked, readiness.Status);
        Assert.Contains(
            catalog.Diagnostics.Single(static item =>
                item.RuleId == "MIG-SQLSERVER-INVENTORY-PARTIAL-001").DiagnosticId,
            readiness.BlockingDiagnosticIds);
    }

    [Fact]
    public void BuildFailsClosedForUnclassifiedRowstoreIndexColumns()
    {
        SqlServerCatalogSnapshot baseline =
            SqlServerTestSnapshot.CreateSupportedRelational();
        SqlServerCatalogSnapshot malformed = Rebuild(
            baseline,
            indexColumns:
            [
                .. baseline.IndexColumns,
                new SqlServerIndexColumnMetadata(
                    ObjectId: 200,
                    IndexId: 1,
                    IndexColumnId: 3,
                    ColumnId: 1,
                    KeyOrdinal: 0,
                    PartitionOrdinal: 0,
                    IsDescending: false,
                    IsIncluded: false),
            ]);

        MigrationCatalog catalog = Build(malformed);
        MigrationCatalogObject index = FindObject(
            catalog,
            MigrationObjectKind.Index,
            "IX_Archive_OrderId");

        Assert.Equal("sqlserver-unresolved-index", Facet(index, "kind"));
        Assert.Contains(
            catalog.Diagnostics,
            item =>
                item.ObjectId == index.ObjectId &&
                item.RuleId == "MIG-SQLSERVER-INDEX-SHAPE-UNSUPPORTED-001" &&
                item.Status == MigrationCompatibilityStatus.Unsupported &&
                !item.CanOverride);
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
    [InlineData(true, false, true)]
    [InlineData(false, true, true)]
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
    public void BuildFailsClosedForPermissionAuditDriftAndMetadataDenials()
    {
        SqlServerDatabaseMetadata database = SqlServerTestSnapshot.Database() with
        {
            IsSysAdmin = false,
            IsDbOwner = false,
            HasControl = false,
            HasViewDefinition = true,
            HasViewSecurityDefinition = true,
        };
        SqlServerPermissionAuditMetadata baseline =
            SqlServerTestSnapshot.PermissionAudit();
        var changed = SqlServerTestSnapshot.PermissionAudit(
            tokens:
            [
                .. baseline.Tokens,
                new SqlServerUserTokenMetadata(
                    7,
                    "DATABASE ROLE",
                    "GRANT OR DENY"),
            ]);

        MigrationCatalog drift = Build(SqlServerTestSnapshot.Create(
            database: database,
            permissionAuditBefore: baseline,
            permissionAuditAfter: changed));
        MigrationCatalogObject driftDatabase = Assert.Single(
            drift.Objects,
            static item => item.Kind == MigrationObjectKind.Database);
        Assert.Equal("unknown", Facet(driftDatabase, "sqlServerMetadataVisibility"));
        Assert.Equal("false", Facet(driftDatabase, "sqlServerPermissionAuditStable"));
        Assert.Contains(
            drift.Diagnostics,
            static item =>
                item.RuleId == "MIG-SQLSERVER-PERMISSION-AUDIT-DRIFT-001" &&
                !item.CanOverride);

        var deniedAudit = SqlServerTestSnapshot.PermissionAudit(
            denials:
            [
                new SqlServerPermissionDenyMetadata(
                    Class: 3,
                    MajorId: 5,
                    MinorId: 0,
                    PermissionName: "VIEW DEFINITION",
                    GranteePrincipalId: 1,
                    TokenUsage: "DENY ONLY"),
            ]);
        MigrationCatalog denied = Build(SqlServerTestSnapshot.Create(
            database: database,
            permissionAuditBefore: deniedAudit,
            permissionAuditAfter: deniedAudit));
        MigrationCatalogObject deniedDatabase = Assert.Single(
            denied.Objects,
            static item => item.Kind == MigrationObjectKind.Database);
        Assert.Equal(
            "incomplete",
            Facet(deniedDatabase, "sqlServerMetadataVisibility"));
        Assert.Contains(
            denied.Diagnostics,
            static item =>
                item.RuleId == "MIG-SQLSERVER-PERMISSION-DENY-001" &&
                !item.CanOverride);

        var columnOnlyDeny = SqlServerTestSnapshot.PermissionAudit(
            denials:
            [
                new SqlServerPermissionDenyMetadata(
                    Class: 1,
                    MajorId: 100,
                    MinorId: 2,
                    PermissionName: "SELECT",
                    GranteePrincipalId: 1,
                    TokenUsage: "DENY ONLY"),
            ]);
        MigrationCatalog columnDenied = Build(SqlServerTestSnapshot.Create(
            database: database,
            permissionAuditBefore: columnOnlyDeny,
            permissionAuditAfter: columnOnlyDeny));
        MigrationCatalogObject columnDeniedDatabase = Assert.Single(
            columnDenied.Objects,
            static item => item.Kind == MigrationObjectKind.Database);
        Assert.Equal(
            "unknown",
            Facet(columnDeniedDatabase, "sqlServerMetadataVisibility"));
        Assert.DoesNotContain(
            columnDenied.Diagnostics,
            static item => item.RuleId == "MIG-SQLSERVER-PERMISSION-DENY-001");
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
        Assert.Throws<SqlServerMigrationException>(
            () => SqlServerCatalogBuilder.Build(
                snapshot,
                Request(),
                new SqlServerInspectionLimits { MaxKeys = 1 },
                Ct));
        Assert.Throws<SqlServerMigrationException>(
            () => SqlServerCatalogBuilder.Build(
                snapshot,
                Request(),
                new SqlServerInspectionLimits { MaxIndexes = 5 },
                Ct));
        Assert.Throws<SqlServerMigrationException>(
            () => SqlServerCatalogBuilder.Build(
                snapshot,
                Request(),
                new SqlServerInspectionLimits { MaxIndexColumns = 7 },
                Ct));
        Assert.Throws<SqlServerMigrationException>(
            () => SqlServerCatalogBuilder.Build(
                snapshot,
                Request(),
                new SqlServerInspectionLimits { MaxForeignKeys = 2 },
                Ct));
        Assert.Throws<SqlServerMigrationException>(
            () => SqlServerCatalogBuilder.Build(
                snapshot,
                Request(),
                new SqlServerInspectionLimits { MaxForeignKeyColumns = 3 },
                Ct));
        Assert.Throws<SqlServerMigrationException>(
            () => SqlServerCatalogBuilder.Build(
                snapshot,
                Request(),
                new SqlServerInspectionLimits { MaxChecks = 1 },
                Ct));
        Assert.Throws<SqlServerMigrationException>(
            () => SqlServerCatalogBuilder.Build(
                snapshot,
                Request(),
                new SqlServerInspectionLimits { MaxSequences = 1 },
                Ct));
        Assert.Throws<SqlServerMigrationException>(
            () => SqlServerCatalogBuilder.Build(
                snapshot,
                Request(),
                new SqlServerInspectionLimits { MaxUserTokens = 1 },
                Ct));
        Assert.Throws<SqlServerMigrationException>(
            () => SqlServerCatalogBuilder.Build(
                snapshot,
                Request(),
                new SqlServerInspectionLimits { MaxStructuralRowsTotal = 1 },
                Ct));
        Assert.Throws<SqlServerMigrationException>(
            () => SqlServerCatalogBuilder.Build(
                snapshot,
                Request(),
                new SqlServerInspectionLimits { MaxPermissionRowsTotal = 3 },
                Ct));

        var twoDenials = SqlServerTestSnapshot.PermissionAudit(
            denials:
            [
                new SqlServerPermissionDenyMetadata(
                    1,
                    100,
                    0,
                    "SELECT",
                    0,
                    "DENY ONLY"),
                new SqlServerPermissionDenyMetadata(
                    1,
                    200,
                    0,
                    "SELECT",
                    1,
                    "DENY ONLY"),
            ]);
        SqlServerCatalogSnapshot denialSnapshot = SqlServerTestSnapshot.Create(
            permissionAuditBefore: twoDenials,
            permissionAuditAfter: twoDenials);
        Assert.Throws<SqlServerMigrationException>(
            () => SqlServerCatalogBuilder.Build(
                denialSnapshot,
                Request(),
                new SqlServerInspectionLimits { MaxPermissionDenials = 1 },
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
            Rebuild(baseline, columns: [changed]);

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

    [Fact]
    public void SnapshotFingerprintBindsRelationalAndPermissionMetadata()
    {
        SqlServerCatalogSnapshot baseline = SqlServerTestSnapshot.Create();
        string expected = SqlServerCatalogBuilder.ComputeSnapshotDigest(baseline);

        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                keys: [baseline.Keys[0] with { IsSystemNamed = true }, .. baseline.Keys.Skip(1)]));
        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                indexes:
                [
                    baseline.Indexes[0] with { FillFactor = 80 },
                    .. baseline.Indexes.Skip(1),
                ]));
        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                indexColumns:
                [
                    baseline.IndexColumns[0] with { IsDescending = true },
                    .. baseline.IndexColumns.Skip(1),
                ]));
        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                foreignKeys:
                [
                    baseline.ForeignKeys[0] with
                    {
                        DeleteAction = 0,
                        DeleteActionDescription = "NO_ACTION",
                    },
                    .. baseline.ForeignKeys.Skip(1),
                ]));
        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                foreignKeyColumns:
                [
                    baseline.ForeignKeyColumns[0] with { ParentColumnId = 3 },
                    .. baseline.ForeignKeyColumns.Skip(1),
                ]));
        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                checks:
                [
                    baseline.Checks[0] with { UsesDatabaseCollation = true },
                    .. baseline.Checks.Skip(1),
                ]));
        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                sequences:
                [
                    baseline.Sequences[0] with { Increment = "6" },
                    .. baseline.Sequences.Skip(1),
                ]));

        SqlServerPermissionAuditMetadata changedAudit =
            SqlServerTestSnapshot.PermissionAudit(
                tokens:
                [
                    .. baseline.PermissionAuditAfter.Tokens,
                    new SqlServerUserTokenMetadata(
                        9,
                        "DATABASE ROLE",
                        "GRANT OR DENY"),
                ]);
        AssertDigestChanges(
            expected,
            Rebuild(baseline, permissionAuditAfter: changedAudit));
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

    private static SqlServerPermissionAuditMetadata Reverse(
        SqlServerPermissionAuditMetadata audit) =>
        new(
            audit.Tokens.Reverse().ToArray(),
            audit.Denials.Reverse().ToArray(),
            audit.Attempted);

    private static MigrationCatalogObject FindObject(
        MigrationCatalog catalog,
        MigrationObjectKind kind,
        string name) =>
        Assert.Single(
            catalog.Objects,
            item =>
                item.Kind == kind &&
                item.SourceName == name);

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
            .Where(member => string.Equals(
                member.Role,
                role,
                StringComparison.Ordinal))
            .OrderBy(static member => member.Ordinal)
            .Select(member => objectsById[member.ObjectId].SourceName)
            .ToArray();
    }

    private static string[] DependencyNames(
        MigrationCatalog catalog,
        MigrationCatalogObject item)
    {
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById =
            catalog.Objects.ToDictionary(
                static candidate => candidate.ObjectId,
                StringComparer.Ordinal);
        return item.DependsOn
            .Select(id => objectsById[id].SourceName)
            .ToArray();
    }

    private static MigrationPlanObject PlanObject(
        MigrationPlan plan,
        MigrationCatalog catalog,
        MigrationObjectKind kind,
        string name)
    {
        string objectId = FindObject(catalog, kind, name).ObjectId;
        return Assert.Single(
            plan.Objects,
            item => item.SourceObjectId == objectId);
    }

    private static SqlServerCatalogSnapshot Rebuild(
        SqlServerCatalogSnapshot source,
        IEnumerable<SqlServerColumnMetadata>? columns = null,
        IEnumerable<SqlServerKeyMetadata>? keys = null,
        IEnumerable<SqlServerIndexMetadata>? indexes = null,
        IEnumerable<SqlServerIndexColumnMetadata>? indexColumns = null,
        IEnumerable<SqlServerForeignKeyMetadata>? foreignKeys = null,
        IEnumerable<SqlServerForeignKeyColumnMetadata>? foreignKeyColumns = null,
        IEnumerable<SqlServerCheckMetadata>? checks = null,
        IEnumerable<SqlServerSequenceMetadata>? sequences = null,
        SqlServerPermissionAuditMetadata? permissionAuditBefore = null,
        SqlServerPermissionAuditMetadata? permissionAuditAfter = null) =>
        new(
            source.EndpointDigest,
            source.ProviderVersion,
            source.Instance,
            source.Database,
            source.Schemas,
            source.Tables,
            columns ?? source.Columns,
            keys ?? source.Keys,
            indexes ?? source.Indexes,
            indexColumns ?? source.IndexColumns,
            foreignKeys ?? source.ForeignKeys,
            foreignKeyColumns ?? source.ForeignKeyColumns,
            checks ?? source.Checks,
            sequences ?? source.Sequences,
            permissionAuditBefore ?? source.PermissionAuditBefore,
            permissionAuditAfter ?? source.PermissionAuditAfter);

    private static void AssertDigestChanges(
        string expected,
        SqlServerCatalogSnapshot snapshot) =>
        Assert.NotEqual(
            expected,
            SqlServerCatalogBuilder.ComputeSnapshotDigest(snapshot));

    private static MigrationCatalogObject FindColumn(
        MigrationCatalog catalog,
        string name,
        string? sourceNamespace = null) =>
        Assert.Single(
            catalog.Objects,
            item =>
                item.Kind == MigrationObjectKind.Column &&
                item.SourceName == name &&
                (sourceNamespace is null ||
                 item.SourceNamespace == sourceNamespace));

    private static string? Facet(MigrationCatalogObject item, string name) =>
        item.Facets.SingleOrDefault(
            facet => string.Equals(facet.Name, name, StringComparison.Ordinal))?.Value;
}
