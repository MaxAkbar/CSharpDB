using CSharpDB.Migration;
using CSharpDB.Migration.CSharpDb;

namespace CSharpDB.Migration.MySql.Tests;

public sealed class MySqlProgrammableInventoryTests
{
    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public void BuildInventoriesDefaultsAndProgrammableObjectsWithoutRawLeakage()
    {
        MySqlCatalogSnapshot snapshot =
            MySqlTestSnapshot.CreateProgrammableInventory();
        MigrationCatalog catalog = Build(snapshot);
        MigrationContractValidator.ValidateCatalog(catalog);
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById =
            catalog.Objects.ToDictionary(
                static item => item.ObjectId,
                StringComparer.Ordinal);

        MigrationCatalogObject database = FindObject(
            catalog,
            MigrationObjectKind.Database,
            "SourceDb");
        Assert.Equal(
            "csharpdb-mysql-catalog/v3",
            Facet(database, "mysqlCatalogContract"));
        Assert.Equal(
            "true",
            Facet(database, "mysqlExplicitDefaultsForTimestamp"));
        Assert.Equal("2", Facet(database, "mysqlViewCount"));
        Assert.Equal("3", Facet(database, "mysqlViewColumnCount"));
        Assert.Equal("1", Facet(database, "mysqlTriggerCount"));
        Assert.Equal("2", Facet(database, "mysqlRoutineCount"));
        Assert.Equal("3", Facet(database, "mysqlRoutineParameterCount"));

        MigrationCatalogObject visibleView = FindObject(
            catalog,
            MigrationObjectKind.View,
            "VisibleOrders");
        MigrationCatalogObject filteredView = FindObject(
            catalog,
            MigrationObjectKind.View,
            "FilteredOrders");
        Assert.Equal(
            MigrationObjectKind.Namespace,
            objectsById[visibleView.ParentObjectId!].Kind);
        Assert.Equal(
            MigrationObjectKind.Namespace,
            objectsById[filteredView.ParentObjectId!].Kind);
        Assert.Equal(
            "true",
            Facet(visibleView, "mysqlViewMetadataVisible"));
        Assert.Equal(
            "available",
            Facet(visibleView, "mysqlViewDefinitionStatus"));
        Assert.StartsWith(
            "sha256:",
            Facet(visibleView, "mysqlViewDefinitionDigest"),
            StringComparison.Ordinal);
        Assert.Equal(
            "false",
            Facet(filteredView, "mysqlViewMetadataVisible"));
        Assert.Equal(
            "unavailable",
            Facet(filteredView, "mysqlViewDefinitionStatus"));
        Assert.Null(Facet(filteredView, "mysqlViewDefinitionDigest"));
        Assert.Equal(
            ["Customer", "Id"],
            catalog.Objects
                .Where(item =>
                    item.Kind == MigrationObjectKind.Column &&
                    item.ParentObjectId == visibleView.ObjectId)
                .Select(static item => item.SourceName)
                .OrderBy(static item => item, StringComparer.Ordinal)
                .ToArray());
        Assert.Single(
            catalog.Objects,
            item =>
                item.Kind == MigrationObjectKind.Column &&
                item.ParentObjectId == filteredView.ObjectId &&
                item.SourceName == "Id");

        MigrationCatalogObject trigger = FindObject(
            catalog,
            MigrationObjectKind.Trigger,
            "TR_Orders_Customer");
        Assert.Equal(
            "Orders",
            objectsById[trigger.ParentObjectId!].SourceName);
        MigrationCatalogObject procedure = FindObject(
            catalog,
            MigrationObjectKind.Routine,
            "RefreshArchive");
        MigrationCatalogObject function = FindObject(
            catalog,
            MigrationObjectKind.Routine,
            "NormalizeCustomer");
        Assert.Equal(
            MigrationObjectKind.Namespace,
            objectsById[procedure.ParentObjectId!].Kind);
        Assert.Equal(
            MigrationObjectKind.Namespace,
            objectsById[function.ParentObjectId!].Kind);
        Assert.Equal(
            ["$return", "value"],
            catalog.Objects
                .Where(item =>
                    item.Kind == MigrationObjectKind.Other &&
                    item.ParentObjectId == function.ObjectId)
                .Select(static item => item.SourceName)
                .OrderBy(static item => item, StringComparer.Ordinal)
                .ToArray());

        MigrationCatalogObject literalDefault = FindObject(
            catalog,
            MigrationObjectKind.Column,
            "LiteralDefault");
        MigrationCatalogObject expressionDefault = FindObject(
            catalog,
            MigrationObjectKind.Column,
            "ExpressionDefault");
        MigrationCatalogObject updatedAt = FindObject(
            catalog,
            MigrationObjectKind.Column,
            "UpdatedAt");
        Assert.Equal("true", Facet(literalDefault, "hasDefault"));
        Assert.Equal("source-expression", Facet(literalDefault, "defaultKind"));
        Assert.Equal("true", Facet(expressionDefault, "hasDefault"));
        Assert.Equal("source-expression", Facet(expressionDefault, "defaultKind"));
        Assert.Equal("true", Facet(updatedAt, "hasDefault"));
        Assert.Equal("source-expression", Facet(updatedAt, "defaultKind"));
        Assert.Equal(
            "information-schema-non-null",
            Facet(literalDefault, "mysqlDefaultEvidence"));
        Assert.StartsWith(
            "sha256:",
            Facet(literalDefault, "mysqlDefaultDigest"),
            StringComparison.Ordinal);
        Assert.Equal(
            "true",
            Facet(expressionDefault, "mysqlDefaultGenerated"));
        Assert.Equal(
            "true",
            Facet(updatedAt, "mysqlOnUpdateCurrentTimestamp"));
        foreach (MigrationCatalogObject column in
                 new[]
                 {
                     literalDefault,
                     expressionDefault,
                     updatedAt,
                 })
        {
            Assert.Null(Facet(column, "targetSql"));
            Assert.Null(Facet(column, "defaultValue"));
            Assert.Null(Facet(column, "defaultExpression"));
        }

        Assert.Contains(
            catalog.Diagnostics,
            item =>
                item.ObjectId == filteredView.ObjectId &&
                item.RuleId ==
                    "MIG-MYSQL-VIEW-METADATA-INCOMPLETE-001" &&
                !item.CanOverride);
        Assert.Contains(
            catalog.Diagnostics,
            item =>
                item.ObjectId == trigger.ObjectId &&
                item.RuleId == "MIG-MYSQL-TRIGGER-UNSUPPORTED-001" &&
                !item.CanOverride);
        Assert.Contains(
            catalog.Diagnostics,
            item =>
                item.ObjectId == function.ObjectId &&
                item.RuleId == "MIG-MYSQL-ROUTINE-UNSUPPORTED-001" &&
                !item.CanOverride);
        Assert.Contains(
            catalog.Diagnostics,
            item =>
                item.ObjectId == literalDefault.ObjectId &&
                item.RuleId == "MIG-MYSQL-DEFAULT-UNSUPPORTED-001" &&
                !item.CanOverride);
        Assert.Contains(
            catalog.Diagnostics,
            item =>
                item.ObjectId == updatedAt.ObjectId &&
                item.RuleId == "MIG-MYSQL-ON-UPDATE-UNSUPPORTED-001" &&
                !item.CanOverride);

        foreach (MigrationCatalogObject item in
                 catalog.Objects.Where(static item =>
                     item.Kind is MigrationObjectKind.View or
                         MigrationObjectKind.Trigger or
                         MigrationObjectKind.Routine))
        {
            Assert.Null(Facet(item, "targetSql"));
            Assert.Null(Facet(item, "deterministic"));
            Assert.Null(Facet(item, "rowLocal"));
        }

        string serialized =
            MigrationArtifactSerializer.SerializeCatalog(catalog);
        Assert.DoesNotContain(
            MySqlTestSnapshot.SecretDefaultValue,
            serialized,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            MySqlTestSnapshot.SecretViewDefinition,
            serialized,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            MySqlTestSnapshot.SecretTriggerStatement,
            serialized,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            MySqlTestSnapshot.SecretProcedureDefinition,
            serialized,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            MySqlTestSnapshot.SecretFunctionDefinition,
            serialized,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            MySqlTestSnapshot.SecretDefinerIdentity,
            serialized,
            StringComparison.Ordinal);
    }

    [Fact]
    public void PrivilegeFilteredRoutineDefinitionRemainsUnavailableAndBlocking()
    {
        MySqlCatalogSnapshot baseline =
            MySqlTestSnapshot.CreateProgrammableInventory();
        MySqlRoutineMetadata function = baseline.Routines.Single(
            static item => item.RoutineType == "FUNCTION");
        MigrationCatalog catalog = Build(
            Rebuild(
                baseline,
                routines: Replace(
                    baseline.Routines,
                    function,
                    function with
                    {
                        DefinitionBytes = null,
                        Definition = null,
                    })));
        MigrationCatalogObject routine = FindObject(
            catalog,
            MigrationObjectKind.Routine,
            "NormalizeCustomer");

        Assert.Equal(
            "unavailable",
            Facet(routine, "mysqlRoutineDefinitionStatus"));
        Assert.Null(Facet(routine, "mysqlRoutineDefinitionDigest"));
        Assert.Contains(
            catalog.Diagnostics,
            item =>
                item.ObjectId == routine.ObjectId &&
                item.RuleId ==
                    "MIG-MYSQL-ROUTINE-DEFINITION-UNAVAILABLE-001" &&
                item.Status == MigrationCompatibilityStatus.Unknown &&
                !item.CanOverride);
    }

    [Fact]
    public void Bug118547GeneratedDefaultWithoutTextIsInventoriedAndExcluded()
    {
        const string tableName = "Bug118547Defaults";
        const string columnName = "GeneratedWithoutText";
        MySqlCatalogSnapshot snapshot = MySqlTestSnapshot.Create(
            tables: [MySqlTestSnapshot.Table(tableName)],
            columns:
            [
                MySqlTestSnapshot.Column(
                    tableName,
                    1,
                    columnName,
                    "int",
                    nullable: false,
                    defaultGenerated: true),
            ]);
        MySqlColumnMetadata sourceColumn = Assert.Single(snapshot.Columns);
        Assert.False(sourceColumn.IsGenerated);
        Assert.True(sourceColumn.IsDefaultGenerated);
        Assert.Null(sourceColumn.DefaultValue);
        Assert.Null(sourceColumn.DefaultBytes);

        MigrationCatalog catalog = Build(snapshot);
        MigrationCatalogObject table = FindObject(
            catalog,
            MigrationObjectKind.Table,
            tableName);
        MigrationCatalogObject column = FindObject(
            catalog,
            MigrationObjectKind.Column,
            columnName);
        Assert.Equal("true", Facet(column, "hasDefault"));
        Assert.Equal("source-expression", Facet(column, "defaultKind"));
        Assert.Equal(
            "information-schema-generated-text-unavailable",
            Facet(column, "mysqlDefaultEvidence"));
        Assert.Equal("true", Facet(column, "mysqlDefaultGenerated"));
        Assert.Null(Facet(column, "mysqlDefaultSourceBytes"));
        Assert.Null(Facet(column, "mysqlDefaultDigest"));
        Assert.Null(Facet(column, "mysqlDefaultLength"));
        Assert.Null(Facet(column, "targetSql"));
        Assert.Null(Facet(column, "defaultValue"));
        Assert.Null(Facet(column, "defaultExpression"));
        Assert.Contains(
            catalog.Diagnostics,
            item =>
                item.ObjectId == column.ObjectId &&
                item.RuleId == "MIG-MYSQL-DEFAULT-UNSUPPORTED-001" &&
                item.Status == MigrationCompatibilityStatus.Unsupported &&
                !item.CanOverride);
        Assert.Contains(
            catalog.Diagnostics,
            item =>
                item.ObjectId == column.ObjectId &&
                item.RuleId ==
                    "MIG-MYSQL-DEFAULT-TEXT-UNAVAILABLE-001" &&
                item.Severity == MigrationDiagnosticSeverity.Error &&
                item.Status == MigrationCompatibilityStatus.Unknown &&
                item.Summary ==
                    "The generated MySQL default text is unavailable." &&
                !item.CanOverride);

        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
        Assert.False(PlanObject(plan, column).Included);
        Assert.False(PlanObject(plan, table).Included);
        CSharpDbDdlPreview preview = CSharpDbDdlPreviewBuilder.Build(
            plan,
            catalog,
            cancellationToken: Ct);
        string renderedSql = string.Join(
            "\n",
            preview.Stages
                .SelectMany(static stage => stage.Actions)
                .Select(static action => action.Sql)
                .Where(static sql => sql is not null));
        Assert.DoesNotContain(
            tableName,
            renderedSql,
            StringComparison.Ordinal);
    }

    [Fact]
    public void BuildIsIndependentOfProgrammableProviderRowOrder()
    {
        MySqlCatalogSnapshot ordered =
            MySqlTestSnapshot.CreateProgrammableInventory();
        MySqlCatalogSnapshot reversed = Rebuild(
            ordered,
            tables: ordered.Tables.Reverse(),
            columns: ordered.Columns.Reverse(),
            tableDefinitions: ordered.TableDefinitions.Reverse(),
            keys: ordered.Keys.Reverse(),
            keyColumns: ordered.KeyColumns.Reverse(),
            foreignKeys: ordered.ForeignKeys.Reverse(),
            foreignKeyColumns: ordered.ForeignKeyColumns.Reverse(),
            checks: ordered.Checks.Reverse(),
            indexes: ordered.Indexes.Reverse(),
            indexParts: ordered.IndexParts.Reverse(),
            views: ordered.Views.Reverse(),
            viewColumns: ordered.ViewColumns.Reverse(),
            triggers: ordered.Triggers.Reverse(),
            routines: ordered.Routines.Reverse(),
            routineParameters: ordered.RoutineParameters.Reverse());

        MigrationCatalog first = Build(ordered);
        MigrationCatalog second = Build(reversed);
        Assert.Equal(
            first.Source.Fingerprint,
            second.Source.Fingerprint);
        Assert.Equal(
            MigrationArtifactSerializer.SerializeCatalog(first),
            MigrationArtifactSerializer.SerializeCatalog(second));
    }

    [Fact]
    public void V3FingerprintBindsEveryNewMetadataFamily()
    {
        MySqlCatalogSnapshot baseline =
            MySqlTestSnapshot.CreateProgrammableInventory();
        string expected = Build(baseline).Source.Fingerprint;
        MySqlColumnMetadata literalDefault = baseline.Columns.Single(
            static item => item.Name == "LiteralDefault");
        MySqlViewMetadata visibleView = baseline.Views.Single(
            static item => item.Name == "VisibleOrders");
        MySqlViewColumnMetadata viewColumn = baseline.ViewColumns.Single(
            static item => item.Name == "Customer");
        MySqlTriggerMetadata trigger = Assert.Single(baseline.Triggers);
        MySqlRoutineMetadata function = baseline.Routines.Single(
            static item => item.RoutineType == "FUNCTION");
        MySqlRoutineParameterMetadata parameter =
            baseline.RoutineParameters.Single(static item =>
                item.RoutineType == "FUNCTION" &&
                item.OrdinalPosition == 1);

        AssertFingerprintChanged(
            expected,
            Rebuild(
                baseline,
                session: baseline.Session with
                {
                    ExplicitDefaultsForTimestamp = false,
                }));
        AssertFingerprintChanged(
            expected,
            Rebuild(
                baseline,
                columns: Replace(
                    baseline.Columns,
                    literalDefault,
                    literalDefault with
                    {
                        DefaultValue =
                            MySqlTestSnapshot.SecretDefaultValue[..^1] + "X",
                    })));
        AssertFingerprintChanged(
            expected,
            Rebuild(
                baseline,
                views: Replace(
                    baseline.Views,
                    visibleView,
                    visibleView with { CheckOption = "CASCADED" })));
        AssertFingerprintChanged(
            expected,
            Rebuild(
                baseline,
                viewColumns: Replace(
                    baseline.ViewColumns,
                    viewColumn,
                    viewColumn with { Name = "Consumer" })));
        AssertFingerprintChanged(
            expected,
            Rebuild(
                baseline,
                triggers: Replace(
                    baseline.Triggers,
                    trigger,
                    trigger with { EventManipulation = "INSERT" })));
        AssertFingerprintChanged(
            expected,
            Rebuild(
                baseline,
                routines: Replace(
                    baseline.Routines,
                    function,
                    function with { IsDeterministic = false })));
        AssertFingerprintChanged(
            expected,
            Rebuild(
                baseline,
                routineParameters: Replace(
                    baseline.RoutineParameters,
                    parameter,
                    parameter with { Name = "input" })));
    }

    [Fact]
    public void SameLengthRawDefaultAndDefinitionsChangeDigestsButNeverLeak()
    {
        MySqlCatalogSnapshot baseline =
            MySqlTestSnapshot.CreateProgrammableInventory();
        MySqlColumnMetadata literalDefault = baseline.Columns.Single(
            static item => item.Name == "LiteralDefault");
        string changedDefault =
            MySqlTestSnapshot.SecretDefaultValue[..^1] + "X";
        AssertRawMutation(
            baseline,
            Rebuild(
                baseline,
                columns: Replace(
                    baseline.Columns,
                    literalDefault,
                    literalDefault with { DefaultValue = changedDefault })),
            MySqlTestSnapshot.SecretDefaultValue,
            changedDefault);

        MySqlViewMetadata view = baseline.Views.Single(
            static item => item.Definition is not null);
        string changedView = MySqlTestSnapshot.SecretViewDefinition.Replace(
            "NeverPersistThis",
            "NeverPersistThat",
            StringComparison.Ordinal);
        AssertRawMutation(
            baseline,
            Rebuild(
                baseline,
                views: Replace(
                    baseline.Views,
                    view,
                    view with { Definition = changedView })),
            MySqlTestSnapshot.SecretViewDefinition,
            changedView);

        MySqlTriggerMetadata trigger = Assert.Single(baseline.Triggers);
        string changedTrigger =
            MySqlTestSnapshot.SecretTriggerStatement.Replace(
                "NeverPersistThis",
                "NeverPersistThat",
                StringComparison.Ordinal);
        AssertRawMutation(
            baseline,
            Rebuild(
                baseline,
                triggers: Replace(
                    baseline.Triggers,
                    trigger,
                    trigger with { ActionStatement = changedTrigger })),
            MySqlTestSnapshot.SecretTriggerStatement,
            changedTrigger);

        MySqlRoutineMetadata function = baseline.Routines.Single(
            static item => item.RoutineType == "FUNCTION");
        string changedFunction =
            MySqlTestSnapshot.SecretFunctionDefinition.Replace(
                "NeverPersistThis",
                "NeverPersistThat",
                StringComparison.Ordinal);
        AssertRawMutation(
            baseline,
            Rebuild(
                baseline,
                routines: Replace(
                    baseline.Routines,
                    function,
                    function with { Definition = changedFunction })),
            MySqlTestSnapshot.SecretFunctionDefinition,
            changedFunction);
    }

    [Fact]
    public void PlannerAndTargetPreviewExcludeProgrammableAndDefaultObjects()
    {
        MigrationCatalog catalog = Build(
            MySqlTestSnapshot.CreateProgrammableInventory());
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);

        foreach (MigrationCatalogObject item in catalog.Objects.Where(
                     static item =>
                         item.Kind is MigrationObjectKind.View or
                             MigrationObjectKind.Trigger or
                             MigrationObjectKind.Routine))
        {
            Assert.False(PlanObject(plan, item).Included);
        }
        foreach (string columnName in
                 new[]
                 {
                     "LiteralDefault",
                     "ExpressionDefault",
                     "UpdatedAt",
                 })
        {
            Assert.False(
                PlanObject(
                    plan,
                    FindObject(
                        catalog,
                        MigrationObjectKind.Column,
                        columnName)).Included);
        }
        Assert.False(
            PlanObject(
                plan,
                FindObject(
                    catalog,
                    MigrationObjectKind.Table,
                    "Archive")).Included);

        CSharpDbDdlPreview preview = CSharpDbDdlPreviewBuilder.Build(
            plan,
            catalog,
            cancellationToken: Ct);
        string renderedSql = string.Join(
            "\n",
            preview.Stages
                .SelectMany(static stage => stage.Actions)
                .Select(static action => action.Sql)
                .Where(static sql => sql is not null));
        foreach (string excludedName in
                 new[]
                 {
                     "Archive",
                     "VisibleOrders",
                     "FilteredOrders",
                     "TR_Orders_Customer",
                     "RefreshArchive",
                     "NormalizeCustomer",
                 })
        {
            Assert.DoesNotContain(
                excludedName,
                renderedSql,
                StringComparison.Ordinal);
        }
    }

    [Fact]
    public void EveryProgrammableObjectCountLimitFailsClosed()
    {
        MySqlCatalogSnapshot snapshot =
            MySqlTestSnapshot.CreateProgrammableInventory();
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                snapshot,
                MySqlInspectionLimits.Default with { MaxViews = 1 }));
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                snapshot,
                MySqlInspectionLimits.Default with { MaxViewColumns = 1 }));
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                Rebuild(
                    snapshot,
                    triggers:
                    [
                        .. snapshot.Triggers,
                        MySqlTestSnapshot.Trigger(
                            "Orders",
                            "TR_Orders_Customer_Second",
                            "SET NEW.`Customer` = NEW.`Customer`",
                            actionOrder: 2),
                    ]),
                MySqlInspectionLimits.Default with { MaxTriggers = 1 }));
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                snapshot,
                MySqlInspectionLimits.Default with { MaxRoutines = 1 }));
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                snapshot,
                MySqlInspectionLimits.Default with
                {
                    MaxRoutineParameters = 1,
                }));

        MySqlCatalogSnapshot reconciledViews = MySqlTestSnapshot.Create(
            tables: [],
            columns: [],
            tableDefinitions: [],
            views: MySqlTestSnapshot.Views());
        Build(
            reconciledViews,
            MySqlInspectionLimits.Default with
            {
                MaxStructuralRowsTotal = 3,
            });
        Assert.Throws<MySqlMigrationException>(
            () => Build(
                reconciledViews,
                MySqlInspectionLimits.Default with
                {
                    MaxStructuralRowsTotal = 2,
                }));
    }

    [Theory]
    [MemberData(nameof(InvalidProgrammableSnapshots))]
    public void BuildRejectsMalformedProgrammableMetadata(object value)
    {
        MySqlCatalogSnapshot snapshot =
            Assert.IsType<MySqlCatalogSnapshot>(value);

        Assert.Throws<MySqlMigrationException>(() => Build(snapshot));
    }

    public static IEnumerable<object[]> InvalidProgrammableSnapshots()
    {
        MySqlCatalogSnapshot baseline =
            MySqlTestSnapshot.CreateProgrammableInventory();
        MySqlColumnMetadata defaultColumn = baseline.Columns.Single(
            static item => item.Name == "LiteralDefault");
        yield return
        [
            Rebuild(
                baseline,
                columns: Replace(
                    baseline.Columns,
                    defaultColumn,
                    defaultColumn with
                    {
                        DefaultBytes = defaultColumn.DefaultBytes + 1,
                    })),
        ];
        yield return
        [
            Rebuild(
                baseline,
                database: baseline.Database with { ViewCount = 1 }),
        ];

        MySqlViewMetadata visibleView = baseline.Views.Single(
            static item => item.MetadataVisible);
        yield return
        [
            Rebuild(
                baseline,
                views: Replace(
                    baseline.Views,
                    visibleView,
                    visibleView with
                    {
                        DefinitionBytes = visibleView.DefinitionBytes + 1,
                    })),
        ];

        MySqlViewColumnMetadata firstViewColumn = baseline.ViewColumns[0];
        MySqlViewColumnMetadata secondViewColumn = baseline.ViewColumns[1];
        yield return
        [
            Rebuild(
                baseline,
                viewColumns: Replace(
                    baseline.ViewColumns,
                    firstViewColumn,
                    firstViewColumn with { ViewName = "MissingView" })),
        ];
        yield return
        [
            Rebuild(
                baseline,
                viewColumns: Replace(
                    baseline.ViewColumns,
                    secondViewColumn,
                    secondViewColumn with { OrdinalPosition = 3 })),
        ];
        yield return
        [
            Rebuild(
                baseline,
                viewColumns:
                [
                    firstViewColumn,
                    firstViewColumn with
                    {
                        Name = "DuplicateOrdinal",
                    },
                    .. baseline.ViewColumns.Skip(2),
                ]),
        ];

        MySqlTriggerMetadata trigger = Assert.Single(baseline.Triggers);
        yield return
        [
            Rebuild(
                baseline,
                triggers:
                [
                    trigger with { EventObjectTable = "MissingTable" },
                ]),
        ];
        yield return
        [
            Rebuild(
                baseline,
                triggers:
                [
                    trigger with { ActionOrder = 0 },
                ]),
        ];
        yield return
        [
            Rebuild(
                baseline,
                triggers:
                [
                    trigger with
                    {
                        ActionStatementBytes =
                            trigger.ActionStatementBytes + 1,
                    },
                ]),
        ];

        MySqlRoutineMetadata function = baseline.Routines.Single(
            static item => item.RoutineType == "FUNCTION");
        yield return
        [
            Rebuild(
                baseline,
                routines: Replace(
                    baseline.Routines,
                    function,
                    function with { DtdIdentifier = null })),
        ];
        yield return
        [
            Rebuild(
                baseline,
                routines: Replace(
                    baseline.Routines,
                    function,
                    function with
                    {
                        DefinitionBytes = function.DefinitionBytes + 1,
                    })),
        ];

        MySqlRoutineParameterMetadata returnRow =
            baseline.RoutineParameters.Single(static item =>
                item.RoutineType == "FUNCTION" &&
                item.OrdinalPosition == 0);
        MySqlRoutineParameterMetadata parameter =
            baseline.RoutineParameters.Single(static item =>
                item.RoutineType == "FUNCTION" &&
                item.OrdinalPosition == 1);
        yield return
        [
            Rebuild(
                baseline,
                routineParameters: Replace(
                    baseline.RoutineParameters,
                    parameter,
                    parameter with { SpecificName = "MissingRoutine" })),
        ];
        yield return
        [
            Rebuild(
                baseline,
                routineParameters: Replace(
                    baseline.RoutineParameters,
                    returnRow,
                    returnRow with
                    {
                        Mode = "OUT",
                        Name = "invalid_return",
                    })),
        ];
        yield return
        [
            Rebuild(
                baseline,
                routineParameters: Replace(
                    baseline.RoutineParameters,
                    parameter,
                    parameter with { OrdinalPosition = 2 })),
        ];
        yield return
        [
            Rebuild(
                baseline,
                routineParameters: Replace(
                    baseline.RoutineParameters,
                    parameter,
                    parameter with
                    {
                        DtdIdentifierBytes =
                            parameter.DtdIdentifierBytes + 1,
                    })),
        ];
    }

    private static MigrationCatalog Build(
        MySqlCatalogSnapshot snapshot,
        MySqlInspectionLimits? limits = null) =>
        MySqlCatalogBuilder.Build(
            snapshot,
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion =
                    CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            },
            limits ?? MySqlInspectionLimits.Default,
            Ct);

    private static MigrationCatalogObject FindObject(
        MigrationCatalog catalog,
        MigrationObjectKind kind,
        string sourceName) =>
        Assert.Single(
            catalog.Objects,
            item =>
                item.Kind == kind &&
                item.SourceName == sourceName);

    private static MigrationPlanObject PlanObject(
        MigrationPlan plan,
        MigrationCatalogObject item) =>
        Assert.Single(
            plan.Objects,
            candidate => candidate.SourceObjectId == item.ObjectId);

    private static string? Facet(
        MigrationCatalogObject item,
        string name) =>
        item.Facets.SingleOrDefault(facet =>
            string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal))?.Value;

    private static void AssertFingerprintChanged(
        string expected,
        MySqlCatalogSnapshot changed) =>
        Assert.NotEqual(expected, Build(changed).Source.Fingerprint);

    private static void AssertRawMutation(
        MySqlCatalogSnapshot firstSnapshot,
        MySqlCatalogSnapshot secondSnapshot,
        string firstRaw,
        string secondRaw)
    {
        Assert.Equal(
            System.Text.Encoding.UTF8.GetByteCount(firstRaw),
            System.Text.Encoding.UTF8.GetByteCount(secondRaw));
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

    private static IEnumerable<T> Replace<T>(
        IEnumerable<T> values,
        T existing,
        T replacement)
        where T : class =>
        values.Select(item =>
            ReferenceEquals(item, existing) ? replacement : item);

    private static MySqlCatalogSnapshot Rebuild(
        MySqlCatalogSnapshot source,
        MySqlSessionMetadata? session = null,
        MySqlDatabaseMetadata? database = null,
        IEnumerable<MySqlTableMetadata>? tables = null,
        IEnumerable<MySqlColumnMetadata>? columns = null,
        IEnumerable<MySqlTableDefinitionMetadata>? tableDefinitions = null,
        IEnumerable<MySqlKeyMetadata>? keys = null,
        IEnumerable<MySqlKeyColumnMetadata>? keyColumns = null,
        IEnumerable<MySqlForeignKeyMetadata>? foreignKeys = null,
        IEnumerable<MySqlForeignKeyColumnMetadata>? foreignKeyColumns = null,
        IEnumerable<MySqlCheckMetadata>? checks = null,
        IEnumerable<MySqlIndexMetadata>? indexes = null,
        IEnumerable<MySqlIndexPartMetadata>? indexParts = null,
        IEnumerable<MySqlViewMetadata>? views = null,
        IEnumerable<MySqlViewColumnMetadata>? viewColumns = null,
        IEnumerable<MySqlTriggerMetadata>? triggers = null,
        IEnumerable<MySqlRoutineMetadata>? routines = null,
        IEnumerable<MySqlRoutineParameterMetadata>? routineParameters = null) =>
        new(
            source.EndpointDigest,
            source.ProviderVersion,
            source.Server,
            session ?? source.Session,
            database ?? source.Database,
            tables ?? source.Tables,
            columns ?? source.Columns,
            tableDefinitions ?? source.TableDefinitions,
            keys ?? source.Keys,
            keyColumns ?? source.KeyColumns,
            foreignKeys ?? source.ForeignKeys,
            foreignKeyColumns ?? source.ForeignKeyColumns,
            checks ?? source.Checks,
            indexes ?? source.Indexes,
            indexParts ?? source.IndexParts,
            views ?? source.Views,
            viewColumns ?? source.ViewColumns,
            triggers ?? source.Triggers,
            routines ?? source.Routines,
            routineParameters ?? source.RoutineParameters);
}
