using CSharpDB.Migration;
using CSharpDB.Migration.SqlServer;

namespace CSharpDB.Migration.SqlServer.Tests;

public sealed class SqlServerTsqlAnalyzerTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(15, 150, "tsql150")]
    [InlineData(16, 160, "tsql160")]
    [InlineData(17, 170, "tsql170")]
    public void AnalyzeUsesQualifiedParserLane(
        int productMajorVersion,
        int compatibilityLevel,
        string grammar)
    {
        SqlServerCatalogSnapshot baseline = SqlServerTestSnapshot.Create();
        SqlServerCatalogSnapshot snapshot = Rebuild(
            baseline,
            instance: baseline.Instance with
            {
                ProductMajorVersion = productMajorVersion,
                ProductVersion = $"{productMajorVersion}.0.1000.1",
            },
            database: baseline.Database with
            {
                CompatibilityLevel = checked((short)compatibilityLevel),
            });

        SqlServerScriptDomAnalysisSnapshot analysis = Analyze(snapshot);

        Assert.Equal(10, analysis.Definitions.Count);
        Assert.All(
            analysis.Definitions,
            item =>
            {
                Assert.Equal(grammar, item.Grammar);
                Assert.Equal(SqlServerScriptDomStatus.Parsed, item.Status);
                Assert.True(item.TokenCount > 0);
                Assert.True(item.NodeCount > 0);
                Assert.StartsWith(
                    "sha256:",
                    item.SourceDigest,
                    StringComparison.Ordinal);
                Assert.StartsWith(
                    "sha256:",
                    item.AnalysisDigest,
                    StringComparison.Ordinal);
            });
    }

    [Theory]
    [InlineData(15, 160)]
    [InlineData(16, 150)]
    [InlineData(17, 160)]
    [InlineData(18, 180)]
    public void AnalyzeDoesNotGuessAnUnqualifiedDialect(
        int productMajorVersion,
        int compatibilityLevel)
    {
        SqlServerCatalogSnapshot baseline = SqlServerTestSnapshot.Create();
        SqlServerCatalogSnapshot snapshot = Rebuild(
            baseline,
            instance: baseline.Instance with
            {
                ProductMajorVersion = productMajorVersion,
            },
            database: baseline.Database with
            {
                CompatibilityLevel = checked((short)compatibilityLevel),
            });

        SqlServerScriptDomAnalysisSnapshot analysis = Analyze(snapshot);

        Assert.NotEmpty(analysis.Definitions);
        Assert.All(
            analysis.Definitions,
            item =>
            {
                Assert.Equal(
                    SqlServerScriptDomStatus.DialectUnqualified,
                    item.Status);
                Assert.Equal("unqualified", item.Grammar);
                Assert.Equal(0, item.TokenCount);
                Assert.Equal(0, item.NodeCount);
                Assert.Equal(0, item.StatementCount);
                Assert.Equal(SqlServerScriptDomRootKind.None, item.RootKind);
            });
    }

    [Fact]
    public void AnalyzeHonorsModuleAndDatabaseQuotedIdentifierSettings()
    {
        const string source =
            "CREATE VIEW \"dbo\".\"OrderSummary\" AS " +
            "SELECT 1 AS \"Value\"";
        SqlServerCatalogSnapshot baseline = SqlServerTestSnapshot.Create();
        SqlServerModuleMetadata view = WithDefinition(
            Module(baseline, 5_000),
            source) with
        {
            UsesQuotedIdentifier = true,
        };
        SqlServerCatalogSnapshot quotedOn = Rebuild(
            baseline,
            database: baseline.Database with
            {
                IsQuotedIdentifierOn = false,
            },
            modules: ReplaceModule(baseline, view));

        SqlServerScriptDomDefinitionAnalysis moduleAnalysis = Find(
            Analyze(quotedOn),
            SqlServerScriptDomDefinitionKind.Module,
            5_000);
        SqlServerScriptDomDefinitionAnalysis defaultAnalysis = Find(
            Analyze(quotedOn),
            SqlServerScriptDomDefinitionKind.DefaultExpression,
            100,
            2);

        Assert.True(moduleAnalysis.QuotedIdentifiers);
        Assert.Equal(SqlServerScriptDomStatus.Parsed, moduleAnalysis.Status);
        Assert.False(defaultAnalysis.QuotedIdentifiers);

        SqlServerModuleMetadata quotedOffView = view with
        {
            UsesQuotedIdentifier = false,
        };
        SqlServerScriptDomDefinitionAnalysis quotedOffAnalysis = Find(
            Analyze(Rebuild(
                quotedOn,
                modules: ReplaceModule(quotedOn, quotedOffView))),
            SqlServerScriptDomDefinitionKind.Module,
            5_000);
        Assert.False(quotedOffAnalysis.QuotedIdentifiers);
        Assert.NotEqual(SqlServerScriptDomStatus.Parsed, quotedOffAnalysis.Status);
    }

    [Fact]
    public void AnalyzeRecognizesEveryDefinitionCategoryAndExpectedRoot()
    {
        SqlServerScriptDomAnalysisSnapshot analysis =
            Analyze(SqlServerTestSnapshot.Create());

        AssertModuleRoot(
            analysis,
            5_000,
            SqlServerScriptDomRootKind.View);
        AssertModuleRoot(
            analysis,
            6_000,
            SqlServerScriptDomRootKind.Trigger);
        AssertModuleRoot(
            analysis,
            6_001,
            SqlServerScriptDomRootKind.Trigger);
        AssertModuleRoot(
            analysis,
            7_000,
            SqlServerScriptDomRootKind.Procedure);
        AssertModuleRoot(
            analysis,
            7_002,
            SqlServerScriptDomRootKind.ScalarFunction);
        AssertExpressionRoot(
            analysis,
            SqlServerScriptDomDefinitionKind.DefaultExpression,
            100,
            2,
            SqlServerScriptDomRootKind.ScalarExpression);
        AssertExpressionRoot(
            analysis,
            SqlServerScriptDomDefinitionKind.ComputedExpression,
            100,
            4,
            SqlServerScriptDomRootKind.ScalarExpression);
        AssertExpressionRoot(
            analysis,
            SqlServerScriptDomDefinitionKind.CheckPredicate,
            3_000,
            0,
            SqlServerScriptDomRootKind.BooleanExpression);
        AssertExpressionRoot(
            analysis,
            SqlServerScriptDomDefinitionKind.CheckPredicate,
            3_001,
            0,
            SqlServerScriptDomRootKind.BooleanExpression);
        AssertExpressionRoot(
            analysis,
            SqlServerScriptDomDefinitionKind.IndexFilterPredicate,
            100,
            5,
            SqlServerScriptDomRootKind.BooleanExpression);
    }

    [Fact]
    public void WrongModuleNameProducesDurableRootMismatchBlocker()
    {
        SqlServerCatalogSnapshot snapshot = WithViewDefinition(
            "CREATE VIEW [dbo].[OtherName] AS SELECT 1 AS [Value]");

        SqlServerScriptDomDefinitionAnalysis analysis = Find(
            Analyze(snapshot),
            SqlServerScriptDomDefinitionKind.Module,
            5_000);

        Assert.Equal(SqlServerScriptDomStatus.RootMismatch, analysis.Status);
        Assert.Equal(SqlServerScriptDomRootKind.View, analysis.ExpectedRootKind);
        Assert.Equal(SqlServerScriptDomRootKind.View, analysis.RootKind);

        MigrationCatalog catalog = Build(snapshot);
        MigrationCatalogObject view = FindObject(
            catalog,
            MigrationObjectKind.View,
            "OrderSummary");
        Assert.Equal(
            "root-mismatch",
            Facet(view, "sqlServerModuleAnalysis"));
        Assert.Contains(
            catalog.Diagnostics,
            item =>
                item.ObjectId == view.ObjectId &&
                item.RuleId == "MIG-SQLSERVER-TSQL-ROOT-MISMATCH-001");
    }

    [Fact]
    public void UnqualifiedModuleNameUsesCatalogSchemaButWrongExplicitSchemaDoesNot()
    {
        SqlServerScriptDomDefinitionAnalysis unqualified = Find(
            Analyze(WithViewDefinition(
                "CREATE VIEW [OrderSummary] AS SELECT 1 AS [Value]")),
            SqlServerScriptDomDefinitionKind.Module,
            5_000);
        Assert.Equal(SqlServerScriptDomStatus.Parsed, unqualified.Status);
        Assert.Equal(SqlServerScriptDomRootKind.View, unqualified.RootKind);

        SqlServerScriptDomDefinitionAnalysis wrongSchema = Find(
            Analyze(WithViewDefinition(
                "CREATE VIEW [other].[OrderSummary] AS SELECT 1 AS [Value]")),
            SqlServerScriptDomDefinitionKind.Module,
            5_000);
        Assert.Equal(SqlServerScriptDomStatus.RootMismatch, wrongSchema.Status);
        Assert.Equal(SqlServerScriptDomRootKind.View, wrongSchema.RootKind);
    }

    [Fact]
    public void WrongModuleKindAndMultipleRootsAreRejected()
    {
        SqlServerScriptDomDefinitionAnalysis wrongKind = Find(
            Analyze(WithViewDefinition(
                "CREATE PROCEDURE [dbo].[OrderSummary] AS SELECT 1")),
            SqlServerScriptDomDefinitionKind.Module,
            5_000);
        Assert.Equal(SqlServerScriptDomStatus.RootMismatch, wrongKind.Status);
        Assert.Equal(SqlServerScriptDomRootKind.View, wrongKind.ExpectedRootKind);
        Assert.Equal(SqlServerScriptDomRootKind.Procedure, wrongKind.RootKind);

        SqlServerScriptDomDefinitionAnalysis multipleRoots = Find(
            Analyze(WithViewDefinition(
                "CREATE VIEW [dbo].[OrderSummary] AS SELECT 1 AS [Value]\n" +
                "GO\n" +
                "CREATE VIEW [dbo].[Other] AS SELECT 2 AS [Value]")),
            SqlServerScriptDomDefinitionKind.Module,
            5_000);
        Assert.Equal(
            SqlServerScriptDomStatus.RootMismatch,
            multipleRoots.Status);
        Assert.Equal(SqlServerScriptDomRootKind.None, multipleRoots.RootKind);
        Assert.True(multipleRoots.StatementCount >= 2);
    }

    [Fact]
    public void FunctionReturnShapeMustMatchCatalogObjectType()
    {
        SqlServerCatalogSnapshot baseline = SqlServerTestSnapshot.Create();
        SqlServerModuleMetadata function = WithDefinition(
            Module(baseline, 7_002),
            "CREATE FUNCTION [dbo].[ufn_OrderAmount](@OrderId int) " +
            "RETURNS TABLE AS RETURN " +
            "(SELECT @OrderId AS [OrderAmount])");
        SqlServerCatalogSnapshot snapshot = Rebuild(
            baseline,
            modules: ReplaceModule(baseline, function));

        SqlServerScriptDomDefinitionAnalysis analysis = Find(
            Analyze(snapshot),
            SqlServerScriptDomDefinitionKind.Module,
            7_002);

        Assert.Equal(SqlServerScriptDomStatus.RootMismatch, analysis.Status);
        Assert.Equal(
            SqlServerScriptDomRootKind.ScalarFunction,
            analysis.ExpectedRootKind);
        Assert.Equal(
            SqlServerScriptDomRootKind.TableValuedFunction,
            analysis.RootKind);
    }

    [Fact]
    public void SyntaxFailurePersistsOnlySanitizedNumericEvidence()
    {
        const string secret = "ParsePassword=NeverPersistThis";
        SqlServerCatalogSnapshot snapshot = WithViewDefinition(
            "CREATE VIEW [dbo].[OrderSummary] AS " +
            $"SELECT N'{secret}' AS [Secret] FROM");

        SqlServerScriptDomDefinitionAnalysis analysis = Find(
            Analyze(snapshot),
            SqlServerScriptDomDefinitionKind.Module,
            5_000);

        Assert.Equal(SqlServerScriptDomStatus.ParserError, analysis.Status);
        Assert.True(analysis.ParseErrorCount > 0);
        Assert.NotNull(analysis.FirstErrorNumber);
        Assert.NotNull(analysis.FirstErrorOffset);
        Assert.NotNull(analysis.FirstErrorLine);
        Assert.NotNull(analysis.FirstErrorColumn);

        MigrationCatalog catalog = Build(snapshot);
        string serialized = MigrationArtifactSerializer.SerializeCatalog(catalog);
        Assert.DoesNotContain(secret, serialized, StringComparison.Ordinal);
        Assert.DoesNotContain(
            "NeverPersistThis",
            serialized,
            StringComparison.Ordinal);
        MigrationDiagnostic diagnostic = Assert.Single(
            catalog.Diagnostics,
            item => item.RuleId == "MIG-SQLSERVER-TSQL-SYNTAX-001");
        Assert.StartsWith(
            "sha256:",
            diagnostic.SourceSpan?.SourceId,
            StringComparison.Ordinal);
        Assert.Equal(analysis.FirstErrorOffset, diagnostic.SourceSpan?.Start);
        Assert.Equal(analysis.FirstErrorLine, diagnostic.SourceSpan?.Line);
        Assert.Equal(analysis.FirstErrorColumn, diagnostic.SourceSpan?.Column);
    }

    [Fact]
    public void MissingDefinitionsAreNotAnalyzed()
    {
        SqlServerCatalogSnapshot baseline = SqlServerTestSnapshot.Create();

        Assert.False(Analyze(baseline).TryGet(
            new(
                SqlServerScriptDomDefinitionKind.Module,
                7_001,
                0),
            out _));

        SqlServerModuleMetadata unavailable = WithDefinition(
            Module(baseline, 5_000),
            null);
        SqlServerScriptDomAnalysisSnapshot analysis = Analyze(Rebuild(
            baseline,
            modules: ReplaceModule(baseline, unavailable)));

        Assert.False(analysis.TryGet(
            new(
                SqlServerScriptDomDefinitionKind.Module,
                5_000,
                0),
            out _));
        Assert.Equal(9, analysis.Definitions.Count);
    }

    [Fact]
    public void DefinitionOrderDoesNotChangeAnalysisOrDigests()
    {
        SqlServerCatalogSnapshot baseline = SqlServerTestSnapshot.Create();
        SqlServerCatalogSnapshot reversed = Rebuild(
            baseline,
            columns: baseline.Columns.Reverse(),
            indexes: baseline.Indexes.Reverse(),
            checks: baseline.Checks.Reverse(),
            modules: baseline.Modules.Reverse());

        SqlServerScriptDomDefinitionAnalysis[] first =
            Analyze(baseline).Definitions.ToArray();
        SqlServerScriptDomDefinitionAnalysis[] second =
            Analyze(reversed).Definitions.ToArray();

        Assert.Equal(first, second);
        Assert.Equal(
            first
                .Select(static item => item.Key)
                .OrderBy(static key => key.Kind)
                .ThenBy(static key => key.ObjectId)
                .ThenBy(static key => key.SubObjectId),
            first.Select(static item => item.Key));
    }

    [Theory]
    [InlineData("input")]
    [InlineData("token")]
    [InlineData("node")]
    [InlineData("nesting")]
    [InlineData("statement")]
    public void PerDefinitionLimitsFailClosed(string limit)
    {
        SqlServerCatalogSnapshot snapshot = limit == "statement"
            ? WithViewDefinition(
                "CREATE VIEW [dbo].[OrderSummary] AS SELECT 1 AS [Value]\n" +
                "GO\n" +
                "CREATE VIEW [dbo].[Other] AS SELECT 2 AS [Value]")
            : SqlServerTestSnapshot.Create();
        SqlServerInspectionLimits limits = limit switch
        {
            "input" => SqlServerInspectionLimits.Default with
            {
                MaxExpressionBytes = 8,
            },
            "token" => SqlServerInspectionLimits.Default with
            {
                MaxScriptDomTokensPerDefinition = 5,
            },
            "node" => SqlServerInspectionLimits.Default with
            {
                MaxScriptDomNodesPerDefinition = 5,
            },
            "nesting" => SqlServerInspectionLimits.Default with
            {
                MaxScriptDomNestingPerDefinition = 1,
            },
            "statement" => SqlServerInspectionLimits.Default with
            {
                MaxScriptDomStatementsPerDefinition = 1,
            },
            _ => throw new InvalidOperationException("Unknown test limit."),
        };
        SqlServerScriptDomDefinitionAnalysis analysis = limit == "nesting"
            ? Find(
                Analyze(snapshot, limits),
                SqlServerScriptDomDefinitionKind.DefaultExpression,
                100,
                2)
            : Find(
                Analyze(snapshot, limits),
                SqlServerScriptDomDefinitionKind.Module,
                5_000);

        SqlServerScriptDomStatus expectedStatus = limit switch
        {
            "input" => SqlServerScriptDomStatus.InputLimitExceeded,
            "token" => SqlServerScriptDomStatus.TokenLimitExceeded,
            "node" => SqlServerScriptDomStatus.NodeLimitExceeded,
            "nesting" => SqlServerScriptDomStatus.NestingLimitExceeded,
            "statement" => SqlServerScriptDomStatus.StatementLimitExceeded,
            _ => throw new InvalidOperationException("Unknown test limit."),
        };
        Assert.Equal(
            expectedStatus,
            analysis.Status);
        Assert.NotEqual(SqlServerScriptDomStatus.Parsed, analysis.Status);
    }

    [Fact]
    public void ParseErrorAndAggregateLimitsFailClosed()
    {
        SqlServerCatalogSnapshot malformed = WithViewDefinition(
            "SELECT FROM\n" +
            "GO\n" +
            "SELECT FROM\n" +
            "GO\n" +
            "SELECT FROM");
        SqlServerInspectionLimits parseErrorLimit =
            SqlServerInspectionLimits.Default with
            {
                MaxScriptDomParseErrorsPerDefinition = 1,
            };

        SqlServerScriptDomDefinitionAnalysis analysis = Find(
            Analyze(malformed, parseErrorLimit),
            SqlServerScriptDomDefinitionKind.Module,
            5_000);
        Assert.Equal(
            SqlServerScriptDomStatus.ParseErrorLimitExceeded,
            analysis.Status);
        Assert.True(analysis.ParseErrorCount > 1);

        SqlServerInspectionLimits aggregateTokenLimit =
            SqlServerInspectionLimits.Default with
            {
                MaxScriptDomTokensTotal = 1,
            };
        SqlServerMigrationException exception =
            Assert.Throws<SqlServerMigrationException>(
                () => Analyze(
                    SqlServerTestSnapshot.Create(),
                    aggregateTokenLimit));
        Assert.Contains(
            "aggregate ScriptDom token inspection limit",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void AnalyzeObservesCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.Throws<OperationCanceledException>(
            () => SqlServerScriptDomAnalyzer.Analyze(
                SqlServerTestSnapshot.Create(),
                SqlServerInspectionLimits.Default,
                cancellation.Token));
    }

    [Fact]
    public void ParsedEvidenceDoesNotOverclaimTargetCompatibility()
    {
        MigrationCatalog catalog = Build(SqlServerTestSnapshot.Create());
        MigrationCatalogObject view = FindObject(
            catalog,
            MigrationObjectKind.View,
            "OrderSummary");
        MigrationCatalogObject amount = FindTableColumn(catalog, "Amount");
        MigrationCatalogObject check = FindObject(
            catalog,
            MigrationObjectKind.CheckConstraint,
            "CK_Orders_Amount");

        Assert.Equal("parsed", Facet(view, "sqlServerModuleAnalysis"));
        Assert.Equal("tsql160", Facet(view, "sqlServerModuleTsqlGrammar"));
        Assert.Null(Facet(view, "targetSql"));
        Assert.Null(Facet(view, "deterministic"));
        Assert.Null(Facet(view, "rowLocal"));
        Assert.Equal(
            "parsed",
            Facet(amount, "sqlServerDefaultTsqlAnalysis"));
        Assert.Null(Facet(amount, "defaultValue"));
        Assert.Equal(
            "parsed",
            Facet(check, "sqlServerCheckTsqlAnalysis"));
        Assert.Null(Facet(check, "targetSql"));
        Assert.Null(Facet(check, "deterministic"));
        Assert.Null(Facet(check, "rowLocal"));
        Assert.Contains(
            catalog.Diagnostics,
            item =>
                item.ObjectId == view.ObjectId &&
                item.RuleId ==
                    "MIG-SQLSERVER-TSQL-PARSED-NOT-LOWERED-001");
        Assert.Contains(
            catalog.Diagnostics,
            item =>
                item.ObjectId == check.ObjectId &&
                item.RuleId ==
                    "MIG-SQLSERVER-TSQL-PARSED-NOT-LOWERED-001");
    }

    private static SqlServerScriptDomAnalysisSnapshot Analyze(
        SqlServerCatalogSnapshot snapshot,
        SqlServerInspectionLimits? limits = null) =>
        SqlServerScriptDomAnalyzer.Analyze(
            snapshot,
            limits ?? SqlServerInspectionLimits.Default,
            Ct);

    private static SqlServerScriptDomDefinitionAnalysis Find(
        SqlServerScriptDomAnalysisSnapshot snapshot,
        SqlServerScriptDomDefinitionKind kind,
        int objectId,
        int subObjectId = 0)
    {
        Assert.True(snapshot.TryGet(
            new(kind, objectId, subObjectId),
            out SqlServerScriptDomDefinitionAnalysis? analysis));
        return Assert.IsType<SqlServerScriptDomDefinitionAnalysis>(analysis);
    }

    private static void AssertModuleRoot(
        SqlServerScriptDomAnalysisSnapshot snapshot,
        int objectId,
        SqlServerScriptDomRootKind root)
    {
        SqlServerScriptDomDefinitionAnalysis analysis = Find(
            snapshot,
            SqlServerScriptDomDefinitionKind.Module,
            objectId);
        Assert.Equal(SqlServerScriptDomStatus.Parsed, analysis.Status);
        Assert.Equal(root, analysis.ExpectedRootKind);
        Assert.Equal(root, analysis.RootKind);
    }

    private static void AssertExpressionRoot(
        SqlServerScriptDomAnalysisSnapshot snapshot,
        SqlServerScriptDomDefinitionKind kind,
        int objectId,
        int subObjectId,
        SqlServerScriptDomRootKind root)
    {
        SqlServerScriptDomDefinitionAnalysis analysis = Find(
            snapshot,
            kind,
            objectId,
            subObjectId);
        Assert.Equal(SqlServerScriptDomStatus.Parsed, analysis.Status);
        Assert.Equal(root, analysis.ExpectedRootKind);
        Assert.Equal(root, analysis.RootKind);
    }

    private static SqlServerCatalogSnapshot WithViewDefinition(string source)
    {
        SqlServerCatalogSnapshot baseline = SqlServerTestSnapshot.Create();
        SqlServerModuleMetadata view = WithDefinition(
            Module(baseline, 5_000),
            source);
        return Rebuild(
            baseline,
            modules: ReplaceModule(baseline, view));
    }

    private static SqlServerModuleMetadata WithDefinition(
        SqlServerModuleMetadata module,
        string? definition) =>
        module with
        {
            Definition = definition,
            DefinitionBytes = definition is null
                ? null
                : checked(definition.Length * 2L),
        };

    private static SqlServerModuleMetadata Module(
        SqlServerCatalogSnapshot snapshot,
        int objectId) =>
        Assert.Single(
            snapshot.Modules,
            item => item.ObjectId == objectId);

    private static IEnumerable<SqlServerModuleMetadata> ReplaceModule(
        SqlServerCatalogSnapshot snapshot,
        SqlServerModuleMetadata replacement) =>
        snapshot.Modules.Select(
            item => item.ObjectId == replacement.ObjectId
                ? replacement
                : item);

    private static MigrationCatalog Build(SqlServerCatalogSnapshot snapshot) =>
        SqlServerCatalogBuilder.Build(
            snapshot,
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion =
                    CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = false,
            },
            SqlServerInspectionLimits.Default,
            Ct);

    private static MigrationCatalogObject FindObject(
        MigrationCatalog catalog,
        MigrationObjectKind kind,
        string name) =>
        Assert.Single(
            catalog.Objects,
            item =>
                item.Kind == kind &&
                item.SourceName == name);

    private static MigrationCatalogObject FindTableColumn(
        MigrationCatalog catalog,
        string name)
    {
        IReadOnlySet<string> tableIds = catalog.Objects
            .Where(static item => item.Kind == MigrationObjectKind.Table)
            .Select(static item => item.ObjectId)
            .ToHashSet(StringComparer.Ordinal);
        return Assert.Single(
            catalog.Objects,
            item =>
                item.Kind == MigrationObjectKind.Column &&
                item.SourceName == name &&
                item.ParentObjectId is not null &&
                tableIds.Contains(item.ParentObjectId));
    }

    private static string? Facet(MigrationCatalogObject item, string name) =>
        item.Facets.SingleOrDefault(
            facet => string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal))?.Value;

    private static SqlServerCatalogSnapshot Rebuild(
        SqlServerCatalogSnapshot source,
        SqlServerInstanceMetadata? instance = null,
        SqlServerDatabaseMetadata? database = null,
        IEnumerable<SqlServerColumnMetadata>? columns = null,
        IEnumerable<SqlServerIndexMetadata>? indexes = null,
        IEnumerable<SqlServerCheckMetadata>? checks = null,
        IEnumerable<SqlServerModuleMetadata>? modules = null) =>
        new(
            source.EndpointDigest,
            source.ProviderVersion,
            instance ?? source.Instance,
            database ?? source.Database,
            source.Schemas,
            source.Tables,
            columns ?? source.Columns,
            source.Keys,
            indexes ?? source.Indexes,
            source.IndexColumns,
            source.ForeignKeys,
            source.ForeignKeyColumns,
            checks ?? source.Checks,
            source.Sequences,
            source.PermissionAuditBefore,
            source.PermissionAuditAfter,
            source.Views,
            source.ViewColumns,
            source.Triggers,
            source.TriggerEvents,
            source.Routines,
            modules ?? source.Modules,
            source.Parameters,
            source.ExpressionDependencyAudit);
}
