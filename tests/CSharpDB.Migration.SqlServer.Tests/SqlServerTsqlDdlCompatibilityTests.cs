using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration.CSharpDb;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace CSharpDB.Migration.SqlServer.Tests;

public sealed class SqlServerTsqlDdlCompatibilityTests
{
    [Fact]
    public async Task IntegralTable_PassesScratchAsRewrite()
    {
        const string script =
            "CREATE TABLE dbo.widgets (id int NOT NULL PRIMARY KEY, value bigint NULL);";

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal("tsql", report.Dialect);
        Assert.Equal("tsql160", report.SourceGrammar);
        Assert.Equal(ExpectedDigest(script), report.ScriptDigest);
        Assert.Equal(
            MigrationCompatibilityStatus.CompatibleWithRewrite,
            report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            report.HighestEvidence);
        Assert.Equal(1, report.StatementCount);
        Assert.Equal(1, report.ProvenStatementCount);
        Assert.NotNull(report.GeneratedDdlDigest);
        Assert.Equal(
            report.ExpectedSchemaDigest,
            report.ActualSchemaDigest);
    }

    [Fact]
    public async Task TextColumn_PassesTargetShapeButRetainsConditional()
    {
        const string script =
            "CREATE TABLE dbo.widgets (id bigint NOT NULL PRIMARY KEY, label nvarchar(100) NULL);";

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(MigrationCompatibilityStatus.Conditional, report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            report.HighestEvidence);
        Assert.Equal(
            SqlServerTsqlDdlCompatibilityAnalyzer.TextCollationRuleId,
            report.RuleId);
        Assert.Contains(
            report.Diagnostics,
            diagnostic => diagnostic.RuleId ==
                SqlServerTsqlDdlCompatibilityAnalyzer.TextCollationRuleId);
    }

    [Theory]
    [InlineData(
        "bit",
        MigrationCompatibilityStatus.CompatibleWithRewrite)]
    [InlineData(
        "decimal(18, 2)",
        MigrationCompatibilityStatus.CompatibleWithRewrite)]
    [InlineData(
        "float(24)",
        MigrationCompatibilityStatus.CompatibleWithRewrite)]
    [InlineData(
        "varbinary(max)",
        MigrationCompatibilityStatus.CompatibleWithRewrite)]
    [InlineData(
        "uniqueidentifier",
        MigrationCompatibilityStatus.CompatibleWithRewrite)]
    [InlineData(
        "date",
        MigrationCompatibilityStatus.CompatibleWithRewrite)]
    [InlineData(
        "time(3)",
        MigrationCompatibilityStatus.CompatibleWithRewrite)]
    [InlineData(
        "datetime2(7)",
        MigrationCompatibilityStatus.CompatibleWithRewrite)]
    [InlineData(
        "datetimeoffset(7)",
        MigrationCompatibilityStatus.CompatibleWithRewrite)]
    [InlineData(
        "varchar(50)",
        MigrationCompatibilityStatus.Conditional)]
    public async Task AllowlistedScalarTypeFamily_PassesScratch(
        string sourceType,
        MigrationCompatibilityStatus expectedStatus)
    {
        string script = string.Concat(
            "CREATE TABLE dbo.scalar_values (value ",
            sourceType,
            " NULL);");

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(expectedStatus, report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            report.HighestEvidence);
        Assert.NotNull(report.GeneratedDdlDigest);
    }

    [Theory]
    [InlineData("rowversion")]
    [InlineData("timestamp")]
    public async Task RowVersionType_PassesScratchAsGeneratedLogicalType(
        string sourceType)
    {
        string script = string.Concat(
            "CREATE TABLE dbo.versioned_values (value ",
            sourceType,
            ");");

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(
            MigrationCompatibilityStatus.CompatibleWithRewrite,
            report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            report.HighestEvidence);
        Assert.NotNull(report.GeneratedDdlDigest);
        Assert.Equal(report.ExpectedSchemaDigest, report.ActualSchemaDigest);
    }

    [Fact]
    public void LoweredCatalog_PreservesSqlServerLogicalTargetTypes()
    {
        const string script =
            """
            CREATE TABLE dbo.logical_values (
                tiny_value tinyint NOT NULL,
                small_value smallint NOT NULL,
                int_value int NOT NULL,
                big_value bigint NOT NULL,
                flag_value bit NOT NULL,
                time_value datetime2(3) NOT NULL,
                offset_value datetimeoffset(4) NOT NULL,
                version_value rowversion NOT NULL
            );
            """;

        MigrationCatalog catalog = LowerCatalog(script);
        var expectedTypes = new Dictionary<string, string>(
            StringComparer.Ordinal)
        {
            ["tiny_value"] = "TINYINT",
            ["small_value"] = "SMALLINT",
            ["int_value"] = "INTEGER",
            ["big_value"] = "BIGINT",
            ["flag_value"] = "BOOLEAN",
            ["time_value"] = "DATETIME2(3)",
            ["offset_value"] = "DATETIMEOFFSET(4)",
            ["version_value"] = "ROWVERSION",
        };
        var provider = new StandardDataTypeMappingProvider();

        foreach (MigrationCatalogObject column in catalog.Objects.Where(
                     static item => item.Kind == MigrationObjectKind.Column))
        {
            MigrationTypeMapping mapping = provider.Map(
                new MigrationTypeMappingRequest
                {
                    SourceObject = column,
                    Profile = MigrationMappingProfile.Preserve,
                    Coverage = new MigrationProfileCoverage
                    {
                        Kind = MigrationCoverageKind.None,
                        RequiresFullStreamValidation = true,
                    },
                }).Mapping;

            Assert.Equal(expectedTypes[column.SourceName!], mapping.TargetSqlType);
        }

        MigrationCatalogObject rowVersion = Assert.Single(
            catalog.Objects,
            static item => item.SourceName == "version_value");
        Assert.Contains(
            rowVersion.Facets,
            static facet => facet.Name == "rowVersion" && facet.Value == "true");
    }

    [Fact]
    public async Task OrderedKeysForeignKeyAndIndex_PassAcrossGoBatches()
    {
        const string script =
            """
            CREATE TABLE dbo.parents (
                id bigint NOT NULL,
                CONSTRAINT pk_parents PRIMARY KEY (id ASC)
            );
            GO
            CREATE TABLE dbo.children (
                id bigint NOT NULL PRIMARY KEY,
                parent_id bigint NULL,
                CONSTRAINT fk_children_parent FOREIGN KEY (parent_id)
                    REFERENCES dbo.parents(id) ON DELETE CASCADE
            );
            CREATE INDEX ix_children_parent
                ON dbo.children(parent_id ASC);
            """;

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(
            MigrationCompatibilityStatus.CompatibleWithRewrite,
            report.Status);
        Assert.Equal(3, report.StatementCount);
        Assert.All(
            report.Statements,
            statement => Assert.Equal(
                MigrationEvidenceLevel.ScratchExecuted,
                statement.Evidence));
    }

    [Theory]
    [InlineData("CREATE TABLE widgets (id int NOT NULL);")]
    [InlineData("CREATE TABLE sales.widgets (id int NOT NULL);")]
    [InlineData("CREATE TABLE DBO.widgets (id int NOT NULL);")]
    [InlineData("CREATE TABLE dbo.widgets (id int);")]
    [InlineData("CREATE TABLE dbo.widgets (id int NOT NULL DEFAULT 1);")]
    [InlineData("CREATE TABLE dbo.widgets (id int IDENTITY NOT NULL);")]
    [InlineData("CREATE TABLE dbo.widgets (value xml NOT NULL);")]
    [InlineData("CREATE TABLE dbo.widgets (value custom_type NOT NULL);")]
    [InlineData("DROP TABLE dbo.widgets;")]
    [InlineData("CREATE TABLE dbo.widgets (label nvarchar(20) NOT NULL PRIMARY KEY);")]
    [InlineData("CREATE TABLE dbo.widgets (id bit NOT NULL PRIMARY KEY);")]
    [InlineData("CREATE TABLE dbo.widgets (id int NULL UNIQUE);")]
    public async Task UnsupportedOrAmbiguousShape_FailsBeforeScratch(
        string script)
    {
        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(MigrationCompatibilityStatus.Unsupported, report.Status);
        Assert.Equal(MigrationEvidenceLevel.Parsed, report.HighestEvidence);
        Assert.Null(report.GeneratedDdlDigest);
        Assert.Single(report.Diagnostics);
        Assert.DoesNotContain(
            script,
            report.Diagnostics[0].Summary,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task ForwardForeignKeyDependency_IsRejected()
    {
        const string script =
            """
            CREATE TABLE dbo.children (
                id bigint NOT NULL PRIMARY KEY,
                parent_id bigint NULL,
                FOREIGN KEY (parent_id) REFERENCES dbo.parents(id)
            );
            CREATE TABLE dbo.parents (id bigint NOT NULL PRIMARY KEY);
            """;

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(MigrationCompatibilityStatus.Unsupported, report.Status);
        Assert.Contains(
            report.Diagnostics,
            diagnostic => diagnostic.RuleId ==
                SqlServerTsqlDdlCompatibilityAnalyzer.InvalidReferenceRuleId);
        Assert.Null(report.CatalogDigest);
    }

    [Fact]
    public async Task CaseMismatchedReference_IsRejectedWithoutCollationInference()
    {
        const string script =
            """
            CREATE TABLE dbo.Parents (Id bigint NOT NULL PRIMARY KEY);
            CREATE TABLE dbo.children (
                id bigint NOT NULL PRIMARY KEY,
                parent_id bigint NULL,
                FOREIGN KEY (parent_id) REFERENCES dbo.parents(Id)
            );
            """;

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(MigrationCompatibilityStatus.Unsupported, report.Status);
        Assert.Contains(
            report.Diagnostics,
            diagnostic => diagnostic.RuleId ==
                SqlServerTsqlDdlCompatibilityAnalyzer.InvalidReferenceRuleId);
        Assert.Null(report.GeneratedDdlDigest);
    }

    [Fact]
    public async Task UnsupportedSuffix_PreventsSupportedPrefixProof()
    {
        const string script =
            "CREATE TABLE dbo.widgets (id int NOT NULL); DROP TABLE dbo.widgets;";

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(MigrationCompatibilityStatus.Unsupported, report.Status);
        Assert.Equal(2, report.StatementCount);
        Assert.Equal(0, report.ProvenStatementCount);
        Assert.Null(report.GeneratedDdlDigest);
        Assert.Single(report.Diagnostics);
    }

    [Fact]
    public async Task StatementSpan_IsExactAndSanitized()
    {
        const string script =
            "-- private comment\r\nCREATE TABLE dbo.widgets (id int NOT NULL); -- tail";
        int start = script.IndexOf("CREATE", StringComparison.Ordinal);
        int length = script.IndexOf(';', start) - start + 1;

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken: TestContext.Current.CancellationToken);

        CSharpDbDdlCompatibilityStatement statement =
            Assert.Single(report.Statements);
        Assert.Equal("input", statement.Span.SourceId);
        Assert.Equal(start, statement.Span.Start);
        Assert.Equal(length, statement.Span.Length);
        Assert.Equal(2, statement.Span.Line);
        Assert.Equal(1, statement.Span.Column);
    }

    [Fact]
    public async Task ReducedBounds_ReturnUnknownWithoutScratch()
    {
        const string script =
            "CREATE TABLE dbo.widgets (id int NOT NULL);";
        var options = new SqlServerTsqlDdlCompatibilityOptions
        {
            MaxTokenCount = 2,
        };

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                options,
                TestContext.Current.CancellationToken);

        Assert.Equal(MigrationCompatibilityStatus.Unknown, report.Status);
        Assert.Equal(
            SqlServerTsqlDdlCompatibilityAnalyzer.LimitRuleId,
            report.RuleId);
        Assert.Null(report.HighestEvidence);
        Assert.Null(report.GeneratedDdlDigest);
    }

    [Fact]
    public async Task PreflightLexicalBudget_RejectsBeforeScriptDomAllocation()
    {
        const string script = "token token token token";
        var options = new SqlServerTsqlDdlCompatibilityOptions
        {
            MaxTokenCount = 3,
        };

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                options,
                TestContext.Current.CancellationToken);

        Assert.Equal(MigrationCompatibilityStatus.Unknown, report.Status);
        Assert.Equal(
            SqlServerTsqlDdlCompatibilityAnalyzer.LimitRuleId,
            report.RuleId);
        Assert.Null(report.HighestEvidence);
    }

    [Fact]
    public async Task PreflightLexicalBudget_RejectsDensePunctuation()
    {
        const string script = "++++";
        var options = new SqlServerTsqlDdlCompatibilityOptions
        {
            MaxTokenCount = 3,
        };

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                options,
                TestContext.Current.CancellationToken);

        Assert.Equal(MigrationCompatibilityStatus.Unknown, report.Status);
        Assert.Equal(
            SqlServerTsqlDdlCompatibilityAnalyzer.LimitRuleId,
            report.RuleId);
        Assert.Null(report.HighestEvidence);
    }

    [Fact]
    public async Task PreflightLexicalBudget_DoesNotSplitUnicodeIdentifiers()
    {
        const string script = "😀a 😀a";
        var options = new SqlServerTsqlDdlCompatibilityOptions
        {
            MaxTokenCount = 4,
        };

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                options,
                TestContext.Current.CancellationToken);

        Assert.NotEqual(
            SqlServerTsqlDdlCompatibilityAnalyzer.LimitRuleId,
            report.RuleId);
    }

    [Fact]
    public async Task PreflightLexicalBudget_RejectsMixedWhitespaceTokens()
    {
        const string script = " \r\n\t ";
        var options = new SqlServerTsqlDdlCompatibilityOptions
        {
            MaxTokenCount = 3,
        };

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                options,
                TestContext.Current.CancellationToken);

        Assert.Equal(MigrationCompatibilityStatus.Unknown, report.Status);
        Assert.Equal(
            SqlServerTsqlDdlCompatibilityAnalyzer.LimitRuleId,
            report.RuleId);
        Assert.Null(report.HighestEvidence);
    }

    [Fact]
    public async Task LoweredCatalogObjectBudget_RejectsBeforePlanning()
    {
        const string script =
            "CREATE TABLE dbo.widgets (id int NOT NULL, value bigint NULL);";
        var options = new SqlServerTsqlDdlCompatibilityOptions
        {
            MaxCatalogObjectCount = 2,
        };

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                options,
                TestContext.Current.CancellationToken);

        Assert.Equal(MigrationCompatibilityStatus.Unknown, report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.Parsed,
            report.HighestEvidence);
        Assert.Equal(
            SqlServerTsqlDdlCompatibilityAnalyzer.LimitRuleId,
            report.RuleId);
        Assert.Null(report.CatalogDigest);
        Assert.Null(report.GeneratedDdlDigest);
    }

    [Fact]
    public async Task CharacterLimit_IsCheckedBeforeInvalidUtf16Scan()
    {
        const string script = "\uD800x";
        var options = new SqlServerTsqlDdlCompatibilityOptions
        {
            MaxScriptCharacters = 1,
        };

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                options,
                TestContext.Current.CancellationToken);

        Assert.Equal(MigrationCompatibilityStatus.Unknown, report.Status);
        Assert.Equal(
            SqlServerTsqlDdlCompatibilityAnalyzer.LimitRuleId,
            report.RuleId);
    }

    [Fact]
    public async Task InvalidUtf16_DigestDistinguishesCodeUnits()
    {
        CSharpDbDdlCompatibilityReport first =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                "\uD800",
                cancellationToken: TestContext.Current.CancellationToken);
        CSharpDbDdlCompatibilityReport second =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                "\uD801",
                cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(MigrationCompatibilityStatus.Unknown, first.Status);
        Assert.Null(first.HighestEvidence);
        Assert.NotEqual(first.ScriptDigest, second.ScriptDigest);
    }

    [Fact]
    public async Task BitIndex_IsRejectedBeforeScratch()
    {
        const string script =
            """
            CREATE TABLE dbo.widgets (flag bit NOT NULL);
            CREATE INDEX ix_widgets_flag ON dbo.widgets(flag);
            """;

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(MigrationCompatibilityStatus.Unsupported, report.Status);
        Assert.Equal(MigrationEvidenceLevel.Parsed, report.HighestEvidence);
        Assert.Null(report.GeneratedDdlDigest);
    }

    [Fact]
    public async Task DuplicateConstraintNames_AreRejectedBeforeScratch()
    {
        const string script =
            """
            CREATE TABLE dbo.first_table (
                id int NOT NULL,
                CONSTRAINT duplicate_name PRIMARY KEY (id)
            );
            CREATE TABLE dbo.second_table (
                id int NOT NULL,
                CONSTRAINT DUPLICATE_NAME PRIMARY KEY (id)
            );
            """;

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(MigrationCompatibilityStatus.Unsupported, report.Status);
        Assert.Contains(
            report.Diagnostics,
            diagnostic => diagnostic.RuleId ==
                SqlServerTsqlDdlCompatibilityAnalyzer.DuplicateObjectRuleId);
        Assert.Null(report.GeneratedDdlDigest);
    }

    [Fact]
    public async Task ConstraintAndTableNameCollision_IsRejectedBeforeScratch()
    {
        const string script =
            """
            CREATE TABLE dbo.shared_name (id int NOT NULL);
            CREATE TABLE dbo.widgets (
                id int NOT NULL,
                CONSTRAINT shared_name PRIMARY KEY (id)
            );
            """;

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(MigrationCompatibilityStatus.Unsupported, report.Status);
        Assert.Equal(
            SqlServerTsqlDdlCompatibilityAnalyzer.DuplicateObjectRuleId,
            report.RuleId);
        Assert.Null(report.GeneratedDdlDigest);
    }

    [Fact]
    public async Task KeyAndIndexNameCollision_IsRejectedBeforeScratch()
    {
        const string script =
            """
            CREATE TABLE dbo.widgets (
                id int NOT NULL,
                value int NOT NULL,
                CONSTRAINT ix_widgets PRIMARY KEY (id)
            );
            CREATE INDEX ix_widgets ON dbo.widgets(value);
            """;

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(MigrationCompatibilityStatus.Unsupported, report.Status);
        Assert.Contains(
            report.Diagnostics,
            diagnostic => diagnostic.RuleId ==
                SqlServerTsqlDdlCompatibilityAnalyzer.DuplicateObjectRuleId);
        Assert.Null(report.GeneratedDdlDigest);
    }

    [Fact]
    public async Task SameIndexNameOnDifferentTables_PassesScratch()
    {
        const string script =
            """
            CREATE TABLE dbo.first_table (id int NOT NULL);
            CREATE TABLE dbo.second_table (id int NOT NULL);
            CREATE INDEX ix_shared ON dbo.first_table(id);
            CREATE INDEX ix_shared ON dbo.second_table(id);
            """;

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(
            MigrationCompatibilityStatus.CompatibleWithRewrite,
            report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            report.HighestEvidence);
    }

    [Fact]
    public async Task OverlongSqlServerIdentifier_IsRejectedBeforeScratch()
    {
        string script = string.Concat(
            "CREATE TABLE dbo.",
            new string('x', 129),
            " (id int NOT NULL);");

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(MigrationCompatibilityStatus.Unsupported, report.Status);
        Assert.NotEqual(
            MigrationEvidenceLevel.ScratchExecuted,
            report.HighestEvidence);
        Assert.Null(report.GeneratedDdlDigest);
    }

    [Fact]
    public async Task ParseError_UsesNumericLocationWithoutParserMessage()
    {
        const string script = "CREATE TABLE dbo.widgets (";

        CSharpDbDdlCompatibilityReport report =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(MigrationCompatibilityStatus.Unsupported, report.Status);
        CSharpDbDdlCompatibilityDiagnostic diagnostic =
            Assert.Single(report.Diagnostics);
        Assert.Equal(
            SqlServerTsqlDdlCompatibilityAnalyzer.ParseRuleId,
            diagnostic.RuleId);
        Assert.NotNull(diagnostic.SourceSpan?.Start);
        Assert.DoesNotContain("460", diagnostic.Summary, StringComparison.Ordinal);
        Assert.DoesNotContain(script, diagnostic.Summary, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PreCanceledToken_Throws()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                "CREATE TABLE dbo.widgets (id int NOT NULL);",
                cancellationToken: cancellation.Token));
    }

    private static string ExpectedDigest(string script)
    {
        using IncrementalHash hash =
            IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(Encoding.UTF8.GetBytes(
            SqlServerTsqlDdlCompatibilityAnalyzer.InputDigestDomain));
        hash.AppendData([0]);
        hash.AppendData(Encoding.UTF8.GetBytes(script));
        return Convert.ToHexString(hash.GetHashAndReset())
            .ToLowerInvariant();
    }

    private static MigrationCatalog LowerCatalog(string script)
    {
        var parser = new TSql160Parser(
            initialQuotedIdentifiers: true,
            SqlEngineType.Standalone);
        using var reader = new StringReader(script);
        TSqlFragment fragment = parser.Parse(reader, out IList<ParseError> errors);
        Assert.Empty(errors);
        TSqlScript parsed = Assert.IsType<TSqlScript>(fragment);
        TsqlDdlLoweringResult result = TsqlDdlLowerer.Lower(
            parsed.Batches.SelectMany(static batch => batch.Statements).ToArray(),
            ExpectedDigest(script),
            CSharpDbCapabilityCatalogLoader.LoadEmbedded(),
            SqlServerTsqlDdlCompatibilityOptions.HardMaxCatalogObjectCount,
            CancellationToken.None);

        return Assert.IsType<MigrationCatalog>(result.Catalog);
    }
}
