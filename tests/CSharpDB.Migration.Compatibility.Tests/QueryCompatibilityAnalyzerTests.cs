namespace CSharpDB.Migration.Compatibility.Tests;

using CSharpDB.Migration.SqlServer;

public sealed class QueryCompatibilityAnalyzerTests
{
    [Fact]
    public void CSharpDbReadOnlyQuery_IsConditionalUntilBindingExists()
    {
        QueryCompatibilityResult result = Analyze(
            QuerySourceDialect.CSharpDb,
            "SELECT id FROM users WHERE id = 1 ORDER BY id;");

        Assert.Equal(MigrationCompatibilityStatus.Conditional, result.Status);
        Assert.Equal(MigrationEvidenceLevel.Parsed, result.Evidence);
        Assert.True(result.SourceParsed);
        Assert.True(result.TargetParsed);
        Assert.True(result.IsReadOnly);
        Assert.Null(result.Rewrite);
        Assert.Contains(
            result.Diagnostics,
            static item =>
                item.RuleId ==
                QueryCompatibilityRuleIds.BindingNotPerformed);
    }

    [Fact]
    public void CSharpDbQuery_FlagsNondeterminismAndUnorderedLimit()
    {
        QueryCompatibilityResult result = Analyze(
            QuerySourceDialect.CSharpDb,
            "SELECT NOW() FROM users LIMIT 1;");

        Assert.Equal(MigrationCompatibilityStatus.Conditional, result.Status);
        Assert.Contains(
            result.Diagnostics,
            static item =>
                item.RuleId ==
                QueryCompatibilityRuleIds.NondeterministicFunction);
        Assert.Contains(
            result.Diagnostics,
            static item =>
                item.RuleId ==
                QueryCompatibilityRuleIds.NondeterministicLimit);
    }

    [Fact]
    public void CSharpDbCustomFunction_RemainsUnknownWithoutBinding()
    {
        QueryCompatibilityResult result = Analyze(
            QuerySourceDialect.CSharpDb,
            "SELECT tenant_score(id) FROM users ORDER BY id;");

        Assert.Equal(MigrationCompatibilityStatus.Unknown, result.Status);
        Assert.True(result.SourceParsed);
        Assert.True(result.TargetParsed);
        Assert.Contains(
            result.Diagnostics,
            static item =>
                item.RuleId ==
                QueryCompatibilityRuleIds.UnboundFunction);
    }

    [Fact]
    public void TsqlPortableSelect_ParsesInBothDialects()
    {
        QueryCompatibilityResult result = Analyze(
            QuerySourceDialect.SqlServerTsql,
            "SELECT id FROM users WHERE id = 1 ORDER BY id;");

        Assert.Equal(MigrationCompatibilityStatus.Conditional, result.Status);
        Assert.True(result.SourceParsed);
        Assert.True(result.TargetParsed);
        Assert.True(result.IsReadOnly);
        Assert.Null(result.Rewrite);
    }

    [Fact]
    public void TsqlRootIntegerTop_GeneratesBoundedLimitCandidate()
    {
        QueryCompatibilityResult result = Analyze(
            QuerySourceDialect.SqlServerTsql,
            "SELECT TOP (10) id FROM users ORDER BY id;");

        Assert.Equal(MigrationCompatibilityStatus.Conditional, result.Status);
        Assert.True(result.SourceParsed);
        Assert.True(result.TargetParsed);
        Assert.True(result.IsReadOnly);
        QueryCompatibilityRewrite rewrite = Assert.IsType<QueryCompatibilityRewrite>(
            result.Rewrite);
        Assert.Equal(
            "tsql-top-integer-to-csharpdb-limit/v1",
            rewrite.RewriteId);
        Assert.Contains("LIMIT 10", rewrite.CandidateCSharpDbSql);
        Assert.Contains(
            result.Diagnostics,
            static item =>
                item.RuleId ==
                QueryCompatibilityRuleIds.TopToLimitRewrite);
    }

    [Fact]
    public void TsqlTopWithoutOrdering_IsExplicitlyConditional()
    {
        QueryCompatibilityResult result = Analyze(
            QuerySourceDialect.SqlServerTsql,
            "SELECT TOP (5) id FROM users;");

        Assert.Equal(MigrationCompatibilityStatus.Conditional, result.Status);
        Assert.NotNull(result.Rewrite);
        Assert.Contains(
            result.Diagnostics,
            static item =>
                item.RuleId ==
                QueryCompatibilityRuleIds.NondeterministicLimit);
    }

    [Fact]
    public void TsqlNonLiteralTop_IsNotReinterpretedAsPortableSql()
    {
        QueryCompatibilityResult result = Analyze(
            QuerySourceDialect.SqlServerTsql,
            "SELECT TOP (@take) id FROM users ORDER BY id;");

        Assert.Equal(MigrationCompatibilityStatus.Unknown, result.Status);
        Assert.True(result.SourceParsed);
        Assert.False(result.TargetParsed);
        Assert.Null(result.Rewrite);
        Assert.Contains(
            result.Diagnostics,
            static item =>
                item.RuleId ==
                QueryCompatibilityRuleIds.TargetParseFailed);
    }

    [Fact]
    public void TsqlVendorFeatures_AreRetainedAsDiagnostics()
    {
        QueryCompatibilityResult result = Analyze(
            QuerySourceDialect.SqlServerTsql,
            "SELECT GETDATE(), @@SPID FROM #working;");

        Assert.Equal(MigrationCompatibilityStatus.Unknown, result.Status);
        Assert.True(result.SourceParsed);
        Assert.Contains(
            result.Diagnostics,
            static item =>
                item.RuleId ==
                QueryCompatibilityRuleIds.NondeterministicFunction);
        Assert.Contains(
            result.Diagnostics,
            static item =>
                item.RuleId ==
                QueryCompatibilityRuleIds.SessionState);
        Assert.Contains(
            result.Diagnostics,
            static item =>
                item.RuleId ==
                QueryCompatibilityRuleIds.TemporaryObject);
        Assert.Equal(
            result.Diagnostics.Count,
            result.Diagnostics.Select(
                static item => item.DiagnosticId).Distinct(
                StringComparer.Ordinal).Count());
    }

    [Fact]
    public void UnqualifiedSqlServerCompatibilityLevel_FailsClosed()
    {
        QueryCompatibilityReport report =
            new SqlServerQueryCompatibilityAnalyzer().Analyze(
                new QueryCompatibilityRequest
                {
                    SqlServerCompatibilityLevel = 140,
                    Queries =
                    [
                        new QueryCompatibilityInput
                        {
                            QueryId = "legacy-query",
                            SourceDialect =
                                QuerySourceDialect.SqlServerTsql,
                            Sql = "SELECT id FROM users;",
                        },
                    ],
                },
                TestContext.Current.CancellationToken);

        QueryCompatibilityResult result = Assert.Single(report.Results);
        Assert.Equal(MigrationCompatibilityStatus.Unknown, result.Status);
        Assert.False(result.SourceParsed);
        Assert.Contains(
            result.Diagnostics,
            static item =>
                item.RuleId ==
                QueryCompatibilityRuleIds.DialectUnqualified);
    }

    [Fact]
    public void BaseAnalyzer_DoesNotLoadSqlServerGrammar()
    {
        QueryCompatibilityReport report =
            new QueryCompatibilityAnalyzer().Analyze(
                new QueryCompatibilityRequest
                {
                    Queries =
                    [
                        new QueryCompatibilityInput
                        {
                            QueryId = "isolated-query",
                            SourceDialect =
                                QuerySourceDialect.SqlServerTsql,
                            Sql = "SELECT id FROM users;",
                        },
                    ],
                },
                TestContext.Current.CancellationToken);

        QueryCompatibilityResult result =
            Assert.Single(report.Results);
        Assert.Equal(
            MigrationCompatibilityStatus.Unknown,
            result.Status);
        Assert.False(result.SourceParsed);
        Assert.Contains(
            result.Diagnostics,
            static item =>
                item.RuleId ==
                QueryCompatibilityRuleIds.DialectUnqualified);
    }

    [Theory]
    [InlineData("UPDATE users SET name = 'changed';")]
    [InlineData("SELECT id INTO copied_users FROM users;")]
    [InlineData("SELECT @value = id FROM users;")]
    public void TsqlStateChangingStatements_AreUnsupported(string sql)
    {
        QueryCompatibilityResult result = Analyze(
            QuerySourceDialect.SqlServerTsql,
            sql);

        Assert.Equal(MigrationCompatibilityStatus.Unsupported, result.Status);
        Assert.True(result.SourceParsed);
        Assert.False(result.IsReadOnly);
        Assert.Contains(
            result.Diagnostics,
            static item =>
                item.RuleId == QueryCompatibilityRuleIds.NotReadOnly);
    }

    [Fact]
    public void TsqlBatch_MustContainOneStatement()
    {
        QueryCompatibilityResult result = Analyze(
            QuerySourceDialect.SqlServerTsql,
            "SELECT 1; SELECT 2;");

        Assert.Equal(MigrationCompatibilityStatus.Unsupported, result.Status);
        Assert.Contains(
            result.Diagnostics,
            static item =>
                item.RuleId ==
                QueryCompatibilityRuleIds.MultipleStatements);
    }

    [Theory]
    [InlineData(QuerySourceDialect.MySql)]
    [InlineData(QuerySourceDialect.Sqlite)]
    [InlineData(QuerySourceDialect.Access)]
    public void UnimplementedSourceDialects_FailClosed(
        QuerySourceDialect dialect)
    {
        QueryCompatibilityResult result = Analyze(
            dialect,
            "SELECT id FROM users;");

        Assert.Equal(MigrationCompatibilityStatus.Unknown, result.Status);
        Assert.False(result.SourceParsed);
        Assert.False(result.TargetParsed);
        Assert.Null(result.IsReadOnly);
        Assert.Contains(
            result.Diagnostics,
            static item =>
                item.RuleId ==
                QueryCompatibilityRuleIds.DialectUnqualified);
    }

    [Fact]
    public void Reports_AreDeterministicAndSortedByQueryId()
    {
        var analyzer = new QueryCompatibilityAnalyzer();
        var request = new QueryCompatibilityRequest
        {
            Queries =
            [
                new QueryCompatibilityInput
                {
                    QueryId = "z-query",
                    SourceDialect = QuerySourceDialect.MySql,
                    Sql = "SELECT id FROM users;",
                },
                new QueryCompatibilityInput
                {
                    QueryId = "a-query",
                    SourceDialect = QuerySourceDialect.CSharpDb,
                    Sql = "SELECT id FROM users ORDER BY id;",
                },
            ],
        };

        QueryCompatibilityReport first = analyzer.Analyze(
            request,
            TestContext.Current.CancellationToken);
        QueryCompatibilityReport second = analyzer.Analyze(
            request,
            TestContext.Current.CancellationToken);

        Assert.Equal(["a-query", "z-query"], first.Results.Select(
            static item => item.QueryId));
        Assert.Equal(
            CompatibilityReportFormatter.ToJson(first),
            CompatibilityReportFormatter.ToJson(second));
        Assert.Equal(
            CompatibilityReportFormatter.ToText(first),
            CompatibilityReportFormatter.ToText(second));
        Assert.Equal(2, first.Summary.Total);
        Assert.Equal(1, first.Summary.Conditional);
        Assert.Equal(1, first.Summary.Unknown);
    }

    [Fact]
    public void OversizedPack_IsRejectedBeforeParserAllocation()
    {
        var analyzer = new QueryCompatibilityAnalyzer();
        var request = new QueryCompatibilityRequest
        {
            Limits = new QueryCompatibilityLimits
            {
                MaxQueries = 1,
                MaxQueryBytes = 8,
                MaxTotalQueryBytes = 8,
            },
            Queries =
            [
                new QueryCompatibilityInput
                {
                    QueryId = "large",
                    SourceDialect = QuerySourceDialect.CSharpDb,
                    Sql = "SELECT id FROM users;",
                },
            ],
        };

        Assert.Throws<ArgumentException>(() => analyzer.Analyze(
            request,
            TestContext.Current.CancellationToken));
    }

    private static QueryCompatibilityResult Analyze(
        QuerySourceDialect dialect,
        string sql)
    {
        var request = new QueryCompatibilityRequest
        {
            Queries =
            [
                new QueryCompatibilityInput
                {
                    QueryId = "test-query",
                    SourceDialect = dialect,
                    Sql = sql,
                },
            ],
        };
        QueryCompatibilityReport report =
            dialect == QuerySourceDialect.SqlServerTsql
                ? new SqlServerQueryCompatibilityAnalyzer()
                    .Analyze(
                        request,
                        TestContext.Current.CancellationToken)
                : new QueryCompatibilityAnalyzer().Analyze(
                    request,
                    TestContext.Current.CancellationToken);
        return Assert.Single(report.Results);
    }
}
