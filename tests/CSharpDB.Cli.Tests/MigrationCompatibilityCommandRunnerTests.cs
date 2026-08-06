using System.Text.Json;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.Compatibility;

namespace CSharpDB.Cli.Tests;

[Collection("CliConsole")]
public sealed class MigrationCompatibilityCommandRunnerTests
{
    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task TypeMap_JsonReportsDriveSuccessWarningAndErrorExits()
    {
        using var workspace = new TemporaryDirectory();
        string exactCatalog = await WriteCatalogAsync(
            workspace.PathFor("exact.catalog.json"),
            nativeType: "BIGINT",
            logicalType: "signedInteger");
        string lossyCatalog = await WriteCatalogAsync(
            workspace.PathFor("lossy.catalog.json"),
            nativeType: "DECIMAL(38,9)",
            logicalType: "decimal",
            new MigrationCatalogFacet { Name = "precision", Value = "38" },
            new MigrationCatalogFacet { Name = "scale", Value = "9" });
        string unsupportedCatalog = await WriteCatalogAsync(
            workspace.PathFor("unsupported.catalog.json"),
            nativeType: "GEOGRAPHY",
            logicalType: "geography");

        RunResult exact = await RunAsync(
            [
                "migrate", "type-map", exactCatalog,
                "--out", workspace.PathFor("exact.report.json"),
                "--format", "json",
            ]);
        RunResult lossy = await RunAsync(
            [
                "migrate", "type-map", lossyCatalog,
                "--out", workspace.PathFor("lossy.report.json"),
                "--profile", "queryable",
                "--format", "json",
            ]);
        RunResult unsupported = await RunAsync(
            [
                "migrate", "type-map", unsupportedCatalog,
                "--out", workspace.PathFor("unsupported.report.json"),
                "--format", "json",
            ]);

        Assert.Equal(InspectorCommandRunner.ExitOk, exact.ExitCode);
        Assert.Equal(InspectorCommandRunner.ExitWarn, lossy.ExitCode);
        Assert.Equal(InspectorCommandRunner.ExitError, unsupported.ExitCode);

        using JsonDocument exactReport = await ReadJsonAsync(
            workspace.PathFor("exact.report.json"));
        AssertMappingReportContract(exactReport.RootElement);
        Assert.Equal(
            1,
            exactReport.RootElement
                .GetProperty("summary")
                .GetProperty("exact")
                .GetInt32());
        Assert.Equal(
            "exact",
            exactReport.RootElement
                .GetProperty("entries")[0]
                .GetProperty("classification")
                .GetString());
        Assert.Equal(
            "integer",
            exactReport.RootElement
                .GetProperty("entries")[0]
                .GetProperty("targetType")
                .GetString());

        using JsonDocument lossyReport = await ReadJsonAsync(
            workspace.PathFor("lossy.report.json"));
        AssertMappingReportContract(lossyReport.RootElement);
        Assert.Equal(
            1,
            lossyReport.RootElement
                .GetProperty("summary")
                .GetProperty("lossy")
                .GetInt32());
        JsonElement lossyEntry = lossyReport.RootElement
            .GetProperty("entries")[0];
        Assert.Equal(
            "lossy",
            lossyEntry.GetProperty("classification").GetString());
        Assert.Equal(
            "decimal-binary64",
            lossyEntry
                .GetProperty("conversion")
                .GetProperty("conversionId")
                .GetString());
        Assert.Equal(
            "conditional",
            lossyEntry
                .GetProperty("diagnostic")
                .GetProperty("status")
                .GetString());

        using JsonDocument unsupportedReport = await ReadJsonAsync(
            workspace.PathFor("unsupported.report.json"));
        AssertMappingReportContract(unsupportedReport.RootElement);
        Assert.Equal(
            1,
            unsupportedReport.RootElement
                .GetProperty("summary")
                .GetProperty("unsupported")
                .GetInt32());
        JsonElement unsupportedEntry = unsupportedReport.RootElement
            .GetProperty("entries")[0];
        Assert.Equal(
            "unsupported",
            unsupportedEntry.GetProperty("classification").GetString());
        Assert.Equal(
            JsonValueKind.Null,
            unsupportedEntry.GetProperty("targetType").ValueKind);
        Assert.Equal(
            "unsupported",
            unsupportedEntry
                .GetProperty("diagnostic")
                .GetProperty("status")
                .GetString());
    }

    [Fact]
    public async Task TypeMap_TextReportIsPublishedAndDeterministic()
    {
        using var workspace = new TemporaryDirectory();
        string catalog = await WriteCatalogAsync(
            workspace.PathFor("source.catalog.json"),
            nativeType: "BIGINT",
            logicalType: "signedInteger");
        string firstPath = workspace.PathFor("first.report.txt");
        string secondPath = workspace.PathFor("second.report.txt");

        RunResult first = await RunAsync(
            [
                "migrate", "type-map", catalog,
                "--out", firstPath,
                "--format", "text",
            ]);
        RunResult second = await RunAsync(
            [
                "migrate", "type-map", catalog,
                "--out", secondPath,
                "--format", "text",
            ]);

        Assert.Equal(InspectorCommandRunner.ExitOk, first.ExitCode);
        Assert.Equal(first.ExitCode, second.ExitCode);
        Assert.Equal(
            await File.ReadAllTextAsync(firstPath, Cancellation),
            await File.ReadAllTextAsync(secondPath, Cancellation));
        string text = await File.ReadAllTextAsync(firstPath, Cancellation);
        Assert.Contains("Data type mapping report", text, StringComparison.Ordinal);
        Assert.Contains(
            "Format: csharpdb-data-type-mapping-report/v1",
            text,
            StringComparison.Ordinal);
        Assert.Contains("classification: exact", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TypeMap_CustomMapIsAppliedPerCatalogObject()
    {
        using var workspace = new TemporaryDirectory();
        string catalog = await WriteCatalogAsync(
            workspace.PathFor("source.catalog.json"),
            nativeType: "BIGINT",
            logicalType: "signedInteger");
        string customMap = workspace.PathFor("custom-map.json");
        string reportPath = workspace.PathFor("custom.report.json");
        await File.WriteAllTextAsync(
            customMap,
            """
            {
              "compat:column:rows:value": "text"
            }
            """,
            Cancellation);

        RunResult result = await RunAsync(
            [
                "migrate", "type-map", catalog,
                "--out", reportPath,
                "--profile", "custom",
                "--custom-map", customMap,
                "--format", "json",
            ]);

        Assert.Equal(InspectorCommandRunner.ExitOk, result.ExitCode);
        using JsonDocument report = await ReadJsonAsync(reportPath);
        AssertMappingReportContract(report.RootElement);
        Assert.Equal(
            "custom",
            report.RootElement.GetProperty("profile").GetString());
        JsonElement entry = report.RootElement.GetProperty("entries")[0];
        Assert.Equal("text", entry.GetProperty("requestedTargetType").GetString());
        Assert.Equal("text", entry.GetProperty("targetType").GetString());
        Assert.Equal(
            "losslessReencoded",
            entry.GetProperty("classification").GetString());
        Assert.Equal(
            "canonical-text",
            entry
                .GetProperty("conversion")
                .GetProperty("conversionId")
                .GetString());
    }

    [Fact]
    public async Task TypeMap_CustomMapAcceptsExactDecimalTarget()
    {
        using var workspace = new TemporaryDirectory();
        string catalog = await WriteCatalogAsync(
            workspace.PathFor("source.catalog.json"),
            nativeType: "DECIMAL(18,2)",
            logicalType: "decimal",
            new MigrationCatalogFacet { Name = "precision", Value = "18" },
            new MigrationCatalogFacet { Name = "scale", Value = "2" });
        string customMap = workspace.PathFor("custom-map.json");
        string reportPath = workspace.PathFor("custom.report.json");
        await File.WriteAllTextAsync(
            customMap,
            """
            {
              "compat:column:rows:value": "decimal"
            }
            """,
            Cancellation);

        RunResult result = await RunAsync(
            [
                "migrate", "type-map", catalog,
                "--out", reportPath,
                "--profile", "custom",
                "--custom-map", customMap,
                "--format", "json",
            ]);

        Assert.Equal(InspectorCommandRunner.ExitOk, result.ExitCode);
        using JsonDocument report = await ReadJsonAsync(reportPath);
        JsonElement entry = report.RootElement.GetProperty("entries")[0];
        Assert.Equal("decimal", entry.GetProperty("requestedTargetType").GetString());
        Assert.Equal("decimal", entry.GetProperty("targetType").GetString());
        Assert.Equal(
            "decimal-native",
            entry.GetProperty("conversion").GetProperty("conversionId").GetString());
    }

    [Fact]
    public async Task TypeMap_InvalidCustomMapReturnsSanitizedErrorWithoutReport()
    {
        using var workspace = new TemporaryDirectory();
        string catalog = await WriteCatalogAsync(
            workspace.PathFor("source.catalog.json"),
            nativeType: "BIGINT",
            logicalType: "signedInteger");
        string customMap = workspace.PathFor("invalid-custom-map.json");
        string reportPath = workspace.PathFor("unused.report.json");
        await File.WriteAllTextAsync(
            customMap,
            """
            {
              "compat:column:rows:value": "credential=private-value"
            }
            """,
            Cancellation);

        RunResult result = await RunAsync(
            [
                "migrate", "type-map", catalog,
                "--out", reportPath,
                "--profile", "custom",
                "--custom-map", customMap,
                "--format", "json",
            ]);

        Assert.Equal(InspectorCommandRunner.ExitError, result.ExitCode);
        Assert.False(File.Exists(reportPath));
        Assert.Contains(
            "MIG-TYPE-MAP-CUSTOM-001",
            result.StdErr,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "private-value",
            result.StdErr,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task TypeMap_CustomMapRejectsNonPersistentNullTarget()
    {
        using var workspace = new TemporaryDirectory();
        string catalog = await WriteCatalogAsync(
            workspace.PathFor("source.catalog.json"),
            nativeType: "BIGINT",
            logicalType: "signedInteger");
        string customMap = workspace.PathFor("invalid-null-map.json");
        string reportPath = workspace.PathFor("unused.report.json");
        await File.WriteAllTextAsync(
            customMap,
            """
            {
              "compat:column:rows:value": "null"
            }
            """,
            Cancellation);

        RunResult result = await RunAsync(
            [
                "migrate", "type-map", catalog,
                "--out", reportPath,
                "--profile", "custom",
                "--custom-map", customMap,
                "--format", "json",
            ]);

        Assert.Equal(InspectorCommandRunner.ExitError, result.ExitCode);
        Assert.False(File.Exists(reportPath));
        Assert.Contains(
            "MIG-TYPE-MAP-CUSTOM-001",
            result.StdErr,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task TypeMap_ExistingOutputIsNeverOverwritten()
    {
        using var workspace = new TemporaryDirectory();
        string catalog = await WriteCatalogAsync(
            workspace.PathFor("source.catalog.json"),
            nativeType: "BIGINT",
            logicalType: "signedInteger");
        string reportPath = workspace.PathFor("existing.report.json");
        byte[] sentinel = "do-not-overwrite-type-map"u8.ToArray();
        await File.WriteAllBytesAsync(reportPath, sentinel, Cancellation);

        RunResult result = await RunAsync(
            [
                "migrate", "type-map", catalog,
                "--out", reportPath,
                "--format", "json",
            ]);

        Assert.Equal(InspectorCommandRunner.ExitError, result.ExitCode);
        Assert.Equal(
            sentinel,
            await File.ReadAllBytesAsync(reportPath, Cancellation));
        AssertNoTemporaryPublications(workspace.Root);
    }

    [Fact]
    public async Task TypeMap_InvalidOptionCombinationsReturnUsageWithoutPublishing()
    {
        using var workspace = new TemporaryDirectory();
        string catalog = await WriteCatalogAsync(
            workspace.PathFor("source.catalog.json"),
            nativeType: "BIGINT",
            logicalType: "signedInteger");
        string customMap = workspace.PathFor("custom-map.json");
        await File.WriteAllTextAsync(customMap, "{}", Cancellation);
        string outputPath = workspace.PathFor("unused.report.json");

        string[][] invalidArguments =
        [
            ["migrate", "type-map"],
            ["migrate", "type-map", catalog],
            [
                "migrate", "type-map", catalog,
                "--out", outputPath,
                "--profile", "unknown",
            ],
            [
                "migrate", "type-map", catalog,
                "--out", outputPath,
                "--format", "yaml",
            ],
            [
                "migrate", "type-map", catalog,
                "--out", outputPath,
                "--profile", "custom",
            ],
            [
                "migrate", "type-map", catalog,
                "--out", outputPath,
                "--profile", "preserve",
                "--custom-map", customMap,
            ],
            [
                "migrate", "type-map", catalog,
                "--out", outputPath,
                "--mystery", "value",
            ],
        ];

        foreach (string[] arguments in invalidArguments)
        {
            RunResult result = await RunAsync(arguments);
            Assert.Equal(InspectorCommandRunner.ExitUsage, result.ExitCode);
            Assert.False(File.Exists(outputPath));
            Assert.False(string.IsNullOrWhiteSpace(result.StdErr));
        }
    }

    [Fact]
    public async Task QueryCheck_CSharpDbJsonReportIsAWarningUntilBindingExists()
    {
        using var workspace = new TemporaryDirectory();
        string queryPath = workspace.PathFor("customers.sql");
        string reportPath = workspace.PathFor("customers.report.json");
        await File.WriteAllTextAsync(
            queryPath,
            "SELECT id FROM customers WHERE id = 1 ORDER BY id;",
            Cancellation);

        RunResult result = await RunAsync(
            [
                "migrate", "query-check", queryPath,
                "--dialect", "csharpdb",
                "--query-id", "customers-by-id",
                "--out", reportPath,
                "--format", "json",
            ]);

        Assert.Equal(InspectorCommandRunner.ExitWarn, result.ExitCode);
        using JsonDocument report = await ReadJsonAsync(reportPath);
        AssertQueryReportContract(report.RootElement);
        JsonElement query = report.RootElement.GetProperty("results")[0];
        Assert.Equal("customers-by-id", query.GetProperty("queryId").GetString());
        Assert.Equal("cSharpDb", query.GetProperty("sourceDialect").GetString());
        Assert.Equal("conditional", query.GetProperty("status").GetString());
        Assert.Equal("parsed", query.GetProperty("evidence").GetString());
        Assert.True(query.GetProperty("sourceParsed").GetBoolean());
        Assert.True(query.GetProperty("targetParsed").GetBoolean());
        Assert.True(query.GetProperty("isReadOnly").GetBoolean());
        Assert.Equal(JsonValueKind.Null, query.GetProperty("rewrite").ValueKind);
        Assert.Contains(
            query.GetProperty("diagnostics").EnumerateArray(),
            static diagnostic =>
                diagnostic.GetProperty("ruleId").GetString() ==
                "MIG-QUERY-UNBOUND-001");
    }

    [Fact]
    public async Task QueryCheck_TsqlTopWritesReviewedRewriteCandidate()
    {
        using var workspace = new TemporaryDirectory();
        string queryPath = workspace.PathFor("top-customers.sql");
        string reportPath = workspace.PathFor("top-customers.report.json");
        await File.WriteAllTextAsync(
            queryPath,
            "SELECT TOP (10) id FROM customers ORDER BY id;",
            Cancellation);

        MigrationCommandDependencies dependencies =
            MigrationCommandDependencies.Default with
            {
                AnalyzeTsqlQueryAsync = (
                    query,
                    queryId,
                    compatibilityLevel,
                    targetVersion,
                    cancellationToken) =>
                {
                    cancellationToken
                        .ThrowIfCancellationRequested();
                    Assert.Equal(160, compatibilityLevel);
                    return ValueTask.FromResult(
                        SqlServerQueryWorkerResult.Success(
                            CreateTsqlRewriteReport(
                                query,
                                queryId,
                                targetVersion)));
                },
            };
        RunResult result = await RunAsync(
            [
                "migrate", "query-check", queryPath,
                "--dialect", "tsql",
                "--compatibility-level", "160",
                "--query-id", "top-customers",
                "--out", reportPath,
                "--format", "json",
            ],
            dependencies);

        Assert.Equal(InspectorCommandRunner.ExitWarn, result.ExitCode);
        using JsonDocument report = await ReadJsonAsync(reportPath);
        AssertQueryReportContract(report.RootElement);
        JsonElement query = report.RootElement.GetProperty("results")[0];
        Assert.Equal("conditional", query.GetProperty("status").GetString());
        JsonElement rewrite = query.GetProperty("rewrite");
        Assert.Equal(
            "tsql-top-integer-to-csharpdb-limit/v1",
            rewrite.GetProperty("rewriteId").GetString());
        Assert.Contains(
            "LIMIT 10",
            rewrite.GetProperty("candidateCSharpDbSql").GetString(),
            StringComparison.Ordinal);
        Assert.Matches(
            "^[0-9a-f]{64}$",
            rewrite.GetProperty("candidateDigest").GetString());
    }

    [Fact]
    public async Task QueryCheck_TsqlMissingWorkerFailsClosed()
    {
        using var workspace = new TemporaryDirectory();
        string queryPath =
            workspace.PathFor("private-query.sql");
        string reportPath =
            workspace.PathFor("unused.report.json");
        const string privateQuery =
            "SELECT private_customer_token FROM customers;";
        await File.WriteAllTextAsync(
            queryPath,
            privateQuery,
            Cancellation);

        MigrationCommandDependencies dependencies =
            MigrationCommandDependencies.Default with
            {
                AnalyzeTsqlQueryAsync = (
                    _,
                    _,
                    _,
                    _,
                    cancellationToken) =>
                {
                    cancellationToken
                        .ThrowIfCancellationRequested();
                    return ValueTask.FromResult(
                        SqlServerQueryWorkerResult.Failure(
                            SqlServerQueryWorkerStatus.Missing));
                },
            };

        RunResult result = await RunAsync(
            [
                "migrate", "query-check", queryPath,
                "--dialect", "tsql",
                "--out", reportPath,
                "--format", "json",
            ],
            dependencies);

        Assert.Equal(
            InspectorCommandRunner.ExitError,
            result.ExitCode);
        Assert.Contains(
            "MIG-TSQL-CLI-ADAPTER-001",
            result.StdErr,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            privateQuery,
            result.StdErr,
            StringComparison.Ordinal);
        Assert.False(File.Exists(reportPath));
    }

    [Fact]
    public async Task QueryCheck_StaticFailuresWriteErrorReports()
    {
        using var workspace = new TemporaryDirectory();
        string invalidQuery = workspace.PathFor("invalid.sql");
        string mysqlQuery = workspace.PathFor("mysql.sql");
        string invalidReport = workspace.PathFor("invalid.report.json");
        string mysqlReport = workspace.PathFor("mysql.report.json");
        await File.WriteAllTextAsync(invalidQuery, "SELECT FROM;", Cancellation);
        await File.WriteAllTextAsync(
            mysqlQuery,
            "SELECT id FROM customers;",
            Cancellation);

        RunResult invalid = await RunAsync(
            [
                "migrate", "query-check", invalidQuery,
                "--dialect", "csharpdb",
                "--out", invalidReport,
                "--format", "json",
            ]);
        RunResult mysql = await RunAsync(
            [
                "migrate", "query-check", mysqlQuery,
                "--dialect", "mysql",
                "--out", mysqlReport,
                "--format", "json",
            ]);

        Assert.Equal(InspectorCommandRunner.ExitError, invalid.ExitCode);
        Assert.Equal(InspectorCommandRunner.ExitError, mysql.ExitCode);

        using JsonDocument invalidDocument = await ReadJsonAsync(invalidReport);
        JsonElement invalidResult =
            invalidDocument.RootElement.GetProperty("results")[0];
        Assert.Equal("unknown", invalidResult.GetProperty("status").GetString());
        Assert.False(invalidResult.GetProperty("sourceParsed").GetBoolean());
        Assert.Contains(
            invalidResult.GetProperty("diagnostics").EnumerateArray(),
            static diagnostic =>
                diagnostic.GetProperty("ruleId").GetString() ==
                "MIG-QUERY-SOURCE-PARSE-001");

        using JsonDocument mysqlDocument = await ReadJsonAsync(mysqlReport);
        JsonElement mysqlResult =
            mysqlDocument.RootElement.GetProperty("results")[0];
        Assert.Equal("mySql", mysqlResult.GetProperty("sourceDialect").GetString());
        Assert.Equal("unknown", mysqlResult.GetProperty("status").GetString());
        Assert.Contains(
            mysqlResult.GetProperty("diagnostics").EnumerateArray(),
            static diagnostic =>
                diagnostic.GetProperty("ruleId").GetString() ==
                "MIG-QUERY-DIALECT-001");
    }

    [Fact]
    public async Task QueryCheck_TextReportAndDefaultQueryIdAreStable()
    {
        using var workspace = new TemporaryDirectory();
        string queryPath = workspace.PathFor("stable-query.sql");
        string firstPath = workspace.PathFor("first.report.txt");
        string secondPath = workspace.PathFor("second.report.txt");
        await File.WriteAllTextAsync(
            queryPath,
            "SELECT id FROM customers ORDER BY id;",
            Cancellation);

        RunResult first = await RunAsync(
            [
                "migrate", "query-check", queryPath,
                "--dialect", "csharpdb",
                "--out", firstPath,
                "--format", "text",
            ]);
        RunResult second = await RunAsync(
            [
                "migrate", "query-check", queryPath,
                "--dialect", "csharpdb",
                "--out", secondPath,
                "--format", "text",
            ]);

        Assert.Equal(InspectorCommandRunner.ExitWarn, first.ExitCode);
        Assert.Equal(first.ExitCode, second.ExitCode);
        string firstText = await File.ReadAllTextAsync(firstPath, Cancellation);
        Assert.Equal(
            firstText,
            await File.ReadAllTextAsync(secondPath, Cancellation));
        Assert.Contains("Query compatibility report", firstText, StringComparison.Ordinal);
        Assert.Contains("[stable-query]", firstText, StringComparison.Ordinal);
        Assert.Contains("MIG-QUERY-UNBOUND-001", firstText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task QueryCheck_ExistingOutputIsNeverOverwritten()
    {
        using var workspace = new TemporaryDirectory();
        string queryPath = workspace.PathFor("query.sql");
        string reportPath = workspace.PathFor("existing.report.json");
        await File.WriteAllTextAsync(
            queryPath,
            "SELECT id FROM customers ORDER BY id;",
            Cancellation);
        byte[] sentinel = "do-not-overwrite-query-report"u8.ToArray();
        await File.WriteAllBytesAsync(reportPath, sentinel, Cancellation);

        RunResult result = await RunAsync(
            [
                "migrate", "query-check", queryPath,
                "--dialect", "csharpdb",
                "--out", reportPath,
                "--format", "json",
            ]);

        Assert.Equal(InspectorCommandRunner.ExitError, result.ExitCode);
        Assert.Equal(
            sentinel,
            await File.ReadAllBytesAsync(reportPath, Cancellation));
        AssertNoTemporaryPublications(workspace.Root);
    }

    [Fact]
    public async Task QueryCheck_InvalidOptionsReturnUsageWithoutPublishing()
    {
        using var workspace = new TemporaryDirectory();
        string queryPath = workspace.PathFor("query.sql");
        string outputPath = workspace.PathFor("unused.report.json");
        await File.WriteAllTextAsync(
            queryPath,
            "SELECT id FROM customers;",
            Cancellation);

        string[][] invalidArguments =
        [
            ["migrate", "query-check"],
            [
                "migrate", "query-check", queryPath,
                "--out", outputPath,
            ],
            [
                "migrate", "query-check", queryPath,
                "--dialect", "oracle",
                "--out", outputPath,
            ],
            [
                "migrate", "query-check", queryPath,
                "--dialect", "tsql",
                "--compatibility-level", "140",
                "--out", outputPath,
            ],
            [
                "migrate", "query-check", queryPath,
                "--dialect", "csharpdb",
                "--compatibility-level", "160",
                "--out", outputPath,
            ],
            [
                "migrate", "query-check", queryPath,
                "--dialect", "csharpdb",
                "--out", outputPath,
                "--format", "sarif",
            ],
            [
                "migrate", "query-check", queryPath,
                "--dialect", "csharpdb",
                "--out", outputPath,
                "--query-id", "",
            ],
            [
                "migrate", "query-check", queryPath,
                "--dialect", "csharpdb",
                "--out", outputPath,
                "--unknown", "value",
            ],
        ];

        foreach (string[] arguments in invalidArguments)
        {
            RunResult result = await RunAsync(arguments);
            Assert.Equal(InspectorCommandRunner.ExitUsage, result.ExitCode);
            Assert.False(File.Exists(outputPath));
            Assert.False(string.IsNullOrWhiteSpace(result.StdErr));
        }
    }

    private static async Task<string> WriteCatalogAsync(
        string path,
        string nativeType,
        string logicalType,
        params MigrationCatalogFacet[] additionalFacets)
    {
        var catalog = new MigrationCatalog
        {
            TargetCSharpDbVersion =
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            Source = new MigrationSourceIdentity
            {
                Kind = MigrationSourceKind.Synthetic,
                Identity = $"compatibility-test:{Path.GetFileName(path)}",
                Fingerprint = $"compatibility-test:{nativeType}:{logicalType}",
                Consistency = new MigrationConsistencyStrategy
                {
                    Kind = MigrationConsistencyKind.Immutable,
                    Description = "Immutable CLI compatibility fixture.",
                },
            },
            Objects =
            [
                new MigrationCatalogObject
                {
                    ObjectId = "compat:table:rows",
                    Kind = MigrationObjectKind.Table,
                    SourceName = "rows",
                },
                new MigrationCatalogObject
                {
                    ObjectId = "compat:column:rows:value",
                    Kind = MigrationObjectKind.Column,
                    ParentObjectId = "compat:table:rows",
                    SourceName = "value",
                    NativeType = nativeType,
                    Facets =
                    [
                        new MigrationCatalogFacet
                        {
                            Name = "logicalType",
                            Value = logicalType,
                        },
                        new MigrationCatalogFacet
                        {
                            Name = "profileKind",
                            Value = "Full",
                        },
                        new MigrationCatalogFacet
                        {
                            Name = "profileValuesExamined",
                            Value = "1",
                        },
                        new MigrationCatalogFacet
                        {
                            Name = "profileTotalValues",
                            Value = "1",
                        },
                        .. additionalFacets,
                    ],
                },
            ],
        };
        await File.WriteAllTextAsync(
            path,
            MigrationArtifactSerializer.SerializeCatalog(catalog),
            Cancellation);
        return path;
    }

    private static QueryCompatibilityReport
        CreateTsqlRewriteReport(
            string query,
            string queryId,
            string targetVersion)
    {
        string candidate =
            "SELECT  id FROM customers ORDER BY id LIMIT 10;";
        return new QueryCompatibilityReport
        {
            TargetCSharpDbVersion = targetVersion,
            CapabilityDigest =
                CSharpDbCapabilityCatalogLoader
                    .LoadEmbedded(targetVersion)
                    .Digest,
            Summary = new QueryCompatibilityReportSummary
            {
                Total = 1,
                Conditional = 1,
            },
            Results =
            [
                new QueryCompatibilityResult
                {
                    QueryId = queryId,
                    SourceDialect =
                        QuerySourceDialect.SqlServerTsql,
                    SourceDigest = Sha256(query),
                    Status =
                        MigrationCompatibilityStatus.Conditional,
                    Evidence = MigrationEvidenceLevel.Parsed,
                    SourceParsed = true,
                    TargetParsed = true,
                    IsReadOnly = true,
                    Rewrite = new QueryCompatibilityRewrite
                    {
                        RewriteId =
                            "tsql-top-integer-to-csharpdb-limit/v1",
                        CandidateCSharpDbSql = candidate,
                        CandidateDigest = Sha256(candidate),
                    },
                    Diagnostics =
                    [
                        new MigrationDiagnostic
                        {
                            DiagnosticId =
                                "query:000000000000000000000000",
                            RuleId =
                                QueryCompatibilityRuleIds
                                    .BindingNotPerformed,
                            Severity =
                                MigrationDiagnosticSeverity.Warning,
                            Status =
                                MigrationCompatibilityStatus.Conditional,
                            Evidence =
                                MigrationEvidenceLevel.Parsed,
                            Summary =
                                "Schema binding was not performed.",
                            Explanation =
                                "The test worker supplied parsed-only evidence.",
                            ObjectId = queryId,
                            Remediation =
                                "Bind the query before execution.",
                            CanOverride = false,
                        },
                    ],
                },
            ],
        };
    }

    private static string Sha256(string value) =>
        Convert.ToHexString(
                SHA256.HashData(
                    Encoding.UTF8.GetBytes(value)))
            .ToLowerInvariant();

    private static async Task<RunResult> RunAsync(
        string[] arguments,
        MigrationCommandDependencies? dependencies = null)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        int exitCode = await MigrationCommandRunner.RunAsync(
            arguments,
            output,
            error,
            dependencies ??
                MigrationCommandDependencies.Default,
            Cancellation);
        return new RunResult(
            exitCode,
            output.ToString(),
            error.ToString());
    }

    private static async Task<JsonDocument> ReadJsonAsync(string path) =>
        JsonDocument.Parse(
            await File.ReadAllTextAsync(path, Cancellation));

    private static void AssertMappingReportContract(JsonElement root)
    {
        Assert.Equal(
            "csharpdb-data-type-mapping-report/v1",
            root.GetProperty("format").GetString());
        Assert.Equal(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            root.GetProperty("targetCSharpDbVersion").GetString());
        Assert.Equal("synthetic", root.GetProperty("sourceKind").GetString());
        Assert.Matches("^[0-9a-f]{64}$", root.GetProperty("catalogDigest").GetString());
        Assert.Equal(
            "csharpdb-standard-mapping",
            root.GetProperty("mappingPolicyId").GetString());
        Assert.Equal(1, root.GetProperty("mappingPolicyVersion").GetInt32());
        Assert.Equal(1, root.GetProperty("summary").GetProperty("total").GetInt32());
        Assert.Single(root.GetProperty("entries").EnumerateArray());
    }

    private static void AssertQueryReportContract(JsonElement root)
    {
        Assert.Equal(
            "csharpdb-query-compatibility-report/v1",
            root.GetProperty("format").GetString());
        Assert.Equal(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            root.GetProperty("targetCSharpDbVersion").GetString());
        Assert.Matches("^[0-9a-f]{64}$", root.GetProperty("capabilityDigest").GetString());
        Assert.Equal(1, root.GetProperty("summary").GetProperty("total").GetInt32());
        Assert.Single(root.GetProperty("results").EnumerateArray());
    }

    private static void AssertNoTemporaryPublications(string directory) =>
        Assert.DoesNotContain(
            Directory.EnumerateFiles(directory),
            static path =>
                Path.GetFileName(path).Contains(
                    ".tmp",
                    StringComparison.OrdinalIgnoreCase));

    private sealed record RunResult(
        int ExitCode,
        string StdOut,
        string StdErr);

    private sealed class TemporaryDirectory : IDisposable
    {
        public TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb_compatibility_cli_{Guid.NewGuid():N}");
            Directory.CreateDirectory(Root);
        }

        public string Root { get; }

        public string PathFor(string fileName) =>
            Path.Combine(Root, fileName);

        public void Dispose()
        {
            if (Directory.Exists(Root))
                Directory.Delete(Root, recursive: true);
        }
    }
}
