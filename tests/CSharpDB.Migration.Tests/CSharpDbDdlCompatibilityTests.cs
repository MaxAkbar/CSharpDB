using System.Text.Json;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Sql;

namespace CSharpDB.Migration.Tests;

public sealed class CSharpDbDdlCompatibilityTests
{
    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task AnalyzeAsync_AllPersistentScalarTypesPassScratchProof()
    {
        const string script = """
            CREATE TABLE scalar_values (
                integer_value INTEGER NOT NULL DEFAULT -7,
                real_value REAL DEFAULT -0.25,
                text_value TEXT COLLATE NOCASE DEFAULT 'O''Brien',
                blob_value BLOB DEFAULT X'00FF'
            );
            """;

        CSharpDbDdlCompatibilityReport report =
            await AnalyzeAsync(script);

        Assert.Equal(
            CSharpDbDdlCompatibilityReport.CurrentFormat,
            report.Format);
        Assert.Equal("csharpdb", report.Dialect);
        Assert.Equal("csharpdb-sql/v1", report.SourceGrammar);
        Assert.True(
            report.Status == MigrationCompatibilityStatus.Compatible,
            Serialize(report));
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            report.HighestEvidence);
        Assert.Equal(
            CSharpDbDdlCompatibilityAnalyzer.ScratchEqualRuleId,
            report.RuleId);
        Assert.Equal(1, report.StatementCount);
        Assert.Equal(1, report.ProvenStatementCount);
        Assert.Equal(1, report.CandidateActionCount);
        Assert.Single(report.Statements);
        Assert.Equal("create-table", report.Statements[0].Kind);
        Assert.Equal(
            MigrationCompatibilityStatus.Compatible,
            report.Statements[0].Status);
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            report.Statements[0].Evidence);
        Assert.Empty(report.Diagnostics);
        Assert.Empty(report.Differences);
        AssertLowerSha256(report.ScriptDigest);
        AssertLowerSha256(report.CapabilityDigest);
        AssertLowerSha256(report.CatalogDigest);
        AssertLowerSha256(report.PlanContractDigest);
        AssertLowerSha256(report.GeneratedDdlDigest);
        AssertLowerSha256(report.ExpectedSchemaDigest);
        Assert.Equal(
            report.ExpectedSchemaDigest,
            report.ActualSchemaDigest);
    }

    [Fact]
    public async Task AnalyzeAsync_NonFiniteRealDefaultIsAnExplicitUnsupportedLiteral()
    {
        string script =
            $"CREATE TABLE non_finite_default (value REAL DEFAULT {new string('9', 400)}.0);";

        CSharpDbDdlCompatibilityReport report =
            await AnalyzeAsync(script);

        Assert.Equal(
            MigrationCompatibilityStatus.Unsupported,
            report.Status);
        CSharpDbDdlCompatibilityDiagnostic diagnostic =
            Assert.Single(report.Diagnostics);
        Assert.Equal(
            CSharpDbDdlCompatibilityAnalyzer.UnsupportedFeatureRuleId,
            diagnostic.RuleId);
        Assert.Contains(
            "literal",
            diagnostic.Summary,
            StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, report.CandidateActionCount);
    }

    [Fact]
    public async Task AnalyzeAsync_RealDefaultsRoundTripAsExponentFreeLiterals()
    {
        const string script = """
            CREATE TABLE real_defaults (
                whole_value REAL DEFAULT 7.0,
                negative_zero REAL DEFAULT -0.0,
                small_value REAL DEFAULT 0.00001,
                large_value REAL
                    DEFAULT 123456789012345678901234567890.0
            );
            """;

        CSharpDbDdlCompatibilityReport report =
            await AnalyzeAsync(script);

        Assert.Equal(
            MigrationCompatibilityStatus.Compatible,
            report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            report.HighestEvidence);
        Assert.Equal(1, report.ProvenStatementCount);
        Assert.Empty(report.Diagnostics);
        Assert.Empty(report.Differences);
        Assert.Equal(
            report.ExpectedSchemaDigest,
            report.ActualSchemaDigest);
    }

    [Fact]
    public async Task AnalyzeAsync_KeysForeignKeyAndIndexPassWithCanonicalRewrite()
    {
        const string script = """
            CREATE TABLE parent_items (
                id INTEGER NOT NULL,
                code TEXT,
                CONSTRAINT pk_parent_items PRIMARY KEY (id),
                CONSTRAINT uq_parent_items_code UNIQUE (code)
            );
            CREATE TABLE child_items (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL,
                CONSTRAINT fk_child_parent FOREIGN KEY (parent_id)
                    REFERENCES parent_items (id) ON DELETE CASCADE
            );
            CREATE INDEX ix_child_parent ON child_items (parent_id);
            """;

        CSharpDbDdlCompatibilityReport report =
            await AnalyzeAsync(script);

        Assert.Equal(
            MigrationCompatibilityStatus.CompatibleWithRewrite,
            report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            report.HighestEvidence);
        Assert.Equal(
            CSharpDbDdlCompatibilityAnalyzer.RewriteRuleId,
            report.RuleId);
        Assert.Equal(3, report.StatementCount);
        Assert.Equal(3, report.ProvenStatementCount);
        Assert.True(report.CandidateActionCount >= 6);
        Assert.All(
            report.Statements,
            item =>
            {
                Assert.Equal(
                    MigrationCompatibilityStatus
                        .CompatibleWithRewrite,
                    item.Status);
                Assert.Equal(
                    MigrationEvidenceLevel.ScratchExecuted,
                    item.Evidence);
            });
        Assert.Equal(
            ["create-table", "create-table", "create-index"],
            report.Statements.Select(item => item.Kind));
        CSharpDbDdlCompatibilityDiagnostic rewrite =
            Assert.Single(report.Diagnostics);
        Assert.Equal(
            CSharpDbDdlCompatibilityAnalyzer.RewriteRuleId,
            rewrite.RuleId);
        Assert.Equal(
            MigrationDiagnosticSeverity.Warning,
            rewrite.Severity);
        Assert.Empty(report.Differences);
        Assert.Equal(
            report.ExpectedSchemaDigest,
            report.ActualSchemaDigest);
    }

    [Fact]
    public async Task AnalyzeAsync_InlineForeignKeyResolvesPrimaryKeyAndIsRewritten()
    {
        const string script = """
            CREATE TABLE owners (
                id INTEGER PRIMARY KEY
            );
            CREATE TABLE owned_items (
                id INTEGER PRIMARY KEY,
                owner_id INTEGER REFERENCES owners
            );
            CREATE UNIQUE INDEX ux_owned_owner
                ON owned_items (owner_id);
            """;

        CSharpDbDdlCompatibilityReport report =
            await AnalyzeAsync(script);

        Assert.Equal(
            MigrationCompatibilityStatus.CompatibleWithRewrite,
            report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            report.HighestEvidence);
        Assert.Equal(3, report.ProvenStatementCount);
        Assert.Empty(report.Differences);
    }

    [Fact]
    public async Task AnalyzeAsync_AllImmediateForeignKeyActionsPassScratchProof()
    {
        const string script = """
            CREATE TABLE action_parent (
                id INTEGER PRIMARY KEY
            );
            CREATE TABLE action_child (
                id INTEGER PRIMARY KEY,
                delete_default_id INTEGER DEFAULT 1,
                update_cascade_id INTEGER NOT NULL,
                update_null_id INTEGER,
                update_default_id INTEGER DEFAULT 1,
                implicit_null_default_id INTEGER,
                CONSTRAINT fk_delete_default
                    FOREIGN KEY (delete_default_id)
                    REFERENCES action_parent (id)
                    ON DELETE SET DEFAULT,
                CONSTRAINT fk_update_cascade
                    FOREIGN KEY (update_cascade_id)
                    REFERENCES action_parent (id)
                    ON UPDATE CASCADE,
                CONSTRAINT fk_update_null
                    FOREIGN KEY (update_null_id)
                    REFERENCES action_parent (id)
                    ON UPDATE SET NULL,
                CONSTRAINT fk_update_default
                    FOREIGN KEY (update_default_id)
                    REFERENCES action_parent (id)
                    ON UPDATE SET DEFAULT,
                CONSTRAINT fk_implicit_null_default
                    FOREIGN KEY (implicit_null_default_id)
                    REFERENCES action_parent (id)
                    ON DELETE SET DEFAULT
                    ON UPDATE NO ACTION
            );
            """;

        CSharpDbDdlCompatibilityReport report =
            await AnalyzeAsync(script);

        Assert.Equal(
            MigrationCompatibilityStatus.CompatibleWithRewrite,
            report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            report.HighestEvidence);
        Assert.Equal(2, report.ProvenStatementCount);
        Assert.Empty(report.Differences);
        Assert.Equal(
            report.ExpectedSchemaDigest,
            report.ActualSchemaDigest);
    }

    [Fact]
    public async Task AnalyzeAsync_CompositeSetDefaultUsesOrderedTypedDefaults()
    {
        const string script = """
            CREATE TABLE composite_parent (
                tenant_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                CONSTRAINT pk_composite_parent
                    PRIMARY KEY (tenant_id, code)
            );
            CREATE TABLE composite_child (
                id INTEGER PRIMARY KEY,
                tenant_id INTEGER NOT NULL DEFAULT 7,
                code TEXT NOT NULL DEFAULT 'fallback',
                CONSTRAINT fk_composite_default
                    FOREIGN KEY (tenant_id, code)
                    REFERENCES composite_parent (tenant_id, code)
                    ON DELETE SET DEFAULT
                    ON UPDATE SET DEFAULT
            );
            """;

        CSharpDbDdlCompatibilityReport report =
            await AnalyzeAsync(script);

        Assert.Equal(
            MigrationCompatibilityStatus.CompatibleWithRewrite,
            report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            report.HighestEvidence);
        Assert.Equal(2, report.ProvenStatementCount);
        Assert.Empty(report.Differences);
        Assert.Equal(
            report.ExpectedSchemaDigest,
            report.ActualSchemaDigest);
    }

    [Theory]
    [InlineData(
        """
        CREATE TABLE action_parent (id INTEGER PRIMARY KEY);
        CREATE TABLE action_child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER NOT NULL REFERENCES action_parent(id)
                ON DELETE SET NULL
        );
        """)]
    [InlineData(
        """
        CREATE TABLE action_parent (id INTEGER PRIMARY KEY);
        CREATE TABLE action_child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER NOT NULL REFERENCES action_parent(id)
                ON DELETE SET DEFAULT
        );
        """)]
    [InlineData(
        """
        CREATE TABLE action_parent (id INTEGER PRIMARY KEY);
        CREATE TABLE action_child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER NOT NULL DEFAULT NULL
                REFERENCES action_parent(id)
                ON UPDATE SET DEFAULT
        );
        """)]
    [InlineData(
        """
        CREATE TABLE action_parent (id INTEGER PRIMARY KEY);
        CREATE TABLE action_child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER NOT NULL REFERENCES action_parent(id)
                ON UPDATE SET NULL
        );
        """)]
    public async Task AnalyzeAsync_RejectsIneligibleMutatingForeignKeyActions(
        string script)
    {
        CSharpDbDdlCompatibilityReport report =
            await AnalyzeAsync(script);

        Assert.Equal(
            MigrationCompatibilityStatus.Unsupported,
            report.Status);
        Assert.Contains(
            report.Diagnostics,
            diagnostic =>
                diagnostic.RuleId ==
                CSharpDbDdlCompatibilityAnalyzer
                    .UnsupportedFeatureRuleId);
        Assert.Equal(0, report.CandidateActionCount);
    }

    [Fact]
    public async Task AnalyzeAsync_IndexBeforeTableRequiresProvenReordering()
    {
        const string script = """
            CREATE INDEX ix_late_table_id ON late_table (id);
            CREATE TABLE late_table (id INTEGER);
            """;

        CSharpDbDdlCompatibilityReport report =
            await AnalyzeAsync(script);

        Assert.Equal(
            MigrationCompatibilityStatus.CompatibleWithRewrite,
            report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            report.HighestEvidence);
        Assert.Equal(2, report.ProvenStatementCount);
        Assert.Equal(
            report.ExpectedSchemaDigest,
            report.ActualSchemaDigest);
    }

    [Fact]
    public async Task AnalyzeAsync_GeneratedAndExplicitConstraintNameCollisionIsRewritten()
    {
        const string script = """
            CREATE TABLE collision_parent (
                id INTEGER PRIMARY KEY
            );
            CREATE TABLE collision_child (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER,
                CONSTRAINT ddl_pk_000001_000000
                    FOREIGN KEY (parent_id)
                    REFERENCES collision_parent (id)
            );
            """;

        CSharpDbDdlCompatibilityReport report =
            await AnalyzeAsync(script);

        Assert.Equal(
            MigrationCompatibilityStatus.CompatibleWithRewrite,
            report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.ScratchExecuted,
            report.HighestEvidence);
        Assert.Equal(2, report.ProvenStatementCount);
    }

    [Fact]
    public async Task AnalyzeAsync_PreservesExactWholeStatementSpans()
    {
        const string first =
            "CREATE TABLE first_table (id INTEGER)";
        const string second =
            "CREATE INDEX ix_first ON first_table (id)";
        string script =
            "-- heading\r\n" +
            "  " + first + ";\r\n" +
            "/* between */\r\n" +
            "    " + second + ";\r\n";

        CSharpDbDdlCompatibilityReport report =
            await AnalyzeAsync(script);

        Assert.Equal(
            MigrationCompatibilityStatus.Compatible,
            report.Status);
        Assert.Collection(
            report.Statements,
            item => AssertSpan(
                item.Span,
                script.IndexOf(first, StringComparison.Ordinal),
                first.Length + 1,
                line: 2,
                column: 3),
            item => AssertSpan(
                item.Span,
                script.IndexOf(second, StringComparison.Ordinal),
                second.Length + 1,
                line: 4,
                column: 5));
    }

    [Theory]
    [MemberData(nameof(UnsupportedScripts))]
    public async Task AnalyzeAsync_RejectsFeaturesOutsideAdditiveAllowlist(
        string script)
    {
        CSharpDbDdlCompatibilityReport report =
            await AnalyzeAsync(script);

        Assert.Equal(
            MigrationCompatibilityStatus.Unsupported,
            report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.Parsed,
            report.HighestEvidence);
        Assert.Equal(0, report.ProvenStatementCount);
        Assert.Equal(0, report.CandidateActionCount);
        Assert.NotEmpty(report.Diagnostics);
        Assert.Null(report.CatalogDigest);
        Assert.Null(report.PlanContractDigest);
        Assert.Null(report.GeneratedDdlDigest);
        Assert.Null(report.ExpectedSchemaDigest);
        Assert.Null(report.ActualSchemaDigest);
        Assert.Empty(report.Differences);
        Assert.All(
            report.Statements,
            item => Assert.NotEqual(
                MigrationCompatibilityStatus.Compatible,
                item.Status));
    }

    public static TheoryData<string> UnsupportedScripts => new()
    {
        "CREATE EXTERNAL TABLE external_data FROM 'private-source.csv';",
        "CREATE TEMP TABLE temp_data (id INTEGER);",
        "CREATE TABLE IF NOT EXISTS conditional_data (id INTEGER);",
        "CREATE TABLE default_data (id INTEGER DEFAULT abs(7));",
        "CREATE TABLE identity_data (id INTEGER IDENTITY);",
        "CREATE TABLE versioned_data (revision BLOB ROWVERSION);",
        "CREATE TABLE checked_data (value INTEGER CHECK (value > 0));",
        "CREATE TABLE unknown_collation (value TEXT COLLATE ordinal);",
        "CREATE VIEW private_view AS SELECT value FROM private_table;",
        """
        CREATE TRIGGER private_trigger AFTER INSERT ON private_table BEGIN
            INSERT INTO private_audit VALUES (1);
        END;
        """,
        "ALTER TABLE private_table ADD COLUMN value INTEGER;",
        "DROP TABLE private_table;",
        "INSERT INTO private_table VALUES (1);",
        """
        CREATE TABLE indexed_collation (value TEXT);
        CREATE INDEX ix_collated
            ON indexed_collation (value COLLATE NOCASE);
        """,
    };

    [Fact]
    public async Task AnalyzeAsync_ExternalTablePathIsNeverTouchedOrPublished()
    {
        string privateName =
            "ddl-external-" + Guid.NewGuid().ToString("N");
        string privatePath = Path.Combine(
            Path.GetTempPath(),
            privateName,
            "private-source.csv");
        string script =
            $"CREATE EXTERNAL TABLE ext FROM '{privatePath.Replace("\\", "/", StringComparison.Ordinal)}';";

        CSharpDbDdlCompatibilityReport report =
            await AnalyzeAsync(script);

        Assert.Equal(
            MigrationCompatibilityStatus.Unsupported,
            report.Status);
        Assert.False(File.Exists(privatePath));
        Assert.False(
            Directory.Exists(
                Path.GetDirectoryName(privatePath)!));
        string published = Serialize(report);
        Assert.DoesNotContain(
            privatePath,
            published,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            privateName,
            published,
            StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, report.CandidateActionCount);
    }

    [Fact]
    public async Task AnalyzeAsync_MalformedInputIsUnsupportedWithExactParseSpan()
    {
        const string script =
            "-- preface\n  CREATE TABLE private_table (id INTEGER, );";

        CSharpDbDdlCompatibilityReport report =
            await AnalyzeAsync(script);

        Assert.Equal(
            MigrationCompatibilityStatus.Unsupported,
            report.Status);
        Assert.Null(report.HighestEvidence);
        Assert.Equal(
            CSharpDbDdlCompatibilityAnalyzer.ParseRuleId,
            report.RuleId);
        Assert.Equal(0, report.StatementCount);
        CSharpDbDdlCompatibilityDiagnostic diagnostic =
            Assert.Single(report.Diagnostics);
        Assert.Equal(
            CSharpDbDdlCompatibilityAnalyzer.ParseRuleId,
            diagnostic.RuleId);
        Assert.Null(diagnostic.Evidence);
        Assert.NotNull(diagnostic.SourceSpan);
        Assert.Equal("input", diagnostic.SourceSpan.SourceId);
        int expectedStart =
            script.IndexOf(")", StringComparison.Ordinal);
        int secondLineStart =
            script.IndexOf('\n') + 1;
        Assert.Equal(expectedStart, diagnostic.SourceSpan.Start);
        Assert.Equal(1, diagnostic.SourceSpan.Length);
        Assert.Equal(2, diagnostic.SourceSpan.Line);
        Assert.Equal(
            expectedStart - secondLineStart + 1,
            diagnostic.SourceSpan.Column);
        Assert.DoesNotContain(
            "private_table",
            Serialize(report),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnalyzeAsync_InvalidUtf16HasNoParsedEvidenceAndDistinctDigest()
    {
        CSharpDbDdlCompatibilityReport first =
            await AnalyzeAsync("\uD800");
        CSharpDbDdlCompatibilityReport second =
            await AnalyzeAsync("\uD801");

        Assert.Equal(
            MigrationCompatibilityStatus.Unsupported,
            first.Status);
        Assert.Null(first.HighestEvidence);
        Assert.Null(Assert.Single(first.Diagnostics).Evidence);
        Assert.NotEqual(first.ScriptDigest, second.ScriptDigest);
    }

    [Fact]
    public async Task AnalyzeAsync_ParserLimitIsUnknownAndSanitized()
    {
        const string script =
            "CREATE TABLE private_limited_table (id INTEGER);";
        var options = CSharpDbDdlCompatibilityOptions.Default with
        {
            ParserOptions =
                SqlScriptParserOptions.Default with
                {
                    MaxScriptCharacters = 12,
                },
        };

        CSharpDbDdlCompatibilityReport report =
            await AnalyzeAsync(script, options);

        Assert.Equal(
            MigrationCompatibilityStatus.Unknown,
            report.Status);
        Assert.Null(report.HighestEvidence);
        Assert.Equal(
            CSharpDbDdlCompatibilityAnalyzer.ParseLimitRuleId,
            report.RuleId);
        Assert.Equal(0, report.CandidateActionCount);
        Assert.DoesNotContain(
            "private_limited_table",
            Serialize(report),
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData("-- comments only\n/* still no DDL */;;")]
    public async Task AnalyzeAsync_EmptyOrCommentOnlyScriptFailsClosed(
        string script)
    {
        CSharpDbDdlCompatibilityReport report =
            await AnalyzeAsync(script);

        Assert.Equal(
            MigrationCompatibilityStatus.Unsupported,
            report.Status);
        Assert.Equal(
            MigrationEvidenceLevel.Parsed,
            report.HighestEvidence);
        Assert.Equal(
            CSharpDbDdlCompatibilityAnalyzer.EmptyScriptRuleId,
            report.RuleId);
        Assert.Equal(0, report.StatementCount);
        Assert.Equal(0, report.ProvenStatementCount);
        Assert.Equal(0, report.CandidateActionCount);
        Assert.Single(report.Diagnostics);
    }

    [Theory]
    [MemberData(nameof(DuplicateAndInvalidReferenceScripts))]
    public async Task AnalyzeAsync_RejectsDuplicateOrUnresolvedObjects(
        string script,
        string expectedRule)
    {
        CSharpDbDdlCompatibilityReport report =
            await AnalyzeAsync(script);

        Assert.Equal(
            MigrationCompatibilityStatus.Unsupported,
            report.Status);
        Assert.Equal(0, report.ProvenStatementCount);
        Assert.Equal(0, report.CandidateActionCount);
        Assert.Contains(
            report.Diagnostics,
            item => item.RuleId == expectedRule);
        Assert.Empty(report.Differences);
    }

    public static TheoryData<string, string>
        DuplicateAndInvalidReferenceScripts => new()
        {
            {
                """
                CREATE TABLE duplicate_name (id INTEGER);
                CREATE TABLE DUPLICATE_NAME (id INTEGER);
                """,
                CSharpDbDdlCompatibilityAnalyzer.DuplicateObjectRuleId
            },
            {
                "CREATE TABLE duplicate_columns (id INTEGER, ID TEXT);",
                CSharpDbDdlCompatibilityAnalyzer.DuplicateObjectRuleId
            },
            {
                """
                CREATE TABLE indexed_table (id INTEGER);
                CREATE INDEX duplicate_index ON indexed_table (id);
                CREATE INDEX DUPLICATE_INDEX ON indexed_table (id);
                """,
                CSharpDbDdlCompatibilityAnalyzer.DuplicateObjectRuleId
            },
            {
                """
                CREATE TABLE indexed_table (id INTEGER);
                CREATE INDEX missing_column ON indexed_table (absent);
                """,
                CSharpDbDdlCompatibilityAnalyzer.InvalidReferenceRuleId
            },
            {
                """
                CREATE TABLE child_table (
                    parent_id INTEGER REFERENCES missing_parent (id)
                );
                """,
                CSharpDbDdlCompatibilityAnalyzer.InvalidReferenceRuleId
            },
            {
                """
                CREATE TABLE parent_table (id INTEGER);
                CREATE TABLE child_table (
                    parent_id INTEGER REFERENCES parent_table (id)
                );
                """,
                CSharpDbDdlCompatibilityAnalyzer.InvalidReferenceRuleId
            },
            {
                """
                CREATE TABLE parent_table (
                    id INTEGER,
                    CONSTRAINT uq_parent_one UNIQUE (id),
                    CONSTRAINT uq_parent_two UNIQUE (id)
                );
                CREATE TABLE child_table (
                    parent_id INTEGER REFERENCES parent_table (id)
                );
                """,
                CSharpDbDdlCompatibilityAnalyzer.InvalidReferenceRuleId
            },
        };

    [Theory]
    [InlineData(
        "CREATE TABLE real_key (value REAL PRIMARY KEY);")]
    [InlineData(
        "CREATE TABLE blob_key (value BLOB, PRIMARY KEY (value));")]
    [InlineData(
        """
        CREATE TABLE real_index (value REAL);
        CREATE INDEX ix_real ON real_index (value);
        """)]
    [InlineData(
        """
        CREATE TABLE blob_index (value BLOB);
        CREATE INDEX ix_blob ON blob_index (value);
        """)]
    public async Task AnalyzeAsync_RejectsRealOrBlobKeyAndIndex(
        string script)
    {
        CSharpDbDdlCompatibilityReport report =
            await AnalyzeAsync(script);

        Assert.Equal(
            MigrationCompatibilityStatus.Unsupported,
            report.Status);
        Assert.Contains(
            report.Diagnostics,
            item =>
                item.RuleId ==
                CSharpDbDdlCompatibilityAnalyzer
                    .UnsupportedFeatureRuleId);
        Assert.Equal(0, report.CandidateActionCount);
    }

    [Fact]
    public async Task AnalyzeAsync_MixedSupportedAndUnsupportedScriptNeverExecutesPartialCandidate()
    {
        const string script = """
            CREATE TABLE safe_table (id INTEGER);
            DROP TABLE private_existing_target;
            """;

        CSharpDbDdlCompatibilityReport report =
            await AnalyzeAsync(script);

        Assert.Equal(
            MigrationCompatibilityStatus.Unsupported,
            report.Status);
        Assert.Equal(2, report.StatementCount);
        Assert.Equal(0, report.ProvenStatementCount);
        Assert.Equal(0, report.CandidateActionCount);
        Assert.Null(report.CatalogDigest);
        Assert.Null(report.GeneratedDdlDigest);
        Assert.Null(report.ExpectedSchemaDigest);
        Assert.Null(report.ActualSchemaDigest);
        Assert.Contains(
            report.Statements,
            item =>
                item.Kind == "unsupported" &&
                item.Status ==
                MigrationCompatibilityStatus.Unsupported);
        Assert.DoesNotContain(
            report.Statements,
            item =>
                item.Status ==
                MigrationCompatibilityStatus.Compatible);
    }

    [Fact]
    public async Task AnalyzeAsync_ReportIsDeterministicAndRedactsInputContent()
    {
        const string privateTable =
            "customer_secret_7412";
        const string privateColumn =
            "account_secret_8273";
        const string privateIndex =
            "index_secret_9631";
        string script = $"""
            CREATE TABLE {privateTable} (
                {privateColumn} TEXT NOT NULL
            );
            CREATE INDEX {privateIndex}
                ON {privateTable} ({privateColumn});
            """;

        CSharpDbDdlCompatibilityReport first =
            await AnalyzeAsync(script);
        CSharpDbDdlCompatibilityReport repeated =
            await AnalyzeAsync(script);
        string firstJson = Serialize(first);
        string repeatedJson = Serialize(repeated);

        Assert.Equal(firstJson, repeatedJson);
        Assert.Equal(
            MigrationCompatibilityStatus.Compatible,
            first.Status);
        Assert.DoesNotContain(
            privateTable,
            firstJson,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            privateColumn,
            firstJson,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            privateIndex,
            firstJson,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            script,
            firstJson,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "CREATE TABLE",
            firstJson,
            StringComparison.OrdinalIgnoreCase);
        Assert.All(
            first.Statements,
            item => Assert.Equal("input", item.Span.SourceId));
    }

    [Fact]
    public async Task AnalyzeAsync_ReducedPreviewAndScratchBoundsFailClosed()
    {
        const string script = """
            CREATE TABLE bounded_table (id INTEGER);
            CREATE INDEX ix_bounded ON bounded_table (id);
            """;
        var previewOptions =
            CSharpDbDdlCompatibilityOptions.Default with
            {
                PreviewOptions =
                    CSharpDbDdlPreviewBuildOptions.Default with
                    {
                        MaxActionCount = 1,
                    },
            };
        var scratchOptions =
            CSharpDbDdlCompatibilityOptions.Default with
            {
                ScratchOptions =
                    CSharpDbDdlScratchValidationOptions.Default with
                    {
                        MaxActionCount = 1,
                    },
            };

        CSharpDbDdlCompatibilityReport previewLimited =
            await AnalyzeAsync(script, previewOptions);
        CSharpDbDdlCompatibilityReport scratchLimited =
            await AnalyzeAsync(script, scratchOptions);

        Assert.Equal(
            MigrationCompatibilityStatus.Unknown,
            previewLimited.Status);
        Assert.Equal(
            CSharpDbDdlCompatibilityAnalyzer.RenderLimitRuleId,
            previewLimited.RuleId);
        Assert.Equal(0, previewLimited.CandidateActionCount);
        Assert.NotNull(previewLimited.CatalogDigest);
        Assert.Null(previewLimited.GeneratedDdlDigest);

        Assert.Equal(
            MigrationCompatibilityStatus.Unknown,
            scratchLimited.Status);
        Assert.Equal(
            CSharpDbDdlCompatibilityAnalyzer.ScratchRejectedRuleId,
            scratchLimited.RuleId);
        Assert.Equal(2, scratchLimited.CandidateActionCount);
        Assert.NotNull(scratchLimited.GeneratedDdlDigest);
        Assert.Equal(0, scratchLimited.ProvenStatementCount);
    }

    [Fact]
    public async Task AnalyzeAsync_PropagatesPreCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () =>
                await CSharpDbDdlCompatibilityAnalyzer.AnalyzeAsync(
                    "CREATE TABLE cancelled_table (id INTEGER);",
                    cancellationToken: cancellation.Token));
    }

    private static async Task<CSharpDbDdlCompatibilityReport>
        AnalyzeAsync(
            string script,
            CSharpDbDdlCompatibilityOptions? options = null) =>
        await CSharpDbDdlCompatibilityAnalyzer.AnalyzeAsync(
            script,
            options,
            Ct);

    private static void AssertSpan(
        MigrationSourceSpan span,
        int start,
        int length,
        int line,
        int column)
    {
        Assert.Equal("input", span.SourceId);
        Assert.Equal(start, span.Start);
        Assert.Equal(length, span.Length);
        Assert.Equal(line, span.Line);
        Assert.Equal(column, span.Column);
    }

    private static string Serialize(
        CSharpDbDdlCompatibilityReport report) =>
        JsonSerializer.Serialize(report);

    private static void AssertLowerSha256(string? digest)
    {
        Assert.NotNull(digest);
        Assert.Matches("^[0-9a-f]{64}$", digest);
    }
}
