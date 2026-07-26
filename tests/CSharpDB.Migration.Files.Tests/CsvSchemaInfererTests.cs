using System.Globalization;
using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Csv;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Tests;

public sealed class CsvSchemaInfererTests
{
    [Fact]
    public async Task FullProfileInfersOnlyStrictLosslessLogicalKinds()
    {
        const string csv =
            "flag,signed,unsigned,amount,guid,date,time,datetime,offset,text\n" +
            "true,-1,9223372036854775808,999,6f9619ff-8b86-d011-b42d-00cf4fc964ff,2024-02-29,12:34:56,2024-02-29T12:34:56,2024-02-29T12:34:56Z,alpha\n" +
            "false,2,18446744073709551615,0.99,7f9619ff-8b86-d011-b42d-00cf4fc964ff,2025-03-01,01:02:03.1234567,2025-03-01 01:02:03.4,2025-03-01T01:02:03.4+05:30,beta\n";
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync(csv);
        await using (snapshot)
        {
            CsvSchemaInferenceResult result = await InferAsync(binding, snapshot, 100);

            Assert.True(result.ReachedEndOfSource);
            Assert.Equal(MigrationCoverageKind.Full, result.Coverage.Kind);
            Assert.Equal(2, result.Coverage.TotalValues);
            Assert.Equal(
                [
                    CsvColumnLogicalType.Boolean,
                    CsvColumnLogicalType.SignedInteger,
                    CsvColumnLogicalType.UnsignedInteger,
                    CsvColumnLogicalType.Decimal,
                    CsvColumnLogicalType.Guid,
                    CsvColumnLogicalType.Date,
                    CsvColumnLogicalType.Time,
                    CsvColumnLogicalType.DateTime,
                    CsvColumnLogicalType.DateTimeOffset,
                    CsvColumnLogicalType.Text,
                ],
                result.Columns.Select(column => column.LogicalType));
            Assert.All(result.Columns, column => Assert.False(column.Nullable));

            CsvColumnSchema amount = result.Columns[3];
            Assert.Equal(5, amount.ObservedPrecision);
            Assert.Equal(2, amount.ObservedScale);
            Assert.Equal(CsvInferenceConfidence.High, amount.Confidence);

            MigrationCatalog catalog = result.CreateCatalog(CurrentVersion);
            MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
            Assert.Equal(DbType.Text, Mapping(plan, "csv:column:2").TargetType);
            Assert.Equal(
                MigrationMappingClassification.LosslessReencoded,
                Mapping(plan, "csv:column:2").Classification);
            Assert.Equal(DbType.Integer, Mapping(plan, "csv:column:3").TargetType);
            Assert.Equal("5", Facet(Column(catalog, 3), "precision"));
            Assert.Equal("2", Facet(Column(catalog, 3), "scale"));
        }
    }

    [Theory]
    [InlineData("001")]
    [InlineData("-01")]
    [InlineData("+1")]
    [InlineData("-0")]
    [InlineData("1e3")]
    public async Task LexicallySignificantNumericTextIsNeverNarrowed(string value)
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync(
            $"identifier\n{value}\n{value}\n");
        await using (snapshot)
        {
            CsvColumnSchema column = Assert.Single((await InferAsync(binding, snapshot, 100)).Columns);

            Assert.Equal(CsvColumnLogicalType.Text, column.LogicalType);
            Assert.Equal(CsvColumnSchemaResolution.DefaultedToText, column.Resolution);
            Assert.Equal(CsvColumnInferenceReason.LexicalPreservation, column.Reason);
            Assert.Equal(2, column.NonCanonicalNumericCount);
        }
    }

    [Fact]
    public async Task ABoundedPrefixReportsSampleCoverageWithUnknownTotal()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync(
            "id\n1\n2\nlate-text\n");
        await using (snapshot)
        {
            CsvSchemaInferenceResult result = await InferAsync(binding, snapshot, 2);
            CsvColumnSchema column = Assert.Single(result.Columns);

            Assert.False(result.ReachedEndOfSource);
            Assert.Equal(MigrationCoverageKind.Sample, result.Coverage.Kind);
            Assert.Equal(2, result.Coverage.ValuesExamined);
            Assert.Null(result.Coverage.TotalValues);
            Assert.True(result.Coverage.RequiresFullStreamValidation);
            Assert.Equal(CsvColumnLogicalType.SignedInteger, column.LogicalType);
            Assert.True(column.Nullable);
            Assert.False(result.TryNormalizeScalar(0, "late-text", out _));

            MigrationCatalog catalog = result.CreateCatalog(CurrentVersion);
            MigrationCatalogObject catalogColumn = Column(catalog, 0);
            Assert.Null(Facet(catalogColumn, "profileTotalValues"));
            MigrationProfileCoverage planned = Mapping(
                new MigrationPlanner().CreatePlan(catalog),
                "csv:column:0").Coverage;
            Assert.Null(planned.TotalValues);
            Assert.True(planned.RequiresFullStreamValidation);
        }
    }

    [Fact]
    public async Task SampledDecimalShapeIsObservedButNeverActivatedAsATargetBound()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync(
            "amount\n999\n0.99\n1.2345\n");
        await using (snapshot)
        {
            CsvSchemaInferenceResult result = await InferAsync(binding, snapshot, 2);
            CsvColumnSchema column = Assert.Single(result.Columns);
            MigrationCatalogObject catalogColumn = Column(result.CreateCatalog(CurrentVersion), 0);

            Assert.Equal(CsvColumnLogicalType.Decimal, column.LogicalType);
            Assert.Equal(5, column.ObservedPrecision);
            Assert.Equal(2, column.ObservedScale);
            Assert.Equal("5", Facet(catalogColumn, "observedPrecision"));
            Assert.Equal("2", Facet(catalogColumn, "observedScale"));
            Assert.Null(Facet(catalogColumn, "precision"));
            Assert.Null(Facet(catalogColumn, "scale"));
        }
    }

    [Fact]
    public async Task ExactSampleLimitStillReportsFullWhenTheLookAheadReachesEof()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync("id\n1\n2\n");
        await using (snapshot)
        {
            CsvSchemaInferenceResult result = await InferAsync(binding, snapshot, 2);

            Assert.True(result.ReachedEndOfSource);
            Assert.Equal(MigrationCoverageKind.Full, result.Coverage.Kind);
            Assert.Equal(2, result.Coverage.TotalValues);
        }
    }

    [Fact]
    public async Task NullEmptyAndMissingRemainSeparateAndMissingBlocksReadiness()
    {
        var readerOptions = new CsvReaderOptions { NullToken = "NULL" };
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync(
            "id,value,note\n1,,x\n2,NULL\n",
            readerOptions);
        await using (snapshot)
        {
            CsvSchemaInferenceResult result = await InferAsync(binding, snapshot, 100);
            CsvColumnSchema value = result.Columns[1];
            CsvColumnSchema note = result.Columns[2];

            Assert.Equal(1, value.EmptyCount);
            Assert.Equal(1, value.NullCount);
            Assert.Equal(CsvColumnLogicalType.Text, value.LogicalType);
            Assert.True(value.Nullable);
            Assert.Equal(1, note.MissingCount);
            Assert.Equal(2, note.FirstMissingDataRecordNumber);
            Assert.Contains(result.Diagnostics, item => item.RuleId == "MIG-CSV-SCHEMA-MISSING-001");

            MigrationCatalog catalog = result.CreateCatalog(CurrentVersion);
            MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
            MigrationPlanReadiness readiness = MigrationPlanReadinessValidator.Evaluate(plan, catalog);
            Assert.Equal(MigrationPlanReadinessStatus.Blocked, readiness.Status);
            Assert.Contains(
                plan.Diagnostics,
                item => item.RuleId == "MIG-CSV-SCHEMA-MISSING-001" &&
                        readiness.BlockingDiagnosticIds.Contains(item.DiagnosticId));
        }
    }

    [Fact]
    public async Task QuotedNullTokenRemainsLiteralTextByDefault()
    {
        var readerOptions = new CsvReaderOptions { NullToken = "NULL" };
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync(
            "value\nNULL\n\"NULL\"\n",
            readerOptions);
        await using (snapshot)
        {
            CsvColumnSchema column = Assert.Single((await InferAsync(binding, snapshot, 100)).Columns);

            Assert.Equal(1, column.NullCount);
            Assert.Equal(1, column.SubstantiveValueCount);
            Assert.Equal(1, column.QuotedCount);
            Assert.Equal(CsvColumnLogicalType.Text, column.LogicalType);
        }
    }

    [Fact]
    public async Task ExplicitOverrideIsOrdinalGuardedAndContradictionsAreVisible()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync(
            "id\n1\nnot-an-integer\n");
        await using (snapshot)
        {
            var options = new CsvSchemaInferenceOptions
            {
                ColumnOverrides =
                [
                    new CsvColumnSchemaOverride
                    {
                        ColumnIndex = 0,
                        ExpectedHeader = "id",
                        LogicalType = CsvColumnLogicalType.SignedInteger,
                        Nullable = false,
                    },
                ],
            };
            CsvSchemaInferenceResult result = await InferAsync(binding, snapshot, 100, options);
            CsvColumnSchema column = Assert.Single(result.Columns);

            Assert.Equal(CsvColumnLogicalType.SignedInteger, column.LogicalType);
            Assert.Equal(CsvColumnSchemaResolution.ExplicitOverride, column.Resolution);
            Assert.Equal(CsvInferenceConfidence.Explicit, column.Confidence);
            Assert.Equal(CsvOverrideValidationStatus.Incompatible, column.OverrideValidation);
            Assert.Equal(2, column.FirstOverrideMismatchDataRecordNumber);
            Assert.Contains(result.Diagnostics, item => item.RuleId == "MIG-CSV-SCHEMA-OVERRIDE-001");
        }
    }

    [Fact]
    public async Task ExplicitNumericOverrideAuthorizesVersionedLexicalNormalization()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync(
            "id\n001\n+2\n");
        await using (snapshot)
        {
            CsvColumnSchema automatic = Assert.Single(
                (await InferAsync(binding, snapshot, 100)).Columns);
            Assert.Equal(CsvColumnLogicalType.Text, automatic.LogicalType);
            Assert.Equal(CsvColumnInferenceReason.LexicalPreservation, automatic.Reason);

            CsvSchemaInferenceResult declared = await InferAsync(
                binding,
                snapshot,
                100,
                new CsvSchemaInferenceOptions
                {
                    ColumnOverrides = [Override(0, CsvColumnLogicalType.SignedInteger)],
                });
            CsvColumnSchema column = Assert.Single(declared.Columns);

            Assert.Equal(CsvColumnLogicalType.SignedInteger, column.LogicalType);
            Assert.Equal(CsvOverrideValidationStatus.FullCompatible, column.OverrideValidation);
            Assert.True(declared.TryNormalizeScalar(0, "001", out string? first));
            Assert.Equal("1", first);
            Assert.True(declared.TryNormalizeScalar(0, "+2", out string? second));
            Assert.Equal("2", second);
        }
    }

    [Fact]
    public async Task SampleCompatibleOverrideStillRejectsAContradictoryTailValue()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync(
            "id\n001\n002\nlate-text\n");
        await using (snapshot)
        {
            CsvSchemaInferenceResult result = await InferAsync(
                binding,
                snapshot,
                2,
                new CsvSchemaInferenceOptions
                {
                    ColumnOverrides = [Override(0, CsvColumnLogicalType.SignedInteger)],
                });
            CsvColumnSchema column = Assert.Single(result.Columns);

            Assert.Equal(MigrationCoverageKind.Sample, result.Coverage.Kind);
            Assert.Equal(CsvOverrideValidationStatus.SampleCompatible, column.OverrideValidation);
            Assert.True(result.TryNormalizeScalar(0, "002", out string? canonical));
            Assert.Equal("2", canonical);
            Assert.False(result.TryNormalizeScalar(0, "late-text", out _));
        }
    }

    [Fact]
    public async Task OverrideValidationRejectsWrongHeadersDuplicatesAndUnknownOrdinals()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync("id\n1\n2\n");
        await using (snapshot)
        {
            var wrongHeader = new CsvSchemaInferenceOptions
            {
                ColumnOverrides = [Override(0, CsvColumnLogicalType.SignedInteger, "other")],
            };
            await Assert.ThrowsAsync<ArgumentException>(
                async () => await InferAsync(binding, snapshot, 10, wrongHeader));

            var duplicate = new CsvSchemaInferenceOptions
            {
                ColumnOverrides =
                [
                    Override(0, CsvColumnLogicalType.SignedInteger),
                    Override(0, CsvColumnLogicalType.Text),
                ],
            };
            await Assert.ThrowsAsync<ArgumentException>(
                async () => await InferAsync(binding, snapshot, 10, duplicate));

            var unknown = new CsvSchemaInferenceOptions
            {
                ColumnOverrides = [Override(1, CsvColumnLogicalType.Text)],
            };
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                async () => await InferAsync(binding, snapshot, 10, unknown));
        }
    }

    [Fact]
    public async Task BlankAndDuplicateHeadersRemainFactsWithOrdinalIds()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync(
            ",Name,name\n1,a,b\n2,c,d\n");
        await using (snapshot)
        {
            CsvSchemaInferenceResult result = await InferAsync(binding, snapshot, 100);
            MigrationCatalog catalog = result.CreateCatalog(CurrentVersion);

            Assert.Equal("column_1", result.Columns[0].SourceName);
            Assert.Equal(string.Empty, result.Columns[0].OriginalHeader);
            Assert.Equal("Name", result.Columns[1].SourceName);
            Assert.Equal("name", result.Columns[2].SourceName);
            Assert.Equal(
                ["csv:column:0", "csv:column:1", "csv:column:2"],
                catalog.Objects.Where(item => item.Kind == MigrationObjectKind.Column)
                    .Select(item => item.ObjectId));
            Assert.Equal(3, result.Diagnostics.Count(item => item.RuleId == "MIG-CSV-SCHEMA-HEADER-001"));

            IReadOnlyDictionary<string, string> names = DeterministicMigrationNameMapper.Map(catalog);
            Assert.False(string.Equals(
                names["csv:column:1"],
                names["csv:column:2"],
                StringComparison.OrdinalIgnoreCase));
        }
    }

    [Fact]
    public async Task HeaderlessWidthUsesStableOrdinalNames()
    {
        var readerOptions = new CsvReaderOptions { HasHeaderRecord = false };
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync(
            "1,alpha\n2,beta\n",
            readerOptions);
        await using (snapshot)
        {
            CsvSchemaInferenceResult result = await InferAsync(binding, snapshot, 100);

            Assert.Equal(["column_1", "column_2"], result.Columns.Select(item => item.SourceName));
            Assert.All(result.Columns, column => Assert.Null(column.OriginalHeader));
            Assert.Equal(CsvColumnLogicalType.SignedInteger, result.Columns[0].LogicalType);
            Assert.Equal(CsvColumnLogicalType.Text, result.Columns[1].LogicalType);
        }
    }

    [Fact]
    public async Task CultureBoundDecimalParsingDoesNotUseAmbientCulture()
    {
        var readerOptions = new CsvReaderOptions
        {
            Delimiter = ";",
            Culture = CultureInfo.GetCultureInfo("de-DE"),
        };
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync(
            "amount\n1,25\n2,50\n",
            readerOptions);
        await using (snapshot)
        {
            CultureInfo original = CultureInfo.CurrentCulture;
            try
            {
                CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("ja-JP");
                CsvColumnSchema column = Assert.Single((await InferAsync(binding, snapshot, 100)).Columns);

                Assert.Equal(CsvColumnLogicalType.Decimal, column.LogicalType);
                Assert.Equal(3, column.ObservedPrecision);
                Assert.Equal(2, column.ObservedScale);
                CsvSchemaInferenceResult result = await InferAsync(binding, snapshot, 100);
                Assert.True(result.TryNormalizeScalar(0, "1,25", out string? canonical));
                Assert.Equal("1.25", canonical);
            }
            finally
            {
                CultureInfo.CurrentCulture = original;
            }
        }
    }

    [Fact]
    public async Task OneValueProducesOnlyALowConfidenceSuggestion()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync("id\n42\n");
        await using (snapshot)
        {
            CsvColumnSchema column = Assert.Single((await InferAsync(binding, snapshot, 100)).Columns);

            Assert.Equal(CsvColumnLogicalType.Text, column.LogicalType);
            Assert.Equal(CsvColumnLogicalType.SignedInteger, column.SuggestedLogicalType);
            Assert.Equal(CsvInferenceConfidence.Low, column.Confidence);
            Assert.Equal(CsvColumnInferenceReason.InsufficientEvidence, column.Reason);
        }
    }

    [Fact]
    public async Task OneSidedBooleanEvidenceRemainsTextWithALowConfidenceSuggestion()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync(
            "enabled\ntrue\ntrue\n");
        await using (snapshot)
        {
            CsvColumnSchema column = Assert.Single((await InferAsync(binding, snapshot, 100)).Columns);

            Assert.Equal(CsvColumnLogicalType.Text, column.LogicalType);
            Assert.Equal(CsvColumnLogicalType.Boolean, column.SuggestedLogicalType);
            Assert.Equal(CsvInferenceConfidence.Low, column.Confidence);
            Assert.Equal(CsvColumnInferenceReason.InsufficientEvidence, column.Reason);
        }
    }

    [Fact]
    public async Task HeaderOnlyColumnsRemainNullableWithoutValueEvidence()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync("id,name\n");
        await using (snapshot)
        {
            CsvSchemaInferenceResult result = await InferAsync(binding, snapshot, 100);

            Assert.Equal(MigrationCoverageKind.Full, result.Coverage.Kind);
            Assert.Equal(0, result.RecordsExamined);
            Assert.All(result.Columns, column => Assert.True(column.Nullable));
        }
    }

    [Fact]
    public async Task CumulativeCharacterLimitBoundsProfileWorkAndReportsSampleCoverage()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync(
            "value\nab\ncd\nef\n");
        await using (snapshot)
        {
            CsvSchemaInferenceResult result = await InferAsync(
                binding,
                snapshot,
                100,
                new CsvSchemaInferenceOptions { MaxProfileCharacters = 4 });

            Assert.True(result.ProfileCharacterLimitReached);
            Assert.Equal(4, result.ProfileCharactersExamined);
            Assert.Equal(2, result.RecordsExamined);
            Assert.Equal(MigrationCoverageKind.Sample, result.Coverage.Kind);
            Assert.Null(result.Coverage.TotalValues);
        }
    }

    [Fact]
    public async Task OverrideIsNotCalledCompatibleWhenNoRecordFitsTheProfileBudget()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync(
            "value\n12345\n67890\n");
        await using (snapshot)
        {
            CsvSchemaInferenceResult result = await InferAsync(
                binding,
                snapshot,
                100,
                new CsvSchemaInferenceOptions
                {
                    MaxProfileCharacters = 4,
                    ColumnOverrides = [Override(0, CsvColumnLogicalType.SignedInteger)],
                });
            CsvColumnSchema column = Assert.Single(result.Columns);

            Assert.Equal(0, result.RecordsExamined);
            Assert.Equal(MigrationCoverageKind.Sample, result.Coverage.Kind);
            Assert.Equal(CsvOverrideValidationStatus.NotProfiled, column.OverrideValidation);
        }
    }

    [Fact]
    public async Task UtcOffsetBoundaryInferenceIsIndependentOfTheLocalTimezone()
    {
        const string maximumUtc = "9999-12-31T23:59:59Z";
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync(
            $"value\n{maximumUtc}\n{maximumUtc}\n");
        await using (snapshot)
        {
            CsvSchemaInferenceResult result = await InferAsync(binding, snapshot, 100);
            CsvColumnSchema column = Assert.Single(result.Columns);

            Assert.Equal(CsvColumnLogicalType.DateTimeOffset, column.LogicalType);
            Assert.True(result.TryNormalizeScalar(0, maximumUtc, out string? canonical));
            Assert.Equal("9999-12-31 23:59:59+00:00", canonical);
        }
    }

    [Theory]
    [InlineData("12:34:56.")]
    [InlineData("2024-01-02T12:34:56.")]
    [InlineData("2024-01-02T12:34:56.Z")]
    public async Task DanglingTemporalFractionSeparatorsRemainText(string value)
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync(
            $"value\n{value}\n{value}\n");
        await using (snapshot)
        {
            CsvColumnSchema column = Assert.Single((await InferAsync(binding, snapshot, 100)).Columns);

            Assert.Equal(CsvColumnLogicalType.Text, column.LogicalType);
        }
    }

    [Fact]
    public async Task ExplicitFloatingPointRejectsNonzeroUnderflow()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync(
            "value\n1e-4000\n2e-4000\n");
        await using (snapshot)
        {
            CsvSchemaInferenceResult result = await InferAsync(
                binding,
                snapshot,
                100,
                new CsvSchemaInferenceOptions
                {
                    ColumnOverrides = [Override(0, CsvColumnLogicalType.FloatingPoint)],
                });
            CsvColumnSchema column = Assert.Single(result.Columns);

            Assert.Equal(CsvOverrideValidationStatus.Incompatible, column.OverrideValidation);
            Assert.False(result.TryNormalizeScalar(0, "1e-4000", out _));
            Assert.Contains(result.Diagnostics, item => item.RuleId == "MIG-CSV-SCHEMA-OVERRIDE-001");
        }
    }

    [Fact]
    public async Task IntegerOverflowWidensWithoutWrapping()
    {
        const string aboveUInt64 = "18446744073709551616";
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync(
            $"value\n{aboveUInt64}\n{aboveUInt64}\n");
        await using (snapshot)
        {
            CsvColumnSchema column = Assert.Single((await InferAsync(binding, snapshot, 100)).Columns);

            Assert.Equal(CsvColumnLogicalType.Decimal, column.LogicalType);
            Assert.Equal(20, column.ObservedPrecision);
            Assert.Equal(0, column.ObservedScale);
        }
    }

    [Fact]
    public async Task SchemaDiagnosticIdsRetainSixteenHexadecimalDigestCharacters()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync("id\n001\n002\n");
        await using (snapshot)
        {
            MigrationDiagnostic diagnostic = Assert.Single(
                (await InferAsync(binding, snapshot, 100)).Diagnostics,
                item => item.RuleId == "MIG-CSV-SCHEMA-TEXT-001");

            Assert.Matches("^diag:mig-csv-schema-text-001:[0-9a-f]{16}$", diagnostic.DiagnosticId);
        }
    }

    [Fact]
    public async Task OverridesChangeTheCatalogDigestButNeverTheBoundSourceIdentity()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync("id\n1\n2\n");
        await using (snapshot)
        {
            CsvSchemaInferenceResult inferred = await InferAsync(binding, snapshot, 100);
            CsvSchemaInferenceResult overridden = await InferAsync(
                binding,
                snapshot,
                100,
                new CsvSchemaInferenceOptions
                {
                    ColumnOverrides = [Override(0, CsvColumnLogicalType.Text)],
                });
            MigrationCatalog inferredCatalog = inferred.CreateCatalog(CurrentVersion);
            MigrationCatalog overriddenCatalog = overridden.CreateCatalog(CurrentVersion);

            Assert.Equal(inferredCatalog.Source.Fingerprint, overriddenCatalog.Source.Fingerprint);
            Assert.NotEqual(
                MigrationArtifactSerializer.ComputeCatalogDigest(inferredCatalog),
                MigrationArtifactSerializer.ComputeCatalogDigest(overriddenCatalog));
        }
    }

    [Fact]
    public async Task InspectorCanDiscoverStructureWithoutClaimingProfileEvidence()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync("id,name\n1,alpha\n2,beta\n");
        await using (snapshot)
        {
            var inspector = new CsvMigrationSourceInspector(binding, snapshot);
            MigrationCatalog catalog = await inspector.InspectAsync(
                new MigrationInspectionRequest
                {
                    TargetCSharpDbVersion = CurrentVersion,
                    IncludeProfile = false,
                },
                TestContext.Current.CancellationToken);

            Assert.Equal(MigrationSourceKind.Csv, inspector.SourceKind);
            Assert.All(
                catalog.Objects.Where(item => item.Kind == MigrationObjectKind.Column),
                column =>
                {
                    Assert.Equal("text", Facet(column, "logicalType"));
                    Assert.Null(Facet(column, "profileKind"));
                });
        }
    }

    [Fact]
    public async Task StructureOnlyOverrideIsMarkedNotProfiledRatherThanCompatible()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync("id\nnot-an-integer\n");
        await using (snapshot)
        {
            var inspector = new CsvMigrationSourceInspector(
                binding,
                snapshot,
                new CsvSchemaInferenceOptions
                {
                    ColumnOverrides = [Override(0, CsvColumnLogicalType.SignedInteger)],
                });
            MigrationCatalog catalog = await inspector.InspectAsync(
                new MigrationInspectionRequest
                {
                    TargetCSharpDbVersion = CurrentVersion,
                    IncludeProfile = false,
                },
                TestContext.Current.CancellationToken);

            MigrationCatalogObject column = Column(catalog, 0);
            Assert.Equal("NotProfiled", Facet(column, "csvOverrideValidation"));
            Assert.Null(Facet(column, "profileKind"));
            Assert.DoesNotContain(
                catalog.Diagnostics,
                item => item.RuleId == "MIG-CSV-SCHEMA-OVERRIDE-001");
        }
    }

    [Fact]
    public async Task InvalidOverrideLogicalTypeIsRejectedBeforeSourceReading()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync("id\n1\n2\n");
        await using (snapshot)
        {
            var options = new CsvSchemaInferenceOptions
            {
                ColumnOverrides =
                [
                    Override(0, (CsvColumnLogicalType)int.MaxValue),
                ],
            };

            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                async () => await InferAsync(binding, snapshot, 10, options));
        }
    }

    [Fact]
    public async Task RejectsInferencePoliciesAboveAbsoluteSafetyCeilings()
    {
        (CsvSourceSnapshot snapshot, CsvSourceBinding binding) = await BindAsync("id\n1\n2\n");
        await using (snapshot)
        {
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                async () => await InferAsync(
                    binding,
                    snapshot,
                    CsvSchemaInferer.MaximumSupportedDataRecords + 1));

            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                async () => await InferAsync(
                    binding,
                    snapshot,
                    10,
                    new CsvSchemaInferenceOptions
                    {
                        MaxProfileCharacters =
                            CsvSchemaInferenceOptions.MaximumSupportedProfileCharacters + 1,
                    }));

            CsvColumnSchemaOverride[] excessiveOverrides = Enumerable
                .Range(0, CsvSchemaInferenceOptions.MaximumSupportedColumnOverrides + 1)
                .Select(index => Override(index, CsvColumnLogicalType.Text))
                .ToArray();
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                async () => await InferAsync(
                    binding,
                    snapshot,
                    10,
                    new CsvSchemaInferenceOptions { ColumnOverrides = excessiveOverrides }));
        }
    }

    private const string CurrentVersion = "4.3.0";

    private static CsvColumnSchemaOverride Override(
        int index,
        CsvColumnLogicalType type,
        string? expectedHeader = null) => new()
        {
            ColumnIndex = index,
            LogicalType = type,
            ExpectedHeader = expectedHeader,
        };

    private static async ValueTask<CsvSchemaInferenceResult> InferAsync(
        CsvSourceBinding binding,
        CsvSourceSnapshot snapshot,
        int maxDataRecords,
        CsvSchemaInferenceOptions? options = null) =>
        await CsvSchemaInferer.InferAsync(
            binding,
            snapshot,
            maxDataRecords,
            options,
            TestContext.Current.CancellationToken);

    private static async ValueTask<(CsvSourceSnapshot Snapshot, CsvSourceBinding Binding)> BindAsync(
        string csv,
        CsvReaderOptions? readerOptions = null)
    {
        readerOptions ??= new CsvReaderOptions();
        CsvSourceSnapshot snapshot = await CsvSourceSnapshot.CreateAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(csv)),
            cancellationToken: TestContext.Current.CancellationToken);
        try
        {
            CsvFormatInspection inspection = await CsvFormatInspector.InspectAsync(
                snapshot,
                readerOptions,
                new CsvInspectionOptions { DelimiterCandidates = [readerOptions.Delimiter] },
                TestContext.Current.CancellationToken);
            CsvSourceBinding binding = await CsvSourceBinding.CreateAsync(
                snapshot,
                inspection,
                cancellationToken: TestContext.Current.CancellationToken);
            return (snapshot, binding);
        }
        catch
        {
            await snapshot.DisposeAsync();
            throw;
        }
    }

    private static MigrationCatalogObject Column(MigrationCatalog catalog, int index) =>
        catalog.Objects.Single(item => item.ObjectId == $"csv:column:{index}");

    private static string? Facet(MigrationCatalogObject item, string name) =>
        item.Facets.SingleOrDefault(facet => facet.Name == name)?.Value;

    private static MigrationTypeMapping Mapping(MigrationPlan plan, string objectId) =>
        Assert.Single(plan.Objects.Single(item => item.SourceObjectId == objectId).TypeMappings);
}
