using System.Text;
using System.Text.Json;
using CSharpDB.Engine;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Migration.Validation;

namespace CSharpDB.Migration.Tests;

public sealed class CSharpDbJsonDocumentCollectionMigrationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FullApplyResumeChecksumAndActivationUseARealCollection(
        bool multipleValues)
    {
        const string rootArray =
            """
            [
              {"é":"<","n":-0,"e":1e+02,"t":1.2300,"nested":[{"b":2,"a":1}]},
              [1,{"x":"é"}],
              "text<é\n",
              123456789012345678901234567890,
              true,
              null
            ]
            """;
        const string ndjson =
            """
            {"é":"<","n":-0,"e":1e+02,"t":1.2300,"nested":[{"b":2,"a":1}]}
            [1,{"x":"é"}]
            "text<é\n"
            123456789012345678901234567890
            true
            null
            """;
        string[] expectedDocuments =
        [
            """{"é":"<","n":-0,"e":1e+02,"t":1.2300,"nested":[{"b":2,"a":1}]}""",
            """[1,{"x":"é"}]""",
            "\"text<é\\n\"",
            "123456789012345678901234567890",
            "true",
            "null",
        ];

        using var files = new TemporaryCollectionDirectory();
        await using JsonSourceSnapshot snapshot =
            await JsonSourceSnapshot.CreateAsync(
                new MemoryStream(
                    Encoding.UTF8.GetBytes(
                        multipleValues ? ndjson : rootArray)),
                cancellationToken: Ct);
        JsonSourceBinding binding = await JsonSourceBinding.CreateAsync(
            snapshot,
            new JsonStreamingReaderOptions
            {
                Framing = multipleValues
                    ? JsonInputFraming.MultipleValues
                    : JsonInputFraming.RootArray,
            },
            cancellationToken: Ct);
        JsonDocumentCollectionProjectionResult projection =
            await JsonDocumentCollectionProjector.ProjectAsync(
                binding,
                snapshot,
                new JsonDocumentCollectionProjectionOptions
                {
                    CollectionName = "documents",
                },
                Ct);
        MigrationCatalog catalog = projection.CreateCatalog(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);
        MigrationPlan plan = new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions
            {
                AcceptAllExclusions = true,
                Load = new MigrationLoadPolicy
                {
                    BatchSize = 2,
                    MaxBatchBytes = 1024 * 1024,
                    MaxValueBytes = 512 * 1024,
                },
            });
        Assert.True(plan.Objects.Single(item =>
            item.SourceObjectId == JsonDocumentCollectionObjectIds.Collection).Included);

        await using JsonDocumentCollectionDataSource source =
            await JsonDocumentCollectionDataSource.CreateAsync(
                projection,
                snapshot,
                catalog,
                Ct);
        await using (CSharpDbStagedMigrationTarget target =
                     await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            MigrationApplyResult applied = await new MigrationApplyRunner()
                .ApplyAsync(
                    new MigrationApplyRequest
                    {
                        Plan = plan,
                        Catalog = catalog,
                        Source = source,
                        Target = target,
                    },
                    Ct);
            Assert.Equal(MigrationApplyStatus.AwaitingValidation, applied.Status);
            Assert.Equal(3, applied.BatchesWritten);
            Assert.Equal(expectedDocuments.Length, applied.RowsWritten);

            MigrationApplyResult replayed = await new MigrationApplyRunner()
                .ApplyAsync(
                    new MigrationApplyRequest
                    {
                        Plan = plan,
                        Catalog = catalog,
                        Source = source,
                        Target = target,
                    },
                    Ct);
            Assert.Equal(0, replayed.BatchesWritten);
            Assert.Equal(3, replayed.BatchesSkipped);
            Assert.Equal(expectedDocuments.Length, replayed.RowsSkipped);

            await using var sourceValidation =
                new MigrationDataSourceValidationSnapshot(
                    plan,
                    catalog,
                    source);
            MigrationValidationRunResult validation =
                await new MigrationValidationRunner().ValidateAsync(
                    new MigrationValidationRunRequest
                    {
                        Plan = plan,
                        Catalog = catalog,
                        SourceSnapshot = sourceValidation,
                        Target = target,
                        Level = MigrationValidationLevel.Checksum,
                        ReportOutputPath = files.ReportPath,
                        ChecksumOptions =
                            new PartitionedChecksumValidatorOptions
                            {
                                SpillRootDirectory = files.DirectoryPath,
                                SortMemoryBudgetBytes = 1024 * 1024,
                                MaxSpillBytes = 16 * 1024 * 1024,
                            },
                    },
                    Ct);
            Assert.Equal(
                MigrationValidationStatus.Passed,
                validation.Report.Outcome);
            Assert.True(validation.Activated);
        }

        await using Database database =
            await Database.OpenAsync(files.TargetPath, Ct);
        Assert.Contains("documents", database.GetCollectionNames());
        Collection<JsonElement> collection =
            await database.GetCollectionAsync<JsonElement>(
                "documents",
                Ct);
        Assert.Equal(expectedDocuments.Length, await collection.CountAsync(Ct));

        IReadOnlyDictionary<string, string> stored =
            await ReadStoredDocumentsAsync(database);
        Assert.Equal(expectedDocuments.Length, stored.Count);
        for (int index = 0; index < expectedDocuments.Length; index++)
        {
            string key =
                MigrationDocumentCollectionContract.FormatOrdinalKey(index);
            Assert.Equal(expectedDocuments[index], stored[key]);
            JsonElement? document = await collection.GetAsync(key, Ct);
            Assert.True(document.HasValue);
            Assert.Equal(expectedDocuments[index], document.Value.GetRawText());
        }
    }

    [Fact]
    public async Task ConfiguredNumberAboveOneMiBReachesTheCollectionUnchanged()
    {
        string number = "1" + new string('0', 1024 * 1024);
        using var files = new TemporaryCollectionDirectory();
        await using JsonSourceSnapshot snapshot =
            await JsonSourceSnapshot.CreateAsync(
                new MemoryStream(Encoding.UTF8.GetBytes($"[{number}]")),
                cancellationToken: Ct);
        JsonSourceBinding binding = await JsonSourceBinding.CreateAsync(
            snapshot,
            new JsonStreamingReaderOptions
            {
                MaxValueBytes = 2 * 1024 * 1024,
                MaxNumberBytes = 2 * 1024 * 1024,
            },
            cancellationToken: Ct);
        JsonDocumentCollectionProjectionResult projection =
            await JsonDocumentCollectionProjector.ProjectAsync(
                binding,
                snapshot,
                new JsonDocumentCollectionProjectionOptions
                {
                    CollectionName = "documents",
                },
                cancellationToken: Ct);
        MigrationCatalog catalog = projection.CreateCatalog(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);
        MigrationPlan plan = new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions
            {
                AcceptAllExclusions = true,
                Load = new MigrationLoadPolicy
                {
                    MaxBatchBytes = 4L * 1024 * 1024,
                    MaxValueBytes = 2 * 1024 * 1024,
                },
            });
        await using JsonDocumentCollectionDataSource source =
            await JsonDocumentCollectionDataSource.CreateAsync(
                projection,
                snapshot,
                catalog,
                Ct);
        await using (CSharpDbStagedMigrationTarget target =
                     await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            MigrationApplyResult result = await new MigrationApplyRunner()
                .ApplyAsync(
                    new MigrationApplyRequest
                    {
                        Plan = plan,
                        Catalog = catalog,
                        Source = source,
                        Target = target,
                    },
                    Ct);
            Assert.Equal(1, result.RowsWritten);
        }

        await using Database database =
            await Database.OpenAsync(files.TargetPath, Ct);
        IReadOnlyDictionary<string, string> stored =
            await ReadStoredDocumentsAsync(database);
        Assert.Equal(
            number,
            stored[MigrationDocumentCollectionContract.FormatOrdinalKey(0)]);
    }

    [Fact]
    public async Task CollectionIndexAddedAfterApplyIsUnexpectedSchema()
    {
        using var files = new TemporaryCollectionDirectory();
        await using JsonSourceSnapshot snapshot =
            await JsonSourceSnapshot.CreateAsync(
                new MemoryStream(Encoding.UTF8.GetBytes("""[{"id":1}]""")),
                cancellationToken: Ct);
        JsonSourceBinding binding = await JsonSourceBinding.CreateAsync(
            snapshot,
            cancellationToken: Ct);
        JsonDocumentCollectionProjectionResult projection =
            await JsonDocumentCollectionProjector.ProjectAsync(
                binding,
                snapshot,
                cancellationToken: Ct);
        MigrationCatalog catalog = projection.CreateCatalog(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);
        MigrationPlan plan = new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions { AcceptAllExclusions = true });
        await using JsonDocumentCollectionDataSource source =
            await JsonDocumentCollectionDataSource.CreateAsync(
                projection,
                snapshot,
                catalog,
                Ct);

        await using (CSharpDbStagedMigrationTarget target =
                     await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            _ = await new MigrationApplyRunner().ApplyAsync(
                new MigrationApplyRequest
                {
                    Plan = plan,
                    Catalog = catalog,
                    Source = source,
                    Target = target,
                },
                Ct);
        }

        await using (Database database =
                     await Database.OpenAsync(files.TargetPath, Ct))
        {
            Collection<JsonElement> collection =
                await database.GetCollectionAsync<JsonElement>(
                    "json_documents",
                    Ct);
            await collection.EnsureIndexAsync("$.id", Ct);
        }

        await using CSharpDbStagedMigrationTarget reopened =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                files.TargetPath,
                plan,
                catalog,
                source.SnapshotIdentity,
                cancellationToken: Ct);
        await using IValidationSnapshot validation =
            await reopened.OpenValidationSnapshotAsync(Ct);
        var evidence =
            Assert.IsAssignableFrom<IMigrationEvidenceValidationSnapshot>(
                validation);
        MigrationNormalizedSchema actual =
            await evidence.ReadSchemaAsync(Ct);
        MigrationNormalizedSchema expected =
            MigrationNormalizedSchemaContract.CreateExpected(plan, catalog);

        MigrationNormalizedSchemaDifference difference = Assert.Single(
            MigrationNormalizedSchemaContract.Compare(expected, actual),
            item =>
                item.SourceDefinitionDigest is null &&
                item.TargetDefinitionDigest is not null);
        MigrationNormalizedSchemaObject unexpected = Assert.Single(
            actual.Objects,
            item => item.ObjectId == difference.ObjectId);
        Assert.Equal(MigrationObjectKind.Index, unexpected.Kind);
        Assert.Equal(
            JsonDocumentCollectionObjectIds.Collection,
            unexpected.ParentObjectId);
    }

    [Theory]
    [InlineData(CSharpDbMigrationFaultPoint.BeforeRows, false)]
    [InlineData(CSharpDbMigrationFaultPoint.AfterRowsBeforeReceipt, false)]
    [InlineData(CSharpDbMigrationFaultPoint.AfterRejectsBeforeReceipt, false)]
    [InlineData(CSharpDbMigrationFaultPoint.AfterReceiptBeforeCommit, false)]
    [InlineData(CSharpDbMigrationFaultPoint.AfterCommit, true)]
    public async Task EveryBatchFaultCutoffResumesWithoutMissingOrDuplicateDocuments(
        CSharpDbMigrationFaultPoint faultPoint,
        bool committedBeforeFault)
    {
        using var files = new TemporaryCollectionDirectory();
        await using JsonSourceSnapshot snapshot =
            await JsonSourceSnapshot.CreateAsync(
                new MemoryStream(Encoding.UTF8.GetBytes("""[{"id":1},{"id":2},{"id":3}]""")),
                cancellationToken: Ct);
        JsonSourceBinding binding = await JsonSourceBinding.CreateAsync(
            snapshot,
            cancellationToken: Ct);
        JsonDocumentCollectionProjectionResult projection =
            await JsonDocumentCollectionProjector.ProjectAsync(
                binding,
                snapshot,
                new JsonDocumentCollectionProjectionOptions
                {
                    CollectionName = "documents",
                },
                Ct);
        MigrationCatalog catalog = projection.CreateCatalog(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);
        MigrationPlan plan = new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions
            {
                AcceptAllExclusions = true,
                Load = new MigrationLoadPolicy { BatchSize = 3 },
            });
        await using JsonDocumentCollectionDataSource source =
            await JsonDocumentCollectionDataSource.CreateAsync(
                projection,
                snapshot,
                catalog,
                Ct);

        var fault = new ThrowOnceFaultInjector(faultPoint);
        await using (CSharpDbStagedMigrationTarget target =
                     await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         fault,
                         Ct))
        {
            await Assert.ThrowsAsync<InjectedCollectionFaultException>(
                async () => await new MigrationApplyRunner().ApplyAsync(
                    new MigrationApplyRequest
                    {
                        Plan = plan,
                        Catalog = catalog,
                        Source = source,
                        Target = target,
                    },
                    Ct));
        }
        Assert.True(fault.Fired);

        await using (Database database =
                     await Database.OpenAsync(files.TargetPath, Ct))
        {
            Collection<JsonElement> collection =
                await database.GetCollectionAsync<JsonElement>(
                    "documents",
                    Ct);
            Assert.Equal(
                committedBeforeFault ? 3 : 0,
                await collection.CountAsync(Ct));
        }

        await using (CSharpDbStagedMigrationTarget resumed =
                     await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            MigrationApplyResult result = await new MigrationApplyRunner()
                .ApplyAsync(
                    new MigrationApplyRequest
                    {
                        Plan = plan,
                        Catalog = catalog,
                        Source = source,
                        Target = resumed,
                    },
                    Ct);
            Assert.Equal(committedBeforeFault ? 0 : 1, result.BatchesWritten);
            Assert.Equal(committedBeforeFault ? 1 : 0, result.BatchesSkipped);
            Assert.Equal(committedBeforeFault ? 0 : 3, result.RowsWritten);
            Assert.Equal(committedBeforeFault ? 3 : 0, result.RowsSkipped);
        }

        await using (Database database =
                     await Database.OpenAsync(files.TargetPath, Ct))
        {
            Collection<JsonElement> collection =
                await database.GetCollectionAsync<JsonElement>(
                    "documents",
                    Ct);
            Assert.Equal(3, await collection.CountAsync(Ct));
        }
    }

    private static async ValueTask<IReadOnlyDictionary<string, string>>
        ReadStoredDocumentsAsync(Database database)
    {
        await using var result = await database.ExecuteAsync(
            "SELECT \"_key\", \"_doc\" FROM \"_col_documents\"",
            Ct);
        var documents = new Dictionary<string, string>(StringComparer.Ordinal);
        while (await result.MoveNextAsync(Ct))
        {
            Assert.Equal(2, result.Current.Length);
            documents.Add(
                result.Current[0].AsText,
                result.Current[1].AsText);
        }
        return documents;
    }

    private sealed class ThrowOnceFaultInjector(
        CSharpDbMigrationFaultPoint selected) :
        ICSharpDbMigrationFaultInjector
    {
        public bool Fired { get; private set; }

        public ValueTask InjectAsync(
            CSharpDbMigrationFaultPoint point,
            MigrationTargetBatch batch,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!Fired && point == selected)
            {
                Fired = true;
                throw new InjectedCollectionFaultException();
            }
            return ValueTask.CompletedTask;
        }
    }

    private sealed class InjectedCollectionFaultException : Exception;

    private sealed class TemporaryCollectionDirectory : IDisposable
    {
        internal TemporaryCollectionDirectory()
        {
            DirectoryPath = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-json-collection-{Guid.NewGuid():N}");
            Directory.CreateDirectory(DirectoryPath);
            TargetPath = Path.Combine(DirectoryPath, "target.csdb");
            ReportPath = Path.Combine(DirectoryPath, "validation.json");
        }

        internal string DirectoryPath { get; }

        internal string TargetPath { get; }

        internal string ReportPath { get; }

        public void Dispose()
        {
            try
            {
                Directory.Delete(DirectoryPath, recursive: true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }
}
