using System.IO.Pipes;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Migration;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Migration.Files.Csv;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Primitives;

return await MigrationCrashHarness.RunAsync(args);

internal static class MigrationCrashHarness
{
    private const string FailFastScenario = "fail-fast";
    private const string AcceptedOnlyScenario = "accepted-only";
    private const string MixedScenario = "mixed";
    private const string AllRejectScenario = "all-reject";
    private const string DeterministicRuleId = "MIG-CSV-ROW-001";
    private const string RejectSourceObjectId = "syn:table:customers-lower";
    private const string RejectColumnObjectId = "syn:column:customers-lower:code-lower";

    public static async Task<int> RunAsync(string[] args)
    {
        string? jsonPackageMode =
            OptionalOption(args, "--json-package-mode");
        if (jsonPackageMode is not null)
        {
            return await RunJsonPackageAsync(args, jsonPackageMode)
                .ConfigureAwait(false);
        }
        if (OptionalOption(args, "--csv-checkpoint-destination") is not null)
            return await RunCsvCheckpointAsync(args).ConfigureAwait(false);
        if (OptionalOption(args, "--csv-publication-destination") is not null)
            return await RunCsvPublicationAsync(args).ConfigureAwait(false);

        string targetPath = Path.GetFullPath(RequiredOption(args, "--target"));
        string pipeName = RequiredOption(args, "--pipe");
        string scenario = OptionalOption(args, "--scenario") ?? FailFastScenario;
        string? artifactOutputOption = OptionalOption(args, "--artifact-output");
        string? artifactFaultName = OptionalOption(args, "--artifact-fault");
        CSharpDbMigrationFaultPoint migrationFaultPoint = default;
        MigrationRejectArtifactFaultPoint artifactFaultPoint = default;
        if (artifactOutputOption is null)
        {
            string faultName = RequiredOption(args, "--fault");
            if (!Enum.TryParse(
                    faultName,
                    ignoreCase: false,
                    out migrationFaultPoint))
            {
                throw new ArgumentException(
                    $"Unknown migration fault point '{faultName}'.",
                    nameof(args));
            }
        }
        else
        {
            if (artifactFaultName is null)
            {
                throw new ArgumentException(
                    "Missing required option '--artifact-fault'.",
                    nameof(args));
            }
            if (!Enum.TryParse(
                    artifactFaultName,
                    ignoreCase: false,
                    out artifactFaultPoint))
            {
                throw new ArgumentException(
                    $"Unknown reject-artifact fault point '{artifactFaultName}'.",
                    nameof(args));
            }
        }

        using var pipe = new NamedPipeClientStream(
            ".",
            pipeName,
            PipeDirection.InOut,
            PipeOptions.Asynchronous);
        using var connectTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await pipe.ConnectAsync(connectTimeout.Token).ConfigureAwait(false);
        using var reader = new StreamReader(
            pipe,
            new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
            detectEncodingFromByteOrderMarks: false,
            bufferSize: 1024,
            leaveOpen: true);
        using var writer = new StreamWriter(
            pipe,
            new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
            bufferSize: 1024,
            leaveOpen: true)
        {
            AutoFlush = true,
        };

        await writer.WriteLineAsync("READY").ConfigureAwait(false);
        try
        {
            MigrationCatalog catalog = await InspectAsync().ConfigureAwait(false);
            if (artifactOutputOption is not null)
            {
                await RunRejectArtifactAsync(
                    targetPath,
                    Path.GetFullPath(artifactOutputOption),
                    catalog,
                    new CoordinatedRejectArtifactFaultInjector(
                        artifactFaultPoint,
                        reader,
                        writer)).ConfigureAwait(false);
            }
            else
            {
                var injector = new CoordinatedCrashFaultInjector(
                    migrationFaultPoint,
                    reader,
                    writer);
                if (string.Equals(scenario, FailFastScenario, StringComparison.Ordinal))
                {
                    await RunFailFastAsync(targetPath, catalog, injector).ConfigureAwait(false);
                }
                else
                {
                    await RunDeterministicRejectAsync(
                        targetPath,
                        catalog,
                        scenario,
                        injector).ConfigureAwait(false);
                }
            }

            await writer.WriteLineAsync("COMPLETED_WITHOUT_FAULT").ConfigureAwait(false);
            return 3;
        }
        catch (Exception error)
        {
            try
            {
                string encodedMessage = Convert.ToBase64String(
                    Encoding.UTF8.GetBytes(error.Message));
                await writer.WriteLineAsync(
                    $"ERROR|{error.GetType().FullName}|{encodedMessage}").ConfigureAwait(false);
            }
            catch
            {
            }

            return 2;
        }
    }

    private static async Task<int> RunJsonPackageAsync(
        string[] args,
        string mode)
    {
        string packagePath = Path.GetFullPath(
            RequiredOption(args, "--json-package"));
        string workspacePath = Path.GetFullPath(
            RequiredOption(args, "--json-workspace"));
        string resultPath = Path.GetFullPath(
            RequiredOption(args, "--json-result"));

        bool typed = string.Equals(
            OptionalOption(
                args,
                "--json-package-kind"),
            "typed",
            StringComparison.Ordinal);
        JsonPackageProcessResult result = typed
            ? mode switch
            {
                "write" =>
                    await WriteJsonTypedPackageAsync(
                            args,
                            packagePath,
                            workspacePath)
                        .ConfigureAwait(false),
                "read" =>
                    await ReadJsonTypedPackageAsync(
                            args,
                            packagePath,
                            workspacePath,
                            resumeCursor: null,
                            mode)
                        .ConfigureAwait(false),
                "resume" =>
                    await ReadJsonTypedPackageAsync(
                            args,
                            packagePath,
                            workspacePath,
                            RequiredOption(
                                args,
                                "--json-resume-cursor"),
                            mode)
                        .ConfigureAwait(false),
                _ => throw new ArgumentException(
                    $"Unknown typed JSON package mode '{mode}'.",
                    nameof(args)),
            }
            : mode switch
        {
            "write" => await WriteJsonPackageAsync(
                    args,
                    packagePath,
                    workspacePath)
                .ConfigureAwait(false),
            "read" => await ReadJsonPackageAsync(
                    args,
                    packagePath,
                    workspacePath,
                    resumeCursor: null,
                    mode)
                .ConfigureAwait(false),
            "resume" => await ReadJsonPackageAsync(
                    args,
                    packagePath,
                    workspacePath,
                    RequiredOption(args, "--json-resume-cursor"),
                    mode)
                .ConfigureAwait(false),
            _ => throw new ArgumentException(
                $"Unknown JSON package mode '{mode}'.",
                nameof(args)),
        };

        await WriteJsonPackageResultAsync(resultPath, result)
            .ConfigureAwait(false);
        return 0;
    }

    private static async Task<JsonPackageProcessResult>
        WriteJsonPackageAsync(
            IReadOnlyList<string> args,
            string packagePath,
            string workspacePath)
    {
        string sourcePath = Path.GetFullPath(
            RequiredOption(args, "--json-source"));
        string framingName =
            RequiredOption(args, "--json-framing");
        if (!Enum.TryParse(
                framingName,
                ignoreCase: false,
                out JsonInputFraming framing) ||
            !Enum.IsDefined(framing))
        {
            throw new ArgumentException(
                $"Unknown JSON framing '{framingName}'.",
                nameof(args));
        }

        await using JsonSourceSnapshot snapshot =
            await JsonSourceSnapshot.CreateFromFileAsync(
                    sourcePath,
                    new JsonSourceSnapshotOptions
                    {
                        WorkspacePath = workspacePath,
                        MaxSourceBytes = 1024 * 1024,
                        CopyBufferBytes = 32 * 1024,
                    })
                .ConfigureAwait(false);
        JsonSourceBinding binding =
            await JsonSourceBinding.CreateAsync(
                    snapshot,
                    CreateJsonReaderOptions(framing),
                    logicalSourceIdentity:
                        "json-package-process-fixture")
                .ConfigureAwait(false);
        JsonTableSchemaInferenceResult schema =
            await JsonTableSchemaInferer.InferAsync(
                    binding,
                    snapshot,
                    maxProfileRecords: 4,
                    new JsonTableSchemaInferenceOptions
                    {
                        TableName = "json_process_rows",
                        MaxColumns = 32,
                        MaxTotalColumnNameBytes = 32 * 1024,
                        MaxProfileBytes = 256 * 1024,
                    })
                .ConfigureAwait(false);
        string targetVersion =
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion;
        MigrationCatalog catalog =
            schema.CreateCatalog(targetVersion);
        JsonSnapshotPackageManifest manifest =
            await JsonSnapshotPackage.WriteAsync(
                    packagePath,
                    snapshot,
                    schema,
                    targetVersion)
                .ConfigureAwait(false);

        await using JsonMigrationDataSource dataSource =
            await JsonMigrationDataSource.CreateAsync(
                    schema,
                    snapshot,
                    catalog)
                .ConfigureAwait(false);
        JsonReadOutcome firstBatch = await ReadJsonRowsAsync(
                dataSource,
                schema,
                resumeCursor: null,
                stopAfterFirstBatch: true)
            .ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(firstBatch.NextCursor))
        {
            throw new InvalidDataException(
                "The first JSON package batch did not produce a resume cursor.");
        }

        return new JsonPackageProcessResult
        {
            Mode = "write",
            ManifestDigest = manifest.ManifestDigest,
            CatalogDigest = manifest.CatalogDigest,
            SnapshotIdentity = manifest.SnapshotIdentity,
            FirstBatchCursor = firstBatch.NextCursor,
            AcceptedRowCount = firstBatch.RowDigests.Count,
            RejectedRowCount = firstBatch.RejectedRowCount,
            RowDigests = firstBatch.RowDigests,
        };
    }

    private static async Task<JsonPackageProcessResult>
        WriteJsonTypedPackageAsync(
            IReadOnlyList<string> args,
            string packagePath,
            string workspacePath)
    {
        string sourcePath = Path.GetFullPath(
            RequiredOption(args, "--json-source"));
        string intentPath = Path.GetFullPath(
            RequiredOption(args, "--json-intent"));
        string framingName =
            RequiredOption(args, "--json-framing");
        if (!Enum.TryParse(
                framingName,
                ignoreCase: false,
                out JsonInputFraming framing) ||
            !Enum.IsDefined(framing))
        {
            throw new ArgumentException(
                $"Unknown JSON framing '{framingName}'.",
                nameof(args));
        }

        await using JsonSourceSnapshot snapshot =
            await JsonSourceSnapshot.CreateFromFileAsync(
                    sourcePath,
                    new JsonSourceSnapshotOptions
                    {
                        WorkspacePath = workspacePath,
                        MaxSourceBytes = 1024 * 1024,
                        CopyBufferBytes = 32 * 1024,
                    })
                .ConfigureAwait(false);
        JsonSourceBinding binding =
            await JsonSourceBinding.CreateAsync(
                    snapshot,
                    CreateJsonReaderOptions(framing),
                    logicalSourceIdentity:
                        "json-typed-package-process-fixture")
                .ConfigureAwait(false);
        JsonTypedIntentManifest intent =
            await JsonTypedIntentSidecar.WriteAsync(
                    intentPath,
                    binding,
                    new JsonTypedIntentOptions
                    {
                        Columns =
                        [
                            new JsonTypedColumnIntent
                            {
                                ColumnIndex = 0,
                                ExpectedPropertyName = "id",
                                Codec =
                                    JsonTypedValueCodec
                                        .Int64String,
                                Nullable = false,
                            },
                            new JsonTypedColumnIntent
                            {
                                ColumnIndex = 1,
                                ExpectedPropertyName =
                                    "amount",
                                Codec =
                                    JsonTypedValueCodec
                                        .DecimalString,
                                Nullable = false,
                                Precision = 38,
                                Scale = 18,
                            },
                            new JsonTypedColumnIntent
                            {
                                ColumnIndex = 2,
                                ExpectedPropertyName =
                                    "binary",
                                Codec =
                                    JsonTypedValueCodec
                                        .BinaryBase64,
                                Nullable = false,
                            },
                        ],
                        MaxDecodedBinaryBytes =
                            1024 * 1024,
                        MaxDecimalDigits = 1024,
                    })
                .ConfigureAwait(false);
        JsonTypedTableSchemaInferenceResult schema =
            await JsonTypedTableSchemaInferer.InferAsync(
                    binding,
                    snapshot,
                    intent,
                    maxProfileRecords: 4,
                    new JsonTableSchemaInferenceOptions
                    {
                        TableName =
                            "json_typed_process_rows",
                        MaxColumns = 32,
                        MaxTotalColumnNameBytes =
                            32 * 1024,
                        MaxProfileBytes = 256 * 1024,
                        ColumnOverrides =
                        [
                            new JsonTableColumnSchemaOverride
                            {
                                ColumnIndex = 3,
                                ExpectedPropertyName =
                                    "name",
                                LogicalType =
                                    JsonTableColumnLogicalType
                                        .Text,
                                Nullable = false,
                            },
                        ],
                    })
                .ConfigureAwait(false);
        string targetVersion =
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion;
        MigrationCatalog catalog =
            schema.CreateCatalog(targetVersion);
        JsonTypedSnapshotPackageManifest manifest =
            await JsonTypedSnapshotPackage.WriteAsync(
                    packagePath,
                    snapshot,
                    schema,
                    targetVersion)
                .ConfigureAwait(false);
        await using JsonMigrationDataSource dataSource =
            await JsonMigrationDataSource.CreateAsync(
                    schema,
                    snapshot,
                    catalog)
                .ConfigureAwait(false);
        JsonReadOutcome firstBatch =
            await ReadJsonRowsAsync(
                    dataSource,
                    schema,
                    resumeCursor: null,
                    stopAfterFirstBatch: true)
                .ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(
                firstBatch.NextCursor))
        {
            throw new InvalidDataException(
                "The first typed JSON package batch did not produce a resume cursor.");
        }

        return new JsonPackageProcessResult
        {
            Mode = "write",
            ManifestDigest = manifest.ManifestDigest,
            CatalogDigest = manifest.CatalogDigest,
            SnapshotIdentity = manifest.SnapshotIdentity,
            FirstBatchCursor = firstBatch.NextCursor,
            AcceptedRowCount =
                firstBatch.RowDigests.Count,
            RejectedRowCount =
                firstBatch.RejectedRowCount,
            RowDigests = firstBatch.RowDigests,
        };
    }

    private static async Task<JsonPackageProcessResult>
        ReadJsonPackageAsync(
            IReadOnlyList<string> args,
            string packagePath,
            string workspacePath,
            string? resumeCursor,
            string mode)
    {
        string expectedManifestDigest =
            RequiredOption(
                args,
                "--json-expected-manifest-digest");
        await using JsonSnapshotPackageSession session =
            await JsonSnapshotPackage.OpenAsync(
                    packagePath,
                    new JsonSnapshotPackageOpenOptions
                    {
                        WorkspacePath = workspacePath,
                        MaxSourceBytes = 1024 * 1024,
                        CopyBufferBytes = 32 * 1024,
                        ExpectedManifestDigest =
                            expectedManifestDigest,
                    })
                .ConfigureAwait(false);
        JsonReadOutcome rows = await ReadJsonRowsAsync(
                session.DataSource,
                session.Schema,
                resumeCursor,
                stopAfterFirstBatch: false)
            .ConfigureAwait(false);

        return new JsonPackageProcessResult
        {
            Mode = mode,
            ManifestDigest =
                session.Manifest.ManifestDigest,
            CatalogDigest = session.Manifest.CatalogDigest,
            SnapshotIdentity =
                session.Manifest.SnapshotIdentity,
            FirstBatchCursor = null,
            AcceptedRowCount = rows.RowDigests.Count,
            RejectedRowCount = rows.RejectedRowCount,
            RowDigests = rows.RowDigests,
        };
    }

    private static async Task<JsonPackageProcessResult>
        ReadJsonTypedPackageAsync(
            IReadOnlyList<string> args,
            string packagePath,
            string workspacePath,
            string? resumeCursor,
            string mode)
    {
        string expectedManifestDigest =
            RequiredOption(
                args,
                "--json-expected-manifest-digest");
        await using JsonTypedSnapshotPackageSession session =
            await JsonTypedSnapshotPackage.OpenAsync(
                    packagePath,
                    new JsonSnapshotPackageOpenOptions
                    {
                        WorkspacePath = workspacePath,
                        MaxSourceBytes = 1024 * 1024,
                        CopyBufferBytes = 32 * 1024,
                        ExpectedManifestDigest =
                            expectedManifestDigest,
                    })
                .ConfigureAwait(false);
        JsonReadOutcome rows =
            await ReadJsonRowsAsync(
                    session.DataSource,
                    session.Schema,
                    resumeCursor,
                    stopAfterFirstBatch: false)
                .ConfigureAwait(false);

        return new JsonPackageProcessResult
        {
            Mode = mode,
            ManifestDigest =
                session.Manifest.ManifestDigest,
            CatalogDigest =
                session.Manifest.CatalogDigest,
            SnapshotIdentity =
                session.Manifest.SnapshotIdentity,
            FirstBatchCursor = null,
            AcceptedRowCount = rows.RowDigests.Count,
            RejectedRowCount = rows.RejectedRowCount,
            RowDigests = rows.RowDigests,
        };
    }

    private static JsonStreamingReaderOptions
        CreateJsonReaderOptions(JsonInputFraming framing) =>
        new()
        {
            Framing = framing,
            MaxValueBytes = 256 * 1024,
            MaxDepth = 32,
            MaxPropertiesPerObject = 128,
            MaxArrayElements = 256,
            MaxTotalNodes = 512,
            MaxPropertyNameBytes = 8 * 1024,
            MaxStringBytes = 128 * 1024,
            MaxNumberBytes = 8 * 1024,
            LeaveOpen = false,
        };

    private static async Task<JsonReadOutcome> ReadJsonRowsAsync(
        JsonMigrationDataSource dataSource,
        JsonTableSchemaInferenceResult schema,
        string? resumeCursor,
        bool stopAfterFirstBatch) =>
        await ReadJsonRowsAsync(
                dataSource,
                schema.Columns.Select(
                    column => column.ColumnIndex),
                resumeCursor,
                stopAfterFirstBatch)
            .ConfigureAwait(false);

    private static async Task<JsonReadOutcome> ReadJsonRowsAsync(
        JsonMigrationDataSource dataSource,
        JsonTypedTableSchemaInferenceResult schema,
        string? resumeCursor,
        bool stopAfterFirstBatch) =>
        await ReadJsonRowsAsync(
                dataSource,
                schema.Columns.Select(
                    column => column.ColumnIndex),
                resumeCursor,
                stopAfterFirstBatch)
            .ConfigureAwait(false);

    private static async Task<JsonReadOutcome> ReadJsonRowsAsync(
        JsonMigrationDataSource dataSource,
        IEnumerable<int> columnIndexes,
        string? resumeCursor,
        bool stopAfterFirstBatch)
    {
        string[] columnIds = columnIndexes
            .Order()
            .Select(columnIndex =>
                JsonMigrationObjectIds.Column(
                    columnIndex))
            .ToArray();
        var request = new MigrationReadRequest
        {
            SourceObjectId = JsonMigrationObjectIds.Table,
            ColumnObjectIds = columnIds,
            BatchSize = 3,
            MaxBatchBytes = 1024 * 1024,
            MaxValueBytes = 256 * 1024,
            SnapshotToken = dataSource.SnapshotIdentity,
            ResumeCursor = resumeCursor,
        };
        var rowDigests = new List<string>();
        int rejectedRows = 0;
        string? nextCursor = resumeCursor;
        await foreach (MigrationDataBatch batch in
                       dataSource.ReadAsync(request))
        {
            foreach (MigrationDataRow row in batch.Rows)
                rowDigests.Add(ComputeJsonRowDigest(row));
            rejectedRows = checked(
                rejectedRows + batch.RejectedRows.Count);
            nextCursor = batch.NextCursor;
            if (stopAfterFirstBatch)
                break;
        }

        return new JsonReadOutcome(
            rowDigests,
            rejectedRows,
            nextCursor);
    }

    private static string ComputeJsonRowDigest(
        MigrationDataRow row)
    {
        byte[] canonicalRow =
            JsonSerializer.SerializeToUtf8Bytes(
                row,
                JsonProcessJson.Options);
        try
        {
            return "sha256:" +
                Convert.ToHexString(
                        SHA256.HashData(canonicalRow))
                    .ToLowerInvariant();
        }
        finally
        {
            CryptographicOperations.ZeroMemory(canonicalRow);
        }
    }

    private static async Task WriteJsonPackageResultAsync(
        string resultPath,
        JsonPackageProcessResult result)
    {
        string? parentPath =
            Path.GetDirectoryName(resultPath);
        if (parentPath is null ||
            !Directory.Exists(parentPath))
        {
            throw new DirectoryNotFoundException(
                "The JSON package result parent directory does not exist.");
        }

        await using var output = new FileStream(
            resultPath,
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.None,
            bufferSize: 16 * 1024,
            FileOptions.Asynchronous |
            FileOptions.SequentialScan);
        await JsonSerializer.SerializeAsync(
                output,
                result,
                JsonProcessJson.Options)
            .ConfigureAwait(false);
        await output.FlushAsync().ConfigureAwait(false);
    }

    private static async Task<int> RunCsvCheckpointAsync(string[] args)
    {
        string destinationPath = Path.GetFullPath(
            RequiredOption(args, "--csv-checkpoint-destination"));
        string checkpointPath = Path.GetFullPath(
            RequiredOption(args, "--csv-next-checkpoint"));
        string appendPath = Path.GetFullPath(
            RequiredOption(args, "--csv-append-bytes"));
        string faultName = RequiredOption(args, "--csv-checkpoint-fault");
        if (!Enum.TryParse(
                faultName,
                ignoreCase: false,
                out CsvExportCheckpointFaultPoint faultPoint))
        {
            throw new ArgumentException(
                $"Unknown CSV checkpoint fault point '{faultName}'.",
                nameof(args));
        }

        return await RunCsvCoordinatedAsync(
                args,
                async (reader, writer) =>
                {
                    CsvExportCheckpoint checkpoint =
                        CsvExportCheckpointSerializer.Deserialize(
                            await File.ReadAllBytesAsync(checkpointPath)
                                .ConfigureAwait(false));
                    byte[] appendBytes = await File.ReadAllBytesAsync(appendPath)
                        .ConfigureAwait(false);
                    await using CsvExportPreparedOutputLease lease =
                        await CsvExportPreparedOutputLease
                            .OpenWithCheckpointFaultInjectorAsync(
                                destinationPath,
                                checkpoint.Binding,
                                new CoordinatedCsvCheckpointFaultInjector(
                                    faultPoint,
                                    reader,
                                    writer),
                                CancellationToken.None)
                            .ConfigureAwait(false);
                    lease.DataStream.Position = lease.DataStream.Length;
                    await lease.DataStream.WriteAsync(appendBytes)
                        .ConfigureAwait(false);
                    await lease.PersistCheckpointAsync(checkpoint)
                        .ConfigureAwait(false);
                })
            .ConfigureAwait(false);
    }

    private static async Task<int> RunCsvPublicationAsync(string[] args)
    {
        string destinationPath = Path.GetFullPath(
            RequiredOption(args, "--csv-publication-destination"));
        string manifestPath = Path.GetFullPath(
            RequiredOption(args, "--csv-publication-manifest"));
        string expectedManifestDigest =
            RequiredOption(args, "--csv-publication-manifest-digest");
        string faultName = RequiredOption(args, "--csv-publication-fault");
        if (!Enum.TryParse(
                faultName,
                ignoreCase: false,
                out CsvExportPublicationFaultPoint faultPoint))
        {
            throw new ArgumentException(
                $"Unknown CSV publication fault point '{faultName}'.",
                nameof(args));
        }

        return await RunCsvCoordinatedAsync(
                args,
                async (reader, writer) =>
                {
                    _ = await new CsvExportPreparedOutputPublisher(
                            new CoordinatedCsvPublicationFaultInjector(
                                faultPoint,
                                reader,
                                writer))
                        .PublishCompletedAsync(
                            new CsvExportPublicationRequest
                            {
                                DestinationPath = destinationPath,
                                ManifestPath = manifestPath,
                                ExpectedManifestDigest =
                                    expectedManifestDigest,
                            })
                        .ConfigureAwait(false);
                })
            .ConfigureAwait(false);
    }

    private static async Task<int> RunCsvCoordinatedAsync(
        IReadOnlyList<string> args,
        Func<StreamReader, StreamWriter, Task> operation)
    {
        string pipeName = RequiredOption(args, "--pipe");
        using var pipe = new NamedPipeClientStream(
            ".",
            pipeName,
            PipeDirection.InOut,
            PipeOptions.Asynchronous);
        using var connectTimeout =
            new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await pipe.ConnectAsync(connectTimeout.Token).ConfigureAwait(false);
        using var reader = new StreamReader(
            pipe,
            new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
            detectEncodingFromByteOrderMarks: false,
            bufferSize: 1024,
            leaveOpen: true);
        using var writer = new StreamWriter(
            pipe,
            new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
            bufferSize: 1024,
            leaveOpen: true)
        {
            AutoFlush = true,
        };

        await writer.WriteLineAsync("READY").ConfigureAwait(false);
        try
        {
            await operation(reader, writer).ConfigureAwait(false);
            await writer.WriteLineAsync("COMPLETED_WITHOUT_FAULT")
                .ConfigureAwait(false);
            return 3;
        }
        catch (Exception error)
        {
            try
            {
                string encodedMessage = Convert.ToBase64String(
                    Encoding.UTF8.GetBytes(error.Message));
                await writer.WriteLineAsync(
                        $"ERROR|{error.GetType().FullName}|{encodedMessage}")
                    .ConfigureAwait(false);
            }
            catch
            {
            }

            return 2;
        }
    }

    private static async Task RunFailFastAsync(
        string targetPath,
        MigrationCatalog catalog,
        ICSharpDbMigrationFaultInjector injector)
    {
        MigrationPlan plan = ReadyPlan(catalog, batchSize: 2);
        await using var source = new SyntheticMigrationDataSource(catalog);
        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                targetPath,
                plan,
                catalog,
                source.SnapshotIdentity,
                injector).ConfigureAwait(false);
        _ = await new MigrationApplyRunner().ApplyAsync(
            new MigrationApplyRequest
            {
                Plan = plan,
                Catalog = catalog,
                Source = source,
                Target = target,
            }).ConfigureAwait(false);
    }

    private static async Task RunDeterministicRejectAsync(
        string targetPath,
        MigrationCatalog catalog,
        string scenario,
        ICSharpDbMigrationFaultInjector injector)
    {
        MigrationPlan plan = ReadyDeterministicRejectPlan(catalog, batchSize: 3);
        MigrationTargetBatch batch = DeterministicBatch(plan, catalog, scenario);
        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                targetPath,
                plan,
                catalog,
                SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                injector).ConfigureAwait(false);
        _ = await target.WriteBatchAsync(batch).ConfigureAwait(false);
    }

    private static async Task RunRejectArtifactAsync(
        string targetPath,
        string outputPath,
        MigrationCatalog catalog,
        IMigrationRejectArtifactFaultInjector injector)
    {
        MigrationPlan plan = ReadyDeterministicRejectPlan(catalog, batchSize: 3);
        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                targetPath,
                plan,
                catalog,
                SyntheticMigrationDataSource.FixtureSnapshotIdentity).ConfigureAwait(false);
        _ = await new MigrationRejectArtifactWriter(injector).WriteAsync(
            new MigrationRejectArtifactWriteRequest
            {
                Plan = plan,
                Catalog = catalog,
                Target = target,
                OutputPath = outputPath,
            }).ConfigureAwait(false);
    }

    private static async ValueTask<MigrationCatalog> InspectAsync() =>
        await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
                ProfileSampleSize = 5,
            }).ConfigureAwait(false);

    private static MigrationPlan ReadyPlan(MigrationCatalog catalog, int batchSize)
    {
        MigrationPlan plan = new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions { AcceptAllExclusions = true });
        return plan with { Load = plan.Load with { BatchSize = batchSize } };
    }

    private static MigrationPlan ReadyDeterministicRejectPlan(
        MigrationCatalog catalog,
        int batchSize)
    {
        MigrationPlan plan = ReadyPlan(catalog, batchSize);
        return plan with
        {
            Load = plan.Load with
            {
                RejectMode = MigrationRejectMode.DeterministicRejects,
                RejectPolicy = new MigrationDeterministicRejectPolicy
                {
                    ContractVersion = MigrationRejectContract.DeterministicRejectsV1,
                    AllowedRuleIds = [DeterministicRuleId],
                    MaxRejectedRowsPerBatch = batchSize,
                    MaxRejectedRowsPerRun = 100,
                    MaxRawValueBytes = 1_024,
                    MaxRawValueBytesPerBatch = 8_192,
                    MaxRawValueBytesPerRun = 65_536,
                    MaxArtifactBytes = 131_072,
                },
            },
        };
    }

    private static MigrationTargetBatch DeterministicBatch(
        MigrationPlan plan,
        MigrationCatalog catalog,
        string scenario)
    {
        IReadOnlyList<MigrationTargetRow> rows;
        IReadOnlyList<MigrationRejectedRow> rejectedRows;
        switch (scenario)
        {
            case AcceptedOnlyScenario:
                rows =
                [
                    AcceptedRow(0, "zero"),
                    AcceptedRow(1, "one"),
                ];
                rejectedRows = [];
                break;
            case MixedScenario:
                rows =
                [
                    AcceptedRow(0, "zero"),
                    AcceptedRow(2, "two"),
                ];
                rejectedRows = [RejectedRow(1, "bad-one")];
                break;
            case AllRejectScenario:
                rows = [];
                rejectedRows =
                [
                    RejectedRow(0, "bad-zero"),
                    RejectedRow(1, "bad-one"),
                ];
                break;
            default:
                throw new ArgumentException(
                    $"Unknown migration crash scenario '{scenario}'.",
                    nameof(scenario));
        }

        var unsigned = new MigrationTargetBatch
        {
            PlanDigest = MigrationArtifactSerializer.ComputePlanDigest(plan),
            CatalogDigest = plan.CatalogDigest,
            SourceFingerprint = plan.Source.Fingerprint,
            SourceSnapshotIdentity = SyntheticMigrationDataSource.FixtureSnapshotIdentity,
            SourceObjectId = RejectSourceObjectId,
            ColumnObjectIds = IncludedColumnIds(catalog, plan, RejectSourceObjectId),
            BatchOrdinal = 0,
            StartCursor = null,
            NextCursor = null,
            BatchDigest = string.Empty,
            RejectContractVersion = MigrationRejectContract.DeterministicRejectsV1,
            Rows = rows,
            RejectedRows = rejectedRows,
        };
        MigrationTargetBatch rejectSealed = unsigned with
        {
            RejectDigest = MigrationRejectDigest.Compute(unsigned),
        };
        return rejectSealed with
        {
            BatchDigest = MigrationBatchDigest.Compute(rejectSealed),
        };
    }

    private static MigrationTargetRow AcceptedRow(long sourceRowOrdinal, string suffix) => new()
    {
        SourceRowOrdinal = sourceRowOrdinal,
        StableKey = suffix,
        Values =
        [
            DbValue.FromText($"lower-{suffix}"),
            DbValue.FromText($"upper-{suffix}"),
        ],
    };

    private static MigrationRejectedRow RejectedRow(
        long sourceRowOrdinal,
        string rawValue) => new()
        {
            SourceRowOrdinal = sourceRowOrdinal,
            RuleId = DeterministicRuleId,
            ColumnObjectId = RejectColumnObjectId,
            Evidence =
            [
                new MigrationRejectEvidence
                {
                    Name = MigrationRejectLedgerCodec.RawValueEvidenceName,
                    Value = rawValue,
                },
            ],
        };

    private static string[] IncludedColumnIds(
        MigrationCatalog catalog,
        MigrationPlan plan,
        string tableObjectId)
    {
        IReadOnlySet<string> included = plan.Objects
            .Where(item => item.Included)
            .Select(item => item.SourceObjectId)
            .ToHashSet(StringComparer.Ordinal);
        return catalog.Objects
            .Where(item => item.Kind == MigrationObjectKind.Column &&
                string.Equals(item.ParentObjectId, tableObjectId, StringComparison.Ordinal) &&
                included.Contains(item.ObjectId))
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .Select(item => item.ObjectId)
            .ToArray();
    }

    private static class JsonProcessJson
    {
        internal static readonly JsonSerializerOptions Options =
            new(JsonSerializerDefaults.Web)
            {
                WriteIndented = false,
            };
    }

    private sealed record JsonReadOutcome(
        IReadOnlyList<string> RowDigests,
        int RejectedRowCount,
        string? NextCursor);

    private sealed record JsonPackageProcessResult
    {
        public required string Mode { get; init; }

        public required string ManifestDigest { get; init; }

        public required string CatalogDigest { get; init; }

        public required string SnapshotIdentity { get; init; }

        public string? FirstBatchCursor { get; init; }

        public required int AcceptedRowCount { get; init; }

        public required int RejectedRowCount { get; init; }

        public required IReadOnlyList<string> RowDigests { get; init; }
    }

    private static string RequiredOption(IReadOnlyList<string> args, string name)
    {
        string? value = OptionalOption(args, name);
        return value ?? throw new ArgumentException(
            $"Missing required option '{name}'.",
            nameof(args));
    }

    private static string? OptionalOption(IReadOnlyList<string> args, string name)
    {
        for (int index = 0; index < args.Count; index++)
        {
            if (!string.Equals(args[index], name, StringComparison.Ordinal))
                continue;
            if (index + 1 >= args.Count || string.IsNullOrWhiteSpace(args[index + 1]))
                throw new ArgumentException($"Missing value for '{name}'.", nameof(args));
            return args[index + 1];
        }

        return null;
    }

    private sealed class CoordinatedCrashFaultInjector(
        CSharpDbMigrationFaultPoint faultPoint,
        StreamReader reader,
        StreamWriter writer) : ICSharpDbMigrationFaultInjector
    {
        private int _fired;

        public async ValueTask InjectAsync(
            CSharpDbMigrationFaultPoint point,
            MigrationTargetBatch batch,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (point != faultPoint || Interlocked.Exchange(ref _fired, 1) != 0)
                return;

            await writer.WriteLineAsync(
                $"REACHED|{point}|{batch.SourceObjectId}|{batch.BatchOrdinal}").ConfigureAwait(false);

            // The parent terminates this process after receiving REACHED. Waiting
            // for an explicit command keeps the process exactly at the boundary.
            string? command = await reader.ReadLineAsync().ConfigureAwait(false);
            if (!string.Equals(command, "CONTINUE", StringComparison.Ordinal))
                throw new EndOfStreamException("Crash coordinator disconnected before releasing the fault point.");
        }
    }

    private sealed class CoordinatedRejectArtifactFaultInjector(
        MigrationRejectArtifactFaultPoint faultPoint,
        StreamReader reader,
        StreamWriter writer) : IMigrationRejectArtifactFaultInjector
    {
        private int _fired;

        public async ValueTask InjectAsync(
            MigrationRejectArtifactFaultPoint point,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (point != faultPoint || Interlocked.Exchange(ref _fired, 1) != 0)
                return;

            await writer.WriteLineAsync($"ARTIFACT_REACHED|{point}").ConfigureAwait(false);
            string? command = await reader.ReadLineAsync().ConfigureAwait(false);
            if (!string.Equals(command, "CONTINUE", StringComparison.Ordinal))
            {
                throw new EndOfStreamException(
                    "Crash coordinator disconnected before releasing the reject-artifact fault point.");
            }
        }
    }

    private sealed class CoordinatedCsvCheckpointFaultInjector(
        CsvExportCheckpointFaultPoint faultPoint,
        StreamReader reader,
        StreamWriter writer) : ICsvExportCheckpointFaultInjector
    {
        private int _fired;

        public async ValueTask InjectAsync(
            CsvExportCheckpointFaultPoint point,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (point != faultPoint ||
                Interlocked.Exchange(ref _fired, 1) != 0)
            {
                return;
            }

            await writer.WriteLineAsync($"CSV_CHECKPOINT_REACHED|{point}")
                .ConfigureAwait(false);
            string? command = await reader.ReadLineAsync()
                .ConfigureAwait(false);
            if (!string.Equals(command, "CONTINUE", StringComparison.Ordinal))
            {
                throw new EndOfStreamException(
                    "Crash coordinator disconnected before releasing the CSV checkpoint fault point.");
            }
        }
    }

    private sealed class CoordinatedCsvPublicationFaultInjector(
        CsvExportPublicationFaultPoint faultPoint,
        StreamReader reader,
        StreamWriter writer) : ICsvExportPublicationFaultInjector
    {
        private int _fired;

        public async ValueTask InjectAsync(
            CsvExportPublicationFaultPoint point,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (point != faultPoint ||
                Interlocked.Exchange(ref _fired, 1) != 0)
            {
                return;
            }

            await writer.WriteLineAsync($"CSV_PUBLICATION_REACHED|{point}")
                .ConfigureAwait(false);
            string? command = await reader.ReadLineAsync()
                .ConfigureAwait(false);
            if (!string.Equals(command, "CONTINUE", StringComparison.Ordinal))
            {
                throw new EndOfStreamException(
                    "Crash coordinator disconnected before releasing the CSV publication fault point.");
            }
        }
    }
}
