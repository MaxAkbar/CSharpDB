using System.IO.Pipes;
using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.CSharpDb;
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
}
