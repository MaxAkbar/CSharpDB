using System.IO.Pipes;
using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.CSharpDb;

return await MigrationCrashHarness.RunAsync(args);

internal static class MigrationCrashHarness
{
    public static async Task<int> RunAsync(string[] args)
    {
        string targetPath = Path.GetFullPath(RequiredOption(args, "--target"));
        string pipeName = RequiredOption(args, "--pipe");
        string faultName = RequiredOption(args, "--fault");
        if (!Enum.TryParse(faultName, ignoreCase: false, out CSharpDbMigrationFaultPoint faultPoint))
            throw new ArgumentException($"Unknown migration fault point '{faultName}'.", nameof(args));

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
            MigrationPlan plan = ReadyPlan(catalog, batchSize: 2);
            var injector = new CoordinatedCrashFaultInjector(faultPoint, reader, writer);

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

    private static string RequiredOption(IReadOnlyList<string> args, string name)
    {
        for (int index = 0; index < args.Count; index++)
        {
            if (!string.Equals(args[index], name, StringComparison.Ordinal))
                continue;
            if (index + 1 >= args.Count || string.IsNullOrWhiteSpace(args[index + 1]))
                throw new ArgumentException($"Missing value for '{name}'.", nameof(args));
            return args[index + 1];
        }

        throw new ArgumentException($"Missing required option '{name}'.", nameof(args));
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
}
