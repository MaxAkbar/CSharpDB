using CSharpDB.Engine;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Benchmarks.Infrastructure;

internal static class CrashHarness
{
    public static async Task RunAsync(string[] args)
    {
        if (args.Length < 3)
            throw new ArgumentException("Usage: --crash-harness <scenario> <dbPath> <markerPath>");

        string scenario = args[0].ToLowerInvariant();
        string dbPath = Path.GetFullPath(args[1]);
        string markerPath = Path.GetFullPath(args[2]);

        Directory.CreateDirectory(Path.GetDirectoryName(markerPath)!);

        switch (scenario)
        {
            case "commit-after-return":
                await CrashImmediatelyAfterCommitReturnsAsync(dbPath, markerPath);
                return;

            case "checkpoint-start":
                await CrashDuringCheckpointStartAsync(dbPath, markerPath);
                return;

            case "checkpoint-after-device-flush":
                await CrashDuringCheckpointAtStoragePhaseAsync(
                    dbPath,
                    markerPath,
                    crashPoint: "checkpoint-after-device-flush",
                    insertedValue: 303);
                return;

            case "checkpoint-after-wal-finalize":
                await CrashDuringCheckpointAtStoragePhaseAsync(
                    dbPath,
                    markerPath,
                    crashPoint: "checkpoint-after-wal-finalize",
                    insertedValue: 404);
                return;

            default:
                throw new ArgumentException($"Unknown crash harness scenario '{scenario}'.");
        }
    }

    private static async Task CrashImmediatelyAfterCommitReturnsAsync(string dbPath, string markerPath)
    {
        await using var db = await Database.OpenAsync(dbPath);
        await db.ExecuteAsync("INSERT INTO t VALUES (1, 101)");
        File.WriteAllText(markerPath, "commit-returned");
        Environment.FailFast("Crash harness: commit returned, then process terminated immediately.");
    }

    private static async Task CrashDuringCheckpointStartAsync(string dbPath, string markerPath)
    {
        var options = new DatabaseOptions().ConfigureStorageEngine(builder =>
        {
            builder.UsePagerOptions(new PagerOptions
            {
                Interceptors = [new FailFastOnCheckpointStartInterceptor(markerPath)],
            });
        });

        await using var db = await Database.OpenAsync(dbPath, options);
        await db.ExecuteAsync("INSERT INTO t VALUES (1, 202)");
        await db.CheckpointAsync();

        throw new InvalidOperationException("Checkpoint crash harness completed without terminating the process.");
    }

    private static async Task CrashDuringCheckpointAtStoragePhaseAsync(
        string dbPath,
        string markerPath,
        string crashPoint,
        int insertedValue)
    {
        AppContext.SetData("CSharpDB.TestCrashPoint", crashPoint);
        AppContext.SetData("CSharpDB.TestCrashMarkerPath", markerPath);

        try
        {
            await using var db = await Database.OpenAsync(dbPath);
            await db.ExecuteAsync($"INSERT INTO t VALUES (1, {insertedValue})");
            await db.CheckpointAsync();
        }
        finally
        {
            AppContext.SetData("CSharpDB.TestCrashPoint", null);
            AppContext.SetData("CSharpDB.TestCrashMarkerPath", null);
        }

        throw new InvalidOperationException(
            $"Checkpoint crash harness completed without terminating the process at '{crashPoint}'.");
    }

    private sealed class FailFastOnCheckpointStartInterceptor : IPageOperationInterceptor
    {
        private readonly string _markerPath;
        private int _fired;

        public FailFastOnCheckpointStartInterceptor(string markerPath)
        {
            _markerPath = markerPath;
        }

        public ValueTask OnBeforeReadAsync(uint pageId, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnAfterReadAsync(uint pageId, PageReadSource source, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnBeforeWriteAsync(uint pageId, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnAfterWriteAsync(uint pageId, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnCommitStartAsync(int dirtyPageCount, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnCommitEndAsync(int dirtyPageCount, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnCheckpointStartAsync(int committedFrameCount, CancellationToken ct = default)
        {
            if (Interlocked.Exchange(ref _fired, 1) != 0)
                return ValueTask.CompletedTask;

            File.WriteAllText(_markerPath, "checkpoint-started");
            Environment.FailFast("Crash harness: process terminated during checkpoint.");
            return ValueTask.CompletedTask;
        }

        public ValueTask OnCheckpointEndAsync(int committedFrameCount, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnRecoveryStartAsync(CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnRecoveryEndAsync(bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;
    }
}
