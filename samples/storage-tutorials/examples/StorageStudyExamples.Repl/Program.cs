using StorageStudyExamples.Core;
using StorageStudyExamples.VirtualDrive;
using StorageStudyExamples.ConfigStore;
using StorageStudyExamples.EventLog;
using StorageStudyExamples.TaskQueue;
using StorageStudyExamples.GraphStore;
using StorageStudyExamples.StorageInternals;

namespace StorageStudyExamples.Repl;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var examples = RegisterExamples();
        var repl = new ReplHost(examples);
        await repl.RunAsync();
        return 0;
    }

    private static IReadOnlyList<IExample> RegisterExamples()
    {
        var examples = new List<IExample>
        {
            // Application-pattern examples (interactive)
            new VirtualDriveStore(),
            new ConfigDataStore(),
            new EventLogStore(),
            new TaskQueueStore(),
            new GraphDataStore(),
        };

        // Storage internals examples (demo-only)
        examples.AddRange(StorageInternalsExample.CreateAll());

        return examples;
    }
}
