using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text.Json;

namespace CSharpDB.Benchmarks.Infrastructure;

internal static class PerfEnvironment
{
    // Capture the actual benchmark process runtime before normal benchmark
    // dispatch. This command does not run or initialize any benchmark workload.
    [ModuleInitializer]
    internal static void CaptureIfRequested()
    {
        string[] args = Environment.GetCommandLineArgs();
        if (args.Length != 2 || args[1] != "--perf-environment")
            return;

        Console.WriteLine(JsonSerializer.Serialize(new
        {
            frameworkDescription = RuntimeInformation.FrameworkDescription,
            runtimeVersion = Environment.Version.ToString(),
            processArchitecture = RuntimeInformation.ProcessArchitecture.ToString(),
            targetFramework = typeof(PerfEnvironment).Assembly.GetCustomAttribute<TargetFrameworkAttribute>()!.FrameworkName,
        }));
        Environment.Exit(0);
    }
}
