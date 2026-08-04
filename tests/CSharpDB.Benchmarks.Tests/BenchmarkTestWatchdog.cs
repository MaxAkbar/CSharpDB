namespace CSharpDB.Benchmarks.Tests;

internal static class BenchmarkTestWatchdog
{
    // This bounds test orchestration only. Logical benchmark durations and
    // deliberately short failure-injection timeouts remain independently set.
    internal static TimeSpan SchedulingTimeout { get; } = TimeSpan.FromSeconds(30);
}
