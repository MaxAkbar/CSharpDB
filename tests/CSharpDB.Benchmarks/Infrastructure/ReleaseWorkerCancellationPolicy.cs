namespace CSharpDB.Benchmarks.Infrastructure;

internal static class ReleaseWorkerCancellationPolicy
{
    // Hosted release runners can briefly starve benchmark workers while other
    // test processes are closing out. Coordinated phase cancellation must allow
    // those workers enough time to run their cancellation and cleanup paths.
    internal static TimeSpan CoordinatedDrainTimeout { get; } = TimeSpan.FromSeconds(30);
}
