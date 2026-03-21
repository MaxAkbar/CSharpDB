using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.InProcess.NoEmit;

namespace CSharpDB.Benchmarks.Infrastructure;

internal sealed class CollectionInProcessBenchmarkConfig : ManualConfig
{
    public CollectionInProcessBenchmarkConfig()
        : this(3, 10)
    {
    }

    private CollectionInProcessBenchmarkConfig(int warmupCount, int iterationCount)
    {
        AddJob(
            Job.Default
                .WithToolchain(InProcessNoEmitToolchain.Instance)
                .WithWarmupCount(warmupCount)
                .WithIterationCount(iterationCount));
    }
}
