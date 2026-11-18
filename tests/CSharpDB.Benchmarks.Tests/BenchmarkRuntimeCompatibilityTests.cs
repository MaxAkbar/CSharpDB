using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Toolchains.InProcess.NoEmit;

namespace CSharpDB.Benchmarks.Tests;

public sealed class BenchmarkRuntimeCompatibilityTests
{
    [Fact]
    public void DefaultToolchainRecognizesCurrentRuntime()
    {
        CoreRuntime runtime = Assert.IsType<CoreRuntime>(
            InProcessNoEmitToolchain.Default.Runtime);

        Assert.Equal(Environment.Version.Major, runtime.Version.Major);
        Assert.Equal(Environment.Version.Minor, runtime.Version.Minor);
    }
}
