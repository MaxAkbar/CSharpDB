using CSharpDB.Client;

namespace CSharpDB.Cli.Tests;

public sealed class CliShellOptionsTests
{
    [Fact]
    public void TryParse_PositionalDatabasePath_UsesDirectDefaults()
    {
        bool ok = CliShellOptions.TryParse(["sample.db"], out var options, out var error);

        Assert.True(ok);
        Assert.Null(error);
        Assert.NotNull(options);
        Assert.Equal("sample.db", options!.DisplayTarget);
        Assert.True(options.EnableLocalDirectFeatures);
        Assert.Equal("sample.db", options.ClientOptions.Endpoint);
        Assert.Null(options.ClientOptions.Transport);
    }

    [Fact]
    public void TryParse_GrpcEndpoint_RequiresExplicitTransport()
    {
        bool ok = CliShellOptions.TryParse(
            ["--transport", "grpc", "--endpoint", "https://localhost:5001"],
            out var options,
            out var error);

        Assert.True(ok);
        Assert.Null(error);
        Assert.NotNull(options);
        Assert.False(options!.EnableLocalDirectFeatures);
        Assert.Equal(CSharpDbTransport.Grpc, options.ClientOptions.Transport);
        Assert.Equal("https://localhost:5001", options.ClientOptions.Endpoint);
    }

    [Fact]
    public void TryParse_UnknownOption_Fails()
    {
        bool ok = CliShellOptions.TryParse(["--unknown"], out var options, out var error);

        Assert.False(ok);
        Assert.Null(options);
        Assert.Equal("Unknown option '--unknown'.", error);
    }
}
