namespace CSharpDB.EntityFrameworkCore.Tools.Tests;

public sealed class EfCoreAnalyzeCommandRunnerTests
{
    [Fact]
    public async Task RunAsync_RequiresAnalyzeCommand()
    {
        using var output = new StringWriter();
        using var error = new StringWriter();

        int exitCode = await EfCoreAnalyzeCommandRunner.RunAsync(
            [],
            output,
            error,
            TestContext.Current.CancellationToken);

        Assert.Equal(EfCoreAnalyzeCommandRunner.ExitUsage, exitCode);
        Assert.Empty(output.ToString());
        Assert.Contains(
            "CSHARPDB-EF-USAGE-001",
            error.ToString(),
            StringComparison.Ordinal);
        Assert.Contains(
            EfCoreAnalyzeCommandRunner.Usage,
            error.ToString(),
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData(".Context")]
    [InlineData("Context.")]
    [InlineData("Context..Nested")]
    [InlineData("1Context")]
    [InlineData("Context`1")]
    [InlineData("Context/Name")]
    [InlineData("Context Name")]
    public async Task RunAsync_RejectsUnsafeContextSelector(
        string context)
    {
        using var output = new StringWriter();
        using var error = new StringWriter();

        int exitCode = await EfCoreAnalyzeCommandRunner.RunAsync(
            [
                "analyze",
                "--project",
                "ignored.csproj",
                "--context",
                context,
            ],
            output,
            error,
            TestContext.Current.CancellationToken);

        Assert.Equal(EfCoreAnalyzeCommandRunner.ExitUsage, exitCode);
        Assert.Empty(output.ToString());
        if (context.Length > 0)
        {
            Assert.DoesNotContain(
                context,
                error.ToString(),
                StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("AppDbContext")]
    [InlineData("MyApp.Data.AppDbContext")]
    [InlineData("MyApp.Data.Outer+AppDbContext")]
    [InlineData("_Context")]
    public void IsSafeContextSelector_AcceptsBoundedTypeNames(
        string context)
    {
        Assert.True(
            EfCoreAnalyzeCommandRunner.IsSafeContextSelector(context));
    }

    [Fact]
    public async Task RunAsync_MissingProjectDoesNotEchoPath()
    {
        string secretPath = Path.Combine(
            Path.GetTempPath(),
            "TOP-SECRET-EF-PROJECT.csproj");
        using var output = new StringWriter();
        using var error = new StringWriter();

        int exitCode = await EfCoreAnalyzeCommandRunner.RunAsync(
            [
                "analyze",
                "--project",
                secretPath,
                "--context",
                "AppDbContext",
            ],
            output,
            error,
            TestContext.Current.CancellationToken);

        Assert.Equal(EfCoreAnalyzeCommandRunner.ExitError, exitCode);
        Assert.Empty(output.ToString());
        Assert.Contains(
            "CSHARPDB-EF-PROJECT-001",
            error.ToString(),
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "TOP-SECRET",
            error.ToString(),
            StringComparison.Ordinal);
    }
}
