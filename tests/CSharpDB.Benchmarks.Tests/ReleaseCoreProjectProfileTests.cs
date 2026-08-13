using System.Xml.Linq;

namespace CSharpDB.Benchmarks.Tests;

public sealed class ReleaseCoreProjectProfileTests
{
    private const string ReleaseCorePropertyName = "CSharpDbReleaseCoreOnly";
    private const string ObservabilityProject =
        @"..\..\src\CSharpDB.Observability\CSharpDB.Observability.csproj";
    private const string ObservabilityBenchmark =
        @"Micro\ObservabilityNoListenerBaselineBenchmarks.cs";

    [Fact]
    public void DefaultProfile_IncludesObservabilityProjectAndBenchmark()
    {
        XDocument project = LoadBenchmarkProject();

        XElement property = project.Descendants(ReleaseCorePropertyName).Single();
        Assert.Equal("false", property.Value);
        Assert.Equal(
            "'$(CSharpDbReleaseCoreOnly)' == ''",
            (string?)property.Attribute("Condition"));

        XElement observabilityReference = FindItem(
            project,
            "ProjectReference",
            "Include",
            ObservabilityProject);
        Assert.Equal(
            "'$(CSharpDbReleaseCoreOnly)' != 'true'",
            (string?)observabilityReference.Attribute("Condition"));

        string benchmarkPath = Path.Combine(
            FindRepoRoot(),
            "tests",
            "CSharpDB.Benchmarks",
            "Micro",
            "ObservabilityNoListenerBaselineBenchmarks.cs");
        Assert.True(File.Exists(benchmarkPath));
    }

    [Fact]
    public void ReleaseCoreProfile_ExcludesOnlyObservabilityCompatibilitySurface()
    {
        XDocument project = LoadBenchmarkProject();

        XElement compileRemoval = FindItem(
            project,
            "Compile",
            "Remove",
            ObservabilityBenchmark);
        XElement releaseCoreGroup = Assert.IsType<XElement>(compileRemoval.Parent);
        Assert.Equal(
            "'$(CSharpDbReleaseCoreOnly)' == 'true'",
            (string?)releaseCoreGroup.Attribute("Condition"));

        XElement observabilityContractReference = FindItem(
            project,
            "Reference",
            "Include",
            "CSharpDB.Observability");
        Assert.Same(releaseCoreGroup, observabilityContractReference.Parent);
        Assert.Equal(
            "Exists('..\\..\\src\\CSharpDB.Observability\\CSharpDB.Observability.csproj')",
            (string?)observabilityContractReference.Attribute("Condition"));
        Assert.EndsWith(
            @"CSharpDB.Observability\bin\$(Configuration)\$(TargetFramework)\CSharpDB.Observability.dll",
            (string?)observabilityContractReference.Attribute("HintPath"));

        XElement[] conditionalReferences = project
            .Descendants("ProjectReference")
            .Where(reference => reference.Attribute("Condition") is not null)
            .ToArray();
        XElement observabilityReference = Assert.Single(conditionalReferences);
        Assert.Equal(
            ObservabilityProject,
            (string?)observabilityReference.Attribute("Include"));

        XElement[] compileRemovals = project
            .Descendants("Compile")
            .Where(item => item.Attribute("Remove") is not null)
            .ToArray();
        Assert.Same(compileRemoval, Assert.Single(compileRemovals));
    }

    private static XElement FindItem(
        XDocument project,
        string itemName,
        string attributeName,
        string attributeValue)
    {
        return project
            .Descendants(itemName)
            .Single(item => string.Equals(
                (string?)item.Attribute(attributeName),
                attributeValue,
                StringComparison.Ordinal));
    }

    private static XDocument LoadBenchmarkProject()
    {
        return XDocument.Load(Path.Combine(
            FindRepoRoot(),
            "tests",
            "CSharpDB.Benchmarks",
            "CSharpDB.Benchmarks.csproj"));
    }

    private static string FindRepoRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "CSharpDB.slnx")))
                return directory.FullName;

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException(
            "Could not locate repository root from test base directory.");
    }
}
