using System.Text.RegularExpressions;

namespace CSharpDB.Admin.Forms.Tests.Components.Tabs;

public sealed class DataModelHelpTests
{
    [Theory]
    [InlineData("index.html")]
    [InlineData("workspace.html")]
    [InlineData("data-and-schema.html")]
    [InlineData("data-modeler.html")]
    [InlineData("forms-and-reports.html")]
    [InlineData("operations.html")]
    [InlineData("shortcuts.html")]
    public void HelpNavigation_IncludesDedicatedModelerGuide(string page)
    {
        string html = File.ReadAllText(Path.Combine(HelpDirectory(), page));
        Match navigation = Regex.Match(html,
            "<nav[^>]*aria-label=\"Help topics\"[^>]*>.*?</nav>",
            RegexOptions.Singleline, TimeSpan.FromSeconds(1));

        Assert.True(navigation.Success, $"Help navigation missing from {page}.");
        Assert.Contains("href=\"data-modeler.html\"", navigation.Value, StringComparison.Ordinal);
    }

    [Fact]
    public void ModelerGuide_HasLinkedWorkflowsAndSeparatesSavingFromApplying()
    {
        string html = File.ReadAllText(Path.Combine(HelpDirectory(), "data-modeler.html"));
        string[] workflows =
        [
            "diagram-or-database", "first-diagram", "selection", "relationships", "layout",
            "groups", "connectors", "saving", "schema-edits", "review-apply", "export-tools",
            "keyboard", "troubleshooting",
        ];

        foreach (string workflow in workflows)
        {
            Assert.Contains($"<h2 id=\"{workflow}\">", html, StringComparison.Ordinal);
            Assert.Contains($"href=\"#{workflow}\"", html, StringComparison.Ordinal);
        }

        Assert.Contains("Stores the diagram, including pending edits. Does not apply them.", html, StringComparison.Ordinal);
        Assert.Contains("The transaction is rolled back and pending changes remain.", html, StringComparison.Ordinal);
        Assert.Contains("The database commit succeeded. Do not reapply the old SQL.", html, StringComparison.Ordinal);
        Assert.Contains("not guaranteed exhaustive", html, StringComparison.Ordinal);
        Assert.Contains("not as passing", html, StringComparison.Ordinal);
        Assert.Contains("sys.diagrams", html, StringComparison.Ordinal);
    }

    [Fact]
    public void HelpBundle_LocalLinksResourcesAndFragmentsResolve()
    {
        string helpDirectory = HelpDirectory();
        foreach (string page in Directory.EnumerateFiles(helpDirectory, "*.html"))
        {
            string html = File.ReadAllText(page);
            foreach (Match match in Regex.Matches(html,
                         """(?:href|src)\s*=\s*["'](?<target>[^"']+)["']""",
                         RegexOptions.IgnoreCase, TimeSpan.FromSeconds(1)))
            {
                string target = match.Groups["target"].Value;
                if (Uri.TryCreate(target, UriKind.Absolute, out _) || target.StartsWith("//", StringComparison.Ordinal))
                    continue;

                string[] parts = target.Split('#', 2);
                string relativePath = Uri.UnescapeDataString(parts[0].Split('?', 2)[0]);
                string resolvedPath = relativePath.Length == 0
                    ? page
                    : Path.GetFullPath(Path.Combine(helpDirectory, relativePath));
                Assert.True(File.Exists(resolvedPath), $"{Path.GetFileName(page)} links to missing resource {target}.");

                if (parts.Length == 2 && parts[1].Length > 0)
                {
                    string fragment = Uri.UnescapeDataString(parts[1]);
                    Assert.Contains(fragment, ElementIds(File.ReadAllText(resolvedPath)));
                }
            }
        }
    }

    [Fact]
    public void ModelerGuide_HasUniqueAnchorsAndNeedsNoRemoteAssetsOrScripts()
    {
        string html = File.ReadAllText(Path.Combine(HelpDirectory(), "data-modeler.html"));
        string[] ids = ElementIds(html).ToArray();
        Assert.Equal(ids.Length, ids.Distinct(StringComparer.Ordinal).Count());
        Assert.DoesNotContain("<script", html, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("https://", html, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("http://", html, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("aria-current=\"page\"", html, StringComparison.Ordinal);
    }

    private static IEnumerable<string> ElementIds(string html) =>
        Regex.Matches(html, """\bid\s*=\s*["'](?<id>[^"']+)["']""",
                RegexOptions.IgnoreCase, TimeSpan.FromSeconds(1))
            .Select(match => match.Groups["id"].Value);

    private static string HelpDirectory()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "CSharpDB.slnx")))
                return Path.Combine(directory.FullName, "src", "CSharpDB.Admin", "wwwroot", "help");
            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate repository root from test base directory.");
    }
}
