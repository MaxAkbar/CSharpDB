using CSharpDB.Admin.Components.Shared;
using CSharpDB.Client.Models;
using CSharpDB.DevOps;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace CSharpDB.Admin.Forms.Tests.Components;

public sealed class DefinitionDependencyGraphTests
{
    [Fact]
    public async Task Graph_PreservesCyclesAndAnnouncesTruncationWithSafeLabels()
    {
        var definitions = Enumerable.Range(0, 80).Select(i => new DefinitionCatalogRecord { Id = i.ToString(), Kind = "View", Name = i == 0 ? "<script>root</script>" : "view" + i, Source = "SELECT 1" }).ToArray();
        var edges = Enumerable.Range(1, 79).Select(i => new DefinitionDependency(i.ToString(), "0", null, null, "Table", "definition")).Append(new("0", "0", null, null, "Table", "definition")).ToArray();
        var analysis = new DefinitionAnalysis(new("test", DateTimeOffset.UtcNow, definitions, []), edges, [], new Dictionary<string, IReadOnlyList<string>>());
        using var services = new ServiceCollection().BuildServiceProvider();
        await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);
        await renderer.Dispatcher.InvokeAsync(async () =>
        {
            var root = await renderer.RenderComponentAsync<DefinitionDependencyGraph>(ParameterView.FromDictionary(new Dictionary<string, object?>
            { [nameof(DefinitionDependencyGraph.Analysis)] = analysis, [nameof(DefinitionDependencyGraph.SelectedId)] = "0" }));
            string html = root.ToHtmlString();
            Assert.Equal(50, System.Text.RegularExpressions.Regex.Matches(html, "class=\"graph-node").Count);
            Assert.Contains("50 of 80 connected objects", html); Assert.Contains("Display limit reached", html);
            Assert.DoesNotContain("<script>root", html); Assert.Contains("&lt;script&gt;root", html);
            Assert.Contains("class=\"graph-edge\" data-relationship=\"usage\" d=", html);
        });
    }

    [Fact]
    public async Task Graph_DistinguishesMembershipProposalsAndArchiveEdgesBetweenSameObjects()
    {
        var definitions = new[] { new DefinitionCatalogRecord { Id = "model", Kind = "Data model", Name = "Orders diagram", Source = "{}" }, new DefinitionCatalogRecord { Id = "table", Kind = "Table", Name = "Orders", Source = "{}" } };
        var edges = Enum.GetValues<DependencyRelationshipKind>().Select(kind => new DefinitionDependency("model", "table", null, null, kind.ToString(), "nodes", Relationship: kind)).ToArray();
        var analysis = new DefinitionAnalysis(new("test", DateTimeOffset.UtcNow, definitions, []), edges, [], new Dictionary<string, IReadOnlyList<string>>());
        using var services = new ServiceCollection().BuildServiceProvider();
        await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);
        await renderer.Dispatcher.InvokeAsync(async () =>
        {
            var root = await renderer.RenderComponentAsync<DefinitionDependencyGraph>(ParameterView.FromDictionary(new Dictionary<string, object?>
            { [nameof(DefinitionDependencyGraph.Analysis)] = analysis, [nameof(DefinitionDependencyGraph.SelectedId)] = "model" }));
            string html = root.ToHtmlString();
            Assert.Contains("4 of 4 visible relationships", html);
            Assert.Contains("Graph relationship type", html);
            foreach (string kind in new[] { "usage", "membership", "proposed", "archive" }) Assert.Contains($"data-relationship=\"{kind}\"", html);
            Assert.Contains("Model membership", html);
            Assert.Contains("Proposed changes", html);
            Assert.Contains("Archive relationships", html);
        });
    }
}
