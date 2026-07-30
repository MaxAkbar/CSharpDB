using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class ForeignKeyConstraintGraphPropertyTests
{
    private const int Seed = 0x0C45_CA5C;
    private const int GeneratedGraphCount = 16;

    [Fact]
    public async Task DeterministicCascadeGraphs_MatchReachabilityRegardlessOfDeclarationOrder()
    {
        var random = new Random(Seed);
        CancellationToken ct = TestContext.Current.CancellationToken;

        for (int graphIndex = 0; graphIndex < GeneratedGraphCount; graphIndex++)
        {
            int nodeCount = random.Next(5, 9);
            List<ForeignKeyEdge> edges = GenerateGraph(random, nodeCount);
            bool[] expectedRows = ExpectedRowsAfterRootDelete(nodeCount, edges);

            bool[] declaredForward = await ExecuteGraphAsync(
                nodeCount,
                edges,
                reverseDeclarations: false,
                ct);
            bool[] declaredReverse = await ExecuteGraphAsync(
                nodeCount,
                edges,
                reverseDeclarations: true,
                ct);

            Assert.True(
                expectedRows.SequenceEqual(declaredForward),
                $"seed={Seed}, graph={graphIndex}, declaration-order=forward, " +
                $"edges={Describe(edges)}");
            Assert.True(
                expectedRows.SequenceEqual(declaredReverse),
                $"seed={Seed}, graph={graphIndex}, declaration-order=reverse, " +
                $"edges={Describe(edges)}");
            Assert.Equal(declaredForward, declaredReverse);
        }
    }

    private static List<ForeignKeyEdge> GenerateGraph(
        Random random,
        int nodeCount)
    {
        var edges = new List<ForeignKeyEdge>
        {
            new(Child: 1, Parent: 0),
        };

        // Keep the final node isolated so every generated case includes a
        // non-cascaded control row.
        for (int child = 2; child < nodeCount - 1; child++)
        {
            for (int parent = 0; parent < child; parent++)
            {
                if (random.Next(100) < 35)
                    edges.Add(new ForeignKeyEdge(child, parent));
            }
        }

        return edges;
    }

    private static bool[] ExpectedRowsAfterRootDelete(
        int nodeCount,
        IReadOnlyList<ForeignKeyEdge> edges)
    {
        var deleted = new bool[nodeCount];
        deleted[0] = true;

        bool changed;
        do
        {
            changed = false;
            foreach (ForeignKeyEdge edge in edges)
            {
                if (deleted[edge.Parent] && !deleted[edge.Child])
                {
                    deleted[edge.Child] = true;
                    changed = true;
                }
            }
        } while (changed);

        return deleted.Select(value => !value).ToArray();
    }

    private static async Task<bool[]> ExecuteGraphAsync(
        int nodeCount,
        IReadOnlyList<ForeignKeyEdge> edges,
        bool reverseDeclarations,
        CancellationToken ct)
    {
        await using Database database = await Database.OpenInMemoryAsync(ct);

        for (int node = 0; node < nodeCount; node++)
        {
            IEnumerable<ForeignKeyEdge> nodeEdges =
                edges.Where(edge => edge.Child == node);
            if (reverseDeclarations)
                nodeEdges = nodeEdges.Reverse();

            List<ForeignKeyEdge> materializedEdges = nodeEdges.ToList();
            string columns = string.Concat(
                materializedEdges.Select(
                    edge =>
                        $", parent_{edge.Parent} INTEGER " +
                        $"REFERENCES graph_node_{edge.Parent}(id) ON DELETE CASCADE"));
            await database.ExecuteAsync(
                $"CREATE TABLE graph_node_{node} " +
                $"(id INTEGER PRIMARY KEY{columns})",
                ct);
        }

        for (int node = 0; node < nodeCount; node++)
        {
            List<ForeignKeyEdge> nodeEdges = edges
                .Where(edge => edge.Child == node)
                .ToList();
            string columnList = nodeEdges.Count == 0
                ? "id"
                : $"id, {string.Join(", ", nodeEdges.Select(edge => $"parent_{edge.Parent}"))}";
            string values = string.Join(
                ", ",
                Enumerable.Repeat("1", nodeEdges.Count + 1));
            await database.ExecuteAsync(
                $"INSERT INTO graph_node_{node} ({columnList}) VALUES ({values})",
                ct);
        }

        await database.ExecuteAsync("DELETE FROM graph_node_0 WHERE id = 1", ct);

        var rowsRemaining = new bool[nodeCount];
        for (int node = 0; node < nodeCount; node++)
        {
            await using QueryResult result = await database.ExecuteAsync(
                $"SELECT COUNT(*) FROM graph_node_{node}",
                ct);
            IReadOnlyList<DbValue[]> rows = await result.ToListAsync(ct);
            rowsRemaining[node] = rows[0][0].AsInteger == 1;
        }

        return rowsRemaining;
    }

    private static string Describe(IEnumerable<ForeignKeyEdge> edges) =>
        string.Join(
            ",",
            edges.Select(edge => $"{edge.Parent}->{edge.Child}"));

    private readonly record struct ForeignKeyEdge(int Child, int Parent);
}
