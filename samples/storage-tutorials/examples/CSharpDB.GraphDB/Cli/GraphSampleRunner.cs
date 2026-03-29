using CSharpDB.GraphDB;

internal sealed class GraphSampleRunner(AnsiConsoleWriter console, GraphConsolePresenter presenter)
{
    public async Task RunAsync(IGraphApi client, CancellationToken ct)
    {
        await console.WriteSectionAsync("Loading sample graph: Social Network + Cities");

        // Reset.
        await client.ResetAsync(ct);
        await console.WriteSuccessAsync("Database reset.");

        // ── Nodes: People ──────────────────────────────────────
        var alice   = await client.AddNodeAsync("Alice",   "person", new() { ["age"] = "30", ["role"] = "engineer" }, ct);
        var bob     = await client.AddNodeAsync("Bob",     "person", new() { ["age"] = "28", ["role"] = "designer" }, ct);
        var carol   = await client.AddNodeAsync("Carol",   "person", new() { ["age"] = "35", ["role"] = "manager" }, ct);
        var dave    = await client.AddNodeAsync("Dave",    "person", new() { ["age"] = "42", ["role"] = "architect" }, ct);
        var eve     = await client.AddNodeAsync("Eve",     "person", new() { ["age"] = "26", ["role"] = "developer" }, ct);
        var frank   = await client.AddNodeAsync("Frank",   "person", new() { ["age"] = "33", ["role"] = "devops" }, ct);
        var grace   = await client.AddNodeAsync("Grace",   "person", new() { ["age"] = "29", ["role"] = "data scientist" }, ct);
        var heidi   = await client.AddNodeAsync("Heidi",   "person", new() { ["age"] = "38", ["role"] = "CTO" }, ct);
        await console.WriteSuccessAsync($"Created 8 people (IDs {alice.Id}–{heidi.Id})");

        // ── Nodes: Cities ──────────────────────────────────────
        var nyc     = await client.AddNodeAsync("New York",     "city", new() { ["country"] = "US" }, ct);
        var london  = await client.AddNodeAsync("London",       "city", new() { ["country"] = "UK" }, ct);
        var paris   = await client.AddNodeAsync("Paris",        "city", new() { ["country"] = "France" }, ct);
        var berlin  = await client.AddNodeAsync("Berlin",       "city", new() { ["country"] = "Germany" }, ct);
        var tokyo   = await client.AddNodeAsync("Tokyo",        "city", new() { ["country"] = "Japan" }, ct);
        var sf      = await client.AddNodeAsync("San Francisco","city", new() { ["country"] = "US" }, ct);
        await console.WriteSuccessAsync($"Created 6 cities (IDs {nyc.Id}–{sf.Id})");

        // ── Nodes: Companies ───────────────────────────────────
        var acme    = await client.AddNodeAsync("Acme Corp",    "company", new() { ["industry"] = "tech" }, ct);
        var globex  = await client.AddNodeAsync("Globex",       "company", new() { ["industry"] = "finance" }, ct);
        var initech = await client.AddNodeAsync("Initech",      "company", new() { ["industry"] = "consulting" }, ct);
        await console.WriteSuccessAsync($"Created 3 companies (IDs {acme.Id}–{initech.Id})");

        // ── Edges: KNOWS (person → person) ─────────────────────
        await client.AddEdgeAsync(alice.Id, bob.Id,   "KNOWS", 0.9,  null, ct);
        await client.AddEdgeAsync(alice.Id, carol.Id, "KNOWS", 0.7,  null, ct);
        await client.AddEdgeAsync(bob.Id,   dave.Id,  "KNOWS", 0.5,  null, ct);
        await client.AddEdgeAsync(bob.Id,   eve.Id,   "KNOWS", 0.8,  null, ct);
        await client.AddEdgeAsync(carol.Id, dave.Id,  "KNOWS", 0.6,  null, ct);
        await client.AddEdgeAsync(carol.Id, frank.Id, "KNOWS", 0.4,  null, ct);
        await client.AddEdgeAsync(dave.Id,  grace.Id, "KNOWS", 0.7,  null, ct);
        await client.AddEdgeAsync(eve.Id,   frank.Id, "KNOWS", 0.3,  null, ct);
        await client.AddEdgeAsync(frank.Id, grace.Id, "KNOWS", 0.6,  null, ct);
        await client.AddEdgeAsync(grace.Id, heidi.Id, "KNOWS", 0.9,  null, ct);
        await client.AddEdgeAsync(heidi.Id, alice.Id, "KNOWS", 0.5,  null, ct);
        await console.WriteSuccessAsync("Created 11 KNOWS edges");

        // ── Edges: LIVES_IN (person → city) ────────────────────
        await client.AddEdgeAsync(alice.Id, nyc.Id,    "LIVES_IN", 0, null, ct);
        await client.AddEdgeAsync(bob.Id,   london.Id, "LIVES_IN", 0, null, ct);
        await client.AddEdgeAsync(carol.Id, paris.Id,  "LIVES_IN", 0, null, ct);
        await client.AddEdgeAsync(dave.Id,  berlin.Id, "LIVES_IN", 0, null, ct);
        await client.AddEdgeAsync(eve.Id,   tokyo.Id,  "LIVES_IN", 0, null, ct);
        await client.AddEdgeAsync(frank.Id, sf.Id,     "LIVES_IN", 0, null, ct);
        await client.AddEdgeAsync(grace.Id, nyc.Id,    "LIVES_IN", 0, null, ct);
        await client.AddEdgeAsync(heidi.Id, london.Id, "LIVES_IN", 0, null, ct);
        await console.WriteSuccessAsync("Created 8 LIVES_IN edges");

        // ── Edges: WORKS_AT (person → company) ─────────────────
        await client.AddEdgeAsync(alice.Id, acme.Id,    "WORKS_AT", 0, null, ct);
        await client.AddEdgeAsync(bob.Id,   acme.Id,    "WORKS_AT", 0, null, ct);
        await client.AddEdgeAsync(carol.Id, globex.Id,  "WORKS_AT", 0, null, ct);
        await client.AddEdgeAsync(dave.Id,  globex.Id,  "WORKS_AT", 0, null, ct);
        await client.AddEdgeAsync(eve.Id,   initech.Id, "WORKS_AT", 0, null, ct);
        await client.AddEdgeAsync(frank.Id, initech.Id, "WORKS_AT", 0, null, ct);
        await client.AddEdgeAsync(grace.Id, acme.Id,    "WORKS_AT", 0, null, ct);
        await client.AddEdgeAsync(heidi.Id, acme.Id,    "WORKS_AT", 0, null, ct);
        await console.WriteSuccessAsync("Created 8 WORKS_AT edges");

        // ── Edges: ROAD_TO (city → city, weighted by distance km) ──
        await client.AddEdgeAsync(nyc.Id,    london.Id, "ROAD_TO", 5570, null, ct);
        await client.AddEdgeAsync(london.Id, paris.Id,  "ROAD_TO", 460,  null, ct);
        await client.AddEdgeAsync(paris.Id,  berlin.Id, "ROAD_TO", 1050, null, ct);
        await client.AddEdgeAsync(berlin.Id, london.Id, "ROAD_TO", 930,  null, ct);
        await client.AddEdgeAsync(nyc.Id,    sf.Id,     "ROAD_TO", 4130, null, ct);
        await client.AddEdgeAsync(sf.Id,     tokyo.Id,  "ROAD_TO", 8270, null, ct);
        await client.AddEdgeAsync(tokyo.Id,  london.Id, "ROAD_TO", 9560, null, ct);
        await console.WriteSuccessAsync("Created 7 ROAD_TO edges (weighted)");

        await console.WriteBlankLineAsync();
        var nodeCount = await client.CountNodesAsync(ct);
        var edgeCount = await client.CountEdgesAsync(ct);
        await console.WriteInfoAsync($"Total: {nodeCount} nodes, {edgeCount} edges");

        // ── Demo 1: Outgoing edges from Alice ──────────────────
        await console.WriteBlankLineAsync();
        var (outEdges, outScanned) = await client.GetOutgoingEdgesAsync(alice.Id, null, ct);
        await presenter.ShowEdgeListAsync($"Demo 1: All outgoing edges from Alice (ID {alice.Id})", outEdges, outScanned);

        // ── Demo 2: Incoming edges to Acme Corp ────────────────
        await console.WriteBlankLineAsync();
        var (inEdges, inScanned) = await client.GetIncomingEdgesAsync(acme.Id, null, ct);
        await presenter.ShowEdgeListAsync($"Demo 2: All incoming edges to Acme Corp (ID {acme.Id})", inEdges, inScanned);

        // ── Demo 3: KNOWS-only from Alice ──────────────────────
        await console.WriteBlankLineAsync();
        var (knowsEdges, knowsScanned) = await client.GetOutgoingEdgesAsync(alice.Id, "KNOWS", ct);
        await presenter.ShowEdgeListAsync($"Demo 3: KNOWS edges from Alice (label filter)", knowsEdges, knowsScanned);

        // ── Demo 4: BFS from Alice, depth 2 ────────────────────
        await console.WriteBlankLineAsync();
        var bfsResult = await client.TraverseBfsAsync(alice.Id, 2, "KNOWS", "outgoing", ct);
        await presenter.ShowTraversalResultAsync($"Demo 4: BFS from Alice (depth 2, KNOWS only)", bfsResult);

        // ── Demo 5: Shortest path Alice → Grace ────────────────
        await console.WriteBlankLineAsync();
        var pathResult = await client.ShortestPathAsync(alice.Id, grace.Id, 10, "KNOWS", ct);
        await presenter.ShowTraversalResultAsync($"Demo 5: Shortest path Alice → Grace (KNOWS edges)", pathResult);

        // ── Demo 6: City routes BFS ────────────────────────────
        await console.WriteBlankLineAsync();
        var cityBfs = await client.TraverseBfsAsync(nyc.Id, 3, "ROAD_TO", "outgoing", ct);
        await presenter.ShowTraversalResultAsync($"Demo 6: BFS city routes from New York (ROAD_TO, depth 3)", cityBfs);
    }
}
