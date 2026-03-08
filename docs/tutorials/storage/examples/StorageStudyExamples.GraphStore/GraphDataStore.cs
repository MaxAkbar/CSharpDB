// ============================================================================
// Graph Store (Social Network) Example
// ============================================================================
//
// Demonstrates using CSharpDB as a graph database with nodes and edges
// stored relationally. Builds a social network and runs traversal queries:
// adjacency lists, friends-of-friends, mutual follows, most connected.
// Shows: JOINs (including self-JOINs), GROUP BY with COUNT, batch inserts,
// indexed lookups, post-query filtering in C#, UPDATE (rename node),
// DELETE (unfollow, remove user with cascading edge cleanup).
// ============================================================================

using CSharpDB.Core;
using StorageStudyExamples.Core;

namespace StorageStudyExamples.GraphStore;

public sealed class GraphDataStore : DataStoreBase
{
    // ── Node ID constants ──────────────────────────────────────────────────
    private const int AliceId = 1;
    private const int BobId = 2;
    private const int CharlieId = 3;
    private const int DianaId = 4;
    private const int EveId = 5;
    private const int FrankId = 6;
    private const int GraceId = 7;
    private const int HankId = 8;
    private const int IvyId = 9;
    private const int JackId = 10;

    /// <summary>Tracks the next available edge ID across seed and demo methods.</summary>
    private int _edgeId = 1;

    public override string Name => "Graph Store";
    public override string CommandName => "graph-store";
    public override string Description => "Social network graph with traversal queries.";

    // ── Schema ─────────────────────────────────────────────────────────────

    protected override async Task CreateSchemaAsync()
    {
        await Db.ExecuteAsync("""
            CREATE TABLE nodes (
                id INTEGER PRIMARY KEY,
                label TEXT,
                node_type TEXT
            )
            """);
        await Db.ExecuteAsync("""
            CREATE TABLE edges (
                id INTEGER PRIMARY KEY,
                source_id INTEGER,
                target_id INTEGER,
                edge_type TEXT
            )
            """);
        await Db.ExecuteAsync("CREATE INDEX idx_edges_source ON edges(source_id)");
        await Db.ExecuteAsync("CREATE INDEX idx_edges_target ON edges(target_id)");
    }

    // ── Seed data ──────────────────────────────────────────────────────────

    protected override async Task SeedDataAsync()
    {
        // Add people
        var people = new[]
        {
            (AliceId, "Alice"),   (BobId, "Bob"),      (CharlieId, "Charlie"),
            (DianaId, "Diana"),   (EveId, "Eve"),      (FrankId, "Frank"),
            (GraceId, "Grace"),   (HankId, "Hank"),    (IvyId, "Ivy"),
            (JackId, "Jack")
        };

        var nodeBatch = Db.PrepareInsertBatch("nodes");
        foreach (var (id, name) in people)
            nodeBatch.AddRow(DbValue.FromInteger(id), DbValue.FromText(name), DbValue.FromText("person"));
        await nodeBatch.ExecuteAsync();

        // Add "follows" relationships (directed edges)
        var follows = new[]
        {
            // Alice's follows
            (AliceId, BobId),      // Alice -> Bob
            (AliceId, CharlieId),  // Alice -> Charlie
            (AliceId, DianaId),    // Alice -> Diana
            // Bob's follows
            (BobId, AliceId),      // Bob -> Alice
            (BobId, CharlieId),    // Bob -> Charlie
            (BobId, EveId),        // Bob -> Eve
            // Charlie's follows
            (CharlieId, AliceId),  // Charlie -> Alice
            (CharlieId, FrankId),  // Charlie -> Frank
            // Diana's follows
            (DianaId, AliceId),    // Diana -> Alice
            (DianaId, GraceId),    // Diana -> Grace
            (DianaId, JackId),     // Diana -> Jack
            // Eve's follows
            (EveId, BobId),        // Eve -> Bob
            (EveId, HankId),       // Eve -> Hank
            // Frank's follows
            (FrankId, CharlieId),  // Frank -> Charlie
            (FrankId, GraceId),    // Frank -> Grace
            // Grace's follows
            (GraceId, DianaId),    // Grace -> Diana
            (GraceId, FrankId),    // Grace -> Frank
            (GraceId, JackId),     // Grace -> Jack
            // Hank's follows
            (HankId, EveId),       // Hank -> Eve
            (HankId, IvyId),       // Hank -> Ivy
            // Ivy's follows
            (IvyId, AliceId),      // Ivy -> Alice
            (IvyId, HankId),       // Ivy -> Hank
            // Jack's follows
            (JackId, DianaId),     // Jack -> Diana
            (JackId, GraceId),     // Jack -> Grace
        };

        var edgeBatch = Db.PrepareInsertBatch("edges");
        foreach (var (src, tgt) in follows)
            edgeBatch.AddRow(
                DbValue.FromInteger(_edgeId++),
                DbValue.FromInteger(src),
                DbValue.FromInteger(tgt),
                DbValue.FromText("follows"));
        await edgeBatch.ExecuteAsync();
    }

    // ── Domain-specific commands ───────────────────────────────────────────

    public override IReadOnlyList<CommandInfo> GetCommands() =>
    [
        new("nodes",      "nodes",                      "List all people"),
        new("graph",      "graph",                      "Full adjacency list"),
        new("follows",    "follows <name>",             "Who this person follows"),
        new("followers",  "followers <name>",           "Who follows this person"),
        new("fof",        "fof <name>",                 "Friends-of-friends (2-hop, excluding direct)"),
        new("mutual",     "mutual <name1> <name2>",     "People both follow"),
        new("reciprocal", "reciprocal",                 "All mutual follow-back pairs"),
        new("follow",     "follow <source> <target>",   "Add a follow edge"),
        new("unfollow",   "unfollow <source> <target>", "Remove a follow edge"),
        new("add-person", "add-person <name>",          "Add a new person"),
        new("rename",     "rename <old-name> <new-name>", "Rename a person"),
        new("remove",     "remove <name>",              "Remove person and all edges (in transaction)"),
        new("stats",      "stats",                      "Connection counts per person"),
    ];

    public override async Task<bool> ExecuteCommandAsync(string commandName, string args, TextWriter output)
    {
        switch (commandName)
        {
            case "nodes":
                await CmdNodesAsync(output);
                return true;

            case "graph":
                await CmdGraphAsync(output);
                return true;

            case "follows":
                if (string.IsNullOrWhiteSpace(args))
                {
                    output.WriteLine("Usage: follows <name>");
                    return true;
                }
                await CmdFollowsAsync(args.Trim(), output);
                return true;

            case "followers":
                if (string.IsNullOrWhiteSpace(args))
                {
                    output.WriteLine("Usage: followers <name>");
                    return true;
                }
                await CmdFollowersAsync(args.Trim(), output);
                return true;

            case "fof":
                if (string.IsNullOrWhiteSpace(args))
                {
                    output.WriteLine("Usage: fof <name>");
                    return true;
                }
                await CmdFofAsync(args.Trim(), output);
                return true;

            case "mutual":
            {
                var parts = (args ?? "").Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length < 2)
                {
                    output.WriteLine("Usage: mutual <name1> <name2>");
                    return true;
                }
                await CmdMutualAsync(parts[0], parts[1], output);
                return true;
            }

            case "reciprocal":
                await CmdReciprocalAsync(output);
                return true;

            case "follow":
            {
                var parts = (args ?? "").Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length < 2)
                {
                    output.WriteLine("Usage: follow <source> <target>");
                    return true;
                }
                await CmdFollowAsync(parts[0], parts[1], output);
                return true;
            }

            case "unfollow":
            {
                var parts = (args ?? "").Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length < 2)
                {
                    output.WriteLine("Usage: unfollow <source> <target>");
                    return true;
                }
                await CmdUnfollowAsync(parts[0], parts[1], output);
                return true;
            }

            case "add-person":
                if (string.IsNullOrWhiteSpace(args))
                {
                    output.WriteLine("Usage: add-person <name>");
                    return true;
                }
                await CmdAddPersonAsync(args.Trim(), output);
                return true;

            case "rename":
            {
                var parts = (args ?? "").Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length < 2)
                {
                    output.WriteLine("Usage: rename <old-name> <new-name>");
                    return true;
                }
                await CmdRenameAsync(parts[0], parts[1], output);
                return true;
            }

            case "remove":
                if (string.IsNullOrWhiteSpace(args))
                {
                    output.WriteLine("Usage: remove <name>");
                    return true;
                }
                await CmdRemoveAsync(args.Trim(), output);
                return true;

            case "stats":
                await CmdStatsAsync(output);
                return true;

            default:
                return false;
        }
    }

    // ── Command implementations ────────────────────────────────────────────

    private async Task CmdNodesAsync(TextWriter output)
    {
        await using var result = await Db.ExecuteAsync(
            "SELECT id, label FROM nodes ORDER BY label");
        await foreach (var row in result.GetRowsAsync())
            output.WriteLine($"  {row[0].AsInteger,3}  {row[1].AsText}");
    }

    private async Task CmdGraphAsync(TextWriter output)
    {
        var adjacency = new Dictionary<string, List<string>>();
        await using (var result = await Db.ExecuteAsync("""
            SELECT n1.label, n2.label
            FROM edges e
            JOIN nodes n1 ON e.source_id = n1.id
            JOIN nodes n2 ON e.target_id = n2.id
            WHERE e.edge_type = 'follows'
            ORDER BY n1.label, n2.label
            """))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                var from = row[0].AsText;
                var to = row[1].AsText;
                if (!adjacency.ContainsKey(from))
                    adjacency[from] = [];
                adjacency[from].Add(to);
            }
        }

        foreach (var (person, targets) in adjacency.OrderBy(x => x.Key))
            output.WriteLine($"  {person} -> {string.Join(", ", targets)}");
    }

    private async Task CmdFollowsAsync(string name, TextWriter output)
    {
        var id = await ResolveNodeId(name);
        if (id < 0)
        {
            output.WriteLine($"Person not found: {name}");
            return;
        }

        var names = new List<string>();
        await using (var result = await Db.ExecuteAsync(
            $"SELECT n.label FROM edges e JOIN nodes n ON e.target_id = n.id WHERE e.source_id = {id} AND e.edge_type = 'follows' ORDER BY n.label"))
        {
            await foreach (var row in result.GetRowsAsync())
                names.Add(row[0].AsText);
        }

        if (names.Count == 0)
            output.WriteLine($"  {name} follows nobody.");
        else
            output.WriteLine($"  {string.Join(", ", names)}");
    }

    private async Task CmdFollowersAsync(string name, TextWriter output)
    {
        var id = await ResolveNodeId(name);
        if (id < 0)
        {
            output.WriteLine($"Person not found: {name}");
            return;
        }

        var names = new List<string>();
        await using (var result = await Db.ExecuteAsync(
            $"SELECT n.label FROM edges e JOIN nodes n ON e.source_id = n.id WHERE e.target_id = {id} AND e.edge_type = 'follows' ORDER BY n.label"))
        {
            await foreach (var row in result.GetRowsAsync())
                names.Add(row[0].AsText);
        }

        if (names.Count == 0)
            output.WriteLine($"  Nobody follows {name}.");
        else
            output.WriteLine($"  {string.Join(", ", names)}");
    }

    private async Task CmdFofAsync(string name, TextWriter output)
    {
        var id = await ResolveNodeId(name);
        if (id < 0)
        {
            output.WriteLine($"Person not found: {name}");
            return;
        }

        // Get direct friends (target_ids from source)
        var directFriends = new HashSet<long> { id };
        await using (var result = await Db.ExecuteAsync(
            $"SELECT target_id FROM edges WHERE source_id = {id} AND edge_type = 'follows'"))
        {
            await foreach (var row in result.GetRowsAsync())
                directFriends.Add(row[0].AsInteger);
        }

        // 2-hop traversal
        var fof = new List<string>();
        await using (var result2 = await Db.ExecuteAsync($"""
            SELECT DISTINCT n.id, n.label
            FROM edges e1
            JOIN edges e2 ON e1.target_id = e2.source_id
            JOIN nodes n ON e2.target_id = n.id
            WHERE e1.source_id = {id}
            ORDER BY n.label
            """))
        {
            await foreach (var row in result2.GetRowsAsync())
            {
                var nodeId = row[0].AsInteger;
                if (!directFriends.Contains(nodeId))
                    fof.Add(row[1].AsText);
            }
        }

        if (fof.Count == 0)
            output.WriteLine($"  No friends-of-friends found for {name}.");
        else
            output.WriteLine($"  {string.Join(", ", fof)}");
    }

    private async Task CmdMutualAsync(string name1, string name2, TextWriter output)
    {
        var id1 = await ResolveNodeId(name1);
        if (id1 < 0)
        {
            output.WriteLine($"Person not found: {name1}");
            return;
        }

        var id2 = await ResolveNodeId(name2);
        if (id2 < 0)
        {
            output.WriteLine($"Person not found: {name2}");
            return;
        }

        var mutual = new List<string>();
        await using (var result = await Db.ExecuteAsync($"""
            SELECT n.label
            FROM edges e1
            JOIN edges e2 ON e1.target_id = e2.target_id
            JOIN nodes n ON e1.target_id = n.id
            WHERE e1.source_id = {id1} AND e2.source_id = {id2}
            ORDER BY n.label
            """))
        {
            await foreach (var row in result.GetRowsAsync())
                mutual.Add(row[0].AsText);
        }

        if (mutual.Count == 0)
            output.WriteLine($"  {name1} and {name2} have no mutual follows.");
        else
            output.WriteLine($"  {string.Join(", ", mutual)}");
    }

    private async Task CmdReciprocalAsync(TextWriter output)
    {
        await using var result = await Db.ExecuteAsync("""
            SELECT n1.label, n2.label
            FROM edges e1
            JOIN edges e2 ON e1.source_id = e2.target_id AND e1.target_id = e2.source_id
            JOIN nodes n1 ON e1.source_id = n1.id
            JOIN nodes n2 ON e1.target_id = n2.id
            WHERE e1.source_id < e1.target_id
            ORDER BY n1.label, n2.label
            """);
        var found = false;
        await foreach (var row in result.GetRowsAsync())
        {
            output.WriteLine($"  {row[0].AsText} <-> {row[1].AsText}");
            found = true;
        }

        if (!found)
            output.WriteLine("  No reciprocal follow pairs found.");
    }

    private async Task CmdFollowAsync(string sourceName, string targetName, TextWriter output)
    {
        var sourceId = await ResolveNodeId(sourceName);
        if (sourceId < 0)
        {
            output.WriteLine($"Person not found: {sourceName}");
            return;
        }

        var targetId = await ResolveNodeId(targetName);
        if (targetId < 0)
        {
            output.WriteLine($"Person not found: {targetName}");
            return;
        }

        // Get next edge ID
        long nextEdgeId = 1;
        await using (var result = await Db.ExecuteAsync("SELECT MAX(id) FROM edges"))
        {
            var rows = await result.ToListAsync();
            if (rows.Count > 0 && !rows[0][0].IsNull)
                nextEdgeId = rows[0][0].AsInteger + 1;
        }

        await Db.ExecuteAsync(
            $"INSERT INTO edges VALUES ({nextEdgeId}, {sourceId}, {targetId}, 'follows')");
        output.WriteLine($"  {sourceName} now follows {targetName}.");
    }

    private async Task CmdUnfollowAsync(string sourceName, string targetName, TextWriter output)
    {
        var sourceId = await ResolveNodeId(sourceName);
        if (sourceId < 0)
        {
            output.WriteLine($"Person not found: {sourceName}");
            return;
        }

        var targetId = await ResolveNodeId(targetName);
        if (targetId < 0)
        {
            output.WriteLine($"Person not found: {targetName}");
            return;
        }

        await using var result = await Db.ExecuteAsync(
            $"DELETE FROM edges WHERE source_id = {sourceId} AND target_id = {targetId} AND edge_type = 'follows'");
        if (result.RowsAffected > 0)
            output.WriteLine($"  {sourceName} unfollowed {targetName}.");
        else
            output.WriteLine($"  {sourceName} was not following {targetName}.");
    }

    private async Task CmdAddPersonAsync(string name, TextWriter output)
    {
        // Get next node ID
        long nextNodeId = 1;
        await using (var result = await Db.ExecuteAsync("SELECT MAX(id) FROM nodes"))
        {
            var rows = await result.ToListAsync();
            if (rows.Count > 0 && !rows[0][0].IsNull)
                nextNodeId = rows[0][0].AsInteger + 1;
        }

        await Db.ExecuteAsync(
            $"INSERT INTO nodes VALUES ({nextNodeId}, '{Esc(name)}', 'person')");
        output.WriteLine($"  Added person: {name} (id={nextNodeId}).");
    }

    private async Task CmdRenameAsync(string oldName, string newName, TextWriter output)
    {
        var id = await ResolveNodeId(oldName);
        if (id < 0)
        {
            output.WriteLine($"Person not found: {oldName}");
            return;
        }

        await Db.ExecuteAsync(
            $"UPDATE nodes SET label = '{Esc(newName)}' WHERE label = '{Esc(oldName)}'");
        output.WriteLine($"  Renamed: {oldName} -> {newName}.");
    }

    private async Task CmdRemoveAsync(string name, TextWriter output)
    {
        var id = await ResolveNodeId(name);
        if (id < 0)
        {
            output.WriteLine($"Person not found: {name}");
            return;
        }

        await Db.BeginTransactionAsync();
        try
        {
            // Count edges to report
            long edgeCount;
            await using (var countResult = await Db.ExecuteAsync(
                $"SELECT COUNT(*) FROM edges WHERE source_id = {id} OR target_id = {id}"))
            {
                var countRows = await countResult.ToListAsync();
                edgeCount = countRows[0][0].AsInteger;
            }

            // Delete all edges involving this node
            await Db.ExecuteAsync(
                $"DELETE FROM edges WHERE source_id = {id} OR target_id = {id}");

            // Delete the node
            await Db.ExecuteAsync(
                $"DELETE FROM nodes WHERE id = {id}");

            await Db.CommitAsync();
            output.WriteLine($"  Removed: {name} (node + {edgeCount} edges).");
        }
        catch
        {
            await Db.RollbackAsync();
            throw;
        }
    }

    private async Task CmdStatsAsync(TextWriter output)
    {
        output.WriteLine($"  {"Name",-12} {"Follows",8} {"Followers",10}");
        output.WriteLine($"  {new string('-', 12)} {new string('-', 8)} {new string('-', 10)}");

        var outgoing = new Dictionary<string, long>();
        await using (var result = await Db.ExecuteAsync("""
            SELECT n.label, COUNT(*)
            FROM edges e
            JOIN nodes n ON e.source_id = n.id
            WHERE e.edge_type = 'follows'
            GROUP BY n.label
            """))
        {
            await foreach (var row in result.GetRowsAsync())
                outgoing[row[0].AsText] = row[1].AsInteger;
        }

        var incoming = new Dictionary<string, long>();
        await using (var result2 = await Db.ExecuteAsync("""
            SELECT n.label, COUNT(*)
            FROM edges e
            JOIN nodes n ON e.target_id = n.id
            WHERE e.edge_type = 'follows'
            GROUP BY n.label
            """))
        {
            await foreach (var row in result2.GetRowsAsync())
                incoming[row[0].AsText] = row[1].AsInteger;
        }

        // Include all people, even those with no edges
        var allNames = new HashSet<string>();
        await using (var result3 = await Db.ExecuteAsync("SELECT label FROM nodes ORDER BY label"))
        {
            await foreach (var row in result3.GetRowsAsync())
                allNames.Add(row[0].AsText);
        }

        foreach (var name in allNames.OrderBy(x => x))
        {
            var o = outgoing.GetValueOrDefault(name, 0);
            var i = incoming.GetValueOrDefault(name, 0);
            output.WriteLine($"  {name,-12} {o,8} {i,10}");
        }
    }

    // ── Helper ─────────────────────────────────────────────────────────────

    private async Task<long> ResolveNodeId(string name)
    {
        await using var result = await Db.ExecuteAsync(
            $"SELECT id FROM nodes WHERE label = '{Esc(name)}'");
        var rows = await result.ToListAsync();
        return rows.Count > 0 ? rows[0][0].AsInteger : -1;
    }

    // ── Demo ───────────────────────────────────────────────────────────────

    public override async Task RunDemoAsync(TextWriter output)
    {
        // ── Print network summary ─────────────────────────────────────────
        output.WriteLine("--- Building social network ---");
        output.WriteLine($"  Added 10 people and 23 follow relationships.");
        output.WriteLine();

        // ── Adjacency list ────────────────────────────────────────────────
        output.WriteLine("--- Adjacency list (who follows whom) ---");

        var adjacency = new Dictionary<string, List<string>>();
        await using (var result = await Db.ExecuteAsync("""
            SELECT n1.label, n2.label
            FROM edges e
            JOIN nodes n1 ON e.source_id = n1.id
            JOIN nodes n2 ON e.target_id = n2.id
            ORDER BY n1.label, n2.label
            """))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                var from = row[0].AsText;
                var to = row[1].AsText;
                if (!adjacency.ContainsKey(from))
                    adjacency[from] = [];
                adjacency[from].Add(to);
            }
        }

        foreach (var (person, targets) in adjacency.OrderBy(x => x.Key))
            output.WriteLine($"  {person,-10} -> {string.Join(", ", targets)}");
        output.WriteLine();

        // ── Friends of Alice (outgoing edges) ─────────────────────────────
        output.WriteLine("--- Alice follows ---");

        await using (var result = await Db.ExecuteAsync($"""
            SELECT n.label
            FROM edges e
            JOIN nodes n ON e.target_id = n.id
            WHERE e.source_id = {AliceId}
            ORDER BY n.label
            """))
        {
            var names = new List<string>();
            await foreach (var row in result.GetRowsAsync())
                names.Add(row[0].AsText);
            output.WriteLine($"  {string.Join(", ", names)}");
        }
        output.WriteLine();

        // ── Followers of Alice (incoming edges) ───────────────────────────
        output.WriteLine("--- Alice's followers ---");

        await using (var result = await Db.ExecuteAsync($"""
            SELECT n.label
            FROM edges e
            JOIN nodes n ON e.source_id = n.id
            WHERE e.target_id = {AliceId}
            ORDER BY n.label
            """))
        {
            var names = new List<string>();
            await foreach (var row in result.GetRowsAsync())
                names.Add(row[0].AsText);
            output.WriteLine($"  {string.Join(", ", names)}");
        }
        output.WriteLine();

        // ── Friends-of-friends (2-hop) ────────────────────────────────────
        output.WriteLine("--- Friends-of-friends of Alice (2-hop, excluding direct) ---");

        var directFriends = new HashSet<long> { AliceId };
        await using (var result = await Db.ExecuteAsync(
            $"SELECT target_id FROM edges WHERE source_id = {AliceId}"))
        {
            await foreach (var row in result.GetRowsAsync())
                directFriends.Add(row[0].AsInteger);
        }

        await using (var result2 = await Db.ExecuteAsync($"""
            SELECT DISTINCT n.id, n.label
            FROM edges e1
            JOIN edges e2 ON e1.target_id = e2.source_id
            JOIN nodes n ON e2.target_id = n.id
            WHERE e1.source_id = {AliceId}
            ORDER BY n.label
            """))
        {
            var fof = new List<string>();
            await foreach (var row in result2.GetRowsAsync())
            {
                var nodeId = row[0].AsInteger;
                if (!directFriends.Contains(nodeId))
                    fof.Add(row[1].AsText);
            }
            output.WriteLine($"  {string.Join(", ", fof)}");
        }
        output.WriteLine();

        // ── Mutual follows ────────────────────────────────────────────────
        output.WriteLine("--- Mutual follows: Alice and Bob both follow ---");

        await using (var result = await Db.ExecuteAsync($"""
            SELECT n.label
            FROM edges e1
            JOIN edges e2 ON e1.target_id = e2.target_id
            JOIN nodes n ON e1.target_id = n.id
            WHERE e1.source_id = {AliceId} AND e2.source_id = {BobId}
            ORDER BY n.label
            """))
        {
            var mutual = new List<string>();
            await foreach (var row in result.GetRowsAsync())
                mutual.Add(row[0].AsText);
            output.WriteLine($"  {string.Join(", ", mutual)}");
        }
        output.WriteLine();

        // ── Reciprocal follows (they follow each other) ───────────────────
        output.WriteLine("--- Reciprocal follows (mutual follow-back pairs) ---");

        await using (var result = await Db.ExecuteAsync("""
            SELECT n1.label, n2.label
            FROM edges e1
            JOIN edges e2 ON e1.source_id = e2.target_id AND e1.target_id = e2.source_id
            JOIN nodes n1 ON e1.source_id = n1.id
            JOIN nodes n2 ON e1.target_id = n2.id
            WHERE e1.source_id < e1.target_id
            ORDER BY n1.label, n2.label
            """))
        {
            await foreach (var row in result.GetRowsAsync())
                output.WriteLine($"  {row[0].AsText} <-> {row[1].AsText}");
        }
        output.WriteLine();

        // ── Connection stats ──────────────────────────────────────────────
        output.WriteLine("--- Connection stats ---");
        output.WriteLine($"  {"Name",-12} {"Follows",8} {"Followers",10}");
        output.WriteLine($"  {new string('-', 12)} {new string('-', 8)} {new string('-', 10)}");

        var outgoing = new Dictionary<string, long>();
        await using (var result = await Db.ExecuteAsync("""
            SELECT n.label, COUNT(*)
            FROM edges e
            JOIN nodes n ON e.source_id = n.id
            GROUP BY n.label
            """))
        {
            await foreach (var row in result.GetRowsAsync())
                outgoing[row[0].AsText] = row[1].AsInteger;
        }

        var incoming = new Dictionary<string, long>();
        await using (var result2 = await Db.ExecuteAsync("""
            SELECT n.label, COUNT(*)
            FROM edges e
            JOIN nodes n ON e.target_id = n.id
            GROUP BY n.label
            """))
        {
            await foreach (var row in result2.GetRowsAsync())
                incoming[row[0].AsText] = row[1].AsInteger;
        }

        foreach (var name in outgoing.Keys.Union(incoming.Keys).OrderBy(x => x))
        {
            var o = outgoing.GetValueOrDefault(name, 0);
            var i = incoming.GetValueOrDefault(name, 0);
            output.WriteLine($"  {name,-12} {o,8} {i,10}");
        }
        output.WriteLine();

        // ── Network mutations ─────────────────────────────────────────────
        output.WriteLine("--- Network mutations ---");

        // Unfollow: Alice unfollows Bob
        await Db.ExecuteAsync(
            $"DELETE FROM edges WHERE source_id = {AliceId} AND target_id = {BobId} AND edge_type = 'follows'");
        output.WriteLine("  Alice unfollowed Bob.");

        // New follow: Eve follows Alice
        await Db.ExecuteAsync(
            $"INSERT INTO edges VALUES ({_edgeId++}, {EveId}, {AliceId}, 'follows')");
        output.WriteLine("  Eve now follows Alice.");

        // Update profile: rename Hank to Henry
        await Db.ExecuteAsync(
            $"UPDATE nodes SET label = 'Henry' WHERE id = {HankId}");
        output.WriteLine("  Renamed: Hank -> Henry.");

        // Add a new edge type: Charlie blocks Frank
        await Db.ExecuteAsync(
            $"INSERT INTO edges VALUES ({_edgeId++}, {CharlieId}, {FrankId}, 'blocks')");
        output.WriteLine("  Charlie blocks Frank (new edge type).");

        // Remove user: delete Jack and all edges in a transaction
        await Db.BeginTransactionAsync();
        try
        {
            long removedEdges;
            await using (var countResult = await Db.ExecuteAsync(
                $"SELECT COUNT(*) FROM edges WHERE source_id = {JackId} OR target_id = {JackId}"))
            {
                var countRows = await countResult.ToListAsync();
                removedEdges = countRows[0][0].AsInteger;
            }

            await Db.ExecuteAsync(
                $"DELETE FROM edges WHERE source_id = {JackId} OR target_id = {JackId}");
            await Db.ExecuteAsync(
                $"DELETE FROM nodes WHERE id = {JackId}");
            await Db.CommitAsync();
            output.WriteLine($"  Removed: Jack (node + {removedEdges} edges).");
        }
        catch
        {
            await Db.RollbackAsync();
            throw;
        }

        output.WriteLine();

        // ── Updated adjacency list ────────────────────────────────────────
        output.WriteLine("--- Adjacency list (after mutations) ---");

        var updatedAdjacency = new Dictionary<string, List<string>>();
        await using (var result = await Db.ExecuteAsync("""
            SELECT n1.label, n2.label
            FROM edges e
            JOIN nodes n1 ON e.source_id = n1.id
            JOIN nodes n2 ON e.target_id = n2.id
            WHERE e.edge_type = 'follows'
            ORDER BY n1.label, n2.label
            """))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                var from = row[0].AsText;
                var to = row[1].AsText;
                if (!updatedAdjacency.ContainsKey(from))
                    updatedAdjacency[from] = [];
                updatedAdjacency[from].Add(to);
            }
        }

        foreach (var (person, targets) in updatedAdjacency.OrderBy(x => x.Key))
            output.WriteLine($"  {person,-10} -> {string.Join(", ", targets)}");

        // Show blocks
        await using (var blockResult = await Db.ExecuteAsync("""
            SELECT n1.label, n2.label
            FROM edges e
            JOIN nodes n1 ON e.source_id = n1.id
            JOIN nodes n2 ON e.target_id = n2.id
            WHERE e.edge_type = 'blocks'
            """))
        {
            await foreach (var row in blockResult.GetRowsAsync())
                output.WriteLine($"  {row[0].AsText,-10} blocks {row[1].AsText}");
        }
        output.WriteLine();

        // ── Network summary ───────────────────────────────────────────────
        output.WriteLine("--- Network summary ---");

        await using (var result = await Db.ExecuteAsync("SELECT COUNT(*) FROM nodes"))
        {
            var rows = await result.ToListAsync();
            output.WriteLine($"  Nodes: {rows[0][0].AsInteger}");
        }

        await using (var result2 = await Db.ExecuteAsync("SELECT COUNT(*) FROM edges"))
        {
            var rows = await result2.ToListAsync();
            var edgeCount = rows[0][0].AsInteger;
            output.WriteLine($"  Edges: {edgeCount}");
            output.WriteLine($"  Avg outgoing: {(double)edgeCount / 10:F1}");
        }
    }
}
