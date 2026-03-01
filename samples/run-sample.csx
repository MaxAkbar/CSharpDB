// dotnet-script runner: dotnet script run-sample.csx -- ecommerce-store.sql
// Or just use this logic inline

using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

var file = Args.Count > 0 ? Args[0] : "ecommerce-store.sql";
var baseUrl = "http://localhost:61818";

var content = File.ReadAllText(file);

// Remove comment lines
var lines = content.Split('\n')
    .Where(l => !l.TrimStart().StartsWith("--") && l.Trim().Length > 0)
    .ToList();

var joined = string.Join("\n", lines);

// Split into statements, respecting BEGIN...END blocks
var statements = new List<string>();
var current = new StringBuilder();
bool inTrigger = false;

foreach (var line in lines)
{
    var trimmed = line.Trim();
    if (trimmed.Contains("CREATE TRIGGER", StringComparison.OrdinalIgnoreCase))
        inTrigger = true;

    current.AppendLine(line);

    if (inTrigger && trimmed.Equals("END;", StringComparison.OrdinalIgnoreCase))
    {
        statements.Add(current.ToString().Trim());
        current.Clear();
        inTrigger = false;
    }
    else if (!inTrigger && trimmed.EndsWith(';'))
    {
        statements.Add(current.ToString().Trim());
        current.Clear();
    }
}

if (current.Length > 0)
    statements.Add(current.ToString().Trim());

using var http = new HttpClient();
int ok = 0, fail = 0;

foreach (var stmt in statements)
{
    if (string.IsNullOrWhiteSpace(stmt)) continue;

    var json = JsonSerializer.Serialize(new { sql = stmt });
    var resp = await http.PostAsync($"{baseUrl}/api/sql/execute",
        new StringContent(json, Encoding.UTF8, "application/json"));
    var body = await resp.Content.ReadAsStringAsync();

    if (resp.IsSuccessStatusCode && !body.Contains("\"error\""))
    {
        ok++;
    }
    else
    {
        var preview = stmt.Length > 70 ? stmt[..70] + "..." : stmt;
        Console.WriteLine($"FAIL: {preview}");
        Console.WriteLine($"  -> {body}");
        fail++;
    }
}

Console.WriteLine($"\n{file}: {ok} passed, {fail} failed out of {ok + fail} statements");
