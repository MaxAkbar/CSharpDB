// dotnet-script runner: dotnet script samples/run-sample.csx -- samples/ecommerce-store.sql
// Optional second arg: explicit procedure definition file (.procedures.json)
#nullable enable

using System.Net.Http;
using System.Net;
using System.Text;
using System.Text.Json;

var sqlFile = Args.Count > 0 ? Args[0] : "samples/ecommerce-store.sql";
var sqlPath = Path.GetFullPath(sqlFile);
var procedureFile = Args.Count > 1
    ? Args[1]
    : Path.Combine(Path.GetDirectoryName(sqlPath)!, $"{Path.GetFileNameWithoutExtension(sqlPath)}.procedures.json");
var procedurePath = Path.GetFullPath(procedureFile);
var baseUrl = Environment.GetEnvironmentVariable("CSHARPDB_API_BASEURL") ?? "http://localhost:61818";

if (!File.Exists(sqlPath))
{
    Console.WriteLine($"SQL file not found: {sqlPath}");
    return;
}

var content = File.ReadAllText(sqlPath);

// Remove comment lines
var lines = content.Split('\n')
    .Where(l => !l.TrimStart().StartsWith("--") && l.Trim().Length > 0)
    .ToList();

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

var http = new HttpClient();
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

Console.WriteLine($"\n{Path.GetFileName(sqlPath)}: {ok} passed, {fail} failed out of {ok + fail} statements");

if (!File.Exists(procedurePath))
{
    Console.WriteLine($"No procedure catalog file found at {procedurePath}. Skipping procedure import.");
    return;
}

var options = new JsonSerializerOptions
{
    PropertyNameCaseInsensitive = true,
};

var procedures = JsonSerializer.Deserialize<List<ProcedureDefinitionInput>>(File.ReadAllText(procedurePath), options)
    ?? new List<ProcedureDefinitionInput>();

if (procedures.Count == 0)
{
    Console.WriteLine($"Procedure catalog file is empty: {procedurePath}");
    return;
}

int created = 0, updated = 0, procFail = 0;
foreach (var procedure in procedures)
{
    var createPayload = JsonSerializer.Serialize(new
    {
        name = procedure.Name,
        bodySql = procedure.BodySql,
        parameters = procedure.Parameters,
        description = procedure.Description,
        isEnabled = procedure.IsEnabled,
    });

    var createResp = await http.PostAsync(
        $"{baseUrl}/api/procedures",
        new StringContent(createPayload, Encoding.UTF8, "application/json"));

    if (createResp.IsSuccessStatusCode)
    {
        created++;
        continue;
    }

    if (createResp.StatusCode != HttpStatusCode.Conflict)
    {
        var error = await createResp.Content.ReadAsStringAsync();
        Console.WriteLine($"FAIL create procedure '{procedure.Name}' -> {error}");
        procFail++;
        continue;
    }

    var updatePayload = JsonSerializer.Serialize(new
    {
        newName = procedure.Name,
        bodySql = procedure.BodySql,
        parameters = procedure.Parameters,
        description = procedure.Description,
        isEnabled = procedure.IsEnabled,
    });

    var updateResp = await http.PutAsync(
        $"{baseUrl}/api/procedures/{Uri.EscapeDataString(procedure.Name)}",
        new StringContent(updatePayload, Encoding.UTF8, "application/json"));

    if (updateResp.IsSuccessStatusCode)
    {
        updated++;
        continue;
    }

    var updateError = await updateResp.Content.ReadAsStringAsync();
    Console.WriteLine($"FAIL update procedure '{procedure.Name}' -> {updateError}");
    procFail++;
}

Console.WriteLine($"Procedure import ({Path.GetFileName(procedurePath)}): {created} created, {updated} updated, {procFail} failed");

public sealed class ProcedureDefinitionInput
{
    public string Name { get; set; } = string.Empty;
    public string BodySql { get; set; } = string.Empty;
    public List<ProcedureParameterInput>? Parameters { get; set; }
    public string? Description { get; set; }
    public bool IsEnabled { get; set; } = true;
}

public sealed class ProcedureParameterInput
{
    public string Name { get; set; } = string.Empty;
    public string Type { get; set; } = "TEXT";
    public bool Required { get; set; } = true;
    public object? Default { get; set; }
    public string? Description { get; set; }
}
