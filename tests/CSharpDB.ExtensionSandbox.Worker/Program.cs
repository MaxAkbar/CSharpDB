using System.Text.Json;

int exitCode = 0;
string? line;
while ((line = await Console.In.ReadLineAsync()) is not null)
{
    if (string.IsNullOrWhiteSpace(line))
        continue;

    WorkerRequest? request;
    try
    {
        request = JsonSerializer.Deserialize<WorkerRequest>(
            line,
            WorkerJson.Options);
    }
    catch (Exception ex)
    {
        await Console.Error.WriteLineAsync(ex.Message);
        return 2;
    }

    if (request is null)
        return 2;

    try
    {
        WorkerResponse response = request.Name switch
        {
            "Echo" => Echo(request),
            "Sleep" => await SleepAsync(request),
            "AllocateMemory" => await AllocateMemoryAsync(request),
            "Crash" => Crash(),
            _ => new WorkerResponse(
                Succeeded: false,
                Message: $"Unknown command '{request.Name}'.",
                ErrorCode: "UnknownCommand"),
        };

        await Console.Out.WriteLineAsync(JsonSerializer.Serialize(response, WorkerJson.Options));
        exitCode = response.Succeeded ? 0 : 3;
    }
    catch (Exception ex)
    {
        await Console.Error.WriteLineAsync(ex.ToString());
        return 1;
    }
}

return exitCode;

static WorkerResponse Echo(WorkerRequest request)
{
    string? message = request.Arguments is not null &&
        request.Arguments.TryGetValue("message", out JsonElement value) &&
        value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;

    return new WorkerResponse(
        Succeeded: true,
        Message: "Echo completed.",
        Value: JsonSerializer.SerializeToElement(message, WorkerJson.Options));
}

static async Task<WorkerResponse> SleepAsync(WorkerRequest request)
{
    int delayMs = 250;
    if (request.Arguments is not null &&
        request.Arguments.TryGetValue("delayMs", out JsonElement value) &&
        value.ValueKind == JsonValueKind.Number &&
        value.TryGetInt32(out int parsedDelayMs))
    {
        delayMs = parsedDelayMs;
    }

    await Task.Delay(delayMs);
    return new WorkerResponse(
        Succeeded: true,
        Message: $"Slept for {delayMs}ms.",
        Value: JsonSerializer.SerializeToElement(delayMs, WorkerJson.Options));
}

static async Task<WorkerResponse> AllocateMemoryAsync(WorkerRequest request)
{
    int megabytes = ReadInt32Argument(request, "megabytes", 64);
    int holdMs = ReadInt32Argument(request, "holdMs", 250);
    if (megabytes <= 0)
    {
        return new WorkerResponse(
            Succeeded: false,
            Message: "megabytes must be greater than zero.",
            ErrorCode: "InvalidArgument");
    }

    byte[] buffer = GC.AllocateUninitializedArray<byte>(checked(megabytes * 1024 * 1024));
    for (int i = 0; i < buffer.Length; i += 4096)
        buffer[i] = 1;

    await Task.Delay(Math.Max(holdMs, 0));
    return new WorkerResponse(
        Succeeded: true,
        Message: $"Allocated {megabytes}MB.",
        Value: JsonSerializer.SerializeToElement(megabytes, WorkerJson.Options));
}

static WorkerResponse Crash()
{
    Environment.Exit(42);
    return new WorkerResponse(false, "unreachable", ErrorCode: "Unreachable");
}

static int ReadInt32Argument(WorkerRequest request, string name, int defaultValue)
{
    if (request.Arguments is not null &&
        request.Arguments.TryGetValue(name, out JsonElement value) &&
        value.ValueKind == JsonValueKind.Number &&
        value.TryGetInt32(out int parsedValue))
    {
        return parsedValue;
    }

    return defaultValue;
}

internal sealed record WorkerRequest(
    string Kind,
    string Name,
    Dictionary<string, JsonElement>? Arguments,
    Dictionary<string, string>? Metadata);

internal sealed record WorkerResponse(
    bool Succeeded,
    string? Message = null,
    JsonElement? Value = null,
    string? ErrorCode = null);

internal static class WorkerJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
}
