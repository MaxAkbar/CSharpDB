using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.Sql;

namespace CSharpDB.Client.Internal;

internal sealed partial class EngineTransportClient
{
    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private static IReadOnlyList<ProcedureParameterDefinition> NormalizeProcedureParameters(IReadOnlyList<ProcedureParameterDefinition> parameters)
    {
        ArgumentNullException.ThrowIfNull(parameters);

        var normalized = new List<ProcedureParameterDefinition>(parameters.Count);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var parameter in parameters)
        {
            if (parameter is null)
                throw new ArgumentException("Procedure parameter entry cannot be null.");

            string name = NormalizeProcedureParameterName(parameter.Name);
            if (!seen.Add(name))
                throw new ArgumentException($"Duplicate parameter '{name}' in procedure definition.");

            object? normalizedDefault = parameter.Default is JsonElement element
                ? ConvertJsonElement(element)
                : parameter.Default;

            normalized.Add(new ProcedureParameterDefinition
            {
                Name = name,
                Type = parameter.Type,
                Required = parameter.Required,
                Default = normalizedDefault,
                Description = string.IsNullOrWhiteSpace(parameter.Description) ? null : parameter.Description.Trim(),
            });
        }

        return normalized;
    }

    private static void ValidateProcedureBodyReferences(string bodySql, IReadOnlyList<ProcedureParameterDefinition> parameters)
    {
        var defined = new HashSet<string>(parameters.Select(p => p.Name), StringComparer.OrdinalIgnoreCase);
        foreach (string bodyParameter in ExtractParameterNamesFromSql(bodySql))
        {
            if (!defined.Contains(bodyParameter))
            {
                throw new ArgumentException(
                    $"Procedure SQL references parameter '@{bodyParameter}' but it is missing from params metadata.");
            }
        }
    }

    private static Dictionary<string, object?> BindProcedureArguments(ProcedureDefinition procedure, IReadOnlyDictionary<string, object?> args)
    {
        var incoming = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var (rawName, rawValue) in args)
        {
            string normalized = NormalizeProcedureParameterName(rawName);
            incoming[normalized] = rawValue is JsonElement element ? ConvertJsonElement(element) : rawValue;
        }

        var known = new HashSet<string>(procedure.Parameters.Select(p => p.Name), StringComparer.OrdinalIgnoreCase);
        foreach (string provided in incoming.Keys)
        {
            if (!known.Contains(provided))
                throw new ArgumentException($"Unknown argument '{provided}'.");
        }

        var bound = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var parameter in procedure.Parameters)
        {
            if (incoming.TryGetValue(parameter.Name, out var providedValue))
            {
                bound[parameter.Name] = providedValue is null
                    ? parameter.Default is not null
                        ? CoerceProcedureParameterValue(parameter.Name, parameter.Type, parameter.Default)
                        : parameter.Required
                            ? throw new ArgumentException($"Required parameter '{parameter.Name}' cannot be null.")
                            : null
                    : CoerceProcedureParameterValue(parameter.Name, parameter.Type, providedValue);
                continue;
            }

            if (parameter.Default is not null)
            {
                bound[parameter.Name] = CoerceProcedureParameterValue(parameter.Name, parameter.Type, parameter.Default);
                continue;
            }

            if (parameter.Required)
                throw new ArgumentException($"Missing required parameter '{parameter.Name}'.");

            bound[parameter.Name] = null;
        }

        return bound;
    }

    private static async Task<ProcedureStatementExecutionResult> ExecuteSingleStatementWithArgumentsAsync(
        Database db,
        int statementIndex,
        string sql,
        IReadOnlyDictionary<string, object?> args,
        CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        string resolvedSql = SubstituteSqlParameters(sql, args);
        var result = await ExecuteQueryAsync(db, resolvedSql, ct);
        stopwatch.Stop();

        return new ProcedureStatementExecutionResult
        {
            StatementIndex = statementIndex,
            StatementText = sql,
            IsQuery = result.IsQuery,
            ColumnNames = result.ColumnNames,
            Rows = result.Rows,
            RowsAffected = result.RowsAffected,
            Elapsed = stopwatch.Elapsed,
        };
    }

    private static string SubstituteSqlParameters(string sql, IReadOnlyDictionary<string, object?> args)
    {
        var tokens = new Tokenizer(sql).Tokenize();
        var builder = new StringBuilder(sql.Length);
        int cursor = 0;

        foreach (var token in tokens)
        {
            if (token.Type != TokenType.Parameter)
                continue;

            builder.Append(sql, cursor, token.Position - cursor);

            if (!args.TryGetValue(token.Value, out object? value))
                throw new ArgumentException($"Missing argument for SQL parameter '@{token.Value}'.");

            builder.Append(FormatSqlLiteral(NormalizeParameterValue(value)));
            cursor = token.Position + token.Value.Length + 1;
        }

        builder.Append(sql, cursor, sql.Length - cursor);
        return builder.ToString();
    }

    private static string SerializeProcedureParameters(IReadOnlyList<ProcedureParameterDefinition> parameters)
    {
        var storage = parameters.Select(parameter => new ProcedureParameterStorage
        {
            Name = parameter.Name,
            Type = parameter.Type.ToString().ToUpperInvariant(),
            Required = parameter.Required,
            Default = parameter.Type == DbType.Blob && parameter.Default is byte[] bytes
                ? Convert.ToBase64String(bytes)
                : parameter.Default,
            Description = parameter.Description,
        });

        return JsonSerializer.Serialize(storage, s_jsonOptions);
    }

    private static IReadOnlyList<ProcedureParameterDefinition> DeserializeProcedureParameters(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return [];

        List<ProcedureParameterStorage>? storage;
        try
        {
            storage = JsonSerializer.Deserialize<List<ProcedureParameterStorage>>(json, s_jsonOptions);
        }
        catch (JsonException ex)
        {
            throw new ArgumentException($"Invalid procedure params_json payload: {ex.Message}");
        }

        if (storage is null || storage.Count == 0)
            return [];

        return NormalizeProcedureParameters(storage
            .Where(item => item is not null)
            .Select(item => new ProcedureParameterDefinition
            {
                Name = NormalizeProcedureParameterName(item!.Name ?? string.Empty),
                Type = Enum.TryParse<DbType>(item.Type, ignoreCase: true, out var parsedType)
                    ? parsedType
                    : throw new ArgumentException($"Unsupported parameter type '{item.Type}' in params_json."),
                Required = item.Required,
                Default = item.Default is JsonElement element ? ConvertJsonElement(element) : item.Default,
                Description = string.IsNullOrWhiteSpace(item.Description) ? null : item.Description.Trim(),
            })
            .ToList());
    }

    private static string NormalizeProcedureParameterName(string rawName)
    {
        if (string.IsNullOrWhiteSpace(rawName))
            throw new ArgumentException("Procedure parameter name is required.");

        string trimmed = rawName.Trim();
        if (trimmed.StartsWith('@'))
            trimmed = trimmed[1..];

        ValidateIdentifier(trimmed, "procedure parameter name");
        return trimmed;
    }

    private static HashSet<string> ExtractParameterNamesFromSql(string sql)
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var token in new Tokenizer(sql).Tokenize())
        {
            if (token.Type == TokenType.Parameter)
                names.Add(token.Value);
        }

        return names;
    }

    private static string NormalizeSavedQueryName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("saved query name is required.");

        string trimmed = name.Trim();
        if (trimmed.Length > 256)
            throw new ArgumentException("saved query name cannot exceed 256 characters.");
        if (trimmed.Any(char.IsControl))
            throw new ArgumentException("saved query name cannot contain control characters.");

        return trimmed;
    }

    private static string NormalizeSqlFragment(string sql, string label)
    {
        if (string.IsNullOrWhiteSpace(sql))
            throw new ArgumentException($"{label} is required.");

        string trimmed = sql.Trim();
        if (trimmed.EndsWith(';'))
            trimmed = trimmed.TrimEnd(';').TrimEnd();
        if (trimmed.Length == 0)
            throw new ArgumentException($"{label} is required.");

        return trimmed;
    }

    private static DateTime ParseStoredUtc(string rawValue, DateTime fallback)
    {
        return DateTime.TryParse(
            rawValue,
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
            out var parsed)
            ? parsed
            : fallback;
    }

    private static object? NormalizeParameterValue(object? value)
        => value is JsonElement element ? ConvertJsonElement(element) : value;

    private static object? ConvertJsonElement(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.Null => null,
        JsonValueKind.String => element.GetString(),
        JsonValueKind.Number => element.TryGetInt64(out long integer) ? integer : element.GetDouble(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        _ => element.ToString(),
    };

    private static object? CoerceProcedureParameterValue(string name, DbType type, object? value)
    {
        if (value is null)
            return null;

        object? normalized = NormalizeParameterValue(value);
        return type switch
        {
            DbType.Integer when TryCoerceInteger(normalized, out long integerValue) => integerValue,
            DbType.Real when TryCoerceReal(normalized, out double realValue) => realValue,
            DbType.Text when normalized is string textValue => textValue,
            DbType.Blob when normalized is byte[] blob => blob,
            DbType.Blob when normalized is string base64 => Convert.FromBase64String(base64),
            DbType.Integer => throw new ArgumentException($"Parameter '{name}' expects INTEGER."),
            DbType.Real => throw new ArgumentException($"Parameter '{name}' expects REAL."),
            DbType.Text => throw new ArgumentException($"Parameter '{name}' expects TEXT."),
            DbType.Blob => throw new ArgumentException($"Parameter '{name}' expects BLOB."),
            _ => throw new ArgumentException($"Unsupported parameter type '{type}' for '{name}'."),
        };
    }

    private static bool TryCoerceInteger(object? value, out long result)
    {
        switch (value)
        {
            case long l: result = l; return true;
            case int i: result = i; return true;
            case short s: result = s; return true;
            case byte b: result = b; return true;
            case sbyte sb: result = sb; return true;
            case uint ui: result = ui; return true;
            case ulong ul when ul <= long.MaxValue: result = (long)ul; return true;
            case double d when IsWholeNumberInRange(d): result = (long)d; return true;
            case float f when IsWholeNumberInRange(f): result = (long)f; return true;
            case decimal m when m >= long.MinValue && m <= long.MaxValue && decimal.Truncate(m) == m: result = (long)m; return true;
            case string text when long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed): result = parsed; return true;
            default: result = 0; return false;
        }
    }

    private static bool TryCoerceReal(object? value, out double result)
    {
        switch (value)
        {
            case double d: result = d; return true;
            case float f: result = f; return true;
            case decimal m: result = (double)m; return true;
            case long l: result = l; return true;
            case int i: result = i; return true;
            case short s: result = s; return true;
            case byte b: result = b; return true;
            case sbyte sb: result = sb; return true;
            case uint ui: result = ui; return true;
            case ulong ul: result = ul; return true;
            case string text when double.TryParse(text, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out double parsed): result = parsed; return true;
            default: result = 0; return false;
        }
    }

    private static bool IsWholeNumberInRange(double value)
        => !double.IsNaN(value)
           && !double.IsInfinity(value)
           && value >= long.MinValue
           && value <= long.MaxValue
           && Math.Truncate(value) == value;

    private static bool IsWholeNumberInRange(float value)
        => !float.IsNaN(value)
           && !float.IsInfinity(value)
           && value >= long.MinValue
           && value <= long.MaxValue
           && MathF.Truncate(value) == value;

    private sealed class ProcedureParameterStorage
    {
        public string? Name { get; init; }
        public string? Type { get; init; }
        public bool Required { get; init; }
        public object? Default { get; init; }
        public string? Description { get; init; }
    }
}
