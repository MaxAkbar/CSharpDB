using System.Text.Json;

namespace CSharpDB.DataGen.Specs;

public static class DatasetSpecLoader
{
    private static readonly JsonSerializerOptions s_options = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
    };

    public static LoadedDatasetSpec Load(DataGenOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        string path = ResolveSpecPath(options);
        DatasetSpec? spec = JsonSerializer.Deserialize<DatasetSpec>(File.ReadAllText(path), s_options);
        if (spec is null)
            throw new InvalidOperationException($"Failed to deserialize dataset spec '{path}'.");

        Validate(spec, options.DatasetLabel, path);
        return new LoadedDatasetSpec(path, spec);
    }

    private static string ResolveSpecPath(DataGenOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.SpecPath))
            return Path.GetFullPath(options.SpecPath);

        string fileName = $"{options.DatasetLabel}.dataset.json";
        string[] candidates =
        [
            Path.Combine(AppContext.BaseDirectory, "Specs", fileName),
            Path.Combine(Environment.CurrentDirectory, "tests", "CSharpDB.DataGen", "Specs", fileName),
            Path.Combine(Environment.CurrentDirectory, "Specs", fileName),
        ];

        foreach (string candidate in candidates)
        {
            if (File.Exists(candidate))
                return Path.GetFullPath(candidate);
        }

        throw new FileNotFoundException($"Could not locate dataset spec '{fileName}'.", fileName);
    }

    private static void Validate(DatasetSpec spec, string expectedDataset, string path)
    {
        if (!string.Equals(spec.Dataset, expectedDataset, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Dataset spec '{path}' targets '{spec.Dataset}', but the selected dataset is '{expectedDataset}'.");
        }

        var generatorKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (SqlTableSpec table in spec.Tables)
        {
            if (string.IsNullOrWhiteSpace(table.GeneratorKey))
                throw new InvalidOperationException($"SQL table spec in '{path}' is missing 'generatorKey'.");

            if (!generatorKeys.Add("table:" + table.GeneratorKey))
                throw new InvalidOperationException($"Duplicate SQL table generatorKey '{table.GeneratorKey}' in '{path}'.");

            if (string.IsNullOrWhiteSpace(table.Name))
                throw new InvalidOperationException($"SQL table spec '{table.GeneratorKey}' in '{path}' is missing 'name'.");

            if (string.IsNullOrWhiteSpace(table.OutputFileName))
                throw new InvalidOperationException($"SQL table spec '{table.GeneratorKey}' in '{path}' is missing 'outputFileName'.");

            if (table.RowCount.ValueKind == JsonValueKind.Undefined)
                throw new InvalidOperationException($"SQL table spec '{table.GeneratorKey}' in '{path}' is missing 'rowCount'.");

            if (table.Columns.Count == 0)
                throw new InvalidOperationException($"SQL table spec '{table.GeneratorKey}' in '{path}' has no columns.");

            ValidateBindings(table.Locals, $"SQL table spec '{table.GeneratorKey}'", path);
        }

        foreach (CollectionSpec collection in spec.Collections)
        {
            if (string.IsNullOrWhiteSpace(collection.GeneratorKey))
                throw new InvalidOperationException($"Collection spec in '{path}' is missing 'generatorKey'.");

            if (!generatorKeys.Add("collection:" + collection.GeneratorKey))
                throw new InvalidOperationException($"Duplicate collection generatorKey '{collection.GeneratorKey}' in '{path}'.");

            if (string.IsNullOrWhiteSpace(collection.Name))
                throw new InvalidOperationException($"Collection spec '{collection.GeneratorKey}' in '{path}' is missing 'name'.");

            if (string.IsNullOrWhiteSpace(collection.OutputFileName))
                throw new InvalidOperationException($"Collection spec '{collection.GeneratorKey}' in '{path}' is missing 'outputFileName'.");

            if (collection.RowCount.ValueKind == JsonValueKind.Undefined)
                throw new InvalidOperationException($"Collection spec '{collection.GeneratorKey}' in '{path}' is missing 'rowCount'.");

            if (collection.Key.ValueKind == JsonValueKind.Undefined)
                throw new InvalidOperationException($"Collection spec '{collection.GeneratorKey}' in '{path}' is missing 'key'.");

            if (collection.Document.ValueKind == JsonValueKind.Undefined)
                throw new InvalidOperationException($"Collection spec '{collection.GeneratorKey}' in '{path}' is missing 'document'.");

            ValidateBindings(collection.Locals, $"Collection spec '{collection.GeneratorKey}'", path);
        }
    }

    private static void ValidateBindings(
        IReadOnlyList<RuleBindingSpec> bindings,
        string owner,
        string path)
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (RuleBindingSpec binding in bindings)
        {
            if (string.IsNullOrWhiteSpace(binding.Name))
                throw new InvalidOperationException($"{owner} in '{path}' has a local binding with no name.");

            if (!names.Add(binding.Name))
                throw new InvalidOperationException($"{owner} in '{path}' has duplicate local binding '{binding.Name}'.");

            if (binding.Value.ValueKind == JsonValueKind.Undefined)
                throw new InvalidOperationException($"{owner} in '{path}' has local binding '{binding.Name}' with no value.");
        }
    }
}
