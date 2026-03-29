using System.Collections;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Bogus;
using CSharpDB.DataGen.Specs;

namespace CSharpDB.DataGen.Generators;

public static class SpecDataGenerator
{
    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        WriteIndented = false,
    };

    private static readonly int[] s_defaultDocumentSizeBuckets = [256, 1024, 4096, 16 * 1024];

    public static DatasetGenerationPlan CreatePlan(DataGenOptions options, DatasetSpec spec)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(spec);

        Dictionary<string, long> counts = ResolveCounts(options, spec);
        var sqlSources = new Dictionary<string, GeneratedSqlTableSource>(StringComparer.OrdinalIgnoreCase);
        var collectionSources = new Dictionary<string, GeneratedCollectionSource>(StringComparer.OrdinalIgnoreCase);

        foreach (SqlTableSpec table in spec.Tables)
        {
            long rowCount = GetRequiredCount(counts, table.GeneratorKey);
            sqlSources[table.GeneratorKey] = new GeneratedSqlTableSource(
                table.GeneratorKey,
                rowCount,
                () => GenerateRows(options, counts, table, rowCount));
        }

        foreach (CollectionSpec collection in spec.Collections)
        {
            long rowCount = GetRequiredCount(counts, collection.GeneratorKey);
            collectionSources[collection.GeneratorKey] = new GeneratedCollectionSource(
                collection.GeneratorKey,
                rowCount,
                () => GenerateDocuments(options, counts, collection, rowCount));
        }

        return new DatasetGenerationPlan(counts, sqlSources, collectionSources);
    }

    private static Dictionary<string, long> ResolveCounts(DataGenOptions options, DatasetSpec spec)
    {
        var counts = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        var planContext = RuleExecutionContext.CreatePlanningContext(options, counts);

        foreach (SqlTableSpec table in spec.Tables)
        {
            long rowCount = RuleEvaluator.EvaluateInt64(table.RowCount, planContext, $"{table.GeneratorKey}.rowCount");
            RegisterCount(counts, table.GeneratorKey, table.Name, rowCount);
        }

        foreach (CollectionSpec collection in spec.Collections)
        {
            long rowCount = RuleEvaluator.EvaluateInt64(collection.RowCount, planContext, $"{collection.GeneratorKey}.rowCount");
            RegisterCount(counts, collection.GeneratorKey, collection.Name, rowCount);
        }

        return counts;
    }

    private static void RegisterCount(
        Dictionary<string, long> counts,
        string generatorKey,
        string objectName,
        long rowCount)
    {
        if (rowCount < 0)
            throw new InvalidOperationException($"Resolved row count for '{generatorKey}' cannot be negative.");

        counts[generatorKey] = rowCount;
        if (!string.IsNullOrWhiteSpace(objectName) && !counts.ContainsKey(objectName))
            counts[objectName] = rowCount;
    }

    private static IEnumerable<IReadOnlyDictionary<string, object?>> GenerateRows(
        DataGenOptions options,
        IReadOnlyDictionary<string, long> counts,
        SqlTableSpec table,
        long rowCount)
    {
        for (long rowIndex = 1; rowIndex <= rowCount; rowIndex++)
        {
            var context = RuleExecutionContext.CreateRowContext(options, counts, table.GeneratorKey, rowIndex);
            PopulateBindings(table.Locals, context, $"{table.GeneratorKey}.locals");

            foreach (SqlColumnSpec column in table.Columns)
            {
                string sourceField = GetSourceField(column);
                if (column.Generator.ValueKind != JsonValueKind.Undefined)
                {
                    object? generated = RuleEvaluator.Evaluate(column.Generator, context, $"{table.GeneratorKey}.columns.{column.Name}");
                    context.SetValue(sourceField, generated);
                    if (!string.Equals(sourceField, column.Name, StringComparison.OrdinalIgnoreCase))
                        context.SetValue(column.Name, generated);
                }
                else if (!context.TryResolveValue(sourceField, out object? existingValue))
                {
                    throw new InvalidOperationException(
                        $"Column '{column.Name}' on table '{table.GeneratorKey}' has no generator and no bound value for '{sourceField}'.");
                }
                else if (!string.Equals(sourceField, column.Name, StringComparison.OrdinalIgnoreCase))
                {
                    context.SetValue(column.Name, existingValue);
                }
            }

            yield return context.SnapshotValues();
        }
    }

    private static IEnumerable<GeneratedCollectionDocument> GenerateDocuments(
        DataGenOptions options,
        IReadOnlyDictionary<string, long> counts,
        CollectionSpec collection,
        long rowCount)
    {
        for (long rowIndex = 1; rowIndex <= rowCount; rowIndex++)
        {
            var context = RuleExecutionContext.CreateRowContext(options, counts, collection.GeneratorKey, rowIndex);
            PopulateBindings(collection.Locals, context, $"{collection.GeneratorKey}.locals");

            string key = RuleEvaluator.EvaluateString(collection.Key, context, $"{collection.GeneratorKey}.key");
            context.SetValue("key", key);

            object? documentObject = RuleEvaluator.Evaluate(collection.Document, context, $"{collection.GeneratorKey}.document");
            JsonElement document = ToJsonElement(documentObject);
            yield return new GeneratedCollectionDocument(key, document);
        }
    }

    private static void PopulateBindings(
        IEnumerable<RuleBindingSpec> bindings,
        RuleExecutionContext context,
        string pathPrefix)
    {
        foreach (RuleBindingSpec binding in bindings)
        {
            object? value = RuleEvaluator.Evaluate(binding.Value, context, $"{pathPrefix}.{binding.Name}");
            context.SetValue(binding.Name, value);
        }
    }

    private static JsonElement ToJsonElement(object? value)
    {
        if (value is JsonElement element)
            return element.Clone();

        return JsonSerializer.SerializeToElement(value, s_jsonOptions);
    }

    private static long GetRequiredCount(IReadOnlyDictionary<string, long> counts, string generatorKey)
    {
        if (counts.TryGetValue(generatorKey, out long count))
            return count;

        throw new InvalidOperationException($"No row count was resolved for '{generatorKey}'.");
    }

    private static string GetSourceField(SqlColumnSpec column)
        => string.IsNullOrWhiteSpace(column.SourceField) ? column.Name : column.SourceField!;

    private sealed class RuleExecutionContext
    {
        private readonly Dictionary<string, Stack<object?>> _variables = new(StringComparer.OrdinalIgnoreCase);

        private RuleExecutionContext(
            DataGenOptions options,
            IReadOnlyDictionary<string, long> counts,
            string scopeKey,
            long rowIndex)
        {
            Options = options;
            Counts = counts;
            ScopeKey = scopeKey;
            RowIndex = rowIndex;
        }

        public DataGenOptions Options { get; }

        public IReadOnlyDictionary<string, long> Counts { get; }

        public string ScopeKey { get; }

        public long RowIndex { get; }

        public Dictionary<string, object?> Values { get; } = new(StringComparer.OrdinalIgnoreCase);

        public static RuleExecutionContext CreatePlanningContext(
            DataGenOptions options,
            IReadOnlyDictionary<string, long> counts)
            => new(options, counts, "__plan__", 0);

        public static RuleExecutionContext CreateRowContext(
            DataGenOptions options,
            IReadOnlyDictionary<string, long> counts,
            string scopeKey,
            long rowIndex)
            => new(options, counts, scopeKey, rowIndex);

        public void SetValue(string name, object? value)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(name);
            Values[name] = value;
        }

        public bool TryResolveValue(string name, out object? value)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(name);

            if (_variables.TryGetValue(name, out Stack<object?>? stack) && stack.Count > 0)
            {
                value = stack.Peek();
                return true;
            }

            if (Values.TryGetValue(name, out value))
                return true;

            int separatorIndex = name.IndexOf('.');
            if (separatorIndex > 0)
            {
                string rootName = name[..separatorIndex];
                if (TryResolveValue(rootName, out object? rootValue))
                    return TryResolvePath(rootValue, name[(separatorIndex + 1)..], out value);
            }

            value = null;
            return false;
        }

        public object? ResolveRequiredValue(string name)
        {
            if (TryResolveValue(name, out object? value))
                return value;

            throw new InvalidOperationException(
                $"Generation rule could not resolve value '{name}' in scope '{ScopeKey}' at row {RowIndex}.");
        }

        public IDisposable PushVariable(string name, object? value)
        {
            if (!_variables.TryGetValue(name, out Stack<object?>? stack))
            {
                stack = new Stack<object?>();
                _variables[name] = stack;
            }

            stack.Push(value);
            return new VariableScope(this, name);
        }

        public IReadOnlyDictionary<string, object?> SnapshotValues()
            => new Dictionary<string, object?>(Values, StringComparer.OrdinalIgnoreCase);

        public object? ResolveOption(string name)
            => name.ToLowerInvariant() switch
            {
                "dataset" => Options.Dataset,
                "datasetlabel" => Options.DatasetLabel,
                "seed" => Options.Seed,
                "rowcount" => Options.RowCount,
                "batchsize" => Options.BatchSize,
                "directload" => Options.DirectLoad,
                "writefiles" => Options.WriteFiles,
                "overwritedatabase" => Options.OverwriteDatabase,
                "buildindexes" => Options.BuildIndexes,
                "outputpath" => Options.OutputPath,
                "databasepath" => Options.DatabasePath,
                "specpath" => Options.SpecPath,
                "nullrate" => Options.NullRate,
                "hotkeyrate" => Options.HotKeyRate,
                "recentrate" => Options.RecentRate,
                "avgdocsizebytes" or "avgsize" => Options.AvgDocSizeBytes,
                "tenantcount" => Options.TenantCount,
                "devicecount" => Options.DeviceCount,
                "orderspercustomer" => Options.OrdersPerCustomer,
                "itemsperorder" => Options.ItemsPerOrder,
                _ => throw new InvalidOperationException($"Unknown option reference '{name}'."),
            };

        public long ResolveCount(string name)
        {
            if (Counts.TryGetValue(name, out long count))
                return count;

            throw new InvalidOperationException($"Unknown count reference '{name}'.");
        }

        public Random CreatePathRandom(string operationName, string path)
        {
            var hash = new HashCode();
            hash.Add(Options.Seed);
            hash.Add(ScopeKey, StringComparer.OrdinalIgnoreCase);
            hash.Add(RowIndex);
            hash.Add(operationName, StringComparer.Ordinal);
            hash.Add(path, StringComparer.Ordinal);
            return new Random(hash.ToHashCode());
        }

        public Random CreateSeededRandom(string operationName, IEnumerable<object?> seedParts)
        {
            var hash = new HashCode();
            hash.Add(Options.Seed);
            hash.Add(operationName, StringComparer.Ordinal);

            foreach (object? part in seedParts)
                AddSeedPart(ref hash, part);

            return new Random(hash.ToHashCode());
        }

        private void PopVariable(string name)
        {
            if (!_variables.TryGetValue(name, out Stack<object?>? stack) || stack.Count == 0)
                throw new InvalidOperationException($"Generation variable '{name}' is not active.");

            stack.Pop();
            if (stack.Count == 0)
                _variables.Remove(name);
        }

        private static void AddSeedPart(ref HashCode hash, object? value)
        {
            switch (value)
            {
                case null:
                    hash.Add(0);
                    return;
                case bool boolean:
                    hash.Add(boolean);
                    return;
                case byte byteValue:
                    hash.Add(byteValue);
                    return;
                case sbyte sbyteValue:
                    hash.Add(sbyteValue);
                    return;
                case short shortValue:
                    hash.Add(shortValue);
                    return;
                case ushort ushortValue:
                    hash.Add(ushortValue);
                    return;
                case int intValue:
                    hash.Add(intValue);
                    return;
                case uint uintValue:
                    hash.Add(uintValue);
                    return;
                case long longValue:
                    hash.Add(longValue);
                    return;
                case ulong ulongValue:
                    hash.Add(unchecked((long)ulongValue));
                    return;
                case float floatValue:
                    hash.Add(floatValue);
                    return;
                case double doubleValue:
                    hash.Add(doubleValue);
                    return;
                case decimal decimalValue:
                    hash.Add(decimalValue);
                    return;
                case string text:
                    hash.Add(text, StringComparer.Ordinal);
                    return;
                case DateTime dateTime:
                    hash.Add(dateTime.ToUniversalTime().Ticks);
                    return;
                case Guid guid:
                    hash.Add(guid.ToString("D"), StringComparer.Ordinal);
                    return;
                case IEnumerable enumerable when value is not string:
                    foreach (object? item in enumerable)
                        AddSeedPart(ref hash, item);

                    return;
                default:
                    hash.Add(value.ToString(), StringComparer.Ordinal);
                    return;
            }
        }

        private static bool TryResolvePath(object? rootValue, string path, out object? value)
        {
            object? current = rootValue;
            foreach (string rawSegment in path.Split('.', StringSplitOptions.RemoveEmptyEntries))
            {
                if (!TryParseIndexedSegment(rawSegment, out string segmentName, out int? index))
                {
                    value = null;
                    return false;
                }

                if (!string.IsNullOrWhiteSpace(segmentName))
                {
                    if (!TryGetNamedValue(current, segmentName, out current))
                    {
                        value = null;
                        return false;
                    }
                }

                if (index.HasValue)
                {
                    if (!TryGetIndexedValue(current, index.Value, out current))
                    {
                        value = null;
                        return false;
                    }
                }
            }

            value = current;
            return true;
        }

        private static bool TryParseIndexedSegment(string segment, out string name, out int? index)
        {
            int bracketIndex = segment.IndexOf('[');
            if (bracketIndex < 0)
            {
                name = segment;
                index = null;
                return true;
            }

            if (!segment.EndsWith(']') || bracketIndex == segment.Length - 1)
            {
                name = string.Empty;
                index = null;
                return false;
            }

            name = segment[..bracketIndex];
            if (!int.TryParse(segment[(bracketIndex + 1)..^1], NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsed))
            {
                index = null;
                return false;
            }

            index = parsed;
            return true;
        }

        private static bool TryGetNamedValue(object? current, string name, out object? value)
        {
            switch (current)
            {
                case null:
                    value = null;
                    return false;
                case IReadOnlyDictionary<string, object?> readOnlyDictionary:
                    return readOnlyDictionary.TryGetValue(name, out value);
                case IDictionary<string, object?> dictionary:
                    if (dictionary.TryGetValue(name, out value))
                        return true;

                    foreach ((string key, object? candidate) in dictionary)
                    {
                        if (string.Equals(key, name, StringComparison.OrdinalIgnoreCase))
                        {
                            value = candidate;
                            return true;
                        }
                    }

                    value = null;
                    return false;
                case JsonElement { ValueKind: JsonValueKind.Object } element:
                    foreach (JsonProperty property in element.EnumerateObject())
                    {
                        if (string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase))
                        {
                            value = ConvertJsonElement(property.Value);
                            return true;
                        }
                    }

                    value = null;
                    return false;
                default:
                    value = null;
                    return false;
            }
        }

        private static bool TryGetIndexedValue(object? current, int index, out object? value)
        {
            switch (current)
            {
                case IList<object?> objectList when index >= 0 && index < objectList.Count:
                    value = objectList[index];
                    return true;
                case object?[] array when index >= 0 && index < array.Length:
                    value = array[index];
                    return true;
                case JsonElement { ValueKind: JsonValueKind.Array } element:
                    int currentIndex = 0;
                    foreach (JsonElement item in element.EnumerateArray())
                    {
                        if (currentIndex == index)
                        {
                            value = ConvertJsonElement(item);
                            return true;
                        }

                        currentIndex++;
                    }

                    value = null;
                    return false;
                default:
                    value = null;
                    return false;
            }
        }

        private static object? ConvertJsonElement(JsonElement value)
        {
            return value.ValueKind switch
            {
                JsonValueKind.Null => null,
                JsonValueKind.False => false,
                JsonValueKind.True => true,
                JsonValueKind.String => value.TryGetDateTime(out DateTime dateTime) ? dateTime : value.GetString(),
                JsonValueKind.Number => value.TryGetInt64(out long longValue) ? longValue : value.GetDouble(),
                JsonValueKind.Array => value.EnumerateArray().Select(ConvertJsonElement).ToList(),
                JsonValueKind.Object => value.EnumerateObject()
                    .ToDictionary(static property => property.Name, static property => ConvertJsonElement(property.Value)),
                _ => value.GetRawText(),
            };
        }

        private sealed class VariableScope : IDisposable
        {
            private readonly RuleExecutionContext _context;
            private readonly string _name;
            private bool _disposed;

            public VariableScope(RuleExecutionContext context, string name)
            {
                _context = context;
                _name = name;
            }

            public void Dispose()
            {
                if (_disposed)
                    return;

                _disposed = true;
                _context.PopVariable(_name);
            }
        }
    }

    private static class RuleEvaluator
    {
        public static object? Evaluate(JsonElement rule, RuleExecutionContext context, string path)
        {
            return rule.ValueKind switch
            {
                JsonValueKind.Undefined => throw new InvalidOperationException($"Generation rule at '{path}' is undefined."),
                JsonValueKind.Object => EvaluateObject(rule, context, path),
                JsonValueKind.Array => EvaluateArray(rule, context, path),
                JsonValueKind.String => rule.GetString(),
                JsonValueKind.Number => rule.TryGetInt64(out long longValue) ? longValue : rule.GetDouble(),
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Null => null,
                _ => throw new InvalidOperationException($"Unsupported JSON value kind '{rule.ValueKind}' at '{path}'."),
            };
        }

        public static long EvaluateInt64(JsonElement rule, RuleExecutionContext context, string path)
            => ConvertToInt64(Evaluate(rule, context, path), path);

        public static string EvaluateString(JsonElement rule, RuleExecutionContext context, string path)
            => ConvertToString(Evaluate(rule, context, path), path);

        private static object? EvaluateObject(JsonElement rule, RuleExecutionContext context, string path)
        {
            if (TryGetProperty(rule, "op", out JsonElement operationElement))
                return EvaluateOperation(operationElement.GetString() ?? string.Empty, rule, context, path);

            var result = new Dictionary<string, object?>(StringComparer.Ordinal);
            foreach (JsonProperty property in rule.EnumerateObject())
                result[property.Name] = Evaluate(property.Value, context, $"{path}.{property.Name}");

            return result;
        }

        private static object?[] EvaluateArray(JsonElement rule, RuleExecutionContext context, string path)
            => rule.EnumerateArray()
                .Select((item, index) => Evaluate(item, context, $"{path}[{index}]"))
                .ToArray();

        private static object? EvaluateOperation(
            string operation,
            JsonElement rule,
            RuleExecutionContext context,
            string path)
        {
            return operation.ToLowerInvariant() switch
            {
                "rowindex" => context.RowIndex,
                "value" => context.ResolveRequiredValue(GetRequiredString(rule, "name", path)),
                "option" => context.ResolveOption(GetRequiredString(rule, "name", path)),
                "count" => context.ResolveCount(GetRequiredString(rule, "name", path)),
                "add" => Add(EvaluateRequiredValues(rule, "values", context, path), path),
                "subtract" => Subtract(EvaluateRequired(rule, "left", context, path), EvaluateRequired(rule, "right", context, path), path),
                "multiply" => Multiply(EvaluateRequiredValues(rule, "values", context, path), path),
                "divide" => Divide(EvaluateRequired(rule, "left", context, path), EvaluateRequired(rule, "right", context, path), path),
                "floordivide" => FloorDivide(EvaluateRequired(rule, "left", context, path), EvaluateRequired(rule, "right", context, path), path),
                "mod" => Mod(EvaluateRequired(rule, "left", context, path), EvaluateRequired(rule, "right", context, path), path),
                "round" => Round(EvaluateRequired(rule, "value", context, path), GetOptionalInt32(rule, "digits", context, path, 0), path),
                "min" => Min(EvaluateRequiredValues(rule, "values", context, path), path),
                "max" => Max(EvaluateRequiredValues(rule, "values", context, path), path),
                "eq" => EqualsValue(EvaluateRequired(rule, "left", context, path), EvaluateRequired(rule, "right", context, path)),
                "ne" => !EqualsValue(EvaluateRequired(rule, "left", context, path), EvaluateRequired(rule, "right", context, path)),
                "gt" => Compare(EvaluateRequired(rule, "left", context, path), EvaluateRequired(rule, "right", context, path), path) > 0,
                "gte" => Compare(EvaluateRequired(rule, "left", context, path), EvaluateRequired(rule, "right", context, path), path) >= 0,
                "lt" => Compare(EvaluateRequired(rule, "left", context, path), EvaluateRequired(rule, "right", context, path), path) < 0,
                "lte" => Compare(EvaluateRequired(rule, "left", context, path), EvaluateRequired(rule, "right", context, path), path) <= 0,
                "if" => EvaluateIf(rule, context, path),
                "chance" => EvaluateChance(rule, context, path),
                "pick" => EvaluatePick(rule, context, path),
                "faker" => EvaluateFaker(rule, context, path),
                "int" => EvaluateInteger(rule, context, path),
                "double" => EvaluateDouble(rule, context, path),
                "money" => EvaluateMoney(rule, context, path),
                "bool" => EvaluateBoolean(rule, context, path),
                "pickskewedid" => EvaluatePickSkewedId(rule, context, path),
                "skewedtimestamp" => EvaluateSkewedTimestamp(rule, context, path),
                "dateadd" => EvaluateDateAdd(rule, context, path),
                "format" => EvaluateFormat(rule, context, path),
                "concat" => string.Concat(EvaluateRequiredValues(rule, "values", context, path).Select((value, index) => ConvertToPrintableString(value, $"{path}.values[{index}]"))),
                "lower" => ConvertToPrintableString(EvaluateRequired(rule, "value", context, path), path).ToLowerInvariant(),
                "upper" => ConvertToPrintableString(EvaluateRequired(rule, "value", context, path), path).ToUpperInvariant(),
                "padleft" => ConvertToPrintableString(EvaluateRequired(rule, "value", context, path), path)
                    .PadLeft(GetOptionalInt32(rule, "length", context, path, 0), GetOptionalChar(rule, "char", context, path, '0')),
                "sanitizeemail" => SanitizeForEmail(ConvertToPrintableString(EvaluateRequired(rule, "value", context, path), path)),
                "repeat" => EvaluateRepeat(rule, context, path),
                "sum" => EvaluateSum(rule, context, path),
                "switch" => EvaluateSwitch(rule, context, path),
                "targetjsonsize" => EvaluateTargetJsonSize(rule, context, path),
                "padobjecttosize" => EvaluatePadObjectToSize(rule, context, path),
                "guid" => EvaluateGuid(rule, context, path),
                "sizedtext" => EvaluateSizedText(rule, context, path),
                _ => throw new InvalidOperationException($"Unsupported generation op '{operation}' at '{path}'."),
            };
        }

        private static object? EvaluateIf(JsonElement rule, RuleExecutionContext context, string path)
        {
            bool condition = ConvertToBoolean(EvaluateRequired(rule, "condition", context, path), path);
            return condition
                ? EvaluateRequired(rule, "then", context, path)
                : EvaluateOptional(rule, "else", context, path);
        }

        private static object? EvaluateChance(JsonElement rule, RuleExecutionContext context, string path)
        {
            double probability = ConvertToDouble(EvaluateRequired(rule, "probability", context, path), path);
            Random rng = CreateRandom(rule, context, path, "chance");
            bool takeThen = rng.NextDouble() < probability;
            return takeThen
                ? EvaluateRequired(rule, "then", context, path)
                : EvaluateOptional(rule, "else", context, path);
        }

        private static object? EvaluatePick(JsonElement rule, RuleExecutionContext context, string path)
        {
            JsonElement valuesElement = GetRequiredProperty(rule, "values", path);
            JsonElement[] values = valuesElement.EnumerateArray().ToArray();
            if (values.Length == 0)
                throw new InvalidOperationException($"Pick rule at '{path}' requires at least one value.");

            Random rng = CreateRandom(rule, context, path, "pick");
            return Evaluate(values[rng.Next(values.Length)], context, $"{path}.values");
        }

        private static object? EvaluateFaker(JsonElement rule, RuleExecutionContext context, string path)
        {
            string fakerName = GetRequiredString(rule, "name", path);
            Random rng = CreateRandom(rule, context, path, "faker");
            var faker = new Faker("en")
            {
                Random = new Randomizer(rng.Next()),
            };

            return fakerName.ToLowerInvariant() switch
            {
                "name.firstname" => faker.Name.FirstName(),
                "name.lastname" => faker.Name.LastName(),
                "name.fullname" or "person.fullname" => faker.Person.FullName,
                "phone.phonenumber" => faker.Phone.PhoneNumber(),
                "address.streetaddress" => faker.Address.StreetAddress(),
                "address.secondaryaddress" => faker.Address.SecondaryAddress(),
                "address.city" => faker.Address.City(),
                "address.stateabbr" => faker.Address.StateAbbr(),
                "address.zipcode" => faker.Address.ZipCode(),
                "address.countrycode" => faker.Address.CountryCode(),
                "commerce.productname" => faker.Commerce.ProductName(),
                "internet.email" => faker.Internet.Email().ToLowerInvariant(),
                "lorem.sentence" => faker.Lorem.Sentence(),
                "lorem.paragraph" => faker.Lorem.Paragraph(),
                _ => throw new InvalidOperationException($"Unsupported faker name '{fakerName}' at '{path}'."),
            };
        }

        private static long EvaluateInteger(JsonElement rule, RuleExecutionContext context, string path)
        {
            long min = ConvertToInt64(EvaluateRequired(rule, "min", context, path), path);
            long max = ConvertToInt64(EvaluateRequired(rule, "max", context, path), path);
            if (max < min)
                throw new InvalidOperationException($"Integer rule at '{path}' has max < min.");

            Random rng = CreateRandom(rule, context, path, "int");
            return NextInt64Inclusive(rng, min, max);
        }

        private static double EvaluateDouble(JsonElement rule, RuleExecutionContext context, string path)
        {
            double min = ConvertToDouble(EvaluateRequired(rule, "min", context, path), path);
            double max = ConvertToDouble(EvaluateRequired(rule, "max", context, path), path);
            if (max < min)
                throw new InvalidOperationException($"Double rule at '{path}' has max < min.");

            Random rng = CreateRandom(rule, context, path, "double");
            double value = min + (rng.NextDouble() * (max - min));
            int? digits = TryGetOptionalInt32(rule, "digits", context, path);
            return digits.HasValue ? Math.Round(value, digits.Value) : value;
        }

        private static double EvaluateMoney(JsonElement rule, RuleExecutionContext context, string path)
        {
            double min = ConvertToDouble(EvaluateRequired(rule, "min", context, path), path);
            double max = ConvertToDouble(EvaluateRequired(rule, "max", context, path), path);
            if (max < min)
                throw new InvalidOperationException($"Money rule at '{path}' has max < min.");

            Random rng = CreateRandom(rule, context, path, "money");
            return GenerationPrimitives.NextMoney(rng, min, max);
        }

        private static bool EvaluateBoolean(JsonElement rule, RuleExecutionContext context, string path)
        {
            double probability = ConvertToDouble(EvaluateRequired(rule, "probability", context, path), path);
            Random rng = CreateRandom(rule, context, path, "bool");
            return rng.NextDouble() < probability;
        }

        private static long EvaluatePickSkewedId(JsonElement rule, RuleExecutionContext context, string path)
        {
            long max = ConvertToInt64(EvaluateRequired(rule, "max", context, path), path);
            double hotKeyRate = ConvertToDouble(EvaluateRequired(rule, "hotKeyRate", context, path), path);
            Random rng = CreateRandom(rule, context, path, "pickSkewedId");
            return GenerationPrimitives.PickSkewedId(rng, max, hotKeyRate);
        }

        private static DateTime EvaluateSkewedTimestamp(JsonElement rule, RuleExecutionContext context, string path)
        {
            int recentWindowDays = GetOptionalInt32(rule, "recentWindowDays", context, path, 30);
            int fullWindowDays = GetOptionalInt32(rule, "fullWindowDays", context, path, recentWindowDays);
            double recentRate = ConvertToDouble(EvaluateRequired(rule, "recentRate", context, path), path);
            DateTime anchorUtc = TryGetProperty(rule, "anchorUtc", out JsonElement anchorRule)
                ? ConvertToDateTime(Evaluate(anchorRule, context, $"{path}.anchorUtc"), $"{path}.anchorUtc")
                : GenerationPrimitives.AnchorUtc;

            Random rng = CreateRandom(rule, context, path, "skewedTimestamp");
            return GenerationPrimitives.PickSkewedTimestamp(rng, anchorUtc, recentWindowDays, fullWindowDays, recentRate);
        }

        private static DateTime EvaluateDateAdd(JsonElement rule, RuleExecutionContext context, string path)
        {
            DateTime value = ConvertToDateTime(EvaluateRequired(rule, "value", context, path), path);

            if (TryGetProperty(rule, "days", out JsonElement daysRule))
                value = value.AddDays(ConvertToDouble(Evaluate(daysRule, context, $"{path}.days"), $"{path}.days"));
            if (TryGetProperty(rule, "hours", out JsonElement hoursRule))
                value = value.AddHours(ConvertToDouble(Evaluate(hoursRule, context, $"{path}.hours"), $"{path}.hours"));
            if (TryGetProperty(rule, "minutes", out JsonElement minutesRule))
                value = value.AddMinutes(ConvertToDouble(Evaluate(minutesRule, context, $"{path}.minutes"), $"{path}.minutes"));
            if (TryGetProperty(rule, "seconds", out JsonElement secondsRule))
                value = value.AddSeconds(ConvertToDouble(Evaluate(secondsRule, context, $"{path}.seconds"), $"{path}.seconds"));

            return value;
        }

        private static string EvaluateFormat(JsonElement rule, RuleExecutionContext context, string path)
        {
            string format = GetRequiredString(rule, "format", path);
            object?[] args = EvaluateRequiredValues(rule, "args", context, path);
            return string.Format(CultureInfo.InvariantCulture, format, args);
        }

        private static object?[] EvaluateRepeat(JsonElement rule, RuleExecutionContext context, string path)
        {
            long count = ConvertToInt64(EvaluateRequired(rule, "count", context, path), path);
            if (count < 0)
                throw new InvalidOperationException($"Repeat rule at '{path}' cannot have a negative count.");

            string variableName = GetOptionalString(rule, "as") ?? "index";
            long start = TryGetProperty(rule, "start", out JsonElement startRule)
                ? ConvertToInt64(Evaluate(startRule, context, $"{path}.start"), $"{path}.start")
                : 0;

            JsonElement valueRule = GetRequiredProperty(rule, "value", path);
            var values = new object?[count];
            for (long i = 0; i < count; i++)
            {
                using IDisposable scope = context.PushVariable(variableName, start + i);
                values[i] = Evaluate(valueRule, context, $"{path}.value[{i}]");
            }

            return values;
        }

        private static object EvaluateSum(JsonElement rule, RuleExecutionContext context, string path)
        {
            object? valuesObject = EvaluateRequired(rule, "values", context, path);
            IReadOnlyList<object?> values = ToValueList(valuesObject, path);
            if (values.Count == 0)
                return 0L;

            bool anyFloating = values.Any(IsFloatingPointNumber);
            if (anyFloating)
                return values.Sum(value => ConvertToDouble(value, path));

            return values.Sum(value => ConvertToInt64(value, path));
        }

        private static object? EvaluateSwitch(JsonElement rule, RuleExecutionContext context, string path)
        {
            object? input = EvaluateRequired(rule, "value", context, path);
            JsonElement casesElement = GetRequiredProperty(rule, "cases", path);
            foreach (JsonElement caseRule in casesElement.EnumerateArray())
            {
                object? whenValue = EvaluateRequired(caseRule, "when", context, $"{path}.cases");
                if (EqualsValue(input, whenValue))
                    return EvaluateRequired(caseRule, "value", context, $"{path}.cases");
            }

            return EvaluateOptional(rule, "default", context, path);
        }

        private static int EvaluateTargetJsonSize(JsonElement rule, RuleExecutionContext context, string path)
        {
            int averageBytes = ConvertToInt32(EvaluateRequired(rule, "averageBytes", context, path), path);
            int[] buckets = TryGetProperty(rule, "buckets", out JsonElement bucketsRule)
                ? EvaluateRequiredValues(bucketsRule, context, $"{path}.buckets").Select(value => ConvertToInt32(value, path)).ToArray()
                : s_defaultDocumentSizeBuckets;

            if (buckets.Length == 0)
                throw new InvalidOperationException($"targetJsonSize rule at '{path}' requires at least one bucket.");

            int closestBucket = buckets.OrderBy(bucket => Math.Abs(bucket - averageBytes)).First();
            Random rng = CreateRandom(rule, context, path, "targetJsonSize");
            return rng.NextDouble() < 0.70
                ? closestBucket
                : buckets[rng.Next(buckets.Length)];
        }

        private static object? EvaluatePadObjectToSize(JsonElement rule, RuleExecutionContext context, string path)
        {
            object? value = EvaluateRequired(rule, "value", context, path);
            if (value is not IDictionary<string, object?> document)
            {
                throw new InvalidOperationException(
                    $"padObjectToSize at '{path}' requires its 'value' to evaluate to an object.");
            }

            int targetSize = ConvertToInt32(EvaluateRequired(rule, "targetSize", context, path), path);
            Random rng = CreateRandom(rule, context, path, "padObjectToSize");
            string? containerPath = GetOptionalString(rule, "containerPath");
            string blobField = GetOptionalString(rule, "blobField") ?? "blob";
            string fragmentsField = GetOptionalString(rule, "fragmentsField") ?? "fragments";
            int fragmentThreshold = GetOptionalInt32(rule, "fragmentThreshold", context, path, 4096);
            int fragmentSize = GetOptionalInt32(rule, "fragmentSize", context, path, 96);

            int currentSize = JsonSerializer.SerializeToUtf8Bytes(document, s_jsonOptions).Length;
            if (currentSize >= targetSize)
                return document;

            IDictionary<string, object?> container = ResolveObjectPath(document, containerPath);
            int remaining = targetSize - currentSize;
            container[blobField] = GenerationPrimitives.BuildSizedText(rng, remaining + 32);

            if (targetSize >= fragmentThreshold)
            {
                int fragmentCount = Math.Max(2, remaining / 512);
                container[fragmentsField] = Enumerable.Range(0, fragmentCount)
                    .Select(_ => GenerationPrimitives.BuildSizedText(rng, fragmentSize))
                    .ToArray();
            }

            return document;
        }

        private static string EvaluateGuid(JsonElement rule, RuleExecutionContext context, string path)
        {
            Random rng = CreateRandom(rule, context, path, "guid");
            return GenerationPrimitives.NextGuid(rng).ToString("D");
        }

        private static string EvaluateSizedText(JsonElement rule, RuleExecutionContext context, string path)
        {
            int length = ConvertToInt32(EvaluateRequired(rule, "length", context, path), path);
            Random rng = CreateRandom(rule, context, path, "sizedText");
            return GenerationPrimitives.BuildSizedText(rng, length);
        }

        private static Random CreateRandom(JsonElement rule, RuleExecutionContext context, string path, string operationName)
        {
            if (!TryGetProperty(rule, "seedParts", out JsonElement seedPartsRule))
                return context.CreatePathRandom(operationName, path);

            object?[] seedParts = EvaluateRequiredValues(seedPartsRule, context, $"{path}.seedParts");
            return context.CreateSeededRandom(operationName, seedParts);
        }

        private static object?[] EvaluateRequiredValues(
            JsonElement rule,
            string propertyName,
            RuleExecutionContext context,
            string path)
            => EvaluateRequiredValues(GetRequiredProperty(rule, propertyName, path), context, $"{path}.{propertyName}");

        private static object?[] EvaluateRequiredValues(
            JsonElement valuesRule,
            RuleExecutionContext context,
            string path)
        {
            if (valuesRule.ValueKind != JsonValueKind.Array)
                throw new InvalidOperationException($"Rule at '{path}' must be an array.");

            return valuesRule.EnumerateArray()
                .Select((item, index) => Evaluate(item, context, $"{path}[{index}]"))
                .ToArray();
        }

        private static object? EvaluateRequired(
            JsonElement rule,
            string propertyName,
            RuleExecutionContext context,
            string path)
            => Evaluate(GetRequiredProperty(rule, propertyName, path), context, $"{path}.{propertyName}");

        private static object? EvaluateOptional(
            JsonElement rule,
            string propertyName,
            RuleExecutionContext context,
            string path)
            => TryGetProperty(rule, propertyName, out JsonElement value)
                ? Evaluate(value, context, $"{path}.{propertyName}")
                : null;

        private static JsonElement GetRequiredProperty(JsonElement rule, string propertyName, string path)
        {
            if (TryGetProperty(rule, propertyName, out JsonElement value))
                return value;

            throw new InvalidOperationException($"Rule at '{path}' is missing required property '{propertyName}'.");
        }

        private static string GetRequiredString(JsonElement rule, string propertyName, string path)
            => GetRequiredProperty(rule, propertyName, path).GetString()
               ?? throw new InvalidOperationException($"Rule at '{path}' is missing required string property '{propertyName}'.");

        private static string? GetOptionalString(JsonElement rule, string propertyName)
            => TryGetProperty(rule, propertyName, out JsonElement value) ? value.GetString() : null;

        private static int GetOptionalInt32(
            JsonElement rule,
            string propertyName,
            RuleExecutionContext context,
            string path,
            int fallback)
            => TryGetOptionalInt32(rule, propertyName, context, path) ?? fallback;

        private static int? TryGetOptionalInt32(
            JsonElement rule,
            string propertyName,
            RuleExecutionContext context,
            string path)
            => TryGetProperty(rule, propertyName, out JsonElement value)
                ? ConvertToInt32(Evaluate(value, context, $"{path}.{propertyName}"), $"{path}.{propertyName}")
                : null;

        private static char GetOptionalChar(
            JsonElement rule,
            string propertyName,
            RuleExecutionContext context,
            string path,
            char fallback)
        {
            if (!TryGetProperty(rule, propertyName, out JsonElement value))
                return fallback;

            string text = ConvertToString(Evaluate(value, context, $"{path}.{propertyName}"), $"{path}.{propertyName}");
            return text.Length == 0 ? fallback : text[0];
        }

        private static bool TryGetProperty(JsonElement element, string propertyName, out JsonElement value)
        {
            foreach (JsonProperty property in element.EnumerateObject())
            {
                if (string.Equals(property.Name, propertyName, StringComparison.OrdinalIgnoreCase))
                {
                    value = property.Value;
                    return true;
                }
            }

            value = default;
            return false;
        }

        private static object Add(IReadOnlyList<object?> values, string path)
        {
            if (values.Count == 0)
                return 0L;

            bool anyFloating = values.Any(IsFloatingPointNumber);
            if (anyFloating)
                return values.Sum(value => ConvertToDouble(value, path));

            return values.Sum(value => ConvertToInt64(value, path));
        }

        private static object Multiply(IReadOnlyList<object?> values, string path)
        {
            if (values.Count == 0)
                return 0L;

            bool anyFloating = values.Any(IsFloatingPointNumber);
            if (anyFloating)
            {
                double result = 1d;
                foreach (object? value in values)
                    result *= ConvertToDouble(value, path);

                return result;
            }

            long integerResult = 1L;
            foreach (object? value in values)
                integerResult = checked(integerResult * ConvertToInt64(value, path));

            return integerResult;
        }

        private static object Subtract(object? left, object? right, string path)
        {
            if (IsFloatingPointNumber(left) || IsFloatingPointNumber(right))
                return ConvertToDouble(left, path) - ConvertToDouble(right, path);

            return checked(ConvertToInt64(left, path) - ConvertToInt64(right, path));
        }

        private static double Divide(object? left, object? right, string path)
            => ConvertToDouble(left, path) / ConvertToDouble(right, path);

        private static long FloorDivide(object? left, object? right, string path)
            => ConvertToInt64(Math.Floor(ConvertToDouble(left, path) / ConvertToDouble(right, path)), path);

        private static long Mod(object? left, object? right, string path)
            => ConvertToInt64(left, path) % ConvertToInt64(right, path);

        private static double Round(object? value, int digits, string path)
            => Math.Round(ConvertToDouble(value, path), digits);

        private static object Min(IReadOnlyList<object?> values, string path)
        {
            if (values.Count == 0)
                throw new InvalidOperationException($"min() at '{path}' requires at least one value.");

            bool anyFloating = values.Any(IsFloatingPointNumber);
            return anyFloating
                ? values.Min(value => ConvertToDouble(value, path))
                : values.Min(value => ConvertToInt64(value, path));
        }

        private static object Max(IReadOnlyList<object?> values, string path)
        {
            if (values.Count == 0)
                throw new InvalidOperationException($"max() at '{path}' requires at least one value.");

            bool anyFloating = values.Any(IsFloatingPointNumber);
            return anyFloating
                ? values.Max(value => ConvertToDouble(value, path))
                : values.Max(value => ConvertToInt64(value, path));
        }

        private static int Compare(object? left, object? right, string path)
        {
            if (left is DateTime || right is DateTime)
            {
                DateTime lhs = ConvertToDateTime(left, path);
                DateTime rhs = ConvertToDateTime(right, path);
                return lhs.CompareTo(rhs);
            }

            if (IsNumeric(left) && IsNumeric(right))
                return ConvertToDouble(left, path).CompareTo(ConvertToDouble(right, path));

            return string.CompareOrdinal(
                ConvertToPrintableString(left, path),
                ConvertToPrintableString(right, path));
        }

        private static bool EqualsValue(object? left, object? right)
        {
            if (left is DateTime leftDateTime && right is DateTime rightDateTime)
                return leftDateTime == rightDateTime;

            if (IsNumeric(left) && IsNumeric(right))
                return Math.Abs(Convert.ToDouble(left, CultureInfo.InvariantCulture) - Convert.ToDouble(right, CultureInfo.InvariantCulture)) < double.Epsilon;

            return Equals(left, right);
        }

        private static bool IsNumeric(object? value)
            => value is byte or sbyte or short or ushort or int or uint or long or ulong or float or double or decimal;

        private static bool IsFloatingPointNumber(object? value)
            => value is float or double or decimal;

        private static long ConvertToInt64(object? value, string path)
        {
            return value switch
            {
                byte byteValue => byteValue,
                sbyte sbyteValue => sbyteValue,
                short shortValue => shortValue,
                ushort ushortValue => ushortValue,
                int intValue => intValue,
                uint uintValue => checked((long)uintValue),
                long longValue => longValue,
                ulong ulongValue => checked((long)ulongValue),
                float floatValue => checked((long)floatValue),
                double doubleValue => checked((long)doubleValue),
                decimal decimalValue => checked((long)decimalValue),
                bool booleanValue => booleanValue ? 1L : 0L,
                JsonElement element when element.ValueKind == JsonValueKind.Number => element.GetInt64(),
                string text when long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed) => parsed,
                _ => throw new InvalidOperationException($"Value '{value}' at '{path}' cannot be converted to Int64."),
            };
        }

        private static int ConvertToInt32(object? value, string path)
            => checked((int)ConvertToInt64(value, path));

        private static double ConvertToDouble(object? value, string path)
        {
            return value switch
            {
                byte byteValue => byteValue,
                sbyte sbyteValue => sbyteValue,
                short shortValue => shortValue,
                ushort ushortValue => ushortValue,
                int intValue => intValue,
                uint uintValue => uintValue,
                long longValue => longValue,
                ulong ulongValue => ulongValue,
                float floatValue => floatValue,
                double doubleValue => doubleValue,
                decimal decimalValue => (double)decimalValue,
                JsonElement element when element.ValueKind == JsonValueKind.Number => element.GetDouble(),
                string text when double.TryParse(text, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out double parsed) => parsed,
                _ => throw new InvalidOperationException($"Value '{value}' at '{path}' cannot be converted to Double."),
            };
        }

        private static bool ConvertToBoolean(object? value, string path)
        {
            return value switch
            {
                bool booleanValue => booleanValue,
                string text when bool.TryParse(text, out bool parsed) => parsed,
                byte byteValue => byteValue != 0,
                sbyte sbyteValue => sbyteValue != 0,
                short shortValue => shortValue != 0,
                ushort ushortValue => ushortValue != 0,
                int intValue => intValue != 0,
                uint uintValue => uintValue != 0,
                long longValue => longValue != 0,
                ulong ulongValue => ulongValue != 0,
                _ => throw new InvalidOperationException($"Value '{value}' at '{path}' cannot be converted to Boolean."),
            };
        }

        private static DateTime ConvertToDateTime(object? value, string path)
        {
            return value switch
            {
                DateTime dateTime => dateTime.Kind == DateTimeKind.Utc ? dateTime : dateTime.ToUniversalTime(),
                DateTimeOffset dateTimeOffset => dateTimeOffset.UtcDateTime,
                string text when DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out DateTime parsed) => parsed,
                _ => throw new InvalidOperationException($"Value '{value}' at '{path}' cannot be converted to DateTime."),
            };
        }

        private static string ConvertToString(object? value, string path)
        {
            return value switch
            {
                null => throw new InvalidOperationException($"Value at '{path}' is null and cannot be converted to string."),
                string text => text,
                DateTime dateTime => dateTime.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
                DateTimeOffset dateTimeOffset => dateTimeOffset.UtcDateTime.ToString("O", CultureInfo.InvariantCulture),
                IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
                _ => value.ToString() ?? throw new InvalidOperationException($"Value at '{path}' cannot be converted to string."),
            };
        }

        private static string ConvertToPrintableString(object? value, string path)
        {
            return value switch
            {
                null => string.Empty,
                string text => text,
                DateTime dateTime => dateTime.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
                DateTimeOffset dateTimeOffset => dateTimeOffset.UtcDateTime.ToString("O", CultureInfo.InvariantCulture),
                IEnumerable<object?> values => JsonSerializer.Serialize(values, s_jsonOptions),
                _ => ConvertToString(value, path),
            };
        }

        private static IReadOnlyList<object?> ToValueList(object? value, string path)
        {
            return value switch
            {
                IReadOnlyList<object?> readOnlyList => readOnlyList,
                IList<object?> list => list.ToArray(),
                IEnumerable enumerable when value is not string => enumerable.Cast<object?>().ToArray(),
                _ => throw new InvalidOperationException($"Value at '{path}' must be an array or list."),
            };
        }

        private static long NextInt64Inclusive(Random rng, long min, long max)
        {
            if (min == max)
                return min;

            if (max == long.MaxValue)
            {
                ulong span = unchecked((ulong)(max - min));
                return min + (long)(NextUInt64(rng) % (span + 1));
            }

            return rng.NextInt64(min, max + 1);
        }

        private static ulong NextUInt64(Random rng)
        {
            Span<byte> bytes = stackalloc byte[8];
            rng.NextBytes(bytes);
            return BitConverter.ToUInt64(bytes);
        }

        private static IDictionary<string, object?> ResolveObjectPath(
            IDictionary<string, object?> document,
            string? path)
        {
            if (string.IsNullOrWhiteSpace(path))
                return document;

            IDictionary<string, object?> current = document;
            foreach (string segment in path.Split('.', StringSplitOptions.RemoveEmptyEntries))
            {
                if (current.TryGetValue(segment, out object? existing) && existing is IDictionary<string, object?> nextObject)
                {
                    current = nextObject;
                    continue;
                }

                var created = new Dictionary<string, object?>(StringComparer.Ordinal);
                current[segment] = created;
                current = created;
            }

            return current;
        }

        private static string SanitizeForEmail(string value)
        {
            var builder = new StringBuilder(value.Length);
            foreach (char c in value)
            {
                if (char.IsLetterOrDigit(c))
                    builder.Append(char.ToLowerInvariant(c));
            }

            return builder.Length == 0 ? "user" : builder.ToString();
        }
    }
}
