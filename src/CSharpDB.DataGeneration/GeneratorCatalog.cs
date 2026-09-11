using System.Globalization;
using System.Text.Json;
using CSharpDB.DataGen;
using CSharpDB.DataGen.Generators;
using CSharpDB.DataGen.Specs;
using CSharpDB.Primitives;

namespace CSharpDB.DataGeneration;

public static class GeneratorCatalog
{
    private static readonly Dictionary<FieldGenerator, string> FakerNames = new()
    {
        [FieldGenerator.FirstName] = "name.firstName", [FieldGenerator.LastName] = "name.lastName",
        [FieldGenerator.FullName] = "name.fullName", [FieldGenerator.Email] = "internet.email",
        [FieldGenerator.Phone] = "phone.phoneNumber", [FieldGenerator.Street] = "address.streetAddress",
        [FieldGenerator.City] = "address.city", [FieldGenerator.Region] = "address.state",
        [FieldGenerator.PostalCode] = "address.zipCode", [FieldGenerator.Country] = "address.country",
        [FieldGenerator.Company] = "company.companyName", [FieldGenerator.Product] = "commerce.productName",
        [FieldGenerator.Text] = "lorem.sentence",
    };

    public static IReadOnlyList<FieldGenerator> Available(ColumnDefinition column)
    {
        if (column.IsRowVersion) return [FieldGenerator.RowVersion];
        var type = column.EffectiveType;
        var choices = new List<FieldGenerator> { FieldGenerator.Constant, FieldGenerator.ValueList };
        if (column.DefaultSql is not null) choices.Add(FieldGenerator.Default);
        switch (type.Kind)
        {
            case SqlTypeKind.Boolean: choices.Add(FieldGenerator.Boolean); break;
            case SqlTypeKind.TinyInt or SqlTypeKind.SmallInt or SqlTypeKind.Integer or SqlTypeKind.BigInt:
                choices.AddRange([FieldGenerator.Sequence, FieldGenerator.Number, FieldGenerator.Boolean]); break;
            case SqlTypeKind.Real or SqlTypeKind.Double or SqlTypeKind.Decimal:
                choices.AddRange([FieldGenerator.Sequence, FieldGenerator.Number]); break;
            case SqlTypeKind.Char or SqlTypeKind.VarChar or SqlTypeKind.Text:
                choices.AddRange(FakerNames.Keys); choices.AddRange([FieldGenerator.Sequence, FieldGenerator.Uuid, FieldGenerator.DateTime]); break;
            case SqlTypeKind.Uuid: choices.Add(FieldGenerator.Uuid); break;
            case SqlTypeKind.Date or SqlTypeKind.Time or SqlTypeKind.Timestamp or SqlTypeKind.TimestampWithTimeZone:
                choices.Add(FieldGenerator.DateTime); break;
            case SqlTypeKind.Binary or SqlTypeKind.VarBinary or SqlTypeKind.Blob: choices.Add(FieldGenerator.Binary); break;
            case SqlTypeKind.Json or SqlTypeKind.Xml: break;
            default: return [];
        }
        return choices.Distinct().OrderBy(c => c.ToString()).ToArray();
    }

    public static ColumnGenerationRule Suggest(ColumnDefinition column, bool unique)
    {
        var type = column.EffectiveType;
        string name = column.Name.Replace("_", "", StringComparison.Ordinal).ToLowerInvariant();
        FieldGenerator generator = type.Kind switch
        {
            _ when column.IsRowVersion => FieldGenerator.RowVersion,
            SqlTypeKind.Boolean => FieldGenerator.Boolean,
            SqlTypeKind.TinyInt or SqlTypeKind.SmallInt or SqlTypeKind.Integer or SqlTypeKind.BigInt
                => unique || column.IsIdentity ? FieldGenerator.Sequence : name.StartsWith("is") || name.StartsWith("has") ? FieldGenerator.Boolean : FieldGenerator.Number,
            SqlTypeKind.Real or SqlTypeKind.Double or SqlTypeKind.Decimal => unique ? FieldGenerator.Sequence : FieldGenerator.Number,
            SqlTypeKind.Uuid => FieldGenerator.Uuid,
            SqlTypeKind.Date or SqlTypeKind.Time or SqlTypeKind.Timestamp or SqlTypeKind.TimestampWithTimeZone => FieldGenerator.DateTime,
            SqlTypeKind.Binary or SqlTypeKind.VarBinary or SqlTypeKind.Blob => FieldGenerator.Binary,
            SqlTypeKind.Json or SqlTypeKind.Xml => FieldGenerator.Constant,
            _ when unique => FieldGenerator.Sequence,
            _ when name.Contains("email") => FieldGenerator.Email,
            _ when name is "firstname" or "fname" => FieldGenerator.FirstName,
            _ when name is "lastname" or "surname" or "lname" => FieldGenerator.LastName,
            _ when name is "name" or "fullname" or "displayname" => FieldGenerator.FullName,
            _ when name.Contains("phone") => FieldGenerator.Phone,
            _ when name is "street" or "address" or "address1" => FieldGenerator.Street,
            _ when name == "city" => FieldGenerator.City,
            _ when name is "state" or "region" or "province" => FieldGenerator.Region,
            _ when name is "postcode" or "postalcode" or "zipcode" or "zip" => FieldGenerator.PostalCode,
            _ when name == "country" => FieldGenerator.Country,
            _ when name is "company" or "companyname" => FieldGenerator.Company,
            _ when name is "product" or "productname" => FieldGenerator.Product,
            _ when name is "status" or "category" or "type" => FieldGenerator.ValueList,
            _ when name.Contains("date") || name.EndsWith("utc") || name is "createdat" or "updatedat" => FieldGenerator.DateTime,
            _ => FieldGenerator.Text,
        };
        return new ColumnGenerationRule
        {
            ColumnName = column.Name, SchemaId = column.SchemaId, Generator = generator,
            NullRate = column.Nullable && !unique && !column.IsPrimaryKey && !column.IsRowVersion ? .05 : 0,
            Length = Math.Min(type.Length ?? 32, 32), Maximum = type.Kind == SqlTypeKind.TinyInt ? 100 : 1000,
            Distribution = generator == FieldGenerator.DateTime ? FieldDistribution.Recent : FieldDistribution.Uniform,
            Values = ["Active", "Inactive", "Pending"], Weights = [70, 20, 10],
            Constant = type.Kind == SqlTypeKind.Json ? "{}" : type.Kind == SqlTypeKind.Xml ? "<item />" : "",
        };
    }

    public static void Validate(ColumnGenerationRule rule, ColumnDefinition column)
    {
        string path = column.Name;
        if (!Enum.IsDefined(rule.Distribution)) throw new InvalidOperationException($"{path}: unrecognized distribution.");
        if (!Available(column).Contains(rule.Generator)) throw new InvalidOperationException($"{path}: {rule.Generator} is not supported for {column.EffectiveType}.");
        if (!double.IsFinite(rule.NullRate) || rule.NullRate is < 0 or > 1 || (!column.Nullable || column.IsPrimaryKey) && rule.NullRate != 0)
            throw new InvalidOperationException($"{path}: null percentage is invalid for this column.");
        if (rule.Length is < 1 or > 4096) throw new InvalidOperationException($"{path}: generated length must be between 1 and 4096.");
        if (rule.Minimum > rule.Maximum || !double.IsFinite(rule.Probability) || rule.Probability is < 0 or > 1)
            throw new InvalidOperationException($"{path}: check range and probability settings.");
        bool numeric = rule.Generator == FieldGenerator.Number;
        if (rule.Distribution == FieldDistribution.Normal && (!double.IsFinite(rule.Mean) || !double.IsFinite(rule.Deviation)
            || rule.Minimum >= rule.Maximum || rule.Mean < (double)rule.Minimum || rule.Mean > (double)rule.Maximum
            || rule.Deviation <= 0 || rule.Deviation > (double)(rule.Maximum - rule.Minimum)))
            throw new InvalidOperationException($"{path}: normal distribution requires a mean within the range and a positive deviation no larger than the range.");
        if (rule.Distribution == FieldDistribution.Normal && !numeric || rule.Distribution == FieldDistribution.Weighted && rule.Generator != FieldGenerator.ValueList
            || rule.Distribution == FieldDistribution.Recent && rule.Generator != FieldGenerator.DateTime || rule.Distribution == FieldDistribution.HotKeys)
            throw new InvalidOperationException($"{path}: this distribution does not apply to {rule.Generator}.");
        if (rule.Generator == FieldGenerator.ValueList && (rule.Values is null || rule.Values.Count is < 1 or > 1000))
            throw new InvalidOperationException($"{path}: provide between 1 and 1000 values.");
        if (rule.Distribution == FieldDistribution.Weighted && (rule.Weights is null || rule.Weights.Count != rule.Values.Count
            || rule.Weights.Any(w => !double.IsFinite(w) || w < 0) || !double.IsFinite(rule.Weights.Sum()) || rule.Weights.Sum() <= 0))
            throw new InvalidOperationException($"{path}: weights must match values and have a finite positive total.");
        if (rule.Generator == FieldGenerator.DateTime && (rule.Days is < 1 or > 36500 || rule.RecentDays < 1 || rule.RecentDays > rule.Days))
            throw new InvalidOperationException($"{path}: date windows must be between 1 and 36500 days, with recent days within the full window.");
        if (rule.Generator == FieldGenerator.Default) GenerationValues.ParseSafeExpression(column.DefaultSql!);
    }

    public static object? Generate(GenerationProfile profile, TableSchema schema, ColumnDefinition column, ColumnGenerationRule rule,
        long ordinal, long sequenceStart, IReadOnlyDictionary<string, object?> values)
    {
        string tableId = schema.SchemaId == Guid.Empty ? schema.TableName.ToLowerInvariant() : schema.SchemaId.ToString("N");
        string columnId = column.SchemaId == Guid.Empty ? column.Name.ToLowerInvariant() : column.SchemaId.ToString("N");
        var random = StableRandom.Create(profile.Seed, tableId, columnId, ordinal);
        if (rule.NullRate > 0 && random.NextDouble() < rule.NullRate) return null;
        if (rule.Generator == FieldGenerator.Default)
            return GenerationValues.ToObject(GenerationValues.Evaluate(GenerationValues.ParseSafeExpression(column.DefaultSql!), schema, values));
        long number = checked(sequenceStart + ordinal - 1);
        if (rule.Generator == FieldGenerator.Sequence)
            return column.Type == DbType.Text ? number.ToString(CultureInfo.InvariantCulture) : number;
        if (rule.Generator == FieldGenerator.Constant) return ParseInput(rule.Constant, column);
        if (rule.Generator == FieldGenerator.Binary)
        {
            var bytes = new byte[rule.Length]; random.NextBytes(bytes); return bytes;
        }
        if (rule.Generator == FieldGenerator.Boolean) return random.NextDouble() < rule.Probability;
        if (rule.Generator == FieldGenerator.Uuid)
        {
            var bytes = new byte[16]; random.NextBytes(bytes);
            return column.Type == DbType.Blob ? bytes : new Guid(bytes).ToString("D");
        }
        object expression;
        if (rule.Generator == FieldGenerator.Number)
        {
            if (rule.Distribution == FieldDistribution.Normal)
                expression = new { op = "normal", min = rule.Minimum, max = rule.Maximum, mean = rule.Mean, deviation = rule.Deviation, digits = column.Type == DbType.Integer ? 0 : Math.Min(column.EffectiveType.Scale ?? 2, 15) };
            else if (column.Type == DbType.Integer)
                return random.NextInclusive(checked((long)decimal.Ceiling(rule.Minimum)), checked((long)decimal.Floor(rule.Maximum)));
            else if (column.Type == DbType.Decimal)
                return decimal.Round(rule.Minimum + (decimal)random.NextDouble() * (rule.Maximum - rule.Minimum), column.EffectiveType.Scale ?? 2);
            else expression = new { op = "double", min = rule.Minimum, max = rule.Maximum, digits = 2 };
        }
        else if (rule.Generator == FieldGenerator.ValueList)
        {
            // Select before conversion: passing typed decimals or bytes through the JSON
            // expression evaluator would lose decimal precision or turn bytes into base64 text.
            int selected = random.Next(rule.Values.Count);
            if (rule.Distribution == FieldDistribution.Weighted)
            {
                double pick = random.NextDouble() * rule.Weights.Sum();
                selected = rule.Weights.FindLastIndex(w => w > 0);
                for (int i = 0; i < rule.Weights.Count; i++)
                {
                    pick -= rule.Weights[i];
                    if (pick < 0) { selected = i; break; }
                }
            }
            return ParseInput(rule.Values[selected], column);
        }
        else if (rule.Generator == FieldGenerator.DateTime)
        {
            DateTime date = profile.ReferenceUtc.AddSeconds(-random.NextInt64(0, checked((long)(rule.Distribution == FieldDistribution.Recent && random.NextDouble() < rule.Probability ? rule.RecentDays : rule.Days) * 86400)));
            return column.EffectiveType.Kind switch
            {
                SqlTypeKind.Date => date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                SqlTypeKind.Time => date.ToString("HH:mm:ss", CultureInfo.InvariantCulture),
                _ => date.ToString("O", CultureInfo.InvariantCulture),
            };
        }
        else
        {
            string? first = schema.Columns.FirstOrDefault(c => c.Name.Replace("_", "").Equals("FirstName", StringComparison.OrdinalIgnoreCase))?.Name;
            string? last = schema.Columns.FirstOrDefault(c => c.Name.Replace("_", "").Equals("LastName", StringComparison.OrdinalIgnoreCase))?.Name;
            if (rule.Generator is FieldGenerator.Email or FieldGenerator.FullName && first is not null && last is not null
                && values.GetValueOrDefault(first) is string firstName && values.GetValueOrDefault(last) is string lastName)
            {
                if (rule.Generator == FieldGenerator.FullName) return firstName + " " + lastName;
                string name = new string((firstName + "." + lastName).ToLowerInvariant().Where(c => char.IsAsciiLetterOrDigit(c) || c == '.').ToArray());
                return Email(name, number, Math.Min(column.EffectiveType.Length ?? rule.Length, rule.Length));
            }
            expression = new { op = "faker", name = FakerNames[rule.Generator] };
        }
        var table = new SqlTableSpec { GeneratorKey = tableId, Name = schema.TableName,
            Columns = [new SqlColumnSpec { Name = columnId, Generator = JsonSerializer.SerializeToElement(expression) }] };
        object? result = SpecDataGenerator.GenerateRow(new GenerationOptions { Seed = profile.Seed, ReferenceUtc = profile.ReferenceUtc, Locale = profile.Locale }, table, ordinal)[columnId];
        if (column.Type == DbType.Decimal && result is double real) return decimal.Round((decimal)real, column.EffectiveType.Scale ?? 2);
        if (column.Type == DbType.Integer && result is double integer) return checked((long)Math.Round(integer));
        // Text generation is explicitly sized; typed constants and lists are validated without truncation.
        if (rule.Generator == FieldGenerator.Email && result is string email)
            return Email(email.Split('@')[0], number, Math.Min(column.EffectiveType.Length ?? rule.Length, rule.Length));
        if (result is string text && FakerNames.ContainsKey(rule.Generator))
            return text.Length <= Math.Min(column.EffectiveType.Length ?? rule.Length, rule.Length) ? text : text[..Math.Min(column.EffectiveType.Length ?? rule.Length, rule.Length)];
        return result;
    }

    private static string Email(string name, long number, int maximumLength)
    {
        string suffix = $".{number}@example.test";
        if (maximumLength <= suffix.Length) throw new InvalidOperationException("The email field is too short for a valid generated address.");
        name = new string(name.Where(c => char.IsAsciiLetterOrDigit(c) || c == '.').ToArray()).Trim('.');
        if (name.Length == 0) name = "user";
        return name[..Math.Min(name.Length, maximumLength - suffix.Length)] + suffix;
    }

    private static object? ParseInput(string input, ColumnDefinition column) => column.Type switch
    {
        DbType.Integer when column.EffectiveType.Kind == SqlTypeKind.Boolean && bool.TryParse(input, out bool boolean) => boolean,
        DbType.Integer => long.Parse(input, CultureInfo.InvariantCulture),
        DbType.Real => double.Parse(input, CultureInfo.InvariantCulture),
        DbType.Decimal => decimal.Parse(input, CultureInfo.InvariantCulture),
        DbType.Blob when column.EffectiveType.Kind == SqlTypeKind.Uuid => Guid.Parse(input).ToByteArray(),
        DbType.Blob => Convert.FromHexString(input.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? input[2..] : input),
        _ => input,
    };
}
