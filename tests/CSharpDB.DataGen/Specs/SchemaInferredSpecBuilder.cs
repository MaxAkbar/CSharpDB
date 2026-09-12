using System.Text.Json;
using CSharpDB.Engine;
using CSharpDB.Primitives;

namespace CSharpDB.DataGen.Specs;

/// <summary>
/// Reads an existing CSharpDB database and builds a <see cref="DatasetSpec"/>
/// from its table schemas and indexes. Column-name heuristics drive the choice
/// of generation rules so the synthetic data looks realistic.
/// </summary>
public static class SchemaInferredSpecBuilder
{
    public static async Task<DatasetSpec> BuildFromDatabaseAsync(
        string sourceDatabasePath,
        CancellationToken ct = default)
    {
        await using Database db = await Database.OpenAsync(sourceDatabasePath, ct);

        IReadOnlyCollection<string> tableNames = db.GetTableNames();
        IReadOnlyCollection<IndexSchema> allIndexes = db.GetIndexes();

        var spec = new DatasetSpec { Dataset = "fromdb" };

        foreach (string tableName in tableNames)
        {
            if (!CSharpDB.DataGeneration.GenerationPlan.IsUserTable(tableName)) continue;
            TableSchema? schema = db.GetTableSchema(tableName);
            if (schema is null)
                continue;

            IReadOnlyList<IndexSchema> tableIndexes = allIndexes
                .Where(idx => string.Equals(idx.TableName, tableName, StringComparison.OrdinalIgnoreCase)
                              && idx.Kind == IndexKind.Sql)
                .ToList();

            spec.Tables.Add(BuildTableSpec(schema, tableIndexes));
        }

        return spec;
    }

    private static SqlTableSpec BuildTableSpec(TableSchema schema, IReadOnlyList<IndexSchema> indexes)
    {
        string key = schema.TableName.ToLowerInvariant();
        var locals = new List<RuleBindingSpec>();
        var columns = new List<SqlColumnSpec>();

        // Create a rowIndex local for identity/PK columns.
        locals.Add(new RuleBindingSpec
        {
            Name = "id",
            Value = ParseRule("""{"op":"rowIndex"}"""),
        });

        foreach (ColumnDefinition col in schema.Columns)
        {
            string typeName = col.EffectiveType.ToSql();
            JsonElement generator = InferGenerator(col, schema);

            columns.Add(new SqlColumnSpec
            {
                Name = col.Name,
                Type = typeName,
                Nullable = col.Nullable,
                PrimaryKey = col.IsPrimaryKey,
                Generator = generator,
            });
        }

        var indexSpecs = indexes.Select(idx => new SqlIndexSpec
        {
            Name = idx.IndexName,
            Columns = idx.Columns.ToList(),
            Unique = idx.IsUnique,
        }).ToList();

        return new SqlTableSpec
        {
            GeneratorKey = key,
            Name = schema.TableName,
            OutputFileName = $"{key}.csv",
            RowCount = ParseRule("""{"op":"option","name":"rowCount"}"""),
            Locals = locals,
            Columns = columns,
            Indexes = indexSpecs,
        };
    }

    private static JsonElement InferGenerator(ColumnDefinition col, TableSchema schema)
    {
        var type = col.EffectiveType;
        // Logical type takes precedence over names such as Email INTEGER or Amount TEXT.
        if (type.Kind == SqlTypeKind.Boolean) return WrapNullable(ParseRule("""{"op":"bool","probability":0.7}"""), col);
        if (type.Kind == SqlTypeKind.Uuid) return WrapNullable(ParseRule("""{"op":"guid"}"""), col);
        if (type.Kind is SqlTypeKind.Json or SqlTypeKind.Xml)
            return WrapNullable(JsonSerializer.SerializeToElement(type.Kind == SqlTypeKind.Json ? "{}" : "<item />"), col);
        if (type.Kind is SqlTypeKind.Date or SqlTypeKind.Time or SqlTypeKind.Timestamp or SqlTypeKind.TimestampWithTimeZone)
        {
            string format = type.Kind == SqlTypeKind.Date ? "{0:yyyy-MM-dd}" : type.Kind == SqlTypeKind.Time ? "{0:HH:mm:ss}" : "{0:O}";
            return WrapNullable(JsonSerializer.SerializeToElement(new { op = "format", format, args = new[] { ParseRule("""{"op":"skewedTimestamp","recentWindowDays":30,"fullWindowDays":365,"recentRate":0.8}""") } }), col);
        }
        if (type.Kind is SqlTypeKind.IntervalYearToMonth or SqlTypeKind.IntervalDayToSecond or SqlTypeKind.Bit or SqlTypeKind.VarBit)
            throw new InvalidOperationException($"{schema.TableName}.{col.Name}: automatic inference does not support {type}. Supply an explicit dataset specification.");
        // Identity / auto-increment primary keys use rowIndex.
        if (col.IsPrimaryKey && (col.IsIdentity || col.Type == Primitives.DbType.Integer))
            return ParseRule("""{"op":"value","name":"id"}""");

        string name = col.Name.ToLowerInvariant();
        string typeName = MapDbType(col.Type);

        if (typeName == "INTEGER")
        {
            if (name.StartsWith("is") || name.StartsWith("has")) return WrapNullable(ParseRule("""{"op":"bool","probability":0.7}"""), col);
            int maximum = type.Kind == SqlTypeKind.TinyInt ? 100 : type.Kind == SqlTypeKind.SmallInt ? 1000 : 1000000;
            return WrapNullable(JsonSerializer.SerializeToElement(new { op = "int", min = 1, max = maximum }), col);
        }
        if (typeName is "REAL" or "DECIMAL")
        {
            int scale = type.Scale ?? 2;
            double maximum = type.Kind == SqlTypeKind.Decimal ? Math.Min(1000, Math.Pow(10, (type.Precision ?? 18) - scale) - Math.Pow(10, -scale)) : 1000;
            return WrapNullable(JsonSerializer.SerializeToElement(new { op = "double", min = 0, max = maximum, digits = Math.Min(scale, 15) }), col);
        }
        if (typeName == "BLOB")
            return WrapNullable(JsonSerializer.SerializeToElement(new { op = "sizedText", length = Math.Min(type.Length ?? 64, 64) }), col);

        // Try name-based heuristics first.
        JsonElement? heuristic = TryNameHeuristic(name, typeName, col, schema);
        if (heuristic.HasValue)
            return WrapNullable(FitText(heuristic.Value, col), col);

        // Fall back to type-based defaults.
        JsonElement fallback = typeName switch
        {
            "INTEGER" => ParseRule("""{"op":"int","min":1,"max":1000000}"""),
            "REAL" or "DECIMAL" => ParseRule("""{"op":"double","min":0,"max":10000,"digits":2}"""),
            "TEXT" => ParseRule("""{"op":"faker","name":"lorem.sentence"}"""),
            "BLOB" => ParseRule("""{"op":"sizedText","length":64}"""),
            _ => ParseRule("""{"op":"faker","name":"lorem.word"}"""),
        };

        return WrapNullable(FitText(fallback, col), col);
    }

    private static JsonElement FitText(JsonElement rule, ColumnDefinition column)
        => column.EffectiveType.Length is int length
            ? JsonSerializer.SerializeToElement(new { op = "truncate", length, value = rule }) : rule;

    private static JsonElement? TryNameHeuristic(string name, string typeName, ColumnDefinition col, TableSchema schema)
    {
        // Foreign-key pattern: ends with "id" and references another table's PK.
        if (name.EndsWith("id", StringComparison.Ordinal) && !col.IsPrimaryKey && typeName == "INTEGER")
        {
            // Try to guess a reasonable range based on option rowCount.
            return ParseRule("""{"op":"int","min":1,"max":{"op":"option","name":"rowCount"}}""");
        }

        // Email
        if (name.Contains("email"))
            return ParseRule("""{"op":"faker","name":"internet.email"}""");

        // First name
        if (name is "firstname" or "first_name" or "fname")
            return ParseRule("""{"op":"faker","name":"name.firstName"}""");

        // Last name
        if (name is "lastname" or "last_name" or "lname" or "surname")
            return ParseRule("""{"op":"faker","name":"name.lastName"}""");

        // Full name
        if (name is "name" or "fullname" or "full_name" or "displayname" or "display_name")
            return ParseRule("""{"op":"faker","name":"name.fullName"}""");

        // Phone
        if (name.Contains("phone") || name.Contains("mobile") || name.Contains("fax"))
            return ParseRule("""{"op":"faker","name":"phone.phoneNumber"}""");

        // Address fields
        if (name is "street" or "street1" or "address1" or "addressline1" or "address_line_1")
            return ParseRule("""{"op":"faker","name":"address.streetAddress"}""");
        if (name is "city")
            return ParseRule("""{"op":"faker","name":"address.city"}""");
        if (name is "state" or "province" or "region")
            return ParseRule("""{"op":"faker","name":"address.state"}""");
        if (name is "country")
            return ParseRule("""{"op":"faker","name":"address.country"}""");
        if (name is "zip" or "zipcode" or "zip_code" or "postalcode" or "postal_code" or "postcode")
            return ParseRule("""{"op":"faker","name":"address.zipCode"}""");

        // Company
        if (name is "company" or "companyname" or "company_name" or "organization" or "org")
            return ParseRule("""{"op":"faker","name":"company.companyName"}""");

        // URL / website
        if (name.Contains("url") || name.Contains("website") || name.Contains("homepage"))
            return ParseRule("""{"op":"faker","name":"internet.url"}""");

        // Username
        if (name is "username" or "user_name" or "login")
            return ParseRule("""{"op":"faker","name":"internet.userName"}""");

        // Description / notes / comments / bio
        if (name is "description" or "desc" or "notes" or "comment" or "comments" or "bio" or "summary")
            return ParseRule("""{"op":"faker","name":"lorem.paragraph"}""");

        // Title
        if (name is "title" or "subject" or "headline")
            return ParseRule("""{"op":"faker","name":"lorem.sentence"}""");

        // SKU / code
        if (name is "sku" or "code" or "productcode" or "product_code" or "barcode")
            return ParseRule("""{"op":"guid"}""");

        // Status fields
        if (name is "status" or "state")
            return ParseRule("""{"op":"pick","values":["Active","Inactive","Pending","Archived"]}""");

        // Type / kind / category
        if (name is "type" or "kind" or "category")
            return ParseRule("""{"op":"pick","values":["TypeA","TypeB","TypeC","TypeD"]}""");

        // Boolean-like fields
        if (name.StartsWith("is") || name.StartsWith("has") || name is "active" or "enabled" or "deleted" or "verified" or "published")
        {
            if (typeName == "INTEGER")
                return ParseRule("""{"op":"if","condition":{"op":"bool","probability":0.7},"then":1,"else":0}""");
            return ParseRule("""{"op":"bool","probability":0.7}""");
        }

        // Timestamp / date fields
        if (name.Contains("date") || name.Contains("time") || name.EndsWith("utc") || name.EndsWith("at")
            || name is "created" or "updated" or "modified" or "timestamp")
        {
            return ParseRule("""{"op":"skewedTimestamp","recentWindowDays":30,"fullWindowDays":365,"recentRate":0.8}""");
        }

        // Price / cost / amount / total monetary values
        if (name.Contains("price") || name.Contains("cost") || name.Contains("amount")
            || name.Contains("total") || name.Contains("subtotal") || name.Contains("tax")
            || name.Contains("fee") || name.Contains("balance") || name.Contains("salary"))
        {
            return ParseRule("""{"op":"money","min":1,"max":10000}""");
        }

        // Quantity / count
        if (name is "qty" or "quantity" or "count" or "units" or "stock")
            return ParseRule("""{"op":"int","min":1,"max":100}""");

        // Percentage / rate
        if (name.Contains("percent") || name.Contains("rate") || name.Contains("ratio") || name.Contains("score"))
            return ParseRule("""{"op":"double","min":0,"max":100,"digits":2}""");

        // Currency code
        if (name is "currency" or "currencycode" or "currency_code")
            return ParseRule("""{"op":"pick","values":["USD","EUR","GBP","CAD","AUD","JPY"]}""");

        // Tenant
        if (name is "tenantid" or "tenant_id" or "tenant")
            return ParseRule("""{"op":"format","format":"tenant-{0:D4}","args":[{"op":"int","min":1,"max":{"op":"option","name":"tenantCount"}}]}""");

        // GUID / UUID fields
        if (name.Contains("guid") || name.Contains("uuid") || name is "externalid" or "external_id" or "correlationid" or "correlation_id")
            return ParseRule("""{"op":"guid"}""");

        return null;
    }

    private static JsonElement WrapNullable(JsonElement inner, ColumnDefinition col)
    {
        if (!col.Nullable)
            return inner;

        // Nullable columns use a chance rule to produce nulls ~5% of the time.
        string json = $$"""{"op":"chance","probability":0.95,"then":{{inner.GetRawText()}},"else":null}""";
        return ParseRule(json);
    }

    private static string MapDbType(Primitives.DbType type) => type switch
    {
        Primitives.DbType.Integer => "INTEGER",
        Primitives.DbType.Real => "REAL",
        Primitives.DbType.Decimal => "DECIMAL",
        Primitives.DbType.Text => "TEXT",
        Primitives.DbType.Blob => "BLOB",
        _ => "TEXT",
    };

    private static JsonElement ParseRule(string json)
        => JsonDocument.Parse(json).RootElement.Clone();
}
