using System.Buffers.Binary;
using System.Globalization;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Canonicalization;

public sealed record CanonicalFieldContract
{
    public required string SourceColumnObjectId { get; init; }

    public required string TargetColumnName { get; init; }

    public required DbType StoredType { get; init; }

    public required CanonicalType CanonicalType { get; init; }

    public CanonicalExclusionReason? ExclusionReason { get; init; }

    public string? ConversionId { get; init; }

    public IReadOnlyList<MigrationCatalogFacet> ConversionParameters { get; init; } = [];
}

public sealed record CanonicalRowContract
{
    public required string SourceObjectId { get; init; }

    public required string TargetObjectId { get; init; }

    public IReadOnlyList<CanonicalFieldContract> Fields { get; init; } = [];

    public IReadOnlyList<int> KeyFieldOrdinals { get; init; } = [];

    public required string ObjectContractDigest { get; init; }

    public bool IsKeyed => KeyFieldOrdinals.Count > 0;
}

/// <summary>
/// Projects persisted CSharpDB values back into the planned logical domains
/// used by <c>csharpdb-canon-v1</c>. Source adapters use the same projection
/// after their planned conversion, preventing physical re-encodings such as a
/// BOOLEAN integer or decimal text from producing false differences.
/// </summary>
public static class CanonicalRowProjector
{
    private static readonly byte[] s_objectDomain = Encoding.ASCII.GetBytes("CSDBOBJ1");
    private static readonly byte[] s_csharpDbTableDomain = Encoding.ASCII.GetBytes("CSDBNAT1");

    /// <summary>
    /// Creates a canonical row contract for a native CSharpDB table. The
    /// contract describes the stored row layout but deliberately excludes the
    /// table identity, allowing the same archive contract to validate a
    /// staging table whose table name has changed during restore. Its object
    /// digest uses the rename-stable <c>CSDBNAT1</c> native row-layout
    /// aggregate domain.
    /// </summary>
    public static CanonicalRowContract CreateCSharpDbTableContract(TableSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        if (string.IsNullOrWhiteSpace(schema.TableName))
            throw new InvalidDataException("A native CSharpDB canonical contract requires a table name.");
        if (schema.Columns is null || schema.Columns.Count == 0)
            throw new InvalidDataException("A native CSharpDB canonical contract requires at least one column.");

        var columnOrdinals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var fields = new CanonicalFieldContract[schema.Columns.Count];
        int rowVersionCount = 0;
        for (int ordinal = 0; ordinal < schema.Columns.Count; ordinal++)
        {
            ColumnDefinition column = schema.Columns[ordinal]
                ?? throw new InvalidDataException("The native CSharpDB schema contains a null column.");
            if (string.IsNullOrWhiteSpace(column.Name))
                throw new InvalidDataException("A native CSharpDB schema column has no name.");
            if (!columnOrdinals.TryAdd(column.Name, ordinal))
                throw new InvalidDataException($"The native CSharpDB schema repeats column '{column.Name}'.");

            CanonicalType canonicalType = ResolveNativeCanonicalType(column);
            if (column.IsRowVersion)
            {
                if (column.Type != DbType.Blob)
                {
                    throw new InvalidDataException(
                        $"Native CSharpDB ROWVERSION column '{column.Name}' must use BLOB storage.");
                }
                if (column.Nullable)
                {
                    throw new InvalidDataException(
                        $"Native CSharpDB ROWVERSION column '{column.Name}' must be non-nullable.");
                }
                if (column.IsIdentity)
                {
                    throw new InvalidDataException(
                        $"Native CSharpDB ROWVERSION column '{column.Name}' cannot be an identity column.");
                }
                if (++rowVersionCount > 1)
                {
                    throw new InvalidDataException(
                        "A native CSharpDB schema cannot contain more than one ROWVERSION column.");
                }
            }

            fields[ordinal] = new CanonicalFieldContract
            {
                SourceColumnObjectId = $"csharpdb:native-column:{ordinal}",
                TargetColumnName = column.Name,
                StoredType = column.Type,
                CanonicalType = canonicalType,
                ExclusionReason = column.IsRowVersion
                    ? CanonicalExclusionReason.RegeneratedRowVersion
                    : null,
            };
        }

        int[] keyOrdinals = ResolveCSharpDbPrimaryKeyOrdinals(schema, columnOrdinals);
        foreach (int ordinal in keyOrdinals)
        {
            ColumnDefinition keyColumn = schema.Columns[ordinal];
            if (keyColumn.IsRowVersion)
                throw new InvalidDataException("A native CSharpDB ROWVERSION column cannot be a primary-key field.");
            if (keyColumn.Nullable)
            {
                throw new InvalidDataException(
                    $"Native CSharpDB primary-key column '{keyColumn.Name}' must be non-nullable.");
            }
        }

        var contract = new CanonicalRowContract
        {
            SourceObjectId = "csharpdb:native-table",
            TargetObjectId = "csharpdb:native-table",
            Fields = fields,
            KeyFieldOrdinals = keyOrdinals,
            ObjectContractDigest = string.Empty,
        };
        return contract with
        {
            ObjectContractDigest = ComputeCSharpDbTableContractDigest(contract),
        };
    }

    public static CanonicalRowContract CreateContract(
        MigrationPlan plan,
        MigrationCatalog catalog,
        string sourceObjectId)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceObjectId);
        if (!string.Equals(
                plan.Validation.CanonicalizationVersion,
                CanonicalRowCodec.CanonicalizationId,
                StringComparison.Ordinal))
        {
            throw new NotSupportedException(
                $"Canonicalization version '{plan.Validation.CanonicalizationVersion}' is not supported.");
        }

        IReadOnlyDictionary<string, MigrationPlanObject> planned = plan.Objects
            .ToDictionary(item => item.SourceObjectId, StringComparer.Ordinal);
        MigrationCatalogObject table = catalog.Objects.SingleOrDefault(item =>
                string.Equals(item.ObjectId, sourceObjectId, StringComparison.Ordinal))
            ?? throw new InvalidDataException($"Validation object '{sourceObjectId}' is absent from the catalog.");
        if (table.Kind is not (MigrationObjectKind.Table or MigrationObjectKind.Collection) ||
            !planned.TryGetValue(sourceObjectId, out MigrationPlanObject? tablePlan) ||
            !tablePlan.Included || string.IsNullOrWhiteSpace(tablePlan.TargetName))
        {
            throw new InvalidDataException(
                $"Validation object '{sourceObjectId}' is not an included table or collection.");
        }

        MigrationCatalogObject[] columns = catalog.Objects
            .Where(item => item.Kind == MigrationObjectKind.Column &&
                string.Equals(item.ParentObjectId, sourceObjectId, StringComparison.Ordinal) &&
                planned.TryGetValue(item.ObjectId, out MigrationPlanObject? value) && value.Included)
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .ToArray();
        if (columns.Length == 0)
            throw new InvalidDataException($"Validation object '{sourceObjectId}' has no included fields.");

        CanonicalFieldContract[] fields = columns
            .Select(column => CreateFieldContract(column, planned[column.ObjectId]))
            .ToArray();
        int[] keyOrdinals = ResolvePrimaryKeyColumns(table, catalog, planned)
            .Select(columnId => Array.FindIndex(columns, column =>
                string.Equals(column.ObjectId, columnId, StringComparison.Ordinal)))
            .Where(index => index >= 0)
            .ToArray();

        var contract = new CanonicalRowContract
        {
            SourceObjectId = sourceObjectId,
            TargetObjectId = tablePlan.TargetName,
            Fields = fields,
            KeyFieldOrdinals = keyOrdinals,
            ObjectContractDigest = string.Empty,
        };
        return contract with { ObjectContractDigest = ComputeObjectContractDigest(contract) };
    }

    public static CanonicalValue[] ProjectRow(
        CanonicalRowContract contract,
        IReadOnlyList<DbValue> values)
    {
        ArgumentNullException.ThrowIfNull(contract);
        ArgumentNullException.ThrowIfNull(values);
        if (values.Count != contract.Fields.Count)
        {
            throw new InvalidDataException(
                $"Validation row for '{contract.SourceObjectId}' has {values.Count} fields; expected {contract.Fields.Count}.");
        }

        var projected = new CanonicalValue[values.Count];
        for (int index = 0; index < values.Count; index++)
            projected[index] = ProjectValue(contract.Fields[index], values[index]);
        return projected;
    }

    public static CanonicalValue[] ProjectKey(
        CanonicalRowContract contract,
        IReadOnlyList<CanonicalValue> projectedRow)
    {
        ArgumentNullException.ThrowIfNull(contract);
        ArgumentNullException.ThrowIfNull(projectedRow);
        if (!contract.IsKeyed)
            return [];
        if (projectedRow.Count != contract.Fields.Count)
            throw new InvalidDataException("Projected validation row does not match its object contract.");

        var key = new CanonicalValue[contract.KeyFieldOrdinals.Count];
        for (int index = 0; index < key.Length; index++)
        {
            int ordinal = contract.KeyFieldOrdinals[index];
            if ((uint)ordinal >= projectedRow.Count)
                throw new InvalidDataException("Canonical key field ordinal is outside the row contract.");
            CanonicalValue value = projectedRow[ordinal];
            if (value.State != CanonicalFieldState.Value)
                throw new InvalidDataException("Canonical primary-key fields must contain non-excluded values.");
            key[index] = value;
        }
        return key;
    }

    public static CanonicalValue ProjectValue(CanonicalFieldContract contract, DbValue value)
    {
        ArgumentNullException.ThrowIfNull(contract);
        if (contract.ExclusionReason is CanonicalExclusionReason.RegeneratedRowVersion)
            return CanonicalValue.RegeneratedRowVersion();
        if (value.IsNull)
            return CanonicalValue.Null(contract.CanonicalType);
        if (value.Type != contract.StoredType)
        {
            throw new InvalidDataException(
                $"Stored validation value for '{contract.SourceColumnObjectId}' is {value.Type}; expected {contract.StoredType}.");
        }

        try
        {
            return contract.CanonicalType switch
            {
                CanonicalType.Boolean => CanonicalValue.Boolean(value.AsInteger switch
                {
                    0 => false,
                    1 => true,
                    _ => throw new InvalidDataException("Canonical BOOLEAN storage must be exactly 0 or 1."),
                }),
                CanonicalType.Int64 => CanonicalValue.Int64(value.Type == DbType.Integer
                    ? value.AsInteger
                    : long.Parse(value.AsText, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture)),
                CanonicalType.UInt64 => CanonicalValue.UInt64(value.Type == DbType.Integer
                    ? checked((ulong)value.AsInteger)
                    : ulong.Parse(value.AsText, NumberStyles.None, CultureInfo.InvariantCulture)),
                CanonicalType.Decimal => ProjectDecimal(contract, value),
                CanonicalType.Binary32 => ProjectBinary32(value.AsReal),
                CanonicalType.Binary64 => CanonicalValue.Binary64(value.Type == DbType.Real
                    ? value.AsReal
                    : double.Parse(value.AsText, NumberStyles.Float, CultureInfo.InvariantCulture)),
                CanonicalType.Text => CanonicalValue.Text(value.AsText),
                CanonicalType.Blob => CanonicalValue.Blob(value.AsBlob),
                CanonicalType.Guid => CanonicalValue.Guid(
                    value.Type == DbType.Blob
                        ? new Guid(value.AsBlob, bigEndian: true)
                        : Guid.ParseExact(value.AsText, "D")),
                CanonicalType.Date => CanonicalValue.Date(CSharpDbTextCodec.ParseDate(value.AsText)),
                CanonicalType.Time => CanonicalValue.Time(CSharpDbTextCodec.ParseTime(value.AsText)),
                CanonicalType.WallDateTime => CanonicalValue.WallDateTime(DateTime.SpecifyKind(
                    CSharpDbTextCodec.ParseDateTime(value.AsText),
                    DateTimeKind.Unspecified)),
                CanonicalType.UtcInstant => CanonicalValue.UtcInstant(ParseUtcInstant(value.AsText)),
                CanonicalType.OffsetDateTime => CanonicalValue.OffsetDateTime(
                    CSharpDbTextCodec.ParseDateTimeOffset(value.AsText)),
                _ => throw new InvalidDataException(
                    $"Canonical type '{contract.CanonicalType}' is not projectable."),
            };
        }
        catch (Exception error) when (error is FormatException or OverflowException or ArgumentException)
        {
            throw new InvalidDataException(
                $"Stored value for '{contract.SourceColumnObjectId}' does not match its canonical logical type.",
                error);
        }
    }

    private static CanonicalFieldContract CreateFieldContract(
        MigrationCatalogObject column,
        MigrationPlanObject planned)
    {
        MigrationTypeMapping mapping = planned.TypeMappings.Single();
        if (mapping.TargetType is not DbType storedType || storedType == DbType.Null)
            throw new InvalidDataException($"Included validation field '{column.ObjectId}' has no persistent target type.");

        bool rowVersion = bool.TryParse(Facet(column, "rowVersion"), out bool parsedRowVersion) && parsedRowVersion;
        CanonicalType logicalType = ResolveCanonicalType(column, mapping);
        if (rowVersion && (storedType != DbType.Blob || logicalType != CanonicalType.Blob))
            throw new InvalidDataException("A regenerated rowversion exclusion must use BLOB storage.");

        return new CanonicalFieldContract
        {
            SourceColumnObjectId = column.ObjectId,
            TargetColumnName = planned.TargetName ?? throw new InvalidDataException(
                $"Included validation field '{column.ObjectId}' has no target name."),
            StoredType = storedType,
            CanonicalType = logicalType,
            ExclusionReason = rowVersion ? CanonicalExclusionReason.RegeneratedRowVersion : null,
            ConversionId = mapping.Conversion?.ConversionId,
            ConversionParameters = mapping.Conversion?.Parameters
                .OrderBy(item => item.Name, StringComparer.Ordinal)
                .ToArray() ?? [],
        };
    }

    private static CanonicalType ResolveCanonicalType(
        MigrationCatalogObject column,
        MigrationTypeMapping mapping)
    {
        if (CSharpDbDeclaredTypeContract.TryRead(
                column,
                out SqlTypeDescriptor declaredType))
        {
            return ResolveDeclaredCanonicalType(declaredType);
        }

        string logicalType = (Facet(column, "logicalType") ?? string.Empty)
            .Replace("_", string.Empty, StringComparison.Ordinal)
            .Replace("-", string.Empty, StringComparison.Ordinal)
            .ToUpperInvariant();
        string? conversionId = mapping.Conversion?.ConversionId;

        if (conversionId is "unsigned-integer-binary64" or "decimal-binary64" or "numeric-binary64")
            return CanonicalType.Binary64;
        if (conversionId == "canonical-text" && logicalType is "JSON" or "XML")
            return CanonicalType.Text;

        return logicalType switch
        {
            "BOOLEAN" => CanonicalType.Boolean,
            "SIGNEDINTEGER" => CanonicalType.Int64,
            "UNSIGNEDINTEGER" => CanonicalType.UInt64,
            "DECIMAL" or "NUMERIC" => CanonicalType.Decimal,
            "FLOATINGPOINT" => IsBinary32(column) ? CanonicalType.Binary32 : CanonicalType.Binary64,
            "TEXT" or "CHARACTER" or "STRING" or "JSON" or "XML" => CanonicalType.Text,
            "BINARY" or "BLOB" or "ROWVERSION" => CanonicalType.Blob,
            "GUID" or "UUID" => CanonicalType.Guid,
            "DATE" => CanonicalType.Date,
            "TIME" => CanonicalType.Time,
            "DATETIME" => CanonicalType.WallDateTime,
            "UTCINSTANT" => CanonicalType.UtcInstant,
            "DATETIMEOFFSET" or "OFFSETDATETIME" => CanonicalType.OffsetDateTime,
            _ => throw new NotSupportedException(
                $"Field '{column.ObjectId}' logical type '{logicalType}' has no csharpdb-canon-v1 projection."),
        };
    }

    private static CanonicalValue ProjectDecimal(CanonicalFieldContract contract, DbValue value)
    {
        if (value.Type == DbType.Decimal)
        {
            return CanonicalValue.Decimal(
                new BigInteger(value.DecimalCoefficient),
                checked((uint)value.DecimalScale));
        }
        if (value.Type == DbType.Integer)
        {
            uint scale = checked((uint)ParameterInt(contract, "scale"));
            return CanonicalValue.Decimal(new BigInteger(value.AsInteger), scale);
        }
        if (value.Type == DbType.Text)
        {
            ParseDecimal(value.AsText, out BigInteger coefficient, out uint scale);
            return CanonicalValue.Decimal(coefficient, scale);
        }
        throw new InvalidDataException("Exact canonical DECIMAL values require scaled INTEGER or canonical TEXT storage.");
    }

    private static CanonicalType ResolveNativeCanonicalType(
        ColumnDefinition column)
    {
        if (column.DeclaredType is SqlTypeDescriptor declaredType)
            return ResolveDeclaredCanonicalType(declaredType);

        return column.Type switch
        {
            DbType.Integer => CanonicalType.Int64,
            DbType.Real => CanonicalType.Binary64,
            DbType.Text => CanonicalType.Text,
            DbType.Blob => CanonicalType.Blob,
            DbType.Decimal => CanonicalType.Decimal,
            _ => throw new InvalidDataException(
                $"Native CSharpDB column '{column.Name}' has no persistent canonical type."),
        };
    }

    private static CanonicalType ResolveDeclaredCanonicalType(
        SqlTypeDescriptor descriptor) => descriptor.Kind switch
    {
        SqlTypeKind.Boolean => CanonicalType.Boolean,
        SqlTypeKind.TinyInt or
        SqlTypeKind.SmallInt or
        SqlTypeKind.Integer or
        SqlTypeKind.BigInt => CanonicalType.Int64,
        // REAL and DOUBLE PRECISION both use the stable binary64 payload in
        // CSharpDB. Canonicalization must describe the persisted value rather
        // than narrow it to the SQL spelling's traditional binary32 domain.
        SqlTypeKind.Real => CanonicalType.Binary64,
        SqlTypeKind.Double => CanonicalType.Binary64,
        SqlTypeKind.Decimal => CanonicalType.Decimal,
        SqlTypeKind.Char or
        SqlTypeKind.VarChar or
        SqlTypeKind.Text or
        SqlTypeKind.IntervalYearToMonth or
        SqlTypeKind.IntervalDayToSecond or
        SqlTypeKind.Json or
        SqlTypeKind.Xml => CanonicalType.Text,
        SqlTypeKind.Binary or
        SqlTypeKind.VarBinary or
        SqlTypeKind.Blob or
        SqlTypeKind.Bit or
        SqlTypeKind.VarBit => CanonicalType.Blob,
        SqlTypeKind.Uuid => CanonicalType.Guid,
        SqlTypeKind.Date => CanonicalType.Date,
        SqlTypeKind.Time => CanonicalType.Time,
        SqlTypeKind.Timestamp => CanonicalType.WallDateTime,
        SqlTypeKind.TimestampWithTimeZone => CanonicalType.OffsetDateTime,
        _ => throw new InvalidDataException(
            $"Declared SQL type '{descriptor.ToSql()}' has no canonical migration type."),
    };

    private static CanonicalValue ProjectBinary32(double value)
    {
        float narrowed = checked((float)value);
        if (!float.IsFinite(narrowed) || (double)narrowed != value)
            throw new InvalidDataException("Stored REAL cannot be represented exactly as planned binary32.");
        return CanonicalValue.Binary32(narrowed);
    }

    private static DateTimeOffset ParseUtcInstant(string value)
    {
        DateTimeOffset parsed = DateTimeOffset.Parse(
            value,
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
        if (!value.EndsWith('Z'))
            throw new FormatException("UTC instant text must have an explicit Z suffix.");
        return parsed;
    }

    private static void ParseDecimal(string text, out BigInteger coefficient, out uint scale)
    {
        if (string.IsNullOrEmpty(text))
            throw new FormatException("Canonical decimal text is empty.");
        int offset = text[0] == '-' ? 1 : 0;
        bool negative = offset == 1;
        if (offset == text.Length)
            throw new FormatException("Canonical decimal text has no digits.");

        int decimalPoint = text.IndexOf('.', offset);
        ReadOnlySpan<char> integer = decimalPoint < 0
            ? text.AsSpan(offset)
            : text.AsSpan(offset, decimalPoint - offset);
        ReadOnlySpan<char> fraction = decimalPoint < 0
            ? ReadOnlySpan<char>.Empty
            : text.AsSpan(decimalPoint + 1);
        if (integer.IsEmpty || (decimalPoint >= 0 && fraction.IsEmpty) ||
            !AllDigits(integer) || !AllDigits(fraction))
        {
            throw new FormatException("Canonical decimal text is not invariant base-10.");
        }

        string digits = string.Concat(integer, fraction);
        coefficient = BigInteger.Parse(digits, NumberStyles.None, CultureInfo.InvariantCulture);
        if (negative)
            coefficient = -coefficient;
        scale = checked((uint)fraction.Length);
    }

    private static bool AllDigits(ReadOnlySpan<char> value)
    {
        foreach (char character in value)
        {
            if (character is < '0' or > '9')
                return false;
        }
        return true;
    }

    private static int ParameterInt(CanonicalFieldContract contract, string name)
    {
        string? value = contract.ConversionParameters
            .SingleOrDefault(item => string.Equals(item.Name, name, StringComparison.Ordinal))?.Value;
        return value is not null && int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out int parsed)
            ? parsed
            : throw new InvalidDataException(
                $"Canonical field '{contract.SourceColumnObjectId}' is missing integer parameter '{name}'.");
    }

    private static IReadOnlyList<string> ResolvePrimaryKeyColumns(
        MigrationCatalogObject table,
        MigrationCatalog catalog,
        IReadOnlyDictionary<string, MigrationPlanObject> planned)
    {
        MigrationCatalogObject? key = catalog.Objects
            .Where(item => item.Kind == MigrationObjectKind.Key &&
                string.Equals(item.ParentObjectId, table.ObjectId, StringComparison.Ordinal) &&
                planned.TryGetValue(item.ObjectId, out MigrationPlanObject? value) && value.Included)
            .Where(item => NormalizeToken(Facet(item, "kind")) is "primary" or "primary-key")
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .FirstOrDefault();
        if (key is null)
            return [];

        MigrationObjectReference[] members = key.Members
            .Where(item => string.Equals(item.Role, MigrationObjectReferenceRoles.Column, StringComparison.Ordinal))
            .OrderBy(item => item.Ordinal)
            .ToArray();
        return members.Length > 0
            ? members.Select(item => item.ObjectId).ToArray()
            : key.DependsOn.Count == 1 ? key.DependsOn : [];
    }

    private static int[] ResolveCSharpDbPrimaryKeyOrdinals(
        TableSchema schema,
        IReadOnlyDictionary<string, int> columnOrdinals)
    {
        if (schema.KeyConstraints is null)
            throw new InvalidDataException("The native CSharpDB key-constraint collection is null.");

        int[]? primaryKeyOrdinals = null;
        foreach (KeyConstraintDefinition key in schema.KeyConstraints)
        {
            if (key is null)
                throw new InvalidDataException("The native CSharpDB key-constraint collection contains a null entry.");
            if (key.Kind is not (KeyConstraintKind.PrimaryKey or KeyConstraintKind.Unique))
            {
                throw new InvalidDataException(
                    $"The native CSharpDB schema contains unsupported key-constraint kind '{(int)key.Kind}'.");
            }

            int[] keyOrdinals = ResolveCSharpDbKeyOrdinals(key, columnOrdinals);
            foreach (int ordinal in keyOrdinals)
            {
                if (schema.Columns[ordinal].IsRowVersion)
                {
                    throw new InvalidDataException(
                        "A native CSharpDB ROWVERSION column cannot participate in a key constraint.");
                }
            }

            if (key.Kind != KeyConstraintKind.PrimaryKey)
                continue;
            if (primaryKeyOrdinals is not null)
                throw new InvalidDataException("The native CSharpDB schema contains more than one primary key.");
            primaryKeyOrdinals = keyOrdinals;
        }

        if (primaryKeyOrdinals is null)
        {
            return schema.Columns
                .Select(static (column, ordinal) => (column, ordinal))
                .Where(static item => item.column.IsPrimaryKey)
                .Select(static item => item.ordinal)
                .ToArray();
        }

        return primaryKeyOrdinals;
    }

    private static int[] ResolveCSharpDbKeyOrdinals(
        KeyConstraintDefinition key,
        IReadOnlyDictionary<string, int> columnOrdinals)
    {
        if (key.Columns is null || key.Columns.Count == 0)
            throw new InvalidDataException("A native CSharpDB key constraint has no columns.");

        var seen = new HashSet<int>();
        var ordinals = new int[key.Columns.Count];
        for (int index = 0; index < key.Columns.Count; index++)
        {
            string columnName = key.Columns[index];
            if (string.IsNullOrWhiteSpace(columnName) ||
                !columnOrdinals.TryGetValue(columnName, out int ordinal))
            {
                throw new InvalidDataException(
                    $"A native CSharpDB key constraint references missing column '{columnName}'.");
            }
            if (!seen.Add(ordinal))
            {
                throw new InvalidDataException(
                    $"A native CSharpDB key constraint repeats column '{columnName}'.");
            }
            ordinals[index] = ordinal;
        }
        return ordinals;
    }

    private static string ComputeCSharpDbTableContractDigest(CanonicalRowContract contract)
    {
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(s_csharpDbTableDomain);
        hash.AppendData(Convert.FromHexString(CanonicalRowCodec.ContractHashHex));
        AppendUInt32(hash, checked((uint)contract.Fields.Count));
        foreach (CanonicalFieldContract field in contract.Fields)
        {
            AppendString(hash, field.TargetColumnName);
            hash.AppendData([(byte)field.StoredType, (byte)field.CanonicalType]);
            hash.AppendData(field.ExclusionReason is null
                ? [0]
                : [1, (byte)field.ExclusionReason.Value]);
        }
        AppendUInt32(hash, checked((uint)contract.KeyFieldOrdinals.Count));
        foreach (int ordinal in contract.KeyFieldOrdinals)
            AppendUInt32(hash, checked((uint)ordinal));
        return Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant();
    }

    private static string ComputeObjectContractDigest(CanonicalRowContract contract)
    {
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(s_objectDomain);
        hash.AppendData(Convert.FromHexString(CanonicalRowCodec.ContractHashHex));
        AppendString(hash, contract.SourceObjectId);
        AppendString(hash, contract.TargetObjectId);
        AppendUInt32(hash, checked((uint)contract.Fields.Count));
        foreach (CanonicalFieldContract field in contract.Fields)
        {
            AppendString(hash, field.SourceColumnObjectId);
            AppendString(hash, field.TargetColumnName);
            hash.AppendData([(byte)field.StoredType, (byte)field.CanonicalType]);
            hash.AppendData(field.ExclusionReason is null
                ? [0]
                : [1, (byte)field.ExclusionReason.Value]);
            AppendNullableString(hash, field.ConversionId);
            AppendUInt32(hash, checked((uint)field.ConversionParameters.Count));
            foreach (MigrationCatalogFacet parameter in field.ConversionParameters)
            {
                AppendString(hash, parameter.Name);
                AppendString(hash, parameter.Value ?? throw new InvalidDataException(
                    $"Canonical field '{field.SourceColumnObjectId}' has a null conversion parameter."));
            }
        }
        AppendUInt32(hash, checked((uint)contract.KeyFieldOrdinals.Count));
        foreach (int ordinal in contract.KeyFieldOrdinals)
            AppendUInt32(hash, checked((uint)ordinal));
        return Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant();
    }

    private static bool IsBinary32(MigrationCatalogObject column) =>
        Facet(column, "binaryWidth") == "32" || Facet(column, "bits") == "32";

    private static string? Facet(MigrationCatalogObject item, string name) => item.Facets
        .SingleOrDefault(facet => string.Equals(facet.Name, name, StringComparison.Ordinal))?.Value;

    private static string NormalizeToken(string? value) => (value ?? string.Empty)
        .Trim()
        .Replace('_', '-')
        .ToLowerInvariant();

    private static void AppendNullableString(IncrementalHash hash, string? value)
    {
        hash.AppendData(value is null ? [0] : [1]);
        if (value is not null)
            AppendString(hash, value);
    }

    private static void AppendString(IncrementalHash hash, string value)
    {
        byte[] bytes = new UTF8Encoding(false, true).GetBytes(value);
        AppendUInt32(hash, checked((uint)bytes.Length));
        hash.AppendData(bytes);
    }

    private static void AppendUInt32(IncrementalHash hash, uint value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(uint)];
        BinaryPrimitives.WriteUInt32BigEndian(bytes, value);
        hash.AppendData(bytes);
    }
}
