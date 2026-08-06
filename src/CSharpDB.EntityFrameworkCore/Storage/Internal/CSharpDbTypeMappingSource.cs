using System.Data;
using System.Globalization;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace CSharpDB.EntityFrameworkCore.Storage.Internal;

public sealed class CSharpDbTypeMappingSource : RelationalTypeMappingSource
{
    private static readonly BoolTypeMapping BoolMapping = new("BOOLEAN", DbType.Boolean);
    private static readonly BoolTypeMapping BitBoolMapping = new("BIT", DbType.Boolean);
    private static readonly ByteTypeMapping ByteMapping = new("TINYINT", DbType.Byte);
    private static readonly ShortTypeMapping ShortMapping = new("SMALLINT", DbType.Int16);
    private static readonly IntTypeMapping IntMapping = new("INTEGER", DbType.Int32);
    private static readonly LongTypeMapping LongMapping = new("BIGINT", DbType.Int64);
    private static readonly FloatTypeMapping FloatMapping = new("REAL", DbType.Single);
    private static readonly DoubleTypeMapping DoubleMapping = new("DOUBLE PRECISION", DbType.Double);
    private static readonly StringTypeMapping TextMapping = new("TEXT", DbType.String);
    private static readonly StringTypeMapping JsonMapping = new("JSON", DbType.String);
    private static readonly StringTypeMapping XmlMapping = new("XML", DbType.Xml);
    private static readonly ByteArrayTypeMapping BlobMapping = new("BLOB", DbType.Binary);
    private static readonly ByteArrayTypeMapping RowVersionMapping =
        new("ROWVERSION", DbType.Binary, sizeof(long));
    private static readonly GuidTypeMapping GuidMapping = new("UUID", DbType.Guid);
    private static readonly DateOnlyTypeMapping DateOnlyMapping = new("DATE", DbType.Date);
    private static readonly TimeOnlyTypeMapping TimeOnlyMapping = new("TIME", DbType.Time);
    private static readonly DateTimeTypeMapping DateTimeMapping = new("DATETIME2", DbType.DateTime2);
    private static readonly DateTimeOffsetTypeMapping DateTimeOffsetMapping =
        new("DATETIMEOFFSET", DbType.DateTimeOffset);
    private static readonly ValueConverter<TimeSpan, string> IntervalDayToSecondConverter =
        new(
            value => value.ToString("c", CultureInfo.InvariantCulture),
            value => TimeSpan.Parse(value, CultureInfo.InvariantCulture),
            new ConverterMappingHints(size: 48));
    private static readonly StringTypeMapping IntervalYearToMonthMapping =
        new("INTERVAL YEAR TO MONTH", DbType.String, unicode: true, size: 32);
    private static readonly RelationalTypeMapping IntervalDayToSecondMapping =
        CreateIntervalDayToSecondMapping("INTERVAL DAY TO SECOND");
    private static readonly ByteArrayTypeMapping BitVaryingMapping =
        new("BIT VARYING", DbType.Binary);

    // Explicit legacy physical store types continue to use the pre-v4.5
    // provider representations so existing EF models do not reinterpret data.
    private static readonly LongTypeMapping LegacyIntegerMapping = new("INTEGER", DbType.Int64);
    private static readonly DoubleTypeMapping LegacyRealMapping = new("REAL", DbType.Double);
    private static readonly StringTypeMapping LegacyTextMapping = new("TEXT", DbType.String);

    private static readonly RelationalTypeMapping SByteMapping = Compose(IntMapping, new ValueConverter<sbyte, int>(value => value, value => checked((sbyte)value)));
    private static readonly RelationalTypeMapping UShortMapping = Compose(IntMapping, new ValueConverter<ushort, int>(value => value, value => checked((ushort)value)));
    private static readonly RelationalTypeMapping UIntMapping = Compose(LongMapping, new ValueConverter<uint, long>(value => value, value => checked((uint)value)));
    private static readonly RelationalTypeMapping ULongMapping = Compose(LongMapping, new ValueConverter<ulong, long>(value => checked((long)value), value => checked((ulong)value)));
    private static readonly RelationalTypeMapping LegacyGuidMapping = Compose(
        LegacyTextMapping,
        new ValueConverter<Guid, string>(
            value => CSharpDB.Primitives.CSharpDbTextCodec.FormatGuid(value),
            value => CSharpDB.Primitives.CSharpDbTextCodec.ParseGuid(value),
            new ConverterMappingHints(size: 36)));
    private static readonly RelationalTypeMapping LegacyDateTimeMapping = Compose(
        LegacyTextMapping,
        new ValueConverter<DateTime, string>(
            value => CSharpDB.Primitives.CSharpDbTextCodec.FormatDateTime(value),
            value => CSharpDB.Primitives.CSharpDbTextCodec.ParseDateTime(value),
            new ConverterMappingHints(size: 48)));
    private static readonly RelationalTypeMapping LegacyDateTimeOffsetMapping = Compose(
        LegacyTextMapping,
        new ValueConverter<DateTimeOffset, string>(
            value => CSharpDB.Primitives.CSharpDbTextCodec.FormatDateTimeOffset(value),
            value => CSharpDB.Primitives.CSharpDbTextCodec.ParseDateTimeOffset(value),
            new ConverterMappingHints(size: 48)));
    private static readonly RelationalTypeMapping LegacyDateOnlyMapping = Compose(
        LegacyTextMapping,
        new ValueConverter<DateOnly, string>(
            value => CSharpDB.Primitives.CSharpDbTextCodec.FormatDate(value),
            value => CSharpDB.Primitives.CSharpDbTextCodec.ParseDate(value),
            new ConverterMappingHints(size: 10)));
    private static readonly RelationalTypeMapping LegacyTimeOnlyMapping = Compose(
        LegacyTextMapping,
        new ValueConverter<TimeOnly, string>(
            value => CSharpDB.Primitives.CSharpDbTextCodec.FormatTime(value),
            value => CSharpDB.Primitives.CSharpDbTextCodec.ParseTime(value),
            new ConverterMappingHints(size: 48)));

    private static readonly Dictionary<string, RelationalTypeMapping> StoreTypeMappings = new(StringComparer.OrdinalIgnoreCase)
    {
        ["BOOLEAN"] = BoolMapping,
        ["BOOL"] = BoolMapping,
        ["TINYINT"] = ByteMapping,
        ["SMALLINT"] = ShortMapping,
        ["INTEGER"] = IntMapping,
        ["INT"] = IntMapping,
        ["BIGINT"] = LongMapping,
        ["REAL"] = FloatMapping,
        ["DOUBLE"] = DoubleMapping,
        ["FLOAT"] = DoubleMapping,
        ["TEXT"] = TextMapping,
        ["JSON"] = JsonMapping,
        ["XML"] = XmlMapping,
        ["BLOB"] = BlobMapping,
        ["ROWVERSION"] = RowVersionMapping,
        ["UUID"] = GuidMapping,
        ["DATE"] = DateOnlyMapping,
        ["TIME"] = TimeOnlyMapping,
        ["DATETIME"] = DateTimeMapping,
        ["DATETIME2"] = DateTimeMapping,
        ["DATETIMEOFFSET"] = DateTimeOffsetMapping,
        ["TIMESTAMP WITH TIME ZONE"] = DateTimeOffsetMapping,
        ["INTERVAL YEAR TO MONTH"] = IntervalYearToMonthMapping,
        ["INTERVAL DAY TO SECOND"] = IntervalDayToSecondMapping,
        ["BIT"] = BitBoolMapping,
        ["BIT VARYING"] = BitVaryingMapping,
        ["VARBIT"] = BitVaryingMapping,
    };

    public CSharpDbTypeMappingSource(
        TypeMappingSourceDependencies dependencies,
        RelationalTypeMappingSourceDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        Type? clrType = mappingInfo.ClrType;
        Type? unwrappedClrType = clrType is null
            ? null
            : Nullable.GetUnderlyingType(clrType) ?? clrType;

        if (mappingInfo.IsRowVersion == true)
        {
            if (unwrappedClrType != typeof(byte[]))
                return null;

            if (string.IsNullOrWhiteSpace(mappingInfo.StoreTypeNameBase) ||
                IsStoreType(mappingInfo.StoreTypeNameBase, "ROWVERSION"))
            {
                return RowVersionMapping;
            }

            if (IsStoreType(mappingInfo.StoreTypeNameBase, "BLOB"))
                return BlobMapping;
        }

        if (IsStoreType(mappingInfo.StoreTypeNameBase, "BIT") &&
            !mappingInfo.Size.HasValue)
        {
            return unwrappedClrType is null || unwrappedClrType == typeof(bool)
                ? BitBoolMapping
                : null;
        }

        if (unwrappedClrType == typeof(decimal))
        {
            (int precision, int scale) =
                CSharpDbDecimalStorage.ResolveFacets(
                    mappingInfo.Precision,
                    mappingInfo.Scale);
            if (IsStoreType(mappingInfo.StoreTypeNameBase, "INTEGER"))
            {
                return Compose(
                    LegacyIntegerMapping,
                    new CSharpDbDecimalToInt64Converter(
                        precision,
                        scale));
            }

            return new DecimalTypeMapping(
                $"DECIMAL({precision},{scale})",
                DbType.Decimal,
                precision,
                scale);
        }

        if (IsDecimalStoreType(mappingInfo.StoreTypeNameBase))
        {
            (int precision, int scale) =
                CSharpDbDecimalStorage.ResolveFacets(
                    mappingInfo.Precision,
                    mappingInfo.Scale);
            return new DecimalTypeMapping(
                $"DECIMAL({precision},{scale})",
                DbType.Decimal,
                precision,
                scale);
        }

        if (unwrappedClrType == typeof(string) &&
            string.IsNullOrWhiteSpace(mappingInfo.StoreTypeNameBase))
        {
            if (mappingInfo.Size.HasValue ||
                mappingInfo.IsFixedLength == true)
            {
                bool fixedLength = mappingInfo.IsFixedLength == true;
                string storeType = mappingInfo.Size is int boundedLength
                    ? $"{(fixedLength ? "CHAR" : "VARCHAR")}({boundedLength})"
                    : "CHAR";
                return new StringTypeMapping(
                    storeType,
                    fixedLength ? DbType.StringFixedLength : DbType.String,
                    unicode: true,
                    size: mappingInfo.Size);
            }

            return TextMapping;
        }

        if (unwrappedClrType == typeof(byte[]) &&
            string.IsNullOrWhiteSpace(mappingInfo.StoreTypeNameBase))
        {
            if (mappingInfo.Size.HasValue ||
                mappingInfo.IsFixedLength == true)
            {
                bool fixedLength = mappingInfo.IsFixedLength == true;
                string storeType = mappingInfo.Size is int boundedLength
                    ? $"{(fixedLength ? "BINARY" : "VARBINARY")}({boundedLength})"
                    : "BINARY";
                return new ByteArrayTypeMapping(
                    storeType,
                    DbType.Binary,
                    mappingInfo.Size);
            }

            return BlobMapping;
        }

        if (mappingInfo.Precision is int fractionalSecondsPrecision &&
            string.IsNullOrWhiteSpace(mappingInfo.StoreTypeNameBase))
        {
            if (unwrappedClrType == typeof(TimeOnly))
            {
                return new TimeOnlyTypeMapping(
                    $"TIME({fractionalSecondsPrecision})",
                    DbType.Time);
            }
            if (unwrappedClrType == typeof(DateTime))
            {
                return new DateTimeTypeMapping(
                    $"DATETIME2({fractionalSecondsPrecision})",
                    DbType.DateTime2);
            }
            if (unwrappedClrType == typeof(DateTimeOffset))
            {
                return new DateTimeOffsetTypeMapping(
                    $"DATETIMEOFFSET({fractionalSecondsPrecision})",
                    DbType.DateTimeOffset);
            }
            if (unwrappedClrType == typeof(TimeSpan))
            {
                return CreateIntervalDayToSecondMapping(
                    $"INTERVAL DAY TO SECOND({fractionalSecondsPrecision})");
            }
        }

        if (TryCreateFacetedStoreTypeMapping(
                mappingInfo,
                out RelationalTypeMapping? facetedStoreTypeMapping))
        {
            return facetedStoreTypeMapping;
        }

        if (!string.IsNullOrWhiteSpace(mappingInfo.StoreTypeNameBase)
            && StoreTypeMappings.TryGetValue(mappingInfo.StoreTypeNameBase, out var storeTypeMapping))
        {
            return AdaptLegacyStoreType(
                storeTypeMapping,
                mappingInfo.StoreTypeNameBase,
                unwrappedClrType);
        }

        if (clrType is null)
            return null;

        if (clrType.IsEnum)
            return CreateEnumMapping(clrType);

        return clrType switch
        {
            var type when type == typeof(bool) => BoolMapping,
            var type when type == typeof(byte) => ByteMapping,
            var type when type == typeof(sbyte) => SByteMapping,
            var type when type == typeof(short) => ShortMapping,
            var type when type == typeof(ushort) => UShortMapping,
            var type when type == typeof(int) => IntMapping,
            var type when type == typeof(uint) => UIntMapping,
            var type when type == typeof(long) => LongMapping,
            var type when type == typeof(ulong) => ULongMapping,
            var type when type == typeof(float) => FloatMapping,
            var type when type == typeof(double) => DoubleMapping,
            var type when type == typeof(string) => TextMapping,
            var type when type == typeof(Guid) => GuidMapping,
            var type when type == typeof(DateTime) => DateTimeMapping,
            var type when type == typeof(DateTimeOffset) => DateTimeOffsetMapping,
            var type when type == typeof(DateOnly) => DateOnlyMapping,
            var type when type == typeof(TimeOnly) => TimeOnlyMapping,
            var type when type == typeof(TimeSpan) => IntervalDayToSecondMapping,
            var type when type == typeof(byte[]) => BlobMapping,
            _ => null,
        };
    }

    private static RelationalTypeMapping AdaptLegacyStoreType(
        RelationalTypeMapping mapping,
        string storeType,
        Type? clrType)
    {
        if (clrType is null)
            return mapping;

        if (IsStoreType(storeType, "INTEGER"))
        {
            if (clrType == typeof(bool))
                return new BoolTypeMapping("INTEGER", DbType.Boolean);
            if (clrType == typeof(long))
                return LegacyIntegerMapping;
        }
        if (IsStoreType(storeType, "REAL") && clrType == typeof(double))
            return LegacyRealMapping;
        if (IsStoreType(storeType, "TEXT"))
        {
            if (clrType == typeof(Guid))
                return LegacyGuidMapping;
            if (clrType == typeof(DateTime))
                return LegacyDateTimeMapping;
            if (clrType == typeof(DateTimeOffset))
                return LegacyDateTimeOffsetMapping;
            if (clrType == typeof(DateOnly))
                return LegacyDateOnlyMapping;
            if (clrType == typeof(TimeOnly))
                return LegacyTimeOnlyMapping;
        }

        return mapping;
    }

    private static bool TryCreateFacetedStoreTypeMapping(
        RelationalTypeMappingInfo mappingInfo,
        out RelationalTypeMapping? mapping)
    {
        mapping = null;
        string? baseName = mappingInfo.StoreTypeNameBase?.Trim().ToUpperInvariant();
        string? storeType = mappingInfo.StoreTypeName?.Trim();
        if (string.IsNullOrWhiteSpace(baseName) ||
            string.IsNullOrWhiteSpace(storeType))
        {
            return false;
        }

        switch (baseName)
        {
            case "CHAR":
            case "CHARACTER":
                mapping = new StringTypeMapping(
                    storeType,
                    DbType.StringFixedLength,
                    unicode: true,
                    size: mappingInfo.Size);
                return true;
            case "VARCHAR":
            case "CHARACTER VARYING":
                mapping = new StringTypeMapping(
                    storeType,
                    DbType.String,
                    unicode: true,
                    size: mappingInfo.Size);
                return true;
            case "BINARY":
            case "VARBINARY":
            case "BINARY VARYING":
                mapping = new ByteArrayTypeMapping(
                    storeType,
                    DbType.Binary,
                    mappingInfo.Size);
                return true;
            case "BIT":
            case "BIT VARYING":
            case "VARBIT":
                mapping = new ByteArrayTypeMapping(
                    storeType,
                    DbType.Binary,
                    mappingInfo.Size);
                return true;
            case "TIME":
                mapping = new TimeOnlyTypeMapping(
                    storeType,
                    DbType.Time);
                return true;
            case "DATETIME":
            case "DATETIME2":
                mapping = new DateTimeTypeMapping(
                    storeType,
                    DbType.DateTime2);
                return true;
            case "DATETIMEOFFSET":
                mapping = new DateTimeOffsetTypeMapping(
                    storeType,
                    DbType.DateTimeOffset);
                return true;
            case "TIMESTAMP":
            case "TIMESTAMP WITH TIME ZONE":
                if (!storeType.Contains(
                    "WITH TIME ZONE",
                    StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }

                mapping = new DateTimeOffsetTypeMapping(
                        storeType,
                        DbType.DateTimeOffset);
                return true;
            case "INTERVAL YEAR TO MONTH":
                mapping = new StringTypeMapping(
                    storeType,
                    DbType.String,
                    unicode: true,
                    size: 32);
                return true;
            case "INTERVAL DAY TO SECOND":
                mapping = UnwrapNullableType(mappingInfo.ClrType) == typeof(TimeSpan)
                    ? CreateIntervalDayToSecondMapping(storeType)
                    : new StringTypeMapping(
                        storeType,
                        DbType.String,
                        unicode: true,
                        size: 48);
                return true;
            default:
                return false;
        }
    }

    private static bool IsStoreType(string? actual, string expected) =>
        !string.IsNullOrWhiteSpace(actual) &&
        string.Equals(actual.Trim(), expected, StringComparison.OrdinalIgnoreCase);

    private static bool IsDecimalStoreType(string? actual) =>
        IsStoreType(actual, "DECIMAL") ||
        IsStoreType(actual, "DEC") ||
        IsStoreType(actual, "NUMERIC");

    private static Type? UnwrapNullableType(Type? clrType) =>
        clrType is null ? null : Nullable.GetUnderlyingType(clrType) ?? clrType;

    private static RelationalTypeMapping CreateIntervalDayToSecondMapping(
        string storeType) =>
        Compose(
            new StringTypeMapping(
                storeType,
                DbType.String,
                unicode: true,
                size: 48),
            IntervalDayToSecondConverter);

    private static RelationalTypeMapping Compose(RelationalTypeMapping baseMapping, ValueConverter converter)
        => (RelationalTypeMapping)baseMapping.WithComposedConverter(converter);

    private static RelationalTypeMapping CreateEnumMapping(Type enumType)
    {
        Type underlyingType = Enum.GetUnderlyingType(enumType);
        RelationalTypeMapping numericMapping = underlyingType switch
        {
            var type when type == typeof(byte) => ByteMapping,
            var type when type == typeof(sbyte) => SByteMapping,
            var type when type == typeof(short) => ShortMapping,
            var type when type == typeof(ushort) => UShortMapping,
            var type when type == typeof(int) => IntMapping,
            var type when type == typeof(uint) => UIntMapping,
            var type when type == typeof(long) => LongMapping,
            var type when type == typeof(ulong) => ULongMapping,
            _ => throw new NotSupportedException($"Enum underlying type '{underlyingType.Name}' is not supported by the CSharpDB EF Core provider."),
        };

        Type converterType = typeof(EnumToNumberConverter<,>).MakeGenericType(enumType, underlyingType);
        var converter = (ValueConverter)Activator.CreateInstance(converterType)!;
        return (RelationalTypeMapping)numericMapping.WithComposedConverter(converter);
    }
}
