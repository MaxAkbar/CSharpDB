using System.Data;
using CSharpDB.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace CSharpDB.EntityFrameworkCore.Storage.Internal;

public sealed class CSharpDbTypeMappingSource : RelationalTypeMappingSource
{
    private static readonly BoolTypeMapping BoolMapping = new("INTEGER", DbType.Boolean);
    private static readonly ByteTypeMapping ByteMapping = new("INTEGER", DbType.Byte);
    private static readonly ShortTypeMapping ShortMapping = new("INTEGER", DbType.Int16);
    private static readonly IntTypeMapping IntMapping = new("INTEGER", DbType.Int32);
    private static readonly LongTypeMapping LongMapping = new("INTEGER", DbType.Int64);
    private static readonly FloatTypeMapping FloatMapping = new("REAL", DbType.Single);
    private static readonly DoubleTypeMapping DoubleMapping = new("REAL", DbType.Double);
    private static readonly StringTypeMapping TextMapping = new("TEXT", DbType.String);
    private static readonly ByteArrayTypeMapping BlobMapping = new("BLOB", DbType.Binary);

    private static readonly RelationalTypeMapping SByteMapping = Compose(LongMapping, new ValueConverter<sbyte, long>(value => value, value => checked((sbyte)value)));
    private static readonly RelationalTypeMapping UShortMapping = Compose(LongMapping, new ValueConverter<ushort, long>(value => value, value => checked((ushort)value)));
    private static readonly RelationalTypeMapping UIntMapping = Compose(LongMapping, new ValueConverter<uint, long>(value => value, value => checked((uint)value)));
    private static readonly RelationalTypeMapping ULongMapping = Compose(LongMapping, new ValueConverter<ulong, long>(value => checked((long)value), value => checked((ulong)value)));
    private static readonly RelationalTypeMapping GuidMapping = Compose(TextMapping, new GuidToStringConverter());
    private static readonly RelationalTypeMapping DateTimeMapping = Compose(TextMapping, new DateTimeToStringConverter());
    private static readonly RelationalTypeMapping DateTimeOffsetMapping = Compose(TextMapping, new DateTimeOffsetToStringConverter());
    private static readonly RelationalTypeMapping DateOnlyMapping = Compose(TextMapping, new DateOnlyToStringConverter());
    private static readonly RelationalTypeMapping TimeOnlyMapping = Compose(TextMapping, new TimeOnlyToStringConverter());

    private static readonly Dictionary<string, RelationalTypeMapping> StoreTypeMappings = new(StringComparer.OrdinalIgnoreCase)
    {
        ["INTEGER"] = LongMapping,
        ["REAL"] = DoubleMapping,
        ["TEXT"] = TextMapping,
        ["BLOB"] = BlobMapping,
    };

    public CSharpDbTypeMappingSource(
        TypeMappingSourceDependencies dependencies,
        RelationalTypeMappingSourceDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        if (!string.IsNullOrWhiteSpace(mappingInfo.StoreTypeNameBase)
            && StoreTypeMappings.TryGetValue(mappingInfo.StoreTypeNameBase, out var storeTypeMapping))
        {
            return storeTypeMapping;
        }

        Type? clrType = mappingInfo.ClrType;
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
            var type when type == typeof(byte[]) => BlobMapping,
            var type when type == typeof(decimal) => null,
            _ => null,
        };
    }

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
