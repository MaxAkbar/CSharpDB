using CSharpDB.Client.Grpc;
using CSharpDB.Client.Models;

namespace CSharpDB.Tests;

public sealed class GrpcLogicalTypeMapperTests
{
    [Fact]
    public void ColumnDefinition_RoundTripPreservesLogicalDescriptorFacets()
    {
        var column = new ColumnDefinition
        {
            Name = "amount",
            Type = DbType.Decimal,
            DeclaredType = new SqlTypeDescriptor
            {
                Kind = SqlTypeKind.Decimal,
                Precision = 18,
                Scale = 3,
            },
            Nullable = false,
        };

        ColumnDefinitionMessage message = GrpcModelMapper.ToMessage(column);
        ColumnDefinition roundTrip = GrpcModelMapper.ToModel(message);

        Assert.Equal(DbTypeEnum.DbTypeDecimal, message.Type);
        Assert.NotNull(message.DeclaredType);
        Assert.Equal(SqlTypeKindEnum.SqlTypeKindDecimal, message.DeclaredType.Kind);
        Assert.Equal(18, message.DeclaredType.Precision);
        Assert.Equal(3, message.DeclaredType.Scale);
        Assert.Equal(DbType.Decimal, roundTrip.Type);
        Assert.Equal(SqlTypeKind.Decimal, roundTrip.DeclaredType?.Kind);
        Assert.Equal(18, roundTrip.DeclaredType?.Precision);
        Assert.Equal(3, roundTrip.DeclaredType?.Scale);
    }

    [Fact]
    public void LegacyColumnDefinitionWithoutDescriptorRemainsLegacy()
    {
        var message = new ColumnDefinitionMessage
        {
            Name = "legacy_id",
            Type = DbTypeEnum.DbTypeInteger,
        };

        ColumnDefinition model = GrpcModelMapper.ToModel(message);

        Assert.Null(model.DeclaredType);
        Assert.Equal(SqlTypeKind.BigInt, model.EffectiveType.Kind);
    }

    [Fact]
    public void DecimalVariant_RoundTripDoesNotUseDouble()
    {
        const decimal value = 123456789012345.678m;

        VariantValue message = GrpcValueMapper.ToMessage(value);
        object? roundTrip = GrpcValueMapper.FromMessage(message);

        Assert.Equal(VariantValue.KindOneofCase.DecimalValue, message.KindCase);
        Assert.Equal("123456789012345.678", message.DecimalValue);
        Assert.Equal(value, Assert.IsType<decimal>(roundTrip));
    }

    [Fact]
    public void BitStringVariant_AddsLengthWithoutChangingBlobPayloadKind()
    {
        var value = new SqlBitString([0x80], bitLength: 1);

        VariantValue message = GrpcValueMapper.ToMessage(value);
        object? roundTrip = GrpcValueMapper.FromMessage(message);

        Assert.Equal(VariantValue.KindOneofCase.BytesValue, message.KindCase);
        Assert.Equal(1, message.BitLength);
        Assert.Equal(value, Assert.IsType<SqlBitString>(roundTrip));

        var legacyBlob = new VariantValue
        {
            BytesValue = Google.Protobuf.ByteString.CopyFrom([0x80]),
        };
        Assert.Equal(
            new byte[] { 0x80 },
            Assert.IsType<byte[]>(GrpcValueMapper.FromMessage(legacyBlob)));
    }
}
