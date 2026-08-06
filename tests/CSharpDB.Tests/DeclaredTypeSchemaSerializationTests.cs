using CSharpDB.Primitives;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Tests;

public sealed class DeclaredTypeSchemaSerializationTests
{
    [Fact]
    public void VersionTen_RoundTripsLogicalDescriptorsAndFacets()
    {
        SqlTypeDescriptor amountType = SqlTypeDescriptor.Create(
            SqlTypeKind.Decimal,
            precision: 18,
            scale: 4);
        SqlTypeDescriptor occurredAtType = SqlTypeDescriptor.Create(
            SqlTypeKind.TimestampWithTimeZone,
            fractionalSecondsPrecision: 6);
        var schema = new TableSchema
        {
            TableName = "payments",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "amount",
                    Type = amountType.StorageType,
                    DeclaredType = amountType,
                    Nullable = false,
                },
                new ColumnDefinition
                {
                    Name = "occurred_at",
                    Type = occurredAtType.StorageType,
                    DeclaredType = occurredAtType,
                    Nullable = false,
                },
                new ColumnDefinition
                {
                    Name = "legacy_note",
                    Type = DbType.Text,
                },
            ],
        };

        TableSchema decoded = SchemaSerializer.Deserialize(
            SchemaSerializer.Serialize(schema));

        Assert.Equal(amountType, decoded.Columns[0].DeclaredType);
        Assert.Equal(occurredAtType, decoded.Columns[1].DeclaredType);
        Assert.Null(decoded.Columns[2].DeclaredType);
        Assert.Equal(SqlTypeKind.Text, decoded.Columns[2].EffectiveType.Kind);
    }

    [Fact]
    public void Serialize_RejectsLogicalAndPhysicalTypeMismatch()
    {
        var schema = new TableSchema
        {
            TableName = "invalid",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "value",
                    Type = DbType.Text,
                    DeclaredType = SqlTypeDescriptor.Create(SqlTypeKind.BigInt),
                },
            ],
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => SchemaSerializer.Serialize(schema));
        Assert.Contains("physical type", error.Message, StringComparison.OrdinalIgnoreCase);
    }
}
