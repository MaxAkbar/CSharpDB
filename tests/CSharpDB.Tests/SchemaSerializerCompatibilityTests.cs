using System.Text;
using CSharpDB.Primitives;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Tests;

public sealed class SchemaSerializerCompatibilityTests
{
    [Fact]
    public void SerializeDeserialize_TableSchema_RoundTripsNextRowId()
    {
        var schema = new TableSchema
        {
            TableName = "users",
            Columns = new[]
            {
                new ColumnDefinition { Name = "id", Type = DbType.Integer, IsPrimaryKey = true, IsIdentity = true, Nullable = false },
                new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = true },
            },
            NextRowId = 1234,
        };

        byte[] encoded = SchemaSerializer.Serialize(schema);
        var decoded = SchemaSerializer.Deserialize(encoded);

        Assert.Equal("users", decoded.TableName);
        Assert.Equal(2, decoded.Columns.Count);
        Assert.Equal(1234L, decoded.NextRowId);
        Assert.True(decoded.Columns[0].IsIdentity);
    }

    [Fact]
    public void Deserialize_LegacyPayloadWithoutNextRowId_DefaultsToUnknown()
    {
        byte[] legacy = BuildLegacyTableSchemaPayload(
            tableName: "legacy_users",
            columns: new[]
            {
                new ColumnDefinition { Name = "id", Type = DbType.Integer, IsPrimaryKey = true, Nullable = false },
                new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = true },
            });

        var decoded = SchemaSerializer.Deserialize(legacy);

        Assert.Equal("legacy_users", decoded.TableName);
        Assert.Equal(2, decoded.Columns.Count);
        Assert.Equal(0L, decoded.NextRowId);
        Assert.True(decoded.Columns[0].IsIdentity);
    }

    private static byte[] BuildLegacyTableSchemaPayload(string tableName, IReadOnlyList<ColumnDefinition> columns)
    {
        using var ms = new MemoryStream();
        WriteVarint(ms, (ulong)Encoding.UTF8.GetByteCount(tableName));
        ms.Write(Encoding.UTF8.GetBytes(tableName));
        WriteVarint(ms, (ulong)columns.Count);

        foreach (var col in columns)
        {
            WriteVarint(ms, (ulong)Encoding.UTF8.GetByteCount(col.Name));
            ms.Write(Encoding.UTF8.GetBytes(col.Name));
            ms.WriteByte((byte)col.Type);

            byte flags = 0;
            if (col.Nullable)
                flags |= 0x01;
            if (col.IsPrimaryKey)
                flags |= 0x02;
            ms.WriteByte(flags);
        }

        return ms.ToArray();
    }

    private static void WriteVarint(Stream stream, ulong value)
    {
        Span<byte> buffer = stackalloc byte[10];
        int len = Varint.Write(buffer, value);
        stream.Write(buffer[..len]);
    }
}
