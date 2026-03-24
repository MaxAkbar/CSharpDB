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

    [Fact]
    public void SerializeDeserialize_IndexSchema_RoundTripsFullTextMetadata()
    {
        var schema = new IndexSchema
        {
            IndexName = "fts_docs",
            TableName = "docs",
            Columns = ["title", "body"],
            IsUnique = false,
            Kind = IndexKind.FullText,
            State = IndexState.Building,
            OwnerIndexName = "fts_docs_owner",
            OptionsJson = "{\"lowercaseInvariant\":true}",
        };

        byte[] encoded = SchemaSerializer.SerializeIndex(schema);
        var decoded = SchemaSerializer.DeserializeIndex(encoded);

        Assert.Equal("fts_docs", decoded.IndexName);
        Assert.Equal("docs", decoded.TableName);
        Assert.Equal(["title", "body"], decoded.Columns);
        Assert.False(decoded.IsUnique);
        Assert.Equal(IndexKind.FullText, decoded.Kind);
        Assert.Equal(IndexState.Building, decoded.State);
        Assert.Equal("fts_docs_owner", decoded.OwnerIndexName);
        Assert.Equal("{\"lowercaseInvariant\":true}", decoded.OptionsJson);
    }

    [Fact]
    public void Deserialize_LegacyIndexPayloadWithoutMetadata_DefaultsToSqlReady()
    {
        byte[] legacy = BuildLegacyIndexSchemaPayload(
            indexName: "idx_users_name",
            tableName: "users",
            columns: ["name"],
            isUnique: true);

        var decoded = SchemaSerializer.DeserializeIndex(legacy);

        Assert.Equal("idx_users_name", decoded.IndexName);
        Assert.Equal("users", decoded.TableName);
        Assert.Equal(["name"], decoded.Columns);
        Assert.True(decoded.IsUnique);
        Assert.Equal(IndexKind.Sql, decoded.Kind);
        Assert.Equal(IndexState.Ready, decoded.State);
        Assert.Null(decoded.OwnerIndexName);
        Assert.Null(decoded.OptionsJson);
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

    private static byte[] BuildLegacyIndexSchemaPayload(
        string indexName,
        string tableName,
        IReadOnlyList<string> columns,
        bool isUnique)
    {
        using var ms = new MemoryStream();
        WriteVarint(ms, (ulong)Encoding.UTF8.GetByteCount(indexName));
        ms.Write(Encoding.UTF8.GetBytes(indexName));
        WriteVarint(ms, (ulong)Encoding.UTF8.GetByteCount(tableName));
        ms.Write(Encoding.UTF8.GetBytes(tableName));
        WriteVarint(ms, (ulong)columns.Count);

        foreach (string column in columns)
        {
            WriteVarint(ms, (ulong)Encoding.UTF8.GetByteCount(column));
            ms.Write(Encoding.UTF8.GetBytes(column));
        }

        ms.WriteByte(isUnique ? (byte)1 : (byte)0);
        return ms.ToArray();
    }

    private static void WriteVarint(Stream stream, ulong value)
    {
        Span<byte> buffer = stackalloc byte[10];
        int len = Varint.Write(buffer, value);
        stream.Write(buffer[..len]);
    }
}
