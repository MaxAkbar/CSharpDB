using System.Text;
using CSharpDB.Core;

namespace CSharpDB.Storage.Serialization;

/// <summary>
/// Serializes/deserializes TableSchema to/from bytes for storage in the catalog B+tree.
/// Format: [nameLen:varint][nameUtf8][colCount:varint] then per column:
///   [nameLen:varint][nameUtf8][type:1][flags:1 (bit0=nullable, bit1=isPK, bit2=isIdentity)]
/// </summary>
public static class SchemaSerializer
{
    private const byte NullableFlag = 0x01;
    private const byte PrimaryKeyFlag = 0x02;
    private const byte IdentityFlag = 0x04;

    public static byte[] Serialize(TableSchema schema)
    {
        var ms = new MemoryStream();
        var nameBytes = Encoding.UTF8.GetBytes(schema.TableName);
        WriteVarint(ms, (ulong)nameBytes.Length);
        ms.Write(nameBytes);
        WriteVarint(ms, (ulong)schema.Columns.Count);

        foreach (var col in schema.Columns)
        {
            var colNameBytes = Encoding.UTF8.GetBytes(col.Name);
            WriteVarint(ms, (ulong)colNameBytes.Length);
            ms.Write(colNameBytes);
            ms.WriteByte((byte)col.Type);
            byte flags = 0;
            if (col.Nullable) flags |= NullableFlag;
            if (col.IsPrimaryKey) flags |= PrimaryKeyFlag;
            if (col.IsIdentity) flags |= IdentityFlag;
            ms.WriteByte(flags);
        }

        // Optional trailing metadata for forward-compatible schema evolution.
        // 0 means unknown/uninitialized (legacy compatibility path).
        ulong nextRowId = schema.NextRowId > 0 ? (ulong)schema.NextRowId : 0UL;
        WriteVarint(ms, nextRowId);

        return ms.ToArray();
    }

    public static TableSchema Deserialize(ReadOnlySpan<byte> data)
    {
        int pos = 0;
        int nameLen = (int)Varint.Read(data[pos..], out int nb);
        pos += nb;
        string tableName = Encoding.UTF8.GetString(data.Slice(pos, nameLen));
        pos += nameLen;

        int colCount = (int)Varint.Read(data[pos..], out int cb);
        pos += cb;

        var columns = new ColumnDefinition[colCount];
        for (int i = 0; i < colCount; i++)
        {
            int colNameLen = (int)Varint.Read(data[pos..], out int cnb);
            pos += cnb;
            string colName = Encoding.UTF8.GetString(data.Slice(pos, colNameLen));
            pos += colNameLen;
            var type = (DbType)data[pos++];
            byte flags = data[pos++];
            bool isPrimaryKey = (flags & PrimaryKeyFlag) != 0;
            bool hasIdentityFlag = (flags & IdentityFlag) != 0;
            columns[i] = new ColumnDefinition
            {
                Name = colName,
                Type = type,
                Nullable = (flags & NullableFlag) != 0,
                IsPrimaryKey = isPrimaryKey,
                // Backward compatibility: historical INTEGER PRIMARY KEY behavior auto-generated rowid.
                // Legacy payloads lack the identity bit, so infer identity for INTEGER PK columns.
                IsIdentity = hasIdentityFlag || (isPrimaryKey && type == DbType.Integer),
            };
        }

        long nextRowId = 0;
        if (pos < data.Length)
        {
            ulong storedNextRowId = Varint.Read(data[pos..], out _);
            if (storedNextRowId <= long.MaxValue)
                nextRowId = (long)storedNextRowId;
        }

        return new TableSchema
        {
            TableName = tableName,
            Columns = columns,
            NextRowId = nextRowId,
        };
    }

    public static byte[] SerializeIndex(IndexSchema index)
    {
        var ms = new MemoryStream();
        var nameBytes = Encoding.UTF8.GetBytes(index.IndexName);
        WriteVarint(ms, (ulong)nameBytes.Length);
        ms.Write(nameBytes);

        var tableNameBytes = Encoding.UTF8.GetBytes(index.TableName);
        WriteVarint(ms, (ulong)tableNameBytes.Length);
        ms.Write(tableNameBytes);

        WriteVarint(ms, (ulong)index.Columns.Count);
        foreach (var col in index.Columns)
        {
            var colBytes = Encoding.UTF8.GetBytes(col);
            WriteVarint(ms, (ulong)colBytes.Length);
            ms.Write(colBytes);
        }

        ms.WriteByte(index.IsUnique ? (byte)1 : (byte)0);
        return ms.ToArray();
    }

    public static IndexSchema DeserializeIndex(ReadOnlySpan<byte> data)
    {
        int pos = 0;
        int nameLen = (int)Varint.Read(data[pos..], out int nb);
        pos += nb;
        string indexName = Encoding.UTF8.GetString(data.Slice(pos, nameLen));
        pos += nameLen;

        int tableNameLen = (int)Varint.Read(data[pos..], out int tnb);
        pos += tnb;
        string tableName = Encoding.UTF8.GetString(data.Slice(pos, tableNameLen));
        pos += tableNameLen;

        int colCount = (int)Varint.Read(data[pos..], out int cb);
        pos += cb;

        var columns = new string[colCount];
        for (int i = 0; i < colCount; i++)
        {
            int colLen = (int)Varint.Read(data[pos..], out int clb);
            pos += clb;
            columns[i] = Encoding.UTF8.GetString(data.Slice(pos, colLen));
            pos += colLen;
        }

        bool isUnique = data[pos] != 0;

        return new IndexSchema
        {
            IndexName = indexName,
            TableName = tableName,
            Columns = columns,
            IsUnique = isUnique,
        };
    }

    /// <summary>
    /// Serialize a table name as a B+tree key (simple UTF-8 encoding mapped to a long hash for rowid-keyed tree).
    /// For the catalog, we use a simple sequential ID approach instead.
    /// </summary>
    public static long TableNameToKey(string tableName)
    {
        // Use a stable hash. For MVP, a simple deterministic hash.
        long hash = 0;
        foreach (char c in tableName.ToLowerInvariant())
            hash = hash * 31 + c;
        return hash & 0x7FFFFFFFFFFFFFFF; // ensure positive
    }

    /// <summary>
    /// Hash index name to a B+tree key. Uses a different multiplier than table names to reduce collisions.
    /// </summary>
    public static long IndexNameToKey(string indexName)
    {
        long hash = 0;
        foreach (char c in indexName.ToLowerInvariant())
            hash = hash * 37 + c;
        return hash & 0x7FFFFFFFFFFFFFFF;
    }

    /// <summary>
    /// Hash view name to a B+tree key. Uses a different multiplier to reduce collisions.
    /// </summary>
    public static long ViewNameToKey(string viewName)
    {
        long hash = 0;
        foreach (char c in viewName.ToLowerInvariant())
            hash = hash * 41 + c;
        return hash & 0x7FFFFFFFFFFFFFFF;
    }

    /// <summary>
    /// Hash trigger name to a B+tree key.
    /// </summary>
    public static long TriggerNameToKey(string triggerName)
    {
        long hash = 0;
        foreach (char c in triggerName.ToLowerInvariant())
            hash = hash * 43 + c;
        return hash & 0x7FFFFFFFFFFFFFFF;
    }

    /// <summary>
    /// Serialize a TriggerSchema.
    /// Format: [nameLen:4][name][tableLen:4][table][timing:1][event:1][bodyLen:4][body]
    /// </summary>
    public static byte[] SerializeTrigger(TriggerSchema trigger)
    {
        var nameBytes = Encoding.UTF8.GetBytes(trigger.TriggerName);
        var tableBytes = Encoding.UTF8.GetBytes(trigger.TableName);
        var bodyBytes = Encoding.UTF8.GetBytes(trigger.BodySql);

        var result = new byte[4 + nameBytes.Length + 4 + tableBytes.Length + 1 + 1 + 4 + bodyBytes.Length];
        int pos = 0;

        BitConverter.TryWriteBytes(result.AsSpan(pos), nameBytes.Length); pos += 4;
        nameBytes.CopyTo(result.AsSpan(pos)); pos += nameBytes.Length;

        BitConverter.TryWriteBytes(result.AsSpan(pos), tableBytes.Length); pos += 4;
        tableBytes.CopyTo(result.AsSpan(pos)); pos += tableBytes.Length;

        result[pos++] = (byte)trigger.Timing;
        result[pos++] = (byte)trigger.Event;

        BitConverter.TryWriteBytes(result.AsSpan(pos), bodyBytes.Length); pos += 4;
        bodyBytes.CopyTo(result.AsSpan(pos));

        return result;
    }

    public static TriggerSchema DeserializeTrigger(ReadOnlySpan<byte> data)
    {
        int pos = 0;

        int nameLen = BitConverter.ToInt32(data.Slice(pos, 4)); pos += 4;
        string triggerName = Encoding.UTF8.GetString(data.Slice(pos, nameLen)); pos += nameLen;

        int tableLen = BitConverter.ToInt32(data.Slice(pos, 4)); pos += 4;
        string tableName = Encoding.UTF8.GetString(data.Slice(pos, tableLen)); pos += tableLen;

        var timing = (TriggerTiming)data[pos++];
        var evt = (TriggerEvent)data[pos++];

        int bodyLen = BitConverter.ToInt32(data.Slice(pos, 4)); pos += 4;
        string bodySql = Encoding.UTF8.GetString(data.Slice(pos, bodyLen));

        return new TriggerSchema
        {
            TriggerName = triggerName,
            TableName = tableName,
            Timing = timing,
            Event = evt,
            BodySql = bodySql,
        };
    }

    private static void WriteVarint(MemoryStream ms, ulong value)
    {
        Span<byte> buf = stackalloc byte[10];
        int len = Varint.Write(buf, value);
        ms.Write(buf[..len]);
    }
}
