namespace CSharpDB.Core;

public sealed class ColumnDefinition
{
    public required string Name { get; init; }
    public required DbType Type { get; init; }
    public bool Nullable { get; init; } = true;
    public bool IsPrimaryKey { get; init; }
    public bool IsIdentity { get; init; }
}

public sealed class TableSchema
{
    public required string TableName { get; init; }
    public required IReadOnlyList<ColumnDefinition> Columns { get; init; }
    
    /// <summary>
    /// Persisted next auto rowid high-water mark for INSERT allocation.
    /// 0 means unknown/uninitialized (legacy metadata), so allocator will compute once.
    /// </summary>
    public long NextRowId { get; set; }

    /// <summary>
    /// Optional mapping of "tablealias.columnname" → column index for JOIN queries.
    /// </summary>
    public Dictionary<string, int>? QualifiedMappings { get; init; }

    /// <summary>
    /// Index of the primary key column, or -1 if using implicit rowid.
    /// </summary>
    public int PrimaryKeyColumnIndex
    {
        get
        {
            for (int i = 0; i < Columns.Count; i++)
                if (Columns[i].IsPrimaryKey) return i;
            return -1;
        }
    }

    public int GetColumnIndex(string name)
    {
        for (int i = 0; i < Columns.Count; i++)
            if (string.Equals(Columns[i].Name, name, StringComparison.OrdinalIgnoreCase))
                return i;
        return -1;
    }

    /// <summary>
    /// Looks up a column by qualified name (tablealias.columnname).
    /// </summary>
    public int GetQualifiedColumnIndex(string tableAlias, string columnName)
    {
        if (QualifiedMappings != null)
        {
            string key = $"{tableAlias}.{columnName}";
            if (QualifiedMappings.TryGetValue(key, out int idx))
                return idx;
        }
        return -1;
    }

    /// <summary>
    /// Creates a composite schema by merging left and right schemas for a JOIN.
    /// Inherits all qualified mappings from sub-schemas with appropriate index offsets.
    /// </summary>
    public static TableSchema CreateJoinSchema(TableSchema leftSchema, TableSchema rightSchema)
    {
        var columns = new List<ColumnDefinition>();
        var qualified = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        // Add left columns
        for (int i = 0; i < leftSchema.Columns.Count; i++)
            columns.Add(leftSchema.Columns[i]);

        // Inherit left qualified mappings (no offset needed — left is at index 0)
        if (leftSchema.QualifiedMappings != null)
        {
            foreach (var (key, idx) in leftSchema.QualifiedMappings)
                qualified[key] = idx;
        }

        // Add right columns
        int rightOffset = leftSchema.Columns.Count;
        for (int i = 0; i < rightSchema.Columns.Count; i++)
            columns.Add(rightSchema.Columns[i]);

        // Inherit right qualified mappings with offset
        if (rightSchema.QualifiedMappings != null)
        {
            foreach (var (key, idx) in rightSchema.QualifiedMappings)
                qualified[key] = rightOffset + idx;
        }

        return new TableSchema
        {
            TableName = "joined",
            Columns = columns.ToArray(),
            QualifiedMappings = qualified,
        };
    }
}

public sealed class IndexSchema
{
    public required string IndexName { get; init; }
    public required string TableName { get; init; }
    public required IReadOnlyList<string> Columns { get; init; }
    public bool IsUnique { get; init; }
}

public sealed class TableStatistics
{
    public required string TableName { get; init; }
    public long RowCount { get; init; }
    public bool HasStaleColumns { get; init; }
}

public sealed class ColumnStatistics
{
    public required string TableName { get; init; }
    public required string ColumnName { get; init; }
    public long DistinctCount { get; init; }
    public long NonNullCount { get; init; }
    public DbValue MinValue { get; init; } = DbValue.Null;
    public DbValue MaxValue { get; init; } = DbValue.Null;
    public bool IsStale { get; init; }
}

public enum TriggerTiming { Before, After }
public enum TriggerEvent { Insert, Update, Delete }

public sealed class TriggerSchema
{
    public required string TriggerName { get; init; }
    public required string TableName { get; init; }
    public required TriggerTiming Timing { get; init; }
    public required TriggerEvent Event { get; init; }
    public required string BodySql { get; init; }
}
