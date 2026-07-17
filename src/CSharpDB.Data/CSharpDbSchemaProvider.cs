using System.Data;
using System.Data.Common;
using CSharpDB.Primitives;
using CoreDbType = CSharpDB.Primitives.DbType;

namespace CSharpDB.Data;

internal static class CSharpDbSchemaProvider
{
    private const string TablesCollection = "Tables";
    private const string ColumnsCollection = "Columns";
    private const string IndexesCollection = "Indexes";
    private const string ForeignKeysCollection = "ForeignKeys";
    private const string ViewsCollection = "Views";
    private const string CheckConstraintsCollection = "CheckConstraints";
    private const string KeyConstraintsCollection = "KeyConstraints";
    private const string KeyColumnsCollection = "KeyColumns";

    public static DataTable GetSchema(
        CSharpDbConnection connection,
        string collectionName,
        string?[]? restrictionValues)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(collectionName);

        if (collectionName.Length == 0)
            throw new ArgumentException("Collection name cannot be empty.", nameof(collectionName));

        if (string.Equals(collectionName, DbMetaDataCollectionNames.MetaDataCollections, StringComparison.OrdinalIgnoreCase))
            return CreateMetaDataCollectionsTable();

        if (string.Equals(collectionName, DbMetaDataCollectionNames.Restrictions, StringComparison.OrdinalIgnoreCase))
            return CreateRestrictionsTable();

        _ = connection.GetSession();

        if (string.Equals(collectionName, TablesCollection, StringComparison.OrdinalIgnoreCase))
            return CreateTablesTable(connection, restrictionValues);

        if (string.Equals(collectionName, ColumnsCollection, StringComparison.OrdinalIgnoreCase))
            return CreateColumnsTable(connection, restrictionValues);

        if (string.Equals(collectionName, IndexesCollection, StringComparison.OrdinalIgnoreCase))
            return CreateIndexesTable(connection, restrictionValues);

        if (string.Equals(collectionName, ForeignKeysCollection, StringComparison.OrdinalIgnoreCase))
            return CreateForeignKeysTable(connection, restrictionValues);

        if (string.Equals(collectionName, ViewsCollection, StringComparison.OrdinalIgnoreCase))
            return CreateViewsTable(connection, restrictionValues);

        if (string.Equals(collectionName, CheckConstraintsCollection, StringComparison.OrdinalIgnoreCase))
            return CreateCheckConstraintsTable(connection, restrictionValues);

        if (string.Equals(collectionName, KeyConstraintsCollection, StringComparison.OrdinalIgnoreCase))
            return CreateKeyConstraintsTable(connection, restrictionValues);

        if (string.Equals(collectionName, KeyColumnsCollection, StringComparison.OrdinalIgnoreCase))
            return CreateKeyColumnsTable(connection, restrictionValues);

        throw new ArgumentException(
            $"Collection '{collectionName}' is not supported.",
            nameof(collectionName));
    }

    private static DataTable CreateMetaDataCollectionsTable()
    {
        var table = new DataTable(DbMetaDataCollectionNames.MetaDataCollections);
        table.Columns.Add(DbMetaDataColumnNames.CollectionName, typeof(string));
        table.Columns.Add(DbMetaDataColumnNames.NumberOfRestrictions, typeof(int));
        table.Columns.Add(DbMetaDataColumnNames.NumberOfIdentifierParts, typeof(int));

        table.Rows.Add(DbMetaDataCollectionNames.MetaDataCollections, 0, 0);
        table.Rows.Add(DbMetaDataCollectionNames.Restrictions, 0, 0);
        table.Rows.Add(TablesCollection, 4, 1);
        table.Rows.Add(ColumnsCollection, 4, 2);
        table.Rows.Add(IndexesCollection, 4, 1);
        table.Rows.Add(ForeignKeysCollection, 4, 1);
        table.Rows.Add(ViewsCollection, 3, 1);
        table.Rows.Add(CheckConstraintsCollection, 4, 1);
        table.Rows.Add(KeyConstraintsCollection, 4, 1);
        table.Rows.Add(KeyColumnsCollection, 4, 2);

        return table;
    }

    private static DataTable CreateRestrictionsTable()
    {
        var table = new DataTable(DbMetaDataCollectionNames.Restrictions);
        table.Columns.Add(DbMetaDataColumnNames.CollectionName, typeof(string));
        table.Columns.Add("RestrictionName", typeof(string));
        table.Columns.Add("RestrictionDefault", typeof(string));
        table.Columns.Add("RestrictionNumber", typeof(int));

        AddRestrictions(table, TablesCollection, "Catalog", "Schema", "Table", "TableType");
        AddRestrictions(table, ColumnsCollection, "Catalog", "Schema", "Table", "Column");
        AddRestrictions(table, IndexesCollection, "Catalog", "Schema", "Table", "Index");
        AddRestrictions(table, ForeignKeysCollection, "Catalog", "Schema", "Table", "Constraint");
        AddRestrictions(table, ViewsCollection, "Catalog", "Schema", "View");
        AddRestrictions(table, CheckConstraintsCollection, "Catalog", "Schema", "Table", "Constraint");
        AddRestrictions(table, KeyConstraintsCollection, "Catalog", "Schema", "Table", "Constraint");
        AddRestrictions(table, KeyColumnsCollection, "Catalog", "Schema", "Table", "Constraint");

        return table;
    }

    private static void AddRestrictions(DataTable table, string collectionName, params string[] restrictionNames)
    {
        for (int i = 0; i < restrictionNames.Length; i++)
            table.Rows.Add(collectionName, restrictionNames[i], DBNull.Value, i + 1);
    }

    private static DataTable CreateTablesTable(CSharpDbConnection connection, string?[]? restrictionValues)
    {
        var table = new DataTable(TablesCollection);
        table.Columns.Add("TABLE_CATALOG", typeof(string));
        table.Columns.Add("TABLE_SCHEMA", typeof(string));
        table.Columns.Add("TABLE_NAME", typeof(string));
        table.Columns.Add("TABLE_TYPE", typeof(string));

        string catalog = connection.DataSource;
        string? catalogRestriction = GetRestrictionValue(restrictionValues, 0);
        string? schemaRestriction = GetRestrictionValue(restrictionValues, 1);
        string? nameRestriction = GetRestrictionValue(restrictionValues, 2);
        string? typeRestriction = GetRestrictionValue(restrictionValues, 3);

        foreach (string tableName in connection.GetTableNames()
            .Where(IsUserVisibleTableName)
            .OrderBy(static name => name, StringComparer.OrdinalIgnoreCase))
        {
            if (!MatchesRestriction(catalog, catalogRestriction) ||
                !MatchesRestriction(actualValue: null, schemaRestriction) ||
                !MatchesRestriction(tableName, nameRestriction) ||
                !MatchesRestriction("BASE TABLE", typeRestriction))
            {
                continue;
            }

            table.Rows.Add(catalog, DBNull.Value, tableName, "BASE TABLE");
        }

        foreach (string viewName in connection.GetViewNames()
            .Where(IsUserVisibleTableName)
            .OrderBy(static name => name, StringComparer.OrdinalIgnoreCase))
        {
            if (!MatchesRestriction(catalog, catalogRestriction) ||
                !MatchesRestriction(actualValue: null, schemaRestriction) ||
                !MatchesRestriction(viewName, nameRestriction) ||
                !MatchesRestriction("VIEW", typeRestriction))
            {
                continue;
            }

            table.Rows.Add(catalog, DBNull.Value, viewName, "VIEW");
        }

        return table;
    }

    private static DataTable CreateColumnsTable(CSharpDbConnection connection, string?[]? restrictionValues)
    {
        var table = new DataTable(ColumnsCollection);
        table.Columns.Add("TABLE_CATALOG", typeof(string));
        table.Columns.Add("TABLE_SCHEMA", typeof(string));
        table.Columns.Add("TABLE_NAME", typeof(string));
        table.Columns.Add("COLUMN_NAME", typeof(string));
        table.Columns.Add("ORDINAL_POSITION", typeof(int));
        table.Columns.Add("COLUMN_DEFAULT", typeof(string));
        table.Columns.Add("IS_NULLABLE", typeof(string));
        table.Columns.Add("DATA_TYPE", typeof(string));
        table.Columns.Add("CHARACTER_MAXIMUM_LENGTH", typeof(int));
        table.Columns.Add("CHARACTER_OCTET_LENGTH", typeof(int));
        table.Columns.Add("NUMERIC_PRECISION", typeof(byte));
        table.Columns.Add("NUMERIC_PRECISION_RADIX", typeof(short));
        table.Columns.Add("NUMERIC_SCALE", typeof(int));
        table.Columns.Add("DATETIME_PRECISION", typeof(short));
        table.Columns.Add("IS_PRIMARY_KEY", typeof(bool));
        table.Columns.Add("IS_IDENTITY", typeof(bool));
        table.Columns.Add("COLLATION_NAME", typeof(string));

        string catalog = connection.DataSource;
        string? catalogRestriction = GetRestrictionValue(restrictionValues, 0);
        string? schemaRestriction = GetRestrictionValue(restrictionValues, 1);
        string? tableRestriction = GetRestrictionValue(restrictionValues, 2);
        string? columnRestriction = GetRestrictionValue(restrictionValues, 3);

        foreach (string tableName in connection.GetTableNames()
            .Where(IsUserVisibleTableName)
            .OrderBy(static name => name, StringComparer.OrdinalIgnoreCase))
        {
            if (!MatchesRestriction(catalog, catalogRestriction) ||
                !MatchesRestriction(actualValue: null, schemaRestriction) ||
                !MatchesRestriction(tableName, tableRestriction))
            {
                continue;
            }

            TableSchema? schema = connection.GetTableSchema(tableName);
            if (schema is null)
                continue;

            for (int i = 0; i < schema.Columns.Count; i++)
            {
                ColumnDefinition column = schema.Columns[i];
                if (!MatchesRestriction(column.Name, columnRestriction))
                    continue;

                table.Rows.Add(
                    catalog,
                    DBNull.Value,
                    tableName,
                    column.Name,
                    i + 1,
                    column.DefaultSql is null ? DBNull.Value : column.DefaultSql,
                    column.Nullable ? "YES" : "NO",
                    TypeMapper.ToDataTypeName(column.Type),
                    DBNull.Value,
                    DBNull.Value,
                    GetNumericPrecision(column.Type),
                    GetNumericPrecisionRadix(column.Type),
                    GetNumericScale(column.Type),
                    DBNull.Value,
                    column.IsPrimaryKey,
                    column.IsIdentity,
                    column.Collation is null ? DBNull.Value : column.Collation);
            }
        }

        return table;
    }

    private static DataTable CreateCheckConstraintsTable(
        CSharpDbConnection connection,
        string?[]? restrictionValues)
    {
        var table = new DataTable(CheckConstraintsCollection);
        table.Columns.Add("CONSTRAINT_CATALOG", typeof(string));
        table.Columns.Add("CONSTRAINT_SCHEMA", typeof(string));
        table.Columns.Add("CONSTRAINT_NAME", typeof(string));
        table.Columns.Add("TABLE_CATALOG", typeof(string));
        table.Columns.Add("TABLE_SCHEMA", typeof(string));
        table.Columns.Add("TABLE_NAME", typeof(string));
        table.Columns.Add("CHECK_CLAUSE", typeof(string));
        table.Columns.Add("COLUMN_NAME", typeof(string));

        string catalog = connection.DataSource;
        string? catalogRestriction = GetRestrictionValue(restrictionValues, 0);
        string? schemaRestriction = GetRestrictionValue(restrictionValues, 1);
        string? tableRestriction = GetRestrictionValue(restrictionValues, 2);
        string? constraintRestriction = GetRestrictionValue(restrictionValues, 3);

        foreach (string tableName in connection.GetTableNames()
            .Where(IsUserVisibleTableName)
            .OrderBy(static name => name, StringComparer.OrdinalIgnoreCase))
        {
            if (!MatchesRestriction(catalog, catalogRestriction) ||
                !MatchesRestriction(actualValue: null, schemaRestriction) ||
                !MatchesRestriction(tableName, tableRestriction))
            {
                continue;
            }

            TableSchema? schema = connection.GetTableSchema(tableName);
            if (schema is null)
                continue;

            foreach (CheckConstraintDefinition check in schema.CheckConstraints)
            {
                if (!MatchesRestriction(check.ConstraintName, constraintRestriction))
                    continue;

                table.Rows.Add(
                    catalog,
                    DBNull.Value,
                    check.ConstraintName is null ? DBNull.Value : check.ConstraintName,
                    catalog,
                    DBNull.Value,
                    tableName,
                    check.ExpressionSql,
                    check.ColumnName is null ? DBNull.Value : check.ColumnName);
            }
        }

        return table;
    }

    private static DataTable CreateKeyConstraintsTable(
        CSharpDbConnection connection,
        string?[]? restrictionValues)
    {
        var table = new DataTable(KeyConstraintsCollection);
        table.Columns.Add("CONSTRAINT_CATALOG", typeof(string));
        table.Columns.Add("CONSTRAINT_SCHEMA", typeof(string));
        table.Columns.Add("CONSTRAINT_NAME", typeof(string));
        table.Columns.Add("TABLE_CATALOG", typeof(string));
        table.Columns.Add("TABLE_SCHEMA", typeof(string));
        table.Columns.Add("TABLE_NAME", typeof(string));
        table.Columns.Add("CONSTRAINT_TYPE", typeof(string));
        table.Columns.Add("BACKING_INDEX_NAME", typeof(string));
        table.Columns.Add("COLUMN_COUNT", typeof(int));

        string catalog = connection.DataSource;
        string? catalogRestriction = GetRestrictionValue(restrictionValues, 0);
        string? schemaRestriction = GetRestrictionValue(restrictionValues, 1);
        string? tableRestriction = GetRestrictionValue(restrictionValues, 2);
        string? constraintRestriction = GetRestrictionValue(restrictionValues, 3);

        foreach ((string tableName, KeyConstraintDefinition key) in EnumerateKeyConstraints(
            connection,
            catalog,
            catalogRestriction,
            schemaRestriction,
            tableRestriction,
            constraintRestriction))
        {
            table.Rows.Add(
                catalog,
                DBNull.Value,
                key.ConstraintName is null ? DBNull.Value : key.ConstraintName,
                catalog,
                DBNull.Value,
                tableName,
                GetKeyConstraintType(key.Kind),
                key.BackingIndexName is null ? DBNull.Value : key.BackingIndexName,
                key.Columns.Count);
        }

        return table;
    }

    private static DataTable CreateKeyColumnsTable(
        CSharpDbConnection connection,
        string?[]? restrictionValues)
    {
        var table = new DataTable(KeyColumnsCollection);
        table.Columns.Add("CONSTRAINT_CATALOG", typeof(string));
        table.Columns.Add("CONSTRAINT_SCHEMA", typeof(string));
        table.Columns.Add("CONSTRAINT_NAME", typeof(string));
        table.Columns.Add("TABLE_CATALOG", typeof(string));
        table.Columns.Add("TABLE_SCHEMA", typeof(string));
        table.Columns.Add("TABLE_NAME", typeof(string));
        table.Columns.Add("COLUMN_NAME", typeof(string));
        table.Columns.Add("ORDINAL_POSITION", typeof(int));

        string catalog = connection.DataSource;
        string? catalogRestriction = GetRestrictionValue(restrictionValues, 0);
        string? schemaRestriction = GetRestrictionValue(restrictionValues, 1);
        string? tableRestriction = GetRestrictionValue(restrictionValues, 2);
        string? constraintRestriction = GetRestrictionValue(restrictionValues, 3);

        foreach ((string tableName, KeyConstraintDefinition key) in EnumerateKeyConstraints(
            connection,
            catalog,
            catalogRestriction,
            schemaRestriction,
            tableRestriction,
            constraintRestriction))
        {
            for (int i = 0; i < key.Columns.Count; i++)
            {
                table.Rows.Add(
                    catalog,
                    DBNull.Value,
                    key.ConstraintName is null ? DBNull.Value : key.ConstraintName,
                    catalog,
                    DBNull.Value,
                    tableName,
                    key.Columns[i],
                    i + 1);
            }
        }

        return table;
    }

    private static IEnumerable<(string TableName, KeyConstraintDefinition Constraint)> EnumerateKeyConstraints(
        CSharpDbConnection connection,
        string catalog,
        string? catalogRestriction,
        string? schemaRestriction,
        string? tableRestriction,
        string? constraintRestriction)
    {
        foreach (string tableName in connection.GetTableNames()
            .Where(IsUserVisibleTableName)
            .OrderBy(static name => name, StringComparer.OrdinalIgnoreCase))
        {
            if (!MatchesRestriction(catalog, catalogRestriction) ||
                !MatchesRestriction(actualValue: null, schemaRestriction) ||
                !MatchesRestriction(tableName, tableRestriction))
            {
                continue;
            }

            TableSchema? schema = connection.GetTableSchema(tableName);
            if (schema is null)
                continue;

            foreach (KeyConstraintDefinition key in GetEffectiveKeyConstraints(schema))
            {
                if (MatchesRestriction(key.ConstraintName, constraintRestriction))
                    yield return (tableName, key);
            }
        }
    }

    private static IReadOnlyList<KeyConstraintDefinition> GetEffectiveKeyConstraints(TableSchema schema)
    {
        if (schema.KeyConstraints.Count > 0)
            return schema.KeyConstraints;

        ColumnDefinition[] legacyPrimaryKeyColumns = schema.Columns
            .Where(static column => column.IsPrimaryKey)
            .ToArray();
        if (legacyPrimaryKeyColumns.Length != 1)
            return Array.Empty<KeyConstraintDefinition>();

        return
        [
            new KeyConstraintDefinition
            {
                ConstraintName = null,
                Kind = KeyConstraintKind.PrimaryKey,
                Columns = [legacyPrimaryKeyColumns[0].Name],
                BackingIndexName = null,
            },
        ];
    }

    private static string GetKeyConstraintType(KeyConstraintKind kind)
        => kind switch
        {
            KeyConstraintKind.PrimaryKey => "PRIMARY KEY",
            KeyConstraintKind.Unique => "UNIQUE",
            _ => throw new InvalidOperationException($"Unsupported key constraint kind '{kind}'."),
        };

    private static DataTable CreateIndexesTable(CSharpDbConnection connection, string?[]? restrictionValues)
    {
        var table = new DataTable(IndexesCollection);
        table.Columns.Add("CONSTRAINT_CATALOG", typeof(string));
        table.Columns.Add("CONSTRAINT_SCHEMA", typeof(string));
        table.Columns.Add("CONSTRAINT_NAME", typeof(string));
        table.Columns.Add("TABLE_CATALOG", typeof(string));
        table.Columns.Add("TABLE_SCHEMA", typeof(string));
        table.Columns.Add("TABLE_NAME", typeof(string));
        table.Columns.Add("INDEX_NAME", typeof(string));
        table.Columns.Add("IS_UNIQUE", typeof(bool));
        table.Columns.Add("INDEX_TYPE", typeof(string));
        table.Columns.Add("INDEX_STATE", typeof(string));
        table.Columns.Add("COLUMN_LIST", typeof(string));
        table.Columns.Add("COLLATION_LIST", typeof(string));

        string catalog = connection.DataSource;
        string? catalogRestriction = GetRestrictionValue(restrictionValues, 0);
        string? schemaRestriction = GetRestrictionValue(restrictionValues, 1);
        string? tableRestriction = GetRestrictionValue(restrictionValues, 2);
        string? indexRestriction = GetRestrictionValue(restrictionValues, 3);

        foreach (IndexSchema index in connection.GetIndexes()
            .Where(IsUserVisibleIndex)
            .OrderBy(static item => item.TableName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static item => item.IndexName, StringComparer.OrdinalIgnoreCase))
        {
            if (!MatchesRestriction(catalog, catalogRestriction) ||
                !MatchesRestriction(actualValue: null, schemaRestriction) ||
                !MatchesRestriction(index.TableName, tableRestriction) ||
                !MatchesRestriction(index.IndexName, indexRestriction))
            {
                continue;
            }

            TableSchema? tableSchema = connection.GetTableSchema(index.TableName);
            string collationList = string.Join(
                ", ",
                index.Columns.Select((columnName, position) => ResolveEffectiveIndexColumnCollation(index, tableSchema, position, columnName) ?? string.Empty));

            table.Rows.Add(
                catalog,
                DBNull.Value,
                index.IndexName,
                catalog,
                DBNull.Value,
                index.TableName,
                index.IndexName,
                index.IsUnique,
                index.Kind.ToString(),
                index.State.ToString(),
                string.Join(", ", index.Columns),
                collationList);
        }

        return table;
    }

    private static DataTable CreateForeignKeysTable(CSharpDbConnection connection, string?[]? restrictionValues)
    {
        var table = new DataTable(ForeignKeysCollection);
        table.Columns.Add("CONSTRAINT_CATALOG", typeof(string));
        table.Columns.Add("CONSTRAINT_SCHEMA", typeof(string));
        table.Columns.Add("CONSTRAINT_NAME", typeof(string));
        table.Columns.Add("TABLE_CATALOG", typeof(string));
        table.Columns.Add("TABLE_SCHEMA", typeof(string));
        table.Columns.Add("TABLE_NAME", typeof(string));
        table.Columns.Add("COLUMN_NAME", typeof(string));
        table.Columns.Add("REFERENCED_TABLE_NAME", typeof(string));
        table.Columns.Add("REFERENCED_COLUMN_NAME", typeof(string));
        table.Columns.Add("DELETE_RULE", typeof(string));
        table.Columns.Add("SUPPORTING_INDEX_NAME", typeof(string));
        table.Columns.Add("ORDINAL_POSITION", typeof(int));

        string catalog = connection.DataSource;
        string? catalogRestriction = GetRestrictionValue(restrictionValues, 0);
        string? schemaRestriction = GetRestrictionValue(restrictionValues, 1);
        string? tableRestriction = GetRestrictionValue(restrictionValues, 2);
        string? constraintRestriction = GetRestrictionValue(restrictionValues, 3);

        foreach (string tableName in connection.GetTableNames()
            .Where(IsUserVisibleTableName)
            .OrderBy(static name => name, StringComparer.OrdinalIgnoreCase))
        {
            if (!MatchesRestriction(catalog, catalogRestriction) ||
                !MatchesRestriction(actualValue: null, schemaRestriction) ||
                !MatchesRestriction(tableName, tableRestriction))
            {
                continue;
            }

            TableSchema? schema = connection.GetTableSchema(tableName);
            if (schema is null || schema.ForeignKeys.Count == 0)
                continue;

            foreach (ForeignKeyDefinition foreignKey in schema.ForeignKeys.OrderBy(fk => fk.ConstraintName, StringComparer.OrdinalIgnoreCase))
            {
                if (!MatchesRestriction(foreignKey.ConstraintName, constraintRestriction))
                    continue;

                IReadOnlyList<string> childColumns = foreignKey.ColumnNames.Count > 0
                    ? foreignKey.ColumnNames
                    : [foreignKey.ColumnName];
                IReadOnlyList<string> referencedColumns = foreignKey.ReferencedColumnNames.Count > 0
                    ? foreignKey.ReferencedColumnNames
                    : [foreignKey.ReferencedColumnName];
                int columnCount = Math.Min(childColumns.Count, referencedColumns.Count);
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
                {
                    table.Rows.Add(
                        catalog,
                        DBNull.Value,
                        foreignKey.ConstraintName,
                        catalog,
                        DBNull.Value,
                        tableName,
                        childColumns[columnIndex],
                        foreignKey.ReferencedTableName,
                        referencedColumns[columnIndex],
                        foreignKey.OnDelete.ToString().ToUpperInvariant(),
                        foreignKey.SupportingIndexName,
                        columnIndex + 1);
                }
            }
        }

        return table;
    }

    private static DataTable CreateViewsTable(CSharpDbConnection connection, string?[]? restrictionValues)
    {
        var table = new DataTable(ViewsCollection);
        table.Columns.Add("TABLE_CATALOG", typeof(string));
        table.Columns.Add("TABLE_SCHEMA", typeof(string));
        table.Columns.Add("TABLE_NAME", typeof(string));
        table.Columns.Add("CHECK_OPTION", typeof(string));
        table.Columns.Add("IS_UPDATABLE", typeof(string));
        table.Columns.Add("VIEW_DEFINITION", typeof(string));

        string catalog = connection.DataSource;
        string? catalogRestriction = GetRestrictionValue(restrictionValues, 0);
        string? schemaRestriction = GetRestrictionValue(restrictionValues, 1);
        string? nameRestriction = GetRestrictionValue(restrictionValues, 2);

        foreach (string viewName in connection.GetViewNames()
            .Where(IsUserVisibleTableName)
            .OrderBy(static name => name, StringComparer.OrdinalIgnoreCase))
        {
            if (!MatchesRestriction(catalog, catalogRestriction) ||
                !MatchesRestriction(actualValue: null, schemaRestriction) ||
                !MatchesRestriction(viewName, nameRestriction))
            {
                continue;
            }

            table.Rows.Add(
                catalog,
                DBNull.Value,
                viewName,
                "NONE",
                "NO",
                connection.GetViewSql(viewName) ?? string.Empty);
        }

        return table;
    }

    private static string? GetRestrictionValue(string?[]? restrictionValues, int index)
        => restrictionValues is not null && index < restrictionValues.Length
            ? restrictionValues[index]
            : null;

    private static string? ResolveEffectiveIndexColumnCollation(
        IndexSchema index,
        TableSchema? tableSchema,
        int columnPosition,
        string columnName)
    {
        if (columnPosition < index.ColumnCollations.Count && !string.IsNullOrWhiteSpace(index.ColumnCollations[columnPosition]))
            return index.ColumnCollations[columnPosition];

        if (tableSchema == null)
            return null;

        int tableColumnIndex = tableSchema.GetColumnIndex(columnName);
        return tableColumnIndex >= 0 ? tableSchema.Columns[tableColumnIndex].Collation : null;
    }

    private static bool MatchesRestriction(string? actualValue, string? restrictionValue)
    {
        if (restrictionValue is null)
            return true;

        return actualValue is not null &&
               string.Equals(actualValue, restrictionValue, StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsUserVisibleTableName(string tableName)
        => !tableName.StartsWith("_", StringComparison.Ordinal);

    private static bool IsUserVisibleIndex(IndexSchema index)
        => IsUserVisibleTableName(index.TableName) &&
           index.Kind is IndexKind.Sql or IndexKind.FullText;

    private static object GetNumericPrecision(CoreDbType type)
    {
        return type switch
        {
            CoreDbType.Integer => (byte)19,
            _ => DBNull.Value,
        };
    }

    private static object GetNumericPrecisionRadix(CoreDbType type)
    {
        return type switch
        {
            CoreDbType.Integer => (short)10,
            _ => DBNull.Value,
        };
    }

    private static object GetNumericScale(CoreDbType type)
    {
        return type switch
        {
            CoreDbType.Integer => 0,
            _ => DBNull.Value,
        };
    }
}
