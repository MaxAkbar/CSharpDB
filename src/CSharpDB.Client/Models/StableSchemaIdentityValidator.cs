namespace CSharpDB.Client.Models;

internal static class StableSchemaIdentityValidator
{
    public static void Validate(TableSchema schema)
    {
        var columnsByName =
            new Dictionary<string, ColumnDefinition>(
                StringComparer.OrdinalIgnoreCase);
        foreach (ColumnDefinition column in schema.Columns)
        {
            if (!columnsByName.TryAdd(column.Name, column))
            {
                throw new CSharpDbClientException(
                    $"The server returned duplicate column '{schema.TableName}.{column.Name}'.");
            }
        }

        foreach (ForeignKeyDefinition foreignKey in schema.ForeignKeys)
            ValidateForeignKeyStructure(schema, columnsByName, foreignKey);

        bool hasAnyIdentity =
            schema.SchemaId != Guid.Empty ||
            schema.Columns.Any(static column =>
                column.SchemaId != Guid.Empty) ||
            schema.ForeignKeys.Any(static foreignKey =>
                foreignKey.SchemaId != Guid.Empty ||
                foreignKey.ReferencedTableSchemaId != Guid.Empty ||
                foreignKey.ReferencedKeySchemaId != Guid.Empty ||
                foreignKey.ColumnSchemaIds.Count > 0 ||
                foreignKey.ReferencedColumnSchemaIds.Count > 0) ||
            schema.CheckConstraints.Any(static check =>
                check.SchemaId != Guid.Empty) ||
            schema.KeyConstraints.Any(static key =>
                key.SchemaId != Guid.Empty);
        if (!hasAnyIdentity)
            return;

        var ownedIdentities = new HashSet<Guid>();
        AddOwnedIdentity(
            ownedIdentities,
            schema.SchemaId,
            $"table '{schema.TableName}'");
        foreach (ColumnDefinition column in schema.Columns)
        {
            AddOwnedIdentity(
                ownedIdentities,
                column.SchemaId,
                $"column '{schema.TableName}.{column.Name}'");
        }
        foreach (ForeignKeyDefinition foreignKey in schema.ForeignKeys)
        {
            AddOwnedIdentity(
                ownedIdentities,
                foreignKey.SchemaId,
                $"foreign key '{schema.TableName}.{foreignKey.ConstraintName}'");
        }
        foreach (CheckConstraintDefinition check in schema.CheckConstraints)
        {
            AddOwnedIdentity(
                ownedIdentities,
                check.SchemaId,
                $"check constraint '{schema.TableName}.{check.ConstraintName ?? "<unnamed>"}'");
        }
        foreach (KeyConstraintDefinition key in schema.KeyConstraints)
        {
            AddOwnedIdentity(
                ownedIdentities,
                key.SchemaId,
                $"key constraint '{schema.TableName}.{key.ConstraintName ?? "<unnamed>"}'");
        }

        var externalIdentityRoles = new Dictionary<Guid, string>();
        foreach (ForeignKeyDefinition foreignKey in schema.ForeignKeys)
        {
            IReadOnlyList<string> childColumns =
                foreignKey.ColumnNames.Count > 0
                    ? foreignKey.ColumnNames
                    : [foreignKey.ColumnName];
            IReadOnlyList<string> referencedColumns =
                foreignKey.ReferencedColumnNames.Count > 0
                    ? foreignKey.ReferencedColumnNames
                    : [foreignKey.ReferencedColumnName];
            if (childColumns.Count != referencedColumns.Count ||
                foreignKey.ReferencedTableSchemaId == Guid.Empty ||
                foreignKey.ColumnSchemaIds.Count != childColumns.Count ||
                foreignKey.ReferencedColumnSchemaIds.Count !=
                referencedColumns.Count)
            {
                throw new CSharpDbClientException(
                    $"The server returned partial stable bindings for foreign key '{foreignKey.ConstraintName}'.");
            }

            var referencedColumnIdentities = new HashSet<Guid>();
            for (int i = 0; i < childColumns.Count; i++)
            {
                if (!columnsByName.TryGetValue(
                        childColumns[i],
                        out ColumnDefinition? childColumn) ||
                    foreignKey.ColumnSchemaIds[i] == Guid.Empty ||
                    foreignKey.ColumnSchemaIds[i] != childColumn.SchemaId)
                {
                    throw new CSharpDbClientException(
                        $"The server returned an invalid child-column identity binding for foreign key '{foreignKey.ConstraintName}'.");
                }

                Guid referencedColumnId =
                    foreignKey.ReferencedColumnSchemaIds[i];
                if (referencedColumnId == Guid.Empty ||
                    !referencedColumnIdentities.Add(referencedColumnId))
                {
                    throw new CSharpDbClientException(
                        $"The server returned an invalid referenced-column identity binding for foreign key '{foreignKey.ConstraintName}'.");
                }
            }

            if (string.Equals(
                    foreignKey.ReferencedTableName,
                    schema.TableName,
                    StringComparison.OrdinalIgnoreCase))
            {
                ValidateSelfReference(
                    schema,
                    columnsByName,
                    foreignKey,
                    referencedColumns);
            }
            else
            {
                AddExternalIdentityRole(
                    externalIdentityRoles,
                    foreignKey.ReferencedTableSchemaId,
                    "table",
                    foreignKey.ConstraintName);
                foreach (Guid referencedColumnId in
                         foreignKey.ReferencedColumnSchemaIds)
                {
                    AddExternalIdentityRole(
                        externalIdentityRoles,
                        referencedColumnId,
                        "column",
                        foreignKey.ConstraintName);
                }
                if (foreignKey.ReferencedKeySchemaId != Guid.Empty)
                {
                    AddExternalIdentityRole(
                        externalIdentityRoles,
                        foreignKey.ReferencedKeySchemaId,
                        "key",
                        foreignKey.ConstraintName);
                }

                if (ownedIdentities.Contains(
                        foreignKey.ReferencedTableSchemaId) ||
                    foreignKey.ReferencedColumnSchemaIds.Any(
                        ownedIdentities.Contains) ||
                    foreignKey.ReferencedKeySchemaId != Guid.Empty &&
                    ownedIdentities.Contains(
                        foreignKey.ReferencedKeySchemaId))
                {
                    throw new CSharpDbClientException(
                        $"The server returned an external foreign key '{foreignKey.ConstraintName}' that reuses an identity owned by its child table.");
                }
            }
        }
    }

    private static void ValidateForeignKeyStructure(
        TableSchema schema,
        IReadOnlyDictionary<string, ColumnDefinition> columnsByName,
        ForeignKeyDefinition foreignKey)
    {
        IReadOnlyList<string> childColumns =
            foreignKey.ColumnNames.Count > 0
                ? foreignKey.ColumnNames
                : [foreignKey.ColumnName];
        IReadOnlyList<string> referencedColumns =
            foreignKey.ReferencedColumnNames.Count > 0
                ? foreignKey.ReferencedColumnNames
                : [foreignKey.ReferencedColumnName];
        if (childColumns.Count == 0 ||
            childColumns.Count != referencedColumns.Count)
        {
            throw new CSharpDbClientException(
                $"The server returned inconsistent ordered columns for foreign key '{foreignKey.ConstraintName}'.");
        }
        if (!string.Equals(
                foreignKey.ColumnName,
                childColumns[0],
                StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(
                foreignKey.ReferencedColumnName,
                referencedColumns[0],
                StringComparison.OrdinalIgnoreCase))
        {
            throw new CSharpDbClientException(
                $"The server returned scalar and ordered columns that disagree for foreign key '{foreignKey.ConstraintName}'.");
        }

        var childNames = new HashSet<string>(
            StringComparer.OrdinalIgnoreCase);
        foreach (string childColumn in childColumns)
        {
            if (!childNames.Add(childColumn) ||
                !columnsByName.ContainsKey(childColumn))
            {
                throw new CSharpDbClientException(
                    $"The server returned an invalid child column '{childColumn}' for foreign key '{foreignKey.ConstraintName}'.");
            }
        }

        var referencedNames = new HashSet<string>(
            StringComparer.OrdinalIgnoreCase);
        bool selfReference = string.Equals(
            foreignKey.ReferencedTableName,
            schema.TableName,
            StringComparison.OrdinalIgnoreCase);
        foreach (string referencedColumn in referencedColumns)
        {
            if (!referencedNames.Add(referencedColumn) ||
                selfReference &&
                !columnsByName.ContainsKey(referencedColumn))
            {
                throw new CSharpDbClientException(
                    $"The server returned an invalid referenced column '{referencedColumn}' for foreign key '{foreignKey.ConstraintName}'.");
            }
        }
    }

    private static void AddExternalIdentityRole(
        IDictionary<Guid, string> identityRoles,
        Guid identity,
        string role,
        string constraintName)
    {
        if (identityRoles.TryGetValue(
                identity,
                out string? existingRole))
        {
            if (!string.Equals(
                    existingRole,
                    role,
                    StringComparison.Ordinal))
            {
                throw new CSharpDbClientException(
                    $"The server returned an external foreign key '{constraintName}' that reuses a stable identity across referenced object roles.");
            }

            return;
        }

        identityRoles.Add(identity, role);
    }

    private static void ValidateSelfReference(
        TableSchema schema,
        IReadOnlyDictionary<string, ColumnDefinition> columnsByName,
        ForeignKeyDefinition foreignKey,
        IReadOnlyList<string> referencedColumns)
    {
        if (foreignKey.ReferencedTableSchemaId != schema.SchemaId)
        {
            throw new CSharpDbClientException(
                $"The server returned an invalid referenced-table identity for self-referencing foreign key '{foreignKey.ConstraintName}'.");
        }

        for (int i = 0; i < referencedColumns.Count; i++)
        {
            if (!columnsByName.TryGetValue(
                    referencedColumns[i],
                    out ColumnDefinition? referencedColumn) ||
                foreignKey.ReferencedColumnSchemaIds[i] !=
                referencedColumn.SchemaId)
            {
                throw new CSharpDbClientException(
                    $"The server returned an invalid referenced-column identity binding for self-referencing foreign key '{foreignKey.ConstraintName}'.");
            }
        }

        KeyConstraintDefinition[] matchingKeys = schema.KeyConstraints
            .Where(key => key.Columns.SequenceEqual(
                referencedColumns,
                StringComparer.OrdinalIgnoreCase))
            .ToArray();
        if (matchingKeys.Length > 0)
        {
            if (!matchingKeys.Any(key =>
                    key.SchemaId == foreignKey.ReferencedKeySchemaId))
            {
                throw new CSharpDbClientException(
                    $"The server returned an invalid referenced-key identity binding for self-referencing foreign key '{foreignKey.ConstraintName}'.");
            }
        }
        else if (foreignKey.ReferencedKeySchemaId != Guid.Empty)
        {
            throw new CSharpDbClientException(
                $"The server returned a referenced-key identity without a matching logical key for self-referencing foreign key '{foreignKey.ConstraintName}'.");
        }
    }

    private static void AddOwnedIdentity(
        ISet<Guid> identities,
        Guid identity,
        string description)
    {
        if (identity == Guid.Empty)
        {
            throw new CSharpDbClientException(
                $"The server returned no stable identity for {description}.");
        }
        if (!identities.Add(identity))
        {
            throw new CSharpDbClientException(
                $"The server returned duplicate stable identity '{identity}' for {description}.");
        }
    }
}
