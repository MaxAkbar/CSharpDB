using CSharpDB.Migration;

namespace CSharpDB.Migration.MySql;

internal static partial class MySqlCatalogBuilder
{
    private static int RelationalObjectCapacity(MySqlCatalogSnapshot snapshot) =>
        checked(
            snapshot.Keys.Count +
            snapshot.ForeignKeys.Count +
            snapshot.Checks.Count +
            snapshot.Indexes.Count);

    private static void AddRelationalObjects(
        MySqlCatalogSnapshot snapshot,
        IReadOnlyDictionary<
            string,
            (MySqlTableMetadata Metadata, string Id)> tablesByIdentity,
        IReadOnlyDictionary<
            string,
            (MySqlColumnMetadata Metadata, string Id)> columnsByIdentity,
        ICollection<MigrationCatalogObject> objects,
        ICollection<MigrationDiagnostic> diagnostics,
        CancellationToken cancellationToken)
    {
        AddKeysAndIndexes(
            snapshot,
            tablesByIdentity,
            columnsByIdentity,
            objects,
            diagnostics,
            cancellationToken,
            out Dictionary<string, BuiltKey> keysByIdentity);
        AddForeignKeys(
            snapshot,
            tablesByIdentity,
            columnsByIdentity,
            keysByIdentity,
            objects,
            diagnostics,
            cancellationToken);
        AddChecks(
            snapshot,
            tablesByIdentity,
            objects,
            diagnostics,
            cancellationToken);
    }

    private static void ValidateRelationalCounts(
        MySqlCatalogSnapshot snapshot,
        MySqlInspectionLimits limits)
    {
        if (snapshot.TableDefinitions.Count > limits.MaxTableDefinitions)
            throw LimitExceeded("table-definition count");
        if (snapshot.Keys.Count > limits.MaxKeys)
            throw LimitExceeded("key count");
        if (snapshot.KeyColumns.Count > limits.MaxKeyColumns)
            throw LimitExceeded("key-column count");
        if (snapshot.ForeignKeys.Count > limits.MaxForeignKeys)
            throw LimitExceeded("foreign-key count");
        if (snapshot.ForeignKeyColumns.Count > limits.MaxForeignKeyColumns)
            throw LimitExceeded("foreign-key-column count");
        if (snapshot.Checks.Count > limits.MaxChecks)
            throw LimitExceeded("check count");
        if (snapshot.Indexes.Count > limits.MaxIndexes)
            throw LimitExceeded("index count");
        if (snapshot.IndexParts.Count > limits.MaxIndexParts)
            throw LimitExceeded("index-part count");

        long structuralRows = checked(
            (long)snapshot.Tables.Count +
            snapshot.Database.ViewCount +
            snapshot.Columns.Count +
            snapshot.TableDefinitions.Count +
            snapshot.Keys.Count +
            snapshot.KeyColumns.Count +
            snapshot.ForeignKeys.Count +
            snapshot.ForeignKeyColumns.Count +
            snapshot.Checks.Count +
            snapshot.IndexParts.Count);
        if (structuralRows > limits.MaxStructuralRowsTotal)
            throw LimitExceeded("aggregate structural-row count");
    }

    private static void ValidateRelationalSnapshot(
        MySqlCatalogSnapshot snapshot,
        IReadOnlySet<string> tableIdentities,
        IReadOnlySet<string> columnIdentities,
        MetadataBudget budget,
        CancellationToken cancellationToken)
    {
        ValidateTableDefinitions(
            snapshot,
            tableIdentities,
            budget,
            cancellationToken);
        ValidateKeys(
            snapshot,
            tableIdentities,
            columnIdentities,
            budget,
            cancellationToken);
        ValidateForeignKeys(
            snapshot,
            tableIdentities,
            columnIdentities,
            budget,
            cancellationToken);
        ValidateChecks(
            snapshot,
            tableIdentities,
            budget,
            cancellationToken);
        ValidateIndexes(
            snapshot,
            tableIdentities,
            columnIdentities,
            budget,
            cancellationToken);
    }

    private static string ColumnIdentity(
        string schema,
        string table,
        string column,
        int lowerCaseTableNames) =>
        string.Concat(
            TableIdentity(schema, table, lowerCaseTableNames),
            "\0",
            column.ToUpperInvariant());

    private static string ConstraintIdentity(
        string schema,
        string table,
        string name,
        int lowerCaseTableNames) =>
        string.Concat(
            TableIdentity(schema, table, lowerCaseTableNames),
            "\0",
            name.ToUpperInvariant());

    private static string IndexIdentity(
        string schema,
        string table,
        string name,
        int lowerCaseTableNames) =>
        ConstraintIdentity(
            schema,
            table,
            name,
            lowerCaseTableNames);

    private static void ValidateTableDefinitions(
        MySqlCatalogSnapshot snapshot,
        IReadOnlySet<string> tableIdentities,
        MetadataBudget budget,
        CancellationToken cancellationToken)
    {
        if (snapshot.TableDefinitions.Count != snapshot.Tables.Count)
        {
            throw InvalidSnapshot(
                "incomplete table-definition metadata");
        }

        int lowerCaseTableNames = snapshot.Server.LowerCaseTableNames;
        var definitions = new HashSet<string>(StringComparer.Ordinal);
        foreach (MySqlTableDefinitionMetadata definition in
                 snapshot.TableDefinitions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            budget.AddRequired(definition.SchemaName, isName: true);
            budget.AddRequired(definition.TableName, isName: true);
            string identity = TableIdentity(
                definition.SchemaName,
                definition.TableName,
                lowerCaseTableNames);
            if (!tableIdentities.Contains(identity) ||
                !definitions.Add(identity) ||
                string.IsNullOrWhiteSpace(definition.Definition))
            {
                throw InvalidSnapshot(
                    "duplicate, invalid, or unowned table-definition metadata");
            }
            budget.AddDefinition(
                definition.Definition,
                definition.DefinitionBytes);
        }
        if (!definitions.SetEquals(tableIdentities))
        {
            throw InvalidSnapshot(
                "incomplete table-definition metadata");
        }
    }

    private static void ValidateKeys(
        MySqlCatalogSnapshot snapshot,
        IReadOnlySet<string> tableIdentities,
        IReadOnlySet<string> columnIdentities,
        MetadataBudget budget,
        CancellationToken cancellationToken)
    {
        int lowerCaseTableNames = snapshot.Server.LowerCaseTableNames;
        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (MySqlKeyMetadata key in snapshot.Keys)
        {
            cancellationToken.ThrowIfCancellationRequested();
            budget.AddRequired(key.SchemaName, isName: true);
            budget.AddRequired(key.TableName, isName: true);
            budget.AddRequired(key.Name, isName: true);
            budget.AddRequired(key.ConstraintType);
            string tableIdentity = TableIdentity(
                key.SchemaName,
                key.TableName,
                lowerCaseTableNames);
            string keyIdentity = ConstraintIdentity(
                key.SchemaName,
                key.TableName,
                key.Name,
                lowerCaseTableNames);
            if (!tableIdentities.Contains(tableIdentity) ||
                key.ConstraintType is not ("PRIMARY KEY" or "UNIQUE") ||
                !keys.Add(keyIdentity))
            {
                throw InvalidSnapshot(
                    "duplicate, invalid, or unowned key metadata");
            }
        }

        var ordinalsByKey =
            new Dictionary<string, HashSet<int>>(StringComparer.Ordinal);
        foreach (MySqlKeyColumnMetadata column in snapshot.KeyColumns)
        {
            cancellationToken.ThrowIfCancellationRequested();
            budget.AddRequired(column.SchemaName, isName: true);
            budget.AddRequired(column.TableName, isName: true);
            budget.AddRequired(column.ConstraintName, isName: true);
            budget.AddRequired(column.ColumnName, isName: true);
            string keyIdentity = ConstraintIdentity(
                column.SchemaName,
                column.TableName,
                column.ConstraintName,
                lowerCaseTableNames);
            string columnIdentity = ColumnIdentity(
                column.SchemaName,
                column.TableName,
                column.ColumnName,
                lowerCaseTableNames);
            if (!keys.Contains(keyIdentity) ||
                !columnIdentities.Contains(columnIdentity) ||
                column.OrdinalPosition <= 0)
            {
                throw InvalidSnapshot(
                    "invalid or unowned key-column metadata");
            }
            if (!ordinalsByKey.TryGetValue(
                    keyIdentity,
                    out HashSet<int>? ordinals))
            {
                ordinals = [];
                ordinalsByKey.Add(keyIdentity, ordinals);
            }
            if (!ordinals.Add(column.OrdinalPosition))
            {
                throw InvalidSnapshot(
                    "duplicate key-column ordinal metadata");
            }
        }
    }

    private static void ValidateForeignKeys(
        MySqlCatalogSnapshot snapshot,
        IReadOnlySet<string> tableIdentities,
        IReadOnlySet<string> columnIdentities,
        MetadataBudget budget,
        CancellationToken cancellationToken)
    {
        int lowerCaseTableNames = snapshot.Server.LowerCaseTableNames;
        var foreignKeys =
            new Dictionary<string, MySqlForeignKeyMetadata>(
                StringComparer.Ordinal);
        foreach (MySqlForeignKeyMetadata foreignKey in snapshot.ForeignKeys)
        {
            cancellationToken.ThrowIfCancellationRequested();
            budget.AddRequired(foreignKey.SchemaName, isName: true);
            budget.AddRequired(foreignKey.TableName, isName: true);
            budget.AddRequired(foreignKey.Name, isName: true);
            budget.AddRequired(
                foreignKey.ReferencedSchemaName,
                isName: true);
            budget.AddRequired(
                foreignKey.ReferencedTableName,
                isName: true);
            budget.Add(
                foreignKey.UniqueConstraintSchemaName,
                isName: true);
            budget.Add(
                foreignKey.UniqueConstraintName,
                isName: true);
            budget.AddRequired(foreignKey.MatchOption);
            budget.AddRequired(foreignKey.UpdateRule);
            budget.AddRequired(foreignKey.DeleteRule);

            string identity = ConstraintIdentity(
                foreignKey.SchemaName,
                foreignKey.TableName,
                foreignKey.Name,
                lowerCaseTableNames);
            if (!tableIdentities.Contains(TableIdentity(
                    foreignKey.SchemaName,
                    foreignKey.TableName,
                    lowerCaseTableNames)) ||
                !foreignKeys.TryAdd(identity, foreignKey))
            {
                throw InvalidSnapshot(
                    "duplicate, invalid, or unowned foreign-key metadata");
            }
        }

        var ordinalsByForeignKey =
            new Dictionary<string, HashSet<int>>(StringComparer.Ordinal);
        var referencedPositionsByForeignKey =
            new Dictionary<string, HashSet<int>>(StringComparer.Ordinal);
        foreach (MySqlForeignKeyColumnMetadata column in
                 snapshot.ForeignKeyColumns)
        {
            cancellationToken.ThrowIfCancellationRequested();
            budget.AddRequired(column.SchemaName, isName: true);
            budget.AddRequired(column.TableName, isName: true);
            budget.AddRequired(column.ConstraintName, isName: true);
            budget.AddRequired(column.ColumnName, isName: true);
            budget.AddRequired(
                column.ReferencedSchemaName,
                isName: true);
            budget.AddRequired(
                column.ReferencedTableName,
                isName: true);
            budget.AddRequired(
                column.ReferencedColumnName,
                isName: true);
            string identity = ConstraintIdentity(
                column.SchemaName,
                column.TableName,
                column.ConstraintName,
                lowerCaseTableNames);
            bool referencedTableIsLocal =
                tableIdentities.Contains(TableIdentity(
                    column.ReferencedSchemaName,
                    column.ReferencedTableName,
                    lowerCaseTableNames));
            bool referencedColumnIsLocal =
                columnIdentities.Contains(ColumnIdentity(
                    column.ReferencedSchemaName,
                    column.ReferencedTableName,
                    column.ReferencedColumnName,
                    lowerCaseTableNames));
            if (!foreignKeys.TryGetValue(
                    identity,
                    out MySqlForeignKeyMetadata? foreignKey) ||
                !DatabaseNamesEqual(
                    column.ReferencedSchemaName,
                    foreignKey.ReferencedSchemaName,
                    lowerCaseTableNames) ||
                !string.Equals(
                    column.ReferencedTableName,
                    foreignKey.ReferencedTableName,
                    lowerCaseTableNames == 0
                        ? StringComparison.Ordinal
                        : StringComparison.OrdinalIgnoreCase) ||
                !columnIdentities.Contains(ColumnIdentity(
                    column.SchemaName,
                    column.TableName,
                    column.ColumnName,
                    lowerCaseTableNames)) ||
                (referencedTableIsLocal &&
                 !referencedColumnIsLocal) ||
                column.OrdinalPosition <= 0 ||
                column.PositionInUniqueConstraint <= 0)
            {
                throw InvalidSnapshot(
                    "invalid or unowned foreign-key-column metadata");
            }
            if (!ordinalsByForeignKey.TryGetValue(
                    identity,
                    out HashSet<int>? ordinals))
            {
                ordinals = [];
                ordinalsByForeignKey.Add(identity, ordinals);
                referencedPositionsByForeignKey.Add(identity, []);
            }
            if (!ordinals.Add(column.OrdinalPosition) ||
                column.PositionInUniqueConstraint is int position &&
                !referencedPositionsByForeignKey[identity].Add(position))
            {
                throw InvalidSnapshot(
                    "duplicate foreign-key-column ordinal metadata");
            }
        }
        ValidateContiguousOrdinals(
            ordinalsByForeignKey.Values,
            "foreign-key-column");
    }

    private static void ValidateChecks(
        MySqlCatalogSnapshot snapshot,
        IReadOnlySet<string> tableIdentities,
        MetadataBudget budget,
        CancellationToken cancellationToken)
    {
        int lowerCaseTableNames = snapshot.Server.LowerCaseTableNames;
        var checks = new HashSet<string>(StringComparer.Ordinal);
        foreach (MySqlCheckMetadata check in snapshot.Checks)
        {
            cancellationToken.ThrowIfCancellationRequested();
            budget.AddRequired(check.SchemaName, isName: true);
            budget.AddRequired(check.TableName, isName: true);
            budget.AddRequired(check.Name, isName: true);
            if (!tableIdentities.Contains(TableIdentity(
                    check.SchemaName,
                    check.TableName,
                    lowerCaseTableNames)) ||
                !checks.Add(ConstraintIdentity(
                    check.SchemaName,
                    check.TableName,
                    check.Name,
                    lowerCaseTableNames)) ||
                string.IsNullOrWhiteSpace(check.Clause))
            {
                throw InvalidSnapshot(
                    "duplicate, invalid, or unowned check metadata");
            }
            budget.AddExpression(check.Clause, check.ClauseBytes);
        }
    }

    private static void ValidateIndexes(
        MySqlCatalogSnapshot snapshot,
        IReadOnlySet<string> tableIdentities,
        IReadOnlySet<string> columnIdentities,
        MetadataBudget budget,
        CancellationToken cancellationToken)
    {
        int lowerCaseTableNames = snapshot.Server.LowerCaseTableNames;
        var indexes = new HashSet<string>(StringComparer.Ordinal);
        foreach (MySqlIndexMetadata index in snapshot.Indexes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            budget.AddRequired(index.SchemaName, isName: true);
            budget.AddRequired(index.TableName, isName: true);
            budget.AddRequired(index.Name, isName: true);
            budget.AddRequired(index.IndexType);
            if (!tableIdentities.Contains(TableIdentity(
                    index.SchemaName,
                    index.TableName,
                    lowerCaseTableNames)) ||
                !indexes.Add(IndexIdentity(
                    index.SchemaName,
                    index.TableName,
                    index.Name,
                    lowerCaseTableNames)))
            {
                throw InvalidSnapshot(
                    "duplicate, invalid, or unowned index metadata");
            }
        }

        var sequencesByIndex =
            new Dictionary<string, HashSet<int>>(StringComparer.Ordinal);
        foreach (MySqlIndexPartMetadata part in snapshot.IndexParts)
        {
            cancellationToken.ThrowIfCancellationRequested();
            budget.AddRequired(part.SchemaName, isName: true);
            budget.AddRequired(part.TableName, isName: true);
            budget.AddRequired(part.IndexName, isName: true);
            budget.Add(part.ColumnName, isName: true);
            budget.Add(part.SortDirection);
            string identity = IndexIdentity(
                part.SchemaName,
                part.TableName,
                part.IndexName,
                lowerCaseTableNames);
            bool hasColumn = !string.IsNullOrWhiteSpace(part.ColumnName);
            bool hasExpression = !string.IsNullOrWhiteSpace(part.Expression);
            if (!indexes.Contains(identity) ||
                part.Sequence <= 0 ||
                hasColumn == hasExpression ||
                hasExpression != (part.ExpressionBytes is not null) ||
                part.SortDirection is not (null or "A" or "D") ||
                part.PrefixLength <= 0 ||
                hasColumn &&
                !columnIdentities.Contains(ColumnIdentity(
                    part.SchemaName,
                    part.TableName,
                    part.ColumnName!,
                    lowerCaseTableNames)))
            {
                throw InvalidSnapshot(
                    "invalid or unowned index-part metadata");
            }
            if (hasExpression)
            {
                budget.AddExpression(
                    part.Expression!,
                    part.ExpressionBytes!.Value);
            }
            if (!sequencesByIndex.TryGetValue(
                    identity,
                    out HashSet<int>? sequences))
            {
                sequences = [];
                sequencesByIndex.Add(identity, sequences);
            }
            if (!sequences.Add(part.Sequence))
            {
                throw InvalidSnapshot(
                    "duplicate index-part ordinal metadata");
            }
        }
        ValidateContiguousOrdinals(
            sequencesByIndex.Values,
            "index-part");
    }

    private static void ValidateContiguousOrdinals(
        IEnumerable<HashSet<int>> groups,
        string category)
    {
        foreach (HashSet<int> ordinals in groups)
        {
            if (!ordinals.Order().SequenceEqual(
                    Enumerable.Range(1, ordinals.Count)))
            {
                throw InvalidSnapshot(
                    $"noncontiguous {category} ordinal metadata");
            }
        }
    }

    private static void AddKeysAndIndexes(
        MySqlCatalogSnapshot snapshot,
        IReadOnlyDictionary<
            string,
            (MySqlTableMetadata Metadata, string Id)> tablesByIdentity,
        IReadOnlyDictionary<
            string,
            (MySqlColumnMetadata Metadata, string Id)> columnsByIdentity,
        ICollection<MigrationCatalogObject> objects,
        ICollection<MigrationDiagnostic> diagnostics,
        CancellationToken cancellationToken,
        out Dictionary<string, BuiltKey> keysByIdentity)
    {
        int lowerCaseTableNames = snapshot.Server.LowerCaseTableNames;
        Dictionary<string, MySqlIndexMetadata> indexesByIdentity =
            snapshot.Indexes.ToDictionary(
                item => IndexIdentity(
                    item.SchemaName,
                    item.TableName,
                    item.Name,
                    lowerCaseTableNames),
                StringComparer.Ordinal);
        Dictionary<string, MySqlIndexPartMetadata[]> partsByIndex =
            snapshot.IndexParts
                .GroupBy(item => IndexIdentity(
                    item.SchemaName,
                    item.TableName,
                    item.IndexName,
                    lowerCaseTableNames))
                .ToDictionary(
                    static group => group.Key,
                    static group => group
                        .OrderBy(static item => item.Sequence)
                        .ToArray(),
                    StringComparer.Ordinal);
        Dictionary<string, MySqlKeyColumnMetadata[]> columnsByKey =
            snapshot.KeyColumns
                .GroupBy(item => ConstraintIdentity(
                    item.SchemaName,
                    item.TableName,
                    item.ConstraintName,
                    lowerCaseTableNames))
                .ToDictionary(
                    static group => group.Key,
                    static group => group
                        .OrderBy(static item => item.OrdinalPosition)
                        .ToArray(),
                    StringComparer.Ordinal);
        var constraintBackedIndexes =
            new HashSet<string>(StringComparer.Ordinal);
        keysByIdentity = new Dictionary<string, BuiltKey>(
            StringComparer.Ordinal);

        foreach (MySqlKeyMetadata key in snapshot.Keys
                     .OrderBy(static item => item.SchemaName, StringComparer.Ordinal)
                     .ThenBy(static item => item.TableName, StringComparer.Ordinal)
                     .ThenBy(static item => item.Name, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string tableIdentity = TableIdentity(
                key.SchemaName,
                key.TableName,
                lowerCaseTableNames);
            string keyIdentity = ConstraintIdentity(
                key.SchemaName,
                key.TableName,
                key.Name,
                lowerCaseTableNames);
            string indexIdentity = IndexIdentity(
                key.SchemaName,
                key.TableName,
                key.Name,
                lowerCaseTableNames);
            (MySqlTableMetadata table, string tableId) =
                tablesByIdentity[tableIdentity];
            columnsByKey.TryGetValue(
                keyIdentity,
                out MySqlKeyColumnMetadata[]? keyColumns);
            keyColumns ??= [];
            indexesByIdentity.TryGetValue(
                indexIdentity,
                out MySqlIndexMetadata? backingIndex);
            partsByIndex.TryGetValue(
                indexIdentity,
                out MySqlIndexPartMetadata[]? backingParts);
            backingParts ??= [];
            if (backingIndex is not null)
                constraintBackedIndexes.Add(indexIdentity);

            (MySqlKeyColumnMetadata Column, MySqlColumnMetadata Metadata, string Id)[]
                resolvedColumns = keyColumns
                    .Where(item => columnsByIdentity.ContainsKey(
                        ColumnIdentity(
                            item.SchemaName,
                            item.TableName,
                            item.ColumnName,
                            lowerCaseTableNames)))
                    .Select(item =>
                    {
                        (MySqlColumnMetadata Metadata, string Id) column =
                            columnsByIdentity[ColumnIdentity(
                                item.SchemaName,
                                item.TableName,
                                item.ColumnName,
                                lowerCaseTableNames)];
                        return (item, column.Metadata, column.Id);
                    })
                    .ToArray();
            bool membershipComplete =
                keyColumns.Length > 0 &&
                keyColumns
                    .Select(static item => item.OrdinalPosition)
                    .SequenceEqual(Enumerable.Range(1, keyColumns.Length)) &&
                resolvedColumns.Length == keyColumns.Length &&
                keyColumns
                    .Select(static item => item.ColumnName)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .Count() == keyColumns.Length;
            bool backingShapeSupported =
                backingIndex is not null &&
                backingIndex.IsUnique &&
                IsExactVisibleAscendingBtree(
                    backingIndex,
                    backingParts);
            bool backingMembershipMatches =
                membershipComplete &&
                backingShapeSupported &&
                KeyColumnsMatchIndexParts(
                    keyColumns,
                    backingParts);
            bool nullableUnique =
                string.Equals(
                    key.ConstraintType,
                    "UNIQUE",
                    StringComparison.Ordinal) &&
                resolvedColumns.Any(static item =>
                    item.Metadata.IsNullable);
            bool nullablePrimary =
                string.Equals(
                    key.ConstraintType,
                    "PRIMARY KEY",
                    StringComparison.Ordinal) &&
                resolvedColumns.Any(static item =>
                    item.Metadata.IsNullable);
            string logicalKind = key.ConstraintType switch
            {
                "PRIMARY KEY" => "primary",
                "UNIQUE" => "unique",
                _ => "mysql-unknown-key",
            };
            string effectiveKind =
                !membershipComplete ||
                backingIndex is null ||
                backingShapeSupported && !backingMembershipMatches
                    ? "mysql-unresolved-key"
                    : nullablePrimary
                        ? "mysql-invalid-primary-key"
                        : nullableUnique
                            ? "mysql-null-sensitive-unique"
                            : !backingShapeSupported
                                ? "mysql-unsupported-key-index"
                                : logicalKind;
            bool bindingComplete =
                backingMembershipMatches &&
                !nullablePrimary;
            string keyId = ObjectId(
                "key",
                key.SchemaName,
                key.TableName,
                key.Name);
            var facets = new List<MigrationCatalogFacet>
            {
                Facet("kind", effectiveKind),
                Facet("mysqlConstraintType", key.ConstraintType),
                Facet(
                    "mysqlMembershipComplete",
                    Boolean(membershipComplete)),
                Facet(
                    "mysqlBackingIndexMatched",
                    Boolean(backingMembershipMatches)),
            };
            if (backingIndex is not null)
            {
                AddIndexEvidenceFacets(
                    facets,
                    "mysqlBackingIndex",
                    backingIndex,
                    backingParts);
            }
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = keyId,
                Kind = MigrationObjectKind.Key,
                ParentObjectId = tableId,
                SourceNamespace = table.SchemaName,
                SourceName = key.Name,
                Facets = facets.AsReadOnly(),
                Members = resolvedColumns
                    .Select((item, ordinal) => Member(
                        item.Id,
                        MigrationObjectReferenceRoles.Column,
                        ordinal))
                    .ToArray(),
                DependsOn = resolvedColumns
                    .Select(static item => item.Id)
                    .Distinct(StringComparer.Ordinal)
                    .ToArray(),
            });
            keysByIdentity.Add(
                keyIdentity,
                new BuiltKey(
                    keyId,
                    effectiveKind,
                    bindingComplete,
                    keyColumns));

            if (!membershipComplete ||
                backingIndex is null ||
                backingShapeSupported && !backingMembershipMatches)
            {
                diagnostics.Add(Diagnostic(
                    keyId,
                    "MIG-MYSQL-KEY-MEMBERSHIP-UNKNOWN-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unknown,
                    "The MySQL key membership could not be proven.",
                    "The ordered constraint columns and an exact same-name unique backing index were not both complete and identical.",
                    "Restore complete key and STATISTICS visibility, then inspect again.",
                    canOverride: false));
            }
            if (nullableUnique)
            {
                diagnostics.Add(Diagnostic(
                    keyId,
                    "MIG-MYSQL-NULLABLE-UNIQUE-SEMANTICS-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unsupported,
                    "The nullable MySQL unique-key semantics are not target-equivalent.",
                    "MySQL permits multiple unique-key tuples containing NULL, which is not admitted as target-equivalent by this checkpoint.",
                    "Choose and validate an explicit target uniqueness design.",
                    canOverride: false));
            }
            if (nullablePrimary ||
                backingIndex is not null && !backingShapeSupported)
            {
                diagnostics.Add(Diagnostic(
                    keyId,
                    "MIG-MYSQL-KEY-BACKING-INDEX-UNSUPPORTED-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unsupported,
                    "The MySQL key has an unsupported backing-index shape.",
                    "Only a nonnullable key backed by an exact visible, ascending, full-column unique BTREE index is admitted.",
                    "Rebuild the key on an ordinary visible BTREE index or provide a reviewed target design.",
                    canOverride: false));
            }
        }

        HashSet<string> ambiguousForeignKeySupportIndexes =
            FindAmbiguousForeignKeySupportIndexes(
                snapshot,
                indexesByIdentity,
                partsByIndex);
        foreach (MySqlIndexMetadata index in snapshot.Indexes
                     .OrderBy(static item => item.SchemaName, StringComparer.Ordinal)
                     .ThenBy(static item => item.TableName, StringComparer.Ordinal)
                     .ThenBy(static item => item.Name, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string identity = IndexIdentity(
                index.SchemaName,
                index.TableName,
                index.Name,
                lowerCaseTableNames);
            if (constraintBackedIndexes.Contains(identity))
                continue;

            partsByIndex.TryGetValue(
                identity,
                out MySqlIndexPartMetadata[]? parts);
            parts ??= [];
            (MySqlIndexPartMetadata Part, string Id)[] resolvedColumns =
                parts
                    .Where(static item => item.ColumnName is not null)
                    .Where(item => columnsByIdentity.ContainsKey(
                        ColumnIdentity(
                            item.SchemaName,
                            item.TableName,
                            item.ColumnName!,
                            lowerCaseTableNames)))
                    .Select(item => (
                        item,
                        columnsByIdentity[ColumnIdentity(
                            item.SchemaName,
                            item.TableName,
                            item.ColumnName!,
                            lowerCaseTableNames)].Id))
                    .ToArray();
            bool membershipComplete =
                parts.Length > 0 &&
                parts.All(static item => item.ColumnName is not null) &&
                resolvedColumns.Length == parts.Length &&
                resolvedColumns
                    .Select(static item => item.Id)
                    .Distinct(StringComparer.Ordinal)
                    .Count() == parts.Length;
            bool exactShape =
                IsExactVisibleAscendingBtree(index, parts);
            bool foreignKeySupportAmbiguous =
                ambiguousForeignKeySupportIndexes.Contains(identity);
            string kind = IndexKind(
                index,
                parts,
                membershipComplete,
                exactShape,
                foreignKeySupportAmbiguous);
            string tableIdentity = TableIdentity(
                index.SchemaName,
                index.TableName,
                lowerCaseTableNames);
            (MySqlTableMetadata table, string tableId) =
                tablesByIdentity[tableIdentity];
            string indexId = ObjectId(
                "index",
                index.SchemaName,
                index.TableName,
                index.Name);
            var facets = new List<MigrationCatalogFacet>
            {
                Facet("kind", kind),
                Facet("unique", Boolean(index.IsUnique)),
                Facet(
                    "mysqlMembershipComplete",
                    Boolean(membershipComplete)),
            };
            AddIndexEvidenceFacets(
                facets,
                "mysqlIndex",
                index,
                parts);
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = indexId,
                Kind = MigrationObjectKind.Index,
                ParentObjectId = tableId,
                SourceNamespace = table.SchemaName,
                SourceName = index.Name,
                Facets = facets.AsReadOnly(),
                Members = membershipComplete
                    ? resolvedColumns
                        .Select((item, ordinal) => Member(
                            item.Id,
                            MigrationObjectReferenceRoles.Column,
                            ordinal))
                        .ToArray()
                    : [],
                DependsOn = resolvedColumns
                    .Select(static item => item.Id)
                    .Distinct(StringComparer.Ordinal)
                    .ToArray(),
            });

            if (foreignKeySupportAmbiguous)
            {
                diagnostics.Add(Diagnostic(
                    indexId,
                    "MIG-MYSQL-FK-SUPPORT-AMBIGUOUS-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unknown,
                    "The MySQL index may be implicit foreign-key support.",
                    "Its exact nonunique child-column tuple matches a foreign key, and static metadata does not prove whether MySQL created it implicitly or a user intended it independently.",
                    "Review index provenance and explicitly retain or replace it in the target design.",
                    canOverride: false));
            }
            else if (!string.Equals(
                         kind,
                         "standard",
                         StringComparison.Ordinal))
            {
                diagnostics.Add(Diagnostic(
                    indexId,
                    "MIG-MYSQL-INDEX-SHAPE-UNSUPPORTED-001",
                    MigrationDiagnosticSeverity.Error,
                    parts.Length > 0
                        ? MigrationCompatibilityStatus.Unsupported
                        : MigrationCompatibilityStatus.Unknown,
                    "The MySQL index shape is not directly target-compatible.",
                    "Only complete visible, ascending, full-column, nonunique BTREE indexes are admitted. Unique standalone, functional, prefix, descending, invisible, and non-BTREE variants remain provider-specific.",
                    "Simplify the index or define and test an explicit target index design.",
                    canOverride: false));
            }
        }
    }

    private static void AddForeignKeys(
        MySqlCatalogSnapshot snapshot,
        IReadOnlyDictionary<
            string,
            (MySqlTableMetadata Metadata, string Id)> tablesByIdentity,
        IReadOnlyDictionary<
            string,
            (MySqlColumnMetadata Metadata, string Id)> columnsByIdentity,
        IReadOnlyDictionary<string, BuiltKey> keysByIdentity,
        ICollection<MigrationCatalogObject> objects,
        ICollection<MigrationDiagnostic> diagnostics,
        CancellationToken cancellationToken)
    {
        int lowerCaseTableNames = snapshot.Server.LowerCaseTableNames;
        Dictionary<string, MySqlForeignKeyColumnMetadata[]> columnsByForeignKey =
            snapshot.ForeignKeyColumns
                .GroupBy(item => ConstraintIdentity(
                    item.SchemaName,
                    item.TableName,
                    item.ConstraintName,
                    lowerCaseTableNames))
                .ToDictionary(
                    static group => group.Key,
                    static group => group
                        .OrderBy(static item => item.OrdinalPosition)
                        .ToArray(),
                    StringComparer.Ordinal);

        foreach (MySqlForeignKeyMetadata foreignKey in snapshot.ForeignKeys
                     .OrderBy(static item => item.SchemaName, StringComparer.Ordinal)
                     .ThenBy(static item => item.TableName, StringComparer.Ordinal)
                     .ThenBy(static item => item.Name, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string identity = ConstraintIdentity(
                foreignKey.SchemaName,
                foreignKey.TableName,
                foreignKey.Name,
                lowerCaseTableNames);
            columnsByForeignKey.TryGetValue(
                identity,
                out MySqlForeignKeyColumnMetadata[]? pairs);
            pairs ??= [];
            string tableIdentity = TableIdentity(
                foreignKey.SchemaName,
                foreignKey.TableName,
                lowerCaseTableNames);
            (MySqlTableMetadata table, string tableId) =
                tablesByIdentity[tableIdentity];
            string foreignKeyId = ObjectId(
                "foreign-key",
                foreignKey.SchemaName,
                foreignKey.TableName,
                foreignKey.Name);

            BuiltKey? referencedKey = null;
            if (!string.IsNullOrWhiteSpace(
                    foreignKey.UniqueConstraintSchemaName) &&
                !string.IsNullOrWhiteSpace(
                    foreignKey.UniqueConstraintName))
            {
                keysByIdentity.TryGetValue(
                    ConstraintIdentity(
                        foreignKey.UniqueConstraintSchemaName,
                        foreignKey.ReferencedTableName,
                        foreignKey.UniqueConstraintName,
                        lowerCaseTableNames),
                    out referencedKey);
            }
            (MySqlForeignKeyColumnMetadata Pair, string Id)[] sourceColumns =
                pairs
                    .Where(item => columnsByIdentity.ContainsKey(
                        ColumnIdentity(
                            item.SchemaName,
                            item.TableName,
                            item.ColumnName,
                            lowerCaseTableNames)))
                    .Select(item => (
                        item,
                        columnsByIdentity[ColumnIdentity(
                            item.SchemaName,
                            item.TableName,
                            item.ColumnName,
                            lowerCaseTableNames)].Id))
                    .ToArray();
            bool bindingComplete =
                referencedKey?.BindingComplete == true &&
                foreignKey.UniqueConstraintSchemaName is not null &&
                DatabaseNamesEqual(
                    foreignKey.UniqueConstraintSchemaName,
                    foreignKey.ReferencedSchemaName,
                    lowerCaseTableNames) &&
                pairs.Length > 0 &&
                sourceColumns.Length == pairs.Length &&
                referencedKey.OrderedColumns.Count == pairs.Length &&
                pairs.Select((pair, index) =>
                        pair.PositionInUniqueConstraint == index + 1 &&
                        DatabaseNamesEqual(
                            pair.ReferencedSchemaName,
                            foreignKey.ReferencedSchemaName,
                            lowerCaseTableNames) &&
                        string.Equals(
                            pair.ReferencedTableName,
                            foreignKey.ReferencedTableName,
                            lowerCaseTableNames == 0
                                ? StringComparison.Ordinal
                                : StringComparison.OrdinalIgnoreCase) &&
                        string.Equals(
                            pair.ReferencedColumnName,
                            referencedKey.OrderedColumns[index].ColumnName,
                            StringComparison.OrdinalIgnoreCase))
                    .All(static item => item);
            bool unsupportedShape =
                ForeignKeyHasUnsupportedShape(foreignKey) ||
                referencedKey is not null &&
                referencedKey.EffectiveKind is not ("primary" or "unique");

            if (!bindingComplete)
            {
                var dependencies = sourceColumns
                    .Select(static item => item.Id)
                    .Distinct(StringComparer.Ordinal)
                    .ToList();
                if (referencedKey is not null)
                    dependencies.Add(referencedKey.ObjectId);
                objects.Add(new MigrationCatalogObject
                {
                    ObjectId = foreignKeyId,
                    Kind = MigrationObjectKind.Other,
                    ParentObjectId = tableId,
                    SourceNamespace = table.SchemaName,
                    SourceName = foreignKey.Name,
                    Facets =
                    [
                        Facet("kind", "mysql-unresolved-foreign-key"),
                        Facet(
                            "mysqlReferencedSchema",
                            foreignKey.ReferencedSchemaName),
                        Facet(
                            "mysqlReferencedTable",
                            foreignKey.ReferencedTableName),
                        Facet(
                            "mysqlUniqueConstraintSchema",
                            foreignKey.UniqueConstraintSchemaName),
                        Facet(
                            "mysqlUniqueConstraintName",
                            foreignKey.UniqueConstraintName),
                        Facet("mysqlMatchOption", foreignKey.MatchOption),
                        Facet("mysqlUpdateRule", foreignKey.UpdateRule),
                        Facet("mysqlDeleteRule", foreignKey.DeleteRule),
                    ],
                    DependsOn = dependencies
                        .Distinct(StringComparer.Ordinal)
                        .ToArray(),
                });
                diagnostics.Add(Diagnostic(
                    foreignKeyId,
                    "MIG-MYSQL-FK-BINDING-UNKNOWN-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unknown,
                    "The MySQL foreign key could not be bound to a target key.",
                    "The exact referenced schema, table, unique constraint, supported key shape, and ordered referenced-column pairing were not all proven.",
                    "Restore complete local metadata visibility or create a reviewed target key and foreign-key design.",
                    canOverride: false));
                if (unsupportedShape)
                {
                    AddForeignKeyShapeDiagnostic(
                        foreignKeyId,
                        diagnostics);
                }
                continue;
            }

            var facets = new List<MigrationCatalogFacet>
            {
                Facet("timing", "immediate"),
                Facet("match", ForeignKeyMatch(foreignKey.MatchOption)),
                Facet("deferrable", "false"),
                Facet("deferred", "false"),
                Facet(
                    "onDelete",
                    ReferentialAction(foreignKey.DeleteRule)),
                Facet(
                    "mysqlReferencedSchema",
                    foreignKey.ReferencedSchemaName),
                Facet(
                    "mysqlReferencedTable",
                    foreignKey.ReferencedTableName),
                Facet(
                    "mysqlUniqueConstraintSchema",
                    foreignKey.UniqueConstraintSchemaName),
                Facet(
                    "mysqlUniqueConstraintName",
                    foreignKey.UniqueConstraintName),
                Facet("mysqlMatchOption", foreignKey.MatchOption),
                Facet("mysqlUpdateRule", foreignKey.UpdateRule),
                Facet("mysqlDeleteRule", foreignKey.DeleteRule),
            };
            if (!IsTrivialReferentialAction(foreignKey.UpdateRule))
            {
                facets.Add(Facet(
                    "onUpdate",
                    ReferentialAction(foreignKey.UpdateRule)));
            }
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = foreignKeyId,
                Kind = MigrationObjectKind.ForeignKey,
                ParentObjectId = tableId,
                SourceNamespace = table.SchemaName,
                SourceName = foreignKey.Name,
                Facets = facets.AsReadOnly(),
                Members = sourceColumns
                    .Select((item, ordinal) => Member(
                        item.Id,
                        MigrationObjectReferenceRoles.SourceColumn,
                        ordinal))
                    .Append(Member(
                        referencedKey!.ObjectId,
                        MigrationObjectReferenceRoles.ReferencedKey,
                        0))
                    .ToArray(),
                DependsOn = sourceColumns
                    .Select(static item => item.Id)
                    .Append(referencedKey!.ObjectId)
                    .Distinct(StringComparer.Ordinal)
                    .ToArray(),
            });
            if (unsupportedShape)
            {
                AddForeignKeyShapeDiagnostic(
                    foreignKeyId,
                    diagnostics);
            }
        }
    }

    private static void AddChecks(
        MySqlCatalogSnapshot snapshot,
        IReadOnlyDictionary<
            string,
            (MySqlTableMetadata Metadata, string Id)> tablesByIdentity,
        ICollection<MigrationCatalogObject> objects,
        ICollection<MigrationDiagnostic> diagnostics,
        CancellationToken cancellationToken)
    {
        int lowerCaseTableNames = snapshot.Server.LowerCaseTableNames;
        foreach (MySqlCheckMetadata check in snapshot.Checks
                     .OrderBy(static item => item.SchemaName, StringComparer.Ordinal)
                     .ThenBy(static item => item.TableName, StringComparer.Ordinal)
                     .ThenBy(static item => item.Name, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            (MySqlTableMetadata table, string tableId) =
                tablesByIdentity[TableIdentity(
                    check.SchemaName,
                    check.TableName,
                    lowerCaseTableNames)];
            string checkId = ObjectId(
                "check",
                check.SchemaName,
                check.TableName,
                check.Name);
            var facets = new List<MigrationCatalogFacet>
            {
                Facet("mysqlEnforced", Boolean(check.IsEnforced)),
            };
            AddDefinitionDigestFacets(
                facets,
                "mysqlCheckClause",
                "csharpdb-mysql-check-clause/v1",
                check.ClauseBytes,
                check.Clause);
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = checkId,
                Kind = MigrationObjectKind.CheckConstraint,
                ParentObjectId = tableId,
                SourceNamespace = table.SchemaName,
                SourceName = check.Name,
                Facets = facets.AsReadOnly(),
            });
            diagnostics.Add(Diagnostic(
                checkId,
                "MIG-MYSQL-CHECK-INVENTORY-ONLY-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "The MySQL check clause is inventory-only.",
                "The bounded clause was retained only in memory and represented by a digest; no MySQL expression parser has proven deterministic row-local target semantics.",
                "Review and translate the check expression explicitly.",
                canOverride: false));
            if (!check.IsEnforced)
            {
                diagnostics.Add(Diagnostic(
                    checkId,
                    "MIG-MYSQL-CHECK-NOT-ENFORCED-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unsupported,
                    "The MySQL check constraint is not enforced.",
                    "An unenforced source check cannot be silently lowered to an enforced target constraint.",
                    "Remove the check or validate and enable it before migration.",
                    canOverride: false));
            }
        }
    }

    private static HashSet<string> FindAmbiguousForeignKeySupportIndexes(
        MySqlCatalogSnapshot snapshot,
        IReadOnlyDictionary<string, MySqlIndexMetadata> indexesByIdentity,
        IReadOnlyDictionary<string, MySqlIndexPartMetadata[]> partsByIndex)
    {
        int lowerCaseTableNames = snapshot.Server.LowerCaseTableNames;
        var indexesByTuple =
            new Dictionary<string, List<string>>(StringComparer.Ordinal);
        foreach ((string identity, MySqlIndexMetadata index) in
                 indexesByIdentity)
        {
            if (index.IsUnique ||
                !partsByIndex.TryGetValue(
                    identity,
                    out MySqlIndexPartMetadata[]? parts) ||
                !IsExactVisibleAscendingBtree(index, parts))
            {
                continue;
            }
            string tupleIdentity = ColumnTupleIdentity(
                index.SchemaName,
                index.TableName,
                parts.Select(static item => item.ColumnName!),
                lowerCaseTableNames);
            if (!indexesByTuple.TryGetValue(
                    tupleIdentity,
                    out List<string>? matchingIndexes))
            {
                matchingIndexes = [];
                indexesByTuple.Add(
                    tupleIdentity,
                    matchingIndexes);
            }
            matchingIndexes.Add(identity);
        }

        var result = new HashSet<string>(StringComparer.Ordinal);
        foreach (IGrouping<string, MySqlForeignKeyColumnMetadata> group in
                 snapshot.ForeignKeyColumns.GroupBy(item =>
                     ConstraintIdentity(
                         item.SchemaName,
                         item.TableName,
                         item.ConstraintName,
                         lowerCaseTableNames)))
        {
            MySqlForeignKeyColumnMetadata[] pairs = group
                .OrderBy(static item => item.OrdinalPosition)
                .ToArray();
            if (pairs.Length == 0 ||
                !pairs
                    .Select(static item => item.OrdinalPosition)
                    .SequenceEqual(Enumerable.Range(1, pairs.Length)))
            {
                continue;
            }
            string tupleIdentity = ColumnTupleIdentity(
                pairs[0].SchemaName,
                pairs[0].TableName,
                pairs.Select(static item => item.ColumnName),
                lowerCaseTableNames);
            if (indexesByTuple.TryGetValue(
                    tupleIdentity,
                    out List<string>? matchingIndexes))
            {
                result.UnionWith(matchingIndexes);
            }
        }
        return result;
    }

    private static string ColumnTupleIdentity(
        string schema,
        string table,
        IEnumerable<string> columns,
        int lowerCaseTableNames) =>
        string.Concat(
            TableIdentity(schema, table, lowerCaseTableNames),
            "\0",
            MySqlStableDigest.Sequence(
                "csharpdb-mysql-column-tuple/v1",
                columns.Select(static item =>
                    (string?)item.ToUpperInvariant())));

    private static bool IsExactVisibleAscendingBtree(
        MySqlIndexMetadata index,
        IReadOnlyList<MySqlIndexPartMetadata> parts) =>
        index.IsVisible &&
        string.Equals(
            index.IndexType,
            "BTREE",
            StringComparison.OrdinalIgnoreCase) &&
        parts.Count > 0 &&
        parts.All(static part =>
            part.ColumnName is not null &&
            part.Expression is null &&
            part.ExpressionBytes is null &&
            part.PrefixLength is null &&
            string.Equals(
                part.SortDirection,
                "A",
                StringComparison.Ordinal));

    private static bool KeyColumnsMatchIndexParts(
        IReadOnlyList<MySqlKeyColumnMetadata> columns,
        IReadOnlyList<MySqlIndexPartMetadata> parts) =>
        columns.Count == parts.Count &&
        columns.Select(static item => item.ColumnName)
            .SequenceEqual(
                parts.Select(static item => item.ColumnName!),
                StringComparer.OrdinalIgnoreCase);

    private static string IndexKind(
        MySqlIndexMetadata index,
        IReadOnlyList<MySqlIndexPartMetadata> parts,
        bool membershipComplete,
        bool exactShape,
        bool foreignKeySupportAmbiguous)
    {
        if (foreignKeySupportAmbiguous)
            return "mysql-fk-support-ambiguous";
        if (parts.Count == 0)
            return "mysql-unresolved-index";
        if (parts.Any(static item => item.Expression is not null))
            return "mysql-functional-index";
        if (parts.Any(static item => item.PrefixLength is not null))
            return "mysql-prefix-index";
        if (parts.Any(static item => item.SortDirection == "D"))
            return "mysql-descending-index";
        if (parts.Any(static item => item.SortDirection is null))
            return "mysql-index-order-unknown";
        if (!index.IsVisible)
            return "mysql-invisible-index";
        if (!string.Equals(
                index.IndexType,
                "BTREE",
                StringComparison.OrdinalIgnoreCase))
        {
            return "mysql-" +
                   index.IndexType.ToLowerInvariant() +
                   "-index";
        }
        if (index.IsUnique)
            return "mysql-standalone-unique-index";
        if (!membershipComplete)
            return "mysql-unresolved-index";
        return exactShape
            ? "standard"
            : "mysql-unsupported-index";
    }

    private static void AddIndexEvidenceFacets(
        ICollection<MigrationCatalogFacet> facets,
        string facetPrefix,
        MySqlIndexMetadata index,
        IReadOnlyList<MySqlIndexPartMetadata> parts)
    {
        facets.Add(Facet(facetPrefix + "Name", index.Name));
        facets.Add(Facet(
            facetPrefix + "Unique",
            Boolean(index.IsUnique)));
        facets.Add(Facet(facetPrefix + "Type", index.IndexType));
        facets.Add(Facet(
            facetPrefix + "Visible",
            Boolean(index.IsVisible)));
        facets.Add(Facet(
            facetPrefix + "PartCount",
            Invariant(parts.Count)));
        facets.Add(Facet(
            facetPrefix + "PartsDigest",
            "sha256:" + MySqlStableDigest.Sequence(
                "csharpdb-mysql-index-parts/v1",
                IndexPartDigestFields(parts))));

        MySqlIndexPartMetadata[] expressionParts = parts
            .Where(static item => item.Expression is not null)
            .ToArray();
        if (expressionParts.Length == 0)
            return;
        facets.Add(Facet(
            facetPrefix + "ExpressionSourceBytes",
            Invariant(expressionParts.Sum(static item =>
                item.ExpressionBytes!.Value))));
        facets.Add(Facet(
            facetPrefix + "ExpressionDigest",
            "sha256:" + MySqlStableDigest.Sequence(
                "csharpdb-mysql-index-expressions/v1",
                expressionParts.Select(static item =>
                    item.Expression))));
        facets.Add(Facet(
            facetPrefix + "ExpressionLength",
            Invariant(expressionParts.Sum(static item =>
                item.Expression!.Length))));
    }

    private static IEnumerable<string?> IndexPartDigestFields(
        IEnumerable<MySqlIndexPartMetadata> parts)
    {
        foreach (MySqlIndexPartMetadata part in parts
                     .OrderBy(static item => item.Sequence))
        {
            yield return Invariant(part.Sequence);
            yield return part.ColumnName;
            yield return part.SortDirection;
            yield return NullableInvariant(part.PrefixLength);
            yield return NullableInvariant(part.ExpressionBytes);
            yield return part.Expression is null
                ? null
                : MySqlStableDigest.Text(
                    "csharpdb-mysql-index-expression/v1",
                    part.Expression);
        }
    }

    private static bool ForeignKeyHasUnsupportedShape(
        MySqlForeignKeyMetadata foreignKey) =>
        !string.Equals(
            foreignKey.MatchOption,
            "NONE",
            StringComparison.OrdinalIgnoreCase) ||
        foreignKey.DeleteRule is "SET NULL" or "SET DEFAULT" ||
        !IsKnownReferentialAction(foreignKey.DeleteRule) ||
        !IsTrivialReferentialAction(foreignKey.UpdateRule);

    private static bool IsKnownReferentialAction(string value) =>
        value is "RESTRICT" or "NO ACTION" or "CASCADE" or
            "SET NULL" or "SET DEFAULT";

    private static bool IsTrivialReferentialAction(string value) =>
        value is "RESTRICT" or "NO ACTION";

    private static string ForeignKeyMatch(string value) =>
        string.Equals(value, "NONE", StringComparison.OrdinalIgnoreCase)
            ? "simple"
            : "mysql-" + value.ToLowerInvariant().Replace(' ', '-');

    private static string ReferentialAction(string value) =>
        value switch
        {
            "RESTRICT" or "NO ACTION" => "restrict",
            "CASCADE" => "cascade",
            "SET NULL" => "set-null",
            "SET DEFAULT" => "set-default",
            _ => "mysql-" + value.ToLowerInvariant().Replace(' ', '-'),
        };

    private static void AddForeignKeyShapeDiagnostic(
        string objectId,
        ICollection<MigrationDiagnostic> diagnostics) =>
        diagnostics.Add(Diagnostic(
            objectId,
            "MIG-MYSQL-FK-SHAPE-UNSUPPORTED-001",
            MigrationDiagnosticSeverity.Error,
            MigrationCompatibilityStatus.Unsupported,
            "The MySQL foreign key has unsupported enforcement semantics.",
            "Only MATCH NONE with immediate nondeferrable enforcement, RESTRICT/NO ACTION or CASCADE delete behavior, and no nontrivial ON UPDATE action is admitted.",
            "Rebuild the foreign key with supported actions or provide a reviewed target design.",
            canOverride: false));

    private static IEnumerable<string?> RelationalSnapshotFields(
        MySqlCatalogSnapshot snapshot)
    {
        foreach (MySqlTableDefinitionMetadata definition in
                 snapshot.TableDefinitions
                     .OrderBy(static item =>
                         item.SchemaName,
                         StringComparer.Ordinal)
                     .ThenBy(static item =>
                         item.TableName,
                         StringComparer.Ordinal))
        {
            yield return "table-definition";
            yield return definition.SchemaName;
            yield return definition.TableName;
            yield return Invariant(definition.DefinitionBytes);
            yield return MySqlStableDigest.Text(
                "csharpdb-mysql-show-create-table/v1",
                definition.Definition);
        }
        foreach (MySqlKeyMetadata key in snapshot.Keys
                     .OrderBy(static item =>
                         item.SchemaName,
                         StringComparer.Ordinal)
                     .ThenBy(static item =>
                         item.TableName,
                         StringComparer.Ordinal)
                     .ThenBy(static item =>
                         item.Name,
                         StringComparer.Ordinal))
        {
            yield return "key";
            yield return key.SchemaName;
            yield return key.TableName;
            yield return key.Name;
            yield return key.ConstraintType;
        }
        foreach (MySqlKeyColumnMetadata column in snapshot.KeyColumns
                     .OrderBy(static item =>
                         item.SchemaName,
                         StringComparer.Ordinal)
                     .ThenBy(static item =>
                         item.TableName,
                         StringComparer.Ordinal)
                     .ThenBy(static item =>
                         item.ConstraintName,
                         StringComparer.Ordinal)
                     .ThenBy(static item =>
                         item.OrdinalPosition))
        {
            yield return "key-column";
            yield return column.SchemaName;
            yield return column.TableName;
            yield return column.ConstraintName;
            yield return Invariant(column.OrdinalPosition);
            yield return column.ColumnName;
        }
        foreach (MySqlForeignKeyMetadata foreignKey in snapshot.ForeignKeys
                     .OrderBy(static item =>
                         item.SchemaName,
                         StringComparer.Ordinal)
                     .ThenBy(static item =>
                         item.TableName,
                         StringComparer.Ordinal)
                     .ThenBy(static item =>
                         item.Name,
                         StringComparer.Ordinal))
        {
            yield return "foreign-key";
            yield return foreignKey.SchemaName;
            yield return foreignKey.TableName;
            yield return foreignKey.Name;
            yield return foreignKey.ReferencedSchemaName;
            yield return foreignKey.ReferencedTableName;
            yield return foreignKey.UniqueConstraintSchemaName;
            yield return foreignKey.UniqueConstraintName;
            yield return foreignKey.MatchOption;
            yield return foreignKey.UpdateRule;
            yield return foreignKey.DeleteRule;
        }
        foreach (MySqlForeignKeyColumnMetadata column in
                 snapshot.ForeignKeyColumns
                     .OrderBy(static item =>
                         item.SchemaName,
                         StringComparer.Ordinal)
                     .ThenBy(static item =>
                         item.TableName,
                         StringComparer.Ordinal)
                     .ThenBy(static item =>
                         item.ConstraintName,
                         StringComparer.Ordinal)
                     .ThenBy(static item =>
                         item.OrdinalPosition))
        {
            yield return "foreign-key-column";
            yield return column.SchemaName;
            yield return column.TableName;
            yield return column.ConstraintName;
            yield return Invariant(column.OrdinalPosition);
            yield return column.ColumnName;
            yield return NullableInvariant(
                column.PositionInUniqueConstraint);
            yield return column.ReferencedSchemaName;
            yield return column.ReferencedTableName;
            yield return column.ReferencedColumnName;
        }
        foreach (MySqlCheckMetadata check in snapshot.Checks
                     .OrderBy(static item =>
                         item.SchemaName,
                         StringComparer.Ordinal)
                     .ThenBy(static item =>
                         item.TableName,
                         StringComparer.Ordinal)
                     .ThenBy(static item =>
                         item.Name,
                         StringComparer.Ordinal))
        {
            yield return "check";
            yield return check.SchemaName;
            yield return check.TableName;
            yield return check.Name;
            yield return Boolean(check.IsEnforced);
            yield return Invariant(check.ClauseBytes);
            yield return MySqlStableDigest.Text(
                "csharpdb-mysql-check-clause/v1",
                check.Clause);
        }
        foreach (MySqlIndexMetadata index in snapshot.Indexes
                     .OrderBy(static item =>
                         item.SchemaName,
                         StringComparer.Ordinal)
                     .ThenBy(static item =>
                         item.TableName,
                         StringComparer.Ordinal)
                     .ThenBy(static item =>
                         item.Name,
                         StringComparer.Ordinal))
        {
            yield return "index";
            yield return index.SchemaName;
            yield return index.TableName;
            yield return index.Name;
            yield return Boolean(index.IsUnique);
            yield return index.IndexType;
            yield return Boolean(index.IsVisible);
        }
        foreach (MySqlIndexPartMetadata part in snapshot.IndexParts
                     .OrderBy(static item =>
                         item.SchemaName,
                         StringComparer.Ordinal)
                     .ThenBy(static item =>
                         item.TableName,
                         StringComparer.Ordinal)
                     .ThenBy(static item =>
                         item.IndexName,
                         StringComparer.Ordinal)
                     .ThenBy(static item =>
                         item.Sequence))
        {
            yield return "index-part";
            yield return part.SchemaName;
            yield return part.TableName;
            yield return part.IndexName;
            yield return Invariant(part.Sequence);
            yield return part.ColumnName;
            yield return part.SortDirection;
            yield return NullableInvariant(part.PrefixLength);
            yield return NullableInvariant(part.ExpressionBytes);
            yield return part.Expression is null
                ? null
                : MySqlStableDigest.Text(
                    "csharpdb-mysql-index-expression/v1",
                    part.Expression);
        }
    }

    private static void AddDefinitionDigestFacets(
        ICollection<MigrationCatalogFacet> facets,
        string facetPrefix,
        string digestDomain,
        long sourceBytes,
        string definition)
    {
        facets.Add(Facet(
            facetPrefix + "SourceBytes",
            Invariant(sourceBytes)));
        facets.Add(Facet(
            facetPrefix + "Digest",
            "sha256:" + MySqlStableDigest.Text(
                digestDomain,
                definition)));
        facets.Add(Facet(
            facetPrefix + "Length",
            Invariant(definition.Length)));
    }

    private static MigrationObjectReference Member(
        string objectId,
        string role,
        int ordinal) =>
        new()
        {
            ObjectId = objectId,
            Role = role,
            Ordinal = ordinal,
        };

    private sealed record BuiltKey(
        string ObjectId,
        string EffectiveKind,
        bool BindingComplete,
        IReadOnlyList<MySqlKeyColumnMetadata> OrderedColumns);
}
