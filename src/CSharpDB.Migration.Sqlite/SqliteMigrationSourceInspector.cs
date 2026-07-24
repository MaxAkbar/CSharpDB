using System.Collections.ObjectModel;
using System.Globalization;
using CSharpDB.Migration;
using Microsoft.Data.Sqlite;

namespace CSharpDB.Migration.Sqlite;

/// <summary>
/// Reads a retained SQLite backup through SQLite's native catalog and PRAGMA
/// surfaces. No source connection exposed by this inspector is writable.
/// </summary>
public sealed class SqliteMigrationSourceInspector : IMigrationSourceInspector
{
    public const string CatalogContract = "csharpdb-sqlite-catalog-v1";

    private readonly SqliteBackupSnapshot snapshot;

    public SqliteMigrationSourceInspector(SqliteBackupSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        this.snapshot = snapshot;
    }

    public MigrationSourceKind SourceKind => MigrationSourceKind.Sqlite;

    public async ValueTask<MigrationCatalog> InspectAsync(
        MigrationInspectionRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();
        if (!string.Equals(
                request.TargetCSharpDbVersion,
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                StringComparison.Ordinal))
        {
            throw new NotSupportedException(
                $"The SQLite adapter is qualified for CSharpDB {CSharpDbCapabilityCatalogLoader.CurrentTargetVersion}.");
        }
        if (request.ProfileSampleSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(request), "Profile sample size must be positive.");

        try
        {
            await using SqliteConnection connection =
                await snapshot.OpenVerifiedReadOnlyConnectionAsync(cancellationToken)
                    .ConfigureAwait(false);
            await using SqliteTransaction transaction =
                connection.BeginTransaction(deferred: true);
            return await BuildCatalogAsync(
                    connection,
                    transaction,
                    snapshot,
                    request,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (SqliteMigrationException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception) when (
            exception is SqliteException or InvalidOperationException or IOException)
        {
            throw new SqliteMigrationException(
                "The retained SQLite schema could not be inspected safely.",
                exception);
        }
    }

    private static async ValueTask<MigrationCatalog> BuildCatalogAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        SqliteBackupSnapshot snapshot,
        MigrationInspectionRequest request,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<SchemaEntry> schema = await ReadSchemaAsync(
                connection,
                transaction,
                cancellationToken)
            .ConfigureAwait(false);
        IReadOnlyList<TableListEntry> tableList = await ReadTableListAsync(
                connection,
                transaction,
                cancellationToken)
            .ConfigureAwait(false);
        string compileOptionsDigest = await ReadCompileOptionsDigestAsync(
                connection,
                transaction,
                cancellationToken)
            .ConfigureAwait(false);

        SchemaEntry? Schema(string type, string name) => schema.FirstOrDefault(
            entry => string.Equals(entry.Type, type, StringComparison.Ordinal) &&
                string.Equals(entry.Name, name, StringComparison.Ordinal));

        var objects = new List<MigrationCatalogObject>();
        var diagnostics = new List<MigrationDiagnostic>();
        var representedSchema = new HashSet<string>(StringComparer.Ordinal);
        var tableStates = new Dictionary<string, TableState>(StringComparer.OrdinalIgnoreCase);
        string namespaceId = SqliteObjectIds.Namespace;
        objects.Add(new MigrationCatalogObject
        {
            ObjectId = namespaceId,
            Kind = MigrationObjectKind.Namespace,
            SourceNamespace = "main",
            SourceName = "main",
            Facets =
            [
                Facet("isDefault", "true"),
                Facet("sqliteCatalogContract", CatalogContract),
                Facet("sqliteCompileOptionsDigest", compileOptionsDigest),
                Facet("sqliteProfileIncluded", Boolean(request.IncludeProfile)),
                Facet(
                    "sqliteProfileSampleSize",
                    request.ProfileSampleSize.ToString(CultureInfo.InvariantCulture)),
            ],
        });

        foreach (TableListEntry entry in tableList)
        {
            cancellationToken.ThrowIfCancellationRequested();
            SchemaEntry? schemaEntry = Schema(
                string.Equals(entry.Type, "view", StringComparison.Ordinal) ? "view" : "table",
                entry.Name);
            if (schemaEntry is not null)
                representedSchema.Add(SchemaKey(schemaEntry.Type, schemaEntry.Name));

            bool ordinary = string.Equals(entry.Type, "table", StringComparison.Ordinal) &&
                !entry.Name.StartsWith("sqlite_", StringComparison.OrdinalIgnoreCase);
            if (!ordinary)
            {
                MigrationObjectKind kind = string.Equals(
                    entry.Type,
                    "view",
                    StringComparison.Ordinal)
                    ? MigrationObjectKind.View
                    : MigrationObjectKind.Other;
                string objectId = SqliteObjectIds.SchemaObject(entry.Type, entry.Name);
                objects.Add(new MigrationCatalogObject
                {
                    ObjectId = objectId,
                    Kind = kind,
                    ParentObjectId = namespaceId,
                    SourceNamespace = "main",
                    SourceName = entry.Name,
                    Facets =
                    [
                        Facet("sqliteCatalogContract", CatalogContract),
                        Facet("sqliteTableType", entry.Type),
                        Facet("sqliteDdlDigest", DdlDigest(schemaEntry?.Sql)),
                        Facet("sqliteWithoutRowId", Boolean(entry.WithoutRowId)),
                        Facet("sqliteStrict", Boolean(entry.Strict)),
                    ],
                });
                string feature = kind == MigrationObjectKind.View
                    ? "view"
                    : entry.Name.StartsWith("sqlite_", StringComparison.OrdinalIgnoreCase)
                        ? "internal-table"
                        : entry.Type;
                diagnostics.Add(Unsupported(
                    objectId,
                    "MIG-SQLITE-OBJECT-UNSUPPORTED-001",
                    $"SQLite {feature} '{entry.Name}' is retained but not migrated by the Tier 1 adapter.",
                    "Exclude this object or migrate it with a reviewed source-specific procedure."));
                continue;
            }

            IReadOnlyList<ColumnInfo> columns = await ReadColumnsAsync(
                    connection,
                    transaction,
                    entry.Name,
                    cancellationToken)
                .ConfigureAwait(false);
            string? rowIdAlias = entry.WithoutRowId ? null : FindRowIdAlias(columns);
            string tableId = SqliteObjectIds.Table(entry.Name);
            var tableFacets = new List<MigrationCatalogFacet>
            {
                Facet("sqliteCatalogContract", CatalogContract),
                Facet("sqliteTableType", "table"),
                Facet("sqliteDdlDigest", DdlDigest(schemaEntry?.Sql)),
                Facet("sqliteWithoutRowId", Boolean(entry.WithoutRowId)),
                Facet("sqliteStrict", Boolean(entry.Strict)),
                Facet("sqliteColumnCount", entry.ColumnCount.ToString(CultureInfo.InvariantCulture)),
            };
            if (rowIdAlias is not null)
                tableFacets.Add(Facet("sqliteRowIdAlias", rowIdAlias));
            if (ContainsSqlKeyword(schemaEntry?.Sql, "AUTOINCREMENT"))
                tableFacets.Add(Facet("sqliteAutoincrement", "true"));
            if (ContainsSqlKeyword(schemaEntry?.Sql, "COLLATE"))
                tableFacets.Add(Facet("sqliteHasDeclaredCollation", "true"));

            var tableObject = new MigrationCatalogObject
            {
                ObjectId = tableId,
                Kind = MigrationObjectKind.Table,
                ParentObjectId = namespaceId,
                SourceNamespace = "main",
                SourceName = entry.Name,
                Facets = tableFacets.AsReadOnly(),
            };
            objects.Add(tableObject);
            var state = new TableState(
                entry,
                schemaEntry,
                tableObject,
                columns,
                rowIdAlias);
            tableStates.Add(entry.Name, state);

            if (entry.WithoutRowId)
            {
                diagnostics.Add(Unsupported(
                    tableId,
                    "MIG-SQLITE-TABLE-WITHOUT-ROWID-001",
                    $"SQLite table '{entry.Name}' uses WITHOUT ROWID and has no Tier 1 replay cursor.",
                    "Exclude the table or use a future key-ordered SQLite adapter."));
            }
            else if (rowIdAlias is null)
            {
                diagnostics.Add(Unsupported(
                    tableId,
                    "MIG-SQLITE-TABLE-ROWID-HIDDEN-001",
                    $"SQLite table '{entry.Name}' shadows every rowid alias.",
                    "Rename one rowid-shadowing column or use a future key-ordered adapter."));
            }
            if (entry.Strict)
            {
                diagnostics.Add(Conditional(
                    tableId,
                    "MIG-SQLITE-TABLE-STRICT-001",
                    $"SQLite STRICT semantics on table '{entry.Name}' are recorded but are not recreated.",
                    "Review target type mappings and explicitly accept this diagnostic."));
            }
            if (ContainsSqlKeyword(schemaEntry?.Sql, "AUTOINCREMENT"))
            {
                diagnostics.Add(Conditional(
                    tableId,
                    "MIG-SQLITE-TABLE-AUTOINCREMENT-001",
                    $"SQLite AUTOINCREMENT allocation semantics on table '{entry.Name}' are not recreated.",
                    "Review identity requirements and explicitly accept this diagnostic."));
            }
            if (ContainsSqlKeyword(schemaEntry?.Sql, "COLLATE"))
            {
                diagnostics.Add(Conditional(
                    tableId,
                    "MIG-SQLITE-TABLE-COLLATION-001",
                    $"One or more declared collations on table '{entry.Name}' require review.",
                    "Verify target comparison semantics before accepting this diagnostic."));
            }

            IReadOnlyList<IndexInfo> indexes = await ReadIndexesAsync(
                    connection,
                    transaction,
                    state,
                    cancellationToken)
                .ConfigureAwait(false);
            state.Indexes.AddRange(indexes);

            ProfileResult? profile = request.IncludeProfile
                ? await ProfileAsync(
                        connection,
                        transaction,
                        state,
                        request.ProfileSampleSize,
                        cancellationToken)
                    .ConfigureAwait(false)
                : null;
            for (int ordinal = 0; ordinal < columns.Count; ordinal++)
            {
                ColumnInfo column = columns[ordinal];
                ColumnProfile? columnProfile = profile?.Columns[ordinal];
                MigrationCatalogObject columnObject = CreateColumn(
                    state,
                    column,
                    columnProfile,
                    profile,
                    request.IncludeProfile);
                state.ColumnObjects.Add(column.Cid, columnObject);
                objects.Add(columnObject);

                if (column.Hidden != 0)
                {
                    diagnostics.Add(Unsupported(
                        columnObject.ObjectId,
                        "MIG-SQLITE-COLUMN-GENERATED-001",
                        $"SQLite generated or hidden column '{column.Name}' is inventoried but not loaded.",
                        "Exclude the column or replace it with an explicit reviewed target expression."));
                }
                if (column.DefaultSql is not null)
                {
                    diagnostics.Add(Conditional(
                        columnObject.ObjectId,
                        "MIG-SQLITE-COLUMN-DEFAULT-001",
                        $"SQLite default semantics for column '{column.Name}' are recorded but not recreated.",
                        "Review target insert behavior and explicitly accept this diagnostic."));
                }
                if (columnProfile is not null &&
                    columnProfile.NonNullStorageClasses.Count > 1)
                {
                    diagnostics.Add(Unsupported(
                        columnObject.ObjectId,
                        "MIG-SQLITE-TYPE-MIXED-001",
                        $"SQLite column '{column.Name}' contains mixed non-null storage classes.",
                        "Normalize the source values or exclude the column."));
                }
            }

            AddKeysAndIndexes(state, objects, diagnostics, representedSchema);

            if (ContainsSqlKeyword(schemaEntry?.Sql, "CHECK"))
            {
                string checkId = SqliteObjectIds.TableFeature(entry.Name, "check");
                objects.Add(new MigrationCatalogObject
                {
                    ObjectId = checkId,
                    Kind = MigrationObjectKind.CheckConstraint,
                    ParentObjectId = tableId,
                    SourceNamespace = "main",
                    SourceName = $"CHECK constraints on {entry.Name}",
                    Facets =
                    [
                        Facet("sqliteDdlDigest", DdlDigest(schemaEntry?.Sql)),
                        Facet("sqliteStructuralDetail", "not-parsed"),
                    ],
                });
                diagnostics.Add(Unsupported(
                    checkId,
                    "MIG-SQLITE-CHECK-UNPARSED-001",
                    $"SQLite CHECK constraints on table '{entry.Name}' are visible but not structurally parsed.",
                    "Recreate reviewed CHECK expressions after migration."));
            }
        }

        foreach (TableState state in tableStates.Values
                     .OrderBy(item => item.Entry.Name, StringComparer.Ordinal))
        {
            IReadOnlyList<ForeignKeyInfo> foreignKeys = await ReadForeignKeysAsync(
                    connection,
                    transaction,
                    state.Entry.Name,
                    cancellationToken)
                .ConfigureAwait(false);
            AddForeignKeys(state, foreignKeys, tableStates, objects, diagnostics);
        }

        foreach (SchemaEntry entry in schema.Where(
                     item => string.Equals(item.Type, "trigger", StringComparison.Ordinal)))
        {
            representedSchema.Add(SchemaKey(entry.Type, entry.Name));
            string objectId = SqliteObjectIds.SchemaObject("trigger", entry.Name);
            string parentId = tableStates.TryGetValue(entry.TableName, out TableState? parent)
                ? parent.TableObject.ObjectId
                : namespaceId;
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = objectId,
                Kind = MigrationObjectKind.Trigger,
                ParentObjectId = parentId,
                SourceNamespace = "main",
                SourceName = entry.Name,
                Facets =
                [
                    Facet("sqliteDdlDigest", DdlDigest(entry.Sql)),
                ],
            });
            diagnostics.Add(Unsupported(
                objectId,
                "MIG-SQLITE-TRIGGER-UNSUPPORTED-001",
                $"SQLite trigger '{entry.Name}' is inventoried but not recreated.",
                "Recreate and validate the trigger explicitly after migration."));
        }

        foreach (SchemaEntry entry in schema)
        {
            string key = SchemaKey(entry.Type, entry.Name);
            if (representedSchema.Contains(key))
                continue;

            string objectId = SqliteObjectIds.SchemaObject(
                "unclassified-" + entry.Type,
                entry.Name);
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = objectId,
                Kind = MigrationObjectKind.Other,
                ParentObjectId = namespaceId,
                SourceNamespace = "main",
                SourceName = entry.Name,
                Facets =
                [
                    Facet("sqliteSchemaType", entry.Type),
                    Facet("sqliteOwningTable", entry.TableName),
                    Facet("sqliteDdlDigest", DdlDigest(entry.Sql)),
                ],
            });
            diagnostics.Add(Unsupported(
                objectId,
                "MIG-SQLITE-SCHEMA-UNCLASSIFIED-001",
                $"SQLite schema object '{entry.Name}' is retained as an unsupported {entry.Type}.",
                "Exclude the object or migrate it with a reviewed source-specific procedure."));
        }

        var catalog = new MigrationCatalog
        {
            TargetCSharpDbVersion = request.TargetCSharpDbVersion,
            Source = snapshot.Source,
            Objects = new ReadOnlyCollection<MigrationCatalogObject>(objects),
            Diagnostics = new ReadOnlyCollection<MigrationDiagnostic>(
                diagnostics
                    .OrderBy(item => item.DiagnosticId, StringComparer.Ordinal)
                    .ToArray()),
        };
        MigrationContractValidator.ValidateCatalog(catalog);
        return catalog;
    }

    private static MigrationCatalogObject CreateColumn(
        TableState table,
        ColumnInfo column,
        ColumnProfile? profile,
        ProfileResult? tableProfile,
        bool includeProfile)
    {
        string affinity = Affinity(column.DeclaredType);
        IReadOnlyList<string> nonNullStorageClasses =
            profile?.NonNullStorageClasses ?? Array.Empty<string>();
        string logicalType = nonNullStorageClasses.Count switch
        {
            1 => LogicalType(nonNullStorageClasses[0]),
            > 1 => "sqliteDynamic",
            _ => LogicalTypeFromAffinity(affinity),
        };
        bool rowIdAlias = table.IsIntegerPrimaryKeyAlias(column);
        bool nullable = column.NotNull == 0 && !rowIdAlias;
        string nativeType = string.IsNullOrEmpty(column.DeclaredType)
            ? "SQLITE_BLOB"
            : column.DeclaredType;
        var facets = new List<MigrationCatalogFacet>
        {
            Facet("logicalType", logicalType),
            Facet("nullable", Boolean(nullable)),
            Facet("sqliteCid", column.Cid.ToString(CultureInfo.InvariantCulture)),
            Facet("sqliteDeclaredType", column.DeclaredType),
            Facet("sqliteAffinity", affinity),
            Facet("sqlitePrimaryKeyOrdinal", column.PrimaryKeyOrdinal.ToString(CultureInfo.InvariantCulture)),
            Facet("sqliteHidden", column.Hidden.ToString(CultureInfo.InvariantCulture)),
            Facet("sqliteRowIdAlias", Boolean(rowIdAlias)),
            Facet("sqliteHasDefault", Boolean(column.DefaultSql is not null)),
            Facet(
                "profileRequiresFullStreamValidation",
                Boolean(!includeProfile || tableProfile?.CoverageKind == MigrationCoverageKind.Sample)),
            Facet("sqliteStorageClasses", string.Join(',', nonNullStorageClasses)),
        };
        if (column.DefaultSql is not null)
            facets.Add(Facet("sqliteDefaultDigest", DdlDigest(column.DefaultSql)));

        if (includeProfile && profile is not null && tableProfile is not null)
        {
            facets.Add(Facet("profileKind", tableProfile.CoverageKind.ToString()));
            facets.Add(Facet(
                "profileValuesExamined",
                tableProfile.ValuesExamined.ToString(CultureInfo.InvariantCulture)));
            facets.Add(Facet(
                "profileTotalValues",
                tableProfile.TotalValues.ToString(CultureInfo.InvariantCulture)));
            facets.Add(Facet(
                "sqliteStorageClassNull",
                profile.NullCount.ToString(CultureInfo.InvariantCulture)));
            facets.Add(Facet(
                "sqliteStorageClassInteger",
                profile.IntegerCount.ToString(CultureInfo.InvariantCulture)));
            facets.Add(Facet(
                "sqliteStorageClassReal",
                profile.RealCount.ToString(CultureInfo.InvariantCulture)));
            facets.Add(Facet(
                "sqliteStorageClassText",
                profile.TextCount.ToString(CultureInfo.InvariantCulture)));
            facets.Add(Facet(
                "sqliteStorageClassBlob",
                profile.BlobCount.ToString(CultureInfo.InvariantCulture)));
        }

        return new MigrationCatalogObject
        {
            ObjectId = SqliteObjectIds.Column(table.Entry.Name, column.Cid),
            Kind = MigrationObjectKind.Column,
            ParentObjectId = table.TableObject.ObjectId,
            SourceNamespace = "main",
            SourceName = column.Name,
            NativeType = nativeType,
            Facets = facets.AsReadOnly(),
        };
    }

    private static void AddKeysAndIndexes(
        TableState table,
        ICollection<MigrationCatalogObject> objects,
        ICollection<MigrationDiagnostic> diagnostics,
        ISet<string> representedSchema)
    {
        ColumnInfo[] primaryColumns = table.Columns
            .Where(column => column.PrimaryKeyOrdinal > 0)
            .OrderBy(column => column.PrimaryKeyOrdinal)
            .ToArray();
        IndexInfo? primaryIndex = table.Indexes.SingleOrDefault(
            index => string.Equals(index.Origin, "pk", StringComparison.Ordinal));
        if (primaryIndex is not null)
            representedSchema.Add(SchemaKey("index", primaryIndex.Name));
        if (primaryColumns.Length > 0)
        {
            string keyId = SqliteObjectIds.PrimaryKey(table.Entry.Name);
            var primary = CreateKey(
                keyId,
                primaryIndex?.Name ?? $"PRIMARY KEY {table.Entry.Name}",
                table,
                "primary",
                primaryColumns);
            table.Keys.Add(new KeyState(primary, primaryColumns));
            objects.Add(primary);
            if (primaryIndex is not null)
                AddIndexFeatureDiagnostics(primary, primaryIndex, diagnostics);
        }

        foreach (IndexInfo index in table.Indexes.OrderBy(item => item.Sequence))
        {
            representedSchema.Add(SchemaKey("index", index.Name));
            if (string.Equals(index.Origin, "pk", StringComparison.Ordinal))
                continue;

            ColumnInfo[] memberColumns = index.Members
                .Where(member => member.Column is not null)
                .Select(member => member.Column!)
                .ToArray();
            if (string.Equals(index.Origin, "u", StringComparison.Ordinal))
            {
                string keyId = SqliteObjectIds.UniqueKey(table.Entry.Name, index.Name);
                MigrationCatalogObject key = CreateKey(
                    keyId,
                    index.Name,
                    table,
                    "unique",
                    memberColumns);
                table.Keys.Add(new KeyState(key, memberColumns));
                objects.Add(key);
                AddIndexFeatureDiagnostics(key, index, diagnostics);
                continue;
            }

            string indexId = SqliteObjectIds.Index(table.Entry.Name, index.Name);
            var facets = new List<MigrationCatalogFacet>
            {
                Facet("kind", "standard"),
                Facet("unique", Boolean(index.Unique)),
                Facet("partial", Boolean(index.Partial)),
                Facet("expression", Boolean(index.HasExpression)),
                Facet("sqliteOrigin", index.Origin),
            };
            if (index.HasDescending)
                facets.Add(Facet("sortDirections", "descending"));
            if (index.Collations.Count > 0)
                facets.Add(Facet("sqliteCollations", string.Join(',', index.Collations)));

            MigrationObjectReference[] members = memberColumns
                .Select((column, ordinal) => new MigrationObjectReference
                {
                    ObjectId = table.ColumnObjects[column.Cid].ObjectId,
                    Role = MigrationObjectReferenceRoles.Column,
                    Ordinal = ordinal,
                })
                .ToArray();
            string[] dependencies = members.Select(member => member.ObjectId).ToArray();
            var indexObject = new MigrationCatalogObject
            {
                ObjectId = indexId,
                Kind = MigrationObjectKind.Index,
                ParentObjectId = table.TableObject.ObjectId,
                SourceNamespace = "main",
                SourceName = index.Name,
                Facets = facets.AsReadOnly(),
                Members = members,
                DependsOn = dependencies,
            };
            objects.Add(indexObject);
            AddIndexFeatureDiagnostics(indexObject, index, diagnostics);
        }
    }

    private static MigrationCatalogObject CreateKey(
        string objectId,
        string sourceName,
        TableState table,
        string kind,
        IReadOnlyList<ColumnInfo> columns)
    {
        MigrationObjectReference[] members = columns
            .Select((column, ordinal) => new MigrationObjectReference
            {
                ObjectId = table.ColumnObjects[column.Cid].ObjectId,
                Role = MigrationObjectReferenceRoles.Column,
                Ordinal = ordinal,
            })
            .ToArray();
        return new MigrationCatalogObject
        {
            ObjectId = objectId,
            Kind = MigrationObjectKind.Key,
            ParentObjectId = table.TableObject.ObjectId,
            SourceNamespace = "main",
            SourceName = sourceName,
            Facets =
            [
                Facet("kind", kind),
            ],
            Members = members,
            DependsOn = members.Select(member => member.ObjectId).ToArray(),
        };
    }

    private static void AddIndexFeatureDiagnostics(
        MigrationCatalogObject catalogObject,
        IndexInfo index,
        ICollection<MigrationDiagnostic> diagnostics)
    {
        if (index.Partial)
        {
            diagnostics.Add(Unsupported(
                catalogObject.ObjectId,
                "MIG-SQLITE-INDEX-PARTIAL-001",
                $"SQLite partial index '{index.Name}' is inventoried but not recreated.",
                "Recreate a reviewed target index or exclude this object."));
        }
        if (index.HasExpression || index.HasRowIdMember)
        {
            diagnostics.Add(Unsupported(
                catalogObject.ObjectId,
                "MIG-SQLITE-INDEX-EXPRESSION-001",
                $"SQLite index '{index.Name}' is not a basic column-only index.",
                "Replace it with reviewed target indexes or exclude this object."));
        }
        if (index.HasDescending)
        {
            diagnostics.Add(Unsupported(
                catalogObject.ObjectId,
                "MIG-SQLITE-INDEX-DIRECTION-001",
                $"SQLite descending order in index '{index.Name}' is not recreated.",
                "Review query requirements and explicitly replace or exclude this index."));
        }
        if (index.Collations.Any(collation =>
                !string.Equals(collation, "BINARY", StringComparison.OrdinalIgnoreCase)))
        {
            diagnostics.Add(Unsupported(
                catalogObject.ObjectId,
                "MIG-SQLITE-INDEX-COLLATION-001",
                $"SQLite index '{index.Name}' uses a non-binary collation.",
                "Verify target comparison semantics and recreate the index explicitly."));
        }
    }

    private static void AddForeignKeys(
        TableState sourceTable,
        IReadOnlyList<ForeignKeyInfo> foreignKeys,
        IReadOnlyDictionary<string, TableState> tables,
        ICollection<MigrationCatalogObject> objects,
        ICollection<MigrationDiagnostic> diagnostics)
    {
        foreach (IGrouping<int, ForeignKeyInfo> group in foreignKeys
                     .GroupBy(item => item.Id)
                     .OrderBy(item => item.Key))
        {
            ForeignKeyInfo[] rows = group.OrderBy(item => item.Sequence).ToArray();
            string sourceName = $"FK_{sourceTable.Entry.Name}_{group.Key.ToString(CultureInfo.InvariantCulture)}";
            string objectId = SqliteObjectIds.ForeignKey(sourceTable.Entry.Name, group.Key);
            ColumnInfo[] sourceColumns = rows
                .Select(row => sourceTable.ColumnByName(row.FromColumn))
                .Where(column => column is not null)
                .Cast<ColumnInfo>()
                .ToArray();
            TableState? targetTable = tables.GetValueOrDefault(rows[0].ReferencedTable);
            KeyState? targetKey = targetTable is null
                ? null
                : ResolveReferencedKey(targetTable, rows);

            if (sourceColumns.Length != rows.Length || targetKey is null)
            {
                objects.Add(new MigrationCatalogObject
                {
                    ObjectId = objectId,
                    Kind = MigrationObjectKind.Other,
                    ParentObjectId = sourceTable.TableObject.ObjectId,
                    SourceNamespace = "main",
                    SourceName = sourceName,
                    Facets =
                    [
                        Facet("sqliteSchemaType", "foreignKey"),
                        Facet("sqliteReferencedTable", rows[0].ReferencedTable),
                        Facet("sqliteStructuralDetail", "unresolved"),
                    ],
                });
                diagnostics.Add(Unsupported(
                    objectId,
                    "MIG-SQLITE-FK-UNRESOLVED-001",
                    $"SQLite foreign key '{sourceName}' could not be bound to source columns and a referenced key.",
                    "Repair the source schema or recreate the relationship explicitly."));
                continue;
            }

            var members = new List<MigrationObjectReference>(sourceColumns.Length + 1);
            members.AddRange(sourceColumns.Select((column, ordinal) =>
                new MigrationObjectReference
                {
                    ObjectId = sourceTable.ColumnObjects[column.Cid].ObjectId,
                    Role = MigrationObjectReferenceRoles.SourceColumn,
                    Ordinal = ordinal,
                }));
            members.Add(new MigrationObjectReference
            {
                ObjectId = targetKey.Object.ObjectId,
                Role = MigrationObjectReferenceRoles.ReferencedKey,
                Ordinal = 0,
            });

            string onDelete = NormalizeForeignKeyAction(rows[0].OnDelete);
            string onUpdate = NormalizeForeignKeyAction(rows[0].OnUpdate);
            bool deferrabilityPresent = ContainsSqlKeyword(sourceTable.Schema?.Sql, "DEFERRABLE");
            var facets = new List<MigrationCatalogFacet>
            {
                Facet("timing", "immediate"),
                Facet("match", NormalizeMatch(rows[0].Match)),
                Facet("onDelete", onDelete),
                Facet("deferrable", Boolean(deferrabilityPresent)),
                Facet("deferred", Boolean(deferrabilityPresent)),
            };
            if (!string.Equals(onUpdate, "restrict", StringComparison.Ordinal))
                facets.Add(Facet("onUpdate", onUpdate));

            var foreignKey = new MigrationCatalogObject
            {
                ObjectId = objectId,
                Kind = MigrationObjectKind.ForeignKey,
                ParentObjectId = sourceTable.TableObject.ObjectId,
                SourceNamespace = "main",
                SourceName = sourceName,
                Facets = facets.AsReadOnly(),
                Members = members.AsReadOnly(),
                DependsOn = members.Select(member => member.ObjectId).ToArray(),
            };
            objects.Add(foreignKey);

            if (deferrabilityPresent)
            {
                diagnostics.Add(Unsupported(
                    objectId,
                    "MIG-SQLITE-FK-DEFERRABLE-001",
                    $"SQLite foreign key '{sourceName}' may use deferrable timing that is not recreated.",
                    "Review the source DDL and recreate the relationship explicitly."));
            }
            if (onDelete is not ("restrict" or "cascade") ||
                !string.Equals(onUpdate, "restrict", StringComparison.Ordinal) ||
                !string.Equals(NormalizeMatch(rows[0].Match), "simple", StringComparison.Ordinal))
            {
                diagnostics.Add(Unsupported(
                    objectId,
                    "MIG-SQLITE-FK-ACTION-001",
                    $"SQLite foreign key '{sourceName}' uses actions outside the Tier 1 target subset.",
                    "Review and recreate the relationship explicitly."));
            }
        }
    }

    private static KeyState? ResolveReferencedKey(
        TableState table,
        IReadOnlyList<ForeignKeyInfo> rows)
    {
        if (rows.All(row => string.IsNullOrEmpty(row.ToColumn)))
        {
            return table.Keys.SingleOrDefault(key =>
                string.Equals(FacetValue(key.Object, "kind"), "primary", StringComparison.Ordinal));
        }
        if (rows.Any(row => string.IsNullOrEmpty(row.ToColumn)))
            return null;

        return table.Keys.FirstOrDefault(key =>
            key.Columns.Count == rows.Count &&
            key.Columns.Select(column => column.Name).SequenceEqual(
                rows.Select(row => row.ToColumn!),
                StringComparer.OrdinalIgnoreCase));
    }

    private static async ValueTask<IReadOnlyList<SchemaEntry>> ReadSchemaAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using SqliteCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT rowid, type, name, tbl_name, sql
            FROM main.sqlite_schema
            ORDER BY rowid;
            """;
        var entries = new List<SchemaEntry>();
        await using SqliteDataReader reader =
            await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            entries.Add(new SchemaEntry(
                reader.GetInt64(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4)));
        }
        return entries.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<TableListEntry>> ReadTableListAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using SqliteCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT name, type, ncol, wr, strict
            FROM pragma_table_list
            WHERE schema = 'main'
            ORDER BY name COLLATE BINARY;
            """;
        var entries = new List<TableListEntry>();
        await using SqliteDataReader reader =
            await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            entries.Add(new TableListEntry(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetInt32(2),
                reader.GetInt32(3) != 0,
                reader.GetInt32(4) != 0));
        }
        return entries.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<ColumnInfo>> ReadColumnsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string tableName,
        CancellationToken cancellationToken)
    {
        await using SqliteCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT cid, name, type, "notnull", dflt_value, pk, hidden
            FROM pragma_table_xinfo($tableName)
            ORDER BY cid;
            """;
        command.Parameters.AddWithValue("$tableName", tableName);
        var columns = new List<ColumnInfo>();
        await using SqliteDataReader reader =
            await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            columns.Add(new ColumnInfo(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                reader.GetInt32(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.GetInt32(5),
                reader.GetInt32(6)));
        }
        return columns.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<IndexInfo>> ReadIndexesAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        TableState table,
        CancellationToken cancellationToken)
    {
        await using SqliteCommand list = connection.CreateCommand();
        list.Transaction = transaction;
        list.CommandText =
            """
            SELECT seq, name, "unique", origin, partial
            FROM pragma_index_list($tableName)
            ORDER BY seq;
            """;
        list.Parameters.AddWithValue("$tableName", table.Entry.Name);
        var headers = new List<IndexHeader>();
        await using (SqliteDataReader reader =
                     await list.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                headers.Add(new IndexHeader(
                    reader.GetInt32(0),
                    reader.GetString(1),
                    reader.GetInt32(2) != 0,
                    reader.GetString(3),
                    reader.GetInt32(4) != 0));
            }
        }

        var indexes = new List<IndexInfo>(headers.Count);
        foreach (IndexHeader header in headers)
        {
            await using SqliteCommand detail = connection.CreateCommand();
            detail.Transaction = transaction;
            detail.CommandText =
                """
                SELECT seqno, cid, name, "desc", coll, "key"
                FROM pragma_index_xinfo($indexName)
                ORDER BY seqno;
                """;
            detail.Parameters.AddWithValue("$indexName", header.Name);
            var members = new List<IndexMember>();
            await using SqliteDataReader reader =
                await detail.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (reader.GetInt32(5) == 0)
                    continue;
                int cid = reader.GetInt32(1);
                members.Add(new IndexMember(
                    reader.GetInt32(0),
                    cid,
                    reader.IsDBNull(2) ? null : reader.GetString(2),
                    reader.GetInt32(3) != 0,
                    reader.IsDBNull(4) ? null : reader.GetString(4),
                    cid >= 0 ? table.Columns.SingleOrDefault(column => column.Cid == cid) : null));
            }

            indexes.Add(new IndexInfo(
                header.Sequence,
                header.Name,
                header.Unique,
                header.Origin,
                header.Partial,
                members.AsReadOnly()));
        }
        return indexes.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<ForeignKeyInfo>> ReadForeignKeysAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string tableName,
        CancellationToken cancellationToken)
    {
        await using SqliteCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT id, seq, "table", "from", "to", on_update, on_delete, "match"
            FROM pragma_foreign_key_list($tableName)
            ORDER BY id, seq;
            """;
        command.Parameters.AddWithValue("$tableName", tableName);
        var rows = new List<ForeignKeyInfo>();
        await using SqliteDataReader reader =
            await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            rows.Add(new ForeignKeyInfo(
                reader.GetInt32(0),
                reader.GetInt32(1),
                reader.GetString(2),
                reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.GetString(5),
                reader.GetString(6),
                reader.GetString(7)));
        }
        return rows.AsReadOnly();
    }

    private static async ValueTask<ProfileResult> ProfileAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        TableState table,
        int sampleSize,
        CancellationToken cancellationToken)
    {
        await using SqliteCommand count = connection.CreateCommand();
        count.Transaction = transaction;
        count.CommandText = $"SELECT COUNT(*) FROM {QuoteIdentifier(table.Entry.Name)};";
        long total = Convert.ToInt64(
            await count.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false),
            CultureInfo.InvariantCulture);

        await using SqliteCommand sample = connection.CreateCommand();
        sample.Transaction = transaction;
        sample.CommandText = BuildProfileSql(table);
        sample.Parameters.AddWithValue("$sampleSize", sampleSize);
        var columns = table.Columns.Select(_ => new ColumnProfile()).ToArray();
        long examined = 0;
        await using SqliteDataReader reader =
            await sample.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            examined++;
            for (int index = 0; index < columns.Length; index++)
                columns[index].Add(reader.GetString(index));
        }

        MigrationCoverageKind coverage = total <= sampleSize
            ? MigrationCoverageKind.Full
            : MigrationCoverageKind.Sample;
        return new ProfileResult(
            coverage,
            examined,
            total,
            new ReadOnlyCollection<ColumnProfile>(columns));
    }

    private static string BuildProfileSql(TableState table)
    {
        string projections = string.Join(
            ", ",
            table.Columns.Select(column =>
                $"typeof({QuoteIdentifier(column.Name)})"));
        string order = table.RowIdAlias is null
            ? string.Empty
            : $" ORDER BY {QuoteIdentifier(table.RowIdAlias)}";
        return $"SELECT {projections} FROM {QuoteIdentifier(table.Entry.Name)}{order} LIMIT $sampleSize;";
    }

    private static async ValueTask<string> ReadCompileOptionsDigestAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using SqliteCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT compile_options FROM pragma_compile_options ORDER BY compile_options;";
        var options = new List<string>();
        await using SqliteDataReader reader =
            await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            options.Add(reader.GetString(0));
        return "sha256:" + SqliteStableDigest.Text(
            "csharpdb-sqlite-compile-options-v1",
            options.Cast<string?>().ToArray());
    }

    private static string? FindRowIdAlias(IReadOnlyList<ColumnInfo> columns)
    {
        var names = columns.Select(column => column.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (string candidate in new[] { "rowid", "_rowid_", "oid" })
        {
            if (!names.Contains(candidate))
                return candidate;
        }
        return null;
    }

    private static string Affinity(string declaredType)
    {
        string upper = declaredType.ToUpperInvariant();
        if (upper.Contains("INT", StringComparison.Ordinal))
            return "integer";
        if (upper.Contains("CHAR", StringComparison.Ordinal) ||
            upper.Contains("CLOB", StringComparison.Ordinal) ||
            upper.Contains("TEXT", StringComparison.Ordinal))
        {
            return "text";
        }
        if (upper.Length == 0 || upper.Contains("BLOB", StringComparison.Ordinal))
            return "blob";
        if (upper.Contains("REAL", StringComparison.Ordinal) ||
            upper.Contains("FLOA", StringComparison.Ordinal) ||
            upper.Contains("DOUB", StringComparison.Ordinal))
        {
            return "real";
        }
        return "numeric";
    }

    private static string LogicalTypeFromAffinity(string affinity) => affinity switch
    {
        "integer" => "signedInteger",
        "real" => "floatingPoint",
        "text" => "text",
        "blob" => "binary",
        _ => "sqliteDynamic",
    };

    private static string LogicalType(string storageClass) => storageClass switch
    {
        "integer" => "signedInteger",
        "real" => "floatingPoint",
        "text" => "text",
        "blob" => "binary",
        _ => "sqliteDynamic",
    };

    private static string NormalizeForeignKeyAction(string action) =>
        action.ToUpperInvariant() switch
        {
            "NO ACTION" or "RESTRICT" => "restrict",
            "CASCADE" => "cascade",
            "SET NULL" => "set-null",
            "SET DEFAULT" => "set-default",
            _ => action.ToLowerInvariant().Replace(' ', '-'),
        };

    private static string NormalizeMatch(string match) =>
        match.ToUpperInvariant() is "NONE" or "SIMPLE"
            ? "simple"
            : match.ToLowerInvariant();

    private static bool ContainsSqlKeyword(string? sql, string keyword)
    {
        if (string.IsNullOrEmpty(sql))
            return false;
        int index = 0;
        while (index < sql.Length)
        {
            char current = sql[index];
            if (current == '-' && index + 1 < sql.Length && sql[index + 1] == '-')
            {
                index += 2;
                while (index < sql.Length && sql[index] is not ('\r' or '\n'))
                    index++;
                continue;
            }
            if (current == '/' && index + 1 < sql.Length && sql[index + 1] == '*')
            {
                index += 2;
                while (index + 1 < sql.Length &&
                       !(sql[index] == '*' && sql[index + 1] == '/'))
                {
                    index++;
                }
                index = Math.Min(sql.Length, index + 2);
                continue;
            }
            if (current is '\'' or '"' or '`' or '[')
            {
                char close = current == '[' ? ']' : current;
                index++;
                while (index < sql.Length)
                {
                    if (sql[index] == close)
                    {
                        if (close != ']' &&
                            index + 1 < sql.Length &&
                            sql[index + 1] == close)
                        {
                            index += 2;
                            continue;
                        }
                        index++;
                        break;
                    }
                    index++;
                }
                continue;
            }
            if (char.IsLetter(current) || current == '_')
            {
                int start = index++;
                while (index < sql.Length &&
                       (char.IsLetterOrDigit(sql[index]) || sql[index] == '_'))
                {
                    index++;
                }
                if (sql.AsSpan(start, index - start).Equals(
                        keyword.AsSpan(),
                        StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
                continue;
            }
            index++;
        }
        return false;
    }

    private static MigrationDiagnostic Unsupported(
        string objectId,
        string ruleId,
        string summary,
        string remediation) => Diagnostic(
        objectId,
        ruleId,
        MigrationDiagnosticSeverity.Error,
        MigrationCompatibilityStatus.Unsupported,
        summary,
        remediation,
        canOverride: false);

    private static MigrationDiagnostic Conditional(
        string objectId,
        string ruleId,
        string summary,
        string remediation) => Diagnostic(
        objectId,
        ruleId,
        MigrationDiagnosticSeverity.Warning,
        MigrationCompatibilityStatus.Conditional,
        summary,
        remediation,
        canOverride: true);

    private static MigrationDiagnostic Diagnostic(
        string objectId,
        string ruleId,
        MigrationDiagnosticSeverity severity,
        MigrationCompatibilityStatus status,
        string summary,
        string remediation,
        bool canOverride)
    {
        string diagnosticHash = SqliteStableDigest.Text(
            "csharpdb-sqlite-diagnostic-v1",
            ruleId,
            objectId)[..16];
        return new MigrationDiagnostic
        {
            DiagnosticId = $"diag:{ruleId.ToLowerInvariant()}:{diagnosticHash}",
            RuleId = ruleId,
            Severity = severity,
            Status = status,
            Evidence = MigrationEvidenceLevel.Parsed,
            Summary = summary,
            Explanation =
                "The SQLite fact was read from sqlite_schema or a native SQLite PRAGMA and remains explicit in this catalog.",
            ObjectId = objectId,
            Remediation = remediation,
            CanOverride = canOverride,
        };
    }

    private static MigrationCatalogFacet Facet(string name, string? value) => new()
    {
        Name = name,
        Value = value,
    };

    private static string? FacetValue(MigrationCatalogObject item, string name) =>
        item.Facets.FirstOrDefault(
            facet => string.Equals(facet.Name, name, StringComparison.Ordinal))?.Value;

    private static string DdlDigest(string? sql) =>
        "sha256:" + SqliteStableDigest.Text("csharpdb-sqlite-ddl-v1", sql);

    private static string Boolean(bool value) => value ? "true" : "false";

    private static string SchemaKey(string type, string name) => type + "\0" + name;

    private static string QuoteIdentifier(string identifier) =>
        "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private sealed record SchemaEntry(
        long RowId,
        string Type,
        string Name,
        string TableName,
        string? Sql);

    private sealed record TableListEntry(
        string Name,
        string Type,
        int ColumnCount,
        bool WithoutRowId,
        bool Strict);

    private sealed record ColumnInfo(
        int Cid,
        string Name,
        string DeclaredType,
        int NotNull,
        string? DefaultSql,
        int PrimaryKeyOrdinal,
        int Hidden);

    private sealed record IndexHeader(
        int Sequence,
        string Name,
        bool Unique,
        string Origin,
        bool Partial);

    private sealed record IndexMember(
        int Sequence,
        int Cid,
        string? Name,
        bool Descending,
        string? Collation,
        ColumnInfo? Column);

    private sealed record IndexInfo(
        int Sequence,
        string Name,
        bool Unique,
        string Origin,
        bool Partial,
        IReadOnlyList<IndexMember> Members)
    {
        public bool HasExpression => Members.Any(member => member.Cid == -2);

        public bool HasRowIdMember => Members.Any(member => member.Cid == -1);

        public bool HasDescending => Members.Any(member => member.Descending);

        public IReadOnlyList<string> Collations => Members
            .Where(member => member.Collation is not null)
            .Select(member => member.Collation!)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(value => value, StringComparer.Ordinal)
            .ToArray();
    }

    private sealed record ForeignKeyInfo(
        int Id,
        int Sequence,
        string ReferencedTable,
        string FromColumn,
        string? ToColumn,
        string OnUpdate,
        string OnDelete,
        string Match);

    private sealed class ColumnProfile
    {
        public long NullCount { get; private set; }

        public long IntegerCount { get; private set; }

        public long RealCount { get; private set; }

        public long TextCount { get; private set; }

        public long BlobCount { get; private set; }

        public IReadOnlyList<string> NonNullStorageClasses
        {
            get
            {
                var values = new List<string>(4);
                if (BlobCount > 0)
                    values.Add("blob");
                if (IntegerCount > 0)
                    values.Add("integer");
                if (RealCount > 0)
                    values.Add("real");
                if (TextCount > 0)
                    values.Add("text");
                return values.AsReadOnly();
            }
        }

        public void Add(string storageClass)
        {
            switch (storageClass)
            {
                case "null":
                    NullCount++;
                    break;
                case "integer":
                    IntegerCount++;
                    break;
                case "real":
                    RealCount++;
                    break;
                case "text":
                    TextCount++;
                    break;
                case "blob":
                    BlobCount++;
                    break;
                default:
                    throw new InvalidDataException(
                        "SQLite returned an unknown storage class during profiling.");
            }
        }
    }

    private sealed record ProfileResult(
        MigrationCoverageKind CoverageKind,
        long ValuesExamined,
        long TotalValues,
        IReadOnlyList<ColumnProfile> Columns);

    private sealed record KeyState(
        MigrationCatalogObject Object,
        IReadOnlyList<ColumnInfo> Columns);

    private sealed class TableState
    {
        public TableState(
            TableListEntry entry,
            SchemaEntry? schema,
            MigrationCatalogObject tableObject,
            IReadOnlyList<ColumnInfo> columns,
            string? rowIdAlias)
        {
            Entry = entry;
            Schema = schema;
            TableObject = tableObject;
            Columns = columns;
            RowIdAlias = rowIdAlias;
        }

        public TableListEntry Entry { get; }

        public SchemaEntry? Schema { get; }

        public MigrationCatalogObject TableObject { get; }

        public IReadOnlyList<ColumnInfo> Columns { get; }

        public string? RowIdAlias { get; }

        public Dictionary<int, MigrationCatalogObject> ColumnObjects { get; } = [];

        public List<IndexInfo> Indexes { get; } = [];

        public List<KeyState> Keys { get; } = [];

        public ColumnInfo? ColumnByName(string name) => Columns.FirstOrDefault(
            column => string.Equals(column.Name, name, StringComparison.OrdinalIgnoreCase));

        public bool IsIntegerPrimaryKeyAlias(ColumnInfo column) =>
            !Entry.WithoutRowId &&
            column.PrimaryKeyOrdinal == 1 &&
            Columns.Count(candidate => candidate.PrimaryKeyOrdinal > 0) == 1 &&
            string.Equals(column.DeclaredType, "INTEGER", StringComparison.OrdinalIgnoreCase) &&
            Indexes.All(index => !string.Equals(index.Origin, "pk", StringComparison.Ordinal));
    }

    private static class SqliteObjectIds
    {
        public const string Namespace = "sqlite:namespace:main";

        public static string Table(string name) => "sqlite:table:" + Hash("table", name);

        public static string Column(string tableName, int cid) =>
            "sqlite:column:" + Hash("table", tableName) + ":" +
            cid.ToString("D10", CultureInfo.InvariantCulture);

        public static string PrimaryKey(string tableName) =>
            "sqlite:key:" + Hash("table", tableName) + ":primary";

        public static string UniqueKey(string tableName, string indexName) =>
            "sqlite:key:" + Hash("table", tableName) + ":unique:" + Hash("index", indexName);

        public static string Index(string tableName, string indexName) =>
            "sqlite:index:" + Hash("table", tableName) + ":" + Hash("index", indexName);

        public static string ForeignKey(string tableName, int id) =>
            "sqlite:foreign-key:" + Hash("table", tableName) + ":" +
            id.ToString("D10", CultureInfo.InvariantCulture);

        public static string TableFeature(string tableName, string feature) =>
            "sqlite:table-feature:" + Hash("table", tableName) + ":" + Hash("feature", feature);

        public static string SchemaObject(string type, string name) =>
            "sqlite:schema:" + Hash("type", type) + ":" + Hash("name", name);

        private static string Hash(string domain, string value) =>
            SqliteStableDigest.Text(
                "csharpdb-sqlite-object-id-v1/" + domain,
                value)[..32];
    }
}
