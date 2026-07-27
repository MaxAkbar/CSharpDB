using CSharpDB.Migration;
using CSharpDB.Migration.Sqlite;
using Microsoft.Data.Sqlite;

namespace CSharpDB.Migration.Sqlite.Tests;

public sealed class SqliteMigrationSourceInspectorTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task InspectInventoriesOrdinarySchemaAndProducesDeterministicCatalog()
    {
        using var temporary = new SqliteTestDirectory();
        string sourcePath = temporary.PathFor("ordinary-source.sqlite");
        string snapshotPath = temporary.PathFor("ordinary-snapshot.sqlite");
        await SqliteTestDatabase.CreateAsync(
            sourcePath,
            """
            CREATE TABLE parents (
                id INTEGER PRIMARY KEY,
                code TEXT NOT NULL UNIQUE
            );
            CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL,
                label TEXT NOT NULL DEFAULT 'unlabelled',
                amount REAL,
                payload BLOB,
                CONSTRAINT fk_children_parent
                    FOREIGN KEY(parent_id) REFERENCES parents(id)
                    ON UPDATE CASCADE ON DELETE RESTRICT
            );
            CREATE INDEX ix_children_parent
                ON children(parent_id DESC);
            CREATE UNIQUE INDEX ux_children_label
                ON children(label);
            INSERT INTO parents(id, code) VALUES (1, 'P-1');
            INSERT INTO children(id, parent_id, label, amount, payload)
                VALUES (10, 1, 'child', 12.5, X'0001FEFF');
            """,
            Ct);

        SqliteBackupSnapshot? snapshot = null;
        try
        {
            snapshot = await SqliteBackupSnapshot.CreateAsync(
                sourcePath,
                snapshotPath,
                Ct);
            var inspector = new SqliteMigrationSourceInspector(snapshot);

            MigrationCatalog first = await InspectAsync(inspector, includeProfile: false);
            MigrationCatalog second = await InspectAsync(inspector, includeProfile: false);

            Assert.Equal(
                MigrationArtifactSerializer.ComputeCatalogDigest(first),
                MigrationArtifactSerializer.ComputeCatalogDigest(second));
            Assert.Equal(
                MigrationArtifactSerializer.SerializeCatalog(first),
                MigrationArtifactSerializer.SerializeCatalog(second));

            MigrationCatalogObject parents = Find(first, MigrationObjectKind.Table, "parents");
            MigrationCatalogObject children = Find(first, MigrationObjectKind.Table, "children");
            IReadOnlyList<MigrationCatalogObject> parentColumns = ChildrenOf(
                first,
                parents,
                MigrationObjectKind.Column);
            IReadOnlyList<MigrationCatalogObject> childColumns = ChildrenOf(
                first,
                children,
                MigrationObjectKind.Column);
            Assert.Equal(
                ["code", "id"],
                parentColumns.Select(static column => column.SourceName).Order(StringComparer.Ordinal));
            Assert.Equal(
                ["amount", "id", "label", "parent_id", "payload"],
                childColumns.Select(static column => column.SourceName).Order(StringComparer.Ordinal));
            Assert.Equal(
                "INTEGER",
                parentColumns.Single(static column => column.SourceName == "id").NativeType);
            Assert.Equal(
                "BLOB",
                childColumns.Single(static column => column.SourceName == "payload").NativeType);

            MigrationCatalogObject parentId =
                parentColumns.Single(static column => column.SourceName == "id");
            MigrationCatalogObject parentCode =
                parentColumns.Single(static column => column.SourceName == "code");
            MigrationCatalogObject childParentId =
                childColumns.Single(static column => column.SourceName == "parent_id");

            IReadOnlyList<MigrationCatalogObject> keys = first.Objects
                .Where(candidate =>
                    candidate.Kind == MigrationObjectKind.Key &&
                    candidate.ParentObjectId == parents.ObjectId)
                .ToArray();
            Assert.Contains(keys, key => ReferencesColumn(key, parentId.ObjectId));
            Assert.Contains(keys, key => ReferencesColumn(key, parentCode.ObjectId));

            MigrationCatalogObject foreignKey = Assert.Single(
                first.Objects,
                candidate =>
                    candidate.Kind == MigrationObjectKind.ForeignKey &&
                    candidate.ParentObjectId == children.ObjectId);
            Assert.True(
                ReferencesColumn(foreignKey, childParentId.ObjectId) ||
                foreignKey.DependsOn.Contains(childParentId.ObjectId, StringComparer.Ordinal));

            IReadOnlyList<MigrationCatalogObject> indexes = first.Objects
                .Where(candidate =>
                    candidate.Kind == MigrationObjectKind.Index &&
                    candidate.ParentObjectId == children.ObjectId)
                .ToArray();
            Assert.Contains(indexes, index => index.SourceName == "ix_children_parent");
            Assert.Contains(indexes, index => index.SourceName == "ux_children_label");
        }
        finally
        {
            await SqliteTestDatabase.DisposeIfSupportedAsync(snapshot);
        }
    }

    [Fact]
    public async Task SampledProfileRecordsStorageClassesAndDiagnosesMixedColumn()
    {
        using var temporary = new SqliteTestDirectory();
        string sourcePath = temporary.PathFor("mixed-source.sqlite");
        string snapshotPath = temporary.PathFor("mixed-snapshot.sqlite");
        await SqliteTestDatabase.CreateAsync(
            sourcePath,
            """
            CREATE TABLE mixed_values (
                id INTEGER PRIMARY KEY,
                mixed NUMERIC,
                stable_text TEXT
            );
            INSERT INTO mixed_values(id, mixed, stable_text) VALUES
                (1, 42, 'one'),
                (2, 'forty-two', 'two'),
                (3, X'002AFF', 'three'),
                (4, 44, 'four'),
                (5, 45, 'five'),
                (6, 46, 'six');
            """,
            Ct);

        SqliteBackupSnapshot? snapshot = null;
        try
        {
            snapshot = await SqliteBackupSnapshot.CreateAsync(
                sourcePath,
                snapshotPath,
                Ct);
            MigrationCatalog catalog = await InspectAsync(
                new SqliteMigrationSourceInspector(snapshot),
                includeProfile: true,
                profileSampleSize: 3);

            MigrationCatalogObject table =
                Find(catalog, MigrationObjectKind.Table, "mixed_values");
            MigrationCatalogObject mixed = ChildrenOf(
                    catalog,
                    table,
                    MigrationObjectKind.Column)
                .Single(static column => column.SourceName == "mixed");

            AssertFacet(mixed, "profileKind", MigrationCoverageKind.Sample.ToString());
            AssertFacet(mixed, "profileValuesExamined", "3");
            AssertFacet(mixed, "profileTotalValues", "6");
            AssertFacet(mixed, "sqliteStorageClassInteger", "1");
            AssertFacet(mixed, "sqliteStorageClassText", "1");
            AssertFacet(mixed, "sqliteStorageClassBlob", "1");

            MigrationDiagnostic mixedDiagnostic = Assert.Single(
                catalog.Diagnostics,
                diagnostic =>
                    diagnostic.ObjectId == mixed.ObjectId &&
                    (
                        diagnostic.Summary.Contains("mixed", StringComparison.OrdinalIgnoreCase) ||
                        diagnostic.Explanation.Contains("mixed", StringComparison.OrdinalIgnoreCase) ||
                        diagnostic.RuleId.Contains("TYPE-MIXED", StringComparison.OrdinalIgnoreCase)
                    ));
            Assert.NotEqual(MigrationCompatibilityStatus.Compatible, mixedDiagnostic.Status);
            Assert.NotEqual(MigrationDiagnosticSeverity.Information, mixedDiagnostic.Severity);
        }
        finally
        {
            await SqliteTestDatabase.DisposeIfSupportedAsync(snapshot);
        }
    }

    [Fact]
    public async Task InspectRetainsUnsupportedAndTierTwoObjectsWithDiagnostics()
    {
        using var temporary = new SqliteTestDirectory();
        string sourcePath = temporary.PathFor("unsupported-source.sqlite");
        string snapshotPath = temporary.PathFor("unsupported-snapshot.sqlite");
        await SqliteTestDatabase.CreateAsync(
            sourcePath,
            """
            CREATE TABLE advanced (
                id INTEGER PRIMARY KEY,
                left_value INTEGER NOT NULL,
                right_value INTEGER NOT NULL,
                total INTEGER GENERATED ALWAYS AS (left_value + right_value) STORED
            );
            CREATE VIEW advanced_view AS
                SELECT id, total FROM advanced;
            CREATE TRIGGER advanced_trigger
                AFTER INSERT ON advanced
                BEGIN
                    UPDATE advanced
                    SET left_value = left_value
                    WHERE id = NEW.id;
                END;
            CREATE INDEX ix_advanced_partial
                ON advanced(left_value)
                WHERE right_value > 0;
            CREATE INDEX ix_advanced_expression
                ON advanced((left_value + right_value));
            CREATE VIRTUAL TABLE search_documents
                USING fts5(content);
            """,
            Ct);

        SqliteBackupSnapshot? snapshot = null;
        try
        {
            snapshot = await SqliteBackupSnapshot.CreateAsync(
                sourcePath,
                snapshotPath,
                Ct);
            MigrationCatalog catalog = await InspectAsync(
                new SqliteMigrationSourceInspector(snapshot),
                includeProfile: false);

            MigrationCatalogObject advanced =
                Find(catalog, MigrationObjectKind.Table, "advanced");
            MigrationCatalogObject generated = ChildrenOf(
                    catalog,
                    advanced,
                    MigrationObjectKind.Column)
                .Single(static column => column.SourceName == "total");
            MigrationCatalogObject view =
                Find(catalog, MigrationObjectKind.View, "advanced_view");
            MigrationCatalogObject trigger =
                Find(catalog, MigrationObjectKind.Trigger, "advanced_trigger");
            MigrationCatalogObject partial =
                Find(catalog, MigrationObjectKind.Index, "ix_advanced_partial");
            MigrationCatalogObject expression =
                Find(catalog, MigrationObjectKind.Index, "ix_advanced_expression");
            MigrationCatalogObject virtualTable = catalog.Objects.Single(candidate =>
                candidate.SourceName == "search_documents" &&
                candidate.Kind is MigrationObjectKind.Table or MigrationObjectKind.Other);

            foreach (MigrationCatalogObject unsupported in
                     new[] { generated, view, trigger, partial, expression, virtualTable })
            {
                Assert.Contains(
                    catalog.Diagnostics,
                    diagnostic =>
                        diagnostic.ObjectId == unsupported.ObjectId &&
                        diagnostic.Status is
                            MigrationCompatibilityStatus.Conditional or
                            MigrationCompatibilityStatus.Unsupported or
                            MigrationCompatibilityStatus.Unknown);
            }

            Assert.Contains(
                catalog.Objects,
                candidate =>
                    candidate.SourceName.StartsWith(
                        "search_documents_",
                        StringComparison.Ordinal) &&
                    candidate.Kind is MigrationObjectKind.Table or MigrationObjectKind.Other);
        }
        finally
        {
            await SqliteTestDatabase.DisposeIfSupportedAsync(snapshot);
        }
    }

    [Fact]
    public async Task IntegerPrimaryKeyDescIsNotReportedAsRowIdAlias()
    {
        using var temporary = new SqliteTestDirectory();
        string sourcePath = temporary.PathFor("descending-pk-source.sqlite");
        string snapshotPath = temporary.PathFor("descending-pk-snapshot.sqlite");
        await SqliteTestDatabase.CreateAsync(
            sourcePath,
            """
            CREATE TABLE descending_pk (
                id INTEGER PRIMARY KEY DESC,
                payload TEXT
            );
            INSERT INTO descending_pk(id, payload) VALUES (10, 'ten');
            """,
            Ct);

        SqliteBackupSnapshot snapshot = await SqliteBackupSnapshot.CreateAsync(
            sourcePath,
            snapshotPath,
            Ct);
        MigrationCatalog catalog = await InspectAsync(
            new SqliteMigrationSourceInspector(snapshot),
            includeProfile: false);

        MigrationCatalogObject table =
            Find(catalog, MigrationObjectKind.Table, "descending_pk");
        MigrationCatalogObject id = ChildrenOf(
                catalog,
                table,
                MigrationObjectKind.Column)
            .Single(static column => column.SourceName == "id");
        AssertFacet(id, "sqliteRowIdAlias", "false");
        AssertFacet(table, "sqliteRowIdAlias", "rowid");
    }

    [Fact]
    public async Task ProfileSkipsGeneratedColumnThatRequiresUnavailableApplicationFunction()
    {
        using var temporary = new SqliteTestDirectory();
        string sourcePath = temporary.PathFor("generated-function-source.sqlite");
        string snapshotPath = temporary.PathFor("generated-function-snapshot.sqlite");
        await CreateFunctionBackedGeneratedColumnDatabaseAsync(sourcePath);

        SqliteBackupSnapshot? snapshot = null;
        try
        {
            snapshot = await SqliteBackupSnapshot.CreateAsync(
                sourcePath,
                snapshotPath,
                Ct);
            MigrationCatalog catalog = await InspectAsync(
                new SqliteMigrationSourceInspector(snapshot),
                includeProfile: true);

            MigrationCatalogObject table =
                Find(catalog, MigrationObjectKind.Table, "generated_values");
            MigrationCatalogObject source = ChildrenOf(
                    catalog,
                    table,
                    MigrationObjectKind.Column)
                .Single(static column => column.SourceName == "source_value");
            MigrationCatalogObject generated = ChildrenOf(
                    catalog,
                    table,
                    MigrationObjectKind.Column)
                .Single(static column => column.SourceName == "generated_value");

            AssertFacet(source, "sqliteStorageClassText", "1");
            Assert.DoesNotContain(
                generated.Facets,
                static facet => facet.Name == "profileKind");
            Assert.DoesNotContain(
                generated.Facets,
                static facet => facet.Name == "sqliteStorageClassText");
            Assert.Contains(
                catalog.Diagnostics,
                diagnostic =>
                    diagnostic.ObjectId == generated.ObjectId &&
                    diagnostic.RuleId == "MIG-SQLITE-COLUMN-GENERATED-001");
        }
        finally
        {
            await SqliteTestDatabase.DisposeIfSupportedAsync(snapshot);
        }
    }

    [Fact]
    public async Task NonRowIdPrimaryKeysHaveDeterministicNonOverrideableDiagnostic()
    {
        using var temporary = new SqliteTestDirectory();
        string sourcePath = temporary.PathFor("nullable-primary-source.sqlite");
        string snapshotPath = temporary.PathFor("nullable-primary-snapshot.sqlite");
        await SqliteTestDatabase.CreateAsync(
            sourcePath,
            """
            CREATE TABLE nullable_primary (
                id TEXT PRIMARY KEY,
                payload TEXT
            );
            CREATE TABLE required_primary (
                id TEXT NOT NULL PRIMARY KEY,
                payload TEXT
            );
            CREATE TABLE rowid_primary (
                id INTEGER PRIMARY KEY,
                payload TEXT
            );
            CREATE TABLE integer_named_primary (
                id INT NOT NULL PRIMARY KEY,
                payload TEXT
            );
            INSERT INTO nullable_primary(id, payload) VALUES (NULL, 'allowed');
            INSERT INTO integer_named_primary(id, payload) VALUES (10, 'not-a-rowid-alias');
            """,
            Ct);

        SqliteBackupSnapshot? snapshot = null;
        try
        {
            snapshot = await SqliteBackupSnapshot.CreateAsync(
                sourcePath,
                snapshotPath,
                Ct);
            var inspector = new SqliteMigrationSourceInspector(snapshot);
            MigrationCatalog first = await InspectAsync(inspector, includeProfile: false);
            MigrationCatalog second = await InspectAsync(inspector, includeProfile: false);

            MigrationCatalogObject nullableTable =
                Find(first, MigrationObjectKind.Table, "nullable_primary");
            MigrationCatalogObject nullablePrimary = Assert.Single(
                first.Objects,
                candidate =>
                    candidate.Kind == MigrationObjectKind.Key &&
                    candidate.ParentObjectId == nullableTable.ObjectId);
            MigrationDiagnostic diagnostic = Assert.Single(
                first.Diagnostics,
                candidate =>
                    candidate.ObjectId == nullablePrimary.ObjectId &&
                    candidate.RuleId == "MIG-SQLITE-PRIMARY-KEY-NON-ROWID-001");
            MigrationDiagnostic repeated = Assert.Single(
                second.Diagnostics,
                candidate =>
                    candidate.ObjectId == nullablePrimary.ObjectId &&
                    candidate.RuleId == "MIG-SQLITE-PRIMARY-KEY-NON-ROWID-001");

            Assert.Equal(MigrationCompatibilityStatus.Unsupported, diagnostic.Status);
            Assert.Equal(MigrationDiagnosticSeverity.Error, diagnostic.Severity);
            Assert.False(diagnostic.CanOverride);
            Assert.Equal(diagnostic.DiagnosticId, repeated.DiagnosticId);
            Assert.Contains("NULL", diagnostic.Summary, StringComparison.Ordinal);

            foreach (string nonRowIdTableName in
                     new[] { "required_primary", "integer_named_primary" })
            {
                MigrationCatalogObject nonRowIdTable =
                    Find(first, MigrationObjectKind.Table, nonRowIdTableName);
                MigrationCatalogObject nonRowIdPrimary = Assert.Single(
                    first.Objects,
                    candidate =>
                        candidate.Kind == MigrationObjectKind.Key &&
                        candidate.ParentObjectId == nonRowIdTable.ObjectId);
                MigrationDiagnostic nonRowIdDiagnostic = Assert.Single(
                    first.Diagnostics,
                    candidate =>
                        candidate.ObjectId == nonRowIdPrimary.ObjectId &&
                        candidate.RuleId == "MIG-SQLITE-PRIMARY-KEY-NON-ROWID-001");
                Assert.False(nonRowIdDiagnostic.CanOverride);
            }

            MigrationCatalogObject rowIdTable =
                Find(first, MigrationObjectKind.Table, "rowid_primary");
            MigrationCatalogObject rowIdPrimary = Assert.Single(
                first.Objects,
                candidate =>
                    candidate.Kind == MigrationObjectKind.Key &&
                    candidate.ParentObjectId == rowIdTable.ObjectId);
            Assert.DoesNotContain(
                first.Diagnostics,
                candidate =>
                    candidate.ObjectId == rowIdPrimary.ObjectId &&
                    candidate.RuleId == "MIG-SQLITE-PRIMARY-KEY-NON-ROWID-001");
        }
        finally
        {
            await SqliteTestDatabase.DisposeIfSupportedAsync(snapshot);
        }
    }

    [Fact]
    public async Task ForeignKeyViolationHasNonOverrideableDiagnosticWithoutRowData()
    {
        using var temporary = new SqliteTestDirectory();
        string sourcePath = temporary.PathFor("foreign-key-violation-source.sqlite");
        string snapshotPath = temporary.PathFor("foreign-key-violation-snapshot.sqlite");
        await SqliteTestDatabase.CreateAsync(
            sourcePath,
            """
            CREATE TABLE parents (
                id INTEGER PRIMARY KEY
            );
            CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER,
                FOREIGN KEY(parent_id) REFERENCES parents(id)
            );
            PRAGMA foreign_keys=OFF;
            INSERT INTO children(id, parent_id) VALUES (1, 424242);
            """,
            Ct);

        SqliteBackupSnapshot? snapshot = null;
        try
        {
            snapshot = await SqliteBackupSnapshot.CreateAsync(
                sourcePath,
                snapshotPath,
                Ct);
            MigrationCatalog catalog = await InspectAsync(
                new SqliteMigrationSourceInspector(snapshot),
                includeProfile: false);

            MigrationCatalogObject foreignKey =
                Find(catalog, MigrationObjectKind.ForeignKey, "FK_children_0");
            MigrationDiagnostic diagnostic = Assert.Single(
                catalog.Diagnostics,
                candidate =>
                    candidate.ObjectId == foreignKey.ObjectId &&
                    candidate.RuleId == "MIG-SQLITE-FK-DATA-VIOLATION-001");

            Assert.Equal(MigrationCompatibilityStatus.Unsupported, diagnostic.Status);
            Assert.Equal(MigrationDiagnosticSeverity.Error, diagnostic.Severity);
            Assert.False(diagnostic.CanOverride);
            Assert.DoesNotContain("424242", diagnostic.Summary, StringComparison.Ordinal);
            Assert.DoesNotContain("424242", diagnostic.Explanation, StringComparison.Ordinal);
            Assert.DoesNotContain("424242", diagnostic.Remediation, StringComparison.Ordinal);
        }
        finally
        {
            await SqliteTestDatabase.DisposeIfSupportedAsync(snapshot);
        }
    }

    [Fact]
    public async Task UnverifiableForeignKeyCheckHasNonOverrideableDiagnostic()
    {
        using var temporary = new SqliteTestDirectory();
        string sourcePath = temporary.PathFor("foreign-key-unverifiable-source.sqlite");
        string snapshotPath = temporary.PathFor("foreign-key-unverifiable-snapshot.sqlite");
        await SqliteTestDatabase.CreateAsync(
            sourcePath,
            """
            CREATE TABLE parents (
                id INTEGER
            );
            CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER,
                FOREIGN KEY(parent_id) REFERENCES parents(id)
            );
            """,
            Ct);

        SqliteBackupSnapshot? snapshot = null;
        try
        {
            snapshot = await SqliteBackupSnapshot.CreateAsync(
                sourcePath,
                snapshotPath,
                Ct);
            MigrationCatalog catalog = await InspectAsync(
                new SqliteMigrationSourceInspector(snapshot),
                includeProfile: false);

            MigrationCatalogObject foreignKey =
                Find(catalog, MigrationObjectKind.Other, "FK_children_0");
            MigrationDiagnostic diagnostic = Assert.Single(
                catalog.Diagnostics,
                candidate =>
                    candidate.ObjectId == foreignKey.ObjectId &&
                    candidate.RuleId == "MIG-SQLITE-FK-DATA-UNVERIFIABLE-001");

            Assert.Equal(MigrationCompatibilityStatus.Unknown, diagnostic.Status);
            Assert.Equal(MigrationDiagnosticSeverity.Error, diagnostic.Severity);
            Assert.False(diagnostic.CanOverride);
        }
        finally
        {
            await SqliteTestDatabase.DisposeIfSupportedAsync(snapshot);
        }
    }

    [Fact]
    public async Task InspectRejectsSchemaObjectCountBeforeGrowingCatalogCollections()
    {
        await AssertInspectionLimitAsync(
            """
            CREATE TABLE first_object (id INTEGER PRIMARY KEY);
            CREATE TABLE sensitive_second_object (id INTEGER PRIMARY KEY);
            """,
            SqliteInspectionLimits.Default with
            {
                MaxSchemaObjects = 1,
            },
            "SQLite inspection exceeded the fixed schema-object limit.",
            "sensitive_second_object");
    }

    [Fact]
    public async Task InspectRejectsColumnCountBeforeReadingColumnMetadata()
    {
        await AssertInspectionLimitAsync(
            """
            CREATE TABLE sensitive_wide_table (
                first_column INTEGER,
                second_column TEXT
            );
            """,
            SqliteInspectionLimits.Default with
            {
                MaxColumnsPerTable = 1,
            },
            "SQLite inspection exceeded the fixed per-table column limit.",
            "sensitive_wide_table");
    }

    [Fact]
    public async Task InspectRejectsIndexCountBeforeGrowingIndexCollections()
    {
        await AssertInspectionLimitAsync(
            """
            CREATE TABLE indexed_values (id INTEGER, value TEXT);
            CREATE INDEX first_index ON indexed_values(id);
            CREATE INDEX sensitive_second_index ON indexed_values(value);
            """,
            SqliteInspectionLimits.Default with
            {
                MaxIndexesPerTable = 1,
            },
            "SQLite inspection exceeded the fixed per-table index limit.",
            "sensitive_second_index");
    }

    [Fact]
    public async Task InspectRejectsForeignKeyCountBeforeGrowingForeignKeyCollections()
    {
        await AssertInspectionLimitAsync(
            """
            CREATE TABLE first_parent (id INTEGER PRIMARY KEY);
            CREATE TABLE second_parent (id INTEGER PRIMARY KEY);
            CREATE TABLE sensitive_child (
                first_parent_id INTEGER,
                second_parent_id INTEGER,
                FOREIGN KEY(first_parent_id) REFERENCES first_parent(id),
                FOREIGN KEY(second_parent_id) REFERENCES second_parent(id)
            );
            """,
            SqliteInspectionLimits.Default with
            {
                MaxForeignKeysPerTable = 1,
            },
            "SQLite inspection exceeded the fixed per-table foreign-key limit.",
            "sensitive_child");
    }

    [Fact]
    public async Task InspectUsesUtf8ByteLengthForIndividualCatalogStrings()
    {
        const string sensitiveName = "\u00e9\u00e9\u00e9";
        await AssertInspectionLimitAsync(
            $"CREATE TABLE \"{sensitiveName}\" (id INTEGER);",
            SqliteInspectionLimits.Default with
            {
                MaxCatalogStringUtf8Bytes = 5,
            },
            "SQLite inspection exceeded the fixed catalog-string byte limit.",
            sensitiveName);
    }

    [Fact]
    public async Task InspectUsesUtf8ByteLengthForUtf16DatabaseCatalogStrings()
    {
        const string sensitiveName = "\u6f22\u6f22\u6f22";
        await AssertInspectionLimitAsync(
            $"""
             PRAGMA encoding='UTF-16le';
             CREATE TABLE "{sensitiveName}" (id INTEGER);
             """,
            SqliteInspectionLimits.Default with
            {
                MaxCatalogStringUtf8Bytes = 7,
            },
            "SQLite inspection exceeded the fixed catalog-string byte limit.",
            sensitiveName);
    }

    [Fact]
    public async Task InspectRejectsAggregateRetainedSchemaMetadata()
    {
        const string sensitiveName = "sensitive_aggregate_table";
        await AssertInspectionLimitAsync(
            $"CREATE TABLE {sensitiveName} (id INTEGER);",
            SqliteInspectionLimits.Default with
            {
                MaxCatalogStringUtf8Bytes = 1_024,
                MaxRetainedSchemaMetadataBytes = 100,
            },
            "SQLite inspection exceeded the fixed aggregate schema-metadata byte limit.",
            sensitiveName);
    }

    private static async ValueTask<MigrationCatalog> InspectAsync(
        SqliteMigrationSourceInspector inspector,
        bool includeProfile,
        int profileSampleSize = 1_000) =>
        await inspector.InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion =
                    CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = includeProfile,
                ProfileSampleSize = profileSampleSize,
            },
            Ct);

    private static async Task AssertInspectionLimitAsync(
        string sql,
        SqliteInspectionLimits limits,
        string expectedMessage,
        string forbiddenText)
    {
        using var temporary = new SqliteTestDirectory();
        string sourcePath = temporary.PathFor("bounded-source.sqlite");
        string snapshotPath = temporary.PathFor("bounded-snapshot.sqlite");
        await SqliteTestDatabase.CreateAsync(sourcePath, sql, Ct);

        SqliteBackupSnapshot? snapshot = null;
        try
        {
            snapshot = await SqliteBackupSnapshot.CreateAsync(
                sourcePath,
                snapshotPath,
                Ct);
            var inspector = new SqliteMigrationSourceInspector(snapshot, limits);

            SqliteMigrationException exception = await Assert.ThrowsAsync<
                SqliteMigrationException>(
                async () => await InspectAsync(inspector, includeProfile: false));

            Assert.Equal(expectedMessage, exception.Message);
            Assert.DoesNotContain(
                forbiddenText,
                exception.ToString(),
                StringComparison.Ordinal);
        }
        finally
        {
            await SqliteTestDatabase.DisposeIfSupportedAsync(snapshot);
        }
    }

    private static MigrationCatalogObject Find(
        MigrationCatalog catalog,
        MigrationObjectKind kind,
        string sourceName) =>
        Assert.Single(
            catalog.Objects,
            candidate =>
            candidate.Kind == kind &&
            candidate.SourceName == sourceName);

    private static IReadOnlyList<MigrationCatalogObject> ChildrenOf(
        MigrationCatalog catalog,
        MigrationCatalogObject parent,
        MigrationObjectKind kind) =>
        catalog.Objects.Where(candidate =>
                candidate.Kind == kind &&
                candidate.ParentObjectId == parent.ObjectId)
            .ToArray();

    private static bool ReferencesColumn(
        MigrationCatalogObject schemaObject,
        string columnObjectId) =>
        schemaObject.Members.Any(reference =>
            reference.ObjectId == columnObjectId &&
            reference.Role is
                MigrationObjectReferenceRoles.Column or
                MigrationObjectReferenceRoles.SourceColumn);

    private static void AssertFacet(
        MigrationCatalogObject schemaObject,
        string name,
        string expectedValue)
    {
        MigrationCatalogFacet facet = Assert.Single(
            schemaObject.Facets,
            candidate => candidate.Name == name);
        Assert.Equal(expectedValue, facet.Value);
    }

    private static async ValueTask CreateFunctionBackedGeneratedColumnDatabaseAsync(
        string path)
    {
        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Private,
            Pooling = false,
        }.ToString();
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(Ct);
        connection.CreateFunction<string?, string?>(
            "app_only",
            static value => value?.ToUpperInvariant(),
            isDeterministic: true);
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText =
            """
            PRAGMA journal_mode=DELETE;
            CREATE TABLE generated_values (
                id INTEGER PRIMARY KEY,
                source_value TEXT NOT NULL,
                generated_value TEXT
                    GENERATED ALWAYS AS (app_only(source_value)) VIRTUAL
            );
            INSERT INTO generated_values(id, source_value) VALUES (1, 'source');
            """;
        await command.ExecuteNonQueryAsync(Ct);
    }
}
