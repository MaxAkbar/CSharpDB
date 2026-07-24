using CSharpDB.Migration;
using CSharpDB.Migration.Sqlite;

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
}
