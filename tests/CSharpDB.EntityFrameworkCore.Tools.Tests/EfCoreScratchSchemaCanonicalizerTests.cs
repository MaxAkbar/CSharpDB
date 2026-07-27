using System.Data.Common;
using CSharpDB.Data;
using CSharpDB.Migration;
using CSharpDB.Migration.Validation;

namespace CSharpDB.EntityFrameworkCore.Tools.Tests;

public sealed class EfCoreScratchSchemaCanonicalizerTests
{
    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task Capture_NormalizesLogicalSchemaAndCoversStructuralKinds()
    {
        await using CSharpDbConnection connection =
            await OpenPrivateMemoryAsync();
        await ExecuteAsync(
            connection,
            """
            CREATE TABLE parents (
                tenant_id INTEGER,
                code TEXT COLLATE NOCASE,
                score INTEGER
                    CONSTRAINT ck_parents_score
                    CHECK (score >= 0),
                CONSTRAINT pk_parents
                    PRIMARY KEY (tenant_id, code),
                CONSTRAINT uq_parents_code
                    UNIQUE (code)
            );
            """);
        await ExecuteAsync(
            connection,
            """
            CREATE TABLE children (
                id INTEGER PRIMARY KEY IDENTITY,
                tenant_id INTEGER NOT NULL,
                parent_code TEXT COLLATE NOCASE NOT NULL,
                version BLOB ROWVERSION NOT NULL,
                CONSTRAINT fk_children_parents
                    FOREIGN KEY (tenant_id, parent_code)
                    REFERENCES parents (tenant_id, code)
                    ON DELETE CASCADE
            );
            """);
        await ExecuteAsync(
            connection,
            """
            CREATE UNIQUE INDEX ix_children_parent
            ON children (tenant_id, parent_code COLLATE NOCASE);
            """);
        await ExecuteAsync(
            connection,
            """
            CREATE VIEW children_view AS
            SELECT id, parent_code FROM children;
            """);
        await ExecuteAsync(
            connection,
            """
            CREATE TRIGGER trg_children_insert
            AFTER INSERT ON children
            BEGIN
                UPDATE children
                SET parent_code = NEW.parent_code
                WHERE id = NEW.id;
            END;
            """);
        await CreateHistorySchemaAsync(connection);

        EfCoreScratchSchemaCaptureResult result =
            EfCoreScratchSchemaCanonicalizer.Capture(connection, Ct);

        Assert.True(result.Succeeded);
        Assert.Equal(
            EfCoreScratchSchemaCaptureFailure.None,
            result.Failure);
        MigrationNormalizedSchema schema =
            Assert.IsType<MigrationNormalizedSchema>(result.Schema);
        Assert.Matches("^[0-9a-f]{64}$", schema.Digest);
        Assert.DoesNotContain(
            schema.Objects,
            item =>
                item.Kind == MigrationObjectKind.Table &&
                string.Equals(
                    item.TargetName,
                    "__EFMIGRATIONSHISTORY",
                    StringComparison.Ordinal));

        Assert.Contains(
            schema.Objects,
            item => item.Kind == MigrationObjectKind.Table);
        Assert.Contains(
            schema.Objects,
            item => item.Kind == MigrationObjectKind.Column);
        Assert.Contains(
            schema.Objects,
            item => item.Kind == MigrationObjectKind.Key);
        Assert.Contains(
            schema.Objects,
            item => item.Kind == MigrationObjectKind.ForeignKey);
        Assert.Contains(
            schema.Objects,
            item => item.Kind == MigrationObjectKind.CheckConstraint);
        Assert.Contains(
            schema.Objects,
            item => item.Kind == MigrationObjectKind.Index);
        Assert.Contains(
            schema.Objects,
            item => item.Kind == MigrationObjectKind.View);
        Assert.Contains(
            schema.Objects,
            item => item.Kind == MigrationObjectKind.Trigger);

        MigrationNormalizedSchemaObject idColumn = Assert.Single(
            schema.Objects,
            item =>
                item.Kind == MigrationObjectKind.Column &&
                item.TargetName == "ID");
        AssertAttribute(idColumn, "ordinal", "0");
        AssertAttribute(idColumn, "storeType", "INTEGER");
        AssertAttribute(idColumn, "nullable", "false");
        AssertAttribute(idColumn, "identity", "true");

        MigrationNormalizedSchemaObject versionColumn = Assert.Single(
            schema.Objects,
            item =>
                item.Kind == MigrationObjectKind.Column &&
                item.TargetName == "VERSION");
        AssertAttribute(versionColumn, "rowVersion", "true");

        MigrationNormalizedSchemaObject codeColumn = Assert.Single(
            schema.Objects,
            item =>
                item.Kind == MigrationObjectKind.Column &&
                item.TargetName == "CODE");
        AssertAttribute(codeColumn, "collation", "NOCASE");

        MigrationNormalizedSchemaObject foreignKey = Assert.Single(
            schema.Objects,
            item =>
                item.Kind == MigrationObjectKind.ForeignKey &&
                item.TargetName == "FK_CHILDREN_PARENTS");
        AssertAttribute(foreignKey, "onDelete", "cascade");
        Assert.Equal(
            2,
            foreignKey.Members.Count(member =>
                member.Role == "sourceColumn"));
        Assert.Equal(
            2,
            foreignKey.Members.Count(member =>
                member.Role == "referencedColumn"));
        Assert.Single(
            foreignKey.Members,
            member => member.Role == "referencedTable");

        MigrationNormalizedSchemaObject index = Assert.Single(
            schema.Objects,
            item =>
                item.Kind == MigrationObjectKind.Index &&
                item.TargetName == "IX_CHILDREN_PARENT");
        AssertAttribute(index, "unique", "true");
        AssertAttribute(index, "collation.000001", "NOCASE");
    }

    [Fact]
    public async Task Capture_IsIndependentOfEnumerationAndIdentifierCase()
    {
        await using CSharpDbConnection first =
            await OpenPrivateMemoryAsync();
        await ExecuteAsync(
            first,
            """
            CREATE TABLE Widgets (
                Id INTEGER PRIMARY KEY,
                Name TEXT COLLATE NOCASE
            );
            """);
        await ExecuteAsync(
            first,
            """
            CREATE TABLE Categories (
                Id INTEGER PRIMARY KEY,
                Label TEXT
            );
            """);
        await ExecuteAsync(
            first,
            "CREATE INDEX IX_Widgets_Name ON Widgets (Name);");
        await ExecuteAsync(
            first,
            "CREATE INDEX IX_Categories_Label ON Categories (Label);");

        await using CSharpDbConnection second =
            await OpenPrivateMemoryAsync();
        await ExecuteAsync(
            second,
            """
            CREATE TABLE categories (
                id INTEGER PRIMARY KEY,
                label TEXT
            );
            """);
        await ExecuteAsync(
            second,
            """
            CREATE TABLE widgets (
                id INTEGER PRIMARY KEY,
                name TEXT COLLATE nocase
            );
            """);
        await ExecuteAsync(
            second,
            "CREATE INDEX ix_categories_label ON categories (label);");
        await ExecuteAsync(
            second,
            "CREATE INDEX ix_widgets_name ON widgets (name);");

        EfCoreScratchSchemaCaptureResult firstResult =
            EfCoreScratchSchemaCanonicalizer.Capture(first, Ct);
        EfCoreScratchSchemaCaptureResult secondResult =
            EfCoreScratchSchemaCanonicalizer.Capture(second, Ct);

        Assert.True(firstResult.Succeeded);
        Assert.True(secondResult.Succeeded);
        Assert.Equal(
            firstResult.Schema!.Digest,
            secondResult.Schema!.Digest);
        Assert.Equal(
            firstResult.Schema.Objects.Select(item => item.ObjectId),
            secondResult.Schema.Objects.Select(item => item.ObjectId));
    }

    [Fact]
    public async Task Capture_ExcludesHistoryTableIndexesAndTriggers()
    {
        await using CSharpDbConnection empty =
            await OpenPrivateMemoryAsync();
        await using CSharpDbConnection historyOnly =
            await OpenPrivateMemoryAsync();
        await CreateHistorySchemaAsync(historyOnly);

        EfCoreScratchSchemaCaptureResult emptyResult =
            EfCoreScratchSchemaCanonicalizer.Capture(empty, Ct);
        EfCoreScratchSchemaCaptureResult historyResult =
            EfCoreScratchSchemaCanonicalizer.Capture(historyOnly, Ct);

        Assert.True(emptyResult.Succeeded);
        Assert.True(historyResult.Succeeded);
        Assert.Empty(emptyResult.Schema!.Objects);
        Assert.Empty(historyResult.Schema!.Objects);
        Assert.Equal(
            emptyResult.Schema.Digest,
            historyResult.Schema.Digest);
    }

    [Fact]
    public async Task Capture_DifferentLogicalShapeChangesDigest()
    {
        await using CSharpDbConnection nullable =
            await OpenPrivateMemoryAsync();
        await ExecuteAsync(
            nullable,
            "CREATE TABLE values_table (id INTEGER PRIMARY KEY, value TEXT);");

        await using CSharpDbConnection required =
            await OpenPrivateMemoryAsync();
        await ExecuteAsync(
            required,
            "CREATE TABLE values_table (id INTEGER PRIMARY KEY, value TEXT NOT NULL);");

        EfCoreScratchSchemaCaptureResult nullableResult =
            EfCoreScratchSchemaCanonicalizer.Capture(nullable, Ct);
        EfCoreScratchSchemaCaptureResult requiredResult =
            EfCoreScratchSchemaCanonicalizer.Capture(required, Ct);

        Assert.True(nullableResult.Succeeded);
        Assert.True(requiredResult.Succeeded);
        Assert.NotEqual(
            nullableResult.Schema!.Digest,
            requiredResult.Schema!.Digest);
    }

    [Fact]
    public async Task Capture_RejectsConnectionThatIsNotOpenPrivateMemory()
    {
        await using var closed = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false");

        EfCoreScratchSchemaCaptureResult result =
            EfCoreScratchSchemaCanonicalizer.Capture(closed, Ct);

        Assert.False(result.Succeeded);
        Assert.Null(result.Schema);
        Assert.Equal(
            EfCoreScratchSchemaCaptureFailure.ConnectionRejected,
            result.Failure);
    }

    [Theory]
    [InlineData(1, 4_194_304)]
    [InlineData(20_000, 1)]
    public async Task Capture_ReportsFixedFailureWhenBoundsAreExceeded(
        int maxObjects,
        int maxInputBytes)
    {
        await using CSharpDbConnection connection =
            await OpenPrivateMemoryAsync();
        await ExecuteAsync(
            connection,
            "CREATE TABLE bounded_values (id INTEGER PRIMARY KEY, value TEXT);");

        EfCoreScratchSchemaCaptureResult result =
            EfCoreScratchSchemaCanonicalizer.Capture(
                connection,
                new EfCoreScratchSchemaCaptureLimits(
                    maxObjects,
                    maxInputBytes),
                Ct);

        Assert.False(result.Succeeded);
        Assert.Null(result.Schema);
        Assert.Equal(
            EfCoreScratchSchemaCaptureFailure.LimitExceeded,
            result.Failure);
    }

    [Fact]
    public async Task Capture_PropagatesCallerCancellation()
    {
        await using CSharpDbConnection connection =
            await OpenPrivateMemoryAsync();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.Throws<OperationCanceledException>(
            () => EfCoreScratchSchemaCanonicalizer.Capture(
                connection,
                cancellation.Token));
    }

    private static void AssertAttribute(
        MigrationNormalizedSchemaObject item,
        string name,
        string value)
    {
        MigrationNormalizedSchemaAttribute attribute = Assert.Single(
            item.Attributes,
            candidate => candidate.Name == name);
        Assert.Equal(value, attribute.Value);
    }

    private static async Task<CSharpDbConnection>
        OpenPrivateMemoryAsync()
    {
        var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false");
        await connection.OpenAsync(Ct);
        return connection;
    }

    private static async Task CreateHistorySchemaAsync(
        CSharpDbConnection connection)
    {
        await ExecuteAsync(
            connection,
            """
            CREATE TABLE __EFMigrationsHistory (
                MigrationId TEXT NOT NULL PRIMARY KEY,
                ProductVersion TEXT NOT NULL
            );
            """);
        await ExecuteAsync(
            connection,
            """
            CREATE INDEX ix_history_version
            ON __EFMigrationsHistory (ProductVersion);
            """);
        await ExecuteAsync(
            connection,
            """
            CREATE TRIGGER trg_history_insert
            AFTER INSERT ON __EFMigrationsHistory
            BEGIN
                UPDATE __EFMigrationsHistory
                SET ProductVersion = NEW.ProductVersion
                WHERE MigrationId = NEW.MigrationId;
            END;
            """);
    }

    private static async Task ExecuteAsync(
        CSharpDbConnection connection,
        string sql)
    {
        await using DbCommand command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(Ct);
    }
}
