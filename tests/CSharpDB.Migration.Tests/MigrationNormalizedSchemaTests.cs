using CSharpDB.Migration.Validation;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationNormalizedSchemaTests
{
    [Fact]
    public void SchemaDigestIsIndependentOfInputOrdering()
    {
        MigrationNormalizedSchemaObject table = Object("table:orders", MigrationObjectKind.Table, "orders");
        MigrationNormalizedSchemaObject column = Object(
            "column:orders:id",
            MigrationObjectKind.Column,
            "id",
            "table:orders",
            [
                new MigrationNormalizedSchemaAttribute { Name = "targetType", Value = "Integer" },
                new MigrationNormalizedSchemaAttribute { Name = "nullable", Value = "false" },
            ]);

        MigrationNormalizedSchema forward = MigrationNormalizedSchemaContract.Create([table, column]);
        MigrationNormalizedSchema reverse = MigrationNormalizedSchemaContract.Create([column, table]);

        Assert.Equal(forward.Digest, reverse.Digest);
        Assert.Equal(
            ["column:orders:id", "table:orders"],
            forward.Objects.Select(item => item.ObjectId));
    }

    [Fact]
    public void CompareLocalizesMissingAndChangedDefinitions()
    {
        MigrationNormalizedSchema source = MigrationNormalizedSchemaContract.Create(
        [
            Object("table:orders", MigrationObjectKind.Table, "orders"),
            Object(
                "column:orders:id",
                MigrationObjectKind.Column,
                "id",
                "table:orders",
                [new MigrationNormalizedSchemaAttribute { Name = "targetType", Value = "Integer" }]),
        ]);
        MigrationNormalizedSchema target = MigrationNormalizedSchemaContract.Create(
        [
            Object(
                "column:orders:id",
                MigrationObjectKind.Column,
                "id",
                "table:orders",
                [new MigrationNormalizedSchemaAttribute { Name = "targetType", Value = "Text" }]),
        ]);

        MigrationNormalizedSchemaDifference[] differences =
            MigrationNormalizedSchemaContract.Compare(source, target).ToArray();

        Assert.Equal(2, differences.Length);
        Assert.Equal("column:orders:id", differences[0].ObjectId);
        Assert.NotEqual(differences[0].SourceDefinitionDigest, differences[0].TargetDefinitionDigest);
        Assert.Equal("table:orders", differences[1].ObjectId);
        Assert.NotNull(differences[1].SourceDefinitionDigest);
        Assert.Null(differences[1].TargetDefinitionDigest);
    }

    [Fact]
    public void CreateRejectsTamperedDefinitionDigest()
    {
        MigrationNormalizedSchemaObject valid = Object("table:orders", MigrationObjectKind.Table, "orders");
        MigrationNormalizedSchemaObject tampered = valid with
        {
            DefinitionDigest = new string('0', 64),
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationNormalizedSchemaContract.Create([tampered]));

        Assert.Contains("does not match", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsDuplicateObjectIdentity()
    {
        MigrationNormalizedSchemaObject first = Object("table:orders", MigrationObjectKind.Table, "orders");
        MigrationNormalizedSchemaObject second = Object("table:orders", MigrationObjectKind.Table, "orders_copy");

        Assert.Throws<InvalidDataException>(
            () => MigrationNormalizedSchemaContract.Create([first, second]));
    }

    private static MigrationNormalizedSchemaObject Object(
        string objectId,
        MigrationObjectKind kind,
        string targetName,
        string? parentObjectId = null,
        IReadOnlyList<MigrationNormalizedSchemaAttribute>? attributes = null) =>
        MigrationNormalizedSchemaContract.CreateObject(
            objectId,
            kind,
            parentObjectId,
            targetName,
            attributes);
}
