using System.Data.OleDb;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Access.Tests;

public sealed class AccessCatalogBuilderTests
{
    [Fact]
    public void OrdinaryPrimaryKeyTableBuildsDeterministicCatalog()
    {
        AccessCatalogSnapshot snapshot =
            AccessTestFixture.CreateSnapshot();
        AccessCatalogBinding first =
            AccessCatalogBuilder.Build(
                snapshot,
                Request());
        AccessCatalogBinding second =
            AccessCatalogBuilder.Build(
                snapshot,
                Request());

        MigrationContractValidator.ValidateCatalog(
            first.Catalog);
        Assert.Equal(
            MigrationArtifactSerializer
                .ComputeCatalogDigest(first.Catalog),
            MigrationArtifactSerializer
                .ComputeCatalogDigest(second.Catalog));
        Assert.Equal(
            MigrationSourceKind.Access,
            first.Catalog.Source.Kind);
        AccessTableBinding table =
            Assert.Single(first.Tables);
        Assert.True(table.IsDataAvailable);
        Assert.Equal(
            ["Id"],
            table.PrimaryKeyColumns.Select(
                static column =>
                    column.Metadata.Name));
        MigrationCatalogObject key =
            Assert.Single(
                first.Catalog.Objects,
                static item =>
                    item.Kind ==
                    MigrationObjectKind.Key);
        Assert.Equal(
            "primary",
            Facet(key, "kind"));
        Assert.Contains(
            first.Catalog.Diagnostics,
            static item =>
                item.RuleId ==
                AccessCatalogBuilder
                    .LiveQualificationRule &&
                !item.CanOverride &&
                item.Status ==
                MigrationCompatibilityStatus.Unknown);
    }

    [Fact]
    public void LocalRelationshipBindsToReferencedPrimaryKey()
    {
        AccessCatalogSnapshot snapshot =
            AccessTestFixture
                .CreateRelationalSnapshot();

        AccessCatalogBinding binding =
            AccessCatalogBuilder.Build(
                snapshot,
                Request());

        MigrationCatalogObject foreignKey =
            Assert.Single(
                binding.Catalog.Objects,
                static item =>
                    item.Kind ==
                    MigrationObjectKind.ForeignKey);
        Assert.Equal(
            "restrict",
            Facet(foreignKey, "onDelete"));
        Assert.Equal(
            2,
            foreignKey.Members.Count);
        Assert.Contains(
            foreignKey.Members,
            static member =>
                member.Role ==
                MigrationObjectReferenceRoles
                    .SourceColumn);
        Assert.Contains(
            foreignKey.Members,
            static member =>
                member.Role ==
                MigrationObjectReferenceRoles
                    .ReferencedKey);
        Assert.DoesNotContain(
            binding.Catalog.Diagnostics,
            static item =>
                item.RuleId ==
                "MIG-ACCESS-FK-BINDING-UNKNOWN-001");
    }

    [Fact]
    public void MissingKeyAndUnsupportedScalarFailClosed()
    {
        AccessCatalogSnapshot baseline =
            AccessTestFixture.CreateSnapshot();
        AccessTableMetadata table =
            Assert.Single(baseline.Tables);
        AccessColumnMetadata unsupported =
            table.Columns[1] with
            {
                ProviderType =
                    OleDbType.IDispatch,
            };
        AccessCatalogSnapshot snapshot =
            baseline with
            {
                Tables =
                [
                    table with
                    {
                        PrimaryKeyColumns = [],
                        Columns =
                        [
                            table.Columns[0],
                            unsupported,
                        ],
                    },
                ],
            };

        AccessCatalogBinding binding =
            AccessCatalogBuilder.Build(
                snapshot,
                Request());

        Assert.False(
            Assert.Single(binding.Tables)
                .IsDataAvailable);
        Assert.Contains(
            binding.Catalog.Diagnostics,
            static item =>
                item.RuleId ==
                "MIG-ACCESS-TABLE-STABLE-ORDER-001" &&
                !item.CanOverride);
        Assert.Contains(
            binding.Catalog.Diagnostics,
            static item =>
                item.RuleId ==
                "MIG-ACCESS-COLUMN-TYPE-UNSUPPORTED-001" &&
                item.Status ==
                MigrationCompatibilityStatus
                    .Unsupported);
    }

    [Theory]
    [InlineData(OleDbType.Integer, "SignedInteger")]
    [InlineData(OleDbType.Currency, "Decimal")]
    [InlineData(OleDbType.VarWChar, "Text")]
    [InlineData(OleDbType.LongVarBinary, "Binary")]
    [InlineData(OleDbType.Date, "DateTime")]
    public void SupportedProviderTypesHaveLogicalMappings(
        OleDbType providerType,
        string logicalType)
    {
        Assert.True(
            AccessTypeCatalog.TryResolve(
                providerType,
                out AccessTypeSemantics semantics));
        Assert.Equal(
            logicalType,
            semantics.LogicalType);
    }

    private static MigrationInspectionRequest
        Request() =>
        new()
        {
            TargetCSharpDbVersion =
                CSharpDbCapabilityCatalogLoader
                    .CurrentTargetVersion,
            IncludeProfile = false,
        };

    private static string? Facet(
        MigrationCatalogObject item,
        string name) =>
        item.Facets.FirstOrDefault(
            facet => string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal))?.Value;
}

internal static class AccessTestFixture
{
    internal const string SourceDigest =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    internal static AccessCatalogSnapshot
        CreateSnapshot() =>
        new()
        {
            SourceContentDigest = SourceDigest,
            ProviderId =
                AccessProviderIds.Ace16,
            ProviderVersion =
                "test-provider/1",
            SourceVersion = "test-source/1",
            SourceName = "fixture",
            SourceExtension = ".accdb",
            Tables =
            [
                new AccessTableMetadata
                {
                    Name = "People",
                    Columns =
                    [
                        new AccessColumnMetadata
                        {
                            Name = "Id",
                            Ordinal = 1,
                            ProviderType =
                                OleDbType.Integer,
                            Nullable = false,
                        },
                        new AccessColumnMetadata
                        {
                            Name = "Name",
                            Ordinal = 2,
                            ProviderType =
                                OleDbType.VarWChar,
                            Nullable = true,
                            MaximumLength = 255,
                        },
                    ],
                    PrimaryKeyColumns = ["Id"],
                    Indexes =
                    [
                        new AccessIndexMetadata
                        {
                            Name = "IX_People_Name",
                            Unique = false,
                            Primary = false,
                            Columns = ["Name"],
                        },
                    ],
                },
            ],
        };

    internal static AccessCatalogSnapshot
        CreateRelationalSnapshot()
    {
        AccessCatalogSnapshot baseline =
            CreateSnapshot();
        return baseline with
        {
            Tables =
            [
                .. baseline.Tables,
                new AccessTableMetadata
                {
                    Name = "Orders",
                    Columns =
                    [
                        new AccessColumnMetadata
                        {
                            Name = "OrderId",
                            Ordinal = 1,
                            ProviderType =
                                OleDbType.Integer,
                            Nullable = false,
                        },
                        new AccessColumnMetadata
                        {
                            Name = "PersonId",
                            Ordinal = 2,
                            ProviderType =
                                OleDbType.Integer,
                            Nullable = false,
                        },
                    ],
                    PrimaryKeyColumns =
                        ["OrderId"],
                },
            ],
            ForeignKeys =
            [
                new AccessForeignKeyMetadata
                {
                    Name =
                        "FK_Orders_People",
                    SourceTable = "Orders",
                    ReferencedTable =
                        "People",
                    ReferencedKeyName =
                        "PK_People",
                    UpdateRule = "NO ACTION",
                    DeleteRule = "NO ACTION",
                    Columns =
                    [
                        new AccessForeignKeyColumnMetadata(
                            "PersonId",
                            "Id",
                            1),
                    ],
                },
            ],
        };
    }
}
