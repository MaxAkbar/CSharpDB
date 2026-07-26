using CSharpDB.Migration;
using CSharpDB.Migration.MySql;

namespace CSharpDB.Migration.MySql.Tests;

public sealed class MySqlMetadataVisibilityProofTests
{
    [Fact]
    public void AllFourDirectSchemaGrantsRemoveAnalyzerBlocker()
    {
        MigrationCatalog catalog = Build(
            MySqlTestSnapshot
                .MetadataVisibilityProof());
        MigrationCatalogObject database =
            Assert.Single(
                catalog.Objects,
                static item =>
                    item.Kind ==
                    MigrationObjectKind.Database);

        Assert.Equal(
            "true",
            Facet(
                database,
                "mysqlMetadataVisibilityProofAttempted"));
        Assert.Equal(
            "true",
            Facet(
                database,
                "mysqlMetadataVisibilityAccountFormatSupported"));
        Assert.Equal(
            "true",
            Facet(
                database,
                "mysqlMetadataVisibilityGranteeMatched"));
        Assert.Equal(
            "true",
            Facet(database, "mysqlDirectSchemaSelect"));
        Assert.Equal(
            "true",
            Facet(database, "mysqlDirectSchemaShowView"));
        Assert.Equal(
            "true",
            Facet(database, "mysqlDirectSchemaTrigger"));
        Assert.Equal(
            "true",
            Facet(database, "mysqlDirectSchemaExecute"));
        Assert.Equal(
            "true",
            Facet(
                database,
                MySqlCatalogBuilder
                    .MetadataVisibilityCompleteFacet));
        Assert.DoesNotContain(
            catalog.Diagnostics,
            static diagnostic =>
                diagnostic.RuleId ==
                "MIG-MYSQL-METADATA-COMPLETENESS-UNKNOWN-001");
    }

    [Theory]
    [InlineData(false, true, true, true)]
    [InlineData(true, false, true, true)]
    [InlineData(true, true, false, true)]
    [InlineData(true, true, true, false)]
    public void EveryMissingDirectGrantKeepsAnalyzerBlocker(
        bool select,
        bool showView,
        bool trigger,
        bool execute)
    {
        MigrationCatalog catalog = Build(
            MySqlTestSnapshot.MetadataVisibilityProof(
                select,
                showView,
                trigger,
                execute));

        Assert.Contains(
            catalog.Diagnostics,
            static diagnostic =>
                diagnostic.RuleId ==
                "MIG-MYSQL-METADATA-COMPLETENESS-UNKNOWN-001");
    }

    [Fact]
    public void WrongGranteeRoleOnlyAndUnsupportedAccountRemainUnproven()
    {
        MySqlMetadataVisibilityProof[] proofs =
        [
            MySqlTestSnapshot.MetadataVisibilityProof(
                select: false,
                showView: false,
                trigger: false,
                execute: false,
                granteeMatched: false),
            MySqlTestSnapshot.MetadataVisibilityProof(
                select: false,
                showView: false,
                trigger: false,
                execute: false,
                accountFormatSupported: false,
                granteeMatched: false),
            MySqlMetadataVisibilityProof.Unproven,
        ];

        Assert.All(
            proofs,
            proof => Assert.Contains(
                Build(proof).Diagnostics,
                static diagnostic =>
                    diagnostic.RuleId ==
                    "MIG-MYSQL-METADATA-COMPLETENESS-UNKNOWN-001"));
    }

    [Theory]
    [InlineData("reader@localhost", "'reader'@'localhost'")]
    [InlineData("reader@%", "'reader'@'%'")]
    [InlineData("reader@10.0.0.5", "'reader'@'10.0.0.5'")]
    public void AuthenticatedAccountConvertsToExactInformationSchemaGrantee(
        string account,
        string expected)
    {
        Assert.True(
            MySqlCatalogReader
                .TryCreateDirectSchemaGrantee(
                    account,
                    out string? actual));
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData("")]
    [InlineData("reader")]
    [InlineData("@localhost")]
    [InlineData("reader@")]
    [InlineData("reader@host@extra")]
    [InlineData("read'er@localhost")]
    [InlineData("reader@local\\host")]
    public void AmbiguousAuthenticatedAccountFailsClosed(
        string account)
    {
        Assert.False(
            MySqlCatalogReader
                .TryCreateDirectSchemaGrantee(
                    account,
                    out string? grantee));
        Assert.Null(grantee);
    }

    [Fact]
    public void InconsistentProofFactsAreRejected()
    {
        var impossible =
            new MySqlMetadataVisibilityProof(
                Attempted: true,
                AccountFormatSupported: true,
                GranteeMatched: false,
                HasDirectSchemaSelect: true,
                HasDirectSchemaShowView: false,
                HasDirectSchemaTrigger: false,
                HasDirectSchemaExecute: false);

        Assert.Throws<MySqlMigrationException>(
            () => Build(impossible));
    }

    private static MigrationCatalog Build(
        MySqlMetadataVisibilityProof proof) =>
        MySqlCatalogBuilder.Build(
            MySqlTestSnapshot.Create(
                metadataVisibilityProof: proof),
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion =
                    CSharpDbCapabilityCatalogLoader
                        .CurrentTargetVersion,
                IncludeProfile = false,
            },
            MySqlInspectionLimits.Default,
            CancellationToken.None);

    private static string? Facet(
        MigrationCatalogObject item,
        string name) =>
        item.Facets.Single(facet =>
            string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal)).Value;
}
