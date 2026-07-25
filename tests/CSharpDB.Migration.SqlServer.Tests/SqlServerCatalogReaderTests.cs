using System.Text.RegularExpressions;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration.SqlServer;

namespace CSharpDB.Migration.SqlServer.Tests;

public sealed partial class SqlServerCatalogReaderTests
{
    private const string Secret = "DistinctiveReaderPassword-42";
    private static readonly string[] s_auditedCommandDigests =
    [
        "f6e73d8ec4a4cb3f666b83aaeef5933aa27dec8f255dfda0771951361f9c5946",
        "7e796bfce6b9d4330e68d662c0928feba19f636aab0fe37e13910882c3d0d72a",
        "5d5ced4675ed6117d78ecc5475bbe214d106e38ba32d655ef742099a07d2e79b",
        "cd178870a4583120c033a3ce1460f6d42d98a88d16e6120939858d9d1d6feafe",
        "2072d04734b060c00d79bdac0099e018780157368ea153eb8d15018aa08a6216",
        "e3b65500b75b0aa66e2beb81fa5a847c90300929f153810aaea85de1c23b6aca",
        "13a0d89c0c528d48c9bd1740c4e4395ba71d314ed8195d389241c9c88e8987f1",
        "ae23f8d31326fadfdfbceff64ad9090968479e7e1652c2fbf3d472bb41b0919f",
        "6b5122618c9eed2c99c943d76ffc116a6e7eb85c4d710dd918f7fb9caea454b8",
        "67300ecd636451a5436093d1337ad572f582f6247e0e98a2e6d0109699d5dfa8",
        "adb505bd36ea55f724a25f7696d5bcd7ef47f2340f26ff5af488456aea5aecfa",
        "4d8728fd4cd5894a561f0eaf2bdeb52635703a81936986b39c845319503fd0a3",
        "18683225b1345714ee7b63c617f1492ce4e307e0cf1984a29c9bebe8385782cc",
        "c9d2a29a6afe30cfda8a5fc1d0ed45f8fd978e11d8a047583ea02280b4cb6650",
        "d7004f740253a659d0bc109e787eaae7cf2785ef9850efb6f58bbabeac5abfd7",
        "2116d5a741048afeed2ded50d90b7bfa848ff762bbba4d0de55e64416943dee2",
        "02019207a66afb224c795eb70018c2be6df44d8eaca934a6bf324d32942ad3a1",
        "acd3574d8e7f0377f54ee81dd0f2b2c83a91b428d3b5d39b69c1f4d3b8b52f8f",
        "c40b05bb190ed8850566541dd910753a6b2351a074cc0f85526dd6296412eaf0",
        "6e2e92e4c0c9ebf4badf8c90b7f00dd43836c431897f669bffecf4bfe1ce42b5",
        "aa27c56da5750d42955456a99854f24cb03b3546d02c4b4e4e46c72328ca444f",
        "8bee27739a5cf3f76bb5e3a9734ef6e2f4306699f237366658f789458a0e896e",
        "787025109ebd799bfad6d8be9d7b1b0ecf618139f04291f6ec26732e81cf4247",
        "0ad808d119751037726f3b62dee3869717bfaccbe70012c59b5e3b739c90a991",
        "b4d99a92550b78c8742401aebd93d0f900c82fb3d6e7336248f7ac50e82f8834",
        "f71ed19eea84c484baa9afa93609c4e1e78d2e26739753e786de358aaaf1cdf2",
        "15d84cbfd377f2c7bb37376aa71babf50de4d1bb7080b44fe0af90f7207a2968",
        "70e5f9d79063af63ddc6d735ee2f9fd4de1f471e394540f811f76730bb71f31b",
        "6d56a05e012d2516ab55f98d9136129ecdbb6e66705830ee1f559a0eef722001",
        "adfb4a33d111111d6e3bb3b469d134c4794240105f845a86728743de1582b34f",
        "372637f7e00918cfd7672e78f57a3bdcb14931926f0b86056f89367f2644db2c",
        "0fa6cfe028e248bd42db8529cccc8a4b78e57b4bf51acd6423a4d24b48c9b353",
        "245163800892637b96482c86705a3e952e425fc1dd0f744c928a97f841c67c54",
        "2589e8949d70a6b92d403ca1bf2dc0da093b3356c83037bde94d59fe34e345e9",
        "6e96af93cfc1bdae045139dcbc89e0cf2ac7a8c402962d6cf03978c09d84d020",
        "5fd5cdcbb49e1cfcb542914c5b2d495f43201aadd90e3a064c7b15f47c4739a2",
    ];

    [Fact]
    public void ConnectionPolicyForcesReadIntentAndPreservesTlsRequirements()
    {
        var reader = new SqlServerCatalogReader(
            "Server=tcp:sql.example.invalid,1433;" +
            "Initial Catalog=SourceDb;" +
            "User ID=reader;" +
            $"Password={Secret};" +
            "Encrypt=Strict;" +
            "TrustServerCertificate=False;" +
            "ApplicationIntent=ReadWrite;" +
            "Connect Timeout=0;" +
            "Connect Retry Count=5;" +
            "Pooling=True;" +
            "Persist Security Info=True;" +
            "Enlist=True;" +
            "MultipleActiveResultSets=True");

        Assert.Equal("ReadOnly", reader.Policy.ApplicationIntent);
        Assert.False(reader.Policy.Pooling);
        Assert.False(reader.Policy.PersistSecurityInfo);
        Assert.False(reader.Policy.Enlist);
        Assert.False(reader.Policy.MultipleActiveResultSets);
        Assert.Equal("Strict", reader.Policy.Encrypt);
        Assert.False(reader.Policy.TrustServerCertificate);
        Assert.Equal(30, reader.Policy.ConnectTimeout);
        Assert.Equal(0, reader.Policy.ConnectRetryCount);
        Assert.DoesNotContain(
            Secret,
            string.Join(
                "|",
                reader.Policy.ApplicationIntent,
                reader.Policy.Encrypt),
            StringComparison.Ordinal);
    }

    [Fact]
    public void ConnectionPolicyPreservesStricterConnectionTimeout()
    {
        var reader = new SqlServerCatalogReader(
            "Server=sql.example.invalid;Initial Catalog=SourceDb;" +
            "Connect Timeout=5;Connect Retry Count=3");

        Assert.Equal(5, reader.Policy.ConnectTimeout);
        Assert.Equal(0, reader.Policy.ConnectRetryCount);
    }

    [Theory]
    [InlineData(
        "Server=(localdb)\\MSSQLLocalDB;Initial Catalog=SourceDb;AttachDbFilename=C:\\fixtures\\source.mdf")]
    [InlineData(
        "Server=.\\SQLEXPRESS;Initial Catalog=SourceDb;User Instance=True")]
    public void ConnectionPolicyRejectsAttachAndUserInstance(string connectionString)
    {
        ArgumentException error = Assert.Throws<ArgumentException>(
            () => new SqlServerCatalogReader(connectionString));

        Assert.Contains("cannot attach", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void InvalidConnectionSettingsDoNotRetainInputExceptionOrSecret()
    {
        ArgumentException error = Assert.Throws<ArgumentException>(
            () => new SqlServerCatalogReader(
                "Server=source;Initial Catalog=fixture;" +
                $"Password={Secret};DefinitelyNotAKeyword=value"));

        Assert.Null(error.InnerException);
        Assert.DoesNotContain(Secret, error.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("DefinitelyNotAKeyword", error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void InvalidUnicodeEndpointIsRejectedWithoutRetainingInput()
    {
        const string invalidEndpoint = "invalid\uD800endpoint";

        ArgumentException error = Assert.Throws<ArgumentException>(
            () => new SqlServerCatalogReader(
                $"Server={invalidEndpoint};Initial Catalog=fixture"));

        Assert.Null(error.InnerException);
        Assert.Equal("connectionString", error.ParamName);
        Assert.DoesNotContain(invalidEndpoint, error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void SupportedVersionGateRejects14AndAccepts15()
    {
        SqlServerMigrationException error =
            Assert.Throws<SqlServerMigrationException>(
                () => SqlServerCatalogReader
                    .EnsureSupportedProductMajorVersion(14));

        Assert.Equal(
            "SQL Server 2019 or later is required for migration inspection.",
            error.Message);
        Assert.Null(error.InnerException);
        SqlServerCatalogReader.EnsureSupportedProductMajorVersion(15);
    }

    [Fact]
    public void CatalogCommandsAreStaticSelectOnlyAndPreflightLargeExpressions()
    {
        Assert.Equal(36, SqlServerCatalogReader.CommandTexts.Count);
        foreach (string command in SqlServerCatalogReader.CommandTexts)
        {
            Assert.StartsWith(
                "SELECT",
                command.TrimStart(),
                StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotMatch(MutatingStatement(), command);
            Assert.DoesNotContain("@", command, StringComparison.Ordinal);
        }

        Assert.Contains(
            "DATALENGTH(default_constraint.definition)",
            SqlServerCatalogReader.ColumnsQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "DATALENGTH(computed_column.definition)",
            SqlServerCatalogReader.ColumnsQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "c.default_object_id",
            SqlServerCatalogReader.ColumnsQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "c.is_identity",
            SqlServerCatalogReader.ColumnsQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "c.is_computed",
            SqlServerCatalogReader.ColumnsQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "DATALENGTH(i.filter_definition)",
            SqlServerCatalogReader.IndexesQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "DATALENGTH(cc.definition)",
            SqlServerCatalogReader.ChecksQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "VIEW SECURITY DEFINITION",
            SqlServerCatalogReader.ServerAndDatabaseQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "sys.user_token",
            SqlServerCatalogReader.UserTokensQuery,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            ".name",
            SqlServerCatalogReader.UserTokensQuery,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            ".sid",
            SqlServerCatalogReader.UserTokensQuery,
            StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            "permission_name",
            SqlServerCatalogReader.PermissionDenialsQuery,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "current_value",
            SqlServerCatalogReader.SequencesQuery,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            "last_used_value",
            SqlServerCatalogReader.SequencesQuery,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            "is_exhausted",
            SqlServerCatalogReader.SequencesQuery,
            StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            "sys.sql_expression_dependencies",
            SqlServerCatalogReader.ServerAndDatabaseQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "DATALENGTH(sm.definition)",
            SqlServerCatalogReader.ModulesQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "OBJECTPROPERTYEX",
            SqlServerCatalogReader.ModulesQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "N'IsEncrypted'",
            SqlServerCatalogReader.ModulesQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "sys.trigger_events",
            SqlServerCatalogReader.TriggerEventsQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "RTRIM(o.type)",
            SqlServerCatalogReader.RoutinesQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "RTRIM(module_object.type)",
            SqlServerCatalogReader.ModulesQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "p.parameter_id",
            SqlServerCatalogReader.ParametersQuery,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "p.default_value",
            SqlServerCatalogReader.ParametersQuery,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            "column_encryption_key",
            SqlServerCatalogReader.ParametersQuery,
            StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            "sys.sql_expression_dependencies",
            SqlServerCatalogReader.ExpressionDependenciesQuery,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "sys.sql_dependencies",
            SqlServerCatalogReader.ExpressionDependenciesQuery,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "dm_sql_",
            SqlServerCatalogReader.ExpressionDependenciesQuery,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            "create_date",
            SqlServerCatalogReader.ModulesQuery,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            "modify_date",
            SqlServerCatalogReader.ModulesQuery,
            StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            "o.type IN (N'U', N'V')",
            SqlServerCatalogReader.IndexesQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "CONVERT(int, NULL)",
            SqlServerCatalogReader.FullTextIndexesQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "fulltext_index.index_version",
            SqlServerCatalogReader.FullTextIndexesV17Query,
            StringComparison.Ordinal);
        Assert.Contains(
            "o.type IN (N'U', N'V')",
            SqlServerCatalogReader.FullTextIndexesQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "o.type IN (N'U', N'V')",
            SqlServerCatalogReader.FullTextIndexColumnsQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "SQL_VARIANT_PROPERTY",
            SqlServerCatalogReader.PartitionRangeValuesQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "DATALENGTH(CONVERT(varbinary(max), range_value.value))",
            SqlServerCatalogReader.PartitionRangeValuesQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "CONVERT(bit, NULL)",
            SqlServerCatalogReader.IndexPartitionsQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "CONVERT(varchar(3), NULL)",
            SqlServerCatalogReader.IndexPartitionsQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "partition.xml_compression",
            SqlServerCatalogReader.IndexPartitionsV16Query,
            StringComparison.Ordinal);
        Assert.Contains(
            "partition.index_id",
            SqlServerCatalogReader.IndexPartitionsQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "NULLIF(index_definition.data_space_id, 0)",
            SqlServerCatalogReader.IndexPartitionsQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "NULLIF(index_definition.data_space_id, 0)",
            SqlServerCatalogReader.IndexPartitionsV16Query,
            StringComparison.Ordinal);

        string physicalCommands = string.Join(
            "\n",
            SqlServerCatalogReader.CommandTexts.Skip(22));
        Assert.DoesNotContain(
            "sys.database_files",
            physicalCommands,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            "sys.allocation_units",
            physicalCommands,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            "dm_db_",
            physicalCommands,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            "crawl",
            physicalCommands,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            "stopword",
            physicalCommands,
            StringComparison.OrdinalIgnoreCase);

        string[] actualDigests = SqlServerCatalogReader.CommandTexts
            .Select(static command =>
                Convert.ToHexString(
                        SHA256.HashData(Encoding.UTF8.GetBytes(command)))
                    .ToLowerInvariant())
            .ToArray();
        Assert.Equal(s_auditedCommandDigests, actualDigests);
    }

    [Fact]
    public void EndpointDigestBindsOnlyNormalizedDataSource()
    {
        var first = new SqlServerCatalogReader(
            "Server=tcp:Sql.Example.Invalid,1433;Initial Catalog=One;" +
            "User ID=first;Password=first-secret");
        var differentAccountAndDatabase = new SqlServerCatalogReader(
            "Server=tcp:sql.example.invalid,1433;Initial Catalog=Two;" +
            "User ID=second;Password=second-secret");
        var differentSource = new SqlServerCatalogReader(
            "Server=tcp:other.example.invalid,1433;Initial Catalog=One;" +
            "User ID=first;Password=first-secret");

        Assert.Equal(first.EndpointDigest, differentAccountAndDatabase.EndpointDigest);
        Assert.NotEqual(first.EndpointDigest, differentSource.EndpointDigest);
        Assert.StartsWith("sha256:", first.EndpointDigest, StringComparison.Ordinal);
    }

    [GeneratedRegex(
        @"\b(INSERT|UPDATE|DELETE|MERGE|ALTER|CREATE|DROP|TRUNCATE|EXEC|EXECUTE)\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex MutatingStatement();
}
