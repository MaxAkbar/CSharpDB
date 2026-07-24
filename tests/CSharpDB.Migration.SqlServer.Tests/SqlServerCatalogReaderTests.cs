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
        "22e2c647c25def7f9da85e8b7bca6de41f517c1920f0f0d26cb3d2f78af46955",
        "7e796bfce6b9d4330e68d662c0928feba19f636aab0fe37e13910882c3d0d72a",
        "0b5dac4b7e0cbc8beca983743f02bce0d7b801bf6a73c99b7d339c95f0e224dd",
        "cd178870a4583120c033a3ce1460f6d42d98a88d16e6120939858d9d1d6feafe",
        "2072d04734b060c00d79bdac0099e018780157368ea153eb8d15018aa08a6216",
        "e3b65500b75b0aa66e2beb81fa5a847c90300929f153810aaea85de1c23b6aca",
        "13a0d89c0c528d48c9bd1740c4e4395ba71d314ed8195d389241c9c88e8987f1",
        "862d93cced51b5610b76b05c0a116e6f9a4f546154ea23d297cd43bf945fc794",
        "b1504a1b2b4a253cd4439db05002c9096ab1ac748753d8a348265bde0c2408d7",
        "67300ecd636451a5436093d1337ad572f582f6247e0e98a2e6d0109699d5dfa8",
        "adb505bd36ea55f724a25f7696d5bcd7ef47f2340f26ff5af488456aea5aecfa",
        "4d8728fd4cd5894a561f0eaf2bdeb52635703a81936986b39c845319503fd0a3",
        "18683225b1345714ee7b63c617f1492ce4e307e0cf1984a29c9bebe8385782cc",
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
    public void CatalogCommandsAreStaticSelectOnlyAndPreflightLargeExpressions()
    {
        Assert.Equal(13, SqlServerCatalogReader.CommandTexts.Count);
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
