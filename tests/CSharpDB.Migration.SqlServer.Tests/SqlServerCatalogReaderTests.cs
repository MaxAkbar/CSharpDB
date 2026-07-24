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
        "cd1475e0cfc7963444bbd2006fd067d460a20fd45e071387f6aef30e6b215e85",
        "4cf1fbb5db2646e3312a59301d6d26d4cae4bf1685ff7cd665081c6c0f382881",
        "d6759973b36f00b17dfab8de8aeca0580f99773d5f00680ac066740e6904d66e",
        "cd178870a4583120c033a3ce1460f6d42d98a88d16e6120939858d9d1d6feafe",
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
        Assert.Equal(4, SqlServerCatalogReader.CommandTexts.Count);
        foreach (string command in SqlServerCatalogReader.CommandTexts)
        {
            Assert.StartsWith(
                "SELECT",
                command.TrimStart(),
                StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotMatch(MutatingStatement(), command);
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
