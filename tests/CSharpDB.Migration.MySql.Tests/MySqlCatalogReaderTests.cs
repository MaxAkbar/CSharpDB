using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using CSharpDB.Migration.MySql;

namespace CSharpDB.Migration.MySql.Tests;

public sealed class MySqlCatalogReaderTests
{
    private const string Secret = "DistinctiveMySqlReaderPassword-42";
    private static readonly string[] s_auditedCommandDigests =
    [
        "b12c2730f1e7a007180a8fc65560555111628073adb4248dbe57cf22fa7f9a38",
        "5456ee05ea9f9eeefdb08cc30e345962a2de7d8df0dd61ce27b080d2b6471c7c",
        "68146a0e02ded98f428bbd6d5a090196c6e126c9b2a74ef1906376933fc5cbfd",
        "4d83896c4248c0a6fd3d55145e983e42edb256c5b2d45fa26cc917a6d1a9ede2",
    ];

    [Fact]
    public void ConnectionPolicyForcesMetadataSafetyAndPreservesTlsVerification()
    {
        var reader = new MySqlCatalogReader(
            "Server=mysql.example.invalid;Port=3307;Database=SourceDb;" +
            "User ID=reader;" +
            $"Password={Secret};" +
            "SslMode=VerifyFull;" +
            "Pooling=True;AllowLoadLocalInfile=True;AllowUserVariables=True;" +
            "AutoEnlist=True;PersistSecurityInfo=True;" +
            "ConnectionTimeout=0;DefaultCommandTimeout=0;CancellationTimeout=0");

        Assert.False(reader.Policy.Pooling);
        Assert.False(reader.Policy.AllowLoadLocalInfile);
        Assert.False(reader.Policy.AllowUserVariables);
        Assert.False(reader.Policy.AutoEnlist);
        Assert.False(reader.Policy.PersistSecurityInfo);
        Assert.Equal(30u, reader.Policy.ConnectionTimeout);
        Assert.Equal(30u, reader.Policy.DefaultCommandTimeout);
        Assert.Equal(5, reader.Policy.CancellationTimeout);
        Assert.Equal("VerifyFull", reader.Policy.SslMode);
        Assert.DoesNotContain(
            Secret,
            string.Join(
                "|",
                reader.EndpointDigest,
                reader.Policy.SslMode),
            StringComparison.Ordinal);
    }

    [Fact]
    public void ConnectionPolicyPreservesStricterTimeouts()
    {
        var reader = new MySqlCatalogReader(
            "Server=mysql.example.invalid;Database=SourceDb;" +
            "SslMode=Required;" +
            "ConnectionTimeout=5;DefaultCommandTimeout=6;CancellationTimeout=2");

        Assert.Equal(5u, reader.Policy.ConnectionTimeout);
        Assert.Equal(6u, reader.Policy.DefaultCommandTimeout);
        Assert.Equal(2, reader.Policy.CancellationTimeout);
    }

    [Theory]
    [InlineData("Required")]
    [InlineData("VerifyCA")]
    [InlineData("VerifyFull")]
    public void ConnectionPolicyDoesNotRewriteCallerSelectedTlsMode(
        string sslMode)
    {
        var reader = new MySqlCatalogReader(
            "Server=mysql.example.invalid;Database=SourceDb;" +
            $"SslMode={sslMode}");

        Assert.Equal(sslMode, reader.Policy.SslMode);
    }

    [Theory]
    [InlineData("Disabled")]
    [InlineData("None")]
    [InlineData("Preferred")]
    public void TcpConnectionPolicyRejectsPlaintextCapableTlsModes(
        string sslMode)
    {
        ArgumentException error = Assert.Throws<ArgumentException>(
            () => new MySqlCatalogReader(
                "Server=mysql.example.invalid;Database=SourceDb;" +
                $"SslMode={sslMode}"));

        Assert.Equal("connectionString", error.ParamName);
        Assert.Null(error.InnerException);
    }

    [Theory]
    [InlineData("Disabled", "None")]
    [InlineData("None", "None")]
    [InlineData("Preferred", "Preferred")]
    public void LocalUnixSocketMayUseNonTlsTransportModes(
        string sslMode,
        string expectedPolicyMode)
    {
        var reader = new MySqlCatalogReader(
            "ConnectionProtocol=UnixSocket;Server=/tmp/mysql.sock;" +
            "Database=SourceDb;" +
            $"SslMode={sslMode}");

        Assert.Equal(expectedPolicyMode, reader.Policy.SslMode);
    }

    [Theory]
    [InlineData("Server=mysql.example.invalid;SslMode=Required")]
    [InlineData("Database=SourceDb;SslMode=Required")]
    public void ConnectionPolicyRequiresOneServerAndDatabase(
        string connectionString)
    {
        ArgumentException error = Assert.Throws<ArgumentException>(
            () => new MySqlCatalogReader(connectionString));

        Assert.Equal("connectionString", error.ParamName);
        Assert.Null(error.InnerException);
    }

    [Theory]
    [InlineData("mysql-one.example.invalid,mysql-two.example.invalid")]
    [InlineData("mysql-one.example.invalid, mysql-two.example.invalid")]
    public void ConnectionPolicyRejectsMultiHostServerValues(string servers)
    {
        ArgumentException error = Assert.Throws<ArgumentException>(
            () => new MySqlCatalogReader(
                $"Server={servers};Database=SourceDb;SslMode=Required"));

        Assert.Equal("connectionString", error.ParamName);
        Assert.Null(error.InnerException);
        Assert.DoesNotContain(servers, error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void InvalidConnectionSettingsDoNotRetainInputExceptionOrSecret()
    {
        ArgumentException error = Assert.Throws<ArgumentException>(
            () => new MySqlCatalogReader(
                "Server=source;Database=fixture;" +
                $"Password={Secret};DefinitelyNotAKeyword=value"));

        Assert.Null(error.InnerException);
        Assert.DoesNotContain(Secret, error.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain(
            "DefinitelyNotAKeyword",
            error.ToString(),
            StringComparison.Ordinal);
    }

    [Fact]
    public void InvalidUnicodeEndpointIsRejectedWithoutRetainingInput()
    {
        const string invalidEndpoint = "invalid\uD800endpoint";

        ArgumentException error = Assert.Throws<ArgumentException>(
            () => new MySqlCatalogReader(
                $"Server={invalidEndpoint};Database=fixture;SslMode=Required"));

        Assert.Null(error.InnerException);
        Assert.Equal("connectionString", error.ParamName);
        Assert.DoesNotContain(
            invalidEndpoint,
            error.ToString(),
            StringComparison.Ordinal);
    }

    [Fact]
    public void EndpointDigestExcludesAccountDatabaseAndCredentialMaterial()
    {
        var first = new MySqlCatalogReader(
            "Server=MySql.Example.Invalid;Port=3307;Database=One;" +
            "User ID=first;Password=first-secret;SslMode=Required");
        var differentAccountAndDatabase = new MySqlCatalogReader(
            "Server=mysql.example.invalid;Port=3307;Database=Two;" +
            "User ID=second;Password=second-secret;SslMode=Required");
        var differentPort = new MySqlCatalogReader(
            "Server=mysql.example.invalid;Port=3308;Database=One;" +
            "User ID=first;Password=first-secret;SslMode=Required");

        Assert.Equal(first.EndpointDigest, differentAccountAndDatabase.EndpointDigest);
        Assert.NotEqual(first.EndpointDigest, differentPort.EndpointDigest);
        Assert.StartsWith("sha256:", first.EndpointDigest, StringComparison.Ordinal);
        Assert.DoesNotContain("mysql.example.invalid", first.EndpointDigest);
        Assert.DoesNotContain("first-secret", first.EndpointDigest);
    }

    [Theory]
    [InlineData("8.0.29", "MySQL Community Server - GPL", false)]
    [InlineData("8.0.30", "MySQL Community Server - GPL", true)]
    [InlineData("8.4.5", "MySQL Enterprise Server - Commercial", true)]
    [InlineData("9.0.1", "MySQL Community Server - GPL", true)]
    [InlineData("10.11.8-MariaDB", "mariadb.org binary distribution", false)]
    [InlineData("8.0.mysql_aurora.3.08.0", "Amazon Aurora MySQL", false)]
    public void GipkVisibilityProbeIsLimitedToSupportingOracleServers(
        string version,
        string versionComment,
        bool expected)
    {
        MethodInfo? method = typeof(MySqlCatalogReader).GetMethod(
            "ShouldReadGeneratedInvisiblePrimaryKeyVisibility",
            BindingFlags.Static | BindingFlags.NonPublic);
        Assert.NotNull(method);
        MySqlServerMetadata server = MySqlTestSnapshot.Server() with
        {
            Version = version,
            VersionComment = versionComment,
        };

        bool actual = Assert.IsType<bool>(method.Invoke(null, [server]));

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void CatalogCommandsAreFixedBoundedMetadataSelects()
    {
        Assert.Equal(4, MySqlCatalogReader.CommandTexts.Count);
        foreach (string command in MySqlCatalogReader.CommandTexts)
        {
            Assert.StartsWith(
                "SELECT",
                command.TrimStart(),
                StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotMatch(MutatingStatement(), command);
            Assert.DoesNotMatch(DangerousReadClause(), command);
        }

        Assert.DoesNotMatch(
            new Regex(
                @"(?<!@)@(?!@)",
                RegexOptions.CultureInvariant),
            MySqlCatalogReader.ServerAndDatabaseQuery);
        Assert.Contains(
            "DATABASE()",
            MySqlCatalogReader.ServerAndDatabaseQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "@@session.show_gipk_in_create_table_and_information_schema",
            MySqlCatalogReader.GeneratedInvisiblePrimaryKeyVisibilityQuery,
            StringComparison.Ordinal);
        Assert.Equal(
            1,
            CountOccurrences(
                MySqlCatalogReader.TablesQuery,
                "@database_name"));
        Assert.Equal(
            1,
            CountOccurrences(
                MySqlCatalogReader.ColumnsQuery,
                "@database_name"));
        Assert.Contains(
            "INFORMATION_SCHEMA.TABLES",
            MySqlCatalogReader.TablesQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "INFORMATION_SCHEMA.COLUMNS",
            MySqlCatalogReader.ColumnsQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "c.COLUMN_TYPE",
            MySqlCatalogReader.ColumnsQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "OCTET_LENGTH(c.GENERATION_EXPRESSION)",
            MySqlCatalogReader.ColumnsQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "t.TABLE_TYPE = 'BASE TABLE'",
            MySqlCatalogReader.ColumnsQuery,
            StringComparison.Ordinal);

        string commands = string.Join("\n", MySqlCatalogReader.CommandTexts);
        Assert.DoesNotContain("SHOW ", commands, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("mysql.", commands, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            "performance_schema.",
            commands,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("TABLE_ROWS", commands, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("DATA_LENGTH", commands, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("INDEX_LENGTH", commands, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("AUTO_INCREMENT", commands, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CREATE_TIME", commands, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("UPDATE_TIME", commands, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CHECK_TIME", commands, StringComparison.OrdinalIgnoreCase);

        string[] actualDigests = MySqlCatalogReader.CommandTexts
            .Select(static command =>
                Convert.ToHexString(
                        SHA256.HashData(Encoding.UTF8.GetBytes(command)))
                    .ToLowerInvariant())
            .ToArray();
        Assert.Equal(s_auditedCommandDigests, actualDigests);
    }

    private static Regex MutatingStatement() =>
        new(
            @"\b(INSERT|UPDATE|DELETE|REPLACE|MERGE|ALTER|CREATE|DROP|TRUNCATE|LOAD|CALL|DO|HANDLER|LOCK|UNLOCK|SET|USE|START|COMMIT|ROLLBACK|SAVEPOINT|PREPARE|EXECUTE|DEALLOCATE|GRANT|REVOKE|ANALYZE|OPTIMIZE|REPAIR|FLUSH|RESET|KILL|INSTALL|UNINSTALL)\b",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static Regex DangerousReadClause() =>
        new(
            @"\b(INTO\s+(OUTFILE|DUMPFILE)|FOR\s+UPDATE|LOCK\s+IN\s+SHARE\s+MODE|SLEEP\s*\(|BENCHMARK\s*\(|LOAD_FILE\s*\()",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static int CountOccurrences(string value, string token) =>
        value.Split(token, StringSplitOptions.None).Length - 1;
}
