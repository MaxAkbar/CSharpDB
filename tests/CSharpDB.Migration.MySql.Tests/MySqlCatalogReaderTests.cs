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
        "2418f45547c5a38ccdacbcdd4ee31a195ae0da1b9e7d97651f31c3559b01f998",
        "5456ee05ea9f9eeefdb08cc30e345962a2de7d8df0dd61ce27b080d2b6471c7c",
        "3b12420233f59978e684098362bb7f556ea0fe94eec5dceabab0bb13915243d8",
        "ea21132749e445022a591add54037d00a87b477c98c18cb9bbe2f42454812e7c",
        "cb880529e6ff62c3076accfc162c88816f9697485f193408255caef9aa12f3ac",
        "025dada3390acf714b4136131926f5d95766e8020af03693d9b3fdcdb60d7f99",
        "77017fb089d66b4eb0e00ddef156fa32d73e7f9be10a2ccf1671a13d945fb9b6",
        "f9f5cacbc5376060306f156ff3f0b98c65c4abea7b6ffea06d4ddc33879f9a06",
        "9609599dbb325bd9253f3d37aeb51c153a05c99627813326aaa016c1e2811b54",
        "0f8b164fd063d4bf0c95a808b82e9548fe1774054814f0bd08f81c15d5c0443e",
        "486cec1707cf49bb5c8ad1af9fa4a2e3f28661d8a1711571735b64a030430a01",
        "ab484bab80e361c6967b346c6a293598b39e0ae8b4c00eed86a400523093f17b",
        "dad175f33479cb37a556e350cff20874f2955b1718eef88ca6d492097f5e9ad5",
        "13ae2da8f65213b2b1452f2d3b3b0e0effb5e59885ab45dd0f8600ed0d3fb4d0",
        "dca39d3ed1bcd6e4e74512833c573271b54b78b37825b172160b16fc82dbf58e",
        "592e5b5d2fd1521edd06ed02a71b2f278d7b66ee2d9f0a21a69624342983e822",
        "e2c97154bc27c81363af413b2a785e4aba489c6bbff4171c8755ec94bb614469",
        "6c3b9b6d613538e6bed70fedd79df9a3c132d065bfb10206174518df419354a0",
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
        Assert.False(reader.Policy.TreatTinyAsBoolean);
        Assert.True(reader.Policy.AllowZeroDateTime);
        Assert.False(reader.Policy.ConvertZeroDateTime);
        Assert.Equal("Unspecified", reader.Policy.DateTimeKind);
        Assert.Equal("None", reader.Policy.GuidFormat);
        Assert.False(reader.Policy.IgnoreCommandTransaction);
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
        Assert.Equal(
            [
                MySqlCatalogReader.ServerAndDatabaseQuery,
                MySqlCatalogReader.GeneratedInvisiblePrimaryKeyVisibilityQuery,
                MySqlCatalogReader.MetadataVisibilityProofQuery,
                MySqlCatalogReader.TablesQuery,
                MySqlCatalogReader.ColumnsQuery,
                MySqlCatalogReader.KeysQuery,
                MySqlCatalogReader.KeyColumnsQuery,
                MySqlCatalogReader.ForeignKeysQuery,
                MySqlCatalogReader.ForeignKeyColumnsQuery,
                MySqlCatalogReader.ChecksQuery,
                MySqlCatalogReader.IndexesQuery,
                MySqlCatalogReader.LegacyIndexesQuery,
                MySqlCatalogReader.UnqualifiedIndexesQuery,
                MySqlCatalogReader.ViewsQuery,
                MySqlCatalogReader.ViewColumnsQuery,
                MySqlCatalogReader.TriggersQuery,
                MySqlCatalogReader.RoutinesQuery,
                MySqlCatalogReader.RoutineParametersQuery,
            ],
            MySqlCatalogReader.CommandTexts);
        foreach (string command in MySqlCatalogReader.CommandTexts)
        {
            Assert.StartsWith(
                "SELECT",
                command.TrimStart(),
                StringComparison.OrdinalIgnoreCase);
            string statementAuditText = command.Replace(
                "'EXECUTE'",
                "''",
                StringComparison.Ordinal);
            Assert.DoesNotMatch(
                MutatingStatement(),
                statementAuditText);
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
            "INFORMATION_SCHEMA.SCHEMATA",
            MySqlCatalogReader.ServerAndDatabaseQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "BINARY s.SCHEMA_NAME = BINARY DATABASE()",
            MySqlCatalogReader.ServerAndDatabaseQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "@@session.show_gipk_in_create_table_and_information_schema",
            MySqlCatalogReader.GeneratedInvisiblePrimaryKeyVisibilityQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "@@session.sql_quote_show_create",
            MySqlCatalogReader.ServerAndDatabaseQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "@@session.explicit_defaults_for_timestamp",
            MySqlCatalogReader.ServerAndDatabaseQuery,
            StringComparison.Ordinal);
        string[] databaseScopedQueries =
        [
            MySqlCatalogReader.MetadataVisibilityProofQuery,
            MySqlCatalogReader.TablesQuery,
            MySqlCatalogReader.ColumnsQuery,
            MySqlCatalogReader.KeysQuery,
            MySqlCatalogReader.KeyColumnsQuery,
            MySqlCatalogReader.ForeignKeysQuery,
            MySqlCatalogReader.ForeignKeyColumnsQuery,
            MySqlCatalogReader.ChecksQuery,
            MySqlCatalogReader.IndexesQuery,
            MySqlCatalogReader.LegacyIndexesQuery,
            MySqlCatalogReader.UnqualifiedIndexesQuery,
            MySqlCatalogReader.ViewsQuery,
            MySqlCatalogReader.ViewColumnsQuery,
            MySqlCatalogReader.TriggersQuery,
            MySqlCatalogReader.RoutinesQuery,
            MySqlCatalogReader.RoutineParametersQuery,
        ];
        Assert.All(
            databaseScopedQueries,
            static query =>
            {
                Assert.Equal(1, CountOccurrences(query, "@database_name"));
                Assert.Contains(
                    "BINARY @database_name",
                    query,
                    StringComparison.Ordinal);
            });
        Assert.Contains(
            "INFORMATION_SCHEMA.TABLES",
            MySqlCatalogReader.TablesQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "CURRENT_USER()",
            MySqlCatalogReader.MetadataVisibilityProofQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "INFORMATION_SCHEMA.SCHEMA_PRIVILEGES",
            MySqlCatalogReader.MetadataVisibilityProofQuery,
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
            "OCTET_LENGTH(c.COLUMN_TYPE)",
            MySqlCatalogReader.ColumnsQuery,
            StringComparison.Ordinal);
        Assert.True(
            MySqlCatalogReader.ColumnsQuery.IndexOf(
                "OCTET_LENGTH(c.COLUMN_TYPE)",
                StringComparison.Ordinal) <
            MySqlCatalogReader.ColumnsQuery.IndexOf(
                "c.COLUMN_TYPE,",
                StringComparison.Ordinal));
        Assert.Contains(
            "OCTET_LENGTH(c.GENERATION_EXPRESSION)",
            MySqlCatalogReader.ColumnsQuery,
            StringComparison.Ordinal);
        AssertLengthProjectionPrecedesValue(
            MySqlCatalogReader.ColumnsQuery,
            "OCTET_LENGTH(c.COLUMN_DEFAULT)",
            "c.COLUMN_DEFAULT");
        Assert.Contains(
            "t.TABLE_TYPE = 'BASE TABLE'",
            MySqlCatalogReader.ColumnsQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "INFORMATION_SCHEMA.TABLE_CONSTRAINTS",
            MySqlCatalogReader.KeysQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "INFORMATION_SCHEMA.KEY_COLUMN_USAGE",
            MySqlCatalogReader.KeyColumnsQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS",
            MySqlCatalogReader.ForeignKeysQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "kcu.POSITION_IN_UNIQUE_CONSTRAINT",
            MySqlCatalogReader.ForeignKeyColumnsQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "OCTET_LENGTH(cc.CHECK_CLAUSE)",
            MySqlCatalogReader.ChecksQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "INFORMATION_SCHEMA.STATISTICS",
            MySqlCatalogReader.IndexesQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "OCTET_LENGTH(s.EXPRESSION)",
            MySqlCatalogReader.IndexesQuery,
            StringComparison.Ordinal);
        Assert.True(
            MySqlCatalogReader.IndexesQuery.IndexOf(
                "OCTET_LENGTH(s.EXPRESSION)",
                StringComparison.Ordinal) <
            MySqlCatalogReader.IndexesQuery.LastIndexOf(
                "s.EXPRESSION",
                StringComparison.Ordinal));
        Assert.DoesNotContain(
            "s.EXPRESSION",
            MySqlCatalogReader.LegacyIndexesQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "CONVERT(NULL, CHAR)",
            MySqlCatalogReader.LegacyIndexesQuery,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "s.IS_VISIBLE",
            MySqlCatalogReader.UnqualifiedIndexesQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "'YES'",
            MySqlCatalogReader.UnqualifiedIndexesQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "INFORMATION_SCHEMA.VIEWS",
            MySqlCatalogReader.ViewsQuery,
            StringComparison.Ordinal);
        AssertLengthProjectionPrecedesValue(
            MySqlCatalogReader.ViewsQuery,
            "OCTET_LENGTH(v.VIEW_DEFINITION)",
            "v.VIEW_DEFINITION");
        Assert.Contains(
            "t.TABLE_TYPE = 'VIEW'",
            MySqlCatalogReader.ViewColumnsQuery,
            StringComparison.Ordinal);
        AssertLengthProjectionPrecedesValue(
            MySqlCatalogReader.ViewColumnsQuery,
            "OCTET_LENGTH(c.COLUMN_TYPE)",
            "c.COLUMN_TYPE");
        Assert.Contains(
            "INFORMATION_SCHEMA.TRIGGERS",
            MySqlCatalogReader.TriggersQuery,
            StringComparison.Ordinal);
        AssertLengthProjectionPrecedesValue(
            MySqlCatalogReader.TriggersQuery,
            "OCTET_LENGTH(tr.ACTION_STATEMENT)",
            "tr.ACTION_STATEMENT");
        Assert.Contains(
            "INFORMATION_SCHEMA.ROUTINES",
            MySqlCatalogReader.RoutinesQuery,
            StringComparison.Ordinal);
        AssertLengthProjectionPrecedesValue(
            MySqlCatalogReader.RoutinesQuery,
            "OCTET_LENGTH(r.DTD_IDENTIFIER)",
            "r.DTD_IDENTIFIER");
        AssertLengthProjectionPrecedesValue(
            MySqlCatalogReader.RoutinesQuery,
            "OCTET_LENGTH(r.ROUTINE_DEFINITION)",
            "r.ROUTINE_DEFINITION");
        Assert.Contains(
            "INFORMATION_SCHEMA.PARAMETERS",
            MySqlCatalogReader.RoutineParametersQuery,
            StringComparison.Ordinal);
        AssertLengthProjectionPrecedesValue(
            MySqlCatalogReader.RoutineParametersQuery,
            "OCTET_LENGTH(p.DTD_IDENTIFIER)",
            "p.DTD_IDENTIFIER");

        string commands = string.Join("\n", MySqlCatalogReader.CommandTexts);
        string commandStatementAudit = commands.Replace(
            "'SHOW VIEW'",
            "''",
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "SHOW ",
            commandStatementAudit,
            StringComparison.OrdinalIgnoreCase);
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
        Assert.DoesNotContain("CARDINALITY", commands, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("TABLE_COMMENT", commands, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("INDEX_COMMENT", commands, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("DEFINER", commands, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("ROUTINE_COMMENT", commands, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CREATED", commands, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("LAST_ALTERED", commands, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("LAST_EXECUTED", commands, StringComparison.OrdinalIgnoreCase);

        string[] actualDigests = MySqlCatalogReader.CommandTexts
            .Select(static command =>
                Convert.ToHexString(
                        SHA256.HashData(Encoding.UTF8.GetBytes(command)))
                    .ToLowerInvariant())
            .ToArray();
        Assert.Equal(s_auditedCommandDigests, actualDigests);
    }

    [Theory]
    [InlineData("8.0.12", "MySQL Community Server - GPL", "legacy")]
    [InlineData("8.0.13", "MySQL Community Server - GPL", "current")]
    [InlineData("8.4.5", "MySQL Enterprise Server - Commercial", "current")]
    [InlineData("5.7.44", "MySQL Community Server - GPL", "unqualified")]
    [InlineData("10.11.8-MariaDB", "mariadb.org binary distribution", "unqualified")]
    [InlineData("8.0.mysql_aurora.3.08.0", "Amazon Aurora MySQL", "unqualified")]
    public void FunctionalIndexProjectionIsVersionAndVariantBounded(
        string version,
        string versionComment,
        string projection)
    {
        MySqlServerMetadata server = MySqlTestSnapshot.Server() with
        {
            Version = version,
            VersionComment = versionComment,
        };

        Assert.Equal(
            projection switch
            {
                "current" => MySqlCatalogReader.IndexesQuery,
                "legacy" => MySqlCatalogReader.LegacyIndexesQuery,
                _ => MySqlCatalogReader.UnqualifiedIndexesQuery,
            },
            MySqlCatalogReader.SelectIndexesQuery(server));
    }

    [Theory]
    [InlineData("8.0.15", "MySQL Community Server - GPL", false)]
    [InlineData("8.0.16", "MySQL Community Server - GPL", true)]
    [InlineData("8.4.5", "MySQL Enterprise Server - Commercial", true)]
    [InlineData("10.11.8-MariaDB", "mariadb.org binary distribution", false)]
    [InlineData("8.0.mysql_aurora.3.08.0", "Amazon Aurora MySQL", false)]
    public void CheckInventoryIsVersionAndVariantBounded(
        string version,
        string versionComment,
        bool expected)
    {
        MySqlServerMetadata server = MySqlTestSnapshot.Server() with
        {
            Version = version,
            VersionComment = versionComment,
        };

        Assert.Equal(
            expected,
            MySqlCatalogReader.ShouldReadCheckConstraints(server));
    }

    [Theory]
    [InlineData("DEFAULT_GENERATED on update CURRENT_TIMESTAMP", true)]
    [InlineData("DEFAULT_GENERATED on update CURRENT_TIMESTAMP()", true)]
    [InlineData("DEFAULT_GENERATED on update CURRENT_TIMESTAMP(6)", true)]
    [InlineData("DEFAULT_GENERATED on update CURRENT_TIMESTAMP(7)", false)]
    [InlineData("DEFAULT_GENERATED on update CURRENT_TIMESTAMP(foo)", false)]
    public void OnUpdateDetectionRecognizesCurrentTimestampPrecision(
        string extra,
        bool expected)
    {
        MethodInfo? method = typeof(MySqlCatalogReader).GetMethod(
            "HasSequence",
            BindingFlags.Static | BindingFlags.NonPublic);
        Assert.NotNull(method);

        bool actual = Assert.IsType<bool>(
            method.Invoke(
                null,
                [extra, new[] { "on", "update", "CURRENT_TIMESTAMP" }]));

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void ShowCreateIdentifiersAreQuotedAsData()
    {
        const string schema = "Source`; DROP DATABASE Important; --";
        const string table = "Order`; SELECT SLEEP(99); --";

        string command = MySqlCatalogReader.BuildShowCreateTableCommand(
            schema,
            table);

        Assert.Equal(
            "SHOW CREATE TABLE " +
            "`Source``; DROP DATABASE Important; --`." +
            "`Order``; SELECT SLEEP(99); --`;",
            command);
        Assert.Equal("`a``b;c`", MySqlCatalogReader.QuoteIdentifier("a`b;c"));
        Assert.Throws<ArgumentException>(
            () => MySqlCatalogReader.QuoteIdentifier(string.Empty));
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

    private static void AssertLengthProjectionPrecedesValue(
        string query,
        string lengthProjection,
        string valueProjection) =>
        Assert.True(
            query.IndexOf(lengthProjection, StringComparison.Ordinal) <
            query.LastIndexOf(valueProjection, StringComparison.Ordinal));
}
